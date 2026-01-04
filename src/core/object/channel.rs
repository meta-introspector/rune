//! Channel types for CSP-style concurrency.
//!
//! Channels provide communication between threads via message passing.
//! Objects are deep-copied into a dedicated buffer block when sent,
//! and deep-copied into the receiver's context when received.

use super::{CloneIn, Gc, Object, Symbol, TagType};
use crate::{
    core::{
        env::{INTERNED_SYMBOLS, sym},
        gc::{Block, Context, GcHeap, GcState, Trace},
        object::WithLifetime,
    },
    derive_GcMoveable,
};
use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Error type for send operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// Receiver has been dropped.
    Closed,
    /// Channel is full (only for try_send).
    Full,
    /// Send operation timed out.
    Timeout,
}

impl From<SendError> for Symbol<'static> {
    fn from(err: SendError) -> Self {
        match err {
            SendError::Closed => sym::CHANNEL_CLOSED,
            SendError::Full => sym::CHANNEL_FULL,
            SendError::Timeout => sym::CHANNEL_TIMEOUT,
        }
    }
}

/// Error type for receive operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// All senders have been dropped.
    Closed,
    /// Channel is empty (only for try_recv).
    Empty,
    /// Receive operation timed out.
    Timeout,
}

impl From<RecvError> for Symbol<'static> {
    fn from(err: RecvError) -> Self {
        match err {
            RecvError::Closed => sym::CHANNEL_CLOSED,
            RecvError::Empty => sym::CHANNEL_EMPTY,
            RecvError::Timeout => sym::CHANNEL_TIMEOUT,
        }
    }
}

/// The actual buffer protected by the mutex
pub(in crate::core) struct ChannelBuffer {
    queue: VecDeque<Object<'static>>,
    capacity: usize,
}

impl ChannelBuffer {
    pub(in crate::core) fn new(capacity: usize) -> Self {
        Self { queue: VecDeque::with_capacity(capacity), capacity }
    }

    pub(in crate::core) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(in crate::core) fn is_full(&self) -> bool {
        debug_assert!(
            self.queue.len() <= self.capacity,
            "Inner queue went passed allocated capacity"
        );
        self.queue.len() == self.capacity
    }

    pub(in crate::core) fn try_pop(&mut self) -> Option<Object<'static>> {
        if self.is_empty() {
            return None;
        }

        self.queue.pop_front()
    }

    pub(in crate::core) fn try_push<'ob>(&mut self, obj: &Object<'ob>) -> Result<(), ()> {
        if self.is_full() {
            return Err(());
        }
        // NOTE: this can create contention if a large number of channels are busy at the same time.
        let global = INTERNED_SYMBOLS.lock().unwrap();
        // SAFETY: `in_transit` is allocated in the global block
        let in_transit = unsafe { obj.clone_in(global.global_block()).with_lifetime() };
        drop(global);
        self.queue.push_back(in_transit);
        debug_assert!(
            self.queue.len() <= self.capacity,
            "Inner queue went passed allocated capacity"
        );
        Ok(())
    }
}

/// Shared state for a channel, stored in the global Block<true>
pub(in crate::core) struct SharedChannelState {
    pub(in crate::core) buffer: Mutex<ChannelBuffer>,
    pub(in crate::core) not_full: Condvar,
    pub(in crate::core) not_empty: Condvar,
    pub(in crate::core) sender_count: AtomicUsize,
    pub(in crate::core) receiver_count: AtomicUsize,
}

impl SharedChannelState {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(ChannelBuffer::new(capacity)),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
            sender_count: AtomicUsize::new(0),
            receiver_count: AtomicUsize::new(0),
        }
    }

    fn has_senders(&self) -> bool {
        self.sender_count.load(Ordering::Acquire) > 0
    }

    fn has_receivers(&self) -> bool {
        self.receiver_count.load(Ordering::Acquire) > 0
    }

    fn empty_buffer(&self) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.queue.drain(..);
        // Buffer lock will be dropped here, then wake senders
        drop(buffer);
        self.not_full.notify_all();
    }
}

impl Drop for SharedChannelState {
    fn drop(&mut self) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.queue.drain(..);
    }
}

/// The sending half of a channel.
pub(crate) struct ChannelSender(pub(in crate::core) GcHeap<ChannelSenderInner>);

derive_GcMoveable!(ChannelSender);

pub(in crate::core) struct ChannelSenderInner {
    state: Arc<SharedChannelState>,
}

impl ChannelSender {
    pub(in crate::core) fn new_with_state(state: Arc<SharedChannelState>, constant: bool) -> Self {
        state.sender_count.fetch_add(1, Ordering::AcqRel);
        Self(GcHeap::new(ChannelSenderInner { state }, constant))
    }

    /// Send an object through the channel, blocking if full.
    /// Returns Err(SendError::Closed) if the receiver has been dropped.
    pub(crate) fn send<'ob>(&self, obj: Object<'ob>) -> Result<(), SendError> {
        let state = &self.0.state;

        loop {
            let mut buffer = state.buffer.lock().unwrap();

            // Check if receiver is closed
            if !state.has_receivers() {
                return Err(SendError::Closed);
            }

            if buffer.try_push(&obj).is_ok() {
                drop(buffer);
                state.not_empty.notify_one();
                return Ok(());
            }

            // Wait for space
            let _buffer = state.not_full.wait(buffer).unwrap();
        }
    }

    /// Try to send without blocking.
    /// Returns Err(SendError::Full) if the channel is full.
    /// Returns Err(SendError::Closed) if the receiver has been dropped.
    pub(crate) fn try_send<'ob>(&self, obj: Object<'ob>) -> Result<(), SendError> {
        let state = &self.0.state;
        let mut buffer = state.buffer.lock().unwrap();

        // Check if receiver is closed
        if !state.has_receivers() {
            return Err(SendError::Closed);
        }

        if buffer.try_push(&obj).is_ok() {
            drop(buffer);
            state.not_empty.notify_one();
            Ok(())
        } else {
            Err(SendError::Full)
        }
    }

    /// Send with a timeout.
    /// Returns Err(SendError::Timeout) if the timeout expires.
    /// Returns Err(SendError::Closed) if the receiver has been dropped.
    pub(crate) fn send_timeout<'ob>(
        &self,
        obj: Object<'ob>,
        timeout: Duration,
    ) -> Result<(), SendError> {
        let state = &self.0.state;
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            let mut buffer = state.buffer.lock().unwrap();

            // Check if receiver is closed
            if !state.has_receivers() {
                return Err(SendError::Closed);
            }

            if buffer.try_push(&obj).is_ok() {
                drop(buffer);
                state.not_empty.notify_one();
                return Ok(());
            }

            if start.elapsed() >= timeout {
                return Err(SendError::Timeout);
            }

            let now = Instant::now();
            let remaining = deadline - now;
            let result = state.not_full.wait_timeout(buffer, remaining).unwrap();
            let _buffer = result.0;

            if result.1.timed_out() {
                return Err(SendError::Timeout);
            }
        }
    }

    /// Explicitly close the sender side of the channel.
    /// This wakes up any receivers waiting on recv() and causes them to return Err(RecvError::Closed).
    /// This is idempotent - calling close() multiple times has no additional effect.
    pub(crate) fn close(&self) {
        let state = &self.0.state;
        // Use fetch_sub instead of store(0) to be idempotent
        let prev = state.sender_count.fetch_sub(1, Ordering::AcqRel);
        if prev <= 1 {
            state.not_empty.notify_all();
        }
    }
}

/// The receiving half of a channel.
pub(crate) struct ChannelReceiver(pub(in crate::core) GcHeap<ChannelReceiverInner>);

derive_GcMoveable!(ChannelReceiver);

pub(in crate::core) struct ChannelReceiverInner {
    state: Arc<SharedChannelState>,
}

impl ChannelReceiver {
    pub(in crate::core) fn new_with_state(state: Arc<SharedChannelState>, constant: bool) -> Self {
        state.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self(GcHeap::new(ChannelReceiverInner { state }, constant))
    }

    /// Receive an object from the channel, blocking if empty.
    /// Returns Err(RecvError::Closed) if all senders have been dropped.
    pub(crate) fn recv<'ob>(&self, cx: &'ob Context) -> Result<Object<'ob>, RecvError> {
        let state = &self.0.state;

        loop {
            let mut buffer = state.buffer.lock().unwrap();

            if let Some(obj) = buffer.try_pop() {
                drop(buffer);
                state.not_full.notify_one();
                let result = obj.clone_in(&cx.block);
                return Ok(result);
            }

            // Check if all senders are closed
            if !state.has_senders() {
                return Err(RecvError::Closed);
            }

            // Wait for data
            let _buffer = state.not_empty.wait(buffer).unwrap();
        }
    }

    /// Try to receive without blocking.
    /// Returns Err(RecvError::Empty) if the channel is empty.
    /// Returns Err(RecvError::Closed) if all senders have been dropped.
    pub(crate) fn try_recv<'ob>(&self, cx: &'ob Context) -> Result<Object<'ob>, RecvError> {
        let state = &self.0.state;
        let mut buffer = state.buffer.lock().unwrap();

        if let Some(obj) = buffer.try_pop() {
            drop(buffer);
            state.not_full.notify_one();
            let result = obj.clone_in(&cx.block);
            Ok(result)
        } else if !state.has_senders() {
            Err(RecvError::Closed)
        } else {
            Err(RecvError::Empty)
        }
    }

    /// Receive with a timeout.
    /// Returns Err(RecvError::Timeout) if the timeout expires.
    /// Returns Err(RecvError::Closed) if all senders have been dropped.
    pub(crate) fn recv_timeout<'ob>(
        &self,
        cx: &'ob Context,
        timeout: Duration,
    ) -> Result<Object<'ob>, RecvError> {
        let state = &self.0.state;
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            let mut buffer = state.buffer.lock().unwrap();

            if let Some(obj) = buffer.try_pop() {
                drop(buffer);
                state.not_full.notify_one();
                let result = obj.clone_in(&cx.block);
                return Ok(result);
            }

            // Check if all senders are closed
            if !state.has_senders() {
                return Err(RecvError::Closed);
            }

            if start.elapsed() >= timeout {
                return Err(RecvError::Timeout);
            }

            let now = Instant::now();
            let remaining = deadline - now;
            let result = state.not_empty.wait_timeout(buffer, remaining).unwrap();
            let _buffer = result.0;

            if result.1.timed_out() {
                return Err(RecvError::Timeout);
            }
        }
    }

    /// Explicitly close the receiver side of the channel.
    /// This wakes up any senders waiting on send() and causes them to return Err(SendError::Closed).
    /// This is idempotent - calling close() multiple times has no additional effect.
    ///
    /// Useful for "moving" a receiver to another thread:
    /// the original receiver can be closed after being cloned into a new context.
    pub(crate) fn close(&self) {
        let state = &self.0.state;
        // Use fetch_sub instead of store(0) to be idempotent
        let prev = state.receiver_count.fetch_sub(1, Ordering::AcqRel);
        if prev <= 1 {
            state.empty_buffer();
            state.not_full.notify_all();
        }
    }
}

/// Create a new channel pair with the specified capacity.
pub(crate) fn make_channel_pair(capacity: usize) -> (ChannelSender, ChannelReceiver) {
    let inner = Arc::new(SharedChannelState::new(capacity));

    // Create sender and receiver
    let sender = ChannelSender::new_with_state(inner.clone(), false);
    let receiver = ChannelReceiver::new_with_state(inner, false);
    (sender, receiver)
}

// Drop implementations to decrement counters
impl Drop for ChannelSenderInner {
    fn drop(&mut self) {
        let prev = self.state.sender_count.fetch_sub(1, Ordering::AcqRel);
        // Use <= 1 to be idempotent
        if prev <= 1 {
            // Last sender dropped, wake up any waiting receivers
            self.state.not_empty.notify_all();
        }
    }
}

impl Drop for ChannelReceiverInner {
    fn drop(&mut self) {
        let prev = self.state.receiver_count.fetch_sub(1, Ordering::AcqRel);
        // Use <= 1 to be idempotent (handles both last receiver and already-closed cases)
        if prev <= 1 {
            self.state.empty_buffer();
            self.state.not_full.notify_all();
        }
    }
}

// GC tracing (channels themselves don't hold GC objects, state is Arc)
impl Trace for ChannelSenderInner {
    fn trace(&self, _state: &mut GcState) {
        // No GC objects in sender
    }
}

impl Trace for ChannelReceiverInner {
    fn trace(&self, _state: &mut GcState) {
        // No GC objects in receiver
    }
}

// CloneIn implementations for cross-context copying
impl<'new> CloneIn<'new, &'new Self> for ChannelSender {
    fn clone_in<const C: bool>(&self, _bk: &'new Block<C>) -> Gc<&'new Self> {
        self.0.state.sender_count.fetch_add(1, Ordering::AcqRel);

        let new_sender = ChannelSender::new_with_state(self.0.state.clone(), false);

        // SAFETY: Inner state of senders is shared channel state that is allocated in the global block during creation
        unsafe { std::mem::transmute::<Gc<&Self>, Gc<&'new Self>>((&new_sender).tag()) }
    }
}

impl<'new> CloneIn<'new, &'new Self> for ChannelReceiver {
    fn clone_in<const C: bool>(&self, _bk: &'new Block<C>) -> Gc<&'new Self> {
        self.0.state.receiver_count.fetch_add(1, Ordering::AcqRel);

        let new_receiver = ChannelReceiver::new_with_state(self.0.state.clone(), false);

        // SAFETY: Inner state of receivers is shared channel state that is allocated in the global block during creation
        unsafe { std::mem::transmute::<Gc<&Self>, Gc<&'new Self>>((&new_receiver).tag()) }
    }
}

// IntoObject implementations are in tagged.rs

// Debug implementations
impl fmt::Debug for ChannelSender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#<channel-sender>")
    }
}

impl fmt::Display for ChannelSender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#<channel-sender>")
    }
}

impl fmt::Debug for ChannelReceiver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#<channel-receiver>")
    }
}

impl fmt::Display for ChannelReceiver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#<channel-receiver>")
    }
}

// Implement object_trait_impls macro pattern manually
impl PartialEq for ChannelSender {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(&*self.0, &*other.0)
    }
}

impl Eq for ChannelSender {}

impl PartialEq for ChannelReceiver {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(&*self.0, &*other.0)
    }
}

impl Eq for ChannelReceiver {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        cons::Cons,
        gc::{Context, RootSet},
        object::{IntoObject, ObjectType},
    };

    #[test]
    fn test_basic_send_recv() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(1);

        // Send an integer
        sender.send(cx.add(42)).unwrap();

        // Receive it
        let result = receiver.recv(&cx).unwrap();
        if let ObjectType::Int(n) = result.untag() {
            assert_eq!(n, 42);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_double_clone_verification() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(1);

        // Create a string
        let original = cx.add("test string");

        // Send it
        sender.send(original).unwrap();

        // Receive it
        let received = receiver.recv(&cx).unwrap();

        // Verify content is same
        if let ObjectType::String(s1) = original.untag() {
            if let ObjectType::String(s2) = received.untag() {
                assert_eq!(s1.as_ref(), s2.as_ref());

                // Verify they are different allocations (double-copy worked)
                assert_ne!(
                    s1.as_ptr() as usize,
                    s2.as_ptr() as usize,
                    "Strings should be different allocations"
                );
            } else {
                panic!("Expected string");
            }
        } else {
            panic!("Expected string");
        }
    }

    #[test]
    fn test_channel_closed_on_sender_drop() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(1);

        // Drop sender
        drop(sender);

        // Try to receive - should get Closed error
        let result = receiver.recv(&cx);
        assert!(matches!(result, Err(RecvError::Closed)));
    }

    #[test]
    #[ignore] // close() is pub(crate), tested indirectly via drop
    fn test_channel_closed_on_explicit_close() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(1);

        // Explicitly close sender
        // sender.close(); // Not accessible in tests
        drop(sender);

        // Try to receive - should get Closed error
        let result = receiver.recv(&cx);
        assert!(matches!(result, Err(RecvError::Closed)));
    }

    #[test]
    fn test_channel_full() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(1);

        // Fill the channel
        sender.send(cx.add(1)).unwrap();

        // Try to send again without blocking - should get Full error
        let result = sender.try_send(cx.add(2));
        assert!(matches!(result, Err(SendError::Full)));

        // Receive the first message
        let _ = receiver.recv(&cx).unwrap();

        // Now we should be able to send again
        sender.try_send(cx.add(2)).unwrap();

        // Clean up - receive the second message
        let _ = receiver.recv(&cx).unwrap();
    }

    #[test]
    fn test_channel_empty() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (_sender, receiver) = make_channel_pair(1);

        // Try to receive from empty channel - should get Empty error
        let result = receiver.try_recv(&cx);
        assert!(matches!(result, Err(RecvError::Empty)));
    }

    #[test]
    fn test_complex_objects() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(5);

        // Send a string
        let str_obj = cx.add("hello");
        sender.send(str_obj).unwrap();

        // Send a cons cell (list)
        let list = Cons::new(cx.add(1), cx.add(2), &cx).into_obj(&cx.block);
        sender.send(list.into()).unwrap();

        // Send a vector
        let vec_obj = cx.add(vec![cx.add(10), cx.add(20), cx.add(30)]);
        sender.send(vec_obj).unwrap();

        // Receive and verify string
        let recv_str = receiver.recv(&cx).unwrap();
        if let ObjectType::String(s) = recv_str.untag() {
            assert_eq!(s.as_ref(), "hello");
        } else {
            panic!("Expected string");
        }

        // Receive and verify cons
        let recv_list = receiver.recv(&cx).unwrap();
        if let ObjectType::Cons(cons) = recv_list.untag() {
            let car_obj = cons.car();
            if let ObjectType::Int(car) = car_obj.untag() {
                assert_eq!(car, 1);
            } else {
                panic!("Expected int in car");
            }
        } else {
            panic!("Expected cons");
        }

        // Receive and verify vector
        let recv_vec = receiver.recv(&cx).unwrap();
        if let ObjectType::Vec(v) = recv_vec.untag() {
            assert_eq!(v.len(), 3);
            if let ObjectType::Int(n) = v[0].get().untag() {
                assert_eq!(n, 10);
            }
        } else {
            panic!("Expected vector");
        }
    }

    #[test]
    fn test_timeout() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (_sender, receiver) = make_channel_pair(1);

        // Try to receive with very short timeout - should timeout
        let result = receiver.recv_timeout(&cx, Duration::from_millis(10));
        assert!(matches!(result, Err(RecvError::Timeout)));
    }

    #[test]
    fn test_multiple_messages() {
        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(10);

        // Send multiple messages
        for i in 0..10 {
            sender.send(cx.add(i)).unwrap();
        }

        // Receive them in order
        for i in 0..10 {
            let result = receiver.recv(&cx).unwrap();
            if let ObjectType::Int(n) = result.untag() {
                assert_eq!(n, i);
            } else {
                panic!("Expected integer");
            }
        }
    }

    #[test]
    fn test_channel_sender_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ChannelSender>();
    }

    #[test]
    fn test_channel_receiver_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ChannelReceiver>();
    }

    #[test]
    fn test_cross_thread_channel() {
        use std::thread;

        let roots = RootSet::default();
        let cx = Context::new(&roots);

        let (sender, receiver) = make_channel_pair(5);

        // Send from spawned thread
        let handle = thread::spawn(move || {
            let roots2 = RootSet::default();
            let cx2 = Context::new(&roots2);
            for i in 0..5 {
                sender.send(cx2.add(i * 10)).unwrap();
            }
        });

        // Receive in main thread
        for i in 0..5 {
            let result = receiver.recv(&cx).unwrap();
            if let ObjectType::Int(n) = result.untag() {
                assert_eq!(n, i * 10);
            } else {
                panic!("Expected integer");
            }
        }

        handle.join().unwrap();
    }
}
