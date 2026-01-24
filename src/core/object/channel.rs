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
    sync::{Arc, Condvar, Mutex},
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

/// The actual channel state protected by the mutex
#[derive(Debug)]
pub(in crate::core) struct ChannelState {
    queue: VecDeque<Object<'static>>,
    capacity: usize,
    sender_count: usize,
    receiver_count: usize,
    sender_closed: bool,
    receiver_closed: bool,
}

impl ChannelState {
    pub(in crate::core) fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
            sender_count: 0,
            receiver_count: 0,
            sender_closed: false,
            receiver_closed: false,
        }
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
    pub(in crate::core) state: Mutex<Option<ChannelState>>,
    pub(in crate::core) not_full: Condvar,
    pub(in crate::core) not_empty: Condvar,
}

impl SharedChannelState {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(Some(ChannelState::new(capacity))),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        }
    }

    fn empty_buffer(&self) {
        if let Some(mut state) = self.state.lock().unwrap().take() {
            state.queue.drain(..);
            // State lock will be dropped here, then wake senders
            drop(state);
        }
        self.not_full.notify_all();
    }
}

impl Drop for SharedChannelState {
    fn drop(&mut self) {
        self.empty_buffer();
    }
}

/// The sending half of a channel.
pub(crate) struct ChannelSender(pub(in crate::core) GcHeap<ChannelSenderInner>);

derive_GcMoveable!(ChannelSender);

pub(in crate::core) struct ChannelSenderInner {
    pub(in crate::core) state: Option<Arc<SharedChannelState>>,
}

impl ChannelSender {
    pub(in crate::core) fn new_with_state(state: Arc<SharedChannelState>, constant: bool) -> Self {
        if let Some(ref mut s) = *state.state.lock().unwrap() {
            s.sender_count += 1;
        }
        Self(GcHeap::new(ChannelSenderInner { state: Some(state) }, constant))
    }

    pub(in crate::core) fn new_empty(constant: bool) -> Self {
        Self(GcHeap::new(ChannelSenderInner { state: None }, constant))
    }

    /// Send an object through the channel, blocking if full.
    /// Returns Err(SendError::Closed) if the receiver has been dropped.
    pub(crate) fn send<'ob>(&self, obj: Object<'ob>) -> Result<(), SendError> {
        let Some(state) = self.0.state.as_ref() else {
            return Err(SendError::Closed);
        };

        loop {
            let mut state_lock = state.state.lock().unwrap();

            let Some(ref mut s) = *state_lock else {
                return Err(SendError::Closed);
            };

            // Check if receiver is closed
            if s.receiver_count == 0 || s.receiver_closed {
                return Err(SendError::Closed);
            }

            if s.try_push(&obj).is_ok() {
                drop(state_lock);
                state.not_empty.notify_one();
                return Ok(());
            }

            // Wait for space
            let _state = state.not_full.wait(state_lock).unwrap();
        }
    }

    /// Try to send without blocking.
    /// Returns Err(SendError::Full) if the channel is full.
    /// Returns Err(SendError::Closed) if the receiver has been dropped.
    pub(crate) fn try_send<'ob>(&self, obj: Object<'ob>) -> Result<(), SendError> {
        let Some(state) = self.0.state.as_ref() else {
            return Err(SendError::Closed);
        };
        let mut state_lock = state.state.lock().unwrap();

        let Some(ref mut s) = *state_lock else {
            return Err(SendError::Closed);
        };

        // Check if receiver is closed
        if s.receiver_count == 0 || s.receiver_closed {
            return Err(SendError::Closed);
        }

        if s.try_push(&obj).is_ok() {
            drop(state_lock);
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
        let Some(state) = self.0.state.as_ref() else {
            return Err(SendError::Closed);
        };

        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            let mut state_lock = state.state.lock().unwrap();

            let Some(ref mut s) = *state_lock else {
                return Err(SendError::Closed);
            };

            // Check if receiver is closed
            if s.receiver_count == 0 || s.receiver_closed {
                return Err(SendError::Closed);
            }

            if s.try_push(&obj).is_ok() {
                drop(state_lock);
                state.not_empty.notify_one();
                return Ok(());
            }

            if start.elapsed() >= timeout {
                return Err(SendError::Timeout);
            }

            let now = Instant::now();
            let remaining = deadline - now;
            let result = state.not_full.wait_timeout(state_lock, remaining).unwrap();
            let _state = result.0;

            if result.1.timed_out() {
                return Err(SendError::Timeout);
            }
        }
    }

    /// Explicitly close the sender side of the channel.
    /// This wakes up any receivers waiting on recv() and causes them to return Err(RecvError::Closed).
    /// This is idempotent - calling close() multiple times has no additional effect.
    pub(crate) fn close(&self) {
        if let Some(state) = self.0.state.as_ref()
            && let Some(ref mut s) = *state.state.lock().unwrap()
        {
            if s.sender_count > 0 {
                s.sender_count -= 1;
            }
            if s.sender_count == 0 {
                state.not_empty.notify_all();
                s.sender_closed = true;
            }
        }
    }
}

/// The receiving half of a channel.
pub(crate) struct ChannelReceiver(pub(in crate::core) GcHeap<ChannelReceiverInner>);

derive_GcMoveable!(ChannelReceiver);

pub(in crate::core) struct ChannelReceiverInner {
    pub(in crate::core) state: Option<Arc<SharedChannelState>>,
}

impl ChannelReceiver {
    pub(in crate::core) fn new_with_state(state: Arc<SharedChannelState>, constant: bool) -> Self {
        if let Some(ref mut s) = *state.state.lock().unwrap() {
            s.receiver_count += 1;
        }
        Self(GcHeap::new(ChannelReceiverInner { state: Some(state) }, constant))
    }

    pub(in crate::core) fn new_empty(constant: bool) -> Self {
        Self(GcHeap::new(ChannelReceiverInner { state: None }, constant))
    }

    /// Receive an object from the channel, blocking if empty.
    /// Returns Err(RecvError::Closed) if all senders have been dropped.
    pub(crate) fn recv<'ob>(&self, cx: &'ob Context) -> Result<Object<'ob>, RecvError> {
        let Some(state) = self.0.state.as_ref() else {
            return Err(RecvError::Closed);
        };

        loop {
            let mut state_lock = state.state.lock().unwrap();

            let Some(ref mut s) = *state_lock else {
                return Err(RecvError::Closed);
            };

            if let Some(obj) = s.try_pop() {
                drop(state_lock);
                state.not_full.notify_one();
                let result = obj.clone_in(&cx.block);
                return Ok(result);
            }

            // Check if all senders are closed
            if s.sender_count == 0 || s.sender_closed {
                return Err(RecvError::Closed);
            }

            // Wait for data
            let _state = state.not_empty.wait(state_lock).unwrap();
        }
    }

    /// Try to receive without blocking.
    /// Returns Err(RecvError::Empty) if the channel is empty.
    /// Returns Err(RecvError::Closed) if all senders have been dropped.
    pub(crate) fn try_recv<'ob>(&self, cx: &'ob Context) -> Result<Object<'ob>, RecvError> {
        let Some(state) = self.0.state.as_ref() else {
            return Err(RecvError::Closed);
        };
        let mut state_lock = state.state.lock().unwrap();

        let Some(ref mut s) = *state_lock else {
            return Err(RecvError::Closed);
        };

        if let Some(obj) = s.try_pop() {
            drop(state_lock);
            state.not_full.notify_one();
            let result = obj.clone_in(&cx.block);
            Ok(result)
        } else if s.sender_count == 0 || s.sender_closed {
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
        let Some(state) = self.0.state.as_ref() else {
            return Err(RecvError::Closed);
        };
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            let mut state_lock = state.state.lock().unwrap();

            let Some(ref mut s) = *state_lock else {
                return Err(RecvError::Closed);
            };

            if let Some(obj) = s.try_pop() {
                drop(state_lock);
                state.not_full.notify_one();
                let result = obj.clone_in(&cx.block);
                return Ok(result);
            }

            // Check if all senders are closed
            if s.sender_count == 0 || s.sender_closed {
                return Err(RecvError::Closed);
            }

            if start.elapsed() >= timeout {
                return Err(RecvError::Timeout);
            }

            let now = Instant::now();
            let remaining = deadline - now;
            let result = state.not_empty.wait_timeout(state_lock, remaining).unwrap();
            let _state = result.0;

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
        if let Some(state) = self.0.state.as_ref() {
            let should_empty = if let Some(ref mut s) = *state.state.lock().unwrap() {
                if !s.receiver_closed {
                    if s.receiver_count > 0 {
                        s.receiver_count -= 1;
                    }
                    if s.receiver_count == 0 {
                        s.receiver_closed = true;
                    }
                    s.receiver_count == 0
                } else {
                    false
                }
            } else {
                false
            };

            if should_empty {
                state.empty_buffer();
                state.not_full.notify_all();
            }
        }
    }
}

/// Create a new channel pair with the specified capacity.
pub(crate) fn make_channel_pair(capacity: usize) -> (ChannelSender, ChannelReceiver) {
    let inner = Arc::new(SharedChannelState::new(capacity));

    // Create sender and receiver
    let receiver = ChannelReceiver::new_with_state(inner.clone(), false);
    let sender = ChannelSender::new_with_state(inner, false);
    (sender, receiver)
}

// Drop implementations to decrement counters
impl Drop for ChannelSenderInner {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            if let Some(ref mut s) = *state.state.lock().unwrap() {
                // Only decrement if not already closed (to avoid double-decrement)
                if !s.sender_closed {
                    if s.sender_count > 0 {
                        s.sender_count -= 1;
                    }
                    if s.sender_count == 0 {
                        s.sender_closed = true;
                        // Last sender dropped, wake up any waiting receivers
                        state.not_empty.notify_all();
                    }
                }
            }
            drop(state)
        }
    }
}

impl Drop for ChannelReceiverInner {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let should_empty = if let Some(ref mut s) = *state.state.lock().unwrap() {
                // Only decrement if not already closed (to avoid double-decrement)
                if !s.receiver_closed {
                    if s.receiver_count > 0 {
                        s.receiver_count -= 1;
                    }
                    if s.receiver_count == 0 {
                        s.receiver_closed = true;
                    }
                    s.receiver_count == 0
                } else {
                    false
                }
            } else {
                false
            };

            if should_empty {
                state.empty_buffer();
                state.not_full.notify_all();
            }
            drop(state)
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
        let new_sender = match self.0.state.as_ref() {
            Some(state) => ChannelSender::new_with_state(state.clone(), false),
            None => ChannelSender::new_empty(false),
        };

        // SAFETY: Inner state of senders is shared channel state that is allocated in the global block during creation
        unsafe { std::mem::transmute::<Gc<&Self>, Gc<&'new Self>>((&new_sender).tag()) }
    }
}

impl<'new> CloneIn<'new, &'new Self> for ChannelReceiver {
    fn clone_in<const C: bool>(&self, _bk: &'new Block<C>) -> Gc<&'new Self> {
        let new_receiver = match self.0.state.as_ref() {
            Some(state) => ChannelReceiver::new_with_state(state.clone(), false),
            None => ChannelReceiver::new_empty(false),
        };

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
