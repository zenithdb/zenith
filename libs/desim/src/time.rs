use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    ops::DerefMut,
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc,
    },
};

use parking_lot::Mutex;
use tracing::trace;

use crate::executor::ThreadContext;

/// Holds current time and all pending wakeup events.
pub struct Timing {
    /// Current world's time.
    current_time: AtomicU64,
    /// Pending timers.
    queue: Mutex<BinaryHeap<Pending>>,
    /// Global nonce. Makes picking events from binary heap queue deterministic
    /// by appending a number to events with the same timestamp.
    nonce: AtomicU32,
    /// Used to schedule fake events.
    fake_context: Arc<ThreadContext>,
}

impl Default for Timing {
    fn default() -> Self {
        Self::new()
    }
}

impl Timing {
    /// Create a new empty clock with time set to 0.
    pub fn new() -> Timing {
        Timing {
            current_time: AtomicU64::new(0),
            queue: Mutex::new(BinaryHeap::new()),
            nonce: AtomicU32::new(0),
            fake_context: Arc::new(ThreadContext::new()),
        }
    }

    /// Return the current world's time.
    pub fn now(&self) -> u64 {
        self.current_time.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Tick-tock the global clock. Return the event ready to be processed
    /// or move the clock forward and then return the event.
    pub(crate) fn step(&self) -> Option<Arc<ThreadContext>> {
        let mut queue = self.queue.lock();

        if queue.is_empty() {
            // no future events
            return None;
        }

        if !self.is_event_ready(queue.deref_mut()) {
            let next_time = queue.peek().unwrap().time;
            self.current_time
                .store(next_time, std::sync::atomic::Ordering::SeqCst);
            trace!("rewind time to {}", next_time);
            assert!(self.is_event_ready(queue.deref_mut()));
        }

        Some(queue.pop().unwrap().wake_context)
    }

    /// Append an event to the queue, to wakeup the thread in `ms` milliseconds.
    pub(crate) fn schedule_wakeup(&self, ms: u64, wake_context: Arc<ThreadContext>) {
        self.nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let nonce = self.nonce.load(std::sync::atomic::Ordering::SeqCst);
        self.queue.lock().push(Pending {
            time: self.now() + ms,
            nonce,
            wake_context,
        })
    }

    /// Append a fake event to the queue, to prevent clocks from skipping this time.
    pub fn schedule_fake(&self, ms: u64) {
        self.queue.lock().push(Pending {
            time: self.now() + ms,
            nonce: 0,
            wake_context: self.fake_context.clone(),
        });
    }

    /// Return true if there is a ready event.
    fn is_event_ready(&self, queue: &mut BinaryHeap<Pending>) -> bool {
        queue.peek().is_some_and(|x| x.time <= self.now())
    }

    /// Clear all pending events.
    pub(crate) fn clear(&self) {
        self.queue.lock().clear();
    }
}

struct Pending {
    time: u64,
    nonce: u32,
    wake_context: Arc<ThreadContext>,
}

// BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
// to get that.
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.time, other.nonce).cmp(&(self.time, self.nonce))
    }
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        (other.time, other.nonce) == (self.time, self.nonce)
    }
}

impl Eq for Pending {}
