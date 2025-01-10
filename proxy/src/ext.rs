use std::panic::resume_unwind;
use std::sync::{Mutex, MutexGuard};

use tokio::task::JoinError;

pub(crate) trait LockExt<T> {
    fn lock_propagate_poison(&self) -> MutexGuard<'_, T>;
}

impl<T> LockExt<T> for Mutex<T> {
    /// Lock the mutex and panic if the mutex was poisoned.
    #[track_caller]
    fn lock_propagate_poison(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(guard) => guard,
            // poison occurs when another thread panicked while holding the lock guard.
            // since panicking is often unrecoverable, propagating the poison panic is reasonable.
            Err(poison) => panic!("{poison}"),
        }
    }
}

pub(crate) trait TaskExt<T> {
    fn propagate_task_panic(self) -> T;
}

impl<T> TaskExt<T> for Result<T, JoinError> {
    /// Unwrap the result and panic if the inner task panicked.
    /// Also panics if the task was cancelled
    #[track_caller]
    fn propagate_task_panic(self) -> T {
        match self {
            Ok(t) => t,
            // Using resume_unwind prevents the panic hook being called twice.
            // Since we use this for structured concurrency, there is only
            // 1 logical panic, so this is more correct.
            Err(e) if e.is_panic() => resume_unwind(e.into_panic()),
            Err(e) => panic!("unexpected task error: {e}"),
        }
    }
}
