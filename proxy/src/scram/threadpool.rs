//! Custom threadpool implementation for password hashing.
//!
//! Requirements:
//! 1. Fairness per endpoint.
//! 2. Yield support for high iteration counts.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use futures::FutureExt;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::pbkdf2::Pbkdf2;
use crate::intern::EndpointIdInt;
use crate::metrics::{ThreadPoolMetrics, ThreadPoolWorkerId};
use crate::scram::countmin::CountMinSketch;

pub struct ThreadPool {
    runtime: Option<tokio::runtime::Runtime>,
    pub metrics: Arc<ThreadPoolMetrics>,
}

/// How often to reset the sketch values
const SKETCH_RESET_INTERVAL: u64 = 1021;

thread_local! {
    static STATE: RefCell<Option<ThreadRt>> = const { RefCell::new(None) };
}

impl ThreadPool {
    pub fn new(mut n_workers: u8) -> Arc<Self> {
        // rayon would be nice here, but yielding in rayon does not work well afaict.

        if n_workers == 0 {
            n_workers = 1;
        }

        Arc::new_cyclic(|pool| {
            let pool = pool.clone();
            let worker_id = AtomicUsize::new(0);

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(n_workers as usize)
                .on_thread_start(move || {
                    STATE.with_borrow_mut(|state| {
                        *state = Some(ThreadRt {
                            pool: pool.clone(),
                            id: ThreadPoolWorkerId(worker_id.fetch_add(1, Ordering::Relaxed)),
                            rng: SmallRng::from_entropy(),
                            // used to determine whether we should temporarily skip tasks for fairness.
                            // 99% of estimates will overcount by no more than 4096 samples
                            countmin: CountMinSketch::with_params(
                                1.0 / (SKETCH_RESET_INTERVAL as f64),
                                0.01,
                            ),
                            tick: 0,
                        });
                    });
                })
                .build()
                .expect("password threadpool runtime should be configured correctly");

            Self {
                runtime: Some(runtime),
                metrics: Arc::new(ThreadPoolMetrics::new(n_workers as usize)),
            }
        })
    }

    pub(crate) fn spawn_job(&self, endpoint: EndpointIdInt, pbkdf2: Pbkdf2) -> JobHandle {
        JobHandle(
            self.runtime
                .as_ref()
                .expect("runtime is always set")
                .spawn(JobSpec { pbkdf2, endpoint }),
        )
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.runtime
            .take()
            .expect("runtime is always set")
            .shutdown_background();
    }
}

struct ThreadRt {
    pool: Weak<ThreadPool>,
    id: ThreadPoolWorkerId,
    rng: SmallRng,
    countmin: CountMinSketch,
    tick: u64,
}

impl ThreadRt {
    fn should_run(&mut self, job: &JobSpec) -> bool {
        let rate = self
            .countmin
            .inc_and_return(&job.endpoint, job.pbkdf2.cost());

        const P: f64 = 2000.0;
        // probability decreases as rate increases.
        // lower probability, higher chance of being skipped
        //
        // estimates (rate in terms of 4096 rounds):
        // rate = 0    => probability = 100%
        // rate = 10   => probability = 71.3%
        // rate = 50   => probability = 62.1%
        // rate = 500  => probability = 52.3%
        // rate = 1021 => probability = 49.8%
        //
        // My expectation is that the pool queue will only begin backing up at ~1000rps
        // in which case the SKETCH_RESET_INTERVAL represents 1 second. Thus, the rates above
        // are in requests per second.
        let probability = P.ln() / (P + rate as f64).ln();
        self.rng.gen_bool(probability)
    }
}

struct JobSpec {
    pbkdf2: Pbkdf2,
    endpoint: EndpointIdInt,
}

impl Future for JobSpec {
    type Output = [u8; 32];

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        STATE.with_borrow_mut(|state| {
            let state = state.as_mut().expect("should be set on thread startup");

            state.tick = state.tick.wrapping_add(1);
            if state.tick % SKETCH_RESET_INTERVAL == 0 {
                state.countmin.reset();
            }

            if state.should_run(&self) {
                if let Some(pool) = state.pool.upgrade() {
                    pool.metrics.worker_task_turns_total.inc(state.id);
                }

                match self.pbkdf2.turn() {
                    Poll::Ready(result) => Poll::Ready(result),
                    // more to do, we shall requeue
                    Poll::Pending => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            } else {
                if let Some(pool) = state.pool.upgrade() {
                    pool.metrics.worker_task_skips_total.inc(state.id);
                }

                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
    }
}

pub(crate) struct JobHandle(tokio::task::JoinHandle<[u8; 32]>);

impl Future for JobHandle {
    type Output = [u8; 32];

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.poll_unpin(cx) {
            Poll::Ready(Ok(ok)) => Poll::Ready(ok),
            Poll::Ready(Err(err)) => std::panic::resume_unwind(err.into_panic()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for JobHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EndpointId;

    #[tokio::test]
    async fn hash_is_correct() {
        let pool = ThreadPool::new(1);

        let ep = EndpointId::from("foo");
        let ep = EndpointIdInt::from(ep);

        let salt = [0x55; 32];
        let actual = pool
            .spawn_job(ep, Pbkdf2::start(b"password", &salt, 4096))
            .await;

        let expected = [
            10, 114, 73, 188, 140, 222, 196, 156, 214, 184, 79, 157, 119, 242, 16, 31, 53, 242,
            178, 43, 95, 8, 225, 182, 122, 40, 219, 21, 89, 147, 64, 140,
        ];
        assert_eq!(actual, expected);
    }
}
