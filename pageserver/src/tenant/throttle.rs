use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use arc_swap::ArcSwap;
use utils::leaky_bucket::{LeakyBucketConfig, RateLimiter};

/// Throttle for `async` functions.
///
/// Runtime reconfigurable.
///
/// To share a throttle among multiple entities, wrap it in an [`Arc`].
///
/// The intial use case for this is tenant-wide throttling of getpage@lsn requests.
pub struct Throttle {
    inner: ArcSwap<Inner>,
    /// will be turned into [`Stats::count_accounted_start`]
    count_accounted_start: AtomicU64,
    /// will be turned into [`Stats::count_accounted_finish`]
    count_accounted_finish: AtomicU64,
    /// will be turned into [`Stats::count_throttled`]
    count_throttled: AtomicU64,
    /// will be turned into [`Stats::sum_throttled_usecs`]
    sum_throttled_usecs: AtomicU64,
}

pub struct Inner {
    enabled: bool,
    rate_limiter: Arc<RateLimiter>,
}

pub type Config = pageserver_api::models::ThrottleConfig;

/// See [`Throttle::reset_stats`].
pub struct Stats {
    /// Number of requests that started [`Throttle::throttle`] calls.
    pub count_accounted_start: u64,
    /// Number of requests that finished [`Throttle::throttle`] calls.
    pub count_accounted_finish: u64,
    /// Subset of the `accounted` requests that were actually throttled.
    /// Note that the numbers are stored as two independent atomics, so, there might be a slight drift.
    pub count_throttled: u64,
    /// Sum of microseconds that throttled requests spent waiting for throttling.
    pub sum_throttled_usecs: u64,
}

pub enum ThrottleResult {
    NotThrottled { end: Instant },
    Throttled { end: Instant },
}

impl Throttle {
    pub fn new(config: Config) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(Self::new_inner(config))),
            count_accounted_start: AtomicU64::new(0),
            count_accounted_finish: AtomicU64::new(0),
            count_throttled: AtomicU64::new(0),
            sum_throttled_usecs: AtomicU64::new(0),
        }
    }
    fn new_inner(config: Config) -> Inner {
        let Config {
            enabled,
            initial,
            refill_interval,
            refill_amount,
            max,
        } = config;

        // steady rate, we expect `refill_amount` requests per `refill_interval`.
        // dividing gives us the rps.
        let rps = f64::from(refill_amount.get()) / refill_interval.as_secs_f64();
        let config = LeakyBucketConfig::new(rps, f64::from(max));

        // initial tracks how many tokens are available to put in the bucket
        // we want how many tokens are currently in the bucket
        let initial_tokens = max - initial;

        let rate_limiter = RateLimiter::with_initial_tokens(config, f64::from(initial_tokens));

        Inner {
            enabled: enabled.is_enabled(),
            rate_limiter: Arc::new(rate_limiter),
        }
    }
    pub fn reconfigure(&self, config: Config) {
        self.inner.store(Arc::new(Self::new_inner(config)));
    }

    /// The [`Throttle`] keeps an internal flag that is true if there was ever any actual throttling.
    /// This method allows retrieving & resetting that flag.
    /// Useful for periodic reporting.
    pub fn reset_stats(&self) -> Stats {
        let count_accounted_start = self.count_accounted_start.swap(0, Ordering::Relaxed);
        let count_accounted_finish = self.count_accounted_finish.swap(0, Ordering::Relaxed);
        let count_throttled = self.count_throttled.swap(0, Ordering::Relaxed);
        let sum_throttled_usecs = self.sum_throttled_usecs.swap(0, Ordering::Relaxed);
        Stats {
            count_accounted_start,
            count_accounted_finish,
            count_throttled,
            sum_throttled_usecs,
        }
    }

    /// See [`Config::steady_rps`].
    pub fn steady_rps(&self) -> f64 {
        self.inner.load().rate_limiter.steady_rps()
    }

    /// `start` must be [`Instant::now`] or earlier.
    pub async fn throttle(&self, key_count: usize, start: Instant) -> ThrottleResult {
        let inner = self.inner.load_full(); // clones the `Inner` Arc

        if !inner.enabled {
            return ThrottleResult::NotThrottled { end: start };
        }

        self.count_accounted_start.fetch_add(1, Ordering::Relaxed);
        let did_throttle = inner.rate_limiter.acquire(key_count).await;
        self.count_accounted_finish.fetch_add(1, Ordering::Relaxed);

        if did_throttle {
            self.count_throttled.fetch_add(1, Ordering::Relaxed);
            let end = Instant::now();
            let wait_time = end - start;
            self.sum_throttled_usecs
                .fetch_add(wait_time.as_micros() as u64, Ordering::Relaxed);
            ThrottleResult::Throttled { end }
        } else {
            ThrottleResult::NotThrottled { end: start }
        }
    }
}
