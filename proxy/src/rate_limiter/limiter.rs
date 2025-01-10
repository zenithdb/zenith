use std::borrow::Cow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use anyhow::bail;
use dashmap::DashMap;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time::{Duration, Instant};
use tracing::info;

use crate::ext::LockExt;
use crate::intern::EndpointIdInt;

pub struct GlobalRateLimiter {
    data: Vec<RateBucket>,
    info: Vec<RateBucketInfo>,
}

impl GlobalRateLimiter {
    pub fn new(info: Vec<RateBucketInfo>) -> Self {
        Self {
            data: vec![
                RateBucket {
                    start: Instant::now(),
                    count: 0,
                };
                info.len()
            ],
            info,
        }
    }

    /// Check that number of connections is below `max_rps` rps.
    pub fn check(&mut self) -> bool {
        let now = Instant::now();

        let should_allow_request = self
            .data
            .iter_mut()
            .zip(&self.info)
            .all(|(bucket, info)| bucket.should_allow_request(info, now, 1));

        if should_allow_request {
            // only increment the bucket counts if the request will actually be accepted
            self.data.iter_mut().for_each(|b| b.inc(1));
        }

        should_allow_request
    }
}

// Simple per-endpoint rate limiter.
//
// Check that number of connections to the endpoint is below `max_rps` rps.
// Purposefully ignore user name and database name as clients can reconnect
// with different names, so we'll end up sending some http requests to
// the control plane.
pub type WakeComputeRateLimiter = BucketRateLimiter<EndpointIdInt, StdRng, RandomState>;

pub struct BucketRateLimiter<Key, Rand = StdRng, Hasher = RandomState> {
    map: DashMap<Key, Vec<RateBucket>, Hasher>,
    info: Cow<'static, [RateBucketInfo]>,
    access_count: AtomicUsize,
    rand: Mutex<Rand>,
}

#[derive(Clone, Copy)]
struct RateBucket {
    start: Instant,
    count: u32,
}

impl RateBucket {
    fn should_allow_request(&mut self, info: &RateBucketInfo, now: Instant, n: u32) -> bool {
        if now - self.start < info.interval {
            self.count + n <= info.max_rpi
        } else {
            // bucket expired, reset
            self.count = 0;
            self.start = now;

            true
        }
    }

    fn inc(&mut self, n: u32) {
        self.count += n;
    }
}

#[derive(Clone, Copy, PartialEq)]
pub struct RateBucketInfo {
    pub(crate) interval: Duration,
    // requests per interval
    pub(crate) max_rpi: u32,
}

impl std::fmt::Display for RateBucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rps = self.rps().floor() as u64;
        write!(f, "{rps}@{}", humantime::format_duration(self.interval))
    }
}

impl std::fmt::Debug for RateBucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::str::FromStr for RateBucketInfo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((max_rps, interval)) = s.split_once('@') else {
            bail!("invalid rate info")
        };
        let max_rps = max_rps.parse()?;
        let interval = humantime::parse_duration(interval)?;
        Ok(Self::new(max_rps, interval))
    }
}

impl RateBucketInfo {
    pub const DEFAULT_SET: [Self; 3] = [
        Self::new(300, Duration::from_secs(1)),
        Self::new(200, Duration::from_secs(60)),
        Self::new(100, Duration::from_secs(600)),
    ];

    pub const DEFAULT_ENDPOINT_SET: [Self; 3] = [
        Self::new(500, Duration::from_secs(1)),
        Self::new(300, Duration::from_secs(60)),
        Self::new(200, Duration::from_secs(600)),
    ];

    /// All of these are per endpoint-maskedip pair.
    /// Context: 4096 rounds of pbkdf2 take about 1ms of cpu time to execute (1 milli-cpu-second or 1mcpus).
    ///
    /// First bucket: 1000mcpus total per endpoint-ip pair
    /// * 4096000 requests per second with 1 hash rounds.
    /// * 1000 requests per second with 4096 hash rounds.
    /// * 6.8 requests per second with 600000 hash rounds.
    pub const DEFAULT_AUTH_SET: [Self; 3] = [
        Self::new(1000 * 4096, Duration::from_secs(1)),
        Self::new(600 * 4096, Duration::from_secs(60)),
        Self::new(300 * 4096, Duration::from_secs(600)),
    ];

    pub fn rps(&self) -> f64 {
        (self.max_rpi as f64) / self.interval.as_secs_f64()
    }

    pub fn validate(info: &mut [Self]) -> anyhow::Result<()> {
        info.sort_unstable_by_key(|info| info.interval);
        let invalid = info
            .iter()
            .tuple_windows()
            .find(|(a, b)| a.max_rpi > b.max_rpi);
        if let Some((a, b)) = invalid {
            bail!(
                "invalid bucket RPS limits. {b} allows fewer requests per bucket than {a} ({} vs {})",
                b.max_rpi,
                a.max_rpi,
            );
        }

        Ok(())
    }

    pub const fn new(max_rps: u32, interval: Duration) -> Self {
        Self {
            interval,
            max_rpi: ((max_rps as u64) * (interval.as_millis() as u64) / 1000) as u32,
        }
    }
}

impl<K: Hash + Eq> BucketRateLimiter<K> {
    pub fn new(info: impl Into<Cow<'static, [RateBucketInfo]>>) -> Self {
        Self::new_with_rand_and_hasher(info, StdRng::from_entropy(), RandomState::new())
    }
}

impl<K: Hash + Eq, R: Rng, S: BuildHasher + Clone> BucketRateLimiter<K, R, S> {
    fn new_with_rand_and_hasher(
        info: impl Into<Cow<'static, [RateBucketInfo]>>,
        rand: R,
        hasher: S,
    ) -> Self {
        let info = info.into();
        info!(buckets = ?info, "endpoint rate limiter");
        Self {
            info,
            map: DashMap::with_hasher_and_shard_amount(hasher, 64),
            access_count: AtomicUsize::new(1), // start from 1 to avoid GC on the first request
            rand: Mutex::new(rand),
        }
    }

    /// Check that number of connections to the endpoint is below `max_rps` rps.
    pub(crate) fn check(&self, key: K, n: u32) -> bool {
        // do a partial GC every 2k requests. This cleans up ~ 1/64th of the map.
        // worst case memory usage is about:
        //    = 2 * 2048 * 64 * (48B + 72B)
        //    = 30MB
        if self.access_count.fetch_add(1, Ordering::AcqRel) % 2048 == 0 {
            self.do_gc();
        }

        let now = Instant::now();
        let mut entry = self.map.entry(key).or_insert_with(|| {
            vec![
                RateBucket {
                    start: now,
                    count: 0,
                };
                self.info.len()
            ]
        });

        let should_allow_request = entry
            .iter_mut()
            .zip(&*self.info)
            .all(|(bucket, info)| bucket.should_allow_request(info, now, n));

        if should_allow_request {
            // only increment the bucket counts if the request will actually be accepted
            entry.iter_mut().for_each(|b| b.inc(n));
        }

        should_allow_request
    }

    /// Clean the map. Simple strategy: remove all entries in a random shard.
    /// At worst, we'll double the effective max_rps during the cleanup.
    /// But that way deletion does not aquire mutex on each entry access.
    pub(crate) fn do_gc(&self) {
        info!(
            "cleaning up bucket rate limiter, current size = {}",
            self.map.len()
        );
        let n = self.map.shards().len();
        // this lock is ok as the periodic cycle of do_gc makes this very unlikely to collide
        // (impossible, infact, unless we have 2048 threads)
        let shard = self.rand.lock_propagate_poison().gen_range(0..n);
        self.map.shards()[shard].write().clear();
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::hash::BuildHasherDefault;
    use std::time::Duration;

    use rand::SeedableRng;
    use rustc_hash::FxHasher;
    use tokio::time;

    use super::{BucketRateLimiter, WakeComputeRateLimiter};
    use crate::intern::EndpointIdInt;
    use crate::rate_limiter::RateBucketInfo;
    use crate::types::EndpointId;

    #[test]
    fn rate_bucket_rpi() {
        let rate_bucket = RateBucketInfo::new(50, Duration::from_secs(5));
        assert_eq!(rate_bucket.max_rpi, 50 * 5);

        let rate_bucket = RateBucketInfo::new(50, Duration::from_millis(500));
        assert_eq!(rate_bucket.max_rpi, 50 / 2);
    }

    #[test]
    fn rate_bucket_parse() {
        let rate_bucket: RateBucketInfo = "100@10s".parse().unwrap();
        assert_eq!(rate_bucket.interval, Duration::from_secs(10));
        assert_eq!(rate_bucket.max_rpi, 100 * 10);
        assert_eq!(rate_bucket.to_string(), "100@10s");

        let rate_bucket: RateBucketInfo = "100@1m".parse().unwrap();
        assert_eq!(rate_bucket.interval, Duration::from_secs(60));
        assert_eq!(rate_bucket.max_rpi, 100 * 60);
        assert_eq!(rate_bucket.to_string(), "100@1m");
    }

    #[test]
    fn default_rate_buckets() {
        let mut defaults = RateBucketInfo::DEFAULT_SET;
        RateBucketInfo::validate(&mut defaults[..]).unwrap();
    }

    #[test]
    #[should_panic = "invalid bucket RPS limits. 10@10s allows fewer requests per bucket than 300@1s (100 vs 300)"]
    fn rate_buckets_validate() {
        let mut rates: Vec<RateBucketInfo> = ["300@1s", "10@10s"]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect();
        RateBucketInfo::validate(&mut rates).unwrap();
    }

    #[tokio::test]
    async fn test_rate_limits() {
        let mut rates: Vec<RateBucketInfo> = ["100@1s", "20@30s"]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect();
        RateBucketInfo::validate(&mut rates).unwrap();
        let limiter = WakeComputeRateLimiter::new(rates);

        let endpoint = EndpointId::from("ep-my-endpoint-1234");
        let endpoint = EndpointIdInt::from(endpoint);

        time::pause();

        for _ in 0..100 {
            assert!(limiter.check(endpoint, 1));
        }
        // more connections fail
        assert!(!limiter.check(endpoint, 1));

        // fail even after 500ms as it's in the same bucket
        time::advance(time::Duration::from_millis(500)).await;
        assert!(!limiter.check(endpoint, 1));

        // after a full 1s, 100 requests are allowed again
        time::advance(time::Duration::from_millis(500)).await;
        for _ in 1..6 {
            for _ in 0..50 {
                assert!(limiter.check(endpoint, 2));
            }
            time::advance(time::Duration::from_millis(1000)).await;
        }

        // more connections after 600 will exceed the 20rps@30s limit
        assert!(!limiter.check(endpoint, 1));

        // will still fail before the 30 second limit
        time::advance(time::Duration::from_millis(30_000 - 6_000 - 1)).await;
        assert!(!limiter.check(endpoint, 1));

        // after the full 30 seconds, 100 requests are allowed again
        time::advance(time::Duration::from_millis(1)).await;
        for _ in 0..100 {
            assert!(limiter.check(endpoint, 1));
        }
    }

    #[tokio::test]
    async fn test_rate_limits_gc() {
        // fixed seeded random/hasher to ensure that the test is not flaky
        let rand = rand::rngs::StdRng::from_seed([1; 32]);
        let hasher = BuildHasherDefault::<FxHasher>::default();

        let limiter =
            BucketRateLimiter::new_with_rand_and_hasher(&RateBucketInfo::DEFAULT_SET, rand, hasher);
        for i in 0..1_000_000 {
            limiter.check(i, 1);
        }
        assert!(limiter.map.len() < 150_000);
    }
}
