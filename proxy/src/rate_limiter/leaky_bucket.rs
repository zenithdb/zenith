use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::RandomState;
use dashmap::DashMap;
use rand::{thread_rng, Rng};
use tokio::time::Instant;
use tracing::info;
use utils::leaky_bucket::LeakyBucketState;

use crate::intern::EndpointIdInt;

// Simple per-endpoint rate limiter.
pub type EndpointRateLimiter = LeakyBucketRateLimiter<EndpointIdInt>;

pub struct LeakyBucketRateLimiter<Key> {
    map: DashMap<Key, LeakyBucketState, RandomState>,
    config: utils::leaky_bucket::LeakyBucketConfig,
    access_count: AtomicUsize,
}

impl<K: Hash + Eq> LeakyBucketRateLimiter<K> {
    pub const DEFAULT: LeakyBucketConfig = LeakyBucketConfig {
        rps: 600.0,
        max: 1500.0,
    };

    pub fn new_with_shards(config: LeakyBucketConfig, shards: usize) -> Self {
        Self {
            map: DashMap::with_hasher_and_shard_amount(RandomState::new(), shards),
            config: config.into(),
            access_count: AtomicUsize::new(0),
        }
    }

    /// Check that number of connections to the endpoint is below `max_rps` rps.
    pub(crate) fn check(&self, key: K, n: u32) -> bool {
        let now = Instant::now();

        if self.access_count.fetch_add(1, Ordering::AcqRel) % 2048 == 0 {
            self.do_gc(now);
        }

        let mut entry = self
            .map
            .entry(key)
            .or_insert_with(|| LeakyBucketState { empty_at: now });

        entry.add_tokens(&self.config, now, n as f64).is_ok()
    }

    fn do_gc(&self, now: Instant) {
        info!(
            "cleaning up bucket rate limiter, current size = {}",
            self.map.len()
        );
        let n = self.map.shards().len();
        let shard = thread_rng().gen_range(0..n);
        self.map.shards()[shard]
            .write()
            .retain(|_, value| !value.get().bucket_is_empty(now));
    }
}

pub struct LeakyBucketConfig {
    pub rps: f64,
    pub max: f64,
}

#[cfg(test)]
impl LeakyBucketConfig {
    pub(crate) fn new(rps: f64, max: f64) -> Self {
        assert!(rps > 0.0, "rps must be positive");
        assert!(max > 0.0, "max must be positive");
        Self { rps, max }
    }
}

impl From<LeakyBucketConfig> for utils::leaky_bucket::LeakyBucketConfig {
    fn from(config: LeakyBucketConfig) -> Self {
        utils::leaky_bucket::LeakyBucketConfig::new(config.rps, config.max)
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;
    use utils::leaky_bucket::LeakyBucketState;

    use super::LeakyBucketConfig;

    #[tokio::test(start_paused = true)]
    async fn check() {
        let config: utils::leaky_bucket::LeakyBucketConfig =
            LeakyBucketConfig::new(500.0, 2000.0).into();
        assert_eq!(config.cost, Duration::from_millis(2));
        assert_eq!(config.bucket_width, Duration::from_secs(4));

        let mut bucket = LeakyBucketState {
            empty_at: Instant::now(),
        };

        // should work for 2000 requests this second
        for _ in 0..2000 {
            bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
        }
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
        assert_eq!(bucket.empty_at - Instant::now(), config.bucket_width);

        // in 1ms we should drain 0.5 tokens.
        // make sure we don't lose any tokens
        tokio::time::advance(Duration::from_millis(1)).await;
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
        tokio::time::advance(Duration::from_millis(1)).await;
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();

        // in 10ms we should drain 5 tokens
        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..5 {
            bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
        }
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();

        // in 10s we should drain 5000 tokens
        // but cap is only 2000
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..2000 {
            bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
        }
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();

        // should sustain 500rps
        for _ in 0..2000 {
            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..5 {
                bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
        }
    }
}
