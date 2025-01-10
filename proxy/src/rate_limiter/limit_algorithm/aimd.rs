use super::{LimitAlgorithm, Outcome, Sample};

/// Loss-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease.
///
/// Adds available currency when:
/// 1. no load-based errors are observed, and
/// 2. the utilisation of the current limit is high.
///
/// Reduces available concurrency by a factor when load-based errors are detected.
#[derive(Clone, Copy, Debug, serde::Deserialize, PartialEq)]
pub(crate) struct Aimd {
    /// Minimum limit for AIMD algorithm.
    pub(crate) min: usize,
    /// Maximum limit for AIMD algorithm.
    pub(crate) max: usize,
    /// Decrease AIMD decrease by value in case of error.
    pub(crate) dec: f32,
    /// Increase AIMD increase by value in case of success.
    pub(crate) inc: usize,
    /// A threshold below which the limit won't be increased.
    pub(crate) utilisation: f32,
}

impl LimitAlgorithm for Aimd {
    fn update(&self, old_limit: usize, sample: Sample) -> usize {
        match sample.outcome {
            Outcome::Success => {
                let utilisation = sample.in_flight as f32 / old_limit as f32;

                if utilisation > self.utilisation {
                    let limit = old_limit + self.inc;
                    let new_limit = limit.clamp(self.min, self.max);
                    if new_limit > old_limit {
                        tracing::info!(old_limit, new_limit, "limit increased");
                    } else {
                        tracing::debug!(old_limit, new_limit, "limit clamped at max");
                    }

                    new_limit
                } else {
                    old_limit
                }
            }
            Outcome::Overload => {
                let new_limit = old_limit as f32 * self.dec;

                // Floor instead of round, so the limit reduces even with small numbers.
                // E.g. round(2 * 0.9) = 2, but floor(2 * 0.9) = 1
                let new_limit = new_limit.floor() as usize;

                let new_limit = new_limit.clamp(self.min, self.max);
                if new_limit < old_limit {
                    tracing::info!(old_limit, new_limit, "limit decreased");
                } else {
                    tracing::debug!(old_limit, new_limit, "limit clamped at min");
                }
                new_limit
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::rate_limiter::limit_algorithm::{
        DynamicLimiter, RateLimitAlgorithm, RateLimiterConfig,
    };

    #[tokio::test(start_paused = true)]
    async fn increase_decrease() {
        let config = RateLimiterConfig {
            initial_limit: 1,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 1,
                    max: 2,
                    inc: 10,
                    dec: 0.5,
                    utilisation: 0.8,
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(Outcome::Success);

        assert_eq!(limiter.state().limit(), 2);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(Outcome::Success);
        assert_eq!(limiter.state().limit(), 2);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(Outcome::Overload);
        assert_eq!(limiter.state().limit(), 1);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(Outcome::Overload);
        assert_eq!(limiter.state().limit(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn should_decrease_limit_on_overload() {
        let config = RateLimiterConfig {
            initial_limit: 10,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 1,
                    max: 1500,
                    inc: 10,
                    dec: 0.5,
                    utilisation: 0.8,
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(100))
            .await
            .unwrap();
        token.release(Outcome::Overload);

        assert_eq!(limiter.state().limit(), 5, "overload: decrease");
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_timeout_times_out() {
        let config = RateLimiterConfig {
            initial_limit: 1,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 1,
                    max: 2,
                    inc: 10,
                    dec: 0.5,
                    utilisation: 0.8,
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        let now = tokio::time::Instant::now();
        limiter
            .acquire_timeout(Duration::from_secs(1))
            .await
            .err()
            .unwrap();

        assert!(now.elapsed() >= Duration::from_secs(1));

        token.release(Outcome::Success);

        assert_eq!(limiter.state().limit(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn should_increase_limit_on_success_when_using_gt_util_threshold() {
        let config = RateLimiterConfig {
            initial_limit: 4,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 1,
                    max: 1500,
                    inc: 1,
                    dec: 0.5,
                    utilisation: 0.5,
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        let _token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        let _token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();

        token.release(Outcome::Success);
        assert_eq!(limiter.state().limit(), 5, "success: increase");
    }

    #[tokio::test(start_paused = true)]
    async fn should_not_change_limit_on_success_when_using_lt_util_threshold() {
        let config = RateLimiterConfig {
            initial_limit: 4,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 1,
                    max: 1500,
                    inc: 10,
                    dec: 0.5,
                    utilisation: 0.5,
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();

        token.release(Outcome::Success);
        assert_eq!(
            limiter.state().limit(),
            4,
            "success: ignore when < half limit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn should_not_change_limit_when_no_outcome() {
        let config = RateLimiterConfig {
            initial_limit: 10,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 1,
                    max: 1500,
                    inc: 10,
                    dec: 0.5,
                    utilisation: 0.5,
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        drop(token);
        assert_eq!(limiter.state().limit(), 10, "ignore");
    }
}
