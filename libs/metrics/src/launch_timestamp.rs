//! A timestamp captured at process startup to identify restarts of the process, e.g., in logs and metrics.

use chrono::Utc;

use super::register_uint_gauge;
use std::fmt::Display;

pub struct LaunchTimestamp(chrono::DateTime<Utc>);

impl LaunchTimestamp {
    pub fn generate() -> Self {
        LaunchTimestamp(Utc::now())
    }
}

impl Display for LaunchTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn set_launch_timestamp_metric(launch_ts: &'static LaunchTimestamp) {
    let millis_since_epoch: u64 = launch_ts
        .0
        .timestamp_millis()
        .try_into()
        .expect("we're after the epoch, this should be positive");
    let metric = register_uint_gauge!(
        "libmetrics_launch_timestamp",
        "Timestamp (millis since epoch) at which the process launched."
    )
    .unwrap();
    metric.set(millis_since_epoch);
}
