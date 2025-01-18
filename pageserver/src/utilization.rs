//! An utilization metric which is used to decide on which pageserver to put next tenant.
//!
//! The metric is exposed via `GET /v1/utilization`. Refer and maintain it's openapi spec as the
//! truth.

use anyhow::Context;
use std::path::Path;
use utils::serde_percent::Percent;

use pageserver_api::models::PageserverUtilization;

use crate::{
    config::PageServerConf, metrics::NODE_UTILIZATION_SCORE, tenant::mgr::TenantShardManager,
};

pub(crate) fn regenerate(
    conf: &PageServerConf,
    tenants_path: &Path,
    tenant_manager: &TenantShardManager,
) -> anyhow::Result<PageserverUtilization> {
    let statvfs = nix::sys::statvfs::statvfs(tenants_path)
        .map_err(std::io::Error::from)
        .context("statvfs tenants directory")?;

    // https://unix.stackexchange.com/a/703650
    let blocksz = if statvfs.fragment_size() > 0 {
        statvfs.fragment_size()
    } else {
        statvfs.block_size()
    };

    #[cfg_attr(not(target_os = "macos"), allow(clippy::unnecessary_cast))]
    let free = statvfs.blocks_available() as u64 * blocksz;

    #[cfg_attr(not(target_os = "macos"), allow(clippy::unnecessary_cast))]
    let used = statvfs
        .blocks()
        // use blocks_free instead of available here to match df in case someone compares
        .saturating_sub(statvfs.blocks_free()) as u64
        * blocksz;

    let captured_at = std::time::SystemTime::now();

    // Calculate aggregate utilization from tenants on this pageserver
    let (disk_wanted_bytes, shard_count) = tenant_manager.calculate_utilization()?;

    // Fetch the fraction of disk space which may be used
    let disk_usable_pct = match conf.disk_usage_based_eviction.clone() {
        Some(e) => e.max_usage_pct,
        None => Percent::new(100).unwrap(),
    };

    // Express a static value for how many shards we may schedule on one node
    const MAX_SHARDS: u32 = 20000;

    let mut doc = PageserverUtilization {
        disk_usage_bytes: used,
        free_space_bytes: free,
        disk_wanted_bytes,
        disk_usable_pct,
        shard_count,
        max_shard_count: MAX_SHARDS,
        utilization_score: None,
        captured_at: utils::serde_system_time::SystemTime(captured_at),
    };

    // Initialize `PageserverUtilization::utilization_score`
    let score = doc.cached_score();
    NODE_UTILIZATION_SCORE.set(score);

    Ok(doc)
}
