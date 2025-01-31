//! Logic for configuring and scaling the Postgres file cache.

use std::num::NonZeroU64;

use crate::MiB;
use anyhow::{anyhow, Context};
use tokio_postgres::{types::ToSql, Client, NoTls, Row};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Manages Postgres' file cache by keeping a connection open.
#[derive(Debug)]
pub struct FileCacheState {
    client: Client,
    conn_str: String,
    pub(crate) config: FileCacheConfig,

    /// A token for cancelling spawned threads during shutdown.
    token: CancellationToken,
}

#[derive(Debug)]
pub struct FileCacheConfig {
    /// The size of the file cache, in terms of the size of the resource it consumes
    /// (currently: only memory)
    ///
    /// For example, setting `resource_multipler = 0.75` gives the cache a target size of 75% of total
    /// resources.
    ///
    /// This value must be strictly between 0 and 1.
    resource_multiplier: f64,

    /// The required minimum amount of memory, in bytes, that must remain available
    /// after subtracting the file cache.
    ///
    /// This value must be non-zero.
    min_remaining_after_cache: NonZeroU64,

    /// Controls the rate of increase in the file cache's size as it grows from zero
    /// (when total resources equals min_remaining_after_cache) to the desired size based on
    /// `resource_multiplier`.
    ///
    /// A `spread_factor` of zero means that all additional resources will go to the cache until it
    /// reaches the desired size. Setting `spread_factor` to N roughly means "for every 1 byte added to
    /// the cache's size, N bytes are reserved for the rest of the system, until the cache gets to
    /// its desired size".
    ///
    /// This value must be >= 0, and must retain an increase that is more than what would be given by
    /// `resource_multiplier`. For example, setting `resource_multiplier` = 0.75 but `spread_factor` = 1
    /// would be invalid, because `spread_factor` would induce only 50% usage - never reaching the 75%
    /// as desired by `resource_multiplier`.
    ///
    /// `spread_factor` is too large if `(spread_factor + 1) * resource_multiplier >= 1`.
    spread_factor: f64,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            resource_multiplier: 0.75,
            // 256 MiB - lower than when in memory because overcommitting is safe; if we don't have
            // memory, the kernel will just evict from its page cache, rather than e.g. killing
            // everything.
            min_remaining_after_cache: NonZeroU64::new(256 * MiB).unwrap(),
            spread_factor: 0.1,
        }
    }
}

impl FileCacheConfig {
    /// Make sure fields of the config are consistent.
    pub fn validate(&self) -> anyhow::Result<()> {
        // Single field validity
        anyhow::ensure!(
            0.0 < self.resource_multiplier && self.resource_multiplier < 1.0,
            "resource_multiplier must be between 0.0 and 1.0 exclusive, got {}",
            self.resource_multiplier
        );
        anyhow::ensure!(
            self.spread_factor >= 0.0,
            "spread_factor must be >= 0, got {}",
            self.spread_factor
        );

        // Check that `resource_multiplier` and `spread_factor` are valid w.r.t. each other.
        //
        // As shown in `calculate_cache_size`, we have two lines resulting from `resource_multiplier` and
        // `spread_factor`, respectively. They are:
        //
        //                 `total`           `min_remaining_after_cache`
        //   size = ————————————————————— - —————————————————————————————
        //           `spread_factor` + 1         `spread_factor` + 1
        //
        // and
        //
        //   size = `resource_multiplier` × total
        //
        // .. where `total` is the total resources. These are isomorphic to the typical 'y = mx + b'
        // form, with y = "size" and x = "total".
        //
        // These lines intersect at:
        //
        //               `min_remaining_after_cache`
        //   ———————————————————————————————————————————————————
        //    1 - `resource_multiplier` × (`spread_factor` + 1)
        //
        // We want to ensure that this value (a) exists, and (b) is >= `min_remaining_after_cache`. This is
        // guaranteed when '`resource_multiplier` × (`spread_factor` + 1)' is less than 1.
        // (We also need it to be >= 0, but that's already guaranteed.)

        let intersect_factor = self.resource_multiplier * (self.spread_factor + 1.0);
        anyhow::ensure!(
            intersect_factor < 1.0,
            "incompatible resource_multipler and spread_factor"
        );
        Ok(())
    }

    /// Calculate the desired size of the cache, given the total memory
    pub fn calculate_cache_size(&self, total: u64) -> u64 {
        // *Note*: all units are in bytes, until the very last line.
        let available = total.saturating_sub(self.min_remaining_after_cache.get());
        if available == 0 {
            return 0;
        }

        // Conversions to ensure we don't overflow from floating-point ops
        let size_from_spread =
            i64::max(0, (available as f64 / (1.0 + self.spread_factor)) as i64) as u64;

        let size_from_normal = (total as f64 * self.resource_multiplier) as u64;

        let byte_size = u64::min(size_from_spread, size_from_normal);

        // The file cache operates in units of mebibytes, so the sizes we produce should
        // be rounded to a mebibyte. We round down to be conservative.
        byte_size / MiB * MiB
    }
}

impl FileCacheState {
    /// Connect to the file cache.
    #[tracing::instrument(skip_all, fields(%conn_str, ?config))]
    pub async fn new(
        conn_str: &str,
        config: FileCacheConfig,
        token: CancellationToken,
    ) -> anyhow::Result<Self> {
        config.validate().context("file cache config is invalid")?;

        info!(conn_str, "connecting to Postgres file cache");
        let client = FileCacheState::connect(conn_str, token.clone())
            .await
            .context("failed to connect to postgres file cache")?;

        let conn_str = conn_str.to_string();
        Ok(Self {
            client,
            config,
            conn_str,
            token,
        })
    }

    /// Connect to Postgres.
    ///
    /// Aborts the spawned thread if the kill signal is received. This is not
    /// a method as it is called in [`FileCacheState::new`].
    #[tracing::instrument(skip_all, fields(%conn_str))]
    async fn connect(conn_str: &str, token: CancellationToken) -> anyhow::Result<Client> {
        let (client, conn) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .context("failed to connect to pg client")?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own. See tokio-postgres docs.
        crate::spawn_with_cancel(
            token,
            |res| {
                if let Err(e) = res {
                    error!(error = format_args!("{e:#}"), "postgres error");
                }
            },
            conn,
        );

        Ok(client)
    }

    /// Execute a query with a retry if necessary.
    ///
    /// If the initial query fails, we restart the database connection and attempt
    /// if again.
    #[tracing::instrument(skip_all, fields(%statement))]
    pub async fn query_with_retry(
        &mut self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> anyhow::Result<Vec<Row>> {
        match self
            .client
            .query(statement, params)
            .await
            .context("failed to execute query")
        {
            Ok(rows) => Ok(rows),
            Err(e) => {
                error!(error = format_args!("{e:#}"), "postgres error -> retrying");

                let client = FileCacheState::connect(&self.conn_str, self.token.clone())
                    .await
                    .context("failed to connect to postgres file cache")?;
                info!("successfully reconnected to postgres client");

                // Replace the old client and attempt the query with the new one
                self.client = client;
                self.client
                    .query(statement, params)
                    .await
                    .context("failed to execute query a second time")
            }
        }
    }

    /// Get the current size of the file cache.
    #[tracing::instrument(skip_all)]
    pub async fn get_file_cache_size(&mut self) -> anyhow::Result<u64> {
        self.query_with_retry(
            // The file cache GUC variable is in MiB, but the conversion with
            // pg_size_bytes means that the end result we get is in bytes.
            "SELECT pg_size_bytes(current_setting('neon.file_cache_size_limit'));",
            &[],
        )
        .await
        .context("failed to query pg for file cache size")?
        .first()
        .ok_or_else(|| anyhow!("file cache size query returned no rows"))?
        // pg_size_bytes returns a bigint which is the same as an i64.
        .try_get::<_, i64>(0)
        // Since the size of the table is not negative, the cast is sound.
        .map(|bytes| bytes as u64)
        .context("failed to extract file cache size from query result")
    }

    /// Attempt to set the file cache size, returning the size it was actually
    /// set to.
    #[tracing::instrument(skip_all, fields(%num_bytes))]
    pub async fn set_file_cache_size(&mut self, num_bytes: u64) -> anyhow::Result<u64> {
        let max_bytes = self
            // The file cache GUC variable is in MiB, but the conversion with pg_size_bytes
            // means that the end result we get is in bytes.
            .query_with_retry(
                "SELECT pg_size_bytes(current_setting('neon.max_file_cache_size'));",
                &[],
            )
            .await
            .context("failed to query pg for max file cache size")?
            .first()
            .ok_or_else(|| anyhow!("max file cache size query returned no rows"))?
            .try_get::<_, i64>(0)
            .map(|bytes| bytes as u64)
            .context("failed to extract max file cache size from query result")?;

        let max_mb = max_bytes / MiB;
        let num_mb = u64::min(num_bytes, max_bytes) / MiB;

        let capped = if num_bytes > max_bytes {
            " (capped by maximum size)"
        } else {
            ""
        };

        info!(
            size = num_mb,
            max = max_mb,
            "updating file cache size {capped}",
        );

        // note: even though the normal ways to get the cache size produce values with trailing "MB"
        // (hence why we call pg_size_bytes in `get_file_cache_size`'s query), the format
        // it expects to set the value is "integer number of MB" without trailing units.
        // For some reason, this *really* wasn't working with normal arguments, so that's
        // why we're constructing the query here.
        self.client
            .query(
                &format!("ALTER SYSTEM SET neon.file_cache_size_limit = {};", num_mb),
                &[],
            )
            .await
            .context("failed to change file cache size limit")?;

        // must use pg_reload_conf to have the settings change take effect
        self.client
            .execute("SELECT pg_reload_conf();", &[])
            .await
            .context("failed to reload config")?;

        Ok(num_mb * MiB)
    }
}
