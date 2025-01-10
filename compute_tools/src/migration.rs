use anyhow::{Context, Result};
use fail::fail_point;
use postgres::{Client, Transaction};
use tracing::info;

/// Runs a series of migrations on a target database
pub(crate) struct MigrationRunner<'m> {
    client: &'m mut Client,
    migrations: &'m [&'m str],
}

impl<'m> MigrationRunner<'m> {
    /// Create a new migration runner
    pub fn new(client: &'m mut Client, migrations: &'m [&'m str]) -> Self {
        // The neon_migration.migration_id::id column is a bigint, which is equivalent to an i64
        assert!(migrations.len() + 1 < i64::MAX as usize);

        Self { client, migrations }
    }

    /// Get the current value neon_migration.migration_id
    fn get_migration_id(&mut self) -> Result<i64> {
        let row = self
            .client
            .query_one("SELECT id FROM neon_migration.migration_id", &[])?;

        Ok(row.get::<&str, i64>("id"))
    }

    /// Update the neon_migration.migration_id value
    ///
    /// This function has a fail point called compute-migration, which can be
    /// used if you would like to fail the application of a series of migrations
    /// at some point.
    fn update_migration_id(txn: &mut Transaction, migration_id: i64) -> Result<()> {
        // We use this fail point in order to check that failing in the
        // middle of applying a series of migrations fails in an expected
        // manner
        if cfg!(feature = "testing") {
            let fail = (|| {
                fail_point!("compute-migration", |fail_migration_id| {
                    migration_id == fail_migration_id.unwrap().parse::<i64>().unwrap()
                });

                false
            })();

            if fail {
                return Err(anyhow::anyhow!(format!(
                    "migration {} was configured to fail because of a failpoint",
                    migration_id
                )));
            }
        }

        txn.query(
            "UPDATE neon_migration.migration_id SET id = $1",
            &[&migration_id],
        )
        .with_context(|| format!("update neon_migration.migration_id to {migration_id}"))?;

        Ok(())
    }

    /// Prepare the migrations the target database for handling migrations
    fn prepare_database(&mut self) -> Result<()> {
        self.client
            .simple_query("CREATE SCHEMA IF NOT EXISTS neon_migration")?;
        self.client.simple_query("CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key INT NOT NULL PRIMARY KEY, id bigint NOT NULL DEFAULT 0)")?;
        self.client.simple_query(
            "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING",
        )?;
        self.client
            .simple_query("ALTER SCHEMA neon_migration OWNER TO cloud_admin")?;
        self.client
            .simple_query("REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC")?;

        Ok(())
    }

    /// Run an individual migration
    fn run_migration(txn: &mut Transaction, migration_id: i64, migration: &str) -> Result<()> {
        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={}", migration_id);

            // Even though we are skipping the migration, updating the
            // migration ID should help keep logic easy to understand when
            // trying to understand the state of a cluster.
            Self::update_migration_id(txn, migration_id)?;
        } else {
            info!("Running migration id={}:\n{}\n", migration_id, migration);

            txn.simple_query(migration)
                .with_context(|| format!("apply migration {migration_id}"))?;

            Self::update_migration_id(txn, migration_id)?;
        }

        Ok(())
    }

    /// Run the configured set of migrations
    pub fn run_migrations(mut self) -> Result<()> {
        self.prepare_database()
            .context("prepare database to handle migrations")?;

        let mut current_migration = self.get_migration_id()? as usize;
        while current_migration < self.migrations.len() {
            // The index lags the migration ID by 1, so the current migration
            // ID is also the next index
            let migration_id = (current_migration + 1) as i64;

            let mut txn = self
                .client
                .transaction()
                .with_context(|| format!("begin transaction for migration {migration_id}"))?;

            Self::run_migration(&mut txn, migration_id, self.migrations[current_migration])
                .with_context(|| format!("running migration {migration_id}"))?;

            txn.commit()
                .with_context(|| format!("commit transaction for migration {migration_id}"))?;

            info!("Finished migration id={}", migration_id);

            current_migration += 1;
        }

        Ok(())
    }
}
