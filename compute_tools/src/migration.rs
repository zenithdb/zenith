use postgres::Client;
use tracing::{error, info};

pub(crate) struct MigrationRunner<'m> {
    client: &'m mut Client,
    migrations: &'m [&'m str],
}

impl<'m> MigrationRunner<'m> {
    pub fn new(client: &'m mut Client, migrations: &'m [&'m str]) -> Self {
        // The neon_migration.migration_id::id column is a bigint, which is equivalent to an i64
        assert!(migrations.len() + 1 < i64::MAX as usize);

        Self { client, migrations }
    }

    fn get_migration_id(&mut self) -> Result<i64, postgres::Error> {
        let row = self
            .client
            .query_one("SELECT id FROM neon_migration.migration_id", &[])?;

        Ok(row.get::<&str, i64>("id"))
    }

    fn update_migration_id(&mut self, migration_id: i64) -> Result<(), postgres::Error> {
        self.client.query(
            "UPDATE neon_migration.migration_id SET id = $1",
            &[&migration_id],
        )?;

        Ok(())
    }

    fn prepare_migrations(&mut self) -> Result<(), postgres::Error> {
        let query = "CREATE SCHEMA IF NOT EXISTS neon_migration";
        self.client.simple_query(query)?;

        let query = "CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key INT NOT NULL PRIMARY KEY, id bigint NOT NULL DEFAULT 0)";
        self.client.simple_query(query)?;

        let query = "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING";
        self.client.simple_query(query)?;

        let query = "ALTER SCHEMA neon_migration OWNER TO cloud_admin";
        self.client.simple_query(query)?;

        let query = "REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC";
        self.client.simple_query(query)?;

        Ok(())
    }

    fn run_migration(&mut self, migration_id: i64, migration: &str) -> Result<(), postgres::Error> {
        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={}", migration_id);
            return Ok(());
        }

        info!("Running migration id={}:\n{}\n", migration_id, migration);

        if let Err(e) = self.client.simple_query("BEGIN") {
            error!("Failed to begin the migration transaction: {}", e);
            return Err(e);
        }

        if let Err(e) = self.client.simple_query(migration) {
            error!("Failed to run the migration: {}", e);
            return Err(e);
        }

        if let Err(e) = self.update_migration_id(migration_id) {
            error!(
                "Failed to update the migration id to {}: {}",
                migration_id, e
            );
            return Err(e);
        }

        if let Err(e) = self.client.simple_query("COMMIT") {
            error!("Failed to commit the migration transaction: {}", e);
            return Err(e);
        }

        info!("Finished migration id={}", migration_id);

        Ok(())
    }

    pub fn run_migrations(mut self) -> Result<(), postgres::Error> {
        if let Err(e) = self.prepare_migrations() {
            error!("Failed to prepare the migration relations: {}", e);
            return Err(e);
        }

        let mut current_migration = match self.get_migration_id() {
            Ok(id) => id as usize,
            Err(e) => {
                error!("Failed to get the current migration id: {}", e);
                return Err(e);
            }
        };

        while current_migration < self.migrations.len() {
            let migration = self.migrations[current_migration];

            self.run_migration(current_migration as i64, migration)?;

            current_migration += 1;
        }

        Ok(())
    }
}
