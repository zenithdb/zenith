use postgres::{Client, NoTls};
use std::{collections::HashMap, fmt::Display};
use tracing::{error, info};

use compute_api::spec::Database;
use url::Url;

use crate::pg_helpers::{get_existing_dbs, Escaping};

pub(crate) enum Migration<'m> {
    /// Cluster migrations are things like catalog updates, where they can be run in the default
    /// Postgres database, but affect every database in the cluster.
    Cluster(&'m str),

    /// Per-database migrations will be run in every database of the cluster. The migration will
    /// not be marked as completed until after it has been run in every database. We will save the
    /// "postgres" database for last so that we can commit the transaction as applied in the
    /// neon_migration.migration_id table.
    ///
    /// Please be aware of the race condition that exists for this type of migration. At the
    /// beginning of running the series of migrations, we get the current list of databases.
    /// However, we run migrations in a separate thread in order to not block connections to the
    /// compute. If after the time we have gotten the list of databases in the cluster, a user
    /// creates a new database, that database will not receive the migration, but we will have
    /// marked the migration as completed successfully, assuming all previous databases ran the
    /// migration to completion.
    PerDatabase(&'m str),
}

impl<'m> Display for Migration<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster(migration) | Self::PerDatabase(migration) => f.write_str(migration),
        }
    }
}

pub(crate) struct MigrationRunner<'m> {
    /// Connection string for creating subsequent Postgres client.
    connstr: Url,
    /// Owns the connection to the database containing the neon_migration.migration_id table. In
    /// addition, all Cluster migrations get ran on this connection.
    cluster_client: Client,
    /// List of migrations to run.
    migrations: &'m [Migration<'m>],
}

impl<'m> MigrationRunner<'m> {
    pub fn new(mut connstr: Url, migrations: &'m [Migration<'m>]) -> Result<Self, postgres::Error> {
        // The neon_migration.migration_id::id column is a bigint, which is equivalent to an i64
        assert!(migrations.len() + 1 < i64::MAX as usize);

        connstr
            .query_pairs_mut()
            .append_pair("application_name", "migrations");

        let cluster_client = match Client::connect(connstr.as_str(), NoTls) {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to create the cluster client for the migration runner");
                return Err(e);
            }
        };

        Ok(Self {
            connstr,
            cluster_client,
            migrations,
        })
    }

    fn get_migration_id(&mut self) -> Result<i64, postgres::Error> {
        let row = self
            .cluster_client
            .query_one("SELECT id FROM neon_migration.migration_id", &[])?;

        Ok(row.get::<&str, i64>("id"))
    }

    fn update_migration_id(client: &mut Client, migration_id: i64) -> Result<(), postgres::Error> {
        client.query(
            "UPDATE neon_migration.migration_id SET id = $1",
            &[&migration_id],
        )?;

        Ok(())
    }

    fn prepare_migrations(&mut self) -> Result<(), postgres::Error> {
        self.cluster_client
            .simple_query("CREATE SCHEMA IF NOT EXISTS neon_migration")?;
        self.cluster_client.simple_query(
            "CREATE TABLE IF NOT EXISTS neon_migration.migration_id (
                key INT NOT NULL PRIMARY KEY,
                id bigint NOT NULL DEFAULT 0
            )",
        )?;
        self.cluster_client.simple_query(
            "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING",
        )?;
        self.cluster_client
            .simple_query("ALTER SCHEMA neon_migration OWNER TO cloud_admin")?;
        self.cluster_client
            .simple_query("REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC")?;

        Ok(())
    }

    fn run_migration(
        client: &mut Client,
        db: &str,
        migration_id: i64,
        migration: &str,
        update_migration_id: bool,
    ) -> Result<(), postgres::Error> {
        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={} db={}", migration_id, db);
            return Ok(());
        }

        info!(
            "Running migration id={} db={}:\n{}\n",
            migration_id, db, migration
        );

        if let Err(e) = client.simple_query("BEGIN") {
            error!("Failed to begin the migration transaction: {}", e);
            return Err(e);
        }

        if let Err(e) = client.simple_query(migration) {
            error!("Failed to run the migration: {}", e);
            return Err(e);
        }

        if update_migration_id {
            if let Err(e) = Self::update_migration_id(client, migration_id) {
                error!(
                    "Failed to update the migration id to {}: {}",
                    migration_id, e
                );
                return Err(e);
            }
        }

        if let Err(e) = client.simple_query("COMMIT") {
            error!("Failed to commit the migration transaction: {}", e);
            return Err(e);
        }

        info!("Finished migration id={} db={}", migration_id, db);

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

        let mut dbs: Option<HashMap<String, Database>> = None;
        if current_migration < self.migrations.len()
            && self
                .migrations
                .iter()
                .any(|m| matches!(m, Migration::PerDatabase(_)))
        {
            dbs = match get_existing_dbs(&mut self.cluster_client) {
                Ok(dbs) => Some(dbs),
                Err(e) => {
                    error!("Failed to collect the existing databases: {}", e);
                    return Err(e);
                }
            };
        }

        // A Postgres connection string will always have a path with 1 segment, the database name
        let admin_db =
            urlencoding::decode(self.connstr.path_segments().unwrap().next().unwrap()).unwrap();

        while current_migration < self.migrations.len() {
            match &self.migrations[current_migration] {
                Migration::Cluster(migration) => Self::run_migration(
                    &mut self.cluster_client,
                    &admin_db,
                    current_migration as i64,
                    migration,
                    true,
                )?,
                Migration::PerDatabase(migration) => {
                    // Iterate over all non-invalid databases (datconnectivity = -2)
                    for db in dbs.as_ref().unwrap().values().filter(|d| !d.invalid) {
                        /* Once all the databases have ran the migration, then we can run it in the
                         * admin database to mark the migration as complete. See the run for the
                         * admin database outside this loop.
                         */
                        if db.name == admin_db {
                            continue;
                        }

                        let db_name = db.name.pg_quote();

                        let mut connstr = self.connstr.clone();
                        connstr.set_path(&urlencoding::encode(&db.name));
                        connstr
                            .query_pairs_mut()
                            .append_pair("application_name", "migrations");

                        /* There are 2 race conditions here. Migrations get ran in a separate thread
                         * to not block the ability to connect to the compute. The race conditions
                         * are as follow:
                         *
                         *   1. If between the time we have retrieved the list of databases in the
                         *      cluster and before we set ALLOW_CONNECTIONS back to false, the user
                         *      has changed allowed connections to the database, we will have
                         *      overwritten their change.
                         *
                         *      This is not the end of the world, but an inconvenience, nonetheless.
                         *
                         *   2. If between the time we have allowed connections to the database
                         *      and the time the migration is performed, the user disallows
                         *      connections to the database, we will fail to connect to the
                         *      database.
                         *
                         *      This is not much of a problem since we will re-run the migration
                         *      the next time we run migrations.
                         */
                        if db.restrict_conn {
                            info!("Allowing connections to {} for migrations", db_name);

                            self.cluster_client.simple_query(
                                format!("ALTER DATABASE {} WITH ALLOW_CONNECTIONS true", db_name)
                                    .as_str(),
                            )?;
                        }

                        let mut client = match Client::connect(connstr.as_str(), NoTls) {
                            Ok(client) => client,
                            Err(e) => {
                                error!(
                                    "Failed to connect to {} for running migrations: {}",
                                    db_name, e
                                );
                                return Err(e);
                            }
                        };

                        /* Do not early return, so that we can try to reset the connectability of
                         * the db.
                         */
                        let result = Self::run_migration(
                            &mut client,
                            &db_name,
                            current_migration as i64,
                            migration,
                            false,
                        );

                        drop(client);

                        if db.restrict_conn {
                            info!(
                                "Disallowing connections to {} because this migrations is done",
                                db_name
                            );

                            // Failing here is not the end of the world
                            let _ = self.cluster_client.simple_query(
                                format!("ALTER DATABASE {} WITH ALLOW_CONNECTIONS false", db_name)
                                    .as_str(),
                            );
                        }

                        result?;
                    }

                    // We can reuse the client here instead of creating a new one
                    Self::run_migration(
                        &mut self.cluster_client,
                        &admin_db.to_string().pg_quote(),
                        current_migration as i64,
                        migration,
                        true,
                    )?;
                }
            }

            current_migration += 1;
        }

        Ok(())
    }
}
