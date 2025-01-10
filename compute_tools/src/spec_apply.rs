use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::iter::empty;
use std::iter::once;
use std::sync::Arc;

use crate::compute::construct_superuser_query;
use crate::pg_helpers::{escape_literal, DatabaseExt, Escaping, GenericOptionsSearch, RoleExt};
use anyhow::{bail, Result};
use compute_api::spec::{ComputeFeature, ComputeSpec, Database, PgIdent, Role};
use futures::future::join_all;
use tokio::sync::RwLock;
use tokio_postgres::Client;
use tracing::{debug, info_span, Instrument};

#[derive(Clone)]
pub enum DB {
    SystemDB,
    UserDB(Database),
}

impl DB {
    pub fn new(db: Database) -> DB {
        Self::UserDB(db)
    }

    pub fn is_owned_by(&self, role: &PgIdent) -> bool {
        match self {
            DB::SystemDB => false,
            DB::UserDB(db) => &db.owner == role,
        }
    }
}

impl Debug for DB {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DB::SystemDB => f.debug_tuple("SystemDB").finish(),
            DB::UserDB(db) => f.debug_tuple("UserDB").field(&db.name).finish(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum PerDatabasePhase {
    DeleteDBRoleReferences,
    ChangeSchemaPerms,
    HandleAnonExtension,
    DropSubscriptionsForDeletedDatabases,
}

#[derive(Clone, Debug)]
pub enum ApplySpecPhase {
    CreateSuperUser,
    DropInvalidDatabases,
    RenameRoles,
    CreateAndAlterRoles,
    RenameAndDeleteDatabases,
    CreateAndAlterDatabases,
    RunInEachDatabase { db: DB, subphase: PerDatabasePhase },
    HandleOtherExtensions,
    HandleNeonExtension,
    CreateAvailabilityCheck,
    DropRoles,
}

pub struct Operation {
    pub query: String,
    pub comment: Option<String>,
}

pub struct MutableApplyContext {
    pub roles: HashMap<String, Role>,
    pub dbs: HashMap<String, Database>,
}

/// Apply the operations that belong to the given spec apply phase.
///
/// Commands within a single phase are executed in order of Iterator yield.
/// Commands of ApplySpecPhase::RunInEachDatabase will execute in the database
/// indicated by its `db` field, and can share a single client for all changes
/// to that database.
///
/// Notes:
/// - Commands are pipelined, and thus may cause incomplete apply if one
///   command of many fails.
/// - Failing commands will fail the phase's apply step once the return value
///   is processed.
/// - No timeouts have (yet) been implemented.
/// - The caller is responsible for limiting and/or applying concurrency.
pub async fn apply_operations<'a, Fut, F>(
    spec: Arc<ComputeSpec>,
    ctx: Arc<RwLock<MutableApplyContext>>,
    jwks_roles: Arc<HashSet<String>>,
    apply_spec_phase: ApplySpecPhase,
    client: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<&'a Client>>,
{
    debug!("Starting phase {:?}", &apply_spec_phase);
    let span = info_span!("db_apply_changes", phase=?apply_spec_phase);
    let span2 = span.clone();
    async move {
        debug!("Processing phase {:?}", &apply_spec_phase);
        let ctx = ctx;

        let mut ops = get_operations(&spec, &ctx, &jwks_roles, &apply_spec_phase)
            .await?
            .peekable();

        // Return (and by doing so, skip requesting the PostgreSQL client) if
        // we don't have any operations scheduled.
        if ops.peek().is_none() {
            return Ok(());
        }

        let client = client().await?;

        debug!("Applying phase {:?}", &apply_spec_phase);

        let active_queries = ops
            .map(|op| {
                let Operation { comment, query } = op;
                let inspan = match comment {
                    None => span.clone(),
                    Some(comment) => info_span!("phase {}: {}", comment),
                };

                async {
                    let query = query;
                    let res = client.simple_query(&query).await;
                    debug!(
                        "{} {}",
                        if res.is_ok() {
                            "successfully executed"
                        } else {
                            "failed to execute"
                        },
                        query
                    );
                    res
                }
                .instrument(inspan)
            })
            .collect::<Vec<_>>();

        drop(ctx);

        for it in join_all(active_queries).await {
            drop(it?);
        }

        debug!("Completed phase {:?}", &apply_spec_phase);

        Ok(())
    }
    .instrument(span2)
    .await
}

/// Create a stream of operations to be executed for that phase of applying
/// changes.
///
/// In the future we may generate a single stream of changes and then
/// sort/merge/batch execution, but for now this is a nice way to improve
/// batching behaviour of the commands.
async fn get_operations<'a>(
    spec: &'a ComputeSpec,
    ctx: &'a RwLock<MutableApplyContext>,
    jwks_roles: &'a HashSet<String>,
    apply_spec_phase: &'a ApplySpecPhase,
) -> Result<Box<dyn Iterator<Item = Operation> + 'a + Send>> {
    match apply_spec_phase {
        ApplySpecPhase::CreateSuperUser => {
            let query = construct_superuser_query(spec);

            Ok(Box::new(once(Operation {
                query,
                comment: None,
            })))
        }
        ApplySpecPhase::DropInvalidDatabases => {
            let mut ctx = ctx.write().await;
            let databases = &mut ctx.dbs;

            let keys: Vec<_> = databases
                .iter()
                .filter(|(_, db)| db.invalid)
                .map(|(dbname, _)| dbname.clone())
                .collect();

            // After recent commit in Postgres, interrupted DROP DATABASE
            // leaves the database in the invalid state. According to the
            // commit message, the only option for user is to drop it again.
            // See:
            //   https://github.com/postgres/postgres/commit/a4b4cc1d60f7e8ccfcc8ff8cb80c28ee411ad9a9
            //
            // Postgres Neon extension is done the way, that db is de-registered
            // in the control plane metadata only after it is dropped. So there is
            // a chance that it still thinks that the db should exist. This means
            // that it will be re-created by the `CreateDatabases` phase. This
            // is fine, as user can just drop the table again (in vanilla
            // Postgres they would need to do the same).
            let operations = keys
                .into_iter()
                .filter_map(move |dbname| ctx.dbs.remove(&dbname))
                .map(|db| Operation {
                    query: format!("DROP DATABASE IF EXISTS {}", db.name.pg_quote()),
                    comment: Some(format!("Dropping invalid database {}", db.name)),
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::RenameRoles => {
            let mut ctx = ctx.write().await;

            let operations = spec
                .delta_operations
                .iter()
                .flatten()
                .filter(|op| op.action == "rename_role")
                .filter_map(move |op| {
                    let roles = &mut ctx.roles;

                    if roles.contains_key(op.name.as_str()) {
                        None
                    } else {
                        let new_name = op.new_name.as_ref().unwrap();
                        let mut role = roles.remove(op.name.as_str()).unwrap();

                        role.name = new_name.clone();
                        role.encrypted_password = None;
                        roles.insert(role.name.clone(), role);

                        Some(Operation {
                            query: format!(
                                "ALTER ROLE {} RENAME TO {}",
                                op.name.pg_quote(),
                                new_name.pg_quote()
                            ),
                            comment: Some(format!("renaming role '{}' to '{}'", op.name, new_name)),
                        })
                    }
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateAndAlterRoles => {
            let mut ctx = ctx.write().await;

            let operations = spec.cluster.roles
                .iter()
                .filter_map(move |role| {
                    let roles = &mut ctx.roles;
                    let db_role = roles.get(&role.name);

                    match db_role {
                        Some(db_role) => {
                            if db_role.encrypted_password != role.encrypted_password {
                                // This can be run on /every/ role! Not just ones created through the console.
                                // This means that if you add some funny ALTER here that adds a permission,
                                // this will get run even on user-created roles! This will result in different
                                // behavior before and after a spec gets reapplied. The below ALTER as it stands
                                // now only grants LOGIN and changes the password. Please do not allow this branch
                                // to do anything silly.
                                Some(Operation {
                                    query: format!(
                                        "ALTER ROLE {} {}",
                                        role.name.pg_quote(),
                                        role.to_pg_options(),
                                    ),
                                    comment: None,
                                })
                            } else {
                                None
                            }
                        }
                        None => {
                            let query = if !jwks_roles.contains(role.name.as_str()) {
                                format!(
                                    "CREATE ROLE {} INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser {}",
                                    role.name.pg_quote(),
                                    role.to_pg_options(),
                                )
                            } else {
                                format!(
                                    "CREATE ROLE {} {}",
                                    role.name.pg_quote(),
                                    role.to_pg_options(),
                                )
                            };
                            Some(Operation {
                                query,
                                comment: Some(format!("creating role {}", role.name)),
                            })
                        }
                    }
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::RenameAndDeleteDatabases => {
            let mut ctx = ctx.write().await;

            let operations = spec
                .delta_operations
                .iter()
                .flatten()
                .filter_map(move |op| {
                    let databases = &mut ctx.dbs;
                    match op.action.as_str() {
                        // We do not check whether the DB exists or not,
                        // Postgres will take care of it for us
                        "delete_db" => {
                            // In Postgres we can't drop a database if it is a template.
                            // So we need to unset the template flag first, but it could
                            // be a retry, so we could've already dropped the database.
                            // Check that database exists first to make it idempotent.
                            let unset_template_query: String = format!(
                                include_str!("sql/unset_template_for_drop_dbs.sql"),
                                datname_str = escape_literal(&op.name),
                                datname = &op.name.pg_quote()
                            );

                            // Use FORCE to drop database even if there are active connections.
                            // We run this from `cloud_admin`, so it should have enough privileges.
                            //
                            // NB: there could be other db states, which prevent us from dropping
                            // the database. For example, if db is used by any active subscription
                            // or replication slot.
                            // Such cases are handled in the DropSubscriptionsForDeletedDatabases
                            // phase. We do all the cleanup before actually dropping the database.
                            let drop_db_query: String = format!(
                                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                                &op.name.pg_quote()
                            );

                            databases.remove(&op.name);

                            Some(vec![
                                Operation {
                                    query: unset_template_query,
                                    comment: Some(format!(
                                        "optionally clearing template flags for DB {}",
                                        op.name,
                                    )),
                                },
                                Operation {
                                    query: drop_db_query,
                                    comment: Some(format!("deleting database {}", op.name,)),
                                },
                            ])
                        }
                        "rename_db" => {
                            if let Some(mut db) = databases.remove(&op.name) {
                                // update state of known databases
                                let new_name = op.new_name.as_ref().unwrap();
                                db.name = new_name.clone();
                                databases.insert(db.name.clone(), db);

                                Some(vec![Operation {
                                    query: format!(
                                        "ALTER DATABASE {} RENAME TO {}",
                                        op.name.pg_quote(),
                                        new_name.pg_quote(),
                                    ),
                                    comment: Some(format!(
                                        "renaming database '{}' to '{}'",
                                        op.name, new_name
                                    )),
                                }])
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                })
                .flatten();

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateAndAlterDatabases => {
            let mut ctx = ctx.write().await;

            let operations = spec
                .cluster
                .databases
                .iter()
                .filter_map(move |db| {
                    let databases = &mut ctx.dbs;
                    if let Some(edb) = databases.get_mut(&db.name) {
                        let change_owner = if edb.owner.starts_with('"') {
                            db.owner.pg_quote() != edb.owner
                        } else {
                            db.owner != edb.owner
                        };

                        edb.owner = db.owner.clone();

                        if change_owner {
                            Some(vec![Operation {
                                query: format!(
                                    "ALTER DATABASE {} OWNER TO {}",
                                    db.name.pg_quote(),
                                    db.owner.pg_quote()
                                ),
                                comment: Some(format!(
                                    "changing database owner of database {} to {}",
                                    db.name, db.owner
                                )),
                            }])
                        } else {
                            None
                        }
                    } else {
                        databases.insert(db.name.clone(), db.clone());

                        Some(vec![
                            Operation {
                                query: format!(
                                    "CREATE DATABASE {} {}",
                                    db.name.pg_quote(),
                                    db.to_pg_options(),
                                ),
                                comment: None,
                            },
                            Operation {
                                query: format!(
                                    "GRANT ALL PRIVILEGES ON DATABASE {} TO neon_superuser",
                                    db.name.pg_quote()
                                ),
                                comment: None,
                            },
                        ])
                    }
                })
                .flatten();

            Ok(Box::new(operations))
        }
        ApplySpecPhase::RunInEachDatabase { db, subphase } => {
            match subphase {
                PerDatabasePhase::DropSubscriptionsForDeletedDatabases => {
                    match &db {
                        DB::UserDB(db) => {
                            let drop_subscription_query: String = format!(
                                include_str!("sql/drop_subscription_for_drop_dbs.sql"),
                                datname_str = escape_literal(&db.name),
                            );

                            let operations = vec![Operation {
                                query: drop_subscription_query,
                                comment: Some(format!(
                                    "optionally dropping subscriptions for DB {}",
                                    db.name,
                                )),
                            }]
                            .into_iter();

                            Ok(Box::new(operations))
                        }
                        // skip this cleanup for the system databases
                        // because users can't drop them
                        DB::SystemDB => Ok(Box::new(empty())),
                    }
                }
                PerDatabasePhase::DeleteDBRoleReferences => {
                    let ctx = ctx.read().await;

                    let operations =
                        spec.delta_operations
                            .iter()
                            .flatten()
                            .filter(|op| op.action == "delete_role")
                            .filter_map(move |op| {
                                if db.is_owned_by(&op.name) {
                                    return None;
                                }
                                if !ctx.roles.contains_key(&op.name) {
                                    return None;
                                }
                                let quoted = op.name.pg_quote();
                                let new_owner = match &db {
                                    DB::SystemDB => PgIdent::from("cloud_admin").pg_quote(),
                                    DB::UserDB(db) => db.owner.pg_quote(),
                                };

                                Some(vec![
                                    // This will reassign all dependent objects to the db owner
                                    Operation {
                                        query: format!(
                                            "REASSIGN OWNED BY {} TO {}",
                                            quoted, new_owner,
                                        ),
                                        comment: None,
                                    },
                                    // Revoke some potentially blocking privileges (Neon-specific currently)
                                    Operation {
                                        query: format!(
                                            include_str!("sql/pre_drop_role_revoke_privileges.sql"),
                                            role_name = quoted,
                                        ),
                                        comment: None,
                                    },
                                    // This now will only drop privileges of the role
                                    // TODO: this is obviously not 100% true because of the above case,
                                    // there could be still some privileges that are not revoked. Maybe this
                                    // only drops privileges that were granted *by this* role, not *to this* role,
                                    // but this has to be checked.
                                    Operation {
                                        query: format!("DROP OWNED BY {}", quoted),
                                        comment: None,
                                    },
                                ])
                            })
                            .flatten();

                    Ok(Box::new(operations))
                }
                PerDatabasePhase::ChangeSchemaPerms => {
                    let ctx = ctx.read().await;
                    let databases = &ctx.dbs;

                    let db = match &db {
                        // ignore schema permissions on the system database
                        DB::SystemDB => return Ok(Box::new(empty())),
                        DB::UserDB(db) => db,
                    };

                    if databases.get(&db.name).is_none() {
                        bail!("database {} doesn't exist in PostgreSQL", db.name);
                    }

                    let edb = databases.get(&db.name).unwrap();

                    if edb.restrict_conn || edb.invalid {
                        return Ok(Box::new(empty()));
                    }

                    let operations = vec![
                        Operation {
                            query: format!(
                                include_str!("sql/set_public_schema_owner.sql"),
                                db_owner = db.owner.pg_quote()
                            ),
                            comment: None,
                        },
                        Operation {
                            query: String::from(include_str!("sql/default_grants.sql")),
                            comment: None,
                        },
                    ]
                    .into_iter();

                    Ok(Box::new(operations))
                }
                PerDatabasePhase::HandleAnonExtension => {
                    // Only install Anon into user databases
                    let db = match &db {
                        DB::SystemDB => return Ok(Box::new(empty())),
                        DB::UserDB(db) => db,
                    };
                    // Never install Anon when it's not enabled as feature
                    if !spec.features.contains(&ComputeFeature::AnonExtension) {
                        return Ok(Box::new(empty()));
                    }

                    // Only install Anon when it's added in preload libraries
                    let opt_libs = spec.cluster.settings.find("shared_preload_libraries");

                    let libs = match opt_libs {
                        Some(libs) => libs,
                        None => return Ok(Box::new(empty())),
                    };

                    if !libs.contains("anon") {
                        return Ok(Box::new(empty()));
                    }

                    let db_owner = db.owner.pg_quote();

                    let operations = vec![
                        // Create anon extension if this compute needs it
                        // Users cannot create it themselves, because superuser is required.
                        Operation {
                            query: String::from("CREATE EXTENSION IF NOT EXISTS anon CASCADE"),
                            comment: Some(String::from("creating anon extension")),
                        },
                        // Initialize anon extension
                        // This also requires superuser privileges, so users cannot do it themselves.
                        Operation {
                            query: String::from("SELECT anon.init()"),
                            comment: Some(String::from("initializing anon extension data")),
                        },
                        Operation {
                            query: format!("GRANT ALL ON SCHEMA anon TO {}", db_owner),
                            comment: Some(String::from(
                                "granting anon extension schema permissions",
                            )),
                        },
                        Operation {
                            query: format!(
                                "GRANT ALL ON ALL FUNCTIONS IN SCHEMA anon TO {}",
                                db_owner
                            ),
                            comment: Some(String::from(
                                "granting anon extension schema functions permissions",
                            )),
                        },
                        // We need this, because some functions are defined as SECURITY DEFINER.
                        // In Postgres SECURITY DEFINER functions are executed with the privileges
                        // of the owner.
                        // In anon extension this it is needed to access some GUCs, which are only accessible to
                        // superuser. But we've patched postgres to allow db_owner to access them as well.
                        // So we need to change owner of these functions to db_owner.
                        Operation {
                            query: format!(
                                include_str!("sql/anon_ext_fn_reassign.sql"),
                                db_owner = db_owner,
                            ),
                            comment: Some(String::from(
                                "change anon extension functions owner to database_owner",
                            )),
                        },
                        Operation {
                            query: format!(
                                "GRANT ALL ON ALL TABLES IN SCHEMA anon TO {}",
                                db_owner,
                            ),
                            comment: Some(String::from(
                                "granting anon extension tables permissions",
                            )),
                        },
                        Operation {
                            query: format!(
                                "GRANT ALL ON ALL SEQUENCES IN SCHEMA anon TO {}",
                                db_owner,
                            ),
                            comment: Some(String::from(
                                "granting anon extension sequences permissions",
                            )),
                        },
                    ]
                    .into_iter();

                    Ok(Box::new(operations))
                }
            }
        }
        // Interestingly, we only install p_s_s in the main database, even when
        // it's preloaded.
        ApplySpecPhase::HandleOtherExtensions => {
            if let Some(libs) = spec.cluster.settings.find("shared_preload_libraries") {
                if libs.contains("pg_stat_statements") {
                    return Ok(Box::new(once(Operation {
                        query: String::from("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"),
                        comment: Some(String::from("create system extensions")),
                    })));
                }
            }
            Ok(Box::new(empty()))
        }
        ApplySpecPhase::HandleNeonExtension => {
            let operations = vec![
                Operation {
                    query: String::from("CREATE SCHEMA IF NOT EXISTS neon"),
                    comment: Some(String::from("init: add schema for extension")),
                },
                Operation {
                    query: String::from("CREATE EXTENSION IF NOT EXISTS neon WITH SCHEMA neon"),
                    comment: Some(String::from(
                        "init: install the extension if not already installed",
                    )),
                },
                Operation {
                    query: String::from(
                        "UPDATE pg_extension SET extrelocatable = true WHERE extname = 'neon'",
                    ),
                    comment: Some(String::from("compat/fix: make neon relocatable")),
                },
                Operation {
                    query: String::from("ALTER EXTENSION neon SET SCHEMA neon"),
                    comment: Some(String::from("compat/fix: alter neon extension schema")),
                },
                Operation {
                    query: String::from("ALTER EXTENSION neon UPDATE"),
                    comment: Some(String::from("compat/update: update neon extension version")),
                },
            ]
            .into_iter();

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateAvailabilityCheck => Ok(Box::new(once(Operation {
            query: String::from(include_str!("sql/add_availabilitycheck_tables.sql")),
            comment: None,
        }))),
        ApplySpecPhase::DropRoles => {
            let operations = spec
                .delta_operations
                .iter()
                .flatten()
                .filter(|op| op.action == "delete_role")
                .map(|op| Operation {
                    query: format!("DROP ROLE IF EXISTS {}", op.name.pg_quote()),
                    comment: None,
                });

            Ok(Box::new(operations))
        }
    }
}
