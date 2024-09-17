use std::fs::File;
use std::path::Path;
use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use postgres::config::Config;
use postgres::{Client, NoTls};
use reqwest::StatusCode;
use tracing::{error, info, info_span, instrument, span_enabled, warn, Level};

use crate::config;
use crate::logger::inlinify;
use crate::migration::MigrationRunner;
use crate::params::PG_HBA_ALL_MD5;
use crate::pg_helpers::*;

use compute_api::responses::{ControlPlaneComputeStatus, ControlPlaneSpecResponse};
use compute_api::spec::{ComputeSpec, PgIdent, Role};

// Do control plane request and return response if any. In case of error it
// returns a bool flag indicating whether it makes sense to retry the request
// and a string with error message.
fn do_control_plane_request(
    uri: &str,
    jwt: &str,
) -> Result<ControlPlaneSpecResponse, (bool, String)> {
    let resp = reqwest::blocking::Client::new()
        .get(uri)
        .header("Authorization", format!("Bearer {}", jwt))
        .send()
        .map_err(|e| {
            (
                true,
                format!("could not perform spec request to control plane: {}", e),
            )
        })?;

    match resp.status() {
        StatusCode::OK => match resp.json::<ControlPlaneSpecResponse>() {
            Ok(spec_resp) => Ok(spec_resp),
            Err(e) => Err((
                true,
                format!("could not deserialize control plane response: {}", e),
            )),
        },
        StatusCode::SERVICE_UNAVAILABLE => {
            Err((true, "control plane is temporarily unavailable".to_string()))
        }
        StatusCode::BAD_GATEWAY => {
            // We have a problem with intermittent 502 errors now
            // https://github.com/neondatabase/cloud/issues/2353
            // It's fine to retry GET request in this case.
            Err((true, "control plane request failed with 502".to_string()))
        }
        // Another code, likely 500 or 404, means that compute is unknown to the control plane
        // or some internal failure happened. Doesn't make much sense to retry in this case.
        _ => Err((
            false,
            format!(
                "unexpected control plane response status code: {}",
                resp.status()
            ),
        )),
    }
}

/// Request spec from the control-plane by compute_id. If `NEON_CONTROL_PLANE_TOKEN`
/// env variable is set, it will be used for authorization.
pub fn get_spec_from_control_plane(
    base_uri: &str,
    compute_id: &str,
) -> Result<Option<ComputeSpec>> {
    let cp_uri = format!("{base_uri}/compute/api/v2/computes/{compute_id}/spec");
    let jwt: String = match std::env::var("NEON_CONTROL_PLANE_TOKEN") {
        Ok(v) => v,
        Err(_) => "".to_string(),
    };
    let mut attempt = 1;
    let mut spec: Result<Option<ComputeSpec>> = Ok(None);

    info!("getting spec from control plane: {}", cp_uri);

    // Do 3 attempts to get spec from the control plane using the following logic:
    // - network error -> then retry
    // - compute id is unknown or any other error -> bail out
    // - no spec for compute yet (Empty state) -> return Ok(None)
    // - got spec -> return Ok(Some(spec))
    while attempt < 4 {
        spec = match do_control_plane_request(&cp_uri, &jwt) {
            Ok(spec_resp) => match spec_resp.status {
                ControlPlaneComputeStatus::Empty => Ok(None),
                ControlPlaneComputeStatus::Attached => {
                    if let Some(spec) = spec_resp.spec {
                        Ok(Some(spec))
                    } else {
                        bail!("compute is attached, but spec is empty")
                    }
                }
            },
            Err((retry, msg)) => {
                if retry {
                    Err(anyhow!(msg))
                } else {
                    bail!(msg);
                }
            }
        };

        if let Err(e) = &spec {
            error!("attempt {} to get spec failed with: {}", attempt, e);
        } else {
            return spec;
        }

        attempt += 1;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // All attempts failed, return error.
    spec
}

/// Check `pg_hba.conf` and update if needed to allow external connections.
pub fn update_pg_hba(pgdata_path: &Path) -> Result<()> {
    // XXX: consider making it a part of spec.json
    info!("checking pg_hba.conf");
    let pghba_path = pgdata_path.join("pg_hba.conf");

    if config::line_in_file(&pghba_path, PG_HBA_ALL_MD5)? {
        info!("updated pg_hba.conf to allow external connections");
    } else {
        info!("pg_hba.conf is up-to-date");
    }

    Ok(())
}

/// Create a standby.signal file
pub fn add_standby_signal(pgdata_path: &Path) -> Result<()> {
    // XXX: consider making it a part of spec.json
    info!("adding standby.signal");
    let signalfile = pgdata_path.join("standby.signal");

    if !signalfile.exists() {
        info!("created standby.signal");
        File::create(signalfile)?;
    } else {
        info!("reused pre-existing standby.signal");
    }
    Ok(())
}

/// Compute could be unexpectedly shut down, for example, during the
/// database dropping. This leaves the database in the invalid state,
/// which prevents new db creation with the same name. This function
/// will clean it up before proceeding with catalog updates. All
/// possible future cleanup operations may go here too.
#[instrument(skip_all)]
pub fn cleanup_instance(client: &mut Client) -> Result<()> {
    let existing_dbs = get_existing_dbs(client)?;

    for (_, db) in existing_dbs {
        if db.invalid {
            // After recent commit in Postgres, interrupted DROP DATABASE
            // leaves the database in the invalid state. According to the
            // commit message, the only option for user is to drop it again.
            // See:
            //   https://github.com/postgres/postgres/commit/a4b4cc1d60f7e8ccfcc8ff8cb80c28ee411ad9a9
            //
            // Postgres Neon extension is done the way, that db is de-registered
            // in the control plane metadata only after it is dropped. So there is
            // a chance that it still thinks that db should exist. This means
            // that it will be re-created by `handle_databases()`. Yet, it's fine
            // as user can just repeat drop (in vanilla Postgres they would need
            // to do the same, btw).
            let query = format!("DROP DATABASE IF EXISTS {}", db.name.pg_quote());
            info!("dropping invalid database {}", db.name);
            client.execute(query.as_str(), &[])?;
        }
    }

    Ok(())
}

/// Given a cluster spec json and open transaction it handles roles creation,
/// deletion and update.
#[instrument(skip_all)]
pub fn handle_roles(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
    let mut xact = client.transaction()?;
    let existing_roles: Vec<Role> = get_existing_roles(&mut xact)?;

    // Print a list of existing Postgres roles (only in debug mode)
    if span_enabled!(Level::INFO) {
        let mut vec = Vec::new();
        for r in &existing_roles {
            vec.push(format!(
                "{}:{}",
                r.name,
                if r.encrypted_password.is_some() {
                    "[FILTERED]"
                } else {
                    "(null)"
                }
            ));
        }

        info!("postgres roles (total {}): {:?}", vec.len(), vec);
    }

    // Process delta operations first
    if let Some(ops) = &spec.delta_operations {
        info!("processing role renames");
        for op in ops {
            match op.action.as_ref() {
                "delete_role" => {
                    // no-op now, roles will be deleted at the end of configuration
                }
                // Renaming role drops its password, since role name is
                // used as a salt there.  It is important that this role
                // is recorded with a new `name` in the `roles` list.
                // Follow up roles update will set the new password.
                "rename_role" => {
                    let new_name = op.new_name.as_ref().unwrap();

                    // XXX: with a limited number of roles it is fine, but consider making it a HashMap
                    if existing_roles.iter().any(|r| r.name == op.name) {
                        let query: String = format!(
                            "ALTER ROLE {} RENAME TO {}",
                            op.name.pg_quote(),
                            new_name.pg_quote()
                        );

                        warn!("renaming role '{}' to '{}'", op.name, new_name);
                        xact.execute(query.as_str(), &[])?;
                    }
                }
                _ => {}
            }
        }
    }

    // Refresh Postgres roles info to handle possible roles renaming
    let existing_roles: Vec<Role> = get_existing_roles(&mut xact)?;

    info!(
        "handling cluster spec roles (total {})",
        spec.cluster.roles.len()
    );
    for role in &spec.cluster.roles {
        let name = &role.name;
        // XXX: with a limited number of roles it is fine, but consider making it a HashMap
        let pg_role = existing_roles.iter().find(|r| r.name == *name);

        enum RoleAction {
            None,
            Update,
            Create,
        }
        let action = if let Some(r) = pg_role {
            if (r.encrypted_password.is_none() && role.encrypted_password.is_some())
                || (r.encrypted_password.is_some() && role.encrypted_password.is_none())
            {
                RoleAction::Update
            } else if let Some(pg_pwd) = &r.encrypted_password {
                // Check whether password changed or not (trim 'md5' prefix first if any)
                //
                // This is a backward compatibility hack, which comes from the times when we were using
                // md5 for everyone and hashes were stored in the console db without md5 prefix. So when
                // role comes from the control-plane (json spec) `Role.encrypted_password` doesn't have md5 prefix,
                // but when role comes from Postgres (`get_existing_roles` / `existing_roles`) it has this prefix.
                // Here is the only place so far where we compare hashes, so it seems to be the best candidate
                // to place this compatibility layer.
                let pg_pwd = if let Some(stripped) = pg_pwd.strip_prefix("md5") {
                    stripped
                } else {
                    pg_pwd
                };
                if pg_pwd != *role.encrypted_password.as_ref().unwrap() {
                    RoleAction::Update
                } else {
                    RoleAction::None
                }
            } else {
                RoleAction::None
            }
        } else {
            RoleAction::Create
        };

        match action {
            RoleAction::None => {}
            RoleAction::Update => {
                // This can be run on /every/ role! Not just ones created through the console.
                // This means that if you add some funny ALTER here that adds a permission,
                // this will get run even on user-created roles! This will result in different
                // behavior before and after a spec gets reapplied. The below ALTER as it stands
                // now only grants LOGIN and changes the password. Please do not allow this branch
                // to do anything silly.
                let mut query: String = format!("ALTER ROLE {} ", name.pg_quote());
                query.push_str(&role.to_pg_options());
                xact.execute(query.as_str(), &[])?;
            }
            RoleAction::Create => {
                // This branch only runs when roles are created through the console, so it is
                // safe to add more permissions here. BYPASSRLS and REPLICATION are inherited
                // from neon_superuser.
                let mut query: String = format!(
                    "CREATE ROLE {} INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser",
                    name.pg_quote()
                );
                info!("running role create query: '{}'", &query);
                query.push_str(&role.to_pg_options());
                xact.execute(query.as_str(), &[])?;
            }
        }

        if span_enabled!(Level::INFO) {
            let pwd = if role.encrypted_password.is_some() {
                "[FILTERED]"
            } else {
                "(null)"
            };
            let action_str = match action {
                RoleAction::None => "",
                RoleAction::Create => " -> create",
                RoleAction::Update => " -> update",
            };
            info!(" - {}:{}{}", name, pwd, action_str);
        }
    }

    xact.commit()?;

    Ok(())
}

/// Reassign all dependent objects and delete requested roles.
#[instrument(skip_all)]
pub fn handle_role_deletions(spec: &ComputeSpec, connstr: &str, client: &mut Client) -> Result<()> {
    if let Some(ops) = &spec.delta_operations {
        // First, reassign all dependent objects to db owners.
        info!("reassigning dependent objects of to-be-deleted roles");

        // Fetch existing roles. We could've exported and used `existing_roles` from
        // `handle_roles()`, but we only make this list there before creating new roles.
        // Which is probably fine as we never create to-be-deleted roles, but that'd
        // just look a bit untidy. Anyway, the entire `pg_roles` should be in shared
        // buffers already, so this shouldn't be a big deal.
        let mut xact = client.transaction()?;
        let existing_roles: Vec<Role> = get_existing_roles(&mut xact)?;
        xact.commit()?;

        for op in ops {
            // Check that role is still present in Postgres, as this could be a
            // restart with the same spec after role deletion.
            if op.action == "delete_role" && existing_roles.iter().any(|r| r.name == op.name) {
                reassign_owned_objects(spec, connstr, &op.name)?;
            }
        }

        // Second, proceed with role deletions.
        info!("processing role deletions");
        let mut xact = client.transaction()?;
        for op in ops {
            // We do not check either role exists or not,
            // Postgres will take care of it for us
            if op.action == "delete_role" {
                let query: String = format!("DROP ROLE IF EXISTS {}", &op.name.pg_quote());

                warn!("deleting role '{}'", &op.name);
                xact.execute(query.as_str(), &[])?;
            }
        }
        xact.commit()?;
    }

    Ok(())
}

fn reassign_owned_objects_in_one_db(
    conf: Config,
    role_name: &PgIdent,
    db_owner: &PgIdent,
) -> Result<()> {
    let mut client = conf.connect(NoTls)?;

    // This will reassign all dependent objects to the db owner
    let reassign_query = format!(
        "REASSIGN OWNED BY {} TO {}",
        role_name.pg_quote(),
        db_owner.pg_quote()
    );
    info!(
        "reassigning objects owned by '{}' in db '{}' to '{}'",
        role_name,
        conf.get_dbname().unwrap_or(""),
        db_owner
    );
    client.simple_query(&reassign_query)?;

    // This now will only drop privileges of the role
    let drop_query = format!("DROP OWNED BY {}", role_name.pg_quote());
    client.simple_query(&drop_query)?;
    Ok(())
}

// Reassign all owned objects in all databases to the owner of the database.
fn reassign_owned_objects(spec: &ComputeSpec, connstr: &str, role_name: &PgIdent) -> Result<()> {
    for db in &spec.cluster.databases {
        if db.owner != *role_name {
            let mut conf = Config::from_str(connstr)?;
            conf.dbname(&db.name);
            reassign_owned_objects_in_one_db(conf, role_name, &db.owner)?;
        }
    }

    // Also handle case when there are no databases in the spec.
    // In this case we need to reassign objects in the default database.
    let conf = Config::from_str(connstr)?;
    let db_owner = PgIdent::from_str("cloud_admin")?;
    reassign_owned_objects_in_one_db(conf, role_name, &db_owner)?;

    Ok(())
}

/// It follows mostly the same logic as `handle_roles()` excepting that we
/// does not use an explicit transactions block, since major database operations
/// like `CREATE DATABASE` and `DROP DATABASE` do not support it. Statement-level
/// atomicity should be enough here due to the order of operations and various checks,
/// which together provide us idempotency.
#[instrument(skip_all)]
pub fn handle_databases(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
    let existing_dbs = get_existing_dbs(client)?;

    // Print a list of existing Postgres databases (only in debug mode)
    if span_enabled!(Level::INFO) {
        let mut vec = Vec::new();
        for (dbname, db) in &existing_dbs {
            vec.push(format!("{}:{}", dbname, db.owner));
        }
        info!("postgres databases (total {}): {:?}", vec.len(), vec);
    }

    // Process delta operations first
    if let Some(ops) = &spec.delta_operations {
        info!("processing delta operations on databases");
        for op in ops {
            match op.action.as_ref() {
                // We do not check either DB exists or not,
                // Postgres will take care of it for us
                "delete_db" => {
                    // In Postgres we can't drop a database if it is a template.
                    // So we need to unset the template flag first, but it could
                    // be a retry, so we could've already dropped the database.
                    // Check that database exists first to make it idempotent.
                    let unset_template_query: String = format!(
                        "
                        DO $$
                        BEGIN
                            IF EXISTS(
                                SELECT 1
                                FROM pg_catalog.pg_database
                                WHERE datname = {}
                            )
                            THEN
                            ALTER DATABASE {} is_template false;
                            END IF;
                        END
                        $$;",
                        escape_literal(&op.name),
                        &op.name.pg_quote()
                    );
                    // Use FORCE to drop database even if there are active connections.
                    // We run this from `cloud_admin`, so it should have enough privileges.
                    // NB: there could be other db states, which prevent us from dropping
                    // the database. For example, if db is used by any active subscription
                    // or replication slot.
                    // TODO: deal with it once we allow logical replication. Proper fix should
                    // involve returning an error code to the control plane, so it could
                    // figure out that this is a non-retryable error, return it to the user
                    // and fail operation permanently.
                    let drop_db_query: String = format!(
                        "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                        &op.name.pg_quote()
                    );

                    warn!("deleting database '{}'", &op.name);
                    client.execute(unset_template_query.as_str(), &[])?;
                    client.execute(drop_db_query.as_str(), &[])?;
                }
                "rename_db" => {
                    let new_name = op.new_name.as_ref().unwrap();

                    if existing_dbs.contains_key(&op.name) {
                        let query: String = format!(
                            "ALTER DATABASE {} RENAME TO {}",
                            op.name.pg_quote(),
                            new_name.pg_quote()
                        );

                        warn!("renaming database '{}' to '{}'", op.name, new_name);
                        client.execute(query.as_str(), &[])?;
                    }
                }
                _ => {}
            }
        }
    }

    // Refresh Postgres databases info to handle possible renames
    let existing_dbs = get_existing_dbs(client)?;

    info!(
        "handling cluster spec databases (total {})",
        spec.cluster.databases.len()
    );
    for db in &spec.cluster.databases {
        let name = &db.name;
        let pg_db = existing_dbs.get(name);

        enum DatabaseAction {
            None,
            Update,
            Create,
        }
        let action = if let Some(r) = pg_db {
            // XXX: db owner name is returned as quoted string from Postgres,
            // when quoting is needed.
            let new_owner = if r.owner.starts_with('"') {
                db.owner.pg_quote()
            } else {
                db.owner.clone()
            };

            if new_owner != r.owner {
                // Update the owner
                DatabaseAction::Update
            } else {
                DatabaseAction::None
            }
        } else {
            DatabaseAction::Create
        };

        match action {
            DatabaseAction::None => {}
            DatabaseAction::Update => {
                let query: String = format!(
                    "ALTER DATABASE {} OWNER TO {}",
                    name.pg_quote(),
                    db.owner.pg_quote()
                );
                let _guard = info_span!("executing", query).entered();
                client.execute(query.as_str(), &[])?;
            }
            DatabaseAction::Create => {
                let mut query: String = format!("CREATE DATABASE {} ", name.pg_quote());
                query.push_str(&db.to_pg_options());
                let _guard = info_span!("executing", query).entered();
                client.execute(query.as_str(), &[])?;
                let grant_query: String = format!(
                    "GRANT ALL PRIVILEGES ON DATABASE {} TO neon_superuser",
                    name.pg_quote()
                );
                client.execute(grant_query.as_str(), &[])?;
            }
        };

        if span_enabled!(Level::INFO) {
            let action_str = match action {
                DatabaseAction::None => "",
                DatabaseAction::Create => " -> create",
                DatabaseAction::Update => " -> update",
            };
            info!(" - {}:{}{}", db.name, db.owner, action_str);
        }
    }

    Ok(())
}

/// Grant CREATE ON DATABASE to the database owner and do some other alters and grants
/// to allow users creating trusted extensions and re-creating `public` schema, for example.
#[instrument(skip_all)]
pub fn handle_grants(
    spec: &ComputeSpec,
    client: &mut Client,
    connstr: &str,
    enable_anon_extension: bool,
) -> Result<()> {
    info!("modifying database permissions");
    let existing_dbs = get_existing_dbs(client)?;

    // Do some per-database access adjustments. We'd better do this at db creation time,
    // but CREATE DATABASE isn't transactional. So we cannot create db + do some grants
    // atomically.
    for db in &spec.cluster.databases {
        match existing_dbs.get(&db.name) {
            Some(pg_db) => {
                if pg_db.restrict_conn || pg_db.invalid {
                    info!(
                        "skipping grants for db {} (invalid: {}, connections not allowed: {})",
                        db.name, pg_db.invalid, pg_db.restrict_conn
                    );
                    continue;
                }
            }
            None => {
                bail!(
                    "database {} doesn't exist in Postgres after handle_databases()",
                    db.name
                );
            }
        }

        let mut conf = Config::from_str(connstr)?;
        conf.dbname(&db.name);

        let mut db_client = conf.connect(NoTls)?;

        // This will only change ownership on the schema itself, not the objects
        // inside it. Without it owner of the `public` schema will be `cloud_admin`
        // and database owner cannot do anything with it. SQL procedure ensures
        // that it won't error out if schema `public` doesn't exist.
        let alter_query = format!(
            "DO $$\n\
                DECLARE\n\
                    schema_owner TEXT;\n\
                BEGIN\n\
                    IF EXISTS(\n\
                        SELECT nspname\n\
                        FROM pg_catalog.pg_namespace\n\
                        WHERE nspname = 'public'\n\
                    )\n\
                    THEN\n\
                        SELECT nspowner::regrole::text\n\
                            FROM pg_catalog.pg_namespace\n\
                            WHERE nspname = 'public'\n\
                            INTO schema_owner;\n\
                \n\
                        IF schema_owner = 'cloud_admin' OR schema_owner = 'zenith_admin'\n\
                        THEN\n\
                            ALTER SCHEMA public OWNER TO {};\n\
                        END IF;\n\
                    END IF;\n\
                END\n\
            $$;",
            db.owner.pg_quote()
        );
        db_client.simple_query(&alter_query)?;

        // Explicitly grant CREATE ON SCHEMA PUBLIC to the web_access user.
        // This is needed because since postgres 15 this privilege is removed by default.
        // TODO: web_access isn't created for almost 1 year. It could be that we have
        // active users of 1 year old projects, but hopefully not, so check it and
        // remove this code if possible. The worst thing that could happen is that
        // user won't be able to use public schema in NEW databases created in the
        // very OLD project.
        //
        // Also, alter default permissions so that relations created by extensions can be
        // used by neon_superuser without permission issues.
        let grant_query = "DO $$\n\
                BEGIN\n\
                    IF EXISTS(\n\
                        SELECT nspname\n\
                        FROM pg_catalog.pg_namespace\n\
                        WHERE nspname = 'public'\n\
                    ) AND\n\
                    current_setting('server_version_num')::int/10000 >= 15\n\
                    THEN\n\
                        IF EXISTS(\n\
                            SELECT rolname\n\
                            FROM pg_catalog.pg_roles\n\
                            WHERE rolname = 'web_access'\n\
                        )\n\
                        THEN\n\
                            GRANT CREATE ON SCHEMA public TO web_access;\n\
                        END IF;\n\
                    END IF;\n\
                    IF EXISTS(\n\
                        SELECT nspname\n\
                        FROM pg_catalog.pg_namespace\n\
                        WHERE nspname = 'public'\n\
                    )\n\
                    THEN\n\
                        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO neon_superuser WITH GRANT OPTION;\n\
                        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO neon_superuser WITH GRANT OPTION;\n\
                    END IF;\n\
                END\n\
            $$;"
        .to_string();

        info!(
            "grant query for db {} : {}",
            &db.name,
            inlinify(&grant_query)
        );
        db_client.simple_query(&grant_query)?;

        // it is important to run this after all grants
        if enable_anon_extension {
            handle_extension_anon(spec, &db.owner, &mut db_client, false)
                .context("handle_grants handle_extension_anon")?;
        }
    }

    Ok(())
}

/// Create required system extensions
#[instrument(skip_all)]
pub fn handle_extensions(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
    if let Some(libs) = spec.cluster.settings.find("shared_preload_libraries") {
        if libs.contains("pg_stat_statements") {
            // Create extension only if this compute really needs it
            let query = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements";
            info!("creating system extensions with query: {}", query);
            client.simple_query(query)?;
        }
    }

    Ok(())
}

/// Run CREATE and ALTER EXTENSION neon UPDATE for postgres database
#[instrument(skip_all)]
pub fn handle_extension_neon(client: &mut Client) -> Result<()> {
    info!("handle extension neon");

    let mut query = "CREATE SCHEMA IF NOT EXISTS neon";
    client.simple_query(query)?;

    query = "CREATE EXTENSION IF NOT EXISTS neon WITH SCHEMA neon";
    info!("create neon extension with query: {}", query);
    client.simple_query(query)?;

    query = "UPDATE pg_extension SET extrelocatable = true WHERE extname = 'neon'";
    client.simple_query(query)?;

    query = "ALTER EXTENSION neon SET SCHEMA neon";
    info!("alter neon extension schema with query: {}", query);
    client.simple_query(query)?;

    // this will be a no-op if extension is already up to date,
    // which may happen in two cases:
    // - extension was just installed
    // - extension was already installed and is up to date
    let query = "ALTER EXTENSION neon UPDATE";
    info!("update neon extension version with query: {}", query);
    if let Err(e) = client.simple_query(query) {
        error!(
            "failed to upgrade neon extension during `handle_extension_neon`: {}",
            e
        );
    }

    Ok(())
}

#[instrument(skip_all)]
pub fn handle_neon_extension_upgrade(client: &mut Client) -> Result<()> {
    info!("handle neon extension upgrade");
    let query = "ALTER EXTENSION neon UPDATE";
    info!("update neon extension version with query: {}", query);
    client.simple_query(query)?;

    Ok(())
}

#[instrument(skip_all)]
pub fn handle_migrations(client: &mut Client) -> Result<(), postgres::Error> {
    info!("handle migrations");

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // !BE SURE TO ONLY ADD MIGRATIONS TO THE END OF THIS ARRAY. IF YOU DO NOT, VERY VERY BAD THINGS MAY HAPPEN!
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // Add new migrations in numerical order.
    let migrations = [
        include_str!("./migrations/0001-neon_superuser_bypass_rls.sql"),
        include_str!("./migrations/0002-alter_roles.sql"),
        include_str!("./migrations/0003-grant_pg_create_subscription_to_neon_superuser.sql"),
        include_str!("./migrations/0004-grant_pg_monitor_to_neon_superuser.sql"),
        include_str!("./migrations/0005-grant_all_on_tables_to_neon_superuser.sql"),
        include_str!("./migrations/0006-grant_all_on_sequences_to_neon_superuser.sql"),
        include_str!(
            "./migrations/0007-grant_all_on_tables_to_neon_superuser_with_grant_option.sql"
        ),
        include_str!(
            "./migrations/0008-grant_all_on_sequences_to_neon_superuser_with_grant_option.sql"
        ),
        include_str!("./migrations/0009-revoke_replication_for_previously_allowed_roles.sql"),
        include_str!(
            "./migrations/0010-grant_snapshot_synchronization_funcs_to_neon_superuser.sql"
        ),
        include_str!(
            "./migrations/0011-grant_pg_show_replication_origin_status_to_neon_superuser.sql"
        ),
    ];

    let runner = MigrationRunner::new(client, &migrations);
    match runner.run_migrations() {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("Failed to run the migrations: {}", e);
            Err(e)
        }
    }
}

/// Connect to the database as superuser and pre-create anon extension
/// if it is present in shared_preload_libraries
#[instrument(skip_all)]
pub fn handle_extension_anon(
    spec: &ComputeSpec,
    db_owner: &str,
    db_client: &mut Client,
    grants_only: bool,
) -> Result<()> {
    info!("handle extension anon");

    if let Some(libs) = spec.cluster.settings.find("shared_preload_libraries") {
        if libs.contains("anon") {
            if !grants_only {
                // check if extension is already initialized using anon.is_initialized()
                let query = "SELECT anon.is_initialized()";
                match db_client.query(query, &[]) {
                    Ok(rows) => {
                        if !rows.is_empty() {
                            let is_initialized: bool = rows[0].get(0);
                            if is_initialized {
                                info!("anon extension is already initialized");
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "anon extension is_installed check failed with expected error: {}",
                            e
                        );
                    }
                };

                // Create anon extension if this compute needs it
                // Users cannot create it themselves, because superuser is required.
                let mut query = "CREATE EXTENSION IF NOT EXISTS anon CASCADE";
                info!("creating anon extension with query: {}", query);
                match db_client.query(query, &[]) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("anon extension creation failed with error: {}", e);
                        return Ok(());
                    }
                }

                // check that extension is installed
                query = "SELECT extname FROM pg_extension WHERE extname = 'anon'";
                let rows = db_client.query(query, &[])?;
                if rows.is_empty() {
                    error!("anon extension is not installed");
                    return Ok(());
                }

                // Initialize anon extension
                // This also requires superuser privileges, so users cannot do it themselves.
                query = "SELECT anon.init()";
                match db_client.query(query, &[]) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("anon.init() failed with error: {}", e);
                        return Ok(());
                    }
                }
            }

            // check that extension is installed, if not bail early
            let query = "SELECT extname FROM pg_extension WHERE extname = 'anon'";
            match db_client.query(query, &[]) {
                Ok(rows) => {
                    if rows.is_empty() {
                        error!("anon extension is not installed");
                        return Ok(());
                    }
                }
                Err(e) => {
                    error!("anon extension check failed with error: {}", e);
                    return Ok(());
                }
            };

            let query = format!("GRANT ALL ON SCHEMA anon TO {}", db_owner);
            info!("granting anon extension permissions with query: {}", query);
            db_client.simple_query(&query)?;

            // Grant permissions to db_owner to use anon extension functions
            let query = format!("GRANT ALL ON ALL FUNCTIONS IN SCHEMA anon TO {}", db_owner);
            info!("granting anon extension permissions with query: {}", query);
            db_client.simple_query(&query)?;

            // This is needed, because some functions are defined as SECURITY DEFINER.
            // In Postgres SECURITY DEFINER functions are executed with the privileges
            // of the owner.
            // In anon extension this it is needed to access some GUCs, which are only accessible to
            // superuser. But we've patched postgres to allow db_owner to access them as well.
            // So we need to change owner of these functions to db_owner.
            let query = format!("
                SELECT 'ALTER FUNCTION '||nsp.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||') OWNER TO {};'
                from pg_proc p
                join pg_namespace nsp ON p.pronamespace = nsp.oid
                where nsp.nspname = 'anon';", db_owner);

            info!("change anon extension functions owner to db owner");
            db_client.simple_query(&query)?;

            //  affects views as well
            let query = format!("GRANT ALL ON ALL TABLES IN SCHEMA anon TO {}", db_owner);
            info!("granting anon extension permissions with query: {}", query);
            db_client.simple_query(&query)?;

            let query = format!("GRANT ALL ON ALL SEQUENCES IN SCHEMA anon TO {}", db_owner);
            info!("granting anon extension permissions with query: {}", query);
            db_client.simple_query(&query)?;
        }
    }

    Ok(())
}
