use anyhow::{anyhow, bail, Result};
use postgres::Client;
use reqwest::StatusCode;
use std::fs::File;
use std::path::Path;
use tracing::{error, info, instrument, warn};

use crate::config;
use crate::metrics::{CPlaneRequestRPC, CPLANE_REQUESTS_TOTAL, UNKNOWN_HTTP_STATUS};
use crate::migration::MigrationRunner;
use crate::params::PG_HBA_ALL_MD5;
use crate::pg_helpers::*;

use compute_api::responses::{ControlPlaneComputeStatus, ControlPlaneSpecResponse};
use compute_api::spec::ComputeSpec;

// Do control plane request and return response if any. In case of error it
// returns a bool flag indicating whether it makes sense to retry the request
// and a string with error message.
fn do_control_plane_request(
    uri: &str,
    jwt: &str,
) -> Result<ControlPlaneSpecResponse, (bool, String, String)> {
    let resp = reqwest::blocking::Client::new()
        .get(uri)
        .header("Authorization", format!("Bearer {}", jwt))
        .send()
        .map_err(|e| {
            (
                true,
                format!("could not perform spec request to control plane: {:?}", e),
                UNKNOWN_HTTP_STATUS.to_string(),
            )
        })?;

    let status = resp.status();
    match status {
        StatusCode::OK => match resp.json::<ControlPlaneSpecResponse>() {
            Ok(spec_resp) => Ok(spec_resp),
            Err(e) => Err((
                true,
                format!("could not deserialize control plane response: {:?}", e),
                status.to_string(),
            )),
        },
        StatusCode::SERVICE_UNAVAILABLE => Err((
            true,
            "control plane is temporarily unavailable".to_string(),
            status.to_string(),
        )),
        StatusCode::BAD_GATEWAY => {
            // We have a problem with intermittent 502 errors now
            // https://github.com/neondatabase/cloud/issues/2353
            // It's fine to retry GET request in this case.
            Err((
                true,
                "control plane request failed with 502".to_string(),
                status.to_string(),
            ))
        }
        // Another code, likely 500 or 404, means that compute is unknown to the control plane
        // or some internal failure happened. Doesn't make much sense to retry in this case.
        _ => Err((
            false,
            format!("unexpected control plane response status code: {}", status),
            status.to_string(),
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
            Ok(spec_resp) => {
                CPLANE_REQUESTS_TOTAL
                    .with_label_values(&[
                        CPlaneRequestRPC::GetSpec.as_str(),
                        &StatusCode::OK.to_string(),
                    ])
                    .inc();
                match spec_resp.status {
                    ControlPlaneComputeStatus::Empty => Ok(None),
                    ControlPlaneComputeStatus::Attached => {
                        if let Some(spec) = spec_resp.spec {
                            Ok(Some(spec))
                        } else {
                            bail!("compute is attached, but spec is empty")
                        }
                    }
                }
            }
            Err((retry, msg, status)) => {
                CPLANE_REQUESTS_TOTAL
                    .with_label_values(&[CPlaneRequestRPC::GetSpec.as_str(), &status])
                    .inc();
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

#[instrument(skip_all)]
pub fn handle_neon_extension_upgrade(client: &mut Client) -> Result<()> {
    info!("handle neon extension upgrade");
    let query = "ALTER EXTENSION neon UPDATE";
    info!("update neon extension version with query: {}", query);
    client.simple_query(query)?;

    Ok(())
}

#[instrument(skip_all)]
pub fn handle_migrations(client: &mut Client) -> Result<()> {
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

    MigrationRunner::new(client, &migrations).run_migrations()?;

    Ok(())
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
