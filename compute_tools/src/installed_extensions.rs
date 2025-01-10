use compute_api::responses::{InstalledExtension, InstalledExtensions};
use metrics::proto::MetricFamily;
use std::collections::HashMap;

use anyhow::Result;
use postgres::{Client, NoTls};

use metrics::core::Collector;
use metrics::{register_uint_gauge_vec, UIntGaugeVec};
use once_cell::sync::Lazy;

/// We don't reuse get_existing_dbs() just for code clarity
/// and to make database listing query here more explicit.
///
/// Limit the number of databases to 500 to avoid excessive load.
fn list_dbs(client: &mut Client) -> Result<Vec<String>> {
    // `pg_database.datconnlimit = -2` means that the database is in the
    // invalid state
    let databases = client
        .query(
            "SELECT datname FROM pg_catalog.pg_database
                WHERE datallowconn
                AND datconnlimit <> - 2
                LIMIT 500",
            &[],
        )?
        .iter()
        .map(|row| {
            let db: String = row.get("datname");
            db
        })
        .collect();

    Ok(databases)
}

/// Connect to every database (see list_dbs above) and get the list of installed extensions.
///
/// Same extension can be installed in multiple databases with different versions,
/// so we report a separate metric (number of databases where it is installed)
/// for each extension version.
pub fn get_installed_extensions(mut conf: postgres::config::Config) -> Result<InstalledExtensions> {
    conf.application_name("compute_ctl:get_installed_extensions");
    let mut client = conf.connect(NoTls)?;
    let databases: Vec<String> = list_dbs(&mut client)?;

    let mut extensions_map: HashMap<(String, String, String), InstalledExtension> = HashMap::new();
    for db in databases.iter() {
        conf.dbname(db);
        let mut db_client = conf.connect(NoTls)?;
        let extensions: Vec<(String, String, i32)> = db_client
            .query(
                "SELECT extname, extversion, extowner::integer FROM pg_catalog.pg_extension",
                &[],
            )?
            .iter()
            .map(|row| {
                (
                    row.get("extname"),
                    row.get("extversion"),
                    row.get("extowner"),
                )
            })
            .collect();

        for (extname, v, extowner) in extensions.iter() {
            let version = v.to_string();

            // check if the extension is owned by superuser
            // 10 is the oid of superuser
            let owned_by_superuser = if *extowner == 10 { "1" } else { "0" };

            extensions_map
                .entry((
                    extname.to_string(),
                    version.clone(),
                    owned_by_superuser.to_string(),
                ))
                .and_modify(|e| {
                    // count the number of databases where the extension is installed
                    e.n_databases += 1;
                })
                .or_insert(InstalledExtension {
                    extname: extname.to_string(),
                    version: version.clone(),
                    n_databases: 1,
                    owned_by_superuser: owned_by_superuser.to_string(),
                });
        }
    }

    for (key, ext) in extensions_map.iter() {
        let (extname, version, owned_by_superuser) = key;
        let n_databases = ext.n_databases as u64;

        INSTALLED_EXTENSIONS
            .with_label_values(&[extname, version, owned_by_superuser])
            .set(n_databases);
    }

    Ok(InstalledExtensions {
        extensions: extensions_map.into_values().collect(),
    })
}

static INSTALLED_EXTENSIONS: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "compute_installed_extensions",
        "Number of databases where the version of extension is installed",
        &["extension_name", "version", "owned_by_superuser"]
    )
    .expect("failed to define a metric")
});

pub fn collect() -> Vec<MetricFamily> {
    INSTALLED_EXTENSIONS.collect()
}
