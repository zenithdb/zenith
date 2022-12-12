//! This module is responsible for locating and loading paths in a local setup.
//!
//! Now it also provides init method which acts like a stub for proper installation
//! script which will use local paths.

use anyhow::{bail, ensure, Context};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use utils::{
    auth::{encode_from_key_file, Claims, Scope},
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    postgres_backend::AuthType,
};

use crate::safekeeper::SafekeeperNode;

pub const DEFAULT_PG_VERSION: u32 = 14;

//
// This data structures represents neon_local CLI config
//
// It is deserialized from the .neon/config file, or the config file passed
// to 'neon_local init --config=<path>' option. See control_plane/simple.conf for
// an example.
//
#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct LocalEnv {
    // Base directory for all the nodes (the pageserver, safekeepers and
    // compute nodes).
    //
    // This is not stored in the config file. Rather, this is the path where the
    // config file itself is. It is read from the NEON_REPO_DIR env variable or
    // '.neon' if not given.
    #[serde(skip)]
    pub base_data_dir: PathBuf,

    // Path to postgres distribution. It's expected that "bin", "include",
    // "lib", "share" from postgres distribution are there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    #[serde(default)]
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary.
    #[serde(default)]
    pub neon_distrib_dir: PathBuf,

    // Default tenant ID to use with the 'neon_local' command line utility, when
    // --tenant_id is not explicitly specified.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub default_tenant_id: Option<TenantId>,

    // used to issue tokens during e.g pg start
    #[serde(default)]
    pub private_key_path: PathBuf,

    pub etcd_broker: EtcdBroker,

    pub pageserver: PageServerConf,

    #[serde(default)]
    pub safekeepers: Vec<SafekeeperConf>,

    /// Keep human-readable aliases in memory (and persist them to config), to hide ZId hex strings from the user.
    #[serde(default)]
    // A `HashMap<String, HashMap<TenantId, TimelineId>>` would be more appropriate here,
    // but deserialization into a generic toml object as `toml::Value::try_from` fails with an error.
    // https://toml.io/en/v1.0.0 does not contain a concept of "a table inside another table".
    #[serde_as(as = "HashMap<_, Vec<(DisplayFromStr, DisplayFromStr)>>")]
    branch_name_mappings: HashMap<String, Vec<(TenantId, TimelineId)>>,
}

/// Etcd broker config for cluster internal communication.
#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct EtcdBroker {
    /// A prefix to all to any key when pushing/polling etcd from a node.
    #[serde(default)]
    pub broker_etcd_prefix: Option<String>,

    /// Broker (etcd) endpoints for storage nodes coordination, e.g. 'http://127.0.0.1:2379'.
    #[serde(default)]
    #[serde_as(as = "Vec<DisplayFromStr>")]
    pub broker_endpoints: Vec<Url>,

    /// Etcd binary path to use.
    #[serde(default)]
    pub etcd_binary_path: PathBuf,
}

impl EtcdBroker {
    pub fn locate_etcd() -> anyhow::Result<PathBuf> {
        let which_output = Command::new("which")
            .arg("etcd")
            .output()
            .context("Failed to run 'which etcd' command")?;
        let stdout = String::from_utf8_lossy(&which_output.stdout);
        ensure!(
            which_output.status.success(),
            "'which etcd' invocation failed. Status: {}, stdout: {stdout}, stderr: {}",
            which_output.status,
            String::from_utf8_lossy(&which_output.stderr)
        );

        let etcd_path = PathBuf::from(stdout.trim());
        ensure!(
            etcd_path.is_file(),
            "'which etcd' invocation was successful, but the path it returned is not a file or does not exist: {}",
            etcd_path.display()
        );

        Ok(etcd_path)
    }

    pub fn comma_separated_endpoints(&self) -> String {
        self.broker_endpoints
            .iter()
            .map(|url| {
                // URL by default adds a '/' path at the end, which is not what etcd CLI wants.
                let url_string = url.as_str();
                if url_string.ends_with('/') {
                    &url_string[0..url_string.len() - 1]
                } else {
                    url_string
                }
            })
            .fold(String::new(), |mut comma_separated_urls, url| {
                if !comma_separated_urls.is_empty() {
                    comma_separated_urls.push(',');
                }
                comma_separated_urls.push_str(url);
                comma_separated_urls
            })
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct PageServerConf {
    // node id
    pub id: NodeId,
    // Pageserver connection settings
    pub listen_pg_addr: String,
    pub listen_http_addr: String,

    // used to determine which auth type is used
    pub auth_type: AuthType,

    // jwt auth token used for communication with pageserver
    pub auth_token: String,

    pub testing_mode: bool,
}

impl Default for PageServerConf {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            listen_pg_addr: String::new(),
            listen_http_addr: String::new(),
            auth_type: AuthType::Trust,
            auth_token: String::new(),
            testing_mode: false,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct SafekeeperConf {
    pub id: NodeId,
    pub pg_port: u16,
    pub http_port: u16,
    pub sync: bool,
    pub remote_storage: Option<String>,
    pub backup_threads: Option<u32>,
    pub auth_enabled: bool,
}

impl Default for SafekeeperConf {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            pg_port: 0,
            http_port: 0,
            sync: true,
            remote_storage: None,
            backup_threads: None,
            auth_enabled: false,
        }
    }
}

impl LocalEnv {
    pub fn pg_distrib_dir_raw(&self) -> PathBuf {
        self.pg_distrib_dir.clone()
    }

    pub fn pg_distrib_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        let path = self.pg_distrib_dir.clone();

        match pg_version {
            14 => Ok(path.join(format!("v{pg_version}"))),
            15 => Ok(path.join(format!("v{pg_version}"))),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pg_bin_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        match pg_version {
            14 => Ok(self.pg_distrib_dir(pg_version)?.join("bin")),
            15 => Ok(self.pg_distrib_dir(pg_version)?.join("bin")),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }
    pub fn pg_lib_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        match pg_version {
            14 => Ok(self.pg_distrib_dir(pg_version)?.join("lib")),
            15 => Ok(self.pg_distrib_dir(pg_version)?.join("lib")),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pageserver_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("pageserver")
    }

    pub fn safekeeper_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("safekeeper")
    }

    pub fn pg_data_dirs_path(&self) -> PathBuf {
        self.base_data_dir.join("pgdatadirs").join("tenants")
    }

    pub fn pg_data_dir(&self, tenant_id: &TenantId, branch_name: &str) -> PathBuf {
        self.pg_data_dirs_path()
            .join(tenant_id.to_string())
            .join(branch_name)
    }

    // TODO: move pageserver files into ./pageserver
    pub fn pageserver_data_dir(&self) -> PathBuf {
        self.base_data_dir.clone()
    }

    pub fn safekeeper_data_dir(&self, data_dir_name: &str) -> PathBuf {
        self.base_data_dir.join("safekeepers").join(data_dir_name)
    }

    pub fn register_branch_mapping(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<()> {
        let existing_values = self
            .branch_name_mappings
            .entry(branch_name.clone())
            .or_default();

        let existing_ids = existing_values
            .iter()
            .find(|(existing_tenant_id, _)| existing_tenant_id == &tenant_id);

        if let Some((_, old_timeline_id)) = existing_ids {
            if old_timeline_id == &timeline_id {
                Ok(())
            } else {
                bail!("branch '{branch_name}' is already mapped to timeline {old_timeline_id}, cannot map to another timeline {timeline_id}");
            }
        } else {
            existing_values.push((tenant_id, timeline_id));
            Ok(())
        }
    }

    pub fn get_branch_timeline_id(
        &self,
        branch_name: &str,
        tenant_id: TenantId,
    ) -> Option<TimelineId> {
        self.branch_name_mappings
            .get(branch_name)?
            .iter()
            .find(|(mapped_tenant_id, _)| mapped_tenant_id == &tenant_id)
            .map(|&(_, timeline_id)| timeline_id)
            .map(TimelineId::from)
    }

    pub fn timeline_name_mappings(&self) -> HashMap<TenantTimelineId, String> {
        self.branch_name_mappings
            .iter()
            .flat_map(|(name, tenant_timelines)| {
                tenant_timelines.iter().map(|&(tenant_id, timeline_id)| {
                    (TenantTimelineId::new(tenant_id, timeline_id), name.clone())
                })
            })
            .collect()
    }

    /// Create a LocalEnv from a config file.
    ///
    /// Unlike 'load_config', this function fills in any defaults that are missing
    /// from the config file.
    pub fn parse_config(toml: &str) -> anyhow::Result<Self> {
        let mut env: LocalEnv = toml::from_str(toml)?;

        // Find postgres binaries.
        // Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "pg_install".
        // Note that later in the code we assume, that distrib dirs follow the same pattern
        // for all postgres versions.
        if env.pg_distrib_dir == Path::new("") {
            if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
                env.pg_distrib_dir = postgres_bin.into();
            } else {
                let cwd = env::current_dir()?;
                env.pg_distrib_dir = cwd.join("pg_install")
            }
        }

        // Find neon binaries.
        if env.neon_distrib_dir == Path::new("") {
            env.neon_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
        }

        // If no initial tenant ID was given, generate it.
        if env.default_tenant_id.is_none() {
            env.default_tenant_id = Some(TenantId::generate());
        }

        env.base_data_dir = base_path();

        Ok(env)
    }

    /// Locate and load config
    pub fn load_config() -> anyhow::Result<Self> {
        let repopath = base_path();

        if !repopath.exists() {
            bail!(
                "Neon config is not found in {}. You need to run 'neon_local init' first",
                repopath.to_str().unwrap()
            );
        }

        // TODO: check that it looks like a neon repository

        // load and parse file
        let config = fs::read_to_string(repopath.join("config"))?;
        let mut env: LocalEnv = toml::from_str(config.as_str())?;

        env.base_data_dir = repopath;

        Ok(env)
    }

    pub fn persist_config(&self, base_path: &Path) -> anyhow::Result<()> {
        // Currently, the user first passes a config file with 'neon_local init --config=<path>'
        // We read that in, in `create_config`, and fill any missing defaults. Then it's saved
        // to .neon/config. TODO: We lose any formatting and comments along the way, which is
        // a bit sad.
        let mut conf_content = r#"# This file describes a locale deployment of the page server
# and safekeeeper node. It is read by the 'neon_local' command-line
# utility.
"#
        .to_string();

        // Convert the LocalEnv to a toml file.
        //
        // This could be as simple as this:
        //
        // conf_content += &toml::to_string_pretty(env)?;
        //
        // But it results in a "values must be emitted before tables". I'm not sure
        // why, AFAICS the table, i.e. 'safekeepers: Vec<SafekeeperConf>' is last.
        // Maybe rust reorders the fields to squeeze avoid padding or something?
        // In any case, converting to toml::Value first, and serializing that, works.
        // See https://github.com/alexcrichton/toml-rs/issues/142
        conf_content += &toml::to_string_pretty(&toml::Value::try_from(self)?)?;

        let target_config_path = base_path.join("config");
        fs::write(&target_config_path, conf_content).with_context(|| {
            format!(
                "Failed to write config file into path '{}'",
                target_config_path.display()
            )
        })
    }

    // this function is used only for testing purposes in CLI e g generate tokens during init
    pub fn generate_auth_token(&self, claims: &Claims) -> anyhow::Result<String> {
        let private_key_path = if self.private_key_path.is_absolute() {
            self.private_key_path.to_path_buf()
        } else {
            self.base_data_dir.join(&self.private_key_path)
        };

        let key_data = fs::read(private_key_path)?;
        encode_from_key_file(claims, &key_data)
    }

    //
    // Initialize a new Neon repository
    //
    pub fn init(&mut self, pg_version: u32) -> anyhow::Result<()> {
        // check if config already exists
        let base_path = &self.base_data_dir;
        ensure!(
            base_path != Path::new(""),
            "repository base path is missing"
        );

        ensure!(
            !base_path.exists(),
            "directory '{}' already exists. Perhaps already initialized?",
            base_path.display()
        );
        if !self.pg_bin_dir(pg_version)?.join("postgres").exists() {
            bail!(
                "Can't find postgres binary at {}",
                self.pg_bin_dir(pg_version)?.display()
            );
        }
        for binary in ["pageserver", "safekeeper"] {
            if !self.neon_distrib_dir.join(binary).exists() {
                bail!(
                    "Can't find binary '{binary}' in neon distrib dir '{}'",
                    self.neon_distrib_dir.display()
                );
            }
        }

        fs::create_dir(&base_path)?;

        // generate keys for jwt
        // openssl genrsa -out private_key.pem 2048
        let private_key_path;
        if self.private_key_path == PathBuf::new() {
            private_key_path = base_path.join("auth_private_key.pem");
            let keygen_output = Command::new("openssl")
                .arg("genrsa")
                .args(&["-out", private_key_path.to_str().unwrap()])
                .arg("2048")
                .stdout(Stdio::null())
                .output()
                .context("failed to generate auth private key")?;
            if !keygen_output.status.success() {
                bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
            self.private_key_path = PathBuf::from("auth_private_key.pem");

            let public_key_path = base_path.join("auth_public_key.pem");
            // openssl rsa -in private_key.pem -pubout -outform PEM -out public_key.pem
            let keygen_output = Command::new("openssl")
                .arg("rsa")
                .args(&["-in", private_key_path.to_str().unwrap()])
                .arg("-pubout")
                .args(&["-outform", "PEM"])
                .args(&["-out", public_key_path.to_str().unwrap()])
                .stdout(Stdio::null())
                .output()
                .context("failed to generate auth private key")?;
            if !keygen_output.status.success() {
                bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
        }

        self.pageserver.auth_token =
            self.generate_auth_token(&Claims::new(None, Scope::PageServerApi))?;

        fs::create_dir_all(self.pg_data_dirs_path())?;

        for safekeeper in &self.safekeepers {
            fs::create_dir_all(SafekeeperNode::datadir_path_by_id(self, safekeeper.id))?;
        }

        self.persist_config(base_path)
    }
}

fn base_path() -> PathBuf {
    match std::env::var_os("NEON_REPO_DIR") {
        Some(val) => PathBuf::from(val),
        None => PathBuf::from(".neon"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_conf_parsing() {
        let simple_conf_toml = include_str!("../simple.conf");
        let simple_conf_parse_result = LocalEnv::parse_config(simple_conf_toml);
        assert!(
            simple_conf_parse_result.is_ok(),
            "failed to parse simple config {simple_conf_toml}, reason: {simple_conf_parse_result:?}"
        );

        let string_to_replace = "broker_endpoints = ['http://127.0.0.1:2379']";
        let spoiled_url_str = "broker_endpoints = ['!@$XOXO%^&']";
        let spoiled_url_toml = simple_conf_toml.replace(string_to_replace, spoiled_url_str);
        assert!(
            spoiled_url_toml.contains(spoiled_url_str),
            "Failed to replace string {string_to_replace} in the toml file {simple_conf_toml}"
        );
        let spoiled_url_parse_result = LocalEnv::parse_config(&spoiled_url_toml);
        assert!(
            spoiled_url_parse_result.is_err(),
            "expected toml with invalid Url {spoiled_url_toml} to fail the parsing, but got {spoiled_url_parse_result:?}"
        );
    }
}
