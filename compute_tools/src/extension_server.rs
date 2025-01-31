// Download extension files from the extension store
// and put them in the right place in the postgres directory (share / lib)
/*
The layout of the S3 bucket is as follows:
5615610098 // this is an extension build number
├── v14
│   ├── extensions
│   │   ├── anon.tar.zst
│   │   └── embedding.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   ├── anon.tar.zst
    │   └── embedding.tar.zst
    └── ext_index.json
5615261079
├── v14
│   ├── extensions
│   │   └── anon.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   └── anon.tar.zst
    └── ext_index.json
5623261088
├── v14
│   ├── extensions
│   │   └── embedding.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   └── embedding.tar.zst
    └── ext_index.json

Note that build number cannot be part of prefix because we might need extensions
from other build numbers.

ext_index.json stores the control files and location of extension archives
It also stores a list of public extensions and a library_index

We don't need to duplicate extension.tar.zst files.
We only need to upload a new one if it is updated.
(Although currently we just upload every time anyways, hopefully will change
this sometime)

*access* is controlled by spec

More specifically, here is an example ext_index.json
{
    "public_extensions": [
        "anon",
        "pg_buffercache"
    ],
    "library_index": {
        "anon": "anon",
        "pg_buffercache": "pg_buffercache"
    },
    "extension_data": {
        "pg_buffercache": {
            "control_data": {
                "pg_buffercache.control": "# pg_buffercache extension \ncomment = 'examine the shared buffer cache' \ndefault_version = '1.3' \nmodule_pathname = '$libdir/pg_buffercache' \nrelocatable = true \ntrusted=true"
            },
            "archive_path": "5670669815/v14/extensions/pg_buffercache.tar.zst"
        },
        "anon": {
            "control_data": {
                "anon.control": "# PostgreSQL Anonymizer (anon) extension \ncomment = 'Data anonymization tools' \ndefault_version = '1.1.0' \ndirectory='extension/anon' \nrelocatable = false \nrequires = 'pgcrypto' \nsuperuser = false \nmodule_pathname = '$libdir/anon' \ntrusted = true \n"
            },
            "archive_path": "5670669815/v14/extensions/anon.tar.zst"
        }
    }
}
*/
use anyhow::Result;
use anyhow::{bail, Context};
use bytes::Bytes;
use compute_api::spec::RemoteExtSpec;
use regex::Regex;
use remote_storage::*;
use reqwest::StatusCode;
use std::path::Path;
use std::str;
use tar::Archive;
use tracing::info;
use tracing::log::warn;
use zstd::stream::read::Decoder;

use crate::metrics::{REMOTE_EXT_REQUESTS_TOTAL, UNKNOWN_HTTP_STATUS};

fn get_pg_config(argument: &str, pgbin: &str) -> String {
    // gives the result of `pg_config [argument]`
    // where argument is a flag like `--version` or `--sharedir`
    let pgconfig = pgbin
        .strip_suffix("postgres")
        .expect("bad pgbin")
        .to_owned()
        + "/pg_config";
    let config_output = std::process::Command::new(pgconfig)
        .arg(argument)
        .output()
        .expect("pg_config error");
    std::str::from_utf8(&config_output.stdout)
        .expect("pg_config error")
        .trim()
        .to_string()
}

pub fn get_pg_version(pgbin: &str) -> PostgresMajorVersion {
    // pg_config --version returns a (platform specific) human readable string
    // such as "PostgreSQL 15.4". We parse this to v14/v15/v16 etc.
    let human_version = get_pg_config("--version", pgbin);
    parse_pg_version(&human_version)
}

pub fn get_pg_version_string(pgbin: &str) -> String {
    match get_pg_version(pgbin) {
        PostgresMajorVersion::V14 => "v14",
        PostgresMajorVersion::V15 => "v15",
        PostgresMajorVersion::V16 => "v16",
        PostgresMajorVersion::V17 => "v17",
    }
    .to_owned()
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PostgresMajorVersion {
    V14,
    V15,
    V16,
    V17,
}

fn parse_pg_version(human_version: &str) -> PostgresMajorVersion {
    use PostgresMajorVersion::*;
    // Normal releases have version strings like "PostgreSQL 15.4". But there
    // are also pre-release versions like "PostgreSQL 17devel" or "PostgreSQL
    // 16beta2" or "PostgreSQL 17rc1". And with the --with-extra-version
    // configure option, you can tack any string to the version number,
    // e.g. "PostgreSQL 15.4foobar".
    match Regex::new(r"^PostgreSQL (?<major>\d+).+")
        .unwrap()
        .captures(human_version)
    {
        Some(captures) if captures.len() == 2 => match &captures["major"] {
            "14" => return V14,
            "15" => return V15,
            "16" => return V16,
            "17" => return V17,
            _ => {}
        },
        _ => {}
    }
    panic!("Unsupported Postgres version {human_version}");
}

/// Download the archive for a given extension,
/// unzip it, and place files in the appropriate locations (share/lib)
pub async fn download_extension(
    ext_name: &str,
    ext_path: &RemotePath,
    ext_remote_storage: &str,
    pgbin: &str,
) -> Result<u64> {
    info!("Downloading extension {:?} from {:?}", ext_name, ext_path);

    // TODO add retry logic
    let download_buffer =
        match download_extension_tar(ext_remote_storage, &ext_path.to_string()).await {
            Ok(buffer) => buffer,
            Err(error_message) => {
                return Err(anyhow::anyhow!(
                    "error downloading extension {:?}: {:?}",
                    ext_name,
                    error_message
                ));
            }
        };

    let download_size = download_buffer.len() as u64;
    info!("Download size {:?}", download_size);
    // it's unclear whether it is more performant to decompress into memory or not
    // TODO: decompressing into memory can be avoided
    let decoder = Decoder::new(download_buffer.as_ref())?;
    let mut archive = Archive::new(decoder);

    let unzip_dest = pgbin
        .strip_suffix("/bin/postgres")
        .expect("bad pgbin")
        .to_string()
        + "/download_extensions";
    archive.unpack(&unzip_dest)?;
    info!("Download + unzip {:?} completed successfully", &ext_path);

    let sharedir_paths = (
        unzip_dest.to_string() + "/share/extension",
        Path::new(&get_pg_config("--sharedir", pgbin)).join("extension"),
    );
    let libdir_paths = (
        unzip_dest.to_string() + "/lib",
        Path::new(&get_pg_config("--pkglibdir", pgbin)).to_path_buf(),
    );
    // move contents of the libdir / sharedir in unzipped archive to the correct local paths
    for paths in [sharedir_paths, libdir_paths] {
        let (zip_dir, real_dir) = paths;
        info!("Moving {zip_dir:?}/* to {real_dir:?}");
        for file in std::fs::read_dir(zip_dir)? {
            let old_file = file?.path();
            let new_file =
                Path::new(&real_dir).join(old_file.file_name().context("error parsing file")?);
            info!("Moving {old_file:?} to {new_file:?}");

            // extension download failed: Directory not empty (os error 39)
            match std::fs::rename(old_file, new_file) {
                Ok(()) => info!("Move succeeded"),
                Err(e) => {
                    warn!("Move failed, probably because the extension already exists: {e}")
                }
            }
        }
    }
    info!("Done moving extension {ext_name}");
    Ok(download_size)
}

// Create extension control files from spec
pub fn create_control_files(remote_extensions: &RemoteExtSpec, pgbin: &str) {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    for (ext_name, ext_data) in remote_extensions.extension_data.iter() {
        // Check if extension is present in public or custom.
        // If not, then it is not allowed to be used by this compute.
        if let Some(public_extensions) = &remote_extensions.public_extensions {
            if !public_extensions.contains(ext_name) {
                if let Some(custom_extensions) = &remote_extensions.custom_extensions {
                    if !custom_extensions.contains(ext_name) {
                        continue; // skip this extension, it is not allowed
                    }
                }
            }
        }

        for (control_name, control_content) in &ext_data.control_data {
            let control_path = local_sharedir.join(control_name);
            if !control_path.exists() {
                info!(
                    "Writing control file content {:?}: {:?}",
                    control_path, control_content
                );
                std::fs::write(control_path, control_content).unwrap();
            } else {
                warn!(
                    "Control file {:?} exists locally. Ignoring the version from the spec.",
                    control_path
                );
            }
        }
    }
}

// Do request to extension storage proxy, i.e.
// curl http://pg-ext-s3-gateway/latest/v15/extensions/anon.tar.zst
// using HTTP GET and return the response body as bytes.
async fn download_extension_tar(ext_remote_storage: &str, ext_path: &str) -> Result<Bytes> {
    let uri = format!("{}/{}", ext_remote_storage, ext_path);

    info!("Download extension {} from uri {}", ext_path, uri);

    match do_extension_server_request(&uri).await {
        Ok(resp) => {
            info!("Successfully downloaded remote extension data {}", ext_path);
            REMOTE_EXT_REQUESTS_TOTAL
                .with_label_values(&[&StatusCode::OK.to_string()])
                .inc();
            Ok(resp)
        }
        Err((msg, status)) => {
            REMOTE_EXT_REQUESTS_TOTAL
                .with_label_values(&[&status])
                .inc();
            bail!(msg);
        }
    }
}

// Do a single remote extensions server request.
// Return result or (error message + stringified status code) in case of any failures.
async fn do_extension_server_request(uri: &str) -> Result<Bytes, (String, String)> {
    let resp = reqwest::get(uri).await.map_err(|e| {
        (
            format!(
                "could not perform remote extensions server request: {:?}",
                e
            ),
            UNKNOWN_HTTP_STATUS.to_string(),
        )
    })?;
    let status = resp.status();

    match status {
        StatusCode::OK => match resp.bytes().await {
            Ok(resp) => Ok(resp),
            Err(e) => Err((
                format!("could not read remote extensions server response: {:?}", e),
                // It's fine to return and report error with status as 200 OK,
                // because we still failed to read the response.
                status.to_string(),
            )),
        },
        StatusCode::SERVICE_UNAVAILABLE => Err((
            "remote extensions server is temporarily unavailable".to_string(),
            status.to_string(),
        )),
        _ => Err((
            format!(
                "unexpected remote extensions server response status code: {}",
                status
            ),
            status.to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_pg_version;

    #[test]
    fn test_parse_pg_version() {
        use super::PostgresMajorVersion::*;
        assert_eq!(parse_pg_version("PostgreSQL 15.4"), V15);
        assert_eq!(parse_pg_version("PostgreSQL 15.14"), V15);
        assert_eq!(
            parse_pg_version("PostgreSQL 15.4 (Ubuntu 15.4-0ubuntu0.23.04.1)"),
            V15
        );

        assert_eq!(parse_pg_version("PostgreSQL 14.15"), V14);
        assert_eq!(parse_pg_version("PostgreSQL 14.0"), V14);
        assert_eq!(
            parse_pg_version("PostgreSQL 14.9 (Debian 14.9-1.pgdg120+1"),
            V14
        );

        assert_eq!(parse_pg_version("PostgreSQL 16devel"), V16);
        assert_eq!(parse_pg_version("PostgreSQL 16beta1"), V16);
        assert_eq!(parse_pg_version("PostgreSQL 16rc2"), V16);
        assert_eq!(parse_pg_version("PostgreSQL 16extra"), V16);
    }

    #[test]
    #[should_panic]
    fn test_parse_pg_unsupported_version() {
        parse_pg_version("PostgreSQL 13.14");
    }

    #[test]
    #[should_panic]
    fn test_parse_pg_incorrect_version_format() {
        parse_pg_version("PostgreSQL 14");
    }
}
