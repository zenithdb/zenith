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
use anyhow::Context;
use anyhow::{self, Result};
use compute_api::spec::RemoteExtSpec;
use regex::Regex;
use remote_storage::*;
use serde_json;
use std::io::Read;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use tar::Archive;
use tokio::io::AsyncReadExt;
use tracing::info;
use tracing::log::warn;
use zstd::stream::read::Decoder;

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

pub fn get_pg_version(pgbin: &str) -> String {
    // pg_config --version returns a (platform specific) human readable string
    // such as "PostgreSQL 15.4". We parse this to v14/v15/v16 etc.
    let human_version = get_pg_config("--version", pgbin);
    return parse_pg_version(&human_version).to_string();
}

fn parse_pg_version(human_version: &str) -> &str {
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
            "14" => return "v14",
            "15" => return "v15",
            "16" => return "v16",
            _ => {}
        },
        _ => {}
    }
    panic!("Unsupported postgres version {human_version}");
}

#[cfg(test)]
mod tests {
    use super::parse_pg_version;

    #[test]
    fn test_parse_pg_version() {
        assert_eq!(parse_pg_version("PostgreSQL 15.4"), "v15");
        assert_eq!(parse_pg_version("PostgreSQL 15.14"), "v15");
        assert_eq!(
            parse_pg_version("PostgreSQL 15.4 (Ubuntu 15.4-0ubuntu0.23.04.1)"),
            "v15"
        );

        assert_eq!(parse_pg_version("PostgreSQL 14.15"), "v14");
        assert_eq!(parse_pg_version("PostgreSQL 14.0"), "v14");
        assert_eq!(
            parse_pg_version("PostgreSQL 14.9 (Debian 14.9-1.pgdg120+1"),
            "v14"
        );

        assert_eq!(parse_pg_version("PostgreSQL 16devel"), "v16");
        assert_eq!(parse_pg_version("PostgreSQL 16beta1"), "v16");
        assert_eq!(parse_pg_version("PostgreSQL 16rc2"), "v16");
        assert_eq!(parse_pg_version("PostgreSQL 16extra"), "v16");
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

// download the archive for a given extension,
// unzip it, and place files in the appropriate locations (share/lib)
pub async fn download_extension(
    ext_name: &str,
    ext_path: &RemotePath,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
) -> Result<u64> {
    info!("Download extension {:?} from {:?}", ext_name, ext_path);
    let mut download = remote_storage.download(ext_path).await?;
    let mut download_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut download_buffer)
        .await?;
    let download_size = download_buffer.len() as u64;
    // it's unclear whether it is more performant to decompress into memory or not
    // TODO: decompressing into memory can be avoided
    let mut decoder = Decoder::new(download_buffer.as_slice())?;
    let mut decompress_buffer = Vec::new();
    decoder.read_to_end(&mut decompress_buffer)?;
    let mut archive = Archive::new(decompress_buffer.as_slice());
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
        info!("mv {zip_dir:?}/*  {real_dir:?}");
        for file in std::fs::read_dir(zip_dir)? {
            let old_file = file?.path();
            let new_file =
                Path::new(&real_dir).join(old_file.file_name().context("error parsing file")?);
            info!("moving {old_file:?} to {new_file:?}");

            // extension download failed: Directory not empty (os error 39)
            match std::fs::rename(old_file, new_file) {
                Ok(()) => info!("move succeeded"),
                Err(e) => {
                    warn!("move failed, probably because the extension already exists: {e}")
                }
            }
        }
    }
    info!("done moving extension {ext_name}");
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
                info!("writing file {:?}{:?}", control_path, control_content);
                std::fs::write(control_path, control_content).unwrap();
            } else {
                warn!("control file {:?} exists both locally and remotely. ignoring the remote version.", control_path);
            }
        }
    }
}

// This function initializes the necessary structs to use remote storage
pub fn init_remote_storage(remote_ext_config: &str) -> anyhow::Result<GenericRemoteStorage> {
    #[derive(Debug, serde::Deserialize)]
    struct RemoteExtJson {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        prefix: Option<String>,
    }
    let remote_ext_json = serde_json::from_str::<RemoteExtJson>(remote_ext_config)?;

    let config = S3Config {
        bucket_name: remote_ext_json.bucket,
        bucket_region: remote_ext_json.region,
        prefix_in_bucket: remote_ext_json.prefix,
        endpoint: remote_ext_json.endpoint,
        concurrency_limit: NonZeroUsize::new(100).expect("100 != 0"),
        max_keys_per_list_response: None,
    };
    let config = RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).expect("100 != 0"),
        max_sync_errors: NonZeroU32::new(100).expect("100 != 0"),
        storage: RemoteStorageKind::AwsS3(config),
    };
    GenericRemoteStorage::from_config(&config)
}
