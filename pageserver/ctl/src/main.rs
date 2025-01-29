//! A helper tool to manage pageserver binary files.
//! Accepts a file as an argument, attempts to parse it with all ways possible
//! and prints its interpreted context.
//!
//! Separate, `metadata` subcommand allows to print and update pageserver's metadata file.

mod draw_timeline_dir;
mod index_part;
mod key;
mod layer_map_analyzer;
mod layers;
mod page_trace;

use page_trace::PageTraceCmd;
use std::{
    str::FromStr,
    time::{Duration, SystemTime},
};

use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use index_part::IndexPartCmd;
use layers::LayerCmd;
use pageserver::{
    context::{DownloadBehavior, RequestContext},
    page_cache,
    task_mgr::TaskKind,
    tenant::{dump_layerfile_from_path, metadata::TimelineMetadata},
    virtual_file::{self, api::IoMode},
};
use pageserver_api::shard::TenantShardId;
use postgres_ffi::ControlFileData;
use remote_storage::{RemotePath, RemoteStorageConfig};
use tokio_util::sync::CancellationToken;
use utils::{
    id::TimelineId,
    logging::{self, LogFormat, TracingErrorLayerEnablement},
    lsn::Lsn,
    project_git_version,
};

project_git_version!(GIT_VERSION);

#[derive(Parser)]
#[command(
    version = GIT_VERSION,
    about = "Neon Pageserver binutils",
    long_about = "Reads pageserver (and related) binary files management utility"
)]
#[command(propagate_version = true)]
struct CliOpts {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Metadata(MetadataCmd),
    #[command(subcommand)]
    IndexPart(IndexPartCmd),
    PrintLayerFile(PrintLayerFileCmd),
    TimeTravelRemotePrefix(TimeTravelRemotePrefixCmd),
    DrawTimeline {},
    AnalyzeLayerMap(AnalyzeLayerMapCmd),
    #[command(subcommand)]
    Layer(LayerCmd),
    /// Debug print a hex key found from logs
    Key(key::DescribeKeyCommand),
    PageTrace(PageTraceCmd),
}

/// Read and update pageserver metadata file
#[derive(Parser)]
struct MetadataCmd {
    /// Input metadata file path
    metadata_path: Utf8PathBuf,
    /// Replace disk consistent Lsn
    disk_consistent_lsn: Option<Lsn>,
    /// Replace previous record Lsn
    prev_record_lsn: Option<Lsn>,
    /// Replace latest gc cuttoff
    latest_gc_cuttoff: Option<Lsn>,
}

#[derive(Parser)]
struct PrintLayerFileCmd {
    /// Pageserver data path
    path: Utf8PathBuf,
}

/// Roll back the time for the specified prefix using S3 history.
///
/// The command is fairly low level and powerful. Validation is only very light,
/// so it is more powerful, and thus potentially more dangerous.
#[derive(Parser)]
struct TimeTravelRemotePrefixCmd {
    /// A configuration string for the remote_storage configuration.
    ///
    /// Example: `remote_storage = { bucket_name = "aws-storage-bucket-name", bucket_region = "us-east-2" }`
    config_toml_str: String,
    /// remote prefix to time travel recover. For safety reasons, we require it to contain
    /// a timeline or tenant ID in the prefix.
    prefix: String,
    /// Timestamp to travel to. Given in format like `2024-01-20T10:45:45Z`. Assumes UTC and second accuracy.
    travel_to: String,
    /// Timestamp of the start of the operation, must be after any changes we want to roll back and after.
    /// You can use a few seconds before invoking the command. Same format as `travel_to`.
    done_if_after: Option<String>,
}

#[derive(Parser)]
struct AnalyzeLayerMapCmd {
    /// Pageserver data path
    path: Utf8PathBuf,
    /// Max holes
    max_holes: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        TracingErrorLayerEnablement::EnableWithRustLogFilter,
        utils::logging::OtelEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    logging::replace_panic_hook_with_tracing_panic_hook().forget();

    let cli = CliOpts::parse();

    match cli.command {
        Commands::Layer(cmd) => {
            layers::main(&cmd).await?;
        }
        Commands::Metadata(cmd) => {
            handle_metadata(&cmd)?;
        }
        Commands::IndexPart(cmd) => {
            index_part::main(&cmd).await?;
        }
        Commands::DrawTimeline {} => {
            draw_timeline_dir::main()?;
        }
        Commands::AnalyzeLayerMap(cmd) => {
            layer_map_analyzer::main(&cmd).await?;
        }
        Commands::PrintLayerFile(cmd) => {
            if let Err(e) = read_pg_control_file(&cmd.path) {
                println!(
                    "Failed to read input file as a pg control one: {e:#}\n\
                    Attempting to read it as layer file"
                );
                print_layerfile(&cmd.path).await?;
            }
        }
        Commands::TimeTravelRemotePrefix(cmd) => {
            let timestamp = humantime::parse_rfc3339(&cmd.travel_to)
                .map_err(|_e| anyhow::anyhow!("Invalid time for travel_to: '{}'", cmd.travel_to))?;

            let done_if_after = if let Some(done_if_after) = &cmd.done_if_after {
                humantime::parse_rfc3339(done_if_after).map_err(|_e| {
                    anyhow::anyhow!("Invalid time for done_if_after: '{}'", done_if_after)
                })?
            } else {
                const SAFETY_MARGIN: Duration = Duration::from_secs(3);
                tokio::time::sleep(SAFETY_MARGIN).await;
                // Convert to string representation and back to get rid of sub-second values
                let done_if_after = SystemTime::now();
                tokio::time::sleep(SAFETY_MARGIN).await;
                done_if_after
            };

            let timestamp = strip_subsecond(timestamp);
            let done_if_after = strip_subsecond(done_if_after);

            let Some(prefix) = validate_prefix(&cmd.prefix) else {
                println!("specified prefix '{}' failed validation", cmd.prefix);
                return Ok(());
            };
            let config = RemoteStorageConfig::from_toml_str(&cmd.config_toml_str)?;
            let storage = remote_storage::GenericRemoteStorage::from_config(&config).await;
            let cancel = CancellationToken::new();
            storage
                .unwrap()
                .time_travel_recover(Some(&prefix), timestamp, done_if_after, &cancel)
                .await?;
        }
        Commands::Key(dkc) => dkc.execute(),
        Commands::PageTrace(cmd) => page_trace::main(&cmd)?,
    };
    Ok(())
}

fn read_pg_control_file(control_file_path: &Utf8Path) -> anyhow::Result<()> {
    let control_file = ControlFileData::decode(&std::fs::read(control_file_path)?)?;
    println!("{control_file:?}");
    let control_file_initdb = Lsn(control_file.checkPoint);
    println!(
        "pg_initdb_lsn: {}, aligned: {}",
        control_file_initdb,
        control_file_initdb.align()
    );
    Ok(())
}

async fn print_layerfile(path: &Utf8Path) -> anyhow::Result<()> {
    // Basic initialization of things that don't change after startup
    virtual_file::init(
        10,
        virtual_file::api::IoEngineKind::StdFs,
        IoMode::preferred(),
        virtual_file::SyncMode::Sync,
    );
    page_cache::init(100);
    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
    dump_layerfile_from_path(path, true, &ctx).await
}

fn handle_metadata(
    MetadataCmd {
        metadata_path: path,
        disk_consistent_lsn,
        prev_record_lsn,
        latest_gc_cuttoff,
    }: &MetadataCmd,
) -> Result<(), anyhow::Error> {
    let metadata_bytes = std::fs::read(path)?;
    let mut meta = TimelineMetadata::from_bytes(&metadata_bytes)?;
    println!("Current metadata:\n{meta:?}");
    let mut update_meta = false;
    // TODO: simplify this part
    if let Some(disk_consistent_lsn) = disk_consistent_lsn {
        meta = TimelineMetadata::new(
            *disk_consistent_lsn,
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }
    if let Some(prev_record_lsn) = prev_record_lsn {
        meta = TimelineMetadata::new(
            meta.disk_consistent_lsn(),
            Some(*prev_record_lsn),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }
    if let Some(latest_gc_cuttoff) = latest_gc_cuttoff {
        meta = TimelineMetadata::new(
            meta.disk_consistent_lsn(),
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            *latest_gc_cuttoff,
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }

    if update_meta {
        let metadata_bytes = meta.to_bytes()?;
        std::fs::write(path, metadata_bytes)?;
    }

    Ok(())
}

/// Ensures that the given S3 prefix is sufficiently constrained.
/// The command is very risky already and we don't want to expose something
/// that allows usually unintentional and quite catastrophic time travel of
/// an entire bucket, which would be a major catastrophy and away
/// by only one character change (similar to "rm -r /home /username/foobar").
fn validate_prefix(prefix: &str) -> Option<RemotePath> {
    if prefix.is_empty() {
        // Empty prefix means we want to specify the *whole* bucket
        return None;
    }
    let components = prefix.split('/').collect::<Vec<_>>();
    let (last, components) = {
        let last = components.last()?;
        if last.is_empty() {
            (
                components.iter().nth_back(1)?,
                &components[..(components.len() - 1)],
            )
        } else {
            (last, &components[..])
        }
    };
    'valid: {
        if let Ok(_timeline_id) = TimelineId::from_str(last) {
            // Ends in either a tenant or timeline ID
            break 'valid;
        }
        if *last == "timelines" {
            if let Some(before_last) = components.iter().nth_back(1) {
                if let Ok(_tenant_id) = TenantShardId::from_str(before_last) {
                    // Has a valid tenant id
                    break 'valid;
                }
            }
        }

        return None;
    }
    RemotePath::from_string(prefix).ok()
}

fn strip_subsecond(timestamp: SystemTime) -> SystemTime {
    let ts_str = humantime::format_rfc3339_seconds(timestamp).to_string();
    humantime::parse_rfc3339(&ts_str).expect("can't parse just created timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_prefix() {
        assert_eq!(validate_prefix(""), None);
        assert_eq!(validate_prefix("/"), None);
        #[track_caller]
        fn assert_valid(prefix: &str) {
            let remote_path = RemotePath::from_string(prefix).unwrap();
            assert_eq!(validate_prefix(prefix), Some(remote_path));
        }
        assert_valid("wal/3aa8fcc61f6d357410b7de754b1d9001/641e5342083b2235ee3deb8066819683/");
        // Path is not relative but absolute
        assert_eq!(
            validate_prefix(
                "/wal/3aa8fcc61f6d357410b7de754b1d9001/641e5342083b2235ee3deb8066819683/"
            ),
            None
        );
        assert_valid("wal/3aa8fcc61f6d357410b7de754b1d9001/");
        // Partial tenant IDs should be invalid, S3 will match all tenants with the specific ID prefix
        assert_eq!(validate_prefix("wal/3aa8fcc61f6d357410b7d"), None);
        assert_eq!(validate_prefix("wal"), None);
        assert_eq!(validate_prefix("/wal/"), None);
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001");
        // Partial tenant ID
        assert_eq!(
            validate_prefix("pageserver/v1/tenants/3aa8fcc61f6d357410b"),
            None
        );
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001/timelines");
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001-0004/timelines");
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001/timelines/");
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001/timelines/641e5342083b2235ee3deb8066819683");
        assert_eq!(validate_prefix("pageserver/v1/tenants/"), None);
    }
}
