//! A helper tool to manage pageserver binary files.
//! Accepts a file as an argument, attempts to parse it with all ways possible
//! and prints its interpreted context.
//!
//! Separate, `metadata` subcommand allows to print and update pageserver's metadata file.

mod draw_timeline_dir;
mod layer_map_analyzer;
mod layers;

use clap::{Parser, Subcommand};
use layers::LayerCmd;
use pageserver::{
    context::{DownloadBehavior, RequestContext},
    page_cache,
    task_mgr::TaskKind,
    tenant::{dump_layerfile_from_path, metadata::TimelineMetadata},
    virtual_file,
};
use postgres_ffi::ControlFileData;
use std::path::{Path, PathBuf};
use utils::{lsn::Lsn, project_git_version};

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
    PrintLayerFile(PrintLayerFileCmd),
    DrawTimeline {},
    AnalyzeLayerMap(AnalyzeLayerMapCmd),
    #[command(subcommand)]
    Layer(LayerCmd),
}

/// Read and update pageserver metadata file
#[derive(Parser)]
struct MetadataCmd {
    /// Input metadata file path
    metadata_path: PathBuf,
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
    path: PathBuf,
}

#[derive(Parser)]
struct AnalyzeLayerMapCmd {
    /// Pageserver data path
    path: PathBuf,
    /// Max holes
    max_holes: Option<usize>,
}

fn main() -> anyhow::Result<()> {
    let cli = CliOpts::parse();

    match cli.command {
        Commands::Layer(cmd) => {
            layers::main(&cmd)?;
        }
        Commands::Metadata(cmd) => {
            handle_metadata(&cmd)?;
        }
        Commands::DrawTimeline {} => {
            draw_timeline_dir::main()?;
        }
        Commands::AnalyzeLayerMap(cmd) => {
            layer_map_analyzer::main(&cmd)?;
        }
        Commands::PrintLayerFile(cmd) => {
            if let Err(e) = read_pg_control_file(&cmd.path) {
                println!(
                    "Failed to read input file as a pg control one: {e:#}\n\
                    Attempting to read it as layer file"
                );
                print_layerfile(&cmd.path)?;
            }
        }
    };
    Ok(())
}

fn read_pg_control_file(control_file_path: &Path) -> anyhow::Result<()> {
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

fn print_layerfile(path: &Path) -> anyhow::Result<()> {
    // Basic initialization of things that don't change after startup
    virtual_file::init(10);
    page_cache::init(100);
    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
    dump_layerfile_from_path(path, true, &ctx)
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
    if let Some(disk_consistent_lsn) = disk_consistent_lsn {
        meta = TimelineMetadata::new(
            *disk_consistent_lsn,
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
            meta.replica_lsn(),
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
            meta.replica_lsn(),
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
            meta.replica_lsn(),
        );
        update_meta = true;
    }

    if update_meta {
        let metadata_bytes = meta.to_bytes()?;
        std::fs::write(path, metadata_bytes)?;
    }

    Ok(())
}
