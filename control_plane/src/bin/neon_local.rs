//!
//! `neon_local` is an executable that can be used to create a local
//! Neon environment, for testing purposes. The local environment is
//! quite different from the cloud environment with Kubernetes, but it
//! easier to work with locally. The python tests in `test_runner`
//! rely on `neon_local` to set up the environment for each test.
//!
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use compute_api::spec::ComputeMode;
use control_plane::endpoint::ComputeControlPlane;
use control_plane::local_env::{
    InitForceMode, LocalEnv, NeonBroker, NeonLocalInitConf, NeonLocalInitPageserverConf,
    SafekeeperConf,
};
use control_plane::pageserver::PageServerNode;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::storage_controller::{
    NeonStorageControllerStartArgs, NeonStorageControllerStopArgs, StorageController,
};
use control_plane::{broker, local_env};
use nix::fcntl::{flock, FlockArg};
use pageserver_api::config::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_PAGESERVER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_PAGESERVER_PG_PORT,
};
use pageserver_api::controller_api::{
    NodeAvailabilityWrapper, PlacementPolicy, TenantCreateRequest,
};
use pageserver_api::models::{ShardParameters, TimelineCreateRequest, TimelineInfo};
use pageserver_api::shard::{ShardCount, ShardStripeSize, TenantShardId};
use postgres_backend::AuthType;
use postgres_connection::parse_host_port;
use safekeeper_api::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT,
};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::time::Duration;
use storage_broker::DEFAULT_LISTEN_ADDR as DEFAULT_BROKER_ADDR;
use tokio::task::JoinSet;
use url::Host;
use utils::{
    auth::{Claims, Scope},
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    project_git_version,
};

// Default id of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_ID: NodeId = NodeId(1);
const DEFAULT_PAGESERVER_ID: NodeId = NodeId(1);
const DEFAULT_BRANCH_NAME: &str = "main";
project_git_version!(GIT_VERSION);

const DEFAULT_PG_VERSION: u32 = 16;

const DEFAULT_PAGESERVER_CONTROL_PLANE_API: &str = "http://127.0.0.1:1234/upcall/v1/";

#[derive(clap::Parser)]
#[command(version = GIT_VERSION, about, name = "Neon CLI")]
struct Cli {
    #[command(subcommand)]
    command: NeonLocalCmd,
}

#[derive(clap::Subcommand)]
enum NeonLocalCmd {
    Init(InitCmdArgs),

    #[command(subcommand)]
    Tenant(TenantCmd),
    #[command(subcommand)]
    Timeline(TimelineCmd),
    #[command(subcommand)]
    Pageserver(PageserverCmd),
    #[command(subcommand)]
    #[clap(alias = "storage_controller")]
    StorageController(StorageControllerCmd),
    #[command(subcommand)]
    #[clap(alias = "storage_broker")]
    StorageBroker(StorageBrokerCmd),
    #[command(subcommand)]
    Safekeeper(SafekeeperCmd),
    #[command(subcommand)]
    Endpoint(EndpointCmd),
    #[command(subcommand)]
    Mappings(MappingsCmd),

    Start(StartCmdArgs),
    Stop(StopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Initialize a new Neon repository, preparing configs for services to start with")]
struct InitCmdArgs {
    #[clap(long, help("How many pageservers to create (default 1)"))]
    num_pageservers: Option<u16>,

    #[clap(long)]
    config: Option<PathBuf>,

    #[clap(long, help("Force initialization even if the repository is not empty"))]
    #[arg(value_parser)]
    #[clap(default_value = "must-not-exist")]
    force: InitForceMode,
}

#[derive(clap::Args)]
#[clap(about = "Start pageserver and safekeepers")]
struct StartCmdArgs {
    #[clap(long = "start-timeout", default_value = "10s")]
    timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop pageserver and safekeepers")]
struct StopCmdArgs {
    #[arg(value_enum)]
    #[clap(long, default_value_t = StopMode::Fast)]
    mode: StopMode,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum StopMode {
    Fast,
    Immediate,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage tenants")]
enum TenantCmd {
    List,
    Create(TenantCreateCmdArgs),
    SetDefault(TenantSetDefaultCmdArgs),
    Config(TenantConfigCmdArgs),
    Import(TenantImportCmdArgs),
}

#[derive(clap::Args)]
struct TenantCreateCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(
        long,
        help = "Use a specific timeline id when creating a tenant and its initial timeline"
    )]
    timeline_id: Option<TimelineId>,

    #[clap(short = 'c')]
    config: Vec<String>,

    #[arg(default_value_t = DEFAULT_PG_VERSION)]
    #[clap(long, help = "Postgres version to use for the initial timeline")]
    pg_version: u32,

    #[clap(
        long,
        help = "Use this tenant in future CLI commands where tenant_id is needed, but not specified"
    )]
    set_default: bool,

    #[clap(long, help = "Number of shards in the new tenant")]
    #[arg(default_value_t = 0)]
    shard_count: u8,
    #[clap(long, help = "Sharding stripe size in pages")]
    shard_stripe_size: Option<u32>,

    #[clap(long, help = "Placement policy shards in this tenant")]
    #[arg(value_parser = parse_placement_policy)]
    placement_policy: Option<PlacementPolicy>,
}

fn parse_placement_policy(s: &str) -> anyhow::Result<PlacementPolicy> {
    Ok(serde_json::from_str::<PlacementPolicy>(s)?)
}

#[derive(clap::Args)]
#[clap(
    about = "Set a particular tenant as default in future CLI commands where tenant_id is needed, but not specified"
)]
struct TenantSetDefaultCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: TenantId,
}

#[derive(clap::Args)]
struct TenantConfigCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(short = 'c')]
    config: Vec<String>,
}

#[derive(clap::Args)]
#[clap(
    about = "Import a tenant that is present in remote storage, and create branches for its timelines"
)]
struct TenantImportCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: TenantId,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage timelines")]
enum TimelineCmd {
    List(TimelineListCmdArgs),
    Branch(TimelineBranchCmdArgs),
    Create(TimelineCreateCmdArgs),
    Import(TimelineImportCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "List all timelines available to this pageserver")]
struct TimelineListCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_shard_id: Option<TenantShardId>,
}

#[derive(clap::Args)]
#[clap(about = "Create a new timeline, branching off from another timeline")]
struct TimelineBranchCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "New timeline's ID")]
    timeline_id: Option<TimelineId>,

    #[clap(long, help = "Human-readable alias for the new timeline")]
    branch_name: String,

    #[clap(
        long,
        help = "Use last Lsn of another timeline (and its data) as base when creating the new timeline. The timeline gets resolved by its branch name."
    )]
    ancestor_branch_name: Option<String>,

    #[clap(
        long,
        help = "When using another timeline as base, use a specific Lsn in it instead of the latest one"
    )]
    ancestor_start_lsn: Option<Lsn>,
}

#[derive(clap::Args)]
#[clap(about = "Create a new blank timeline")]
struct TimelineCreateCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "New timeline's ID")]
    timeline_id: Option<TimelineId>,

    #[clap(long, help = "Human-readable alias for the new timeline")]
    branch_name: String,

    #[arg(default_value_t = DEFAULT_PG_VERSION)]
    #[clap(long, help = "Postgres version")]
    pg_version: u32,
}

#[derive(clap::Args)]
#[clap(about = "Import timeline from a basebackup directory")]
struct TimelineImportCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "New timeline's ID")]
    timeline_id: TimelineId,

    #[clap(long, help = "Human-readable alias for the new timeline")]
    branch_name: String,

    #[clap(long, help = "Basebackup tarfile to import")]
    base_tarfile: PathBuf,

    #[clap(long, help = "Lsn the basebackup starts at")]
    base_lsn: Lsn,

    #[clap(long, help = "Wal to add after base")]
    wal_tarfile: Option<PathBuf>,

    #[clap(long, help = "Lsn the basebackup ends at")]
    end_lsn: Option<Lsn>,

    #[arg(default_value_t = DEFAULT_PG_VERSION)]
    #[clap(long, help = "Postgres version of the backup being imported")]
    pg_version: u32,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage pageservers")]
enum PageserverCmd {
    Status(PageserverStatusCmdArgs),
    Start(PageserverStartCmdArgs),
    Stop(PageserverStopCmdArgs),
    Restart(PageserverRestartCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Show status of a local pageserver")]
struct PageserverStatusCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,
}

#[derive(clap::Args)]
#[clap(about = "Start local pageserver")]
struct PageserverStartCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop local pageserver")]
struct PageserverStopCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,

    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

#[derive(clap::Args)]
#[clap(about = "Restart local pageserver")]
struct PageserverRestartCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage storage controller")]
enum StorageControllerCmd {
    Start(StorageControllerStartCmdArgs),
    Stop(StorageControllerStopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Start storage controller")]
struct StorageControllerStartCmdArgs {
    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,

    #[clap(
        long,
        help = "Identifier used to distinguish storage controller instances"
    )]
    #[arg(default_value_t = 1)]
    instance_id: u8,

    #[clap(
        long,
        help = "Base port for the storage controller instance idenfified by instance-id (defaults to pageserver cplane api)"
    )]
    base_port: Option<u16>,
}

#[derive(clap::Args)]
#[clap(about = "Stop storage controller")]
struct StorageControllerStopCmdArgs {
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,

    #[clap(
        long,
        help = "Identifier used to distinguish storage controller instances"
    )]
    #[arg(default_value_t = 1)]
    instance_id: u8,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage storage broker")]
enum StorageBrokerCmd {
    Start(StorageBrokerStartCmdArgs),
    Stop(StorageBrokerStopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Start broker")]
struct StorageBrokerStartCmdArgs {
    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "stop broker")]
struct StorageBrokerStopCmdArgs {
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage safekeepers")]
enum SafekeeperCmd {
    Start(SafekeeperStartCmdArgs),
    Stop(SafekeeperStopCmdArgs),
    Restart(SafekeeperRestartCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Start local safekeeper")]
struct SafekeeperStartCmdArgs {
    #[clap(help = "safekeeper id")]
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    #[clap(
        short = 'e',
        long = "safekeeper-extra-opt",
        help = "Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo"
    )]
    extra_opt: Vec<String>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop local safekeeper")]
struct SafekeeperStopCmdArgs {
    #[clap(help = "safekeeper id")]
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    #[arg(value_enum, default_value = "fast")]
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    stop_mode: StopMode,
}

#[derive(clap::Args)]
#[clap(about = "Restart local safekeeper")]
struct SafekeeperRestartCmdArgs {
    #[clap(help = "safekeeper id")]
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    #[arg(value_enum, default_value = "fast")]
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    stop_mode: StopMode,

    #[clap(
        short = 'e',
        long = "safekeeper-extra-opt",
        help = "Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo"
    )]
    extra_opt: Vec<String>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage Postgres instances")]
enum EndpointCmd {
    List(EndpointListCmdArgs),
    Create(EndpointCreateCmdArgs),
    Start(EndpointStartCmdArgs),
    Reconfigure(EndpointReconfigureCmdArgs),
    Stop(EndpointStopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "List endpoints")]
struct EndpointListCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_shard_id: Option<TenantShardId>,
}

#[derive(clap::Args)]
#[clap(about = "Create a compute endpoint")]
struct EndpointCreateCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(help = "Postgres endpoint id")]
    endpoint_id: Option<String>,
    #[clap(long, help = "Name of the branch the endpoint will run on")]
    branch_name: Option<String>,
    #[clap(
        long,
        help = "Specify Lsn on the timeline to start from. By default, end of the timeline would be used"
    )]
    lsn: Option<Lsn>,
    #[clap(long)]
    pg_port: Option<u16>,
    #[clap(long)]
    http_port: Option<u16>,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    #[clap(
        long,
        help = "Don't do basebackup, create endpoint directory with only config files",
        action = clap::ArgAction::Set,
        default_value_t = false
    )]
    config_only: bool,

    #[arg(default_value_t = DEFAULT_PG_VERSION)]
    #[clap(long, help = "Postgres version")]
    pg_version: u32,

    #[clap(
        long,
        help = "If set, the node will be a hot replica on the specified timeline",
        action = clap::ArgAction::Set,
        default_value_t = false
    )]
    hot_standby: bool,

    #[clap(long, help = "If set, will set up the catalog for neon_superuser")]
    update_catalog: bool,

    #[clap(
        long,
        help = "Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but useful for tests."
    )]
    allow_multiple: bool,
}

#[derive(clap::Args)]
#[clap(about = "Start postgres. If the endpoint doesn't exist yet, it is created.")]
struct EndpointStartCmdArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    #[clap(long)]
    safekeepers: Option<String>,

    #[clap(
        long,
        help = "Configure the remote extensions storage proxy gateway to request for extensions."
    )]
    remote_ext_config: Option<String>,

    #[clap(
        long,
        help = "If set, will create test user `user` and `neondb` database. Requires `update-catalog = true`"
    )]
    create_test_user: bool,

    #[clap(
        long,
        help = "Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but useful for tests."
    )]
    allow_multiple: bool,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Reconfigure an endpoint")]
struct EndpointReconfigureCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    #[clap(long)]
    safekeepers: Option<String>,
}

#[derive(clap::Args)]
#[clap(about = "Stop an endpoint")]
struct EndpointStopCmdArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,

    #[clap(
        long,
        help = "Also delete data directory (now optional, should be default in future)"
    )]
    destroy: bool,

    #[clap(long, help = "Postgres shutdown mode, passed to \"pg_ctl -m <mode>\"")]
    #[arg(value_parser(["smart", "fast", "immediate"]))]
    #[arg(default_value = "fast")]
    mode: String,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage neon_local branch name mappings")]
enum MappingsCmd {
    Map(MappingsMapCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Create new mapping which cannot exist already")]
struct MappingsMapCmdArgs {
    #[clap(
        long,
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: TenantId,
    #[clap(
        long,
        help = "Timeline id. Represented as a hexadecimal string 32 symbols length"
    )]
    timeline_id: TimelineId,
    #[clap(long, help = "Branch name to give to the timeline")]
    branch_name: String,
}

///
/// Timelines tree element used as a value in the HashMap.
///
struct TimelineTreeEl {
    /// `TimelineInfo` received from the `pageserver` via the `timeline_list` http API call.
    pub info: TimelineInfo,
    /// Name, recovered from neon config mappings
    pub name: Option<String>,
    /// Holds all direct children of this timeline referenced using `timeline_id`.
    pub children: BTreeSet<TimelineId>,
}

/// A flock-based guard over the neon_local repository directory
struct RepoLock {
    _file: File,
}

impl RepoLock {
    fn new() -> Result<Self> {
        let repo_dir = File::open(local_env::base_path())?;
        let repo_dir_fd = repo_dir.as_raw_fd();
        flock(repo_dir_fd, FlockArg::LockExclusive)?;

        Ok(Self { _file: repo_dir })
    }
}

// Main entry point for the 'neon_local' CLI utility
//
// This utility helps to manage neon installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let cli = Cli::parse();

    // Check for 'neon init' command first.
    let (subcommand_result, _lock) = if let NeonLocalCmd::Init(args) = cli.command {
        (handle_init(&args).map(|env| Some(Cow::Owned(env))), None)
    } else {
        // This tool uses a collection of simple files to store its state, and consequently
        // it is not generally safe to run multiple commands concurrently.  Rather than expect
        // all callers to know this, use a lock file to protect against concurrent execution.
        let _repo_lock = RepoLock::new().unwrap();

        // all other commands need an existing config
        let env = LocalEnv::load_config(&local_env::base_path()).context("Error loading config")?;
        let original_env = env.clone();
        let env = Box::leak(Box::new(env));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let subcommand_result = match cli.command {
            NeonLocalCmd::Init(_) => unreachable!("init was handled earlier already"),
            NeonLocalCmd::Start(args) => rt.block_on(handle_start_all(&args, env)),
            NeonLocalCmd::Stop(args) => rt.block_on(handle_stop_all(&args, env)),
            NeonLocalCmd::Tenant(subcmd) => rt.block_on(handle_tenant(&subcmd, env)),
            NeonLocalCmd::Timeline(subcmd) => rt.block_on(handle_timeline(&subcmd, env)),
            NeonLocalCmd::Pageserver(subcmd) => rt.block_on(handle_pageserver(&subcmd, env)),
            NeonLocalCmd::StorageController(subcmd) => {
                rt.block_on(handle_storage_controller(&subcmd, env))
            }
            NeonLocalCmd::StorageBroker(subcmd) => rt.block_on(handle_storage_broker(&subcmd, env)),
            NeonLocalCmd::Safekeeper(subcmd) => rt.block_on(handle_safekeeper(&subcmd, env)),
            NeonLocalCmd::Endpoint(subcmd) => rt.block_on(handle_endpoint(&subcmd, env)),
            NeonLocalCmd::Mappings(subcmd) => handle_mappings(&subcmd, env),
        };

        let subcommand_result = if &original_env != env {
            subcommand_result.map(|()| Some(Cow::Borrowed(env)))
        } else {
            subcommand_result.map(|()| None)
        };
        (subcommand_result, Some(_repo_lock))
    };

    match subcommand_result {
        Ok(Some(updated_env)) => updated_env.persist_config()?,
        Ok(None) => (),
        Err(e) => {
            eprintln!("command failed: {e:?}");
            exit(1);
        }
    }
    Ok(())
}

///
/// Prints timelines list as a tree-like structure.
///
fn print_timelines_tree(
    timelines: Vec<TimelineInfo>,
    mut timeline_name_mappings: HashMap<TenantTimelineId, String>,
) -> Result<()> {
    let mut timelines_hash = timelines
        .iter()
        .map(|t| {
            (
                t.timeline_id,
                TimelineTreeEl {
                    info: t.clone(),
                    children: BTreeSet::new(),
                    name: timeline_name_mappings
                        .remove(&TenantTimelineId::new(t.tenant_id.tenant_id, t.timeline_id)),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    // Memorize all direct children of each timeline.
    for timeline in timelines.iter() {
        if let Some(ancestor_timeline_id) = timeline.ancestor_timeline_id {
            timelines_hash
                .get_mut(&ancestor_timeline_id)
                .context("missing timeline info in the HashMap")?
                .children
                .insert(timeline.timeline_id);
        }
    }

    for timeline in timelines_hash.values() {
        // Start with root local timelines (no ancestors) first.
        if timeline.info.ancestor_timeline_id.is_none() {
            print_timeline(0, &Vec::from([true]), timeline, &timelines_hash)?;
        }
    }

    Ok(())
}

///
/// Recursively prints timeline info with all its children.
///
fn print_timeline(
    nesting_level: usize,
    is_last: &[bool],
    timeline: &TimelineTreeEl,
    timelines: &HashMap<TimelineId, TimelineTreeEl>,
) -> Result<()> {
    if nesting_level > 0 {
        let ancestor_lsn = match timeline.info.ancestor_lsn {
            Some(lsn) => lsn.to_string(),
            None => "Unknown Lsn".to_string(),
        };

        let mut br_sym = "┣━";

        // Draw each nesting padding with proper style
        // depending on whether its timeline ended or not.
        if nesting_level > 1 {
            for l in &is_last[1..is_last.len() - 1] {
                if *l {
                    print!("   ");
                } else {
                    print!("┃  ");
                }
            }
        }

        // We are the last in this sub-timeline
        if *is_last.last().unwrap() {
            br_sym = "┗━";
        }

        print!("{} @{}: ", br_sym, ancestor_lsn);
    }

    // Finally print a timeline id and name with new line
    println!(
        "{} [{}]",
        timeline.name.as_deref().unwrap_or("_no_name_"),
        timeline.info.timeline_id
    );

    let len = timeline.children.len();
    let mut i: usize = 0;
    let mut is_last_new = Vec::from(is_last);
    is_last_new.push(false);

    for child in &timeline.children {
        i += 1;

        // Mark that the last padding is the end of the timeline
        if i == len {
            if let Some(last) = is_last_new.last_mut() {
                *last = true;
            }
        }

        print_timeline(
            nesting_level + 1,
            &is_last_new,
            timelines
                .get(child)
                .context("missing timeline info in the HashMap")?,
            timelines,
        )?;
    }

    Ok(())
}

/// Returns a map of timeline IDs to timeline_id@lsn strings.
/// Connects to the pageserver to query this information.
async fn get_timeline_infos(
    env: &local_env::LocalEnv,
    tenant_shard_id: &TenantShardId,
) -> Result<HashMap<TimelineId, TimelineInfo>> {
    Ok(get_default_pageserver(env)
        .timeline_list(tenant_shard_id)
        .await?
        .into_iter()
        .map(|timeline_info| (timeline_info.timeline_id, timeline_info))
        .collect())
}

/// Helper function to get tenant id from an optional --tenant_id option or from the config file
fn get_tenant_id(
    tenant_id_arg: Option<TenantId>,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantId> {
    if let Some(tenant_id_from_arguments) = tenant_id_arg {
        Ok(tenant_id_from_arguments)
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(default_id)
    } else {
        anyhow::bail!("No tenant id. Use --tenant-id, or set a default tenant");
    }
}

/// Helper function to get tenant-shard ID from an optional --tenant_id option or from the config file,
/// for commands that accept a shard suffix
fn get_tenant_shard_id(
    tenant_shard_id_arg: Option<TenantShardId>,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantShardId> {
    if let Some(tenant_id_from_arguments) = tenant_shard_id_arg {
        Ok(tenant_id_from_arguments)
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(TenantShardId::unsharded(default_id))
    } else {
        anyhow::bail!("No tenant shard id. Use --tenant-id, or set a default tenant");
    }
}

fn handle_init(args: &InitCmdArgs) -> anyhow::Result<LocalEnv> {
    // Create the in-memory `LocalEnv` that we'd normally load from disk in `load_config`.
    let init_conf: NeonLocalInitConf = if let Some(config_path) = &args.config {
        // User (likely the Python test suite) provided a description of the environment.
        if args.num_pageservers.is_some() {
            bail!("Cannot specify both --num-pageservers and --config, use key `pageservers` in the --config file instead");
        }
        // load and parse the file
        let contents = std::fs::read_to_string(config_path).with_context(|| {
            format!(
                "Could not read configuration file '{}'",
                config_path.display()
            )
        })?;
        toml_edit::de::from_str(&contents)?
    } else {
        // User (likely interactive) did not provide a description of the environment, give them the default
        NeonLocalInitConf {
            control_plane_api: Some(DEFAULT_PAGESERVER_CONTROL_PLANE_API.parse().unwrap()),
            broker: NeonBroker {
                listen_addr: DEFAULT_BROKER_ADDR.parse().unwrap(),
            },
            safekeepers: vec![SafekeeperConf {
                id: DEFAULT_SAFEKEEPER_ID,
                pg_port: DEFAULT_SAFEKEEPER_PG_PORT,
                http_port: DEFAULT_SAFEKEEPER_HTTP_PORT,
                ..Default::default()
            }],
            pageservers: (0..args.num_pageservers.unwrap_or(1))
                .map(|i| {
                    let pageserver_id = NodeId(DEFAULT_PAGESERVER_ID.0 + i as u64);
                    let pg_port = DEFAULT_PAGESERVER_PG_PORT + i;
                    let http_port = DEFAULT_PAGESERVER_HTTP_PORT + i;
                    NeonLocalInitPageserverConf {
                        id: pageserver_id,
                        listen_pg_addr: format!("127.0.0.1:{pg_port}"),
                        listen_http_addr: format!("127.0.0.1:{http_port}"),
                        pg_auth_type: AuthType::Trust,
                        http_auth_type: AuthType::Trust,
                        other: Default::default(),
                        // Typical developer machines use disks with slow fsync, and we don't care
                        // about data integrity: disable disk syncs.
                        no_sync: true,
                    }
                })
                .collect(),
            pg_distrib_dir: None,
            neon_distrib_dir: None,
            default_tenant_id: TenantId::from_array(std::array::from_fn(|_| 0)),
            storage_controller: None,
            control_plane_compute_hook_api: None,
        }
    };

    LocalEnv::init(init_conf, &args.force)
        .context("materialize initial neon_local environment on disk")?;
    Ok(LocalEnv::load_config(&local_env::base_path())
        .expect("freshly written config should be loadable"))
}

/// The default pageserver is the one where CLI tenant/timeline operations are sent by default.
/// For typical interactive use, one would just run with a single pageserver.  Scenarios with
/// tenant/timeline placement across multiple pageservers are managed by python test code rather
/// than this CLI.
fn get_default_pageserver(env: &local_env::LocalEnv) -> PageServerNode {
    let ps_conf = env
        .pageservers
        .first()
        .expect("Config is validated to contain at least one pageserver");
    PageServerNode::from_env(env, ps_conf)
}

async fn handle_tenant(subcmd: &TenantCmd, env: &mut local_env::LocalEnv) -> anyhow::Result<()> {
    let pageserver = get_default_pageserver(env);
    match subcmd {
        TenantCmd::List => {
            for t in pageserver.tenant_list().await? {
                println!("{} {:?}", t.id, t.state);
            }
        }
        TenantCmd::Import(args) => {
            let tenant_id = args.tenant_id;

            let storage_controller = StorageController::from_env(env);
            let create_response = storage_controller.tenant_import(tenant_id).await?;

            let shard_zero = create_response
                .shards
                .first()
                .expect("Import response omitted shards");

            let attached_pageserver_id = shard_zero.node_id;
            let pageserver =
                PageServerNode::from_env(env, env.get_pageserver_conf(attached_pageserver_id)?);

            println!(
                "Imported tenant {tenant_id}, attached to pageserver {attached_pageserver_id}"
            );

            let timelines = pageserver
                .http_client
                .list_timelines(shard_zero.shard_id)
                .await?;

            // Pick a 'main' timeline that has no ancestors, the rest will get arbitrary names
            let main_timeline = timelines
                .iter()
                .find(|t| t.ancestor_timeline_id.is_none())
                .expect("No timelines found")
                .timeline_id;

            let mut branch_i = 0;
            for timeline in timelines.iter() {
                let branch_name = if timeline.timeline_id == main_timeline {
                    "main".to_string()
                } else {
                    branch_i += 1;
                    format!("branch_{branch_i}")
                };

                println!(
                    "Importing timeline {tenant_id}/{} as branch {branch_name}",
                    timeline.timeline_id
                );

                env.register_branch_mapping(branch_name, tenant_id, timeline.timeline_id)?;
            }
        }
        TenantCmd::Create(args) => {
            let tenant_conf: HashMap<_, _> =
                args.config.iter().flat_map(|c| c.split_once(':')).collect();

            let tenant_conf = PageServerNode::parse_config(tenant_conf)?;

            // If tenant ID was not specified, generate one
            let tenant_id = args.tenant_id.unwrap_or_else(TenantId::generate);

            // We must register the tenant with the storage controller, so
            // that when the pageserver restarts, it will be re-attached.
            let storage_controller = StorageController::from_env(env);
            storage_controller
                .tenant_create(TenantCreateRequest {
                    // Note that ::unsharded here isn't actually because the tenant is unsharded, its because the
                    // storage controller expects a shard-naive tenant_id in this attribute, and the TenantCreateRequest
                    // type is used both in the storage controller (for creating tenants) and in the pageserver (for
                    // creating shards)
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: None,
                    shard_parameters: ShardParameters {
                        count: ShardCount::new(args.shard_count),
                        stripe_size: args
                            .shard_stripe_size
                            .map(ShardStripeSize)
                            .unwrap_or(ShardParameters::DEFAULT_STRIPE_SIZE),
                    },
                    placement_policy: args.placement_policy.clone(),
                    config: tenant_conf,
                })
                .await?;
            println!("tenant {tenant_id} successfully created on the pageserver");

            // Create an initial timeline for the new tenant
            let new_timeline_id = args.timeline_id.unwrap_or(TimelineId::generate());

            // FIXME: passing None for ancestor_start_lsn is not kosher in a sharded world: we can't have
            // different shards picking different start lsns.  Maybe we have to teach storage controller
            // to let shard 0 branch first and then propagate the chosen LSN to other shards.
            storage_controller
                .tenant_timeline_create(
                    tenant_id,
                    TimelineCreateRequest {
                        new_timeline_id,
                        mode: pageserver_api::models::TimelineCreateRequestMode::Bootstrap {
                            existing_initdb_timeline_id: None,
                            pg_version: Some(args.pg_version),
                        },
                    },
                )
                .await?;

            env.register_branch_mapping(
                DEFAULT_BRANCH_NAME.to_string(),
                tenant_id,
                new_timeline_id,
            )?;

            println!("Created an initial timeline '{new_timeline_id}' for tenant: {tenant_id}",);

            if args.set_default {
                println!("Setting tenant {tenant_id} as a default one");
                env.default_tenant_id = Some(tenant_id);
            }
        }
        TenantCmd::SetDefault(args) => {
            println!("Setting tenant {} as a default one", args.tenant_id);
            env.default_tenant_id = Some(args.tenant_id);
        }
        TenantCmd::Config(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let tenant_conf: HashMap<_, _> =
                args.config.iter().flat_map(|c| c.split_once(':')).collect();

            pageserver
                .tenant_config(tenant_id, tenant_conf)
                .await
                .with_context(|| format!("Tenant config failed for tenant with id {tenant_id}"))?;
            println!("tenant {tenant_id} successfully configured on the pageserver");
        }
    }
    Ok(())
}

async fn handle_timeline(cmd: &TimelineCmd, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = get_default_pageserver(env);

    match cmd {
        TimelineCmd::List(args) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(args.tenant_shard_id, env)?;
            let timelines = pageserver.timeline_list(&tenant_shard_id).await?;
            print_timelines_tree(timelines, env.timeline_name_mappings())?;
        }
        TimelineCmd::Create(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let new_branch_name = &args.branch_name;
            let new_timeline_id_opt = args.timeline_id;
            let new_timeline_id = new_timeline_id_opt.unwrap_or(TimelineId::generate());

            let storage_controller = StorageController::from_env(env);
            let create_req = TimelineCreateRequest {
                new_timeline_id,
                mode: pageserver_api::models::TimelineCreateRequestMode::Bootstrap {
                    existing_initdb_timeline_id: None,
                    pg_version: Some(args.pg_version),
                },
            };
            let timeline_info = storage_controller
                .tenant_timeline_create(tenant_id, create_req)
                .await?;

            let last_record_lsn = timeline_info.last_record_lsn;
            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}",
                timeline_info.timeline_id
            );
        }
        // TODO: rename to import-basebackup-plus-wal
        TimelineCmd::Import(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let timeline_id = args.timeline_id;
            let branch_name = &args.branch_name;

            // Parse base inputs
            let base = (args.base_lsn, args.base_tarfile.clone());

            // Parse pg_wal inputs
            let wal_tarfile = args.wal_tarfile.clone();
            let end_lsn = args.end_lsn;
            // TODO validate both or none are provided
            let pg_wal = end_lsn.zip(wal_tarfile);

            println!("Importing timeline into pageserver ...");
            pageserver
                .timeline_import(tenant_id, timeline_id, base, pg_wal, args.pg_version)
                .await?;
            env.register_branch_mapping(branch_name.to_string(), tenant_id, timeline_id)?;
            println!("Done");
        }
        TimelineCmd::Branch(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let new_timeline_id = args.timeline_id.unwrap_or(TimelineId::generate());
            let new_branch_name = &args.branch_name;
            let ancestor_branch_name = args
                .ancestor_branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_owned());
            let ancestor_timeline_id = env
                .get_branch_timeline_id(&ancestor_branch_name, tenant_id)
                .ok_or_else(|| {
                    anyhow!("Found no timeline id for branch name '{ancestor_branch_name}'")
                })?;

            let start_lsn = args.ancestor_start_lsn;
            let storage_controller = StorageController::from_env(env);
            let create_req = TimelineCreateRequest {
                new_timeline_id,
                mode: pageserver_api::models::TimelineCreateRequestMode::Branch {
                    ancestor_timeline_id,
                    ancestor_start_lsn: start_lsn,
                    pg_version: None,
                },
            };
            let timeline_info = storage_controller
                .tenant_timeline_create(tenant_id, create_req)
                .await?;

            let last_record_lsn = timeline_info.last_record_lsn;

            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}. Ancestor timeline: '{ancestor_branch_name}'",
                timeline_info.timeline_id
            );
        }
    }

    Ok(())
}

async fn handle_endpoint(subcmd: &EndpointCmd, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match subcmd {
        EndpointCmd::List(args) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(args.tenant_shard_id, env)?;
            let timeline_infos = get_timeline_infos(env, &tenant_shard_id)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to load timeline info: {}", e);
                    HashMap::new()
                });

            let timeline_name_mappings = env.timeline_name_mappings();

            let mut table = comfy_table::Table::new();

            table.load_preset(comfy_table::presets::NOTHING);

            table.set_header([
                "ENDPOINT",
                "ADDRESS",
                "TIMELINE",
                "BRANCH NAME",
                "LSN",
                "STATUS",
            ]);

            for (endpoint_id, endpoint) in cplane
                .endpoints
                .iter()
                .filter(|(_, endpoint)| endpoint.tenant_id == tenant_shard_id.tenant_id)
            {
                let lsn_str = match endpoint.mode {
                    ComputeMode::Static(lsn) => {
                        // -> read-only endpoint
                        // Use the node's LSN.
                        lsn.to_string()
                    }
                    _ => {
                        // -> primary endpoint or hot replica
                        // Use the LSN at the end of the timeline.
                        timeline_infos
                            .get(&endpoint.timeline_id)
                            .map(|bi| bi.last_record_lsn.to_string())
                            .unwrap_or_else(|| "?".to_string())
                    }
                };

                let branch_name = timeline_name_mappings
                    .get(&TenantTimelineId::new(
                        tenant_shard_id.tenant_id,
                        endpoint.timeline_id,
                    ))
                    .map(|name| name.as_str())
                    .unwrap_or("?");

                table.add_row([
                    endpoint_id.as_str(),
                    &endpoint.pg_address.to_string(),
                    &endpoint.timeline_id.to_string(),
                    branch_name,
                    lsn_str.as_str(),
                    &format!("{}", endpoint.status()),
                ]);
            }

            println!("{table}");
        }
        EndpointCmd::Create(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let branch_name = args
                .branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_owned());
            let endpoint_id = args
                .endpoint_id
                .clone()
                .unwrap_or_else(|| format!("ep-{branch_name}"));

            let timeline_id = env
                .get_branch_timeline_id(&branch_name, tenant_id)
                .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))?;

            let mode = match (args.lsn, args.hot_standby) {
                (Some(lsn), false) => ComputeMode::Static(lsn),
                (None, true) => ComputeMode::Replica,
                (None, false) => ComputeMode::Primary,
                (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
            };

            match (mode, args.hot_standby) {
                (ComputeMode::Static(_), true) => {
                    bail!("Cannot start a node in hot standby mode when it is already configured as a static replica")
                }
                (ComputeMode::Primary, true) => {
                    bail!("Cannot start a node as a hot standby replica, it is already configured as primary node")
                }
                _ => {}
            }

            if !args.allow_multiple {
                cplane.check_conflicting_endpoints(mode, tenant_id, timeline_id)?;
            }

            cplane.new_endpoint(
                &endpoint_id,
                tenant_id,
                timeline_id,
                args.pg_port,
                args.http_port,
                args.pg_version,
                mode,
                !args.update_catalog,
            )?;
        }
        EndpointCmd::Start(args) => {
            let endpoint_id = &args.endpoint_id;
            let pageserver_id = args.endpoint_pageserver_id;
            let remote_ext_config = &args.remote_ext_config;

            // If --safekeepers argument is given, use only the listed
            // safekeeper nodes; otherwise all from the env.
            let safekeepers = if let Some(safekeepers) = parse_safekeepers(&args.safekeepers)? {
                safekeepers
            } else {
                env.safekeepers.iter().map(|sk| sk.id).collect()
            };

            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .ok_or_else(|| anyhow::anyhow!("endpoint {endpoint_id} not found"))?;

            if !args.allow_multiple {
                cplane.check_conflicting_endpoints(
                    endpoint.mode,
                    endpoint.tenant_id,
                    endpoint.timeline_id,
                )?;
            }

            let (pageservers, stripe_size) = if let Some(pageserver_id) = pageserver_id {
                let conf = env.get_pageserver_conf(pageserver_id).unwrap();
                let parsed = parse_host_port(&conf.listen_pg_addr).expect("Bad config");
                (
                    vec![(parsed.0, parsed.1.unwrap_or(5432))],
                    // If caller is telling us what pageserver to use, this is not a tenant which is
                    // full managed by storage controller, therefore not sharded.
                    ShardParameters::DEFAULT_STRIPE_SIZE,
                )
            } else {
                // Look up the currently attached location of the tenant, and its striping metadata,
                // to pass these on to postgres.
                let storage_controller = StorageController::from_env(env);
                let locate_result = storage_controller.tenant_locate(endpoint.tenant_id).await?;
                let pageservers = futures::future::try_join_all(
                    locate_result.shards.into_iter().map(|shard| async move {
                        if let ComputeMode::Static(lsn) = endpoint.mode {
                            // Initialize LSN leases for static computes.
                            let conf = env.get_pageserver_conf(shard.node_id).unwrap();
                            let pageserver = PageServerNode::from_env(env, conf);

                            pageserver
                                .http_client
                                .timeline_init_lsn_lease(shard.shard_id, endpoint.timeline_id, lsn)
                                .await?;
                        }

                        anyhow::Ok((
                            Host::parse(&shard.listen_pg_addr)
                                .expect("Storage controller reported bad hostname"),
                            shard.listen_pg_port,
                        ))
                    }),
                )
                .await?;
                let stripe_size = locate_result.shard_params.stripe_size;

                (pageservers, stripe_size)
            };
            assert!(!pageservers.is_empty());

            let ps_conf = env.get_pageserver_conf(DEFAULT_PAGESERVER_ID)?;
            let auth_token = if matches!(ps_conf.pg_auth_type, AuthType::NeonJWT) {
                let claims = Claims::new(Some(endpoint.tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            println!("Starting existing endpoint {endpoint_id}...");
            endpoint
                .start(
                    &auth_token,
                    safekeepers,
                    pageservers,
                    remote_ext_config.as_ref(),
                    stripe_size.0 as usize,
                    args.create_test_user,
                )
                .await?;
        }
        EndpointCmd::Reconfigure(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            let pageservers = if let Some(ps_id) = args.endpoint_pageserver_id {
                let pageserver = PageServerNode::from_env(env, env.get_pageserver_conf(ps_id)?);
                vec![(
                    pageserver.pg_connection_config.host().clone(),
                    pageserver.pg_connection_config.port(),
                )]
            } else {
                let storage_controller = StorageController::from_env(env);
                storage_controller
                    .tenant_locate(endpoint.tenant_id)
                    .await?
                    .shards
                    .into_iter()
                    .map(|shard| {
                        (
                            Host::parse(&shard.listen_pg_addr)
                                .expect("Storage controller reported malformed host"),
                            shard.listen_pg_port,
                        )
                    })
                    .collect::<Vec<_>>()
            };
            // If --safekeepers argument is given, use only the listed
            // safekeeper nodes; otherwise all from the env.
            let safekeepers = parse_safekeepers(&args.safekeepers)?;
            endpoint.reconfigure(pageservers, None, safekeepers).await?;
        }
        EndpointCmd::Stop(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id)
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            endpoint.stop(&args.mode, args.destroy)?;
        }
    }

    Ok(())
}

/// Parse --safekeepers as list of safekeeper ids.
fn parse_safekeepers(safekeepers_str: &Option<String>) -> Result<Option<Vec<NodeId>>> {
    if let Some(safekeepers_str) = safekeepers_str {
        let mut safekeepers: Vec<NodeId> = Vec::new();
        for sk_id in safekeepers_str.split(',').map(str::trim) {
            let sk_id = NodeId(
                u64::from_str(sk_id)
                    .map_err(|_| anyhow!("invalid node ID \"{sk_id}\" in --safekeepers list"))?,
            );
            safekeepers.push(sk_id);
        }
        Ok(Some(safekeepers))
    } else {
        Ok(None)
    }
}

fn handle_mappings(subcmd: &MappingsCmd, env: &mut local_env::LocalEnv) -> Result<()> {
    match subcmd {
        MappingsCmd::Map(args) => {
            env.register_branch_mapping(
                args.branch_name.to_owned(),
                args.tenant_id,
                args.timeline_id,
            )?;

            Ok(())
        }
    }
}

fn get_pageserver(
    env: &local_env::LocalEnv,
    pageserver_id_arg: Option<NodeId>,
) -> Result<PageServerNode> {
    let node_id = pageserver_id_arg.unwrap_or(DEFAULT_PAGESERVER_ID);

    Ok(PageServerNode::from_env(
        env,
        env.get_pageserver_conf(node_id)?,
    ))
}

async fn handle_pageserver(subcmd: &PageserverCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        PageserverCmd::Start(args) => {
            if let Err(e) = get_pageserver(env, args.pageserver_id)?
                .start(&args.start_timeout)
                .await
            {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Stop(args) => {
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = get_pageserver(env, args.pageserver_id)?.stop(immediate) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        PageserverCmd::Restart(args) => {
            let pageserver = get_pageserver(env, args.pageserver_id)?;
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = pageserver.start(&args.start_timeout).await {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Status(args) => {
            match get_pageserver(env, args.pageserver_id)?
                .check_status()
                .await
            {
                Ok(_) => println!("Page server is up and running"),
                Err(err) => {
                    eprintln!("Page server is not available: {}", err);
                    exit(1);
                }
            }
        }
    }
    Ok(())
}

async fn handle_storage_controller(
    subcmd: &StorageControllerCmd,
    env: &local_env::LocalEnv,
) -> Result<()> {
    let svc = StorageController::from_env(env);
    match subcmd {
        StorageControllerCmd::Start(args) => {
            let start_args = NeonStorageControllerStartArgs {
                instance_id: args.instance_id,
                base_port: args.base_port,
                start_timeout: args.start_timeout,
            };

            if let Err(e) = svc.start(start_args).await {
                eprintln!("start failed: {e}");
                exit(1);
            }
        }

        StorageControllerCmd::Stop(args) => {
            let stop_args = NeonStorageControllerStopArgs {
                instance_id: args.instance_id,
                immediate: match args.stop_mode {
                    StopMode::Fast => false,
                    StopMode::Immediate => true,
                },
            };
            if let Err(e) = svc.stop(stop_args).await {
                eprintln!("stop failed: {}", e);
                exit(1);
            }
        }
    }
    Ok(())
}

fn get_safekeeper(env: &local_env::LocalEnv, id: NodeId) -> Result<SafekeeperNode> {
    if let Some(node) = env.safekeepers.iter().find(|node| node.id == id) {
        Ok(SafekeeperNode::from_env(env, node))
    } else {
        bail!("could not find safekeeper {id}")
    }
}

async fn handle_safekeeper(subcmd: &SafekeeperCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        SafekeeperCmd::Start(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;

            if let Err(e) = safekeeper.start(&args.extra_opt, &args.start_timeout).await {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        SafekeeperCmd::Stop(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }
        }

        SafekeeperCmd::Restart(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = safekeeper.start(&args.extra_opt, &args.start_timeout).await {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }
    }
    Ok(())
}

async fn handle_storage_broker(subcmd: &StorageBrokerCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        StorageBrokerCmd::Start(args) => {
            if let Err(e) = broker::start_broker_process(env, &args.start_timeout).await {
                eprintln!("broker start failed: {e}");
                exit(1);
            }
        }

        StorageBrokerCmd::Stop(_args) => {
            // FIXME: stop_mode unused
            if let Err(e) = broker::stop_broker_process(env) {
                eprintln!("broker stop failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

async fn handle_start_all(
    args: &StartCmdArgs,
    env: &'static local_env::LocalEnv,
) -> anyhow::Result<()> {
    // FIXME: this was called "retry_timeout", is it right?
    let Err(errors) = handle_start_all_impl(env, args.timeout).await else {
        neon_start_status_check(env, args.timeout.as_ref())
            .await
            .context("status check after successful startup of all services")?;
        return Ok(());
    };

    eprintln!("startup failed because one or more services could not be started");

    for e in errors {
        eprintln!("{e}");
        let debug_repr = format!("{e:?}");
        for line in debug_repr.lines() {
            eprintln!("  {line}");
        }
    }

    try_stop_all(env, true).await;

    exit(2);
}

/// Returns Ok() if and only if all services could be started successfully.
/// Otherwise, returns the list of errors that occurred during startup.
async fn handle_start_all_impl(
    env: &'static local_env::LocalEnv,
    retry_timeout: humantime::Duration,
) -> Result<(), Vec<anyhow::Error>> {
    // Endpoints are not started automatically

    let mut js = JoinSet::new();

    // force infalliblity through closure
    #[allow(clippy::redundant_closure_call)]
    (|| {
        js.spawn(async move {
            let retry_timeout = retry_timeout;
            broker::start_broker_process(env, &retry_timeout).await
        });

        js.spawn(async move {
            let storage_controller = StorageController::from_env(env);
            storage_controller
                .start(NeonStorageControllerStartArgs::with_default_instance_id(
                    retry_timeout,
                ))
                .await
                .map_err(|e| e.context("start storage_controller"))
        });

        for ps_conf in &env.pageservers {
            js.spawn(async move {
                let pageserver = PageServerNode::from_env(env, ps_conf);
                pageserver
                    .start(&retry_timeout)
                    .await
                    .map_err(|e| e.context(format!("start pageserver {}", ps_conf.id)))
            });
        }

        for node in env.safekeepers.iter() {
            js.spawn(async move {
                let safekeeper = SafekeeperNode::from_env(env, node);
                safekeeper
                    .start(&[], &retry_timeout)
                    .await
                    .map_err(|e| e.context(format!("start safekeeper {}", safekeeper.id)))
            });
        }
    })();

    let mut errors = Vec::new();
    while let Some(result) = js.join_next().await {
        let result = result.expect("we don't panic or cancel the tasks");
        if let Err(e) = result {
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(())
}

async fn neon_start_status_check(
    env: &local_env::LocalEnv,
    retry_timeout: &Duration,
) -> anyhow::Result<()> {
    const RETRY_INTERVAL: Duration = Duration::from_millis(100);
    const NOTICE_AFTER_RETRIES: Duration = Duration::from_secs(5);

    let storcon = StorageController::from_env(env);

    let retries = retry_timeout.as_millis() / RETRY_INTERVAL.as_millis();
    let notice_after_retries = retry_timeout.as_millis() / NOTICE_AFTER_RETRIES.as_millis();

    println!("\nRunning neon status check");

    for retry in 0..retries {
        if retry == notice_after_retries {
            println!("\nNeon status check has not passed yet, continuing to wait")
        }

        let mut passed = true;
        let mut nodes = storcon.node_list().await?;
        let mut pageservers = env.pageservers.clone();

        if nodes.len() != pageservers.len() {
            continue;
        }

        nodes.sort_by_key(|ps| ps.id);
        pageservers.sort_by_key(|ps| ps.id);

        for (idx, pageserver) in pageservers.iter().enumerate() {
            let node = &nodes[idx];
            if node.id != pageserver.id {
                passed = false;
                break;
            }

            if !matches!(node.availability, NodeAvailabilityWrapper::Active) {
                passed = false;
                break;
            }
        }

        if passed {
            println!("\nNeon started and passed status check");
            return Ok(());
        }

        tokio::time::sleep(RETRY_INTERVAL).await;
    }

    anyhow::bail!("\nNeon passed status check")
}

async fn handle_stop_all(args: &StopCmdArgs, env: &local_env::LocalEnv) -> Result<()> {
    let immediate = match args.mode {
        StopMode::Fast => false,
        StopMode::Immediate => true,
    };

    try_stop_all(env, immediate).await;

    Ok(())
}

async fn try_stop_all(env: &local_env::LocalEnv, immediate: bool) {
    // Stop all endpoints
    match ComputeControlPlane::load(env.clone()) {
        Ok(cplane) => {
            for (_k, node) in cplane.endpoints {
                if let Err(e) = node.stop(if immediate { "immediate" } else { "fast" }, false) {
                    eprintln!("postgres stop failed: {e:#}");
                }
            }
        }
        Err(e) => {
            eprintln!("postgres stop failed, could not restore control plane data from env: {e:#}")
        }
    }

    for ps_conf in &env.pageservers {
        let pageserver = PageServerNode::from_env(env, ps_conf);
        if let Err(e) = pageserver.stop(immediate) {
            eprintln!("pageserver {} stop failed: {:#}", ps_conf.id, e);
        }
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.stop(immediate) {
            eprintln!("safekeeper {} stop failed: {:#}", safekeeper.id, e);
        }
    }

    if let Err(e) = broker::stop_broker_process(env) {
        eprintln!("neon broker stop failed: {e:#}");
    }

    // Stop all storage controller instances. In the most common case there's only one,
    // but iterate though the base data directory in order to discover the instances.
    let storcon_instances = env
        .storage_controller_instances()
        .await
        .expect("Must inspect data dir");
    for (instance_id, _instance_dir_path) in storcon_instances {
        let storage_controller = StorageController::from_env(env);
        let stop_args = NeonStorageControllerStopArgs {
            instance_id,
            immediate,
        };

        if let Err(e) = storage_controller.stop(stop_args).await {
            eprintln!("Storage controller instance {instance_id} stop failed: {e:#}");
        }
    }
}
