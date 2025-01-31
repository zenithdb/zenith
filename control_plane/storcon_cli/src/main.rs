use futures::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};

use clap::{Parser, Subcommand};
use pageserver_api::{
    controller_api::{
        AvailabilityZone, NodeAvailabilityWrapper, NodeDescribeResponse, NodeShardResponse,
        SafekeeperDescribeResponse, SafekeeperSchedulingPolicyRequest, ShardSchedulingPolicy,
        ShardsPreferredAzsRequest, ShardsPreferredAzsResponse, SkSchedulingPolicy,
        TenantCreateRequest, TenantDescribeResponse, TenantPolicyRequest,
    },
    models::{
        EvictionPolicy, EvictionPolicyLayerAccessThreshold, LocationConfigSecondary,
        ShardParameters, TenantConfig, TenantConfigPatchRequest, TenantConfigRequest,
        TenantShardSplitRequest, TenantShardSplitResponse,
    },
    shard::{ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api::{self};
use reqwest::{Method, StatusCode, Url};
use utils::id::{NodeId, TenantId};

use pageserver_api::controller_api::{
    NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy, PlacementPolicy,
    TenantShardMigrateRequest, TenantShardMigrateResponse,
};
use storage_controller_client::control_api::Client;

#[derive(Subcommand, Debug)]
enum Command {
    /// Register a pageserver with the storage controller.  This shouldn't usually be necessary,
    /// since pageservers auto-register when they start up
    NodeRegister {
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        listen_pg_addr: String,
        #[arg(long)]
        listen_pg_port: u16,

        #[arg(long)]
        listen_http_addr: String,
        #[arg(long)]
        listen_http_port: u16,
        #[arg(long)]
        availability_zone_id: String,
    },

    /// Modify a node's configuration in the storage controller
    NodeConfigure {
        #[arg(long)]
        node_id: NodeId,

        /// Availability is usually auto-detected based on heartbeats.  Set 'offline' here to
        /// manually mark a node offline
        #[arg(long)]
        availability: Option<NodeAvailabilityArg>,
        /// Scheduling policy controls whether tenant shards may be scheduled onto this node.
        #[arg(long)]
        scheduling: Option<NodeSchedulingPolicy>,
    },
    NodeDelete {
        #[arg(long)]
        node_id: NodeId,
    },
    /// Modify a tenant's policies in the storage controller
    TenantPolicy {
        #[arg(long)]
        tenant_id: TenantId,
        /// Placement policy controls whether a tenant is `detached`, has only a secondary location (`secondary`),
        /// or is in the normal attached state with N secondary locations (`attached:N`)
        #[arg(long)]
        placement: Option<PlacementPolicyArg>,
        /// Scheduling policy enables pausing the controller's scheduling activity involving this tenant.  `active` is normal,
        /// `essential` disables optimization scheduling changes, `pause` disables all scheduling changes, and `stop` prevents
        /// all reconciliation activity including for scheduling changes already made.  `pause` and `stop` can make a tenant
        /// unavailable, and are only for use in emergencies.
        #[arg(long)]
        scheduling: Option<ShardSchedulingPolicyArg>,
    },
    /// List nodes known to the storage controller
    Nodes {},
    /// List tenants known to the storage controller
    Tenants {
        /// If this field is set, it will list the tenants on a specific node
        node_id: Option<NodeId>,
    },
    /// Create a new tenant in the storage controller, and by extension on pageservers.
    TenantCreate {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Delete a tenant in the storage controller, and by extension on pageservers.
    TenantDelete {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Split an existing tenant into a higher number of shards than its current shard count.
    TenantShardSplit {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        shard_count: u8,
        /// Optional, in 8kiB pages.  e.g. set 2048 for 16MB stripes.
        #[arg(long)]
        stripe_size: Option<u32>,
    },
    /// Migrate the attached location for a tenant shard to a specific pageserver.
    TenantShardMigrate {
        #[arg(long)]
        tenant_shard_id: TenantShardId,
        #[arg(long)]
        node: NodeId,
    },
    /// Migrate the secondary location for a tenant shard to a specific pageserver.
    TenantShardMigrateSecondary {
        #[arg(long)]
        tenant_shard_id: TenantShardId,
        #[arg(long)]
        node: NodeId,
    },
    /// Cancel any ongoing reconciliation for this shard
    TenantShardCancelReconcile {
        #[arg(long)]
        tenant_shard_id: TenantShardId,
    },
    /// Set the pageserver tenant configuration of a tenant: this is the configuration structure
    /// that is passed through to pageservers, and does not affect storage controller behavior.
    /// Any previous tenant configs are overwritten.
    SetTenantConfig {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        config: String,
    },
    /// Patch the pageserver tenant configuration of a tenant. Any fields with null values in the
    /// provided JSON are unset from the tenant config and all fields with non-null values are set.
    /// Unspecified fields are not changed.
    PatchTenantConfig {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        config: String,
    },
    /// Print details about a particular tenant, including all its shards' states.
    TenantDescribe {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// For a tenant which hasn't been onboarded to the storage controller yet, add it in secondary
    /// mode so that it can warm up content on a pageserver.
    TenantWarmup {
        #[arg(long)]
        tenant_id: TenantId,
    },
    TenantSetPreferredAz {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        preferred_az: Option<String>,
    },
    /// Uncleanly drop a tenant from the storage controller: this doesn't delete anything from pageservers. Appropriate
    /// if you e.g. used `tenant-warmup` by mistake on a tenant ID that doesn't really exist, or is in some other region.
    TenantDrop {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        unclean: bool,
    },
    NodeDrop {
        #[arg(long)]
        node_id: NodeId,
        #[arg(long)]
        unclean: bool,
    },
    TenantSetTimeBasedEviction {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        period: humantime::Duration,
        #[arg(long)]
        threshold: humantime::Duration,
    },
    // Migrate away from a set of specified pageservers by moving the primary attachments to pageservers
    // outside of the specified set.
    BulkMigrate {
        // Set of pageserver node ids to drain.
        #[arg(long)]
        nodes: Vec<NodeId>,
        // Optional: migration concurrency (default is 8)
        #[arg(long)]
        concurrency: Option<usize>,
        // Optional: maximum number of shards to migrate
        #[arg(long)]
        max_shards: Option<usize>,
        // Optional: when set to true, nothing is migrated, but the plan is printed to stdout
        #[arg(long)]
        dry_run: Option<bool>,
    },
    /// Start draining the specified pageserver.
    /// The drain is complete when the schedulling policy returns to active.
    StartDrain {
        #[arg(long)]
        node_id: NodeId,
    },
    /// Cancel draining the specified pageserver and wait for `timeout`
    /// for the operation to be canceled. May be retried.
    CancelDrain {
        #[arg(long)]
        node_id: NodeId,
        #[arg(long)]
        timeout: humantime::Duration,
    },
    /// Start filling the specified pageserver.
    /// The drain is complete when the schedulling policy returns to active.
    StartFill {
        #[arg(long)]
        node_id: NodeId,
    },
    /// Cancel filling the specified pageserver and wait for `timeout`
    /// for the operation to be canceled. May be retried.
    CancelFill {
        #[arg(long)]
        node_id: NodeId,
        #[arg(long)]
        timeout: humantime::Duration,
    },
    /// List safekeepers known to the storage controller
    Safekeepers {},
    /// Set the scheduling policy of the specified safekeeper
    SafekeeperScheduling {
        #[arg(long)]
        node_id: NodeId,
        #[arg(long)]
        scheduling_policy: SkSchedulingPolicyArg,
    },
}

#[derive(Parser)]
#[command(
    author,
    version,
    about,
    long_about = "CLI for Storage Controller Support/Debug"
)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[arg(long)]
    /// URL to storage controller.  e.g. http://127.0.0.1:1234 when using `neon_local`
    api: Url,

    #[arg(long)]
    /// JWT token for authenticating with storage controller.  Depending on the API used, this
    /// should have either `pageserverapi` or `admin` scopes: for convenience, you should mint
    /// a token with both scopes to use with this tool.
    jwt: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone)]
struct PlacementPolicyArg(PlacementPolicy);

impl FromStr for PlacementPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "detached" => Ok(Self(PlacementPolicy::Detached)),
            "secondary" => Ok(Self(PlacementPolicy::Secondary)),
            _ if s.starts_with("attached:") => {
                let mut splitter = s.split(':');
                let _prefix = splitter.next().unwrap();
                match splitter.next().and_then(|s| s.parse::<usize>().ok()) {
                    Some(n) => Ok(Self(PlacementPolicy::Attached(n))),
                    None => Err(anyhow::anyhow!(
                        "Invalid format '{s}', a valid example is 'attached:1'"
                    )),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Unknown placement policy '{s}', try detached,secondary,attached:<n>"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct SkSchedulingPolicyArg(SkSchedulingPolicy);

impl FromStr for SkSchedulingPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SkSchedulingPolicy::from_str(s).map(Self)
    }
}

#[derive(Debug, Clone)]
struct ShardSchedulingPolicyArg(ShardSchedulingPolicy);

impl FromStr for ShardSchedulingPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self(ShardSchedulingPolicy::Active)),
            "essential" => Ok(Self(ShardSchedulingPolicy::Essential)),
            "pause" => Ok(Self(ShardSchedulingPolicy::Pause)),
            "stop" => Ok(Self(ShardSchedulingPolicy::Stop)),
            _ => Err(anyhow::anyhow!(
                "Unknown scheduling policy '{s}', try active,essential,pause,stop"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct NodeAvailabilityArg(NodeAvailabilityWrapper);

impl FromStr for NodeAvailabilityArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self(NodeAvailabilityWrapper::Active)),
            "offline" => Ok(Self(NodeAvailabilityWrapper::Offline)),
            _ => Err(anyhow::anyhow!("Unknown availability state '{s}'")),
        }
    }
}

async fn wait_for_scheduling_policy<F>(
    client: Client,
    node_id: NodeId,
    timeout: Duration,
    f: F,
) -> anyhow::Result<NodeSchedulingPolicy>
where
    F: Fn(NodeSchedulingPolicy) -> bool,
{
    let waiter = tokio::time::timeout(timeout, async move {
        loop {
            let node = client
                .dispatch::<(), NodeDescribeResponse>(
                    Method::GET,
                    format!("control/v1/node/{node_id}"),
                    None,
                )
                .await?;

            if f(node.scheduling) {
                return Ok::<NodeSchedulingPolicy, mgmt_api::Error>(node.scheduling);
            }
        }
    });

    Ok(waiter.await??)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storcon_client = Client::new(cli.api.clone(), cli.jwt.clone());

    let mut trimmed = cli.api.to_string();
    trimmed.pop();
    let vps_client = mgmt_api::Client::new(trimmed, cli.jwt.as_deref());

    match cli.command {
        Command::NodeRegister {
            node_id,
            listen_pg_addr,
            listen_pg_port,
            listen_http_addr,
            listen_http_port,
            availability_zone_id,
        } => {
            storcon_client
                .dispatch::<_, ()>(
                    Method::POST,
                    "control/v1/node".to_string(),
                    Some(NodeRegisterRequest {
                        node_id,
                        listen_pg_addr,
                        listen_pg_port,
                        listen_http_addr,
                        listen_http_port,
                        availability_zone_id: AvailabilityZone(availability_zone_id),
                    }),
                )
                .await?;
        }
        Command::TenantCreate { tenant_id } => {
            storcon_client
                .dispatch::<_, ()>(
                    Method::POST,
                    "v1/tenant".to_string(),
                    Some(TenantCreateRequest {
                        new_tenant_id: TenantShardId::unsharded(tenant_id),
                        generation: None,
                        shard_parameters: ShardParameters::default(),
                        placement_policy: Some(PlacementPolicy::Attached(1)),
                        config: TenantConfig::default(),
                    }),
                )
                .await?;
        }
        Command::TenantDelete { tenant_id } => {
            let status = vps_client
                .tenant_delete(TenantShardId::unsharded(tenant_id))
                .await?;
            tracing::info!("Delete status: {}", status);
        }
        Command::Nodes {} => {
            let mut resp = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;

            resp.sort_by(|a, b| a.listen_http_addr.cmp(&b.listen_http_addr));

            let mut table = comfy_table::Table::new();
            table.set_header(["Id", "Hostname", "AZ", "Scheduling", "Availability"]);
            for node in resp {
                table.add_row([
                    format!("{}", node.id),
                    node.listen_http_addr,
                    node.availability_zone_id,
                    format!("{:?}", node.scheduling),
                    format!("{:?}", node.availability),
                ]);
            }
            println!("{table}");
        }
        Command::NodeConfigure {
            node_id,
            availability,
            scheduling,
        } => {
            let req = NodeConfigureRequest {
                node_id,
                availability: availability.map(|a| a.0),
                scheduling,
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/node/{node_id}/config"),
                    Some(req),
                )
                .await?;
        }
        Command::Tenants {
            node_id: Some(node_id),
        } => {
            let describe_response = storcon_client
                .dispatch::<(), NodeShardResponse>(
                    Method::GET,
                    format!("control/v1/node/{node_id}/shards"),
                    None,
                )
                .await?;
            let shards = describe_response.shards;
            let mut table = comfy_table::Table::new();
            table.set_header([
                "Shard",
                "Intended Primary/Secondary",
                "Observed Primary/Secondary",
            ]);
            for shard in shards {
                table.add_row([
                    format!("{}", shard.tenant_shard_id),
                    match shard.is_intended_secondary {
                        None => "".to_string(),
                        Some(true) => "Secondary".to_string(),
                        Some(false) => "Primary".to_string(),
                    },
                    match shard.is_observed_secondary {
                        None => "".to_string(),
                        Some(true) => "Secondary".to_string(),
                        Some(false) => "Primary".to_string(),
                    },
                ]);
            }
            println!("{table}");
        }
        Command::Tenants { node_id: None } => {
            // Set up output formatting
            let mut table = comfy_table::Table::new();
            table.set_header([
                "TenantId",
                "Preferred AZ",
                "ShardCount",
                "StripeSize",
                "Placement",
                "Scheduling",
            ]);

            // Pagination loop over listing API
            let mut start_after = None;
            const LIMIT: usize = 1000;
            loop {
                let path = match start_after {
                    None => format!("control/v1/tenant?limit={LIMIT}"),
                    Some(start_after) => {
                        format!("control/v1/tenant?limit={LIMIT}&start_after={start_after}")
                    }
                };

                let resp = storcon_client
                    .dispatch::<(), Vec<TenantDescribeResponse>>(Method::GET, path, None)
                    .await?;

                if resp.is_empty() {
                    // End of data reached
                    break;
                }

                // Give some visual feedback while we're building up the table (comfy_table doesn't have
                // streaming output)
                if resp.len() >= LIMIT {
                    eprint!(".");
                }

                start_after = Some(resp.last().unwrap().tenant_id);

                for tenant in resp {
                    let shard_zero = tenant.shards.into_iter().next().unwrap();
                    table.add_row([
                        format!("{}", tenant.tenant_id),
                        shard_zero
                            .preferred_az_id
                            .as_ref()
                            .cloned()
                            .unwrap_or("".to_string()),
                        format!("{}", shard_zero.tenant_shard_id.shard_count.literal()),
                        format!("{:?}", tenant.stripe_size),
                        format!("{:?}", tenant.policy),
                        format!("{:?}", shard_zero.scheduling_policy),
                    ]);
                }
            }

            // Terminate progress dots
            if table.row_count() > LIMIT {
                eprint!("");
            }

            println!("{table}");
        }
        Command::TenantPolicy {
            tenant_id,
            placement,
            scheduling,
        } => {
            let req = TenantPolicyRequest {
                scheduling: scheduling.map(|s| s.0),
                placement: placement.map(|p| p.0),
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_id}/policy"),
                    Some(req),
                )
                .await?;
        }
        Command::TenantShardSplit {
            tenant_id,
            shard_count,
            stripe_size,
        } => {
            let req = TenantShardSplitRequest {
                new_shard_count: shard_count,
                new_stripe_size: stripe_size.map(ShardStripeSize),
            };

            let response = storcon_client
                .dispatch::<TenantShardSplitRequest, TenantShardSplitResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_id}/shard_split"),
                    Some(req),
                )
                .await?;
            println!(
                "Split tenant {} into {} shards: {}",
                tenant_id,
                shard_count,
                response
                    .new_shards
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        Command::TenantShardMigrate {
            tenant_shard_id,
            node,
        } => {
            let req = TenantShardMigrateRequest {
                node_id: node,
                migration_config: None,
            };

            storcon_client
                .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_shard_id}/migrate"),
                    Some(req),
                )
                .await?;
        }
        Command::TenantShardMigrateSecondary {
            tenant_shard_id,
            node,
        } => {
            let req = TenantShardMigrateRequest {
                node_id: node,
                migration_config: None,
            };

            storcon_client
                .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_shard_id}/migrate_secondary"),
                    Some(req),
                )
                .await?;
        }
        Command::TenantShardCancelReconcile { tenant_shard_id } => {
            storcon_client
                .dispatch::<(), ()>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_shard_id}/cancel_reconcile"),
                    None,
                )
                .await?;
        }
        Command::SetTenantConfig { tenant_id, config } => {
            let tenant_conf = serde_json::from_str(&config)?;

            vps_client
                .set_tenant_config(&TenantConfigRequest {
                    tenant_id,
                    config: tenant_conf,
                })
                .await?;
        }
        Command::PatchTenantConfig { tenant_id, config } => {
            let tenant_conf = serde_json::from_str(&config)?;

            vps_client
                .patch_tenant_config(&TenantConfigPatchRequest {
                    tenant_id,
                    config: tenant_conf,
                })
                .await?;
        }
        Command::TenantDescribe { tenant_id } => {
            let TenantDescribeResponse {
                tenant_id,
                shards,
                stripe_size,
                policy,
                config,
            } = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await?;

            let nodes = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;
            let nodes = nodes
                .into_iter()
                .map(|n| (n.id, n))
                .collect::<HashMap<_, _>>();

            println!("Tenant {tenant_id}");
            let mut table = comfy_table::Table::new();
            table.add_row(["Policy", &format!("{:?}", policy)]);
            table.add_row(["Stripe size", &format!("{:?}", stripe_size)]);
            table.add_row(["Config", &serde_json::to_string_pretty(&config).unwrap()]);
            println!("{table}");
            println!("Shards:");
            let mut table = comfy_table::Table::new();
            table.set_header([
                "Shard",
                "Attached",
                "Attached AZ",
                "Secondary",
                "Last error",
                "status",
            ]);
            for shard in shards {
                let secondary = shard
                    .node_secondary
                    .iter()
                    .map(|n| format!("{}", n))
                    .collect::<Vec<_>>()
                    .join(",");

                let mut status_parts = Vec::new();
                if shard.is_reconciling {
                    status_parts.push("reconciling");
                }

                if shard.is_pending_compute_notification {
                    status_parts.push("pending_compute");
                }

                if shard.is_splitting {
                    status_parts.push("splitting");
                }
                let status = status_parts.join(",");

                let attached_node = shard
                    .node_attached
                    .as_ref()
                    .map(|id| nodes.get(id).expect("Shard references nonexistent node"));

                table.add_row([
                    format!("{}", shard.tenant_shard_id),
                    attached_node
                        .map(|n| format!("{} ({})", n.listen_http_addr, n.id))
                        .unwrap_or(String::new()),
                    attached_node
                        .map(|n| n.availability_zone_id.clone())
                        .unwrap_or(String::new()),
                    secondary,
                    shard.last_error,
                    status,
                ]);
            }
            println!("{table}");
        }
        Command::TenantSetPreferredAz {
            tenant_id,
            preferred_az,
        } => {
            // First learn about the tenant's shards
            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await?;

            // Learn about nodes to validate the AZ ID
            let nodes = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;

            if let Some(preferred_az) = &preferred_az {
                let azs = nodes
                    .into_iter()
                    .map(|n| (n.availability_zone_id))
                    .collect::<HashSet<_>>();
                if !azs.contains(preferred_az) {
                    anyhow::bail!(
                        "AZ {} not found on any node: known AZs are: {:?}",
                        preferred_az,
                        azs
                    );
                }
            } else {
                // Make it obvious to the user that since they've omitted an AZ, we're clearing it
                eprintln!("Clearing preferred AZ for tenant {}", tenant_id);
            }

            // Construct a request that modifies all the tenant's shards
            let req = ShardsPreferredAzsRequest {
                preferred_az_ids: describe_response
                    .shards
                    .into_iter()
                    .map(|s| {
                        (
                            s.tenant_shard_id,
                            preferred_az.clone().map(AvailabilityZone),
                        )
                    })
                    .collect(),
            };
            storcon_client
                .dispatch::<ShardsPreferredAzsRequest, ShardsPreferredAzsResponse>(
                    Method::PUT,
                    "control/v1/preferred_azs".to_string(),
                    Some(req),
                )
                .await?;
        }
        Command::TenantWarmup { tenant_id } => {
            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await;
            match describe_response {
                Ok(describe) => {
                    if matches!(describe.policy, PlacementPolicy::Secondary) {
                        // Fine: it's already known to controller in secondary mode: calling
                        // again to put it into secondary mode won't cause problems.
                    } else {
                        anyhow::bail!("Tenant already present with policy {:?}", describe.policy);
                    }
                }
                Err(mgmt_api::Error::ApiError(StatusCode::NOT_FOUND, _)) => {
                    // Fine: this tenant isn't know to the storage controller yet.
                }
                Err(e) => {
                    // Unexpected API error
                    return Err(e.into());
                }
            }

            vps_client
                .location_config(
                    TenantShardId::unsharded(tenant_id),
                    pageserver_api::models::LocationConfig {
                        mode: pageserver_api::models::LocationConfigMode::Secondary,
                        generation: None,
                        secondary_conf: Some(LocationConfigSecondary { warm: true }),
                        shard_number: 0,
                        shard_count: 0,
                        shard_stripe_size: ShardParameters::DEFAULT_STRIPE_SIZE.0,
                        tenant_conf: TenantConfig::default(),
                    },
                    None,
                    true,
                )
                .await?;

            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await?;

            let secondary_ps_id = describe_response
                .shards
                .first()
                .unwrap()
                .node_secondary
                .first()
                .unwrap();

            println!("Tenant {tenant_id} warming up on pageserver {secondary_ps_id}");
            loop {
                let (status, progress) = vps_client
                    .tenant_secondary_download(
                        TenantShardId::unsharded(tenant_id),
                        Some(Duration::from_secs(10)),
                    )
                    .await?;
                println!(
                    "Progress: {}/{} layers, {}/{} bytes",
                    progress.layers_downloaded,
                    progress.layers_total,
                    progress.bytes_downloaded,
                    progress.bytes_total
                );
                match status {
                    StatusCode::OK => {
                        println!("Download complete");
                        break;
                    }
                    StatusCode::ACCEPTED => {
                        // Loop
                    }
                    _ => {
                        anyhow::bail!("Unexpected download status: {status}");
                    }
                }
            }
        }
        Command::TenantDrop { tenant_id, unclean } => {
            if !unclean {
                anyhow::bail!("This command is not a tenant deletion, and uncleanly drops all controller state for the tenant.  If you know what you're doing, add `--unclean` to proceed.")
            }
            storcon_client
                .dispatch::<(), ()>(
                    Method::POST,
                    format!("debug/v1/tenant/{tenant_id}/drop"),
                    None,
                )
                .await?;
        }
        Command::NodeDrop { node_id, unclean } => {
            if !unclean {
                anyhow::bail!("This command is not a clean node decommission, and uncleanly drops all controller state for the node, without checking if any tenants still refer to it.  If you know what you're doing, add `--unclean` to proceed.")
            }
            storcon_client
                .dispatch::<(), ()>(Method::POST, format!("debug/v1/node/{node_id}/drop"), None)
                .await?;
        }
        Command::NodeDelete { node_id } => {
            storcon_client
                .dispatch::<(), ()>(Method::DELETE, format!("control/v1/node/{node_id}"), None)
                .await?;
        }
        Command::TenantSetTimeBasedEviction {
            tenant_id,
            period,
            threshold,
        } => {
            vps_client
                .set_tenant_config(&TenantConfigRequest {
                    tenant_id,
                    config: TenantConfig {
                        eviction_policy: Some(EvictionPolicy::LayerAccessThreshold(
                            EvictionPolicyLayerAccessThreshold {
                                period: period.into(),
                                threshold: threshold.into(),
                            },
                        )),
                        heatmap_period: Some("300s".to_string()),
                        ..Default::default()
                    },
                })
                .await?;
        }
        Command::BulkMigrate {
            nodes,
            concurrency,
            max_shards,
            dry_run,
        } => {
            // Load the list of nodes, split them up into the drained and filled sets,
            // and validate that draining is possible.
            let node_descs = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;

            let mut node_to_drain_descs = Vec::new();
            let mut node_to_fill_descs = Vec::new();

            for desc in node_descs {
                let to_drain = nodes.iter().any(|id| *id == desc.id);
                if to_drain {
                    node_to_drain_descs.push(desc);
                } else {
                    node_to_fill_descs.push(desc);
                }
            }

            if nodes.len() != node_to_drain_descs.len() {
                anyhow::bail!("Bulk migration requested away from node which doesn't exist.")
            }

            node_to_fill_descs.retain(|desc| {
                matches!(desc.availability, NodeAvailabilityWrapper::Active)
                    && matches!(
                        desc.scheduling,
                        NodeSchedulingPolicy::Active | NodeSchedulingPolicy::Filling
                    )
            });

            if node_to_fill_descs.is_empty() {
                anyhow::bail!("There are no nodes to migrate to")
            }

            // Set the node scheduling policy to draining for the nodes which
            // we plan to drain.
            for node_desc in node_to_drain_descs.iter() {
                let req = NodeConfigureRequest {
                    node_id: node_desc.id,
                    availability: None,
                    scheduling: Some(NodeSchedulingPolicy::Draining),
                };

                storcon_client
                    .dispatch::<_, ()>(
                        Method::PUT,
                        format!("control/v1/node/{}/config", node_desc.id),
                        Some(req),
                    )
                    .await?;
            }

            // Perform the migration: move each tenant shard scheduled on a node to
            // be drained to a node which is being filled. A simple round robin
            // strategy is used to pick the new node.
            let tenants = storcon_client
                .dispatch::<(), Vec<TenantDescribeResponse>>(
                    Method::GET,
                    "control/v1/tenant".to_string(),
                    None,
                )
                .await?;

            let mut selected_node_idx = 0;

            struct MigrationMove {
                tenant_shard_id: TenantShardId,
                from: NodeId,
                to: NodeId,
            }

            let mut moves: Vec<MigrationMove> = Vec::new();

            let shards = tenants
                .into_iter()
                .flat_map(|tenant| tenant.shards.into_iter());
            for shard in shards {
                if let Some(max_shards) = max_shards {
                    if moves.len() >= max_shards {
                        println!(
                            "Stop planning shard moves since the requested maximum was reached"
                        );
                        break;
                    }
                }

                let should_migrate = {
                    if let Some(attached_to) = shard.node_attached {
                        node_to_drain_descs
                            .iter()
                            .map(|desc| desc.id)
                            .any(|id| id == attached_to)
                    } else {
                        false
                    }
                };

                if !should_migrate {
                    continue;
                }

                moves.push(MigrationMove {
                    tenant_shard_id: shard.tenant_shard_id,
                    from: shard
                        .node_attached
                        .expect("We only migrate attached tenant shards"),
                    to: node_to_fill_descs[selected_node_idx].id,
                });
                selected_node_idx = (selected_node_idx + 1) % node_to_fill_descs.len();
            }

            let total_moves = moves.len();

            if dry_run == Some(true) {
                println!("Dryrun requested. Planned {total_moves} moves:");
                for mv in &moves {
                    println!("{}: {} -> {}", mv.tenant_shard_id, mv.from, mv.to)
                }

                return Ok(());
            }

            const DEFAULT_MIGRATE_CONCURRENCY: usize = 8;
            let mut stream = futures::stream::iter(moves)
                .map(|mv| {
                    let client = Client::new(cli.api.clone(), cli.jwt.clone());
                    async move {
                        client
                            .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                                Method::PUT,
                                format!("control/v1/tenant/{}/migrate", mv.tenant_shard_id),
                                Some(TenantShardMigrateRequest {
                                    node_id: mv.to,
                                    migration_config: None,
                                }),
                            )
                            .await
                            .map_err(|e| (mv.tenant_shard_id, mv.from, mv.to, e))
                    }
                })
                .buffered(concurrency.unwrap_or(DEFAULT_MIGRATE_CONCURRENCY));

            let mut success = 0;
            let mut failure = 0;

            while let Some(res) = stream.next().await {
                match res {
                    Ok(_) => {
                        success += 1;
                    }
                    Err((tenant_shard_id, from, to, error)) => {
                        failure += 1;
                        println!(
                            "Failed to migrate {} from node {} to node {}: {}",
                            tenant_shard_id, from, to, error
                        );
                    }
                }

                if (success + failure) % 20 == 0 {
                    println!(
                        "Processed {}/{} shards: {} succeeded, {} failed",
                        success + failure,
                        total_moves,
                        success,
                        failure
                    );
                }
            }

            println!(
                "Processed {}/{} shards: {} succeeded, {} failed",
                success + failure,
                total_moves,
                success,
                failure
            );
        }
        Command::StartDrain { node_id } => {
            storcon_client
                .dispatch::<(), ()>(
                    Method::PUT,
                    format!("control/v1/node/{node_id}/drain"),
                    None,
                )
                .await?;
            println!("Drain started for {node_id}");
        }
        Command::CancelDrain { node_id, timeout } => {
            storcon_client
                .dispatch::<(), ()>(
                    Method::DELETE,
                    format!("control/v1/node/{node_id}/drain"),
                    None,
                )
                .await?;

            println!("Waiting for node {node_id} to quiesce on scheduling policy ...");

            let final_policy =
                wait_for_scheduling_policy(storcon_client, node_id, *timeout, |sched| {
                    use NodeSchedulingPolicy::*;
                    matches!(sched, Active | PauseForRestart)
                })
                .await?;

            println!(
                "Drain was cancelled for node {node_id}. Schedulling policy is now {final_policy:?}"
            );
        }
        Command::StartFill { node_id } => {
            storcon_client
                .dispatch::<(), ()>(Method::PUT, format!("control/v1/node/{node_id}/fill"), None)
                .await?;

            println!("Fill started for {node_id}");
        }
        Command::CancelFill { node_id, timeout } => {
            storcon_client
                .dispatch::<(), ()>(
                    Method::DELETE,
                    format!("control/v1/node/{node_id}/fill"),
                    None,
                )
                .await?;

            println!("Waiting for node {node_id} to quiesce on scheduling policy ...");

            let final_policy =
                wait_for_scheduling_policy(storcon_client, node_id, *timeout, |sched| {
                    use NodeSchedulingPolicy::*;
                    matches!(sched, Active)
                })
                .await?;

            println!(
                "Fill was cancelled for node {node_id}. Schedulling policy is now {final_policy:?}"
            );
        }
        Command::Safekeepers {} => {
            let mut resp = storcon_client
                .dispatch::<(), Vec<SafekeeperDescribeResponse>>(
                    Method::GET,
                    "control/v1/safekeeper".to_string(),
                    None,
                )
                .await?;

            resp.sort_by(|a, b| a.id.cmp(&b.id));

            let mut table = comfy_table::Table::new();
            table.set_header([
                "Id",
                "Version",
                "Host",
                "Port",
                "Http Port",
                "AZ Id",
                "Scheduling",
            ]);
            for sk in resp {
                table.add_row([
                    format!("{}", sk.id),
                    format!("{}", sk.version),
                    sk.host,
                    format!("{}", sk.port),
                    format!("{}", sk.http_port),
                    sk.availability_zone_id.clone(),
                    String::from(sk.scheduling_policy),
                ]);
            }
            println!("{table}");
        }
        Command::SafekeeperScheduling {
            node_id,
            scheduling_policy,
        } => {
            let scheduling_policy = scheduling_policy.0;
            storcon_client
                .dispatch::<SafekeeperSchedulingPolicyRequest, ()>(
                    Method::POST,
                    format!("control/v1/safekeeper/{node_id}/scheduling_policy"),
                    Some(SafekeeperSchedulingPolicyRequest { scheduling_policy }),
                )
                .await?;
            println!(
                "Scheduling policy of {node_id} set to {}",
                String::from(scheduling_policy)
            );
        }
    }

    Ok(())
}
