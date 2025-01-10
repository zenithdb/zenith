//! Functions for handling per-tenant configuration options
//!
//! If tenant is created with --config option,
//! the tenant-specific config will be stored in tenant's directory.
//! Otherwise, global pageserver's config is used.
//!
//! If the tenant config file is corrupted, the tenant will be disabled.
//! We cannot use global or default config instead, because wrong settings
//! may lead to a data loss.
//!
pub(crate) use pageserver_api::config::TenantConfigToml as TenantConf;
use pageserver_api::models::CompactionAlgorithmSettings;
use pageserver_api::models::EvictionPolicy;
use pageserver_api::models::{self, TenantConfigPatch, ThrottleConfig};
use pageserver_api::shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroU64;
use std::time::Duration;
use utils::generation::Generation;
use utils::postgres_client::PostgresClientProtocol;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum AttachmentMode {
    /// Our generation is current as far as we know, and as far as we know we are the only attached
    /// pageserver.  This is the "normal" attachment mode.
    Single,
    /// Our generation number is current as far as we know, but we are advised that another
    /// pageserver is still attached, and therefore to avoid executing deletions.   This is
    /// the attachment mode of a pagesever that is the destination of a migration.
    Multi,
    /// Our generation number is superseded, or about to be superseded.  We are advised
    /// to avoid remote storage writes if possible, and to avoid sending billing data.  This
    /// is the attachment mode of a pageserver that is the origin of a migration.
    Stale,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachedLocationConfig {
    pub(crate) generation: Generation,
    pub(crate) attach_mode: AttachmentMode,
    // TODO: add a flag to override AttachmentMode's policies under
    // disk pressure (i.e. unblock uploads under disk pressure in Stale
    // state, unblock deletions after timeout in Multi state)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SecondaryLocationConfig {
    /// If true, keep the local cache warm by polling remote storage
    pub(crate) warm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum LocationMode {
    Attached(AttachedLocationConfig),
    Secondary(SecondaryLocationConfig),
}

/// Per-tenant, per-pageserver configuration.  All pageservers use the same TenantConf,
/// but have distinct LocationConf.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LocationConf {
    /// The location-specific part of the configuration, describes the operating
    /// mode of this pageserver for this tenant.
    pub(crate) mode: LocationMode,

    /// The detailed shard identity.  This structure is already scoped within
    /// a TenantShardId, but we need the full ShardIdentity to enable calculating
    /// key->shard mappings.
    #[serde(default = "ShardIdentity::unsharded")]
    #[serde(skip_serializing_if = "ShardIdentity::is_unsharded")]
    pub(crate) shard: ShardIdentity,

    /// The pan-cluster tenant configuration, the same on all locations
    pub(crate) tenant_conf: TenantConfOpt,
}

impl std::fmt::Debug for LocationConf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.mode {
            LocationMode::Attached(conf) => {
                write!(
                    f,
                    "Attached {:?}, gen={:?}",
                    conf.attach_mode, conf.generation
                )
            }
            LocationMode::Secondary(conf) => {
                write!(f, "Secondary, warm={}", conf.warm)
            }
        }
    }
}

impl AttachedLocationConfig {
    /// Consult attachment mode to determine whether we are currently permitted
    /// to delete layers.  This is only advisory, not required for data safety.
    /// See [`AttachmentMode`] for more context.
    pub(crate) fn may_delete_layers_hint(&self) -> bool {
        // TODO: add an override for disk pressure in AttachedLocationConfig,
        // and respect it here.
        match &self.attach_mode {
            AttachmentMode::Single => true,
            AttachmentMode::Multi | AttachmentMode::Stale => {
                // In Multi mode we avoid doing deletions because some other
                // attached pageserver might get 404 while trying to read
                // a layer we delete which is still referenced in their metadata.
                //
                // In Stale mode, we avoid doing deletions because we expect
                // that they would ultimately fail validation in the deletion
                // queue due to our stale generation.
                false
            }
        }
    }

    /// Whether we are currently hinted that it is worthwhile to upload layers.
    /// This is only advisory, not required for data safety.
    /// See [`AttachmentMode`] for more context.
    pub(crate) fn may_upload_layers_hint(&self) -> bool {
        // TODO: add an override for disk pressure in AttachedLocationConfig,
        // and respect it here.
        match &self.attach_mode {
            AttachmentMode::Single | AttachmentMode::Multi => true,
            AttachmentMode::Stale => {
                // In Stale mode, we avoid doing uploads because we expect that
                // our replacement pageserver will already have started its own
                // IndexPart that will never reference layers we upload: it is
                // wasteful.
                false
            }
        }
    }
}

impl LocationConf {
    /// For use when loading from a legacy configuration: presence of a tenant
    /// implies it is in AttachmentMode::Single, which used to be the only
    /// possible state.  This function should eventually be removed.
    pub(crate) fn attached_single(
        tenant_conf: TenantConfOpt,
        generation: Generation,
        shard_params: &models::ShardParameters,
    ) -> Self {
        Self {
            mode: LocationMode::Attached(AttachedLocationConfig {
                generation,
                attach_mode: AttachmentMode::Single,
            }),
            shard: ShardIdentity::from_params(ShardNumber(0), shard_params),
            tenant_conf,
        }
    }

    /// For use when attaching/re-attaching: update the generation stored in this
    /// structure.  If we were in a secondary state, promote to attached (posession
    /// of a fresh generation implies this).
    pub(crate) fn attach_in_generation(&mut self, mode: AttachmentMode, generation: Generation) {
        match &mut self.mode {
            LocationMode::Attached(attach_conf) => {
                attach_conf.generation = generation;
                attach_conf.attach_mode = mode;
            }
            LocationMode::Secondary(_) => {
                // We are promoted to attached by the control plane's re-attach response
                self.mode = LocationMode::Attached(AttachedLocationConfig {
                    generation,
                    attach_mode: mode,
                })
            }
        }
    }

    pub(crate) fn try_from(conf: &'_ models::LocationConfig) -> anyhow::Result<Self> {
        let tenant_conf = TenantConfOpt::try_from(&conf.tenant_conf)?;

        fn get_generation(conf: &'_ models::LocationConfig) -> Result<Generation, anyhow::Error> {
            conf.generation
                .map(Generation::new)
                .ok_or_else(|| anyhow::anyhow!("Generation must be set when attaching"))
        }

        let mode = match &conf.mode {
            models::LocationConfigMode::AttachedMulti => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Multi,
                })
            }
            models::LocationConfigMode::AttachedSingle => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Single,
                })
            }
            models::LocationConfigMode::AttachedStale => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Stale,
                })
            }
            models::LocationConfigMode::Secondary => {
                anyhow::ensure!(conf.generation.is_none());

                let warm = conf
                    .secondary_conf
                    .as_ref()
                    .map(|c| c.warm)
                    .unwrap_or(false);
                LocationMode::Secondary(SecondaryLocationConfig { warm })
            }
            models::LocationConfigMode::Detached => {
                // Should not have been called: API code should translate this mode
                // into a detach rather than trying to decode it as a LocationConf
                return Err(anyhow::anyhow!("Cannot decode a Detached configuration"));
            }
        };

        let shard = if conf.shard_count == 0 {
            ShardIdentity::unsharded()
        } else {
            ShardIdentity::new(
                ShardNumber(conf.shard_number),
                ShardCount::new(conf.shard_count),
                ShardStripeSize(conf.shard_stripe_size),
            )?
        };

        Ok(Self {
            shard,
            mode,
            tenant_conf,
        })
    }
}

impl Default for LocationConf {
    // TODO: this should be removed once tenant loading can guarantee that we are never
    // loading from a directory without a configuration.
    // => tech debt since https://github.com/neondatabase/neon/issues/1555
    fn default() -> Self {
        Self {
            mode: LocationMode::Attached(AttachedLocationConfig {
                generation: Generation::none(),
                attach_mode: AttachmentMode::Single,
            }),
            tenant_conf: TenantConfOpt::default(),
            shard: ShardIdentity::unsharded(),
        }
    }
}

/// Same as TenantConf, but this struct preserves the information about
/// which parameters are set and which are not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TenantConfOpt {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub checkpoint_distance: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub checkpoint_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub compaction_target_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub compaction_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub compaction_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub compaction_algorithm: Option<CompactionAlgorithmSettings>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub gc_horizon: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub gc_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub image_creation_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub pitr_interval: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub walreceiver_connect_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub lagging_wal_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub max_lsn_wal_lag: Option<NonZeroU64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub eviction_policy: Option<EvictionPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub min_resident_size_override: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub evictions_low_residence_duration_metric_threshold: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub heatmap_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub lazy_slru_download: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline_get_throttle: Option<pageserver_api::models::ThrottleConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_layer_creation_check_threshold: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub lsn_lease_length: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub lsn_lease_length_for_ts: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub timeline_offloading: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_receiver_protocol_override: Option<PostgresClientProtocol>,
}

impl TenantConfOpt {
    pub fn merge(&self, global_conf: TenantConf) -> TenantConf {
        TenantConf {
            checkpoint_distance: self
                .checkpoint_distance
                .unwrap_or(global_conf.checkpoint_distance),
            checkpoint_timeout: self
                .checkpoint_timeout
                .unwrap_or(global_conf.checkpoint_timeout),
            compaction_target_size: self
                .compaction_target_size
                .unwrap_or(global_conf.compaction_target_size),
            compaction_period: self
                .compaction_period
                .unwrap_or(global_conf.compaction_period),
            compaction_threshold: self
                .compaction_threshold
                .unwrap_or(global_conf.compaction_threshold),
            compaction_algorithm: self
                .compaction_algorithm
                .as_ref()
                .unwrap_or(&global_conf.compaction_algorithm)
                .clone(),
            gc_horizon: self.gc_horizon.unwrap_or(global_conf.gc_horizon),
            gc_period: self.gc_period.unwrap_or(global_conf.gc_period),
            image_creation_threshold: self
                .image_creation_threshold
                .unwrap_or(global_conf.image_creation_threshold),
            pitr_interval: self.pitr_interval.unwrap_or(global_conf.pitr_interval),
            walreceiver_connect_timeout: self
                .walreceiver_connect_timeout
                .unwrap_or(global_conf.walreceiver_connect_timeout),
            lagging_wal_timeout: self
                .lagging_wal_timeout
                .unwrap_or(global_conf.lagging_wal_timeout),
            max_lsn_wal_lag: self.max_lsn_wal_lag.unwrap_or(global_conf.max_lsn_wal_lag),
            eviction_policy: self.eviction_policy.unwrap_or(global_conf.eviction_policy),
            min_resident_size_override: self
                .min_resident_size_override
                .or(global_conf.min_resident_size_override),
            evictions_low_residence_duration_metric_threshold: self
                .evictions_low_residence_duration_metric_threshold
                .unwrap_or(global_conf.evictions_low_residence_duration_metric_threshold),
            heatmap_period: self.heatmap_period.unwrap_or(global_conf.heatmap_period),
            lazy_slru_download: self
                .lazy_slru_download
                .unwrap_or(global_conf.lazy_slru_download),
            timeline_get_throttle: self
                .timeline_get_throttle
                .clone()
                .unwrap_or(global_conf.timeline_get_throttle),
            image_layer_creation_check_threshold: self
                .image_layer_creation_check_threshold
                .unwrap_or(global_conf.image_layer_creation_check_threshold),
            lsn_lease_length: self
                .lsn_lease_length
                .unwrap_or(global_conf.lsn_lease_length),
            lsn_lease_length_for_ts: self
                .lsn_lease_length_for_ts
                .unwrap_or(global_conf.lsn_lease_length_for_ts),
            timeline_offloading: self
                .lazy_slru_download
                .unwrap_or(global_conf.timeline_offloading),
            wal_receiver_protocol_override: self
                .wal_receiver_protocol_override
                .or(global_conf.wal_receiver_protocol_override),
        }
    }

    pub fn apply_patch(self, patch: TenantConfigPatch) -> anyhow::Result<TenantConfOpt> {
        let Self {
            mut checkpoint_distance,
            mut checkpoint_timeout,
            mut compaction_target_size,
            mut compaction_period,
            mut compaction_threshold,
            mut compaction_algorithm,
            mut gc_horizon,
            mut gc_period,
            mut image_creation_threshold,
            mut pitr_interval,
            mut walreceiver_connect_timeout,
            mut lagging_wal_timeout,
            mut max_lsn_wal_lag,
            mut eviction_policy,
            mut min_resident_size_override,
            mut evictions_low_residence_duration_metric_threshold,
            mut heatmap_period,
            mut lazy_slru_download,
            mut timeline_get_throttle,
            mut image_layer_creation_check_threshold,
            mut lsn_lease_length,
            mut lsn_lease_length_for_ts,
            mut timeline_offloading,
            mut wal_receiver_protocol_override,
        } = self;

        patch.checkpoint_distance.apply(&mut checkpoint_distance);
        patch
            .checkpoint_timeout
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut checkpoint_timeout);
        patch
            .compaction_target_size
            .apply(&mut compaction_target_size);
        patch
            .compaction_period
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut compaction_period);
        patch.compaction_threshold.apply(&mut compaction_threshold);
        patch.compaction_algorithm.apply(&mut compaction_algorithm);
        patch.gc_horizon.apply(&mut gc_horizon);
        patch
            .gc_period
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut gc_period);
        patch
            .image_creation_threshold
            .apply(&mut image_creation_threshold);
        patch
            .pitr_interval
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut pitr_interval);
        patch
            .walreceiver_connect_timeout
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut walreceiver_connect_timeout);
        patch
            .lagging_wal_timeout
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut lagging_wal_timeout);
        patch.max_lsn_wal_lag.apply(&mut max_lsn_wal_lag);
        patch.eviction_policy.apply(&mut eviction_policy);
        patch
            .min_resident_size_override
            .apply(&mut min_resident_size_override);
        patch
            .evictions_low_residence_duration_metric_threshold
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut evictions_low_residence_duration_metric_threshold);
        patch
            .heatmap_period
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut heatmap_period);
        patch.lazy_slru_download.apply(&mut lazy_slru_download);
        patch
            .timeline_get_throttle
            .apply(&mut timeline_get_throttle);
        patch
            .image_layer_creation_check_threshold
            .apply(&mut image_layer_creation_check_threshold);
        patch
            .lsn_lease_length
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut lsn_lease_length);
        patch
            .lsn_lease_length_for_ts
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut lsn_lease_length_for_ts);
        patch.timeline_offloading.apply(&mut timeline_offloading);
        patch
            .wal_receiver_protocol_override
            .apply(&mut wal_receiver_protocol_override);

        Ok(Self {
            checkpoint_distance,
            checkpoint_timeout,
            compaction_target_size,
            compaction_period,
            compaction_threshold,
            compaction_algorithm,
            gc_horizon,
            gc_period,
            image_creation_threshold,
            pitr_interval,
            walreceiver_connect_timeout,
            lagging_wal_timeout,
            max_lsn_wal_lag,
            eviction_policy,
            min_resident_size_override,
            evictions_low_residence_duration_metric_threshold,
            heatmap_period,
            lazy_slru_download,
            timeline_get_throttle,
            image_layer_creation_check_threshold,
            lsn_lease_length,
            lsn_lease_length_for_ts,
            timeline_offloading,
            wal_receiver_protocol_override,
        })
    }
}

impl TryFrom<&'_ models::TenantConfig> for TenantConfOpt {
    type Error = anyhow::Error;

    fn try_from(request_data: &'_ models::TenantConfig) -> Result<Self, Self::Error> {
        // Convert the request_data to a JSON Value
        let json_value: Value = serde_json::to_value(request_data)?;

        // Create a Deserializer from the JSON Value
        let deserializer = json_value.into_deserializer();

        // Use serde_path_to_error to deserialize the JSON Value into TenantConfOpt
        let tenant_conf: TenantConfOpt = serde_path_to_error::deserialize(deserializer)?;

        Ok(tenant_conf)
    }
}

/// This is a conversion from our internal tenant config object to the one used
/// in external APIs.
impl From<TenantConfOpt> for models::TenantConfig {
    fn from(value: TenantConfOpt) -> Self {
        fn humantime(d: Duration) -> String {
            format!("{}s", d.as_secs())
        }
        Self {
            checkpoint_distance: value.checkpoint_distance,
            checkpoint_timeout: value.checkpoint_timeout.map(humantime),
            compaction_algorithm: value.compaction_algorithm,
            compaction_target_size: value.compaction_target_size,
            compaction_period: value.compaction_period.map(humantime),
            compaction_threshold: value.compaction_threshold,
            gc_horizon: value.gc_horizon,
            gc_period: value.gc_period.map(humantime),
            image_creation_threshold: value.image_creation_threshold,
            pitr_interval: value.pitr_interval.map(humantime),
            walreceiver_connect_timeout: value.walreceiver_connect_timeout.map(humantime),
            lagging_wal_timeout: value.lagging_wal_timeout.map(humantime),
            max_lsn_wal_lag: value.max_lsn_wal_lag,
            eviction_policy: value.eviction_policy,
            min_resident_size_override: value.min_resident_size_override,
            evictions_low_residence_duration_metric_threshold: value
                .evictions_low_residence_duration_metric_threshold
                .map(humantime),
            heatmap_period: value.heatmap_period.map(humantime),
            lazy_slru_download: value.lazy_slru_download,
            timeline_get_throttle: value.timeline_get_throttle.map(ThrottleConfig::from),
            image_layer_creation_check_threshold: value.image_layer_creation_check_threshold,
            lsn_lease_length: value.lsn_lease_length.map(humantime),
            lsn_lease_length_for_ts: value.lsn_lease_length_for_ts.map(humantime),
            timeline_offloading: value.timeline_offloading,
            wal_receiver_protocol_override: value.wal_receiver_protocol_override,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use models::TenantConfig;

    #[test]
    fn de_serializing_pageserver_config_omits_empty_values() {
        let small_conf = TenantConfOpt {
            gc_horizon: Some(42),
            ..TenantConfOpt::default()
        };

        let toml_form = toml_edit::ser::to_string(&small_conf).unwrap();
        assert_eq!(toml_form, "gc_horizon = 42\n");
        assert_eq!(small_conf, toml_edit::de::from_str(&toml_form).unwrap());

        let json_form = serde_json::to_string(&small_conf).unwrap();
        assert_eq!(json_form, "{\"gc_horizon\":42}");
        assert_eq!(small_conf, serde_json::from_str(&json_form).unwrap());
    }

    #[test]
    fn test_try_from_models_tenant_config_err() {
        let tenant_config = models::TenantConfig {
            lagging_wal_timeout: Some("5a".to_string()),
            ..TenantConfig::default()
        };

        let tenant_conf_opt = TenantConfOpt::try_from(&tenant_config);

        assert!(
            tenant_conf_opt.is_err(),
            "Suceeded to convert TenantConfig to TenantConfOpt"
        );

        let expected_error_str =
            "lagging_wal_timeout: invalid value: string \"5a\", expected a duration";
        assert_eq!(tenant_conf_opt.unwrap_err().to_string(), expected_error_str);
    }

    #[test]
    fn test_try_from_models_tenant_config_success() {
        let tenant_config = models::TenantConfig {
            lagging_wal_timeout: Some("5s".to_string()),
            ..TenantConfig::default()
        };

        let tenant_conf_opt = TenantConfOpt::try_from(&tenant_config).unwrap();

        assert_eq!(
            tenant_conf_opt.lagging_wal_timeout,
            Some(Duration::from_secs(5))
        );
    }
}
