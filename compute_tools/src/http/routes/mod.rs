use compute_api::responses::ComputeStatusResponse;

use crate::compute::ComputeState;

pub(in crate::http) mod check_writability;
pub(in crate::http) mod configure;
pub(in crate::http) mod database_schema;
pub(in crate::http) mod dbs_and_roles;
pub(in crate::http) mod extension_server;
pub(in crate::http) mod extensions;
pub(in crate::http) mod failpoints;
pub(in crate::http) mod grants;
pub(in crate::http) mod info;
pub(in crate::http) mod insights;
pub(in crate::http) mod installed_extensions;
pub(in crate::http) mod metrics;
pub(in crate::http) mod metrics_json;
pub(in crate::http) mod status;
pub(in crate::http) mod terminate;

impl From<&ComputeState> for ComputeStatusResponse {
    fn from(state: &ComputeState) -> Self {
        ComputeStatusResponse {
            start_time: state.start_time,
            tenant: state
                .pspec
                .as_ref()
                .map(|pspec| pspec.tenant_id.to_string()),
            timeline: state
                .pspec
                .as_ref()
                .map(|pspec| pspec.timeline_id.to_string()),
            status: state.status,
            last_active: state.last_active,
            error: state.error.clone(),
        }
    }
}
