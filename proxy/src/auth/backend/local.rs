use std::net::SocketAddr;

use arc_swap::ArcSwapOption;
use tokio::sync::Semaphore;

use super::jwt::{AuthRule, FetchAuthRules};
use crate::auth::backend::jwt::FetchAuthRulesError;
use crate::compute::ConnCfg;
use crate::compute_ctl::ComputeCtlApi;
use crate::context::RequestContext;
use crate::control_plane::messages::{ColdStartInfo, EndpointJwksResponse, MetricsAuxInfo};
use crate::control_plane::NodeInfo;
use crate::http;
use crate::intern::{BranchIdTag, EndpointIdTag, InternId, ProjectIdTag};
use crate::types::EndpointId;
use crate::url::ApiUrl;

pub struct LocalBackend {
    pub(crate) initialize: Semaphore,
    pub(crate) compute_ctl: ComputeCtlApi,
    pub(crate) node_info: NodeInfo,
}

impl LocalBackend {
    pub fn new(postgres_addr: SocketAddr, compute_ctl: ApiUrl) -> Self {
        LocalBackend {
            initialize: Semaphore::new(1),
            compute_ctl: ComputeCtlApi {
                api: http::Endpoint::new(compute_ctl, http::new_client()),
            },
            node_info: NodeInfo {
                config: ConnCfg::new(postgres_addr.ip().to_string(), postgres_addr.port()),
                // TODO(conrad): make this better reflect compute info rather than endpoint info.
                aux: MetricsAuxInfo {
                    endpoint_id: EndpointIdTag::get_interner().get_or_intern("local"),
                    project_id: ProjectIdTag::get_interner().get_or_intern("local"),
                    branch_id: BranchIdTag::get_interner().get_or_intern("local"),
                    cold_start_info: ColdStartInfo::WarmCached,
                },
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StaticAuthRules;

pub static JWKS_ROLE_MAP: ArcSwapOption<EndpointJwksResponse> = ArcSwapOption::const_empty();

impl FetchAuthRules for StaticAuthRules {
    async fn fetch_auth_rules(
        &self,
        _ctx: &RequestContext,
        _endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, FetchAuthRulesError> {
        let mappings = JWKS_ROLE_MAP.load();
        let role_mappings = mappings
            .as_deref()
            .ok_or(FetchAuthRulesError::RoleJwksNotConfigured)?;
        let mut rules = vec![];
        for setting in &role_mappings.jwks {
            rules.push(AuthRule {
                id: setting.id.clone(),
                jwks_url: setting.jwks_url.clone(),
                audience: setting.jwt_audience.clone(),
                role_names: setting.role_names.clone(),
            });
        }

        Ok(rules)
    }
}
