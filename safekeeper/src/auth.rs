use utils::auth::{AuthError, Claims, Scope};
use utils::id::TenantId;

/// If tenant_id is provided, allow if token (claims) is for this tenant or
/// whole safekeeper scope (SafekeeperData). Else, allow only if token is
/// SafekeeperData.
pub fn check_permission(claims: &Claims, tenant_id: Option<TenantId>) -> Result<(), AuthError> {
    match (&claims.scope, tenant_id) {
        (Scope::Tenant, None) => Err(AuthError(
            "Attempt to access management api with tenant scope. Permission denied".into(),
        )),
        (Scope::Tenant, Some(tenant_id)) => {
            if claims.tenant_id.unwrap() != tenant_id {
                return Err(AuthError("Tenant id mismatch. Permission denied".into()));
            }
            Ok(())
        }
        (
            Scope::Admin
            | Scope::PageServerApi
            | Scope::GenerationsApi
            | Scope::Infra
            | Scope::Scrubber
            | Scope::ControllerPeer,
            _,
        ) => Err(AuthError(
            format!(
                "JWT scope '{:?}' is ineligible for Safekeeper auth",
                claims.scope
            )
            .into(),
        )),
        (Scope::SafekeeperData, _) => Ok(()),
    }
}
