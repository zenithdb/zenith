use std::sync::Arc;

use axum::{extract::State, response::Response};
use compute_api::{
    requests::SetRoleGrantsRequest,
    responses::{ComputeStatus, SetRoleGrantsResponse},
};
use http::StatusCode;

use crate::{
    compute::ComputeNode,
    http::{extract::Json, JsonResponse},
};

/// Add grants for a role.
pub(in crate::http) async fn add_grant(
    State(compute): State<Arc<ComputeNode>>,
    request: Json<SetRoleGrantsRequest>,
) -> Response {
    let status = compute.get_status();
    if status != ComputeStatus::Running {
        return JsonResponse::invalid_status(status);
    }

    match compute
        .set_role_grants(
            &request.database,
            &request.schema,
            &request.privileges,
            &request.role,
        )
        .await
    {
        Ok(()) => JsonResponse::success(
            StatusCode::CREATED,
            Some(SetRoleGrantsResponse {
                database: request.database.clone(),
                schema: request.schema.clone(),
                role: request.role.clone(),
                privileges: request.privileges.clone(),
            }),
        ),
        Err(e) => JsonResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to grant role privileges to the schema: {e}"),
        ),
    }
}
