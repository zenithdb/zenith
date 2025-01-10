use std::sync::Arc;

use axum::{extract::State, response::Response};
use compute_api::{
    requests::ExtensionInstallRequest,
    responses::{ComputeStatus, ExtensionInstallResponse},
};
use http::StatusCode;

use crate::{
    compute::ComputeNode,
    http::{extract::Json, JsonResponse},
};

/// Install a extension.
pub(in crate::http) async fn install_extension(
    State(compute): State<Arc<ComputeNode>>,
    request: Json<ExtensionInstallRequest>,
) -> Response {
    let status = compute.get_status();
    if status != ComputeStatus::Running {
        return JsonResponse::invalid_status(status);
    }

    match compute
        .install_extension(
            &request.extension,
            &request.database,
            request.version.to_string(),
        )
        .await
    {
        Ok(version) => JsonResponse::success(
            StatusCode::CREATED,
            Some(ExtensionInstallResponse {
                extension: request.extension.clone(),
                version,
            }),
        ),
        Err(e) => JsonResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to install extension: {e}"),
        ),
    }
}
