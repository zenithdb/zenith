use std::sync::Arc;

use axum::{extract::State, response::Response};
use compute_api::responses::ComputeStatus;
use http::StatusCode;
use tokio::task;

use crate::{compute::ComputeNode, http::JsonResponse, installed_extensions};

/// Get a list of installed extensions.
pub(in crate::http) async fn get_installed_extensions(
    State(compute): State<Arc<ComputeNode>>,
) -> Response {
    let status = compute.get_status();
    if status != ComputeStatus::Running {
        return JsonResponse::invalid_status(status);
    }

    let conf = compute.get_conn_conf(None);
    let res = task::spawn_blocking(move || installed_extensions::get_installed_extensions(conf))
        .await
        .unwrap();

    match res {
        Ok(installed_extensions) => {
            JsonResponse::success(StatusCode::OK, Some(installed_extensions))
        }
        Err(e) => JsonResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to get list of installed extensions: {e}"),
        ),
    }
}
