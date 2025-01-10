use std::sync::Arc;

use axum::{extract::State, response::Response};
use http::StatusCode;

use crate::{compute::ComputeNode, http::JsonResponse};

/// Get startup metrics.
pub(in crate::http) async fn get_metrics(State(compute): State<Arc<ComputeNode>>) -> Response {
    let metrics = compute.state.lock().unwrap().metrics.clone();
    JsonResponse::success(StatusCode::OK, metrics)
}
