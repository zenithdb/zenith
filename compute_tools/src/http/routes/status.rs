use std::{ops::Deref, sync::Arc};

use axum::{extract::State, http::StatusCode, response::Response};
use compute_api::responses::ComputeStatusResponse;

use crate::{compute::ComputeNode, http::JsonResponse};

/// Retrieve the state of the comute.
pub(in crate::http) async fn get_status(State(compute): State<Arc<ComputeNode>>) -> Response {
    let state = compute.state.lock().unwrap();
    let body = ComputeStatusResponse::from(state.deref());

    JsonResponse::success(StatusCode::OK, body)
}
