use std::sync::Arc;

use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use compute_api::responses::ComputeStatus;
use http::StatusCode;
use tokio::task;
use tracing::info;

use crate::{
    compute::{forward_termination_signal, ComputeNode},
    http::JsonResponse,
};

/// Terminate the compute.
pub(in crate::http) async fn terminate(State(compute): State<Arc<ComputeNode>>) -> Response {
    {
        let mut state = compute.state.lock().unwrap();
        if state.status == ComputeStatus::Terminated {
            return StatusCode::CREATED.into_response();
        }

        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        state.set_status(ComputeStatus::TerminationPending, &compute.state_changed);
        drop(state);
    }

    forward_termination_signal();
    info!("sent signal and notified waiters");

    // Spawn a blocking thread to wait for compute to become Terminated.
    // This is needed to do not block the main pool of workers and
    // be able to serve other requests while some particular request
    // is waiting for compute to finish configuration.
    let c = compute.clone();
    task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        while state.status != ComputeStatus::Terminated {
            state = c.state_changed.wait(state).unwrap();
            info!(
                "waiting for compute to become {}, current status: {:?}",
                ComputeStatus::Terminated,
                state.status
            );
        }
    })
    .await
    .unwrap();

    info!("terminated Postgres");

    StatusCode::OK.into_response()
}
