use std::sync::Arc;

use axum::{extract::State, response::Response};
use compute_api::{
    requests::ConfigurationRequest,
    responses::{ComputeStatus, ComputeStatusResponse},
};
use http::StatusCode;
use tokio::task;
use tracing::info;

use crate::{
    compute::{ComputeNode, ParsedSpec},
    http::{extract::Json, JsonResponse},
};

// Accept spec in JSON format and request compute configuration. If anything
// goes wrong after we set the compute status to `ConfigurationPending` and
// update compute state with new spec, we basically leave compute in the
// potentially wrong state. That said, it's control-plane's responsibility to
// watch compute state after reconfiguration request and to clean restart in
// case of errors.
pub(in crate::http) async fn configure(
    State(compute): State<Arc<ComputeNode>>,
    request: Json<ConfigurationRequest>,
) -> Response {
    if !compute.live_config_allowed {
        return JsonResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "live configuration is not allowed for this compute node".to_string(),
        );
    }

    let pspec = match ParsedSpec::try_from(request.spec.clone()) {
        Ok(p) => p,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    // XXX: wrap state update under lock in a code block. Otherwise, we will try
    // to `Send` `mut state` into the spawned thread bellow, which will cause
    // the following rustc error:
    //
    // error: future cannot be sent between threads safely
    {
        let mut state = compute.state.lock().unwrap();
        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        state.pspec = Some(pspec);
        state.set_status(ComputeStatus::ConfigurationPending, &compute.state_changed);
        drop(state);
    }

    // Spawn a blocking thread to wait for compute to become Running. This is
    // needed to do not block the main pool of workers and be able to serve
    // other requests while some particular request is waiting for compute to
    // finish configuration.
    let c = compute.clone();
    let completed = task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        while state.status != ComputeStatus::Running {
            state = c.state_changed.wait(state).unwrap();
            info!(
                "waiting for compute to become {}, current status: {}",
                ComputeStatus::Running,
                state.status
            );

            if state.status == ComputeStatus::Failed {
                let err = state.error.as_ref().map_or("unknown error", |x| x);
                let msg = format!("compute configuration failed: {:?}", err);
                return Err(msg);
            }
        }

        Ok(())
    })
    .await
    .unwrap();

    if let Err(e) = completed {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e);
    }

    // Return current compute state if everything went well.
    let state = compute.state.lock().unwrap().clone();
    let body = ComputeStatusResponse::from(&state);

    JsonResponse::success(StatusCode::OK, body)
}
