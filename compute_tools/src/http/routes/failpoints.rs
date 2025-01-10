use axum::response::{IntoResponse, Response};
use http::StatusCode;
use tracing::info;
use utils::failpoint_support::{apply_failpoint, ConfigureFailpointsRequest};

use crate::http::{extract::Json, JsonResponse};

/// Configure failpoints for testing purposes.
pub(in crate::http) async fn configure_failpoints(
    failpoints: Json<ConfigureFailpointsRequest>,
) -> Response {
    if !fail::has_failpoints() {
        return JsonResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "Cannot manage failpoints because neon was compiled without failpoints support",
        );
    }

    for fp in &*failpoints {
        info!("cfg failpoint: {} {}", fp.name, fp.actions);

        // We recognize one extra "action" that's not natively recognized
        // by the failpoints crate: exit, to immediately kill the process
        let cfg_result = apply_failpoint(&fp.name, &fp.actions);

        if let Err(e) = cfg_result {
            return JsonResponse::error(
                StatusCode::BAD_REQUEST,
                format!("failed to configure failpoints: {e}"),
            );
        }
    }

    StatusCode::OK.into_response()
}
