use axum::{body::Body, response::Response};
use compute_api::responses::{ComputeStatus, GenericAPIError};
use http::{header::CONTENT_TYPE, StatusCode};
use serde::Serialize;
use tracing::error;

pub use server::launch_http_server;

mod extract;
mod routes;
mod server;

/// Convenience response builder for JSON responses
struct JsonResponse;

impl JsonResponse {
    /// Helper for actually creating a response
    fn create_response(code: StatusCode, body: impl Serialize) -> Response {
        Response::builder()
            .status(code)
            .header(CONTENT_TYPE.as_str(), "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap()
    }

    /// Create a successful error response
    pub(self) fn success(code: StatusCode, body: impl Serialize) -> Response {
        assert!({
            let code = code.as_u16();

            (200..300).contains(&code)
        });

        Self::create_response(code, body)
    }

    /// Create an error response
    pub(self) fn error(code: StatusCode, error: impl ToString) -> Response {
        assert!(code.as_u16() >= 400);

        let message = error.to_string();
        error!(message);

        Self::create_response(code, &GenericAPIError { error: message })
    }

    /// Create an error response related to the compute being in an invalid state
    pub(self) fn invalid_status(status: ComputeStatus) -> Response {
        Self::create_response(
            StatusCode::PRECONDITION_FAILED,
            &GenericAPIError {
                error: format!("invalid compute status: {status}"),
            },
        )
    }
}
