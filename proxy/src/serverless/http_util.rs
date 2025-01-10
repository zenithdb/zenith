//! Things stolen from `libs/utils/src/http` to add hyper 1.0 compatibility
//! Will merge back in at some point in the future.

use anyhow::Context;
use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use utils::http::error::ApiError;

/// Like [`ApiError::into_response`]
pub(crate) fn api_error_into_response(this: ApiError) -> Response<BoxBody<Bytes, hyper::Error>> {
    match this {
        ApiError::BadRequest(err) => HttpErrorBody::response_from_msg_and_status(
            format!("{err:#?}"), // use debug printing so that we give the cause
            StatusCode::BAD_REQUEST,
        ),
        ApiError::Forbidden(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::FORBIDDEN)
        }
        ApiError::Unauthorized(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::UNAUTHORIZED)
        }
        ApiError::NotFound(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::NOT_FOUND)
        }
        ApiError::Conflict(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::CONFLICT)
        }
        ApiError::PreconditionFailed(_) => HttpErrorBody::response_from_msg_and_status(
            this.to_string(),
            StatusCode::PRECONDITION_FAILED,
        ),
        ApiError::ShuttingDown => HttpErrorBody::response_from_msg_and_status(
            "Shutting down".to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ApiError::ResourceUnavailable(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ApiError::TooManyRequests(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::TOO_MANY_REQUESTS,
        ),
        ApiError::Timeout(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::REQUEST_TIMEOUT,
        ),
        ApiError::Cancelled => HttpErrorBody::response_from_msg_and_status(
            this.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        ApiError::InternalServerError(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

/// Same as [`utils::http::error::HttpErrorBody`]
#[derive(Serialize)]
struct HttpErrorBody {
    pub(crate) msg: String,
}

impl HttpErrorBody {
    /// Same as [`utils::http::error::HttpErrorBody::response_from_msg_and_status`]
    fn response_from_msg_and_status(
        msg: String,
        status: StatusCode,
    ) -> Response<BoxBody<Bytes, hyper::Error>> {
        HttpErrorBody { msg }.to_response(status)
    }

    /// Same as [`utils::http::error::HttpErrorBody::to_response`]
    fn to_response(&self, status: StatusCode) -> Response<BoxBody<Bytes, hyper::Error>> {
        Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            // we do not have nested maps with non string keys so serialization shouldn't fail
            .body(
                Full::new(Bytes::from(
                    serde_json::to_string(self)
                        .expect("serialising HttpErrorBody should never fail"),
                ))
                .map_err(|x| match x {})
                .boxed(),
            )
            .expect("content-type header should be valid")
    }
}

/// Same as [`utils::http::json::json_response`]
pub(crate) fn json_response<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, ApiError> {
    let json = serde_json::to_string(&data)
        .context("Failed to serialize JSON response")
        .map_err(ApiError::InternalServerError)?;
    let response = Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(json)).map_err(|x| match x {}).boxed())
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}
