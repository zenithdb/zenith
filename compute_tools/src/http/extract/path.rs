use std::ops::{Deref, DerefMut};

use axum::{
    async_trait,
    extract::{rejection::PathRejection, FromRequestParts},
};
use compute_api::responses::GenericAPIError;
use http::{request::Parts, StatusCode};

/// Custom `Path` extractor, so that we can format errors into
/// `JsonResponse<GenericAPIError>`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Path<T>(pub T);

#[async_trait]
impl<S, T> FromRequestParts<S> for Path<T>
where
    axum::extract::Path<T>: FromRequestParts<S, Rejection = PathRejection>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<GenericAPIError>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Path::<T>::from_request_parts(parts, state).await {
            Ok(value) => Ok(Self(value.0)),
            Err(rejection) => Err((
                rejection.status(),
                axum::Json(GenericAPIError {
                    error: rejection.body_text().to_ascii_lowercase(),
                }),
            )),
        }
    }
}

impl<T> Deref for Path<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Path<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
