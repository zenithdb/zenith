use std::ops::{Deref, DerefMut};

use axum::{
    async_trait,
    extract::{rejection::QueryRejection, FromRequestParts},
};
use compute_api::responses::GenericAPIError;
use http::{request::Parts, StatusCode};

/// Custom `Query` extractor, so that we can format errors into
/// `JsonResponse<GenericAPIError>`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Query<T>(pub T);

#[async_trait]
impl<S, T> FromRequestParts<S> for Query<T>
where
    axum::extract::Query<T>: FromRequestParts<S, Rejection = QueryRejection>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<GenericAPIError>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Query::<T>::from_request_parts(parts, state).await {
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

impl<T> Deref for Query<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Query<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
