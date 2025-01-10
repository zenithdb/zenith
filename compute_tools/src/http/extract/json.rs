use std::ops::{Deref, DerefMut};

use axum::{
    async_trait,
    extract::{rejection::JsonRejection, FromRequest, Request},
};
use compute_api::responses::GenericAPIError;
use http::StatusCode;

/// Custom `Json` extractor, so that we can format errors into
/// `JsonResponse<GenericAPIError>`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Json<T>(pub T);

#[async_trait]
impl<S, T> FromRequest<S> for Json<T>
where
    axum::Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<GenericAPIError>);

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(value) => Ok(Self(value.0)),
            Err(rejection) => Err((
                rejection.status(),
                axum::Json(GenericAPIError {
                    error: rejection.body_text().to_lowercase(),
                }),
            )),
        }
    }
}

impl<T> Deref for Json<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Json<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
