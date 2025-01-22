use axum::{body::Body, RequestExt};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use futures::future::BoxFuture;
use http::{Request, Response, StatusCode};
use jsonwebtoken::{jwk::JwkSet, DecodingKey, TokenData, Validation};
use serde::Deserialize;
use tower_http::auth::AsyncAuthorizeRequest;

use crate::http::JsonResponse;

#[derive(Clone, Debug, Deserialize)]
pub(in crate::http) struct Claims {
    compute_id: String,
}

#[derive(Clone, Debug)]
pub(in crate::http) struct Authorize {
    compute_id: String,
    jwks: JwkSet,
}

impl Authorize {
    pub fn new(compute_id: String, jwks: JwkSet) -> Self {
        Self { compute_id, jwks }
    }
}

impl AsyncAuthorizeRequest<Body> for Authorize {
    type RequestBody = Body;
    type ResponseBody = Body;
    type Future = BoxFuture<'static, Result<Request<Body>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: Request<Body>) -> Self::Future {
        let jwks = self.jwks.clone();
        let compute_id = self.compute_id.clone();

        Box::pin(async move {
            if jwks.keys.is_empty() {
                return Err(JsonResponse::error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "authorization has not been configured",
                ));
            }

            let TypedHeader(Authorization(bearer)) = request
                .extract_parts::<TypedHeader<Authorization<Bearer>>>()
                .await
                .map_err(|_| {
                    JsonResponse::error(StatusCode::BAD_REQUEST, "invalid authorization token")
                })?;

            let mut data: Option<TokenData<Claims>> = None;
            for jwk in jwks.keys.iter() {
                let decording_key = match DecodingKey::from_jwk(&jwk) {
                    Ok(key) => key,
                    Err(_) => continue,
                };

                match jsonwebtoken::decode::<Claims>(
                    bearer.token(),
                    &decording_key,
                    &Validation::default(),
                ) {
                    Ok(d) => {
                        data = Some(d);
                        break;
                    }
                    Err(_) => continue,
                }
            }

            match data {
                Some(data) => {
                    if data.claims.compute_id != compute_id {
                        return Err(JsonResponse::error(
                            StatusCode::UNAUTHORIZED,
                            "invalid claims in authorization token",
                        ));
                    }

                    request.extensions_mut().insert(data.claims);
                }
                None => {
                    return Err(JsonResponse::error(
                        StatusCode::UNAUTHORIZED,
                        "no claims in authorization token",
                    ));
                }
            };

            Ok(request)
        })
    }
}
