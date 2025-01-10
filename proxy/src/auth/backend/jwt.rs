use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use jose_jwk::crypto::KeyInfo;
use reqwest::{redirect, Client};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use signature::Verifier;
use thiserror::Error;
use tokio::time::Instant;

use crate::auth::backend::ComputeCredentialKeys;
use crate::context::RequestContext;
use crate::control_plane::errors::GetEndpointJwksError;
use crate::http::read_body_with_limit;
use crate::intern::RoleNameInt;
use crate::types::{EndpointId, RoleName};

// TODO(conrad): make these configurable.
const CLOCK_SKEW_LEEWAY: Duration = Duration::from_secs(30);
const MIN_RENEW: Duration = Duration::from_secs(30);
const AUTO_RENEW: Duration = Duration::from_secs(300);
const MAX_RENEW: Duration = Duration::from_secs(3600);
const MAX_JWK_BODY_SIZE: usize = 64 * 1024;
const JWKS_USER_AGENT: &str = "neon-proxy";

const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_FETCH_RETRIES: u32 = 3;

/// How to get the JWT auth rules
pub(crate) trait FetchAuthRules: Clone + Send + Sync + 'static {
    fn fetch_auth_rules(
        &self,
        ctx: &RequestContext,
        endpoint: EndpointId,
    ) -> impl Future<Output = Result<Vec<AuthRule>, FetchAuthRulesError>> + Send;
}

#[derive(Error, Debug)]
pub(crate) enum FetchAuthRulesError {
    #[error(transparent)]
    GetEndpointJwks(#[from] GetEndpointJwksError),

    #[error("JWKs settings for this role were not configured")]
    RoleJwksNotConfigured,
}

#[derive(Clone)]
pub(crate) struct AuthRule {
    pub(crate) id: String,
    pub(crate) jwks_url: url::Url,
    pub(crate) audience: Option<String>,
    pub(crate) role_names: Vec<RoleNameInt>,
}

pub struct JwkCache {
    client: reqwest_middleware::ClientWithMiddleware,

    map: DashMap<(EndpointId, RoleName), Arc<JwkCacheEntryLock>>,
}

pub(crate) struct JwkCacheEntry {
    /// Should refetch at least every hour to verify when old keys have been removed.
    /// Should refetch when new key IDs are seen only every 5 minutes or so
    last_retrieved: Instant,

    /// cplane will return multiple JWKs urls that we need to scrape.
    key_sets: ahash::HashMap<String, KeySet>,
}

impl JwkCacheEntry {
    fn find_jwk_and_audience(
        &self,
        key_id: &str,
        role_name: &RoleName,
    ) -> Option<(&jose_jwk::Jwk, Option<&str>)> {
        self.key_sets
            .values()
            // make sure our requested role has access to the key set
            .filter(|key_set| key_set.role_names.iter().any(|role| **role == **role_name))
            // try and find the requested key-id in the key set
            .find_map(|key_set| {
                key_set
                    .find_key(key_id)
                    .map(|jwk| (jwk, key_set.audience.as_deref()))
            })
    }
}

struct KeySet {
    jwks: jose_jwk::JwkSet,
    audience: Option<String>,
    role_names: Vec<RoleNameInt>,
}

impl KeySet {
    fn find_key(&self, key_id: &str) -> Option<&jose_jwk::Jwk> {
        self.jwks
            .keys
            .iter()
            .find(|jwk| jwk.prm.kid.as_deref() == Some(key_id))
    }
}

pub(crate) struct JwkCacheEntryLock {
    cached: ArcSwapOption<JwkCacheEntry>,
    lookup: tokio::sync::Semaphore,
}

impl Default for JwkCacheEntryLock {
    fn default() -> Self {
        JwkCacheEntryLock {
            cached: ArcSwapOption::empty(),
            lookup: tokio::sync::Semaphore::new(1),
        }
    }
}

#[derive(Deserialize)]
struct JwkSet<'a> {
    /// we parse into raw-value because not all keys in a JWKS are ones
    /// we can parse directly, so we parse them lazily.
    #[serde(borrow)]
    keys: Vec<&'a RawValue>,
}

/// Given a jwks_url, fetch the JWKS and parse out all the signing JWKs.
/// Returns `None` and log a warning if there are any errors.
async fn fetch_jwks(
    client: &reqwest_middleware::ClientWithMiddleware,
    jwks_url: url::Url,
) -> Option<jose_jwk::JwkSet> {
    let req = client.get(jwks_url.clone());
    // TODO(conrad): We need to filter out URLs that point to local resources. Public internet only.
    let resp = req.send().await.and_then(|r| {
        r.error_for_status()
            .map_err(reqwest_middleware::Error::Reqwest)
    });

    let resp = match resp {
        Ok(r) => r,
        // TODO: should we re-insert JWKs if we want to keep this JWKs URL?
        // I expect these failures would be quite sparse.
        Err(e) => {
            tracing::warn!(url=?jwks_url, error=?e, "could not fetch JWKs");
            return None;
        }
    };

    let resp: http::Response<reqwest::Body> = resp.into();

    let bytes = match read_body_with_limit(resp.into_body(), MAX_JWK_BODY_SIZE).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(url=?jwks_url, error=?e, "could not decode JWKs");
            return None;
        }
    };

    let jwks = match serde_json::from_slice::<JwkSet>(&bytes) {
        Ok(jwks) => jwks,
        Err(e) => {
            tracing::warn!(url=?jwks_url, error=?e, "could not decode JWKs");
            return None;
        }
    };

    // `jose_jwk::Jwk` is quite large (288 bytes). Let's not pre-allocate for what we don't need.
    //
    // Even though we limit our responses to 64KiB, we could still receive a payload like
    // `{"keys":[` + repeat(`0`).take(30000).join(`,`) + `]}`. Parsing this as `RawValue` uses 468KiB.
    // Pre-allocating the corresponding `Vec::<jose_jwk::Jwk>::with_capacity(30000)` uses 8.2MiB.
    let mut keys = vec![];

    let mut failed = 0;
    for key in jwks.keys {
        let key = match serde_json::from_str::<jose_jwk::Jwk>(key.get()) {
            Ok(key) => key,
            Err(e) => {
                tracing::debug!(url=?jwks_url, failed=?e, "could not decode JWK");
                failed += 1;
                continue;
            }
        };

        // if `use` (called `cls` in rust) is specified to be something other than signing,
        // we can skip storing it.
        if key
            .prm
            .cls
            .as_ref()
            .is_some_and(|c| *c != jose_jwk::Class::Signing)
        {
            continue;
        }

        keys.push(key);
    }

    keys.shrink_to_fit();

    if failed > 0 {
        tracing::warn!(url=?jwks_url, failed, "could not decode JWKs");
    }

    if keys.is_empty() {
        tracing::warn!(url=?jwks_url, "no valid JWKs found inside the response body");
        return None;
    }

    Some(jose_jwk::JwkSet { keys })
}

impl JwkCacheEntryLock {
    async fn acquire_permit<'a>(self: &'a Arc<Self>) -> JwkRenewalPermit<'a> {
        JwkRenewalPermit::acquire_permit(self).await
    }

    fn try_acquire_permit<'a>(self: &'a Arc<Self>) -> Option<JwkRenewalPermit<'a>> {
        JwkRenewalPermit::try_acquire_permit(self)
    }

    async fn renew_jwks<F: FetchAuthRules>(
        &self,
        _permit: JwkRenewalPermit<'_>,
        ctx: &RequestContext,
        client: &reqwest_middleware::ClientWithMiddleware,
        endpoint: EndpointId,
        auth_rules: &F,
    ) -> Result<Arc<JwkCacheEntry>, JwtError> {
        // double check that no one beat us to updating the cache.
        let now = Instant::now();
        let guard = self.cached.load_full();
        if let Some(cached) = guard {
            let last_update = now.duration_since(cached.last_retrieved);
            if last_update < Duration::from_secs(300) {
                return Ok(cached);
            }
        }

        let rules = auth_rules.fetch_auth_rules(ctx, endpoint).await?;
        let mut key_sets =
            ahash::HashMap::with_capacity_and_hasher(rules.len(), ahash::RandomState::new());

        // TODO(conrad): run concurrently
        // TODO(conrad): strip the JWKs urls (should be checked by cplane as well - cloud#16284)
        for rule in rules {
            if let Some(jwks) = fetch_jwks(client, rule.jwks_url).await {
                key_sets.insert(
                    rule.id,
                    KeySet {
                        jwks,
                        audience: rule.audience,
                        role_names: rule.role_names,
                    },
                );
            }
        }

        let entry = Arc::new(JwkCacheEntry {
            last_retrieved: now,
            key_sets,
        });
        self.cached.swap(Some(Arc::clone(&entry)));

        Ok(entry)
    }

    async fn get_or_update_jwk_cache<F: FetchAuthRules>(
        self: &Arc<Self>,
        ctx: &RequestContext,
        client: &reqwest_middleware::ClientWithMiddleware,
        endpoint: EndpointId,
        fetch: &F,
    ) -> Result<Arc<JwkCacheEntry>, JwtError> {
        let now = Instant::now();
        let guard = self.cached.load_full();

        // if we have no cached JWKs, try and get some
        let Some(cached) = guard else {
            let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
            let permit = self.acquire_permit().await;
            return self.renew_jwks(permit, ctx, client, endpoint, fetch).await;
        };

        let last_update = now.duration_since(cached.last_retrieved);

        // check if the cached JWKs need updating.
        if last_update > MAX_RENEW {
            let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
            let permit = self.acquire_permit().await;

            // it's been too long since we checked the keys. wait for them to update.
            return self.renew_jwks(permit, ctx, client, endpoint, fetch).await;
        }

        // every 5 minutes we should spawn a job to eagerly update the token.
        if last_update > AUTO_RENEW {
            if let Some(permit) = self.try_acquire_permit() {
                tracing::debug!("JWKs should be renewed. Renewal permit acquired");
                let permit = permit.into_owned();
                let entry = self.clone();
                let client = client.clone();
                let fetch = fetch.clone();
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = entry
                        .renew_jwks(permit, &ctx, &client, endpoint, &fetch)
                        .await
                    {
                        tracing::warn!(error=?e, "could not fetch JWKs in background job");
                    }
                });
            } else {
                tracing::debug!("JWKs should be renewed. Renewal permit already taken, skipping");
            }
        }

        Ok(cached)
    }

    async fn check_jwt<F: FetchAuthRules>(
        self: &Arc<Self>,
        ctx: &RequestContext,
        jwt: &str,
        client: &reqwest_middleware::ClientWithMiddleware,
        endpoint: EndpointId,
        role_name: &RoleName,
        fetch: &F,
    ) -> Result<ComputeCredentialKeys, JwtError> {
        // JWT compact form is defined to be
        // <B64(Header)> || . || <B64(Payload)> || . || <B64(Signature)>
        // where Signature = alg(<B64(Header)> || . || <B64(Payload)>);

        let (header_payload, signature) = jwt
            .rsplit_once('.')
            .ok_or(JwtEncodingError::InvalidCompactForm)?;
        let (header, payload) = header_payload
            .split_once('.')
            .ok_or(JwtEncodingError::InvalidCompactForm)?;

        let header = base64::decode_config(header, base64::URL_SAFE_NO_PAD)?;
        let header = serde_json::from_slice::<JwtHeader<'_>>(&header)?;

        let payloadb = base64::decode_config(payload, base64::URL_SAFE_NO_PAD)?;
        let payload = serde_json::from_slice::<JwtPayload<'_>>(&payloadb)?;

        if let Some(iss) = &payload.issuer {
            ctx.set_jwt_issuer(iss.as_ref().to_owned());
        }

        let sig = base64::decode_config(signature, base64::URL_SAFE_NO_PAD)?;

        let kid = header.key_id.ok_or(JwtError::MissingKeyId)?;

        let mut guard = self
            .get_or_update_jwk_cache(ctx, client, endpoint.clone(), fetch)
            .await?;

        // get the key from the JWKs if possible. If not, wait for the keys to update.
        let (jwk, expected_audience) = loop {
            match guard.find_jwk_and_audience(&kid, role_name) {
                Some(jwk) => break jwk,
                None if guard.last_retrieved.elapsed() > MIN_RENEW => {
                    let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);

                    let permit = self.acquire_permit().await;
                    guard = self
                        .renew_jwks(permit, ctx, client, endpoint.clone(), fetch)
                        .await?;
                }
                _ => return Err(JwtError::JwkNotFound),
            }
        };

        if !jwk.is_supported(&header.algorithm) {
            return Err(JwtError::SignatureAlgorithmNotSupported);
        }

        match &jwk.key {
            jose_jwk::Key::Ec(key) => {
                verify_ec_signature(header_payload.as_bytes(), &sig, key)?;
            }
            jose_jwk::Key::Rsa(key) => {
                verify_rsa_signature(header_payload.as_bytes(), &sig, key, &header.algorithm)?;
            }
            key => return Err(JwtError::UnsupportedKeyType(key.into())),
        };

        tracing::debug!(?payload, "JWT signature valid with claims");

        if let Some(aud) = expected_audience {
            if payload.audience.0.iter().all(|s| s != aud) {
                return Err(JwtError::InvalidClaims(
                    JwtClaimsError::InvalidJwtTokenAudience,
                ));
            }
        }

        let now = SystemTime::now();

        if let Some(exp) = payload.expiration {
            if now >= exp + CLOCK_SKEW_LEEWAY {
                return Err(JwtError::InvalidClaims(JwtClaimsError::JwtTokenHasExpired));
            }
        }

        if let Some(nbf) = payload.not_before {
            if nbf >= now + CLOCK_SKEW_LEEWAY {
                return Err(JwtError::InvalidClaims(
                    JwtClaimsError::JwtTokenNotYetReadyToUse,
                ));
            }
        }

        Ok(ComputeCredentialKeys::JwtPayload(payloadb))
    }
}

impl JwkCache {
    pub(crate) async fn check_jwt<F: FetchAuthRules>(
        &self,
        ctx: &RequestContext,
        endpoint: EndpointId,
        role_name: &RoleName,
        fetch: &F,
        jwt: &str,
    ) -> Result<ComputeCredentialKeys, JwtError> {
        // try with just a read lock first
        let key = (endpoint.clone(), role_name.clone());
        let entry = self.map.get(&key).as_deref().map(Arc::clone);
        let entry = entry.unwrap_or_else(|| {
            // acquire a write lock after to insert.
            let entry = self.map.entry(key).or_default();
            Arc::clone(&*entry)
        });

        entry
            .check_jwt(ctx, jwt, &self.client, endpoint, role_name, fetch)
            .await
    }
}

impl Default for JwkCache {
    fn default() -> Self {
        let client = Client::builder()
            .user_agent(JWKS_USER_AGENT)
            .redirect(redirect::Policy::none())
            .tls_built_in_native_certs(true)
            .connect_timeout(JWKS_CONNECT_TIMEOUT)
            .timeout(JWKS_FETCH_TIMEOUT)
            .build()
            .expect("client config should be valid");

        // Retry up to 3 times with increasing intervals between attempts.
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(JWKS_FETCH_RETRIES);

        let client = reqwest_middleware::ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        JwkCache {
            client,
            map: DashMap::default(),
        }
    }
}

fn verify_ec_signature(data: &[u8], sig: &[u8], key: &jose_jwk::Ec) -> Result<(), JwtError> {
    use ecdsa::Signature;
    use signature::Verifier;

    match key.crv {
        jose_jwk::EcCurves::P256 => {
            let pk = p256::PublicKey::try_from(key).map_err(JwtError::InvalidP256Key)?;
            let key = p256::ecdsa::VerifyingKey::from(&pk);
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        key => return Err(JwtError::UnsupportedEcKeyType(key)),
    }

    Ok(())
}

fn verify_rsa_signature(
    data: &[u8],
    sig: &[u8],
    key: &jose_jwk::Rsa,
    alg: &jose_jwa::Algorithm,
) -> Result<(), JwtError> {
    use jose_jwa::{Algorithm, Signing};
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::RsaPublicKey;

    let key = RsaPublicKey::try_from(key).map_err(JwtError::InvalidRsaKey)?;

    match alg {
        Algorithm::Signing(Signing::Rs256) => {
            let key = VerifyingKey::<sha2::Sha256>::new(key);
            let sig = Signature::try_from(sig)?;
            key.verify(data, &sig)?;
        }
        _ => return Err(JwtError::InvalidRsaSigningAlgorithm),
    };

    Ok(())
}

/// <https://datatracker.ietf.org/doc/html/rfc7515#section-4.1>
#[derive(serde::Deserialize, serde::Serialize)]
struct JwtHeader<'a> {
    /// must be a supported alg
    #[serde(rename = "alg")]
    algorithm: jose_jwa::Algorithm,
    /// key id, must be provided for our usecase
    #[serde(rename = "kid", borrow)]
    key_id: Option<Cow<'a, str>>,
}

/// <https://datatracker.ietf.org/doc/html/rfc7519#section-4.1>
#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct JwtPayload<'a> {
    /// Audience - Recipient for which the JWT is intended
    #[serde(rename = "aud", default)]
    audience: OneOrMany,
    /// Expiration - Time after which the JWT expires
    #[serde(deserialize_with = "numeric_date_opt", rename = "exp", default)]
    expiration: Option<SystemTime>,
    /// Not before - Time after which the JWT expires
    #[serde(deserialize_with = "numeric_date_opt", rename = "nbf", default)]
    not_before: Option<SystemTime>,

    // the following entries are only extracted for the sake of debug logging.
    /// Issuer of the JWT
    #[serde(rename = "iss", borrow)]
    issuer: Option<Cow<'a, str>>,
    /// Subject of the JWT (the user)
    #[serde(rename = "sub", borrow)]
    subject: Option<Cow<'a, str>>,
    /// Unique token identifier
    #[serde(rename = "jti", borrow)]
    jwt_id: Option<Cow<'a, str>>,
    /// Unique session identifier
    #[serde(rename = "sid", borrow)]
    session_id: Option<Cow<'a, str>>,
}

/// `OneOrMany` supports parsing either a single item or an array of items.
///
/// Needed for <https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.3>
///
/// > The "aud" (audience) claim identifies the recipients that the JWT is
/// > intended for.  Each principal intended to process the JWT MUST
/// > identify itself with a value in the audience claim.  If the principal
/// > processing the claim does not identify itself with a value in the
/// > "aud" claim when this claim is present, then the JWT MUST be
/// > rejected.  In the general case, the "aud" value is **an array of case-
/// > sensitive strings**, each containing a StringOrURI value.  In the
/// > special case when the JWT has one audience, the "aud" value MAY be a
/// > **single case-sensitive string** containing a StringOrURI value.  The
/// > interpretation of audience values is generally application specific.
/// > Use of this claim is OPTIONAL.
#[derive(Default, Debug)]
struct OneOrMany(Vec<String>);

impl<'de> Deserialize<'de> for OneOrMany {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OneOrManyVisitor;
        impl<'de> Visitor<'de> for OneOrManyVisitor {
            type Value = OneOrMany;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a single string or an array of strings")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(OneOrMany(vec![v.to_owned()]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut v = vec![];
                while let Some(s) = seq.next_element()? {
                    v.push(s);
                }
                Ok(OneOrMany(v))
            }
        }
        deserializer.deserialize_any(OneOrManyVisitor)
    }
}

fn numeric_date_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SystemTime>, D::Error> {
    let d = <Option<u64>>::deserialize(d)?;
    Ok(d.map(|n| SystemTime::UNIX_EPOCH + Duration::from_secs(n)))
}

struct JwkRenewalPermit<'a> {
    inner: Option<JwkRenewalPermitInner<'a>>,
}

enum JwkRenewalPermitInner<'a> {
    Owned(Arc<JwkCacheEntryLock>),
    Borrowed(&'a Arc<JwkCacheEntryLock>),
}

impl JwkRenewalPermit<'_> {
    fn into_owned(mut self) -> JwkRenewalPermit<'static> {
        JwkRenewalPermit {
            inner: self.inner.take().map(JwkRenewalPermitInner::into_owned),
        }
    }

    async fn acquire_permit(from: &Arc<JwkCacheEntryLock>) -> JwkRenewalPermit<'_> {
        match from.lookup.acquire().await {
            Ok(permit) => {
                permit.forget();
                JwkRenewalPermit {
                    inner: Some(JwkRenewalPermitInner::Borrowed(from)),
                }
            }
            Err(_) => panic!("semaphore should not be closed"),
        }
    }

    fn try_acquire_permit(from: &Arc<JwkCacheEntryLock>) -> Option<JwkRenewalPermit<'_>> {
        match from.lookup.try_acquire() {
            Ok(permit) => {
                permit.forget();
                Some(JwkRenewalPermit {
                    inner: Some(JwkRenewalPermitInner::Borrowed(from)),
                })
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => None,
            Err(tokio::sync::TryAcquireError::Closed) => panic!("semaphore should not be closed"),
        }
    }
}

impl JwkRenewalPermitInner<'_> {
    fn into_owned(self) -> JwkRenewalPermitInner<'static> {
        match self {
            JwkRenewalPermitInner::Owned(p) => JwkRenewalPermitInner::Owned(p),
            JwkRenewalPermitInner::Borrowed(p) => JwkRenewalPermitInner::Owned(Arc::clone(p)),
        }
    }
}

impl Drop for JwkRenewalPermit<'_> {
    fn drop(&mut self) {
        let entry = match &self.inner {
            None => return,
            Some(JwkRenewalPermitInner::Owned(p)) => p,
            Some(JwkRenewalPermitInner::Borrowed(p)) => *p,
        };
        entry.lookup.add_permits(1);
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum JwtError {
    #[error("jwk not found")]
    JwkNotFound,

    #[error("missing key id")]
    MissingKeyId,

    #[error("Provided authentication token is not a valid JWT encoding")]
    JwtEncoding(#[from] JwtEncodingError),

    #[error(transparent)]
    InvalidClaims(#[from] JwtClaimsError),

    #[error("invalid P256 key")]
    InvalidP256Key(jose_jwk::crypto::Error),

    #[error("invalid RSA key")]
    InvalidRsaKey(jose_jwk::crypto::Error),

    #[error("invalid RSA signing algorithm")]
    InvalidRsaSigningAlgorithm,

    #[error("unsupported EC key type {0:?}")]
    UnsupportedEcKeyType(jose_jwk::EcCurves),

    #[error("unsupported key type {0:?}")]
    UnsupportedKeyType(KeyType),

    #[error("signature algorithm not supported")]
    SignatureAlgorithmNotSupported,

    #[error("signature error: {0}")]
    Signature(#[from] signature::Error),

    #[error("failed to fetch auth rules: {0}")]
    FetchAuthRules(#[from] FetchAuthRulesError),
}

impl From<base64::DecodeError> for JwtError {
    fn from(err: base64::DecodeError) -> Self {
        JwtEncodingError::Base64Decode(err).into()
    }
}

impl From<serde_json::Error> for JwtError {
    fn from(err: serde_json::Error) -> Self {
        JwtEncodingError::SerdeJson(err).into()
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum JwtEncodingError {
    #[error(transparent)]
    Base64Decode(#[from] base64::DecodeError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error("invalid compact form")]
    InvalidCompactForm,
}

#[derive(Error, Debug, PartialEq)]
#[non_exhaustive]
pub enum JwtClaimsError {
    #[error("invalid JWT token audience")]
    InvalidJwtTokenAudience,

    #[error("JWT token has expired")]
    JwtTokenHasExpired,

    #[error("JWT token is not yet ready to use")]
    JwtTokenNotYetReadyToUse,
}

#[allow(dead_code, reason = "Debug use only")]
#[derive(Debug)]
pub(crate) enum KeyType {
    Ec(jose_jwk::EcCurves),
    Rsa,
    Oct,
    Okp(jose_jwk::OkpCurves),
    Unknown,
}

impl From<&jose_jwk::Key> for KeyType {
    fn from(key: &jose_jwk::Key) -> Self {
        match key {
            jose_jwk::Key::Ec(ec) => Self::Ec(ec.crv),
            jose_jwk::Key::Rsa(_rsa) => Self::Rsa,
            jose_jwk::Key::Oct(_oct) => Self::Oct,
            jose_jwk::Key::Okp(okp) => Self::Okp(okp.crv),
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::future::IntoFuture;
    use std::net::SocketAddr;
    use std::time::SystemTime;

    use base64::URL_SAFE_NO_PAD;
    use bytes::Bytes;
    use http::Response;
    use http_body_util::Full;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use rand::rngs::OsRng;
    use rsa::pkcs8::DecodePrivateKey;
    use serde::Serialize;
    use serde_json::json;
    use signature::Signer;
    use tokio::net::TcpListener;

    use super::*;
    use crate::types::RoleName;

    fn new_ec_jwk(kid: String) -> (p256::SecretKey, jose_jwk::Jwk) {
        let sk = p256::SecretKey::random(&mut OsRng);
        let pk = sk.public_key().into();
        let jwk = jose_jwk::Jwk {
            key: jose_jwk::Key::Ec(pk),
            prm: jose_jwk::Parameters {
                kid: Some(kid),
                alg: Some(jose_jwa::Algorithm::Signing(jose_jwa::Signing::Es256)),
                ..Default::default()
            },
        };
        (sk, jwk)
    }

    fn new_rsa_jwk(key: &str, kid: String) -> (rsa::RsaPrivateKey, jose_jwk::Jwk) {
        let sk = rsa::RsaPrivateKey::from_pkcs8_pem(key).unwrap();
        let pk = sk.to_public_key().into();
        let jwk = jose_jwk::Jwk {
            key: jose_jwk::Key::Rsa(pk),
            prm: jose_jwk::Parameters {
                kid: Some(kid),
                alg: Some(jose_jwa::Algorithm::Signing(jose_jwa::Signing::Rs256)),
                ..Default::default()
            },
        };
        (sk, jwk)
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn build_jwt_payload(kid: String, sig: jose_jwa::Signing) -> String {
        let now = now();
        let body = typed_json::json! {{
            "exp": now + 3600,
            "nbf": now,
            "aud": ["audience1", "neon", "audience2"],
            "sub": "user1",
            "sid": "session1",
            "jti": "token1",
            "iss": "neon-testing",
        }};
        build_custom_jwt_payload(kid, body, sig)
    }

    fn build_custom_jwt_payload(
        kid: String,
        body: impl Serialize,
        sig: jose_jwa::Signing,
    ) -> String {
        let header = JwtHeader {
            algorithm: jose_jwa::Algorithm::Signing(sig),
            key_id: Some(Cow::Owned(kid)),
        };

        let header =
            base64::encode_config(serde_json::to_string(&header).unwrap(), URL_SAFE_NO_PAD);
        let body = base64::encode_config(serde_json::to_string(&body).unwrap(), URL_SAFE_NO_PAD);

        format!("{header}.{body}")
    }

    fn new_ec_jwt(kid: String, key: &p256::SecretKey) -> String {
        use p256::ecdsa::{Signature, SigningKey};

        let payload = build_jwt_payload(kid, jose_jwa::Signing::Es256);
        let sig: Signature = SigningKey::from(key).sign(payload.as_bytes());
        let sig = base64::encode_config(sig.to_bytes(), URL_SAFE_NO_PAD);

        format!("{payload}.{sig}")
    }

    fn new_custom_ec_jwt(kid: String, key: &p256::SecretKey, body: impl Serialize) -> String {
        use p256::ecdsa::{Signature, SigningKey};

        let payload = build_custom_jwt_payload(kid, body, jose_jwa::Signing::Es256);
        let sig: Signature = SigningKey::from(key).sign(payload.as_bytes());
        let sig = base64::encode_config(sig.to_bytes(), URL_SAFE_NO_PAD);

        format!("{payload}.{sig}")
    }

    fn new_rsa_jwt(kid: String, key: rsa::RsaPrivateKey) -> String {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::SignatureEncoding;

        let payload = build_jwt_payload(kid, jose_jwa::Signing::Rs256);
        let sig = SigningKey::<sha2::Sha256>::new(key).sign(payload.as_bytes());
        let sig = base64::encode_config(sig.to_bytes(), URL_SAFE_NO_PAD);

        format!("{payload}.{sig}")
    }

    // RSA key gen is slow....
    const RS1: &str = "-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNuWBIWTlo+54Y
aifpGInIrpv6LlsbI/2/2CC81Arlx4RsABORklgA9XSGwaCbHTshHsfd1S916JwA
SpjyPQYWfqo6iAV8a4MhjIeJIkRr74prDCSzOGZvIc6VaGeCIb9clf3HSrPHm3hA
cfLMB8/p5MgoxERPDOIn3XYoS9SEEuP7l0LkmEZMerg6W6lDjQRDny0Lb50Jky9X
mDqnYXBhs99ranbwL5vjy0ba6OIeCWFJme5u+rv5C/P0BOYrJfGxIcEoKa8Ukw5s
PlM+qrz9ope1eOuXMNNdyFDReNBUyaM1AwBAayU5rz57crer7K/UIofaJ42T4cMM
nx/SWfBNAgMBAAECggEACqdpBxYn1PoC6/zDaFzu9celKEWyTiuE/qRwvZa1ocS9
ZOJ0IPvVNud/S2NHsADJiSOQ8joSJScQvSsf1Ju4bv3MTw+wSQtAVUJz2nQ92uEi
5/xPAkEPfP3hNvebNLAOuvrBk8qYmOPCTIQaMNrOt6wzeXkAmJ9wLuRXNCsJLHW+
KLpf2WdgTYxqK06ZiJERFgJ2r1MsC2IgTydzjOAdEIrtMarerTLqqCpwFrk/l0cz
1O2OAb17ZxmhuzMhjNMin81c8F2fZAGMeOjn92Jl5kUsYw/pG+0S8QKlbveR/fdP
We2tJsgXw2zD0q7OJpp8NXS2yddrZGyysYsof983wQKBgQD2McqNJqo+eWL5zony
UbL19loYw0M15EjhzIuzW1Jk0rPj65yQyzpJ6pqicRuWr34MvzCx+ZHM2b3jSiNu
GES2fnC7xLIKyeRxfqsXF71xz+6UStEGRQX27r1YWEtyQVuBhvlqB+AGWP3PYAC+
HecZecnZ+vcihJ2K3+l5O3paVQKBgQDV6vKH5h2SY9vgO8obx0P7XSS+djHhmPuU
f8C/Fq6AuRbIA1g04pzuLU2WS9T26eIjgM173uVNg2TuqJveWzz+CAAp6nCR6l24
DBg49lMGCWrMo4FqPG46QkUqvK8uSj42GkX/e5Rut1Gyu0209emeM6h2d2K15SvY
9563tYSmGQKBgQDwcH5WTi20KA7e07TroJi8GKWzS3gneNUpGQBS4VxdtV4UuXXF
/4TkzafJ/9cm2iurvUmMd6XKP9lw0mY5zp/E70WgTCBp4vUlVsU3H2tYbO+filYL
3ntNx6nKTykX4/a/UJfj0t8as+zli+gNxNx/h+734V9dKdFG4Rl+2fTLpQKBgQCE
qJkTEe+Q0wCOBEYICADupwqcWqwAXWDW7IrZdfVtulqYWwqecVIkmk+dPxWosc4d
ekjz4nyNH0i+gC15LVebqdaAJ/T7aD4KXuW+nXNLMRfcJCGjgipRUruWD0EMEdqW
rqBuGXMpXeH6VxGPgVkJVLvKC6tZZe9VM+pnvteuMQKBgQC8GaL+Lz+al4biyZBf
JE8ekWrIotq/gfUBLP7x70+PB9bNtXtlgmTvjgYg4jiu3KR/ZIYYQ8vfVgkb6tDI
rWGZw86Pzuoi1ppg/pYhKk9qrmCIT4HPEXbHl7ATahu2BOCIU3hybjTh2lB6LbX9
8LMFlz1QPqSZYN/A/kOcLBfa3A==
-----END PRIVATE KEY-----
";
    const RS2: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDipm6FIKSRab3J
HwmK18t7hp+pohllxIDUSPi7S5mIhN/JG2Plq2Lp746E/fuT8dcBF2R4sJlG2L0J
zmxOvBU/i/sQF9s1i4CEfg05k2//gKENIEsF3pMMmrH+mcZi0TTD6rezHpdVxPHk
qWxSyOCtIJV29X+wxPwAB59kQFHzy2ooPB1isZcpE8tO0KthAM+oZ3KuCwE0++cO
IWLeq9aPwyKhtip/xjTMxd1kzdKh592mGSyzr9D0QSWOYFGvgJXANDdiPdhSSOLt
ECWPNPlm2FQvGGvYYBafUqz7VumKHE6x8J6lKdYa2J0ZdDzCIo2IHzlxe+RZNgwy
uAD2jhVxAgMBAAECggEAbsZHWBu3MzcKQiVARbLoygvnN0J5xUqAaMDtiKUPejDv
K1yOu67DXnDuKEP2VL2rhuYG/hHaKE1AP227c9PrUq6424m9YvM2sgrlrdFIuQkG
LeMtp8W7+zoUasp/ssZrUqICfLIj5xCl5UuFHQT/Ar7dLlIYwa3VOLKBDb9+Dnfe
QH5/So4uMXG6vw34JN9jf+eAc8Yt0PeIz62ycvRwdpTJQ0MxZN9ZKpCAQp+VTuXT
zlzNvDMilabEdqUvAyGyz8lBLNl0wdaVrqPqAEWM5U45QXsdFZknWammP7/tijeX
0z+Bi0J0uSEU5X502zm7GArj/NNIiWMcjmDjwUUhwQKBgQD9C2GoqxOxuVPYqwYR
+Jz7f2qMjlSP8adA5Lzuh8UKXDp8JCEQC8ryweLzaOKS9C5MAw+W4W2wd4nJoQI1
P1dgGvBlfvEeRHMgqWtq7FuTsjSe7e0uSEkC4ngDb4sc0QOpv15cMuEz+4+aFLPL
x29EcHWAaBX+rkid3zpQHFU4eQKBgQDlTCEqRuXwwa3V+Sq+mNWzD9QIGtD87TH/
FPO/Ij/cK2+GISgFDqhetiGTH4qrvPL0psPT+iH5zGFYcoFmTtwLdWQJdxhxz0bg
iX/AceyX5e1Bm+ThT36sU83NrxKPkrdk6jNmr2iUF1OTzTwUKOYdHOPZqdMPfF4M
4XAaWVT2uQKBgQD4nKcNdU+7LE9Rr+4d1/o8Klp/0BMK/ayK2HE7lc8kt6qKb2DA
iCWUTqPw7Fq3cQrPia5WWhNP7pJEtFkcAaiR9sW7onW5fBz0uR+dhK0QtmR2xWJj
N4fsOp8ZGQ0/eae0rh1CTobucLkM9EwV6VLLlgYL67e4anlUCo8bSEr+WQKBgQCB
uf6RgqcY/RqyklPCnYlZ0zyskS9nyXKd1GbK3j+u+swP4LZZlh9f5j88k33LCA2U
qLzmMwAB6cWxWqcnELqhqPq9+ClWSmTZKDGk2U936NfAZMirSGRsbsVi9wfTPriP
WYlXMSpDjqb0WgsBhNob4npubQxCGKTFOM5Jufy90QKBgB0Lte1jX144uaXx6dtB
rjXNuWNir0Jy31wHnQuCA+XnfUgPcrKmRLm8taMbXgZwxkNvgFkpUWU8aPEK08Ne
X0n5X2/pBLJzxZc62ccvZYVnctBiFs6HbSnxpuMQCfkt/BcR/ttIepBQQIW86wHL
5JiconnI5aLek0QVPoFaVXFa
-----END PRIVATE KEY-----
";

    #[derive(Clone)]
    struct Fetch(Vec<AuthRule>);

    impl FetchAuthRules for Fetch {
        async fn fetch_auth_rules(
            &self,
            _ctx: &RequestContext,
            _endpoint: EndpointId,
        ) -> Result<Vec<AuthRule>, FetchAuthRulesError> {
            Ok(self.0.clone())
        }
    }

    async fn jwks_server(
        router: impl for<'a> Fn(&'a str) -> Option<Vec<u8>> + Send + Sync + 'static,
    ) -> SocketAddr {
        let router = Arc::new(router);
        let service = service_fn(move |req| {
            let router = Arc::clone(&router);
            async move {
                match router(req.uri().path()) {
                    Some(body) => Response::builder()
                        .status(200)
                        .body(Full::new(Bytes::from(body))),
                    None => Response::builder()
                        .status(404)
                        .body(Full::new(Bytes::new())),
                }
            }
        });

        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let server = hyper::server::conn::http1::Builder::new();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (s, _) = listener.accept().await.unwrap();
                let serve = server.serve_connection(TokioIo::new(s), service.clone());
                tokio::spawn(serve.into_future());
            }
        });

        addr
    }

    #[tokio::test]
    async fn check_jwt_happy_path() {
        let (rs1, jwk1) = new_rsa_jwk(RS1, "rs1".into());
        let (rs2, jwk2) = new_rsa_jwk(RS2, "rs2".into());
        let (ec1, jwk3) = new_ec_jwk("ec1".into());
        let (ec2, jwk4) = new_ec_jwk("ec2".into());

        let foo_jwks = jose_jwk::JwkSet {
            keys: vec![jwk1, jwk3],
        };
        let bar_jwks = jose_jwk::JwkSet {
            keys: vec![jwk2, jwk4],
        };

        let jwks_addr = jwks_server(move |path| match path {
            "/foo" => Some(serde_json::to_vec(&foo_jwks).unwrap()),
            "/bar" => Some(serde_json::to_vec(&bar_jwks).unwrap()),
            _ => None,
        })
        .await;

        let role_name1 = RoleName::from("anonymous");
        let role_name2 = RoleName::from("authenticated");

        let roles = vec![
            RoleNameInt::from(&role_name1),
            RoleNameInt::from(&role_name2),
        ];
        let rules = vec![
            AuthRule {
                id: "foo".to_owned(),
                jwks_url: format!("http://{jwks_addr}/foo").parse().unwrap(),
                audience: None,
                role_names: roles.clone(),
            },
            AuthRule {
                id: "bar".to_owned(),
                jwks_url: format!("http://{jwks_addr}/bar").parse().unwrap(),
                audience: None,
                role_names: roles.clone(),
            },
        ];

        let fetch = Fetch(rules);
        let jwk_cache = JwkCache::default();

        let endpoint = EndpointId::from("ep");

        let jwt1 = new_rsa_jwt("rs1".into(), rs1);
        let jwt2 = new_rsa_jwt("rs2".into(), rs2);
        let jwt3 = new_ec_jwt("ec1".into(), &ec1);
        let jwt4 = new_ec_jwt("ec2".into(), &ec2);

        let tokens = [jwt1, jwt2, jwt3, jwt4];
        let role_names = [role_name1, role_name2];
        for role in &role_names {
            for token in &tokens {
                jwk_cache
                    .check_jwt(
                        &RequestContext::test(),
                        endpoint.clone(),
                        role,
                        &fetch,
                        token,
                    )
                    .await
                    .unwrap();
            }
        }
    }

    /// AWS Cognito escapes the `/` in the URL.
    #[tokio::test]
    async fn check_jwt_regression_cognito_issuer() {
        let (key, jwk) = new_ec_jwk("key".into());

        let now = now();
        let token = new_custom_ec_jwt(
            "key".into(),
            &key,
            typed_json::json! {{
                "sub": "dd9a73fd-e785-4a13-aae1-e691ce43e89d",
                // cognito uses `\/`. I cannot replicated that easily here as serde_json will refuse
                // to write that escape character. instead I will make a bogus URL using `\` instead.
                "iss": "https:\\\\cognito-idp.us-west-2.amazonaws.com\\us-west-2_abcdefgh",
                "client_id": "abcdefghijklmnopqrstuvwxyz",
                "origin_jti": "6759d132-3fe7-446e-9e90-2fe7e8017893",
                "event_id": "ec9c36ab-b01d-46a0-94e4-87fde6767065",
                "token_use": "access",
                "scope": "aws.cognito.signin.user.admin",
                "auth_time":now,
                "exp":now + 60,
                "iat":now,
                "jti": "b241614b-0b93-4bdc-96db-0a3c7061d9c0",
                "username": "dd9a73fd-e785-4a13-aae1-e691ce43e89d",
            }},
        );

        let jwks = jose_jwk::JwkSet { keys: vec![jwk] };

        let jwks_addr = jwks_server(move |_path| Some(serde_json::to_vec(&jwks).unwrap())).await;

        let role_name = RoleName::from("anonymous");
        let rules = vec![AuthRule {
            id: "aws-cognito".to_owned(),
            jwks_url: format!("http://{jwks_addr}/").parse().unwrap(),
            audience: None,
            role_names: vec![RoleNameInt::from(&role_name)],
        }];

        let fetch = Fetch(rules);
        let jwk_cache = JwkCache::default();

        let endpoint = EndpointId::from("ep");

        jwk_cache
            .check_jwt(
                &RequestContext::test(),
                endpoint.clone(),
                &role_name,
                &fetch,
                &token,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn check_jwt_invalid_signature() {
        let (_, jwk) = new_ec_jwk("1".into());
        let (key, _) = new_ec_jwk("1".into());

        // has a matching kid, but signed by the wrong key
        let bad_jwt = new_ec_jwt("1".into(), &key);

        let jwks = jose_jwk::JwkSet { keys: vec![jwk] };
        let jwks_addr = jwks_server(move |path| match path {
            "/" => Some(serde_json::to_vec(&jwks).unwrap()),
            _ => None,
        })
        .await;

        let role = RoleName::from("authenticated");

        let rules = vec![AuthRule {
            id: String::new(),
            jwks_url: format!("http://{jwks_addr}/").parse().unwrap(),
            audience: None,
            role_names: vec![RoleNameInt::from(&role)],
        }];

        let fetch = Fetch(rules);
        let jwk_cache = JwkCache::default();

        let ep = EndpointId::from("ep");

        let ctx = RequestContext::test();
        let err = jwk_cache
            .check_jwt(&ctx, ep, &role, &fetch, &bad_jwt)
            .await
            .unwrap_err();
        assert!(
            matches!(err, JwtError::Signature(_)),
            "expected \"signature error\", got {err:?}"
        );
    }

    #[tokio::test]
    async fn check_jwt_unknown_role() {
        let (key, jwk) = new_rsa_jwk(RS1, "1".into());
        let jwt = new_rsa_jwt("1".into(), key);

        let jwks = jose_jwk::JwkSet { keys: vec![jwk] };
        let jwks_addr = jwks_server(move |path| match path {
            "/" => Some(serde_json::to_vec(&jwks).unwrap()),
            _ => None,
        })
        .await;

        let role = RoleName::from("authenticated");
        let rules = vec![AuthRule {
            id: String::new(),
            jwks_url: format!("http://{jwks_addr}/").parse().unwrap(),
            audience: None,
            role_names: vec![RoleNameInt::from(&role)],
        }];

        let fetch = Fetch(rules);
        let jwk_cache = JwkCache::default();

        let ep = EndpointId::from("ep");

        // this role_name is not accepted
        let bad_role_name = RoleName::from("cloud_admin");

        let ctx = RequestContext::test();
        let err = jwk_cache
            .check_jwt(&ctx, ep, &bad_role_name, &fetch, &jwt)
            .await
            .unwrap_err();

        assert!(
            matches!(err, JwtError::JwkNotFound),
            "expected \"jwk not found\", got {err:?}"
        );
    }

    #[tokio::test]
    async fn check_jwt_invalid_claims() {
        let (key, jwk) = new_ec_jwk("1".into());

        let jwks = jose_jwk::JwkSet { keys: vec![jwk] };
        let jwks_addr = jwks_server(move |path| match path {
            "/" => Some(serde_json::to_vec(&jwks).unwrap()),
            _ => None,
        })
        .await;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        struct Test {
            body: serde_json::Value,
            error: JwtClaimsError,
        }

        let table = vec![
            Test {
                body: json! {{
                    "nbf": now + 60,
                    "aud": "neon",
                }},
                error: JwtClaimsError::JwtTokenNotYetReadyToUse,
            },
            Test {
                body: json! {{
                    "exp": now - 60,
                    "aud": ["neon"],
                }},
                error: JwtClaimsError::JwtTokenHasExpired,
            },
            Test {
                body: json! {{
                }},
                error: JwtClaimsError::InvalidJwtTokenAudience,
            },
            Test {
                body: json! {{
                    "aud": [],
                }},
                error: JwtClaimsError::InvalidJwtTokenAudience,
            },
            Test {
                body: json! {{
                    "aud": "foo",
                }},
                error: JwtClaimsError::InvalidJwtTokenAudience,
            },
            Test {
                body: json! {{
                    "aud": ["foo"],
                }},
                error: JwtClaimsError::InvalidJwtTokenAudience,
            },
            Test {
                body: json! {{
                    "aud": ["foo", "bar"],
                }},
                error: JwtClaimsError::InvalidJwtTokenAudience,
            },
        ];

        let role = RoleName::from("authenticated");

        let rules = vec![AuthRule {
            id: String::new(),
            jwks_url: format!("http://{jwks_addr}/").parse().unwrap(),
            audience: Some("neon".to_string()),
            role_names: vec![RoleNameInt::from(&role)],
        }];

        let fetch = Fetch(rules);
        let jwk_cache = JwkCache::default();

        let ep = EndpointId::from("ep");

        let ctx = RequestContext::test();
        for test in table {
            let jwt = new_custom_ec_jwt("1".into(), &key, test.body);

            match jwk_cache
                .check_jwt(&ctx, ep.clone(), &role, &fetch, &jwt)
                .await
            {
                Err(JwtError::InvalidClaims(error)) if error == test.error => {}
                Err(err) => {
                    panic!("expected {:?}, got {err:?}", test.error)
                }
                Ok(_payload) => {
                    panic!("expected {:?}, got ok", test.error)
                }
            }
        }
    }

    #[tokio::test]
    async fn check_jwk_keycloak_regression() {
        let (rs, valid_jwk) = new_rsa_jwk(RS1, "rs1".into());
        let valid_jwk = serde_json::to_value(valid_jwk).unwrap();

        // This is valid, but we cannot parse it as we have no support for encryption JWKs, only signature based ones.
        // This is taken directly from keycloak.
        let invalid_jwk = serde_json::json! {
            {
                "kid": "U-Jc9xRli84eNqRpYQoIPF-GNuRWV3ZvAIhziRW2sbQ",
                "kty": "RSA",
                "alg": "RSA-OAEP",
                "use": "enc",
                "n": "yypYWsEKmM_wWdcPnSGLSm5ytw1WG7P7EVkKSulcDRlrM6HWj3PR68YS8LySYM2D9Z-79oAdZGKhIfzutqL8rK1vS14zDuPpAM-RWY3JuQfm1O_-1DZM8-07PmVRegP5KPxsKblLf_My8ByH6sUOIa1p2rbe2q_b0dSTXYu1t0dW-cGL5VShc400YymvTwpc-5uYNsaVxZajnB7JP1OunOiuCJ48AuVp3PqsLzgoXqlXEB1ZZdch3xT3bxaTtNruGvG4xmLZY68O_T3yrwTCNH2h_jFdGPyXdyZToCMSMK2qSbytlfwfN55pT9Vv42Lz1YmoB7XRjI9aExKPc5AxFw",
                "e": "AQAB",
                "x5c": [
                    "MIICmzCCAYMCBgGS41E6azANBgkqhkiG9w0BAQsFADARMQ8wDQYDVQQDDAZtYXN0ZXIwHhcNMjQxMDMxMTYwMTQ0WhcNMzQxMDMxMTYwMzI0WjARMQ8wDQYDVQQDDAZtYXN0ZXIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDLKlhawQqYz/BZ1w+dIYtKbnK3DVYbs/sRWQpK6VwNGWszodaPc9HrxhLwvJJgzYP1n7v2gB1kYqEh/O62ovysrW9LXjMO4+kAz5FZjcm5B+bU7/7UNkzz7Ts+ZVF6A/ko/GwpuUt/8zLwHIfqxQ4hrWnatt7ar9vR1JNdi7W3R1b5wYvlVKFzjTRjKa9PClz7m5g2xpXFlqOcHsk/U66c6K4InjwC5Wnc+qwvOCheqVcQHVll1yHfFPdvFpO02u4a8bjGYtljrw79PfKvBMI0faH+MV0Y/Jd3JlOgIxIwrapJvK2V/B83nmlP1W/jYvPViagHtdGMj1oTEo9zkDEXAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAECYX59+Q9v6c9sb6Q0/C6IgLWG2nVCgVE1YWwIzz+68WrhlmNCRuPjY94roB+tc2tdHbj+Nh3LMzJk7L1KCQoW1+LPK6A6E8W9ad0YPcuw8csV2pUA3+H56exQMH0fUAPQAU7tXWvnQ7otcpV1XA8afn/NTMTsnxi9mSkor8MLMYQ3aeRyh1+LAchHBthWiltqsSUqXrbJF59u5p0ghquuKcWR3TXsA7klGYBgGU5KAJifr9XT87rN0bOkGvbeWAgKvnQnjZwxdnLqTfp/pRY/PiJJHhgIBYPIA7STGnMPjmJ995i34zhnbnd8WHXJA3LxrIMqLW/l8eIdvtM1w8KI="
                ],
                "x5t": "QhfzMMnuAfkReTgZ1HtrfyOeeZs",
                "x5t#S256": "cmHDUdKgLiRCEN28D5FBy9IJLFmR7QWfm77SLhGTCTU"
            }
        };

        let jwks = serde_json::json! {{ "keys": [invalid_jwk, valid_jwk ] }};
        let jwks_addr = jwks_server(move |path| match path {
            "/" => Some(serde_json::to_vec(&jwks).unwrap()),
            _ => None,
        })
        .await;

        let role_name = RoleName::from("anonymous");
        let role = RoleNameInt::from(&role_name);

        let rules = vec![AuthRule {
            id: "foo".to_owned(),
            jwks_url: format!("http://{jwks_addr}/").parse().unwrap(),
            audience: None,
            role_names: vec![role],
        }];

        let fetch = Fetch(rules);
        let jwk_cache = JwkCache::default();

        let endpoint = EndpointId::from("ep");

        let token = new_rsa_jwt("rs1".into(), rs);

        jwk_cache
            .check_jwt(
                &RequestContext::test(),
                endpoint.clone(),
                &role_name,
                &fetch,
                &token,
            )
            .await
            .unwrap();
    }
}
