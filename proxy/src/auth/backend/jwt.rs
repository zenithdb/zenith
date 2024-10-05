use std::{
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, ensure, Context};
use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use jose_jwk::crypto::KeyInfo;
use serde::{de::Visitor, Deserialize, Deserializer};
use signature::Verifier;
use tokio::time::Instant;

use crate::{
    context::RequestMonitoring, http::parse_json_body_with_limit, intern::RoleNameInt, EndpointId,
    RoleName,
};

// TODO(conrad): make these configurable.
const CLOCK_SKEW_LEEWAY: Duration = Duration::from_secs(30);
const MIN_RENEW: Duration = Duration::from_secs(30);
const AUTO_RENEW: Duration = Duration::from_secs(300);
const MAX_RENEW: Duration = Duration::from_secs(3600);
const MAX_JWK_BODY_SIZE: usize = 64 * 1024;

/// How to get the JWT auth rules
pub(crate) trait FetchAuthRules: Clone + Send + Sync + 'static {
    fn fetch_auth_rules(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> impl Future<Output = anyhow::Result<Vec<AuthRule>>> + Send;
}

pub(crate) struct AuthRule {
    pub(crate) id: String,
    pub(crate) jwks_url: url::Url,
    pub(crate) audience: Option<String>,
    pub(crate) role_names: Vec<RoleNameInt>,
}

#[derive(Default)]
pub struct JwkCache {
    client: reqwest::Client,

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
        ctx: &RequestMonitoring,
        client: &reqwest::Client,
        endpoint: EndpointId,
        auth_rules: &F,
    ) -> anyhow::Result<Arc<JwkCacheEntry>> {
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
            let req = client.get(rule.jwks_url.clone());
            // TODO(conrad): eventually switch to using reqwest_middleware/`new_client_with_timeout`.
            // TODO(conrad): We need to filter out URLs that point to local resources. Public internet only.
            match req.send().await.and_then(|r| r.error_for_status()) {
                // todo: should we re-insert JWKs if we want to keep this JWKs URL?
                // I expect these failures would be quite sparse.
                Err(e) => tracing::warn!(url=?rule.jwks_url, error=?e, "could not fetch JWKs"),
                Ok(r) => {
                    let resp: http::Response<reqwest::Body> = r.into();
                    match parse_json_body_with_limit::<jose_jwk::JwkSet, _>(
                        PhantomData,
                        resp.into_body(),
                        MAX_JWK_BODY_SIZE,
                    )
                    .await
                    {
                        Err(e) => {
                            tracing::warn!(url=?rule.jwks_url, error=%e, "could not decode JWKs");
                        }
                        Ok(jwks) => {
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
                }
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
        ctx: &RequestMonitoring,
        client: &reqwest::Client,
        endpoint: EndpointId,
        fetch: &F,
    ) -> Result<Arc<JwkCacheEntry>, anyhow::Error> {
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
        ctx: &RequestMonitoring,
        jwt: &str,
        client: &reqwest::Client,
        endpoint: EndpointId,
        role_name: &RoleName,
        fetch: &F,
    ) -> Result<(), anyhow::Error> {
        // JWT compact form is defined to be
        // <B64(Header)> || . || <B64(Payload)> || . || <B64(Signature)>
        // where Signature = alg(<B64(Header)> || . || <B64(Payload)>);

        let (header_payload, signature) = jwt
            .rsplit_once('.')
            .context("Provided authentication token is not a valid JWT encoding")?;
        let (header, payload) = header_payload
            .split_once('.')
            .context("Provided authentication token is not a valid JWT encoding")?;

        let header = base64::decode_config(header, base64::URL_SAFE_NO_PAD)
            .context("Provided authentication token is not a valid JWT encoding")?;
        let header = serde_json::from_slice::<JwtHeader<'_>>(&header)
            .context("Provided authentication token is not a valid JWT encoding")?;

        let sig = base64::decode_config(signature, base64::URL_SAFE_NO_PAD)
            .context("Provided authentication token is not a valid JWT encoding")?;

        let kid = header.key_id.context("missing key id")?;

        let mut guard = self
            .get_or_update_jwk_cache(ctx, client, endpoint.clone(), fetch)
            .await?;

        // get the key from the JWKs if possible. If not, wait for the keys to update.
        let (jwk, expected_audience) = loop {
            match guard.find_jwk_and_audience(kid, role_name) {
                Some(jwk) => break jwk,
                None if guard.last_retrieved.elapsed() > MIN_RENEW => {
                    let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);

                    let permit = self.acquire_permit().await;
                    guard = self
                        .renew_jwks(permit, ctx, client, endpoint.clone(), fetch)
                        .await?;
                }
                _ => {
                    bail!("jwk not found");
                }
            }
        };

        ensure!(
            jwk.is_supported(&header.algorithm),
            "signature algorithm not supported"
        );

        match &jwk.key {
            jose_jwk::Key::Ec(key) => {
                verify_ec_signature(header_payload.as_bytes(), &sig, key)?;
            }
            jose_jwk::Key::Rsa(key) => {
                verify_rsa_signature(header_payload.as_bytes(), &sig, key, &header.algorithm)?;
            }
            key => bail!("unsupported key type {key:?}"),
        };

        let payload = base64::decode_config(payload, base64::URL_SAFE_NO_PAD)
            .context("Provided authentication token is not a valid JWT encoding")?;
        let payload = serde_json::from_slice::<JwtPayload<'_>>(&payload)
            .context("Provided authentication token is not a valid JWT encoding")?;

        tracing::debug!(?payload, "JWT signature valid with claims");

        if let Some(aud) = expected_audience {
            ensure!(
                payload.audience.0.iter().any(|s| s == aud),
                "invalid JWT token audience"
            );
        }

        let now = SystemTime::now();

        if let Some(exp) = payload.expiration {
            ensure!(now < exp + CLOCK_SKEW_LEEWAY, "JWT token has expired");
        }

        if let Some(nbf) = payload.not_before {
            ensure!(
                nbf < now + CLOCK_SKEW_LEEWAY,
                "JWT token is not yet ready to use"
            );
        }

        Ok(())
    }
}

impl JwkCache {
    pub(crate) async fn check_jwt<F: FetchAuthRules>(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
        role_name: &RoleName,
        fetch: &F,
        jwt: &str,
    ) -> Result<(), anyhow::Error> {
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

fn verify_ec_signature(data: &[u8], sig: &[u8], key: &jose_jwk::Ec) -> anyhow::Result<()> {
    use ecdsa::Signature;
    use signature::Verifier;

    match key.crv {
        jose_jwk::EcCurves::P256 => {
            let pk =
                p256::PublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid P256 key"))?;
            let key = p256::ecdsa::VerifyingKey::from(&pk);
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        key => bail!("unsupported ec key type {key:?}"),
    }

    Ok(())
}

fn verify_rsa_signature(
    data: &[u8],
    sig: &[u8],
    key: &jose_jwk::Rsa,
    alg: &jose_jwa::Algorithm,
) -> anyhow::Result<()> {
    use jose_jwa::{Algorithm, Signing};
    use rsa::{
        pkcs1v15::{Signature, VerifyingKey},
        RsaPublicKey,
    };

    let key = RsaPublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid RSA key"))?;

    match alg {
        Algorithm::Signing(Signing::Rs256) => {
            let key = VerifyingKey::<sha2::Sha256>::new(key);
            let sig = Signature::try_from(sig)?;
            key.verify(data, &sig)?;
        }
        _ => bail!("invalid RSA signing algorithm"),
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
    #[serde(rename = "kid")]
    key_id: Option<&'a str>,
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
    #[serde(rename = "iss")]
    issuer: Option<&'a str>,
    /// Subject of the JWT (the user)
    #[serde(rename = "sub")]
    subject: Option<&'a str>,
    /// Unique token identifier
    #[serde(rename = "jti")]
    jwt_id: Option<&'a str>,
    /// Unique session identifier
    #[serde(rename = "sid")]
    session_id: Option<&'a str>,
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

#[cfg(test)]
mod tests {
    use crate::RoleName;

    use super::*;

    use std::{future::IntoFuture, net::SocketAddr, time::SystemTime};

    use base64::URL_SAFE_NO_PAD;
    use bytes::Bytes;
    use http::Response;
    use http_body_util::Full;
    use hyper1::service::service_fn;
    use hyper_util::rt::TokioIo;
    use rand::rngs::OsRng;
    use rsa::pkcs8::DecodePrivateKey;
    use signature::Signer;
    use tokio::net::TcpListener;

    fn new_ec_jwk(kid: String) -> (p256::SecretKey, jose_jwk::Jwk) {
        let sk = p256::SecretKey::random(&mut OsRng);
        let pk = sk.public_key().into();
        let jwk = jose_jwk::Jwk {
            key: jose_jwk::Key::Ec(pk),
            prm: jose_jwk::Parameters {
                kid: Some(kid),
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
                ..Default::default()
            },
        };
        (sk, jwk)
    }

    fn build_jwt_payload(kid: String, sig: jose_jwa::Signing) -> String {
        let header = JwtHeader {
            algorithm: jose_jwa::Algorithm::Signing(sig),
            key_id: Some(&kid),
        };
        let body = typed_json::json! {{
            "exp": SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() + 3600,
        }};

        let header =
            base64::encode_config(serde_json::to_string(&header).unwrap(), URL_SAFE_NO_PAD);
        let body = base64::encode_config(body.to_string(), URL_SAFE_NO_PAD);

        format!("{header}.{body}")
    }

    fn new_ec_jwt(kid: String, key: &p256::SecretKey) -> String {
        use p256::ecdsa::{Signature, SigningKey};

        let payload = build_jwt_payload(kid, jose_jwa::Signing::Es256);
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

    #[tokio::test]
    async fn renew() {
        let (rs1, jwk1) = new_rsa_jwk(RS1, "1".into());
        let (rs2, jwk2) = new_rsa_jwk(RS2, "2".into());
        let (ec1, jwk3) = new_ec_jwk("3".into());
        let (ec2, jwk4) = new_ec_jwk("4".into());

        let foo_jwks = jose_jwk::JwkSet {
            keys: vec![jwk1, jwk3],
        };
        let bar_jwks = jose_jwk::JwkSet {
            keys: vec![jwk2, jwk4],
        };

        let service = service_fn(move |req| {
            let foo_jwks = foo_jwks.clone();
            let bar_jwks = bar_jwks.clone();
            async move {
                let jwks = match req.uri().path() {
                    "/foo" => &foo_jwks,
                    "/bar" => &bar_jwks,
                    _ => {
                        return Response::builder()
                            .status(404)
                            .body(Full::new(Bytes::new()));
                    }
                };
                let body = serde_json::to_vec(jwks).unwrap();
                Response::builder()
                    .status(200)
                    .body(Full::new(Bytes::from(body)))
            }
        });

        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let server = hyper1::server::conn::http1::Builder::new();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (s, _) = listener.accept().await.unwrap();
                let serve = server.serve_connection(TokioIo::new(s), service.clone());
                tokio::spawn(serve.into_future());
            }
        });

        let client = reqwest::Client::new();

        #[derive(Clone)]
        struct Fetch(SocketAddr, Vec<RoleNameInt>);

        impl FetchAuthRules for Fetch {
            async fn fetch_auth_rules(
                &self,
                _ctx: &RequestMonitoring,
                _endpoint: EndpointId,
            ) -> anyhow::Result<Vec<AuthRule>> {
                Ok(vec![
                    AuthRule {
                        id: "foo".to_owned(),
                        jwks_url: format!("http://{}/foo", self.0).parse().unwrap(),
                        audience: None,
                        role_names: self.1.clone(),
                    },
                    AuthRule {
                        id: "bar".to_owned(),
                        jwks_url: format!("http://{}/bar", self.0).parse().unwrap(),
                        audience: None,
                        role_names: self.1.clone(),
                    },
                ])
            }
        }

        let role_name1 = RoleName::from("anonymous");
        let role_name2 = RoleName::from("authenticated");

        let fetch = Fetch(
            addr,
            vec![
                RoleNameInt::from(&role_name1),
                RoleNameInt::from(&role_name2),
            ],
        );

        let endpoint = EndpointId::from("ep");

        let jwk_cache = Arc::new(JwkCacheEntryLock::default());

        let jwt1 = new_rsa_jwt("1".into(), rs1);
        let jwt2 = new_rsa_jwt("2".into(), rs2);
        let jwt3 = new_ec_jwt("3".into(), &ec1);
        let jwt4 = new_ec_jwt("4".into(), &ec2);

        // had the wrong kid, therefore will have the wrong ecdsa signature
        let bad_jwt = new_ec_jwt("3".into(), &ec2);
        // this role_name is not accepted
        let bad_role_name = RoleName::from("cloud_admin");

        let err = jwk_cache
            .check_jwt(
                &RequestMonitoring::test(),
                &bad_jwt,
                &client,
                endpoint.clone(),
                &role_name1,
                &fetch,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("signature error"));

        let err = jwk_cache
            .check_jwt(
                &RequestMonitoring::test(),
                &jwt1,
                &client,
                endpoint.clone(),
                &bad_role_name,
                &fetch,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("jwk not found"));

        let tokens = [jwt1, jwt2, jwt3, jwt4];
        let role_names = [role_name1, role_name2];
        for role in &role_names {
            for token in &tokens {
                jwk_cache
                    .check_jwt(
                        &RequestMonitoring::test(),
                        token,
                        &client,
                        endpoint.clone(),
                        role,
                        &fetch,
                    )
                    .await
                    .unwrap();
            }
        }
    }
}
