// For details about authentication see docs/authentication.md

use arc_swap::ArcSwap;
use std::{borrow::Cow, fmt::Display, fs, sync::Arc};

use anyhow::Result;
use camino::Utf8Path;
use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation,
};
use serde::{Deserialize, Serialize};

use crate::{http::error::ApiError, id::TenantId};

/// Algorithm to use. We require EdDSA.
const STORAGE_TOKEN_ALGORITHM: Algorithm = Algorithm::EdDSA;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Provides access to all data for a specific tenant (specified in `struct Claims` below)
    // TODO: join these two?
    Tenant,
    /// Provides blanket access to all tenants on the pageserver plus pageserver-wide APIs.
    /// Should only be used e.g. for status check/tenant creation/list.
    PageServerApi,
    /// Provides blanket access to all data on the safekeeper plus safekeeper-wide APIs.
    /// Should only be used e.g. for status check.
    /// Currently also used for connection from any pageserver to any safekeeper.
    SafekeeperData,
    /// The scope used by pageservers in upcalls to storage controller and cloud control plane
    #[serde(rename = "generations_api")]
    GenerationsApi,
    /// Allows access to control plane managment API and all storage controller endpoints.
    Admin,

    /// Allows access to control plane & storage controller endpoints used in infrastructure automation (e.g. node registration)
    Infra,

    /// Allows access to storage controller APIs used by the scrubber, to interrogate the state
    /// of a tenant & post scrub results.
    Scrubber,

    /// This scope is used for communication with other storage controller instances.
    /// At the time of writing, this is only used for the step down request.
    #[serde(rename = "controller_peer")]
    ControllerPeer,
}

/// JWT payload. See docs/authentication.md for the format
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Claims {
    #[serde(default)]
    pub tenant_id: Option<TenantId>,
    pub scope: Scope,
}

impl Claims {
    pub fn new(tenant_id: Option<TenantId>, scope: Scope) -> Self {
        Self { tenant_id, scope }
    }
}

pub struct SwappableJwtAuth(ArcSwap<JwtAuth>);

impl SwappableJwtAuth {
    pub fn new(jwt_auth: JwtAuth) -> Self {
        SwappableJwtAuth(ArcSwap::new(Arc::new(jwt_auth)))
    }
    pub fn swap(&self, jwt_auth: JwtAuth) {
        self.0.swap(Arc::new(jwt_auth));
    }
    pub fn decode(&self, token: &str) -> std::result::Result<TokenData<Claims>, AuthError> {
        self.0.load().decode(token)
    }
}

impl std::fmt::Debug for SwappableJwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Swappable({:?})", self.0.load())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AuthError(pub Cow<'static, str>);

impl Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<AuthError> for ApiError {
    fn from(_value: AuthError) -> Self {
        // Don't pass on the value of the AuthError as a precautionary measure.
        // Being intentionally vague in public error communication hurts debugability
        // but it is more secure.
        ApiError::Forbidden("JWT authentication error".to_string())
    }
}

pub struct JwtAuth {
    decoding_keys: Vec<DecodingKey>,
    validation: Validation,
}

impl JwtAuth {
    pub fn new(decoding_keys: Vec<DecodingKey>) -> Self {
        let mut validation = Validation::default();
        validation.algorithms = vec![STORAGE_TOKEN_ALGORITHM];
        // The default 'required_spec_claims' is 'exp'. But we don't want to require
        // expiration.
        validation.required_spec_claims = [].into();
        Self {
            decoding_keys,
            validation,
        }
    }

    pub fn from_key_path(key_path: &Utf8Path) -> Result<Self> {
        let metadata = key_path.metadata()?;
        let decoding_keys = if metadata.is_dir() {
            let mut keys = Vec::new();
            for entry in fs::read_dir(key_path)? {
                let path = entry?.path();
                if !path.is_file() {
                    // Ignore directories (don't recurse)
                    continue;
                }
                let public_key = fs::read(path)?;
                keys.push(DecodingKey::from_ed_pem(&public_key)?);
            }
            keys
        } else if metadata.is_file() {
            let public_key = fs::read(key_path)?;
            vec![DecodingKey::from_ed_pem(&public_key)?]
        } else {
            anyhow::bail!("path is neither a directory or a file")
        };
        if decoding_keys.is_empty() {
            anyhow::bail!("Configured for JWT auth with zero decoding keys. All JWT gated requests would be rejected.");
        }
        Ok(Self::new(decoding_keys))
    }

    pub fn from_key(key: String) -> Result<Self> {
        Ok(Self::new(vec![DecodingKey::from_ed_pem(key.as_bytes())?]))
    }

    /// Attempt to decode the token with the internal decoding keys.
    ///
    /// The function tries the stored decoding keys in succession,
    /// and returns the first yielding a successful result.
    /// If there is no working decoding key, it returns the last error.
    pub fn decode(&self, token: &str) -> std::result::Result<TokenData<Claims>, AuthError> {
        let mut res = None;
        for decoding_key in &self.decoding_keys {
            res = Some(decode(token, decoding_key, &self.validation));
            if let Some(Ok(res)) = res {
                return Ok(res);
            }
        }
        if let Some(res) = res {
            res.map_err(|e| AuthError(Cow::Owned(e.to_string())))
        } else {
            Err(AuthError(Cow::Borrowed("no JWT decoding keys configured")))
        }
    }
}

impl std::fmt::Debug for JwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtAuth")
            .field("validation", &self.validation)
            .finish()
    }
}

// this function is used only for testing purposes in CLI e g generate tokens during init
pub fn encode_from_key_file(claims: &Claims, key_data: &[u8]) -> Result<String> {
    let key = EncodingKey::from_ed_pem(key_data)?;
    Ok(encode(&Header::new(STORAGE_TOKEN_ALGORITHM), claims, &key)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Generated with:
    //
    // openssl genpkey -algorithm ed25519 -out ed25519-priv.pem
    // openssl pkey -in ed25519-priv.pem -pubout -out ed25519-pub.pem
    const TEST_PUB_KEY_ED25519: &[u8] = br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARYwaNBayR+eGI0iXB4s3QxE3Nl2g1iWbr6KtLWeVD/w=
-----END PUBLIC KEY-----
"#;

    const TEST_PRIV_KEY_ED25519: &[u8] = br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEID/Drmc1AA6U/znNRWpF3zEGegOATQxfkdWxitcOMsIH
-----END PRIVATE KEY-----
"#;

    #[test]
    fn test_decode() {
        let expected_claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081").unwrap()),
            scope: Scope::Tenant,
        };

        // A test token containing the following payload, signed using TEST_PRIV_KEY_ED25519:
        //
        // ```
        // {
        //   "scope": "tenant",
        //   "tenant_id": "3d1f7595b468230304e0b73cecbcb081",
        //   "iss": "neon.controlplane",
        //   "iat": 1678442479
        // }
        // ```
        //
        let encoded_eddsa = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJpYXQiOjE2Nzg0NDI0Nzl9.rNheBnluMJNgXzSTTJoTNIGy4P_qe0JUHl_nVEGuDCTgHOThPVr552EnmKccrCKquPeW3c2YUk0Y9Oh4KyASAw";

        // Check it can be validated with the public key
        let auth = JwtAuth::new(vec![DecodingKey::from_ed_pem(TEST_PUB_KEY_ED25519).unwrap()]);
        let claims_from_token = auth.decode(encoded_eddsa).unwrap().claims;
        assert_eq!(claims_from_token, expected_claims);
    }

    #[test]
    fn test_encode() {
        let claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081").unwrap()),
            scope: Scope::Tenant,
        };

        let encoded = encode_from_key_file(&claims, TEST_PRIV_KEY_ED25519).unwrap();

        // decode it back
        let auth = JwtAuth::new(vec![DecodingKey::from_ed_pem(TEST_PUB_KEY_ED25519).unwrap()]);
        let decoded = auth.decode(&encoded).unwrap();

        assert_eq!(decoded.claims, claims);
    }
}
