//! Tools for SCRAM server secret management.

use subtle::{Choice, ConstantTimeEq};

use super::base64_decode_array;
use super::key::ScramKey;

/// Server secret is produced from user's password,
/// and is used throughout the authentication process.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct ServerSecret {
    /// Number of iterations for `PBKDF2` function.
    pub(crate) iterations: u32,
    /// Salt used to hash user's password.
    pub(crate) salt_base64: String,
    /// Hashed `ClientKey`.
    pub(crate) stored_key: ScramKey,
    /// Used by client to verify server's signature.
    pub(crate) server_key: ScramKey,
    /// Should auth fail no matter what?
    /// This is exactly the case for mocked secrets.
    pub(crate) doomed: bool,
}

impl ServerSecret {
    pub(crate) fn parse(input: &str) -> Option<Self> {
        // SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
        let s = input.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = s.split_once('$')?;

        let ((iterations, salt), (stored_key, server_key)) =
            params.split_once(':').zip(keys.split_once(':'))?;

        let secret = ServerSecret {
            iterations: iterations.parse().ok()?,
            salt_base64: salt.to_owned(),
            stored_key: base64_decode_array(stored_key)?.into(),
            server_key: base64_decode_array(server_key)?.into(),
            doomed: false,
        };

        Some(secret)
    }

    pub(crate) fn is_password_invalid(&self, client_key: &ScramKey) -> Choice {
        // constant time to not leak partial key match
        client_key.sha256().ct_ne(&self.stored_key) | Choice::from(self.doomed as u8)
    }

    /// To avoid revealing information to an attacker, we use a
    /// mocked server secret even if the user doesn't exist.
    /// See `auth-scram.c : mock_scram_secret` for details.
    pub(crate) fn mock(nonce: [u8; 32]) -> Self {
        Self {
            // this doesn't reveal much information as we're going to use
            // iteration count 1 for our generated passwords going forward.
            // PG16 users can set iteration count=1 already today.
            iterations: 1,
            salt_base64: base64::encode(nonce),
            stored_key: ScramKey::default(),
            server_key: ScramKey::default(),
            doomed: true,
        }
    }

    /// Build a new server secret from the prerequisites.
    /// XXX: We only use this function in tests.
    #[cfg(test)]
    pub(crate) async fn build(password: &str) -> Option<Self> {
        Self::parse(&postgres_protocol::password::scram_sha_256(password.as_bytes()).await)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_scram_secret() {
        let iterations = 4096;
        let salt = "+/tQQax7twvwTj64mjBsxQ==";
        let stored_key = "D5h6KTMBlUvDJk2Y8ELfC1Sjtc6k9YHjRyuRZyBNJns=";
        let server_key = "Pi3QHbcluX//NDfVkKlFl88GGzlJ5LkyPwcdlN/QBvI=";

        let secret = format!("SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}");

        let parsed = ServerSecret::parse(&secret).unwrap();
        assert_eq!(parsed.iterations, iterations);
        assert_eq!(parsed.salt_base64, salt);

        assert_eq!(base64::encode(parsed.stored_key), stored_key);
        assert_eq!(base64::encode(parsed.server_key), server_key);
    }
}
