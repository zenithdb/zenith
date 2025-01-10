//! Salted Challenge Response Authentication Mechanism.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc5802>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-scram.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth-scram.c>

mod countmin;
mod exchange;
mod key;
mod messages;
mod pbkdf2;
mod secret;
mod signature;
pub mod threadpool;

pub(crate) use exchange::{exchange, Exchange};
use hmac::{Hmac, Mac};
pub(crate) use key::ScramKey;
pub(crate) use secret::ServerSecret;
use sha2::{Digest, Sha256};

const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// A list of supported SCRAM methods.
pub(crate) const METHODS: &[&str] = &[SCRAM_SHA_256_PLUS, SCRAM_SHA_256];
pub(crate) const METHODS_WITHOUT_PLUS: &[&str] = &[SCRAM_SHA_256];

/// Decode base64 into array without any heap allocations
fn base64_decode_array<const N: usize>(input: impl AsRef<[u8]>) -> Option<[u8; N]> {
    let mut bytes = [0u8; N];

    let size = base64::decode_config_slice(input, base64::STANDARD, &mut bytes).ok()?;
    if size != N {
        return None;
    }

    Some(bytes)
}

/// This function essentially is `Hmac(sha256, key, input)`.
/// Further reading: <https://datatracker.ietf.org/doc/html/rfc2104>.
fn hmac_sha256<'a>(key: &[u8], parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("bad key size");
    parts.into_iter().for_each(|s| mac.update(s));

    mac.finalize().into_bytes().into()
}

fn sha256<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    parts.into_iter().for_each(|s| hasher.update(s));

    hasher.finalize().into()
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::threadpool::ThreadPool;
    use super::{Exchange, ServerSecret};
    use crate::intern::EndpointIdInt;
    use crate::sasl::{Mechanism, Step};
    use crate::types::EndpointId;

    #[test]
    fn snapshot() {
        let iterations = 4096;
        let salt = "QSXCR+Q6sek8bf92";
        let stored_key = "FO+9jBb3MUukt6jJnzjPZOWc5ow/Pu6JtPyju0aqaE8=";
        let server_key = "qxJ1SbmSAi5EcS0J5Ck/cKAm/+Ixa+Kwp63f4OHDgzo=";
        let secret = format!("SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}",);
        let secret = ServerSecret::parse(&secret).unwrap();

        const NONCE: [u8; 18] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let mut exchange =
            Exchange::new(&secret, || NONCE, crate::tls::TlsServerEndPoint::Undefined);

        let client_first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let client_final = "c=biws,r=rOprNGfwEbeRWgbNEkqOAQIDBAUGBwgJCgsMDQ4PEBES,p=rw1r5Kph5ThxmaUBC2GAQ6MfXbPnNkFiTIvdb/Rear0=";
        let server_first =
            "r=rOprNGfwEbeRWgbNEkqOAQIDBAUGBwgJCgsMDQ4PEBES,s=QSXCR+Q6sek8bf92,i=4096";
        let server_final = "v=qtUDIofVnIhM7tKn93EQUUt5vgMOldcDVu1HC+OH0o0=";

        exchange = match exchange.exchange(client_first).unwrap() {
            Step::Continue(exchange, message) => {
                assert_eq!(message, server_first);
                exchange
            }
            Step::Success(_, _) => panic!("expected continue, got success"),
            Step::Failure(f) => panic!("{f}"),
        };

        let key = match exchange.exchange(client_final).unwrap() {
            Step::Success(key, message) => {
                assert_eq!(message, server_final);
                key
            }
            Step::Continue(_, _) => panic!("expected success, got continue"),
            Step::Failure(f) => panic!("{f}"),
        };

        assert_eq!(
            key.as_bytes(),
            [
                74, 103, 1, 132, 12, 31, 200, 48, 28, 54, 82, 232, 207, 12, 138, 189, 40, 32, 134,
                27, 125, 170, 232, 35, 171, 167, 166, 41, 70, 228, 182, 112,
            ]
        );
    }

    async fn run_round_trip_test(server_password: &str, client_password: &str) {
        let pool = ThreadPool::new(1);

        let ep = EndpointId::from("foo");
        let ep = EndpointIdInt::from(ep);

        let scram_secret = ServerSecret::build(server_password).await.unwrap();
        let outcome = super::exchange(&pool, ep, &scram_secret, client_password.as_bytes())
            .await
            .unwrap();

        match outcome {
            crate::sasl::Outcome::Success(_) => {}
            crate::sasl::Outcome::Failure(r) => panic!("{r}"),
        }
    }

    #[tokio::test]
    async fn round_trip() {
        run_round_trip_test("pencil", "pencil").await;
    }

    #[tokio::test]
    #[should_panic(expected = "password doesn't match")]
    async fn failure() {
        run_round_trip_test("pencil", "eraser").await;
    }
}
