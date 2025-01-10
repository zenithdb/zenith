//! Implementation of the SCRAM authentication algorithm.

use std::convert::Infallible;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::messages::{
    ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage, SCRAM_RAW_NONCE_LEN,
};
use super::pbkdf2::Pbkdf2;
use super::secret::ServerSecret;
use super::signature::SignatureBuilder;
use super::threadpool::ThreadPool;
use super::ScramKey;
use crate::intern::EndpointIdInt;
use crate::sasl::{self, ChannelBinding, Error as SaslError};

/// The only channel binding mode we currently support.
#[derive(Debug)]
struct TlsServerEndPoint;

impl std::fmt::Display for TlsServerEndPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tls-server-end-point")
    }
}

impl std::str::FromStr for TlsServerEndPoint {
    type Err = sasl::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tls-server-end-point" => Ok(TlsServerEndPoint),
            _ => Err(sasl::Error::ChannelBindingBadMethod(s.into())),
        }
    }
}

struct SaslSentInner {
    cbind_flag: ChannelBinding<TlsServerEndPoint>,
    client_first_message_bare: String,
    server_first_message: OwnedServerFirstMessage,
}

struct SaslInitial {
    nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
}

enum ExchangeState {
    /// Waiting for [`ClientFirstMessage`].
    Initial(SaslInitial),
    /// Waiting for [`ClientFinalMessage`].
    SaltSent(SaslSentInner),
}

/// Server's side of SCRAM auth algorithm.
pub(crate) struct Exchange<'a> {
    state: ExchangeState,
    secret: &'a ServerSecret,
    tls_server_end_point: crate::tls::TlsServerEndPoint,
}

impl<'a> Exchange<'a> {
    pub(crate) fn new(
        secret: &'a ServerSecret,
        nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
        tls_server_end_point: crate::tls::TlsServerEndPoint,
    ) -> Self {
        Self {
            state: ExchangeState::Initial(SaslInitial { nonce }),
            secret,
            tls_server_end_point,
        }
    }
}

// copied from <https://github.com/neondatabase/rust-postgres/blob/20031d7a9ee1addeae6e0968e3899ae6bf01cee2/postgres-protocol/src/authentication/sasl.rs#L236-L248>
async fn derive_client_key(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> ScramKey {
    let salted_password = pool
        .spawn_job(endpoint, Pbkdf2::start(password, salt, iterations))
        .await;

    let make_key = |name| {
        let key = Hmac::<Sha256>::new_from_slice(&salted_password)
            .expect("HMAC is able to accept all key sizes")
            .chain_update(name)
            .finalize();

        <[u8; 32]>::from(key.into_bytes())
    };

    make_key(b"Client Key").into()
}

pub(crate) async fn exchange(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    secret: &ServerSecret,
    password: &[u8],
) -> sasl::Result<sasl::Outcome<super::ScramKey>> {
    let salt = base64::decode(&secret.salt_base64)?;
    let client_key = derive_client_key(pool, endpoint, password, &salt, secret.iterations).await;

    if secret.is_password_invalid(&client_key).into() {
        Ok(sasl::Outcome::Failure("password doesn't match"))
    } else {
        Ok(sasl::Outcome::Success(client_key))
    }
}

impl SaslInitial {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_end_point: &crate::tls::TlsServerEndPoint,
        input: &str,
    ) -> sasl::Result<sasl::Step<SaslSentInner, Infallible>> {
        let client_first_message = ClientFirstMessage::parse(input)
            .ok_or(SaslError::BadClientMessage("invalid client-first-message"))?;

        // If the flag is set to "y" and the server supports channel
        // binding, the server MUST fail authentication
        if client_first_message.cbind_flag == ChannelBinding::NotSupportedServer
            && tls_server_end_point.supported()
        {
            return Err(SaslError::ChannelBindingFailed("SCRAM-PLUS not used"));
        }

        let server_first_message = client_first_message.build_server_first_message(
            &(self.nonce)(),
            &secret.salt_base64,
            secret.iterations,
        );
        let msg = server_first_message.as_str().to_owned();

        let next = SaslSentInner {
            cbind_flag: client_first_message.cbind_flag.and_then(str::parse)?,
            client_first_message_bare: client_first_message.bare.to_owned(),
            server_first_message,
        };

        Ok(sasl::Step::Continue(next, msg))
    }
}

impl SaslSentInner {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_end_point: &crate::tls::TlsServerEndPoint,
        input: &str,
    ) -> sasl::Result<sasl::Step<Infallible, super::ScramKey>> {
        let Self {
            cbind_flag,
            client_first_message_bare,
            server_first_message,
        } = self;

        let client_final_message = ClientFinalMessage::parse(input)
            .ok_or(SaslError::BadClientMessage("invalid client-final-message"))?;

        let channel_binding = cbind_flag.encode(|_| match tls_server_end_point {
            crate::tls::TlsServerEndPoint::Sha256(x) => Ok(x),
            crate::tls::TlsServerEndPoint::Undefined => Err(SaslError::MissingBinding),
        })?;

        // This might've been caused by a MITM attack
        if client_final_message.channel_binding != channel_binding {
            return Err(SaslError::ChannelBindingFailed(
                "insecure connection: secure channel data mismatch",
            ));
        }

        if client_final_message.nonce != server_first_message.nonce() {
            return Err(SaslError::BadClientMessage("combined nonce doesn't match"));
        }

        let signature_builder = SignatureBuilder {
            client_first_message_bare,
            server_first_message: server_first_message.as_str(),
            client_final_message_without_proof: client_final_message.without_proof,
        };

        let client_key = signature_builder
            .build(&secret.stored_key)
            .derive_client_key(&client_final_message.proof);

        // Auth fails either if keys don't match or it's pre-determined to fail.
        if secret.is_password_invalid(&client_key).into() {
            return Ok(sasl::Step::Failure("password doesn't match"));
        }

        let msg =
            client_final_message.build_server_final_message(signature_builder, &secret.server_key);

        Ok(sasl::Step::Success(client_key, msg))
    }
}

impl sasl::Mechanism for Exchange<'_> {
    type Output = super::ScramKey;

    fn exchange(mut self, input: &str) -> sasl::Result<sasl::Step<Self, Self::Output>> {
        use sasl::Step;
        use ExchangeState;
        match &self.state {
            ExchangeState::Initial(init) => {
                match init.transition(self.secret, &self.tls_server_end_point, input)? {
                    Step::Continue(sent, msg) => {
                        self.state = ExchangeState::SaltSent(sent);
                        Ok(Step::Continue(self, msg))
                    }
                    Step::Failure(msg) => Ok(Step::Failure(msg)),
                }
            }
            ExchangeState::SaltSent(sent) => {
                match sent.transition(self.secret, &self.tls_server_end_point, input)? {
                    Step::Success(keys, msg) => Ok(Step::Success(keys, msg)),
                    Step::Failure(msg) => Ok(Step::Failure(msg)),
                }
            }
        }
    }
}
