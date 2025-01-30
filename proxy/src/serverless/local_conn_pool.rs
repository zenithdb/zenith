//! Manages the pool of connections between local_proxy and postgres.
//!
//! The pool is keyed by database and role_name, and can contain multiple connections
//! shared between users.
//!
//! The pool manages the pg_session_jwt extension used for authorizing
//! requests in the db.
//!
//! The first time a db/role pair is seen, local_proxy attempts to install the extension
//! and grant usage to the role on the given schema.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{ready, Poll};
use std::time::Duration;

use ed25519_dalek::{Signature, Signer, SigningKey};
use futures::future::poll_fn;
use futures::Future;
use indexmap::IndexMap;
use jose_jwk::jose_b64::base64ct::{Base64UrlUnpadded, Encoding};
use parking_lot::RwLock;
use postgres_client::tls::NoTlsStream;
use postgres_client::types::ToSql;
use postgres_client::AsyncMessage;
use serde_json::value::RawValue;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, warn, Instrument};

use super::backend::HttpConnError;
use super::conn_pool_lib::{
    Client, ClientDataEnum, ClientInnerCommon, ClientInnerExt, ConnInfo, DbUserConn,
    EndpointConnPool,
};
use crate::context::RequestContext;
use crate::control_plane::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::Metrics;

pub(crate) const EXT_NAME: &str = "pg_session_jwt";
pub(crate) const EXT_VERSION: &str = "0.2.0";
pub(crate) const EXT_SCHEMA: &str = "auth";

#[derive(Clone)]
pub(crate) struct ClientDataLocal {
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    cancel: CancellationToken,
    key: SigningKey,
    jti: u64,
}

impl ClientDataLocal {
    pub fn session(&mut self) -> &mut tokio::sync::watch::Sender<uuid::Uuid> {
        &mut self.session
    }

    pub fn cancel(&mut self) {
        self.cancel.cancel();
    }
}

pub(crate) struct LocalConnPool<C: ClientInnerExt> {
    global_pool: Arc<RwLock<EndpointConnPool<C>>>,

    config: &'static crate::config::HttpConfig,
}

impl<C: ClientInnerExt> LocalConnPool<C> {
    pub(crate) fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        Arc::new(Self {
            global_pool: Arc::new(RwLock::new(EndpointConnPool::new(
                HashMap::new(),
                0,
                config.pool_options.max_conns_per_endpoint,
                Arc::new(AtomicUsize::new(0)),
                config.pool_options.max_total_conns,
                String::from("local_pool"),
            ))),
            config,
        })
    }

    pub(crate) fn get_idle_timeout(&self) -> Duration {
        self.config.pool_options.idle_timeout
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        ctx: &RequestContext,
        conn_info: &ConnInfo,
    ) -> Result<Option<Client<C>>, HttpConnError> {
        let client = self
            .global_pool
            .write()
            .get_conn_entry(conn_info.db_and_user())
            .map(|entry| entry.conn);

        // ok return cached connection if found and establish a new one otherwise
        if let Some(mut client) = client {
            if client.inner.is_closed() {
                info!("local_pool: cached connection '{conn_info}' is closed, opening a new one");
                return Ok(None);
            }

            tracing::Span::current()
                .record("conn_id", tracing::field::display(client.get_conn_id()));
            tracing::Span::current().record(
                "pid",
                tracing::field::display(client.inner.get_process_id()),
            );
            debug!(
                cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
                "local_pool: reusing connection '{conn_info}'"
            );

            match client.get_data() {
                ClientDataEnum::Local(data) => {
                    data.session().send(ctx.session_id())?;
                }

                ClientDataEnum::Remote(data) => {
                    data.session().send(ctx.session_id())?;
                }
                ClientDataEnum::Http(_) => (),
            }

            ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
            ctx.success();

            return Ok(Some(Client::new(
                client,
                conn_info.clone(),
                Arc::downgrade(&self.global_pool),
            )));
        }
        Ok(None)
    }

    pub(crate) fn initialized(self: &Arc<Self>, conn_info: &ConnInfo) -> bool {
        if let Some(pool) = self.global_pool.read().get_pool(conn_info.db_and_user()) {
            return pool.is_initialized();
        }
        false
    }

    pub(crate) fn set_initialized(self: &Arc<Self>, conn_info: &ConnInfo) {
        if let Some(pool) = self
            .global_pool
            .write()
            .get_pool_mut(conn_info.db_and_user())
        {
            pool.set_initialized();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn poll_client<C: ClientInnerExt>(
    global_pool: Arc<LocalConnPool<C>>,
    ctx: &RequestContext,
    conn_info: ConnInfo,
    client: C,
    mut connection: postgres_client::Connection<TcpStream, NoTlsStream>,
    key: SigningKey,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<C> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol());
    let mut session_id = ctx.session_id();
    let (tx, mut rx) = tokio::sync::watch::channel(session_id);

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info();
    span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, %session_id, "new connection");
    });
    let pool = Arc::downgrade(&global_pool);

    let db_user = conn_info.db_and_user();
    let idle = global_pool.get_idle_timeout();
    let cancel = CancellationToken::new();
    let cancelled = cancel.clone().cancelled_owned();

    tokio::spawn(
    async move {
        let _conn_gauge = conn_gauge;
        let mut idle_timeout = pin!(tokio::time::sleep(idle));
        let mut cancelled = pin!(cancelled);

        poll_fn(move |cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                info!("connection dropped");
                return Poll::Ready(())
            }

            match rx.has_changed() {
                Ok(true) => {
                    session_id = *rx.borrow_and_update();
                    info!(%session_id, "changed session");
                    idle_timeout.as_mut().reset(Instant::now() + idle);
                }
                Err(_) => {
                    info!("connection dropped");
                    return Poll::Ready(())
                }
                _ => {}
            }

            // 5 minute idle connection timeout
            if idle_timeout.as_mut().poll(cx).is_ready() {
                idle_timeout.as_mut().reset(Instant::now() + idle);
                info!("connection idle");
                if let Some(pool) = pool.clone().upgrade() {
                    // remove client from pool - should close the connection if it's idle.
                    // does nothing if the client is currently checked-out and in-use
                    if pool.global_pool.write().remove_client(db_user.clone(), conn_id) {
                        info!("idle connection removed");
                    }
                }
            }

            loop {
                let message = ready!(connection.poll_message(cx));

                match message {
                    Some(Ok(AsyncMessage::Notification(notif))) => {
                        warn!(%session_id, pid = notif.process_id(), channel = notif.channel(), "notification received");
                    }
                    Some(Ok(_)) => {
                        warn!(%session_id, "unknown message");
                    }
                    Some(Err(e)) => {
                        error!(%session_id, "connection error: {}", e);
                        break
                    }
                    None => {
                        info!("connection closed");
                        break
                    }
                }
            }

            // remove from connection pool
            if let Some(pool) = pool.clone().upgrade() {
                if pool.global_pool.write().remove_client(db_user.clone(), conn_id) {
                    info!("closed connection removed");
                }
            }

            Poll::Ready(())
        }).await;

    }
    .instrument(span));

    let inner = ClientInnerCommon {
        inner: client,
        aux,
        conn_id,
        data: ClientDataEnum::Local(ClientDataLocal {
            session: tx,
            cancel,
            key,
            jti: 0,
        }),
    };

    Client::new(inner, conn_info, Arc::downgrade(&global_pool.global_pool))
}

impl ClientInnerCommon<postgres_client::Client> {
    pub(crate) async fn set_jwt_session(&mut self, payload: &[u8]) -> Result<(), HttpConnError> {
        if let ClientDataEnum::Local(local_data) = &mut self.data {
            local_data.jti += 1;
            let token = resign_jwt(&local_data.key, payload, local_data.jti)?;

            // initiates the auth session
            self.inner.batch_execute("discard all").await?;
            self.inner
                .execute(
                    "select auth.jwt_session_init($1)",
                    &[&&*token as &(dyn ToSql + Sync)],
                )
                .await?;

            let pid = self.inner.get_process_id();
            info!(pid, jti = local_data.jti, "user session state init");
            Ok(())
        } else {
            panic!("unexpected client data type");
        }
    }
}

/// implements relatively efficient in-place json object key upserting
///
/// only supports top-level keys
fn upsert_json_object(
    payload: &[u8],
    key: &str,
    value: &RawValue,
) -> Result<String, serde_json::Error> {
    let mut payload = serde_json::from_slice::<IndexMap<&str, &RawValue>>(payload)?;
    payload.insert(key, value);
    serde_json::to_string(&payload)
}

fn resign_jwt(sk: &SigningKey, payload: &[u8], jti: u64) -> Result<String, HttpConnError> {
    let mut buffer = itoa::Buffer::new();

    // encode the jti integer to a json rawvalue
    let jti = serde_json::from_str::<&RawValue>(buffer.format(jti))
        .expect("itoa formatted integer should be guaranteed valid json");

    // update the jti in-place
    let payload =
        upsert_json_object(payload, "jti", jti).map_err(HttpConnError::JwtPayloadError)?;

    // sign the jwt
    let token = sign_jwt(sk, payload.as_bytes());

    Ok(token)
}

fn sign_jwt(sk: &SigningKey, payload: &[u8]) -> String {
    let header_len = 20;
    let payload_len = Base64UrlUnpadded::encoded_len(payload);
    let signature_len = Base64UrlUnpadded::encoded_len(&[0; 64]);
    let total_len = header_len + payload_len + signature_len + 2;

    let mut jwt = String::with_capacity(total_len);
    let cap = jwt.capacity();

    // we only need an empty header with the alg specified.
    // base64url(r#"{"alg":"EdDSA"}"#) == "eyJhbGciOiJFZERTQSJ9"
    jwt.push_str("eyJhbGciOiJFZERTQSJ9.");

    // encode the jwt payload in-place
    base64::encode_config_buf(payload, base64::URL_SAFE_NO_PAD, &mut jwt);

    // create the signature from the encoded header || payload
    let sig: Signature = sk.sign(jwt.as_bytes());

    jwt.push('.');

    // encode the jwt signature in-place
    base64::encode_config_buf(sig.to_bytes(), base64::URL_SAFE_NO_PAD, &mut jwt);

    debug_assert_eq!(
        jwt.len(),
        total_len,
        "the jwt len should match our expected len"
    );
    debug_assert_eq!(jwt.capacity(), cap, "the jwt capacity should not change");

    jwt
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use ed25519_dalek::SigningKey;
    use typed_json::json;

    use super::resign_jwt;

    #[test]
    fn jwt_token_snapshot() {
        let key = SigningKey::from_bytes(&[1; 32]);
        let data =
            json!({"foo":"bar","jti":"foo\nbar","nested":{"jti":"tricky nesting"}}).to_string();

        let jwt = resign_jwt(&key, data.as_bytes(), 2).unwrap();

        // To validate the JWT, copy the JWT string and paste it into https://jwt.io/.
        // In the public-key box, paste the following jwk public key
        // `{"kty":"OKP","crv":"Ed25519","x":"iojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1w"}`
        // Note - jwt.io doesn't support EdDSA :(
        // https://github.com/jsonwebtoken/jsonwebtoken.github.io/issues/509

        // let jwk = jose_jwk::Key::Okp(jose_jwk::Okp {
        //     crv: jose_jwk::OkpCurves::Ed25519,
        //     x: jose_jwk::jose_b64::serde::Bytes::from(key.verifying_key().to_bytes().to_vec()),
        //     d: None,
        // });
        // println!("{}", serde_json::to_string(&jwk).unwrap());

        assert_eq!(jwt, "eyJhbGciOiJFZERTQSJ9.eyJmb28iOiJiYXIiLCJqdGkiOjIsIm5lc3RlZCI6eyJqdGkiOiJ0cmlja3kgbmVzdGluZyJ9fQ.Cvyc2By33KI0f0obystwdy8PN111L3Sc9_Mr2CU3XshtSqSdxuRxNEZGbb_RvyJf2IzheC_s7aBZ-jLeQ9N0Bg");
    }
}
