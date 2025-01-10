use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::{ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo, RedisResult};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::elasticache::CredentialsProvider;

enum Credentials {
    Static(ConnectionInfo),
    Dynamic(Arc<CredentialsProvider>, redis::ConnectionAddr),
}

impl Clone for Credentials {
    fn clone(&self) -> Self {
        match self {
            Credentials::Static(info) => Credentials::Static(info.clone()),
            Credentials::Dynamic(provider, addr) => {
                Credentials::Dynamic(Arc::clone(provider), addr.clone())
            }
        }
    }
}

/// A wrapper around `redis::MultiplexedConnection` that automatically refreshes the token.
/// Provides PubSub connection without credentials refresh.
pub struct ConnectionWithCredentialsProvider {
    credentials: Credentials,
    con: Option<MultiplexedConnection>,
    refresh_token_task: Option<JoinHandle<()>>,
    mutex: tokio::sync::Mutex<()>,
}

impl Clone for ConnectionWithCredentialsProvider {
    fn clone(&self) -> Self {
        Self {
            credentials: self.credentials.clone(),
            con: None,
            refresh_token_task: None,
            mutex: tokio::sync::Mutex::new(()),
        }
    }
}

impl ConnectionWithCredentialsProvider {
    pub fn new_with_credentials_provider(
        host: String,
        port: u16,
        credentials_provider: Arc<CredentialsProvider>,
    ) -> Self {
        Self {
            credentials: Credentials::Dynamic(
                credentials_provider,
                redis::ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                    tls_params: None,
                },
            ),
            con: None,
            refresh_token_task: None,
            mutex: tokio::sync::Mutex::new(()),
        }
    }

    pub fn new_with_static_credentials<T: IntoConnectionInfo>(params: T) -> Self {
        Self {
            credentials: Credentials::Static(
                params
                    .into_connection_info()
                    .expect("static configured redis credentials should be a valid format"),
            ),
            con: None,
            refresh_token_task: None,
            mutex: tokio::sync::Mutex::new(()),
        }
    }

    async fn ping(con: &mut MultiplexedConnection) -> RedisResult<()> {
        redis::cmd("PING").query_async(con).await
    }

    pub(crate) async fn connect(&mut self) -> anyhow::Result<()> {
        let _guard = self.mutex.lock().await;
        if let Some(con) = self.con.as_mut() {
            match Self::ping(con).await {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!("Error during PING: {e:?}");
                }
            }
        } else {
            info!("Connection is not established");
        }
        info!("Establishing a new connection...");
        self.con = None;
        if let Some(f) = self.refresh_token_task.take() {
            f.abort();
        }
        let mut con = self
            .get_client()
            .await?
            .get_multiplexed_tokio_connection()
            .await?;
        if let Credentials::Dynamic(credentials_provider, _) = &self.credentials {
            let credentials_provider = credentials_provider.clone();
            let con2 = con.clone();
            let f = tokio::spawn(async move {
                Self::keep_connection(con2, credentials_provider)
                    .await
                    .inspect_err(|e| debug!("keep_connection failed: {e}"))
                    .ok();
            });
            self.refresh_token_task = Some(f);
        }
        match Self::ping(&mut con).await {
            Ok(()) => {
                info!("Connection succesfully established");
            }
            Err(e) => {
                warn!("Connection is broken. Error during PING: {e:?}");
            }
        }
        self.con = Some(con);
        Ok(())
    }

    async fn get_connection_info(&self) -> anyhow::Result<ConnectionInfo> {
        match &self.credentials {
            Credentials::Static(info) => Ok(info.clone()),
            Credentials::Dynamic(provider, addr) => {
                let (username, password) = provider.provide_credentials().await?;
                Ok(ConnectionInfo {
                    addr: addr.clone(),
                    redis: RedisConnectionInfo {
                        db: 0,
                        username: Some(username),
                        password: Some(password.clone()),
                    },
                })
            }
        }
    }

    async fn get_client(&self) -> anyhow::Result<redis::Client> {
        let client = redis::Client::open(self.get_connection_info().await?)?;
        Ok(client)
    }

    // PubSub does not support credentials refresh.
    // Requires manual reconnection every 12h.
    pub(crate) async fn get_async_pubsub(&self) -> anyhow::Result<redis::aio::PubSub> {
        Ok(self.get_client().await?.get_async_pubsub().await?)
    }

    // The connection lives for 12h.
    // It can be prolonged with sending `AUTH` commands with the refreshed token.
    // https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/auth-iam.html#auth-iam-limits
    async fn keep_connection(
        mut con: MultiplexedConnection,
        credentials_provider: Arc<CredentialsProvider>,
    ) -> anyhow::Result<()> {
        loop {
            // The connection lives for 12h, for the sanity check we refresh it every hour.
            tokio::time::sleep(Duration::from_secs(60 * 60)).await;
            match Self::refresh_token(&mut con, credentials_provider.clone()).await {
                Ok(()) => {
                    info!("Token refreshed");
                }
                Err(e) => {
                    error!("Error during token refresh: {e:?}");
                }
            }
        }
    }
    async fn refresh_token(
        con: &mut MultiplexedConnection,
        credentials_provider: Arc<CredentialsProvider>,
    ) -> anyhow::Result<()> {
        let (user, password) = credentials_provider.provide_credentials().await?;
        let _: () = redis::cmd("AUTH")
            .arg(user)
            .arg(password)
            .query_async(con)
            .await?;
        Ok(())
    }
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub(crate) async fn send_packed_command(
        &mut self,
        cmd: &redis::Cmd,
    ) -> RedisResult<redis::Value> {
        // Clone connection to avoid having to lock the ArcSwap in write mode
        let con = self.con.as_mut().ok_or(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Connection not established",
        )))?;
        con.send_packed_command(cmd).await
    }

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    pub(crate) async fn send_packed_commands(
        &mut self,
        cmd: &redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<redis::Value>> {
        // Clone shared connection future to avoid having to lock the ArcSwap in write mode
        let con = self.con.as_mut().ok_or(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Connection not established",
        )))?;
        con.send_packed_commands(cmd, offset, count).await
    }
}

impl ConnectionLike for ConnectionWithCredentialsProvider {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        (async move { self.send_packed_command(cmd).await }).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        (async move { self.send_packed_commands(cmd, offset, count).await }).boxed()
    }

    fn get_db(&self) -> i64 {
        0
    }
}
