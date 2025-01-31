use core::net::IpAddr;
use std::sync::Arc;

use pq_proto::CancelKeyData;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::rate_limiter::{GlobalRateLimiter, RateBucketInfo};

pub trait CancellationPublisherMut: Send + Sync + 'static {
    #[allow(async_fn_in_trait)]
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()>;
}

pub trait CancellationPublisher: Send + Sync + 'static {
    #[allow(async_fn_in_trait)]
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()>;
}

impl CancellationPublisher for () {
    async fn try_publish(
        &self,
        _cancel_key_data: CancelKeyData,
        _session_id: Uuid,
        _peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<P: CancellationPublisher> CancellationPublisherMut for P {
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        <P as CancellationPublisher>::try_publish(self, cancel_key_data, session_id, peer_addr)
            .await
    }
}

impl<P: CancellationPublisher> CancellationPublisher for Option<P> {
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        if let Some(p) = self {
            p.try_publish(cancel_key_data, session_id, peer_addr).await
        } else {
            Ok(())
        }
    }
}

impl<P: CancellationPublisherMut> CancellationPublisher for Arc<Mutex<P>> {
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        self.lock()
            .await
            .try_publish(cancel_key_data, session_id, peer_addr)
            .await
    }
}

pub struct RedisPublisherClient {
    #[allow(dead_code)]
    client: ConnectionWithCredentialsProvider,
    _region_id: String,
    _limiter: GlobalRateLimiter,
}

impl RedisPublisherClient {
    pub fn new(
        client: ConnectionWithCredentialsProvider,
        region_id: String,
        info: &'static [RateBucketInfo],
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            _region_id: region_id,
            _limiter: GlobalRateLimiter::new(info.into()),
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.connect().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e);
            }
        }
        Ok(())
    }
}
