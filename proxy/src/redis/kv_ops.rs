use redis::{AsyncCommands, ToRedisArgs};

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;

use crate::rate_limiter::{GlobalRateLimiter, RateBucketInfo};

pub struct RedisKVClient {
    client: ConnectionWithCredentialsProvider,
    limiter: GlobalRateLimiter,
}

impl RedisKVClient {
    pub fn new(client: ConnectionWithCredentialsProvider, info: &'static [RateBucketInfo]) -> Self {
        Self {
            client,
            limiter: GlobalRateLimiter::new(info.into()),
        }
    }

    pub async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.connect().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e);
            }
        }
        Ok(())
    }

    pub(crate) async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hset(&key, &field, &value).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hset(key, field, value)
            .await
            .map_err(anyhow::Error::new)
    }

    #[allow(dead_code)]
    pub(crate) async fn hset_multiple<K, V>(
        &mut self,
        key: &str,
        items: &[(K, V)],
    ) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset_multiple");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hset_multiple(key, items).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hset_multiple(key, items)
            .await
            .map_err(anyhow::Error::new)
    }

    #[allow(dead_code)]
    pub(crate) async fn expire<K>(&mut self, key: K, seconds: i64) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping expire");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.expire(&key, seconds).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .expire(key, seconds)
            .await
            .map_err(anyhow::Error::new)
    }

    #[allow(dead_code)]
    pub(crate) async fn hget<K, F, V>(&mut self, key: K, field: F) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hget");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hget(&key, &field).await {
            Ok(value) => return Ok(value),
            Err(e) => {
                tracing::error!("failed to get a value: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hget(key, field)
            .await
            .map_err(anyhow::Error::new)
    }

    pub(crate) async fn hget_all<K, V>(&mut self, key: K) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hgetall");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hgetall(&key).await {
            Ok(value) => return Ok(value),
            Err(e) => {
                tracing::error!("failed to get a value: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client.hgetall(key).await.map_err(anyhow::Error::new)
    }

    pub(crate) async fn hdel<K, F>(&mut self, key: K, field: F) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hdel");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hdel(&key, &field).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to delete a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hdel(key, field)
            .await
            .map_err(anyhow::Error::new)
    }
}
