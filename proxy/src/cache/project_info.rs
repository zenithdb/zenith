use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use rand::{thread_rng, Rng};
use smol_str::SmolStr;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, info};

use super::{Cache, Cached};
use crate::auth::IpPattern;
use crate::config::ProjectInfoCacheOptions;
use crate::control_plane::AuthSecret;
use crate::intern::{EndpointIdInt, ProjectIdInt, RoleNameInt};
use crate::types::{EndpointId, RoleName};

#[async_trait]
pub(crate) trait ProjectInfoCache {
    fn invalidate_allowed_ips_for_project(&self, project_id: ProjectIdInt);
    fn invalidate_role_secret_for_project(&self, project_id: ProjectIdInt, role_name: RoleNameInt);
    async fn decrement_active_listeners(&self);
    async fn increment_active_listeners(&self);
}

struct Entry<T> {
    created_at: Instant,
    value: T,
}

impl<T> Entry<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            created_at: Instant::now(),
            value,
        }
    }
}

impl<T> From<T> for Entry<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

#[derive(Default)]
struct EndpointInfo {
    secret: std::collections::HashMap<RoleNameInt, Entry<Option<AuthSecret>>>,
    allowed_ips: Option<Entry<Arc<Vec<IpPattern>>>>,
}

impl EndpointInfo {
    fn check_ignore_cache(ignore_cache_since: Option<Instant>, created_at: Instant) -> bool {
        match ignore_cache_since {
            None => false,
            Some(t) => t < created_at,
        }
    }
    pub(crate) fn get_role_secret(
        &self,
        role_name: RoleNameInt,
        valid_since: Instant,
        ignore_cache_since: Option<Instant>,
    ) -> Option<(Option<AuthSecret>, bool)> {
        if let Some(secret) = self.secret.get(&role_name) {
            if valid_since < secret.created_at {
                return Some((
                    secret.value.clone(),
                    Self::check_ignore_cache(ignore_cache_since, secret.created_at),
                ));
            }
        }
        None
    }

    pub(crate) fn get_allowed_ips(
        &self,
        valid_since: Instant,
        ignore_cache_since: Option<Instant>,
    ) -> Option<(Arc<Vec<IpPattern>>, bool)> {
        if let Some(allowed_ips) = &self.allowed_ips {
            if valid_since < allowed_ips.created_at {
                return Some((
                    allowed_ips.value.clone(),
                    Self::check_ignore_cache(ignore_cache_since, allowed_ips.created_at),
                ));
            }
        }
        None
    }
    pub(crate) fn invalidate_allowed_ips(&mut self) {
        self.allowed_ips = None;
    }
    pub(crate) fn invalidate_role_secret(&mut self, role_name: RoleNameInt) {
        self.secret.remove(&role_name);
    }
}

/// Cache for project info.
/// This is used to cache auth data for endpoints.
/// Invalidation is done by console notifications or by TTL (if console notifications are disabled).
///
/// We also store endpoint-to-project mapping in the cache, to be able to access per-endpoint data.
/// One may ask, why the data is stored per project, when on the user request there is only data about the endpoint available?
/// On the cplane side updates are done per project (or per branch), so it's easier to invalidate the whole project cache.
pub struct ProjectInfoCacheImpl {
    cache: DashMap<EndpointIdInt, EndpointInfo>,

    project2ep: DashMap<ProjectIdInt, HashSet<EndpointIdInt>>,
    config: ProjectInfoCacheOptions,

    start_time: Instant,
    ttl_disabled_since_us: AtomicU64,
    active_listeners_lock: Mutex<usize>,
}

#[async_trait]
impl ProjectInfoCache for ProjectInfoCacheImpl {
    fn invalidate_allowed_ips_for_project(&self, project_id: ProjectIdInt) {
        info!("invalidating allowed ips for project `{}`", project_id);
        let endpoints = self
            .project2ep
            .get(&project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_allowed_ips();
            }
        }
    }
    fn invalidate_role_secret_for_project(&self, project_id: ProjectIdInt, role_name: RoleNameInt) {
        info!(
            "invalidating role secret for project_id `{}` and role_name `{}`",
            project_id, role_name,
        );
        let endpoints = self
            .project2ep
            .get(&project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_role_secret(role_name);
            }
        }
    }
    async fn decrement_active_listeners(&self) {
        let mut listeners_guard = self.active_listeners_lock.lock().await;
        if *listeners_guard == 0 {
            tracing::error!("active_listeners count is already 0, something is broken");
            return;
        }
        *listeners_guard -= 1;
        if *listeners_guard == 0 {
            self.ttl_disabled_since_us
                .store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn increment_active_listeners(&self) {
        let mut listeners_guard = self.active_listeners_lock.lock().await;
        *listeners_guard += 1;
        if *listeners_guard == 1 {
            let new_ttl = (self.start_time.elapsed() + self.config.ttl).as_micros() as u64;
            self.ttl_disabled_since_us
                .store(new_ttl, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl ProjectInfoCacheImpl {
    pub(crate) fn new(config: ProjectInfoCacheOptions) -> Self {
        Self {
            cache: DashMap::new(),
            project2ep: DashMap::new(),
            config,
            ttl_disabled_since_us: AtomicU64::new(u64::MAX),
            start_time: Instant::now(),
            active_listeners_lock: Mutex::new(0),
        }
    }

    pub(crate) fn get_role_secret(
        &self,
        endpoint_id: &EndpointId,
        role_name: &RoleName,
    ) -> Option<Cached<&Self, Option<AuthSecret>>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;
        let role_name = RoleNameInt::get(role_name)?;
        let (valid_since, ignore_cache_since) = self.get_cache_times();
        let endpoint_info = self.cache.get(&endpoint_id)?;
        let (value, ignore_cache) =
            endpoint_info.get_role_secret(role_name, valid_since, ignore_cache_since)?;
        if !ignore_cache {
            let cached = Cached {
                token: Some((
                    self,
                    CachedLookupInfo::new_role_secret(endpoint_id, role_name),
                )),
                value,
            };
            return Some(cached);
        }
        Some(Cached::new_uncached(value))
    }
    pub(crate) fn get_allowed_ips(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<Cached<&Self, Arc<Vec<IpPattern>>>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;
        let (valid_since, ignore_cache_since) = self.get_cache_times();
        let endpoint_info = self.cache.get(&endpoint_id)?;
        let value = endpoint_info.get_allowed_ips(valid_since, ignore_cache_since);
        let (value, ignore_cache) = value?;
        if !ignore_cache {
            let cached = Cached {
                token: Some((self, CachedLookupInfo::new_allowed_ips(endpoint_id))),
                value,
            };
            return Some(cached);
        }
        Some(Cached::new_uncached(value))
    }
    pub(crate) fn insert_role_secret(
        &self,
        project_id: ProjectIdInt,
        endpoint_id: EndpointIdInt,
        role_name: RoleNameInt,
        secret: Option<AuthSecret>,
    ) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }
        self.insert_project2endpoint(project_id, endpoint_id);
        let mut entry = self.cache.entry(endpoint_id).or_default();
        if entry.secret.len() < self.config.max_roles {
            entry.secret.insert(role_name, secret.into());
        }
    }
    pub(crate) fn insert_allowed_ips(
        &self,
        project_id: ProjectIdInt,
        endpoint_id: EndpointIdInt,
        allowed_ips: Arc<Vec<IpPattern>>,
    ) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }
        self.insert_project2endpoint(project_id, endpoint_id);
        self.cache.entry(endpoint_id).or_default().allowed_ips = Some(allowed_ips.into());
    }
    fn insert_project2endpoint(&self, project_id: ProjectIdInt, endpoint_id: EndpointIdInt) {
        if let Some(mut endpoints) = self.project2ep.get_mut(&project_id) {
            endpoints.insert(endpoint_id);
        } else {
            self.project2ep
                .insert(project_id, HashSet::from([endpoint_id]));
        }
    }
    fn get_cache_times(&self) -> (Instant, Option<Instant>) {
        let mut valid_since = Instant::now() - self.config.ttl;
        // Only ignore cache if ttl is disabled.
        let ttl_disabled_since_us = self
            .ttl_disabled_since_us
            .load(std::sync::atomic::Ordering::Relaxed);
        let ignore_cache_since = if ttl_disabled_since_us == u64::MAX {
            None
        } else {
            let ignore_cache_since = self.start_time + Duration::from_micros(ttl_disabled_since_us);
            // We are fine if entry is not older than ttl or was added before we are getting notifications.
            valid_since = valid_since.min(ignore_cache_since);
            Some(ignore_cache_since)
        };
        (valid_since, ignore_cache_since)
    }

    pub async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let mut interval =
            tokio::time::interval(self.config.gc_interval / (self.cache.shards().len()) as u32);
        loop {
            interval.tick().await;
            if self.cache.len() < self.config.size {
                // If there are not too many entries, wait until the next gc cycle.
                continue;
            }
            self.gc();
        }
    }

    fn gc(&self) {
        let shard = thread_rng().gen_range(0..self.project2ep.shards().len());
        debug!(shard, "project_info_cache: performing epoch reclamation");

        // acquire a random shard lock
        let mut removed = 0;
        let shard = self.project2ep.shards()[shard].write();
        for (_, endpoints) in shard.iter() {
            for endpoint in endpoints.get() {
                self.cache.remove(endpoint);
                removed += 1;
            }
        }
        // We can drop this shard only after making sure that all endpoints are removed.
        drop(shard);
        info!("project_info_cache: removed {removed} endpoints");
    }
}

/// Lookup info for project info cache.
/// This is used to invalidate cache entries.
pub(crate) struct CachedLookupInfo {
    /// Search by this key.
    endpoint_id: EndpointIdInt,
    lookup_type: LookupType,
}

impl CachedLookupInfo {
    pub(self) fn new_role_secret(endpoint_id: EndpointIdInt, role_name: RoleNameInt) -> Self {
        Self {
            endpoint_id,
            lookup_type: LookupType::RoleSecret(role_name),
        }
    }
    pub(self) fn new_allowed_ips(endpoint_id: EndpointIdInt) -> Self {
        Self {
            endpoint_id,
            lookup_type: LookupType::AllowedIps,
        }
    }
}

enum LookupType {
    RoleSecret(RoleNameInt),
    AllowedIps,
}

impl Cache for ProjectInfoCacheImpl {
    type Key = SmolStr;
    // Value is not really used here, but we need to specify it.
    type Value = SmolStr;

    type LookupInfo<Key> = CachedLookupInfo;

    fn invalidate(&self, key: &Self::LookupInfo<SmolStr>) {
        match &key.lookup_type {
            LookupType::RoleSecret(role_name) => {
                if let Some(mut endpoint_info) = self.cache.get_mut(&key.endpoint_id) {
                    endpoint_info.invalidate_role_secret(*role_name);
                }
            }
            LookupType::AllowedIps => {
                if let Some(mut endpoint_info) = self.cache.get_mut(&key.endpoint_id) {
                    endpoint_info.invalidate_allowed_ips();
                }
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::scram::ServerSecret;
    use crate::types::ProjectId;

    #[tokio::test]
    async fn test_project_info_cache_settings() {
        tokio::time::pause();
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
        });
        let project_id: ProjectId = "project".into();
        let endpoint_id: EndpointId = "endpoint".into();
        let user1: RoleName = "user1".into();
        let user2: RoleName = "user2".into();
        let secret1 = Some(AuthSecret::Scram(ServerSecret::mock([1; 32])));
        let secret2 = None;
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user1).into(),
            secret1.clone(),
        );
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user2).into(),
            secret2.clone(),
        );
        cache.insert_allowed_ips(
            (&project_id).into(),
            (&endpoint_id).into(),
            allowed_ips.clone(),
        );

        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, secret1);
        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, secret2);

        // Shouldn't add more than 2 roles.
        let user3: RoleName = "user3".into();
        let secret3 = Some(AuthSecret::Scram(ServerSecret::mock([3; 32])));
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user3).into(),
            secret3.clone(),
        );
        assert!(cache.get_role_secret(&endpoint_id, &user3).is_none());

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, allowed_ips);

        tokio::time::advance(Duration::from_secs(2)).await;
        let cached = cache.get_role_secret(&endpoint_id, &user1);
        assert!(cached.is_none());
        let cached = cache.get_role_secret(&endpoint_id, &user2);
        assert!(cached.is_none());
        let cached = cache.get_allowed_ips(&endpoint_id);
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_project_info_cache_invalidations() {
        tokio::time::pause();
        let cache = Arc::new(ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
        }));
        cache.clone().increment_active_listeners().await;
        tokio::time::advance(Duration::from_secs(2)).await;

        let project_id: ProjectId = "project".into();
        let endpoint_id: EndpointId = "endpoint".into();
        let user1: RoleName = "user1".into();
        let user2: RoleName = "user2".into();
        let secret1 = Some(AuthSecret::Scram(ServerSecret::mock([1; 32])));
        let secret2 = Some(AuthSecret::Scram(ServerSecret::mock([2; 32])));
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user1).into(),
            secret1.clone(),
        );
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user2).into(),
            secret2.clone(),
        );
        cache.insert_allowed_ips(
            (&project_id).into(),
            (&endpoint_id).into(),
            allowed_ips.clone(),
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        // Nothing should be invalidated.

        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        // TTL is disabled, so it should be impossible to invalidate this value.
        assert!(!cached.cached());
        assert_eq!(cached.value, secret1);

        cached.invalidate(); // Shouldn't do anything.
        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert_eq!(cached.value, secret1);

        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, secret2);

        // The only way to invalidate this value is to invalidate via the api.
        cache.invalidate_role_secret_for_project((&project_id).into(), (&user2).into());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, allowed_ips);
    }

    #[tokio::test]
    async fn test_increment_active_listeners_invalidate_added_before() {
        tokio::time::pause();
        let cache = Arc::new(ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
        }));

        let project_id: ProjectId = "project".into();
        let endpoint_id: EndpointId = "endpoint".into();
        let user1: RoleName = "user1".into();
        let user2: RoleName = "user2".into();
        let secret1 = Some(AuthSecret::Scram(ServerSecret::mock([1; 32])));
        let secret2 = Some(AuthSecret::Scram(ServerSecret::mock([2; 32])));
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user1).into(),
            secret1.clone(),
        );
        cache.clone().increment_active_listeners().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        cache.insert_role_secret(
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user2).into(),
            secret2.clone(),
        );

        // Added before ttl was disabled + ttl should be still cached.
        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert!(cached.cached());
        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(cached.cached());

        tokio::time::advance(Duration::from_secs(1)).await;
        // Added before ttl was disabled + ttl should expire.
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());

        // Added after ttl was disabled + ttl should not be cached.
        cache.insert_allowed_ips(
            (&project_id).into(),
            (&endpoint_id).into(),
            allowed_ips.clone(),
        );
        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());

        tokio::time::advance(Duration::from_secs(1)).await;
        // Added before ttl was disabled + ttl still should expire.
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());
        // Shouldn't be invalidated.

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, allowed_ips);
    }
}
