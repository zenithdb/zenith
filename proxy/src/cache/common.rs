use std::ops::{Deref, DerefMut};

/// A generic trait which exposes types of cache's key and value,
/// as well as the notion of cache entry invalidation.
/// This is useful for [`Cached`].
pub trait Cache {
    /// Entry's key.
    type Key;

    /// Entry's value.
    type Value;

    /// Used for entry invalidation.
    type LookupInfo<Key>;

    /// Invalidate an entry using a lookup info.
    /// We don't have an empty default impl because it's error-prone.
    fn invalidate(&self, _: &Self::LookupInfo<Self::Key>);
}

impl<C: Cache> Cache for &C {
    type Key = C::Key;
    type Value = C::Value;
    type LookupInfo<Key> = C::LookupInfo<Key>;

    fn invalidate(&self, info: &Self::LookupInfo<Self::Key>) {
        C::invalidate(self, info)
    }
}

/// Wrapper for convenient entry invalidation.
pub struct Cached<C: Cache, V = <C as Cache>::Value> {
    /// Cache + lookup info.
    pub token: Option<(C, C::LookupInfo<C::Key>)>,

    /// The value itself.
    pub value: V,
}

impl<C: Cache, V> Cached<C, V> {
    /// Place any entry into this wrapper; invalidation will be a no-op.
    pub fn new_uncached(value: V) -> Self {
        Self { token: None, value }
    }

    /// Place any entry into this wrapper; invalidation will be a no-op.
    pub fn map<U>(self, f: impl FnOnce(V) -> U) -> Cached<C, U> {
        let token = self.token;
        Cached {
            token,
            value: f(self.value),
        }
    }

    pub fn take_value(self) -> (Cached<C, ()>, V) {
        (
            Cached {
                token: self.token,
                value: (),
            },
            self.value,
        )
    }

    /// Drop this entry from a cache if it's still there.
    pub fn invalidate(self) -> V {
        if let Some((cache, info)) = &self.token {
            cache.invalidate(info);
        }
        self.value
    }

    /// Tell if this entry is actually cached.
    pub fn cached(&self) -> bool {
        self.token.is_some()
    }
}

impl<C: Cache, V> Deref for Cached<C, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<C: Cache, V> DerefMut for Cached<C, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
