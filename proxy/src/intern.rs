use std::hash::BuildHasherDefault;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::Index;
use std::sync::OnceLock;

use lasso::{Capacity, MemoryLimits, Spur, ThreadedRodeo};
use rustc_hash::FxHasher;

use crate::types::{BranchId, EndpointId, ProjectId, RoleName};

pub trait InternId: Sized + 'static {
    fn get_interner() -> &'static StringInterner<Self>;
}

pub struct StringInterner<Id> {
    inner: ThreadedRodeo<Spur, BuildHasherDefault<FxHasher>>,
    _id: PhantomData<Id>,
}

#[derive(PartialEq, Debug, Clone, Copy, Eq, Hash)]
pub struct InternedString<Id> {
    inner: Spur,
    _id: PhantomData<Id>,
}

impl<Id: InternId> std::fmt::Display for InternedString<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl<Id: InternId> InternedString<Id> {
    pub(crate) fn as_str(&self) -> &'static str {
        Id::get_interner().inner.resolve(&self.inner)
    }
    pub(crate) fn get(s: &str) -> Option<Self> {
        Id::get_interner().get(s)
    }
}

impl<Id: InternId> AsRef<str> for InternedString<Id> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<Id: InternId> std::ops::Deref for InternedString<Id> {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<'de, Id: InternId> serde::de::Deserialize<'de> for InternedString<Id> {
    fn deserialize<D: serde::de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor<Id>(PhantomData<Id>);
        impl<Id: InternId> serde::de::Visitor<'_> for Visitor<Id> {
            type Value = InternedString<Id>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Id::get_interner().get_or_intern(v))
            }
        }
        d.deserialize_str(Visitor::<Id>(PhantomData))
    }
}

impl<Id: InternId> serde::Serialize for InternedString<Id> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(s)
    }
}

impl<Id: InternId> StringInterner<Id> {
    pub(crate) fn new() -> Self {
        StringInterner {
            inner: ThreadedRodeo::with_capacity_memory_limits_and_hasher(
                Capacity::new(2500, NonZeroUsize::new(1 << 16).expect("value is nonzero")),
                // unbounded
                MemoryLimits::for_memory_usage(usize::MAX),
                BuildHasherDefault::<FxHasher>::default(),
            ),
            _id: PhantomData,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(test)]
    fn current_memory_usage(&self) -> usize {
        self.inner.current_memory_usage()
    }

    pub(crate) fn get_or_intern(&self, s: &str) -> InternedString<Id> {
        InternedString {
            inner: self.inner.get_or_intern(s),
            _id: PhantomData,
        }
    }

    pub(crate) fn get(&self, s: &str) -> Option<InternedString<Id>> {
        Some(InternedString {
            inner: self.inner.get(s)?,
            _id: PhantomData,
        })
    }
}

impl<Id: InternId> Index<InternedString<Id>> for StringInterner<Id> {
    type Output = str;

    fn index(&self, index: InternedString<Id>) -> &Self::Output {
        self.inner.resolve(&index.inner)
    }
}

impl<Id: InternId> Default for StringInterner<Id> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoleNameTag;
impl InternId for RoleNameTag {
    fn get_interner() -> &'static StringInterner<Self> {
        static ROLE_NAMES: OnceLock<StringInterner<RoleNameTag>> = OnceLock::new();
        ROLE_NAMES.get_or_init(Default::default)
    }
}
pub type RoleNameInt = InternedString<RoleNameTag>;
impl From<&RoleName> for RoleNameInt {
    fn from(value: &RoleName) -> Self {
        RoleNameTag::get_interner().get_or_intern(value)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointIdTag;
impl InternId for EndpointIdTag {
    fn get_interner() -> &'static StringInterner<Self> {
        static ROLE_NAMES: OnceLock<StringInterner<EndpointIdTag>> = OnceLock::new();
        ROLE_NAMES.get_or_init(Default::default)
    }
}
pub type EndpointIdInt = InternedString<EndpointIdTag>;
impl From<&EndpointId> for EndpointIdInt {
    fn from(value: &EndpointId) -> Self {
        EndpointIdTag::get_interner().get_or_intern(value)
    }
}
impl From<EndpointId> for EndpointIdInt {
    fn from(value: EndpointId) -> Self {
        EndpointIdTag::get_interner().get_or_intern(&value)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct BranchIdTag;
impl InternId for BranchIdTag {
    fn get_interner() -> &'static StringInterner<Self> {
        static ROLE_NAMES: OnceLock<StringInterner<BranchIdTag>> = OnceLock::new();
        ROLE_NAMES.get_or_init(Default::default)
    }
}
pub type BranchIdInt = InternedString<BranchIdTag>;
impl From<&BranchId> for BranchIdInt {
    fn from(value: &BranchId) -> Self {
        BranchIdTag::get_interner().get_or_intern(value)
    }
}
impl From<BranchId> for BranchIdInt {
    fn from(value: BranchId) -> Self {
        BranchIdTag::get_interner().get_or_intern(&value)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectIdTag;
impl InternId for ProjectIdTag {
    fn get_interner() -> &'static StringInterner<Self> {
        static ROLE_NAMES: OnceLock<StringInterner<ProjectIdTag>> = OnceLock::new();
        ROLE_NAMES.get_or_init(Default::default)
    }
}
pub type ProjectIdInt = InternedString<ProjectIdTag>;
impl From<&ProjectId> for ProjectIdInt {
    fn from(value: &ProjectId) -> Self {
        ProjectIdTag::get_interner().get_or_intern(value)
    }
}
impl From<ProjectId> for ProjectIdInt {
    fn from(value: ProjectId) -> Self {
        ProjectIdTag::get_interner().get_or_intern(&value)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::OnceLock;

    use super::InternId;
    use crate::intern::StringInterner;

    struct MyId;
    impl InternId for MyId {
        fn get_interner() -> &'static StringInterner<Self> {
            pub(crate) static ROLE_NAMES: OnceLock<StringInterner<MyId>> = OnceLock::new();
            ROLE_NAMES.get_or_init(Default::default)
        }
    }

    #[test]
    fn push_many_strings() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::Zipf;

        let endpoint_dist = Zipf::new(500000, 0.8).unwrap();
        let endpoints = StdRng::seed_from_u64(272488357).sample_iter(endpoint_dist);

        let interner = MyId::get_interner();

        const N: usize = 100_000;
        let mut verify = Vec::with_capacity(N);
        for endpoint in endpoints.take(N) {
            let endpoint = format!("ep-string-interning-{endpoint}");
            let key = interner.get_or_intern(&endpoint);
            verify.push((endpoint, key));
        }

        for (s, key) in verify {
            assert_eq!(interner[key], s);
        }

        // 2031616/59861 = 34 bytes per string
        assert_eq!(interner.len(), 59_861);
        // will have other overhead for the internal hashmaps that are not accounted for.
        assert_eq!(interner.current_memory_usage(), 2_031_616);
    }
}
