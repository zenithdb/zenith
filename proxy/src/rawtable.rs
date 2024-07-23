//! Dashmap moved to using RawTable for the shards.
//! Some of the APIs we used before are unsafe to access, but we can copy the implementations from the safe
//! HashMap wrappers for our needs.

// Safety info: All implementations here are taken directly from hashbrown HashMap impl.

use std::marker::PhantomData;

use hashbrown::raw;

// taken from https://docs.rs/hashbrown/0.14.5/src/hashbrown/map.rs.html#919-932
pub fn retain<K, V, F>(table: &mut raw::RawTable<(K, V)>, mut f: F)
where
    F: FnMut(&K, &mut V) -> bool,
{
    // SAFETY: Here we only use `iter` as a temporary, preventing use-after-free
    unsafe {
        for item in table.iter() {
            let &mut (ref key, ref mut value) = item.as_mut();
            if !f(key, value) {
                table.erase(item);
            }
        }
    }
}

// taken from https://docs.rs/hashbrown/0.14.5/src/hashbrown/map.rs.html#756-764
pub fn iter<K, V>(table: &raw::RawTable<(K, V)>) -> impl Iterator<Item = (&K, &V)> + '_ {
    pub struct Iter<'a, K, V> {
        inner: raw::RawIter<(K, V)>,
        marker: PhantomData<(&'a K, &'a V)>,
    }

    impl<'a, K, V> Iterator for Iter<'a, K, V> {
        type Item = (&'a K, &'a V);

        #[cfg_attr(feature = "inline-more", inline)]
        fn next(&mut self) -> Option<(&'a K, &'a V)> {
            let x = self.inner.next()?;
            // SAFETY: the borrows do not outlive the rawtable
            unsafe {
                let r = x.as_ref();
                Some((&r.0, &r.1))
            }
        }
        #[cfg_attr(feature = "inline-more", inline)]
        fn size_hint(&self) -> (usize, Option<usize>) {
            self.inner.size_hint()
        }
    }

    // SAFETY:
    // > It is up to the caller to ensure that the RawTable outlives the RawIter
    // Here we tie the lifetime of self to the iter.
    unsafe {
        Iter {
            inner: table.iter(),
            marker: PhantomData,
        }
    }
}
