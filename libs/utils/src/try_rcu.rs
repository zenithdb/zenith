//! Try RCU extension lifted from <https://github.com/vorner/arc-swap/issues/94#issuecomment-1987154023>

pub trait ArcSwapExt<T> {
    /// [`ArcSwap::rcu`](arc_swap::ArcSwap::rcu), but with Result that short-circuits on error.
    fn try_rcu<R, F, E>(&self, f: F) -> Result<T, E>
    where
        F: FnMut(&T) -> Result<R, E>,
        R: Into<T>;
}

impl<T, S> ArcSwapExt<T> for arc_swap::ArcSwapAny<T, S>
where
    T: arc_swap::RefCnt,
    S: arc_swap::strategy::CaS<T>,
{
    fn try_rcu<R, F, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut(&T) -> Result<R, E>,
        R: Into<T>,
    {
        fn ptr_eq<Base, A, B>(a: A, b: B) -> bool
        where
            A: arc_swap::AsRaw<Base>,
            B: arc_swap::AsRaw<Base>,
        {
            let a = a.as_raw();
            let b = b.as_raw();
            std::ptr::eq(a, b)
        }

        let mut cur = self.load();
        loop {
            let new = f(&cur)?.into();
            let prev = self.compare_and_swap(&*cur, new);
            let swapped = ptr_eq(&*cur, &*prev);
            if swapped {
                return Ok(arc_swap::Guard::into_inner(prev));
            } else {
                cur = prev;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arc_swap::ArcSwap;
    use std::sync::Arc;

    #[test]
    fn test_try_rcu_success() {
        let swap = ArcSwap::from(Arc::new(42));

        let result = swap.try_rcu(|value| -> Result<_, String> { Ok(**value + 1) });

        assert!(result.is_ok());
        assert_eq!(**swap.load(), 43);
    }

    #[test]
    fn test_try_rcu_error() {
        let swap = ArcSwap::from(Arc::new(42));

        let result = swap.try_rcu(|value| -> Result<i32, _> {
            if **value == 42 {
                Err("err")
            } else {
                Ok(**value + 1)
            }
        });

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "err");
        assert_eq!(**swap.load(), 42);
    }
}
