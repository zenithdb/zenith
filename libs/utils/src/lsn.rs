#![warn(missing_docs)]

use serde::{de::Visitor, Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, AddAssign};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::seqwait::MonotonicCounter;

/// Transaction log block size in bytes
pub const XLOG_BLCKSZ: u32 = 8192;

/// A Postgres LSN (Log Sequence Number), also known as an XLogRecPtr
#[derive(Clone, Copy, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Lsn(pub u64);

impl Serialize for Lsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LsnVisitor {
            is_human_readable_deserializer: bool,
        }

        impl Visitor<'_> for LsnVisitor {
            type Value = Lsn;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                if self.is_human_readable_deserializer {
                    formatter.write_str(
                        "value in form of hex string({upper_u32_hex}/{lower_u32_hex}) representing u64 integer",
                    )
                } else {
                    formatter.write_str("value in form of integer(u64)")
                }
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Lsn(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Lsn::from_str(v).map_err(|e| E::custom(e))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(LsnVisitor {
                is_human_readable_deserializer: true,
            })
        } else {
            deserializer.deserialize_u64(LsnVisitor {
                is_human_readable_deserializer: false,
            })
        }
    }
}

/// Allows (de)serialization of an `Lsn` always as `u64`.
///
/// ### Example
///
/// ```rust
/// # use serde::{Serialize, Deserialize};
/// use utils::lsn::Lsn;
///
/// #[derive(PartialEq, Serialize, Deserialize, Debug)]
/// struct Foo {
///   #[serde(with = "utils::lsn::serde_as_u64")]
///   always_u64: Lsn,
/// }
///
/// let orig = Foo { always_u64: Lsn(1234) };
///
/// let res = serde_json::to_string(&orig).unwrap();
/// assert_eq!(res, r#"{"always_u64":1234}"#);
///
/// let foo = serde_json::from_str::<Foo>(&res).unwrap();
/// assert_eq!(foo, orig);
/// ```
///
pub mod serde_as_u64 {
    use super::Lsn;

    /// Serializes the Lsn as u64 disregarding the human readability of the format.
    ///
    /// Meant to be used via `#[serde(with = "...")]` or `#[serde(serialize_with = "...")]`.
    pub fn serialize<S: serde::Serializer>(lsn: &Lsn, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::Serialize;
        lsn.0.serialize(serializer)
    }

    /// Deserializes the Lsn as u64 disregarding the human readability of the format.
    ///
    /// Meant to be used via `#[serde(with = "...")]` or `#[serde(deserialize_with = "...")]`.
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Lsn, D::Error> {
        use serde::Deserialize;
        u64::deserialize(deserializer).map(Lsn)
    }
}

/// We tried to parse an LSN from a string, but failed
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("LsnParseError")]
pub struct LsnParseError;

impl Lsn {
    /// Maximum possible value for an LSN
    pub const MAX: Lsn = Lsn(u64::MAX);

    /// Invalid value for InvalidXLogRecPtr, as defined in xlogdefs.h
    pub const INVALID: Lsn = Lsn(0);

    /// Subtract a number, returning None on overflow.
    pub fn checked_sub<T: Into<u64>>(self, other: T) -> Option<Lsn> {
        let other: u64 = other.into();
        self.0.checked_sub(other).map(Lsn)
    }

    /// Subtract a number, saturating at numeric bounds instead of overflowing.
    pub fn saturating_sub<T: Into<u64>>(self, other: T) -> Lsn {
        Lsn(self.0.saturating_sub(other.into()))
    }

    /// Subtract a number, returning the difference as i128 to avoid overflow.
    pub fn widening_sub<T: Into<u64>>(self, other: T) -> i128 {
        let other: u64 = other.into();
        i128::from(self.0) - i128::from(other)
    }

    /// Parse an LSN from a string in the form `0000000000000000`
    pub fn from_hex<S>(s: S) -> Result<Self, LsnParseError>
    where
        S: AsRef<str>,
    {
        let s: &str = s.as_ref();
        let n = u64::from_str_radix(s, 16).or(Err(LsnParseError))?;
        Ok(Lsn(n))
    }

    /// Compute the offset into a segment
    #[inline]
    pub fn segment_offset(self, seg_sz: usize) -> usize {
        (self.0 % seg_sz as u64) as usize
    }

    /// Compute LSN of the segment start.
    #[inline]
    pub fn segment_lsn(self, seg_sz: usize) -> Lsn {
        Lsn(self.0 - (self.0 % seg_sz as u64))
    }

    /// Compute the segment number
    #[inline]
    pub fn segment_number(self, seg_sz: usize) -> u64 {
        self.0 / seg_sz as u64
    }

    /// Compute the offset into a block
    #[inline]
    pub fn block_offset(self) -> u64 {
        const BLCKSZ: u64 = XLOG_BLCKSZ as u64;
        self.0 % BLCKSZ
    }

    /// Compute the block offset of the first byte of this Lsn within this
    /// segment
    #[inline]
    pub fn page_lsn(self) -> Lsn {
        Lsn(self.0 - self.block_offset())
    }

    /// Compute the block offset of the first byte of this Lsn within this
    /// segment
    #[inline]
    pub fn page_offset_in_segment(self, seg_sz: usize) -> u64 {
        (self.0 - self.block_offset()) - self.segment_lsn(seg_sz).0
    }

    /// Compute the bytes remaining in this block
    ///
    /// If the LSN is already at the block boundary, it will return `XLOG_BLCKSZ`.
    #[inline]
    pub fn remaining_in_block(self) -> u64 {
        const BLCKSZ: u64 = XLOG_BLCKSZ as u64;
        BLCKSZ - (self.0 % BLCKSZ)
    }

    /// Compute the bytes remaining to fill a chunk of some size
    ///
    /// If the LSN is already at the chunk boundary, it will return 0.
    pub fn calc_padding<T: Into<u64>>(self, sz: T) -> u64 {
        let sz: u64 = sz.into();
        // By using wrapping_sub, we can subtract first and then mod second.
        // If it's done the other way around, then we would return a full
        // chunk size if we're already at the chunk boundary.
        // (Regular subtraction will panic on overflow in debug builds.)
        (sz.wrapping_sub(self.0)) % sz
    }

    /// Align LSN on 8-byte boundary (alignment of WAL records).
    pub fn align(&self) -> Lsn {
        Lsn((self.0 + 7) & !7)
    }

    /// Align LSN on 8-byte boundary (alignment of WAL records).
    pub fn is_aligned(&self) -> bool {
        *self == self.align()
    }

    /// Return if the LSN is valid
    /// mimics postgres XLogRecPtrIsInvalid macro
    pub fn is_valid(self) -> bool {
        self != Lsn::INVALID
    }
}

impl From<u64> for Lsn {
    fn from(n: u64) -> Self {
        Lsn(n)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> u64 {
        lsn.0
    }
}

impl FromStr for Lsn {
    type Err = LsnParseError;

    /// Parse an LSN from a string in the form `00000000/00000000`
    ///
    /// If the input string is missing the '/' character, then use `Lsn::from_hex`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splitter = s.trim().split('/');
        if let (Some(left), Some(right), None) = (splitter.next(), splitter.next(), splitter.next())
        {
            let left_num = u32::from_str_radix(left, 16).map_err(|_| LsnParseError)?;
            let right_num = u32::from_str_radix(right, 16).map_err(|_| LsnParseError)?;
            Ok(Lsn(((left_num as u64) << 32) | right_num as u64))
        } else {
            Err(LsnParseError)
        }
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl Add<u64> for Lsn {
    type Output = Lsn;

    fn add(self, other: u64) -> Self::Output {
        // panic if the addition overflows.
        Lsn(self.0.checked_add(other).unwrap())
    }
}

impl AddAssign<u64> for Lsn {
    fn add_assign(&mut self, other: u64) {
        // panic if the addition overflows.
        self.0 = self.0.checked_add(other).unwrap();
    }
}

/// An [`Lsn`] that can be accessed atomically.
pub struct AtomicLsn {
    inner: AtomicU64,
}

impl AtomicLsn {
    /// Creates a new atomic `Lsn`.
    pub fn new(val: u64) -> Self {
        AtomicLsn {
            inner: AtomicU64::new(val),
        }
    }

    /// Atomically retrieve the `Lsn` value from memory.
    pub fn load(&self) -> Lsn {
        Lsn(self.inner.load(Ordering::Acquire))
    }

    /// Atomically store a new `Lsn` value to memory.
    pub fn store(&self, lsn: Lsn) {
        self.inner.store(lsn.0, Ordering::Release);
    }

    /// Adds to the current value, returning the previous value.
    ///
    /// This operation will panic on overflow.
    pub fn fetch_add(&self, val: u64) -> Lsn {
        let prev = self.inner.fetch_add(val, Ordering::AcqRel);
        assert!(prev.checked_add(val).is_some(), "AtomicLsn overflow");
        Lsn(prev)
    }

    /// Atomically sets the Lsn to the max of old and new value, returning the old value.
    pub fn fetch_max(&self, lsn: Lsn) -> Lsn {
        let prev = self.inner.fetch_max(lsn.0, Ordering::AcqRel);
        Lsn(prev)
    }
}

impl From<Lsn> for AtomicLsn {
    fn from(lsn: Lsn) -> Self {
        Self::new(lsn.0)
    }
}

/// Pair of LSN's pointing to the end of the last valid record and previous one
#[derive(Debug, Clone, Copy)]
pub struct RecordLsn {
    /// LSN at the end of the current record
    pub last: Lsn,
    /// LSN at the end of the previous record
    pub prev: Lsn,
}

/// Expose `self.last` as counter to be able to use RecordLsn in SeqWait
impl MonotonicCounter<Lsn> for RecordLsn {
    fn cnt_advance(&mut self, lsn: Lsn) {
        assert!(self.last <= lsn);
        let new_prev = self.last;
        self.last = lsn;
        self.prev = new_prev;
    }
    fn cnt_value(&self) -> Lsn {
        self.last
    }
}

/// Implements  [`rand::distributions::uniform::UniformSampler`] so we can sample [`Lsn`]s.
///
/// This is used by the `pagebench` pageserver benchmarking tool.
pub struct LsnSampler(<u64 as rand::distributions::uniform::SampleUniform>::Sampler);

impl rand::distributions::uniform::SampleUniform for Lsn {
    type Sampler = LsnSampler;
}

impl rand::distributions::uniform::UniformSampler for LsnSampler {
    type X = Lsn;

    fn new<B1, B2>(low: B1, high: B2) -> Self
    where
        B1: rand::distributions::uniform::SampleBorrow<Self::X> + Sized,
        B2: rand::distributions::uniform::SampleBorrow<Self::X> + Sized,
    {
        Self(
            <u64 as rand::distributions::uniform::SampleUniform>::Sampler::new(
                low.borrow().0,
                high.borrow().0,
            ),
        )
    }

    fn new_inclusive<B1, B2>(low: B1, high: B2) -> Self
    where
        B1: rand::distributions::uniform::SampleBorrow<Self::X> + Sized,
        B2: rand::distributions::uniform::SampleBorrow<Self::X> + Sized,
    {
        Self(
            <u64 as rand::distributions::uniform::SampleUniform>::Sampler::new_inclusive(
                low.borrow().0,
                high.borrow().0,
            ),
        )
    }

    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Self::X {
        Lsn(self.0.sample(rng))
    }
}

#[cfg(test)]
mod tests {
    use crate::bin_ser::BeSer;

    use super::*;

    use serde_assert::{Deserializer, Serializer, Token, Tokens};

    #[test]
    fn test_lsn_strings() {
        assert_eq!("12345678/AAAA5555".parse(), Ok(Lsn(0x12345678AAAA5555)));
        assert_eq!("aaaa/bbbb".parse(), Ok(Lsn(0x0000AAAA0000BBBB)));
        assert_eq!("1/A".parse(), Ok(Lsn(0x000000010000000A)));
        assert_eq!("0/0".parse(), Ok(Lsn(0)));
        "ABCDEFG/12345678".parse::<Lsn>().unwrap_err();
        "123456789/AAAA5555".parse::<Lsn>().unwrap_err();
        "12345678/AAAA55550".parse::<Lsn>().unwrap_err();
        "-1/0".parse::<Lsn>().unwrap_err();
        "1/-1".parse::<Lsn>().unwrap_err();

        assert_eq!(format!("{}", Lsn(0x12345678AAAA5555)), "12345678/AAAA5555");
        assert_eq!(format!("{}", Lsn(0x000000010000000A)), "1/A");

        assert_eq!(
            Lsn::from_hex("12345678AAAA5555"),
            Ok(Lsn(0x12345678AAAA5555))
        );
        assert_eq!(Lsn::from_hex("0"), Ok(Lsn(0)));
        assert_eq!(Lsn::from_hex("F12345678AAAA5555"), Err(LsnParseError));

        let expected_lsn = Lsn(0x3C490F8);
        assert_eq!(" 0/3C490F8".parse(), Ok(expected_lsn));
        assert_eq!("0/3C490F8 ".parse(), Ok(expected_lsn));
        assert_eq!(" 0/3C490F8 ".parse(), Ok(expected_lsn));
    }

    #[test]
    fn test_lsn_math() {
        assert_eq!(Lsn(1234) + 11u64, Lsn(1245));

        assert_eq!(
            {
                let mut lsn = Lsn(1234);
                lsn += 11u64;
                lsn
            },
            Lsn(1245)
        );

        assert_eq!(Lsn(1234).checked_sub(1233u64), Some(Lsn(1)));
        assert_eq!(Lsn(1234).checked_sub(1235u64), None);

        assert_eq!(Lsn(1235).widening_sub(1234u64), 1);
        assert_eq!(Lsn(1234).widening_sub(1235u64), -1);
        assert_eq!(Lsn(u64::MAX).widening_sub(0u64), i128::from(u64::MAX));
        assert_eq!(Lsn(0).widening_sub(u64::MAX), -i128::from(u64::MAX));

        let seg_sz: usize = 16 * 1024 * 1024;
        assert_eq!(Lsn(0x1000007).segment_offset(seg_sz), 7);
        assert_eq!(Lsn(0x1000007).segment_number(seg_sz), 1u64);

        assert_eq!(Lsn(0x4007).block_offset(), 7u64);
        assert_eq!(Lsn(0x4000).block_offset(), 0u64);
        assert_eq!(Lsn(0x4007).remaining_in_block(), 8185u64);
        assert_eq!(Lsn(0x4000).remaining_in_block(), 8192u64);

        assert_eq!(Lsn(0xffff01).calc_padding(seg_sz as u64), 255u64);
        assert_eq!(Lsn(0x2000000).calc_padding(seg_sz as u64), 0u64);
        assert_eq!(Lsn(0xffff01).calc_padding(8u32), 7u64);
        assert_eq!(Lsn(0xffff00).calc_padding(8u32), 0u64);
    }

    #[test]
    fn test_atomic_lsn() {
        let lsn = AtomicLsn::new(0);
        assert_eq!(lsn.fetch_add(1234), Lsn(0));
        assert_eq!(lsn.load(), Lsn(1234));
        lsn.store(Lsn(5678));
        assert_eq!(lsn.load(), Lsn(5678));

        assert_eq!(lsn.fetch_max(Lsn(6000)), Lsn(5678));
        assert_eq!(lsn.fetch_max(Lsn(5000)), Lsn(6000));
    }

    #[test]
    fn test_lsn_serde() {
        let original_lsn = Lsn(0x0123456789abcdef);
        let expected_readable_tokens = Tokens(vec![Token::U64(0x0123456789abcdef)]);
        let expected_non_readable_tokens =
            Tokens(vec![Token::Str(String::from("1234567/89ABCDEF"))]);

        // Testing human_readable ser/de
        let serializer = Serializer::builder().is_human_readable(false).build();
        let readable_ser_tokens = original_lsn.serialize(&serializer).unwrap();
        assert_eq!(readable_ser_tokens, expected_readable_tokens);

        let mut deserializer = Deserializer::builder()
            .is_human_readable(false)
            .tokens(readable_ser_tokens)
            .build();
        let des_lsn = Lsn::deserialize(&mut deserializer).unwrap();
        assert_eq!(des_lsn, original_lsn);

        // Testing NON human_readable ser/de
        let serializer = Serializer::builder().is_human_readable(true).build();
        let non_readable_ser_tokens = original_lsn.serialize(&serializer).unwrap();
        assert_eq!(non_readable_ser_tokens, expected_non_readable_tokens);

        let mut deserializer = Deserializer::builder()
            .is_human_readable(true)
            .tokens(non_readable_ser_tokens)
            .build();
        let des_lsn = Lsn::deserialize(&mut deserializer).unwrap();
        assert_eq!(des_lsn, original_lsn);

        // Testing mismatching ser/de
        let serializer = Serializer::builder().is_human_readable(false).build();
        let non_readable_ser_tokens = original_lsn.serialize(&serializer).unwrap();

        let mut deserializer = Deserializer::builder()
            .is_human_readable(true)
            .tokens(non_readable_ser_tokens)
            .build();
        Lsn::deserialize(&mut deserializer).unwrap_err();

        let serializer = Serializer::builder().is_human_readable(true).build();
        let readable_ser_tokens = original_lsn.serialize(&serializer).unwrap();

        let mut deserializer = Deserializer::builder()
            .is_human_readable(false)
            .tokens(readable_ser_tokens)
            .build();
        Lsn::deserialize(&mut deserializer).unwrap_err();
    }

    #[test]
    fn test_lsn_ensure_roundtrip() {
        let original_lsn = Lsn(0xaaaabbbb);

        let serializer = Serializer::builder().is_human_readable(false).build();
        let ser_tokens = original_lsn.serialize(&serializer).unwrap();

        let mut deserializer = Deserializer::builder()
            .is_human_readable(false)
            .tokens(ser_tokens)
            .build();

        let des_lsn = Lsn::deserialize(&mut deserializer).unwrap();
        assert_eq!(des_lsn, original_lsn);
    }

    #[test]
    fn test_lsn_bincode_serde() {
        let lsn = Lsn(0x0123456789abcdef);
        let expected_bytes = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];

        let ser_bytes = lsn.ser().unwrap();
        assert_eq!(ser_bytes, expected_bytes);

        let des_lsn = Lsn::des(&ser_bytes).unwrap();
        assert_eq!(des_lsn, lsn);
    }

    #[test]
    fn test_lsn_bincode_ensure_roundtrip() {
        let original_lsn = Lsn(0x01_02_03_04_05_06_07_08);
        let expected_bytes = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

        let ser_bytes = original_lsn.ser().unwrap();
        assert_eq!(ser_bytes, expected_bytes);

        let des_lsn = Lsn::des(&ser_bytes).unwrap();
        assert_eq!(des_lsn, original_lsn);
    }
}
