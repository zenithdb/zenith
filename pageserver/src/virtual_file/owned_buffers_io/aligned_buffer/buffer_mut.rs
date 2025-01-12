use std::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};

use super::{
    alignment::{Alignment, ConstAlign},
    buffer::AlignedBuffer,
    raw::RawAlignedBuffer,
};

/// A mutable aligned buffer type.
#[derive(Debug)]
pub struct AlignedBufferMut<A: Alignment> {
    raw: RawAlignedBuffer<A>,
}

impl<const A: usize> AlignedBufferMut<ConstAlign<A>> {
    /// Constructs a new, empty `IoBufferMut` with at least the specified capacity and alignment.
    ///
    /// The buffer will be able to hold at most `capacity` elements and will never resize.
    ///
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` _bytes_, or if the following alignment requirement is not met:
    /// * `align` must not be zero,
    ///
    /// * `align` must be a power of two,
    ///
    /// * `capacity`, when rounded up to the nearest multiple of `align`,
    ///    must not overflow isize (i.e., the rounded value must be
    ///    less than or equal to `isize::MAX`).
    pub fn with_capacity(capacity: usize) -> Self {
        AlignedBufferMut {
            raw: RawAlignedBuffer::with_capacity(capacity),
        }
    }

    /// Constructs a new `IoBufferMut` with at least the specified capacity and alignment, filled with zeros.
    pub fn with_capacity_zeroed(capacity: usize) -> Self {
        use bytes::BufMut;
        let mut buf = Self::with_capacity(capacity);
        buf.put_bytes(0, capacity);
        // SAFETY: `put_bytes` filled the entire buffer.
        unsafe { buf.set_len(capacity) };
        buf
    }
}

impl<A: Alignment> AlignedBufferMut<A> {
    /// Constructs a mutable aligned buffer from raw.
    pub(super) fn from_raw(raw: RawAlignedBuffer<A>) -> Self {
        AlignedBufferMut { raw }
    }

    /// Returns the total number of bytes the buffer can hold.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.raw.capacity()
    }

    /// Returns the alignment of the buffer.
    #[inline]
    pub fn align(&self) -> usize {
        self.raw.align()
    }

    /// Returns the number of bytes in the buffer, also referred to as its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Force the length of the buffer to `new_len`.
    #[inline]
    unsafe fn set_len(&mut self, new_len: usize) {
        self.raw.set_len(new_len)
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        self.raw.as_ptr()
    }

    #[inline]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.raw.as_mut_ptr()
    }

    /// Extracts a slice containing the entire buffer.
    ///
    /// Equivalent to `&s[..]`.
    #[inline]
    fn as_slice(&self) -> &[u8] {
        self.raw.as_slice()
    }

    /// Extracts a mutable slice of the entire buffer.
    ///
    /// Equivalent to `&mut s[..]`.
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.raw.as_mut_slice()
    }

    /// Drops the all the contents of the buffer, setting its length to `0`.
    #[inline]
    pub fn clear(&mut self) {
        self.raw.clear()
    }

    /// Reserves capacity for at least `additional` more bytes to be inserted
    /// in the given `IoBufferMut`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` _bytes_.
    pub fn reserve(&mut self, additional: usize) {
        self.raw.reserve(additional);
    }

    /// Shortens the buffer, keeping the first len bytes.
    pub fn truncate(&mut self, len: usize) {
        self.raw.truncate(len);
    }

    /// Consumes and leaks the `IoBufferMut`, returning a mutable reference to the contents, &'a mut [u8].
    pub fn leak<'a>(self) -> &'a mut [u8] {
        self.raw.leak()
    }

    pub fn freeze(self) -> AlignedBuffer<A> {
        let len = self.len();
        AlignedBuffer::from_raw(self.raw, 0..len)
    }

    /// Clones and appends all elements in a slice to the buffer. Reserves additional capacity as needed.
    #[inline]
    pub fn extend_from_slice(&mut self, extend: &[u8]) {
        let cnt = extend.len();
        self.reserve(cnt);

        // SAFETY: we already reserved additional `cnt` bytes, safe to perform memcpy.
        unsafe {
            let dst = self.spare_capacity_mut();
            // Reserved above
            debug_assert!(dst.len() >= cnt);

            core::ptr::copy_nonoverlapping(extend.as_ptr(), dst.as_mut_ptr().cast(), cnt);
        }
        // SAFETY: We do have at least `cnt` bytes remaining before advance.
        unsafe {
            bytes::BufMut::advance_mut(self, cnt);
        }
    }

    /// Returns the remaining spare capacity of the vector as a slice of `MaybeUninit<u8>`.
    #[inline]
    fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: we guarantees that the `Self::capacity()` bytes from
        // `Self::as_mut_ptr()` are allocated.
        unsafe {
            let ptr = self.as_mut_ptr().add(self.len());
            let len = self.capacity() - self.len();

            core::slice::from_raw_parts_mut(ptr.cast(), len)
        }
    }
}

impl<A: Alignment> Deref for AlignedBufferMut<A> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<A: Alignment> DerefMut for AlignedBufferMut<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<A: Alignment> AsRef<[u8]> for AlignedBufferMut<A> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<A: Alignment> AsMut<[u8]> for AlignedBufferMut<A> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl<A: Alignment> PartialEq<[u8]> for AlignedBufferMut<A> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice().eq(other)
    }
}

/// SAFETY: When advancing the internal cursor, the caller needs to make sure the bytes advcanced past have been initialized.
unsafe impl<A: Alignment> bytes::BufMut for AlignedBufferMut<A> {
    #[inline]
    fn remaining_mut(&self) -> usize {
        // Although a `Vec` can have at most isize::MAX bytes, we never want to grow `IoBufferMut`.
        // Thus, it can have at most `self.capacity` bytes.
        self.capacity() - self.len()
    }

    // SAFETY: Caller needs to make sure the bytes being advanced past have been initialized.
    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let len = self.len();
        let remaining = self.remaining_mut();

        if remaining < cnt {
            panic_advance(cnt, remaining);
        }

        // Addition will not overflow since the sum is at most the capacity.
        self.set_len(len + cnt);
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        let cap = self.capacity();
        let len = self.len();

        // SAFETY: Since `self.ptr` is valid for `cap` bytes, `self.ptr.add(len)` must be
        // valid for `cap - len` bytes. The subtraction will not underflow since
        // `len <= cap`.
        unsafe {
            bytes::buf::UninitSlice::from_raw_parts_mut(self.as_mut_ptr().add(len), cap - len)
        }
    }
}

/// Panic with a nice error message.
#[cold]
fn panic_advance(idx: usize, len: usize) -> ! {
    panic!(
        "advance out of bounds: the len is {} but advancing by {}",
        len, idx
    );
}

/// Safety: [`AlignedBufferMut`] has exclusive ownership of the io buffer,
/// and the underlying pointer remains stable while io-uring is owning the buffer.
/// The tokio-epoll-uring crate itself will not resize the buffer and will respect
/// [`tokio_epoll_uring::IoBuf::bytes_total`].
unsafe impl<A: Alignment> tokio_epoll_uring::IoBuf for AlignedBufferMut<A> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: See above.
unsafe impl<A: Alignment> tokio_epoll_uring::IoBufMut for AlignedBufferMut<A> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        if self.len() < init_len {
            self.set_len(init_len);
        }
    }
}

impl<A: Alignment> std::io::Write for AlignedBufferMut<A> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const ALIGN: usize = 4 * 1024;
    type TestIoBufferMut = AlignedBufferMut<ConstAlign<ALIGN>>;

    #[test]
    fn test_with_capacity() {
        let v = TestIoBufferMut::with_capacity(ALIGN * 4);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN * 4);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);

        let v = TestIoBufferMut::with_capacity(ALIGN / 2);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN / 2);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }

    #[test]
    fn test_with_capacity_zeroed() {
        let v = TestIoBufferMut::with_capacity_zeroed(ALIGN);
        assert_eq!(v.len(), ALIGN);
        assert_eq!(v.capacity(), ALIGN);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
        assert_eq!(&v[..], &[0; ALIGN])
    }

    #[test]
    fn test_reserve() {
        use bytes::BufMut;
        let mut v = TestIoBufferMut::with_capacity(ALIGN);
        let capacity = v.capacity();
        v.reserve(capacity);
        assert_eq!(v.capacity(), capacity);
        let data = [b'a'; ALIGN];
        v.put(&data[..]);
        v.reserve(capacity);
        assert!(v.capacity() >= capacity * 2);
        assert_eq!(&v[..], &data[..]);
        let capacity = v.capacity();
        v.clear();
        v.reserve(capacity);
        assert_eq!(capacity, v.capacity());
    }

    #[test]
    fn test_bytes_put() {
        use bytes::BufMut;
        let mut v = TestIoBufferMut::with_capacity(ALIGN * 4);
        let x = [b'a'; ALIGN];

        for _ in 0..2 {
            for _ in 0..4 {
                v.put(&x[..]);
            }
            assert_eq!(v.len(), ALIGN * 4);
            assert_eq!(v.capacity(), ALIGN * 4);
            assert_eq!(v.align(), ALIGN);
            assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
            v.clear()
        }
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN * 4);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }

    #[test]
    #[should_panic]
    fn test_bytes_put_panic() {
        use bytes::BufMut;
        const ALIGN: usize = 4 * 1024;
        let mut v = TestIoBufferMut::with_capacity(ALIGN * 4);
        let x = [b'a'; ALIGN];
        for _ in 0..5 {
            v.put_slice(&x[..]);
        }
    }

    #[test]
    fn test_io_buf_put_slice() {
        use tokio_epoll_uring::BoundedBufMut;
        const ALIGN: usize = 4 * 1024;
        let mut v = TestIoBufferMut::with_capacity(ALIGN);
        let x = [b'a'; ALIGN];

        for _ in 0..2 {
            v.put_slice(&x[..]);
            assert_eq!(v.len(), ALIGN);
            assert_eq!(v.capacity(), ALIGN);
            assert_eq!(v.align(), ALIGN);
            assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
            v.clear()
        }
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }
}
