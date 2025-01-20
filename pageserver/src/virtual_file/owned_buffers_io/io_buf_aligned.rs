use tokio_epoll_uring::{IoBuf, IoBufMut};

use crate::virtual_file::{self, IoBuffer, IoBufferMut, PageWriteGuardBuf};

/// A marker trait for a mutable aligned buffer type.
pub trait IoBufAlignedMut: IoBufMut {
    const ALIGN: usize = virtual_file::get_io_buffer_alignment();
}

/// A marker trait for an aligned buffer type.
pub trait IoBufAligned: IoBuf {}

impl IoBufAlignedMut for IoBufferMut {}

impl IoBufAligned for IoBuffer {}

impl IoBufAlignedMut for PageWriteGuardBuf {}
