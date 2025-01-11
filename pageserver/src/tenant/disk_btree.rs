//!
//! Simple on-disk B-tree implementation
//!
//! This is used as the index structure within image and delta layers
//!
//! Features:
//! - Fixed-width keys
//! - Fixed-width values (VALUE_SZ)
//! - The tree is created in a bulk operation. Insert/deletion after creation
//!   is not supported
//! - page-oriented
//!
//! TODO:
//! - maybe something like an Adaptive Radix Tree would be more efficient?
//! - the values stored by image and delta layers are offsets into the file,
//!   and they are in monotonically increasing order. Prefix compression would
//!   be very useful for them, too.
//! - An Iterator interface would be more convenient for the callers than the
//!   'visit' function
//!
use async_stream::try_stream;
use byteorder::{ReadBytesExt, BE};
use bytes::{BufMut, Bytes, BytesMut};
use either::Either;
use futures::{Stream, StreamExt};
use hex;
use std::{
    cmp::Ordering,
    io,
    iter::Rev,
    ops::{Range, RangeInclusive},
    result,
};
use thiserror::Error;
use tracing::error;

use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::block_io::{BlockReader, BlockWriter},
};

// The maximum size of a value stored in the B-tree. 5 bytes is enough currently.
pub const VALUE_SZ: usize = 5;
pub const MAX_VALUE: u64 = 0x007f_ffff_ffff;

pub const PAGE_SZ: usize = 8192;

#[derive(Clone, Copy, Debug)]
struct Value([u8; VALUE_SZ]);

impl Value {
    fn from_slice(slice: &[u8]) -> Value {
        let mut b = [0u8; VALUE_SZ];
        b.copy_from_slice(slice);
        Value(b)
    }

    fn from_u64(x: u64) -> Value {
        assert!(x <= 0x007f_ffff_ffff);
        Value([
            (x >> 32) as u8,
            (x >> 24) as u8,
            (x >> 16) as u8,
            (x >> 8) as u8,
            x as u8,
        ])
    }

    fn from_blknum(x: u32) -> Value {
        Value([
            0x80,
            (x >> 24) as u8,
            (x >> 16) as u8,
            (x >> 8) as u8,
            x as u8,
        ])
    }

    #[allow(dead_code)]
    fn is_offset(self) -> bool {
        self.0[0] & 0x80 != 0
    }

    fn to_u64(self) -> u64 {
        let b = &self.0;
        ((b[0] as u64) << 32)
            | ((b[1] as u64) << 24)
            | ((b[2] as u64) << 16)
            | ((b[3] as u64) << 8)
            | b[4] as u64
    }

    fn to_blknum(self) -> u32 {
        let b = &self.0;
        assert!(b[0] == 0x80);
        ((b[1] as u32) << 24) | ((b[2] as u32) << 16) | ((b[3] as u32) << 8) | b[4] as u32
    }
}

#[derive(Error, Debug)]
pub enum DiskBtreeError {
    #[error("Attempt to append a value that is too large {0} > {}", MAX_VALUE)]
    AppendOverflow(u64),

    #[error("Unsorted input: key {key:?} is <= last_key {last_key:?}")]
    UnsortedInput { key: Box<[u8]>, last_key: Box<[u8]> },

    #[error("Could not push to new leaf node")]
    FailedToPushToNewLeafNode,

    #[error("IoError: {0}")]
    Io(#[from] io::Error),
}

pub type Result<T> = result::Result<T, DiskBtreeError>;

/// This is the on-disk representation.
struct OnDiskNode<'a, const L: usize> {
    // Fixed-width fields
    num_children: u16,
    level: u8,
    prefix_len: u8,
    suffix_len: u8,

    // Variable-length fields. These are stored on-disk after the fixed-width
    // fields, in this order. In the in-memory representation, these point to
    // the right parts in the page buffer.
    prefix: &'a [u8],
    keys: &'a [u8],
    values: &'a [u8],
}

impl<const L: usize> OnDiskNode<'_, L> {
    ///
    /// Interpret a PAGE_SZ page as a node.
    ///
    fn deparse(buf: &[u8]) -> Result<OnDiskNode<L>> {
        let mut cursor = std::io::Cursor::new(buf);
        let num_children = cursor.read_u16::<BE>()?;
        let level = cursor.read_u8()?;
        let prefix_len = cursor.read_u8()?;
        let suffix_len = cursor.read_u8()?;

        let mut off = cursor.position();
        let prefix_off = off as usize;
        off += prefix_len as u64;

        let keys_off = off as usize;
        let keys_len = num_children as usize * suffix_len as usize;
        off += keys_len as u64;

        let values_off = off as usize;
        let values_len = num_children as usize * VALUE_SZ;
        //off += values_len as u64;

        let prefix = &buf[prefix_off..prefix_off + prefix_len as usize];
        let keys = &buf[keys_off..keys_off + keys_len];
        let values = &buf[values_off..values_off + values_len];

        Ok(OnDiskNode {
            num_children,
            level,
            prefix_len,
            suffix_len,
            prefix,
            keys,
            values,
        })
    }

    ///
    /// Read a value at 'idx'
    ///
    fn value(&self, idx: usize) -> Value {
        let value_off = idx * VALUE_SZ;
        let value_slice = &self.values[value_off..value_off + VALUE_SZ];
        Value::from_slice(value_slice)
    }

    fn binary_search(
        &self,
        search_key: &[u8; L],
        keybuf: &mut [u8],
    ) -> result::Result<usize, usize> {
        let mut size = self.num_children as usize;
        let mut low = 0;
        let mut high = size;
        while low < high {
            let mid = low + size / 2;

            let key_off = mid * self.suffix_len as usize;
            let suffix = &self.keys[key_off..key_off + self.suffix_len as usize];
            // Does this match?
            keybuf[self.prefix_len as usize..].copy_from_slice(suffix);

            let cmp = keybuf[..].cmp(search_key);

            if cmp == Ordering::Less {
                low = mid + 1;
            } else if cmp == Ordering::Greater {
                high = mid;
            } else {
                return Ok(mid);
            }
            size = high - low;
        }
        Err(low)
    }
}

///
/// Public reader object, to search the tree.
///
#[derive(Clone)]
pub struct DiskBtreeReader<R, const L: usize>
where
    R: BlockReader,
{
    start_blk: u32,
    root_blk: u32,
    reader: R,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VisitDirection {
    Forwards,
    Backwards,
}

impl<R, const L: usize> DiskBtreeReader<R, L>
where
    R: BlockReader,
{
    pub fn new(start_blk: u32, root_blk: u32, reader: R) -> Self {
        DiskBtreeReader {
            start_blk,
            root_blk,
            reader,
        }
    }

    ///
    /// Read the value for given key. Returns the value, or None if it doesn't exist.
    ///
    pub async fn get(&self, search_key: &[u8; L], ctx: &RequestContext) -> Result<Option<u64>> {
        let mut result: Option<u64> = None;
        self.visit(
            search_key,
            VisitDirection::Forwards,
            |key, value| {
                if key == search_key {
                    result = Some(value);
                }
                false
            },
            ctx,
        )
        .await?;
        Ok(result)
    }

    pub fn iter<'a>(self, start_key: &'a [u8; L], ctx: &'a RequestContext) -> DiskBtreeIterator<'a>
    where
        R: 'a + Send,
    {
        DiskBtreeIterator {
            stream: Box::pin(self.into_stream(start_key, ctx)),
        }
    }

    /// Return a stream which yields all key, value pairs from the index
    /// starting from the first key greater or equal to `start_key`.
    ///
    /// Note 1: that this is a copy of [`Self::visit`].
    /// TODO: Once the sequential read path is removed this will become
    /// the only index traversal method.
    ///
    /// Note 2: this function used to take `&self` but it now consumes `self`. This is due to
    /// the lifetime constraints of the reader and the stream / iterator it creates. Using `&self`
    /// requires the reader to be present when the stream is used, and this creates a lifetime
    /// dependency between the reader and the stream. Now if we want to create an iterator that
    /// holds the stream, someone will need to keep a reference to the reader, which is inconvenient
    /// to use from the image/delta layer APIs.
    ///
    /// Feel free to add the `&self` variant back if it's necessary.
    pub fn into_stream<'a>(
        self,
        start_key: &'a [u8; L],
        ctx: &'a RequestContext,
    ) -> impl Stream<Item = std::result::Result<(Vec<u8>, u64), DiskBtreeError>> + 'a
    where
        R: 'a,
    {
        try_stream! {
            let mut stack = Vec::new();
            stack.push((self.root_blk, None));
            let block_cursor = self.reader.block_cursor();
            let mut node_buf = [0_u8; PAGE_SZ];
            while let Some((node_blknum, opt_iter)) = stack.pop() {
                // Read the node, through the PS PageCache, into local variable `node_buf`.
                // We could keep the page cache read guard alive, but, at the time of writing,
                // we run quite small PS PageCache s => can't risk running out of
                // PageCache space because this stream isn't consumed fast enough.
                let page_read_guard = block_cursor
                    .read_blk(self.start_blk + node_blknum, ctx)
                    .await?;
                node_buf.copy_from_slice(page_read_guard.as_ref());
                drop(page_read_guard); // drop page cache read guard early

                let node = OnDiskNode::deparse(&node_buf)?;
                let prefix_len = node.prefix_len as usize;
                let suffix_len = node.suffix_len as usize;

                assert!(node.num_children > 0);

                let mut keybuf = Vec::new();
                keybuf.extend(node.prefix);
                keybuf.resize(prefix_len + suffix_len, 0);

                let mut iter: Either<Range<usize>, Rev<RangeInclusive<usize>>> = if let Some(iter) = opt_iter {
                    iter
                } else {
                    // Locate the first match
                    let idx = match node.binary_search(start_key, keybuf.as_mut_slice()) {
                        Ok(idx) => idx,
                        Err(idx) => {
                            if node.level == 0 {
                                // Imagine that the node contains the following keys:
                                //
                                // 1
                                // 3  <-- idx
                                // 5
                                //
                                // If the search key is '2' and there is exact match,
                                // the binary search would return the index of key
                                // '3'. That's cool, '3' is the first key to return.
                                idx
                            } else {
                                // This is an internal page, so each key represents a lower
                                // bound for what's in the child page. If there is no exact
                                // match, we have to return the *previous* entry.
                                //
                                // 1  <-- return this
                                // 3  <-- idx
                                // 5
                                idx.saturating_sub(1)
                            }
                        }
                    };
                    Either::Left(idx..node.num_children.into())
                };


                // idx points to the first match now. Keep going from there
                while let Some(idx) = iter.next() {
                    let key_off = idx * suffix_len;
                    let suffix = &node.keys[key_off..key_off + suffix_len];
                    keybuf[prefix_len..].copy_from_slice(suffix);
                    let value = node.value(idx);
                    #[allow(clippy::collapsible_if)]
                    if node.level == 0 {
                        // leaf
                        yield (keybuf.clone(), value.to_u64());
                    } else {
                        stack.push((node_blknum, Some(iter)));
                        stack.push((value.to_blknum(), None));
                        break;
                    }
                }
            }
        }
    }

    ///
    /// Scan the tree, starting from 'search_key', in the given direction. 'visitor'
    /// will be called for every key >= 'search_key' (or <= 'search_key', if scanning
    /// backwards)
    ///
    pub async fn visit<V>(
        &self,
        search_key: &[u8; L],
        dir: VisitDirection,
        mut visitor: V,
        ctx: &RequestContext,
    ) -> Result<bool>
    where
        V: FnMut(&[u8], u64) -> bool,
    {
        let mut stack = Vec::new();
        stack.push((self.root_blk, None));
        let block_cursor = self.reader.block_cursor();
        while let Some((node_blknum, opt_iter)) = stack.pop() {
            // Locate the node.
            let node_buf = block_cursor
                .read_blk(self.start_blk + node_blknum, ctx)
                .await?;

            let node = OnDiskNode::deparse(node_buf.as_ref())?;
            let prefix_len = node.prefix_len as usize;
            let suffix_len = node.suffix_len as usize;

            assert!(node.num_children > 0);

            let mut keybuf = Vec::new();
            keybuf.extend(node.prefix);
            keybuf.resize(prefix_len + suffix_len, 0);

            let mut iter = if let Some(iter) = opt_iter {
                iter
            } else if dir == VisitDirection::Forwards {
                // Locate the first match
                let idx = match node.binary_search(search_key, keybuf.as_mut_slice()) {
                    Ok(idx) => idx,
                    Err(idx) => {
                        if node.level == 0 {
                            // Imagine that the node contains the following keys:
                            //
                            // 1
                            // 3  <-- idx
                            // 5
                            //
                            // If the search key is '2' and there is exact match,
                            // the binary search would return the index of key
                            // '3'. That's cool, '3' is the first key to return.
                            idx
                        } else {
                            // This is an internal page, so each key represents a lower
                            // bound for what's in the child page. If there is no exact
                            // match, we have to return the *previous* entry.
                            //
                            // 1  <-- return this
                            // 3  <-- idx
                            // 5
                            idx.saturating_sub(1)
                        }
                    }
                };
                Either::Left(idx..node.num_children.into())
            } else {
                let idx = match node.binary_search(search_key, keybuf.as_mut_slice()) {
                    Ok(idx) => {
                        // Exact match. That's the first entry to return, and walk
                        // backwards from there.
                        idx
                    }
                    Err(idx) => {
                        // No exact match. The binary search returned the index of the
                        // first key that's > search_key. Back off by one, and walk
                        // backwards from there.
                        if let Some(idx) = idx.checked_sub(1) {
                            idx
                        } else {
                            return Ok(false);
                        }
                    }
                };
                Either::Right((0..=idx).rev())
            };

            // idx points to the first match now. Keep going from there
            while let Some(idx) = iter.next() {
                let key_off = idx * suffix_len;
                let suffix = &node.keys[key_off..key_off + suffix_len];
                keybuf[prefix_len..].copy_from_slice(suffix);
                let value = node.value(idx);
                #[allow(clippy::collapsible_if)]
                if node.level == 0 {
                    // leaf
                    if !visitor(&keybuf, value.to_u64()) {
                        return Ok(false);
                    }
                } else {
                    stack.push((node_blknum, Some(iter)));
                    stack.push((value.to_blknum(), None));
                    break;
                }
            }
        }
        Ok(true)
    }

    #[allow(dead_code)]
    pub async fn dump(&self) -> Result<()> {
        let mut stack = Vec::new();
        let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);

        stack.push((self.root_blk, String::new(), 0, 0, 0));

        let block_cursor = self.reader.block_cursor();

        while let Some((blknum, path, depth, child_idx, key_off)) = stack.pop() {
            let blk = block_cursor.read_blk(self.start_blk + blknum, &ctx).await?;
            let buf: &[u8] = blk.as_ref();
            let node = OnDiskNode::<L>::deparse(buf)?;

            if child_idx == 0 {
                print!("{:indent$}", "", indent = depth * 2);
                let path_prefix = stack
                    .iter()
                    .map(|(_blknum, path, ..)| path.as_str())
                    .collect::<String>();
                println!(
                    "blk #{blknum}: path {path_prefix}{path}: prefix {}, suffix_len {}",
                    hex::encode(node.prefix),
                    node.suffix_len
                );
            }

            if child_idx + 1 < node.num_children {
                let key_off = key_off + node.suffix_len as usize;
                stack.push((blknum, path.clone(), depth, child_idx + 1, key_off));
            }
            let key = &node.keys[key_off..key_off + node.suffix_len as usize];
            let val = node.value(child_idx as usize);

            print!("{:indent$}", "", indent = depth * 2 + 2);
            println!("{}: {}", hex::encode(key), hex::encode(val.0));

            if node.level > 0 {
                stack.push((val.to_blknum(), hex::encode(node.prefix), depth + 1, 0, 0));
            }
        }
        Ok(())
    }
}

pub struct DiskBtreeIterator<'a> {
    #[allow(clippy::type_complexity)]
    stream: std::pin::Pin<
        Box<dyn Stream<Item = std::result::Result<(Vec<u8>, u64), DiskBtreeError>> + 'a + Send>,
    >,
}

impl DiskBtreeIterator<'_> {
    pub async fn next(&mut self) -> Option<std::result::Result<(Vec<u8>, u64), DiskBtreeError>> {
        self.stream.next().await
    }
}

///
/// Public builder object, for creating a new tree.
///
/// Usage: Create a builder object by calling 'new', load all the data into the
/// tree by calling 'append' for each key-value pair, and then call 'finish'
///
/// 'L' is the key length in bytes
pub struct DiskBtreeBuilder<W, const L: usize>
where
    W: BlockWriter,
{
    writer: W,

    ///
    /// `stack[0]` is the current root page, `stack.last()` is the leaf.
    ///
    /// We maintain the length of the stack to be always greater than zero.
    /// Two exceptions are:
    /// 1. `Self::flush_node`. The method will push the new node if it extracted the last one.
    ///    So because other methods cannot see the intermediate state invariant still holds.
    /// 2. `Self::finish`. It consumes self and does not return it back,
    ///    which means that this is where the structure is destroyed.
    ///    Thus stack of zero length cannot be observed by other methods.
    stack: Vec<BuildNode<L>>,

    /// Last key that was appended to the tree. Used to sanity check that append
    /// is called in increasing key order.
    last_key: Option<[u8; L]>,
}

impl<W, const L: usize> DiskBtreeBuilder<W, L>
where
    W: BlockWriter,
{
    pub fn new(writer: W) -> Self {
        DiskBtreeBuilder {
            writer,
            last_key: None,
            stack: vec![BuildNode::new(0)],
        }
    }

    pub fn append(&mut self, key: &[u8; L], value: u64) -> Result<()> {
        if value > MAX_VALUE {
            return Err(DiskBtreeError::AppendOverflow(value));
        }
        if let Some(last_key) = &self.last_key {
            if key <= last_key {
                return Err(DiskBtreeError::UnsortedInput {
                    key: key.as_slice().into(),
                    last_key: last_key.as_slice().into(),
                });
            }
        }
        self.last_key = Some(*key);

        self.append_internal(key, Value::from_u64(value))
    }

    fn append_internal(&mut self, key: &[u8; L], value: Value) -> Result<()> {
        // Try to append to the current leaf buffer
        let last = self
            .stack
            .last_mut()
            .expect("should always have at least one item");
        let level = last.level;
        if last.push(key, value) {
            return Ok(());
        }

        // It did not fit. Try to compress, and if it succeeds to make
        // some room on the node, try appending to it again.
        #[allow(clippy::collapsible_if)]
        if last.compress() {
            if last.push(key, value) {
                return Ok(());
            }
        }

        // Could not append to the current leaf. Flush it and create a new one.
        self.flush_node()?;

        // Replace the node we flushed with an empty one and append the new
        // key to it.
        let mut last = BuildNode::new(level);
        if !last.push(key, value) {
            return Err(DiskBtreeError::FailedToPushToNewLeafNode);
        }

        self.stack.push(last);

        Ok(())
    }

    /// Flush the bottommost node in the stack to disk. Appends a downlink to its parent,
    /// and recursively flushes the parent too, if it becomes full. If the root page becomes full,
    /// creates a new root page, increasing the height of the tree.
    fn flush_node(&mut self) -> Result<()> {
        // Get the current bottommost node in the stack and flush it to disk.
        let last = self
            .stack
            .pop()
            .expect("should always have at least one item");
        let buf = last.pack();
        let downlink_key = last.first_key();
        let downlink_ptr = self.writer.write_blk(buf)?;

        // Append the downlink to the parent. If there is no parent, ie. this was the root page,
        // create a new root page, increasing the height of the tree.
        if self.stack.is_empty() {
            self.stack.push(BuildNode::new(last.level + 1));
        }
        self.append_internal(&downlink_key, Value::from_blknum(downlink_ptr))
    }

    ///
    /// Flushes everything to disk, and returns the block number of the root page.
    /// The caller must store the root block number "out-of-band", and pass it
    /// to the DiskBtreeReader::new() when you want to read the tree again.
    /// (In the image and delta layers, it is stored in the beginning of the file,
    /// in the summary header)
    ///
    pub fn finish(mut self) -> Result<(u32, W)> {
        // flush all levels, except the root.
        while self.stack.len() > 1 {
            self.flush_node()?;
        }

        let root = self
            .stack
            .first()
            .expect("by the check above we left one item there");
        let buf = root.pack();
        let root_blknum = self.writer.write_blk(buf)?;

        Ok((root_blknum, self.writer))
    }

    pub fn borrow_writer(&self) -> &W {
        &self.writer
    }
}

///
/// BuildNode represesnts an incomplete page that we are appending to.
///
#[derive(Clone, Debug)]
struct BuildNode<const L: usize> {
    num_children: u16,
    level: u8,
    prefix: Vec<u8>,
    suffix_len: usize,

    keys: Vec<u8>,
    values: Vec<u8>,

    size: usize, // physical size of this node, if it was written to disk like this
}

const NODE_SIZE: usize = PAGE_SZ;

const NODE_HDR_SIZE: usize = 2 + 1 + 1 + 1;

impl<const L: usize> BuildNode<L> {
    fn new(level: u8) -> Self {
        BuildNode {
            num_children: 0,
            level,
            prefix: Vec::new(),
            suffix_len: 0,
            keys: Vec::new(),
            values: Vec::new(),
            size: NODE_HDR_SIZE,
        }
    }

    /// Try to append a key-value pair to this node. Returns 'true' on
    /// success, 'false' if the page was full or the key was
    /// incompatible with the prefix of the existing keys.
    fn push(&mut self, key: &[u8; L], value: Value) -> bool {
        // If we have already performed prefix-compression on the page,
        // check that the incoming key has the same prefix.
        if self.num_children > 0 {
            // does the prefix allow it?
            if !key.starts_with(&self.prefix) {
                return false;
            }
        } else {
            self.suffix_len = key.len();
        }

        // Is the node too full?
        if self.size + self.suffix_len + VALUE_SZ >= NODE_SIZE {
            return false;
        }

        // All clear
        self.num_children += 1;
        self.keys.extend(&key[self.prefix.len()..]);
        self.values.extend(value.0);

        assert!(self.keys.len() == self.num_children as usize * self.suffix_len);
        assert!(self.values.len() == self.num_children as usize * VALUE_SZ);

        self.size += self.suffix_len + VALUE_SZ;

        true
    }

    ///
    /// Perform prefix-compression.
    ///
    /// Returns 'true' on success, 'false' if no compression was possible.
    ///
    fn compress(&mut self) -> bool {
        let first_suffix = self.first_suffix();
        let last_suffix = self.last_suffix();

        // Find the common prefix among all keys
        let mut prefix_len = 0;
        while prefix_len < self.suffix_len {
            if first_suffix[prefix_len] != last_suffix[prefix_len] {
                break;
            }
            prefix_len += 1;
        }
        if prefix_len == 0 {
            return false;
        }

        // Can compress. Rewrite the keys without the common prefix.
        self.prefix.extend(&self.keys[..prefix_len]);

        let mut new_keys = Vec::new();
        let mut key_off = 0;
        while key_off < self.keys.len() {
            let next_key_off = key_off + self.suffix_len;
            new_keys.extend(&self.keys[key_off + prefix_len..next_key_off]);
            key_off = next_key_off;
        }
        self.keys = new_keys;
        self.suffix_len -= prefix_len;

        self.size -= prefix_len * self.num_children as usize;
        self.size += prefix_len;

        assert!(self.keys.len() == self.num_children as usize * self.suffix_len);
        assert!(self.values.len() == self.num_children as usize * VALUE_SZ);

        true
    }

    ///
    /// Serialize the node to on-disk format.
    ///
    fn pack(&self) -> Bytes {
        assert!(self.keys.len() == self.num_children as usize * self.suffix_len);
        assert!(self.values.len() == self.num_children as usize * VALUE_SZ);
        assert!(self.num_children > 0);

        let mut buf = BytesMut::new();

        buf.put_u16(self.num_children);
        buf.put_u8(self.level);
        buf.put_u8(self.prefix.len() as u8);
        buf.put_u8(self.suffix_len as u8);
        buf.put(&self.prefix[..]);
        buf.put(&self.keys[..]);
        buf.put(&self.values[..]);

        assert!(buf.len() == self.size);

        assert!(buf.len() <= PAGE_SZ);
        buf.resize(PAGE_SZ, 0);
        buf.freeze()
    }

    fn first_suffix(&self) -> &[u8] {
        &self.keys[..self.suffix_len]
    }
    fn last_suffix(&self) -> &[u8] {
        &self.keys[self.keys.len() - self.suffix_len..]
    }

    /// Return the full first key of the page, including the prefix
    fn first_key(&self) -> [u8; L] {
        let mut key = [0u8; L];
        key[..self.prefix.len()].copy_from_slice(&self.prefix);
        key[self.prefix.len()..].copy_from_slice(self.first_suffix());
        key
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::tenant::block_io::{BlockCursor, BlockLease, BlockReaderRef};
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    pub(crate) struct TestDisk {
        blocks: Vec<Bytes>,
    }
    impl TestDisk {
        fn new() -> Self {
            Self::default()
        }
        pub(crate) fn read_blk(&self, blknum: u32) -> io::Result<BlockLease> {
            let mut buf = [0u8; PAGE_SZ];
            buf.copy_from_slice(&self.blocks[blknum as usize]);
            Ok(std::sync::Arc::new(buf).into())
        }
    }
    impl BlockReader for TestDisk {
        fn block_cursor(&self) -> BlockCursor<'_> {
            BlockCursor::new(BlockReaderRef::TestDisk(self))
        }
    }
    impl BlockWriter for &mut TestDisk {
        fn write_blk(&mut self, buf: Bytes) -> io::Result<u32> {
            let blknum = self.blocks.len();
            self.blocks.push(buf);
            Ok(blknum as u32)
        }
    }

    #[tokio::test]
    async fn basic() -> Result<()> {
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::<_, 6>::new(&mut disk);

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let all_keys: Vec<&[u8; 6]> = vec![
            b"xaaaaa", b"xaaaba", b"xaaaca", b"xabaaa", b"xababa", b"xabaca", b"xabada", b"xabadb",
        ];
        let all_data: Vec<(&[u8; 6], u64)> = all_keys
            .iter()
            .enumerate()
            .map(|(idx, key)| (*key, idx as u64))
            .collect();
        for (key, val) in all_data.iter() {
            writer.append(key, *val)?;
        }

        let (root_offset, _writer) = writer.finish()?;

        let reader = DiskBtreeReader::new(0, root_offset, disk);

        reader.dump().await?;

        // Test the `get` function on all the keys.
        for (key, val) in all_data.iter() {
            assert_eq!(reader.get(key, &ctx).await?, Some(*val));
        }
        // And on some keys that don't exist
        assert_eq!(reader.get(b"aaaaaa", &ctx).await?, None);
        assert_eq!(reader.get(b"zzzzzz", &ctx).await?, None);
        assert_eq!(reader.get(b"xaaabx", &ctx).await?, None);

        // Test search with `visit` function
        let search_key = b"xabaaa";
        let expected: Vec<(Vec<u8>, u64)> = all_data
            .iter()
            .filter(|(key, _value)| key[..] >= search_key[..])
            .map(|(key, value)| (key.to_vec(), *value))
            .collect();

        let mut data = Vec::new();
        reader
            .visit(
                search_key,
                VisitDirection::Forwards,
                |key, value| {
                    data.push((key.to_vec(), value));
                    true
                },
                &ctx,
            )
            .await?;
        assert_eq!(data, expected);

        // Test a backwards scan
        let mut expected: Vec<(Vec<u8>, u64)> = all_data
            .iter()
            .filter(|(key, _value)| key[..] <= search_key[..])
            .map(|(key, value)| (key.to_vec(), *value))
            .collect();
        expected.reverse();
        let mut data = Vec::new();
        reader
            .visit(
                search_key,
                VisitDirection::Backwards,
                |key, value| {
                    data.push((key.to_vec(), value));
                    true
                },
                &ctx,
            )
            .await?;
        assert_eq!(data, expected);

        // Backward scan where nothing matches
        reader
            .visit(
                b"aaaaaa",
                VisitDirection::Backwards,
                |key, value| {
                    panic!("found unexpected key {}: {}", hex::encode(key), value);
                },
                &ctx,
            )
            .await?;

        // Full scan
        let expected: Vec<(Vec<u8>, u64)> = all_data
            .iter()
            .map(|(key, value)| (key.to_vec(), *value))
            .collect();
        let mut data = Vec::new();
        reader
            .visit(
                &[0u8; 6],
                VisitDirection::Forwards,
                |key, value| {
                    data.push((key.to_vec(), value));
                    true
                },
                &ctx,
            )
            .await?;
        assert_eq!(data, expected);

        Ok(())
    }

    #[tokio::test]
    async fn lots_of_keys() -> Result<()> {
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::<_, 8>::new(&mut disk);
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        const NUM_KEYS: u64 = 1000;

        let mut all_data: BTreeMap<u64, u64> = BTreeMap::new();

        for idx in 0..NUM_KEYS {
            let key_int: u64 = 1 + idx * 2;
            let key = u64::to_be_bytes(key_int);
            writer.append(&key, idx)?;

            all_data.insert(key_int, idx);
        }

        let (root_offset, _writer) = writer.finish()?;

        let reader = DiskBtreeReader::new(0, root_offset, disk);

        reader.dump().await?;

        use std::sync::Mutex;

        let result = Mutex::new(Vec::new());
        let limit: AtomicUsize = AtomicUsize::new(10);
        let take_ten = |key: &[u8], value: u64| {
            let mut keybuf = [0u8; 8];
            keybuf.copy_from_slice(key);
            let key_int = u64::from_be_bytes(keybuf);

            let mut result = result.lock().unwrap();
            result.push((key_int, value));

            // keep going until we have 10 matches
            result.len() < limit.load(Ordering::Relaxed)
        };

        for search_key_int in 0..(NUM_KEYS * 2 + 10) {
            let search_key = u64::to_be_bytes(search_key_int);
            assert_eq!(
                reader.get(&search_key, &ctx).await?,
                all_data.get(&search_key_int).cloned()
            );

            // Test a forward scan starting with this key
            result.lock().unwrap().clear();
            reader
                .visit(&search_key, VisitDirection::Forwards, take_ten, &ctx)
                .await?;
            let expected = all_data
                .range(search_key_int..)
                .take(10)
                .map(|(&key, &val)| (key, val))
                .collect::<Vec<(u64, u64)>>();
            assert_eq!(*result.lock().unwrap(), expected);

            // And a backwards scan
            result.lock().unwrap().clear();
            reader
                .visit(&search_key, VisitDirection::Backwards, take_ten, &ctx)
                .await?;
            let expected = all_data
                .range(..=search_key_int)
                .rev()
                .take(10)
                .map(|(&key, &val)| (key, val))
                .collect::<Vec<(u64, u64)>>();
            assert_eq!(*result.lock().unwrap(), expected);
        }

        // full scan
        let search_key = u64::to_be_bytes(0);
        limit.store(usize::MAX, Ordering::Relaxed);
        result.lock().unwrap().clear();
        reader
            .visit(&search_key, VisitDirection::Forwards, take_ten, &ctx)
            .await?;
        let expected = all_data
            .iter()
            .map(|(&key, &val)| (key, val))
            .collect::<Vec<(u64, u64)>>();
        assert_eq!(*result.lock().unwrap(), expected);

        // full scan
        let search_key = u64::to_be_bytes(u64::MAX);
        limit.store(usize::MAX, Ordering::Relaxed);
        result.lock().unwrap().clear();
        reader
            .visit(&search_key, VisitDirection::Backwards, take_ten, &ctx)
            .await?;
        let expected = all_data
            .iter()
            .rev()
            .map(|(&key, &val)| (key, val))
            .collect::<Vec<(u64, u64)>>();
        assert_eq!(*result.lock().unwrap(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn random_data() -> Result<()> {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        // Generate random keys with exponential distribution, to
        // exercise the prefix compression
        const NUM_KEYS: usize = 100000;
        let mut all_data: BTreeMap<u128, u64> = BTreeMap::new();
        for idx in 0..NUM_KEYS {
            let u: f64 = rand::thread_rng().gen_range(0.0..1.0);
            let t = -(f64::ln(u));
            let key_int = (t * 1000000.0) as u128;

            all_data.insert(key_int, idx as u64);
        }

        // Build a tree from it
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::<_, 16>::new(&mut disk);

        for (&key, &val) in all_data.iter() {
            writer.append(&u128::to_be_bytes(key), val)?;
        }
        let (root_offset, _writer) = writer.finish()?;

        let reader = DiskBtreeReader::new(0, root_offset, disk);

        // Test get() operation on all the keys
        for (&key, &val) in all_data.iter() {
            let search_key = u128::to_be_bytes(key);
            assert_eq!(reader.get(&search_key, &ctx).await?, Some(val));
        }

        // Test get() operations on random keys, most of which will not exist
        for _ in 0..100000 {
            let key_int = rand::thread_rng().gen::<u128>();
            let search_key = u128::to_be_bytes(key_int);
            assert!(reader.get(&search_key, &ctx).await? == all_data.get(&key_int).cloned());
        }

        // Test boundary cases
        assert!(
            reader.get(&u128::to_be_bytes(u128::MIN), &ctx).await?
                == all_data.get(&u128::MIN).cloned()
        );
        assert!(
            reader.get(&u128::to_be_bytes(u128::MAX), &ctx).await?
                == all_data.get(&u128::MAX).cloned()
        );

        // Test iterator and get_stream API
        let mut iter = reader.iter(&[0; 16], &ctx);
        let mut cnt = 0;
        while let Some(res) = iter.next().await {
            let (key, val) = res?;
            let key = u128::from_be_bytes(key.as_slice().try_into().unwrap());
            assert_eq!(val, *all_data.get(&key).unwrap());
            cnt += 1;
        }
        assert_eq!(cnt, all_data.len());

        Ok(())
    }

    #[test]
    fn unsorted_input() {
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::<_, 2>::new(&mut disk);

        let _ = writer.append(b"ba", 1);
        let _ = writer.append(b"bb", 2);
        let err = writer.append(b"aa", 3).expect_err("should've failed");
        match err {
            DiskBtreeError::UnsortedInput { key, last_key } => {
                assert_eq!(key.as_ref(), b"aa".as_slice());
                assert_eq!(last_key.as_ref(), b"bb".as_slice());
            }
            _ => panic!("unexpected error variant, expected DiskBtreeError::UnsortedInput"),
        }
    }

    ///
    /// This test contains a particular data set, see disk_btree_test_data.rs
    ///
    #[tokio::test]
    async fn particular_data() -> Result<()> {
        // Build a tree from it
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::<_, 26>::new(&mut disk);
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        for (key, val) in disk_btree_test_data::TEST_DATA {
            writer.append(&key, val)?;
        }
        let (root_offset, writer) = writer.finish()?;

        println!("SIZE: {} blocks", writer.blocks.len());

        let reader = DiskBtreeReader::new(0, root_offset, disk);

        // Test get() operation on all the keys
        for (key, val) in disk_btree_test_data::TEST_DATA {
            assert_eq!(reader.get(&key, &ctx).await?, Some(val));
        }

        // Test full scan
        let mut count = 0;
        reader
            .visit(
                &[0u8; 26],
                VisitDirection::Forwards,
                |_key, _value| {
                    count += 1;
                    true
                },
                &ctx,
            )
            .await?;
        assert_eq!(count, disk_btree_test_data::TEST_DATA.len());

        reader.dump().await?;

        Ok(())
    }
}

#[cfg(test)]
#[path = "disk_btree_test_data.rs"]
mod disk_btree_test_data;
