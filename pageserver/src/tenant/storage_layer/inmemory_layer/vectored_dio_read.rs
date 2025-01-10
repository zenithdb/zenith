use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use itertools::Itertools;
use tokio_epoll_uring::{BoundedBuf, IoBufMut, Slice};

use crate::{
    assert_u64_eq_usize::{U64IsUsize, UsizeIsU64},
    context::RequestContext,
    virtual_file::{owned_buffers_io::io_buf_aligned::IoBufAlignedMut, IoBufferMut},
};

/// The file interface we require. At runtime, this is a [`crate::tenant::ephemeral_file::EphemeralFile`].
pub trait File: Send {
    /// Attempt to read the bytes in `self` in range `[start,start+dst.bytes_total())`
    /// and return the number of bytes read (let's call it `nread`).
    /// The bytes read are placed in `dst`, i.e., `&dst[..nread]` will contain the read bytes.
    ///
    /// The only reason why the read may be short (i.e., `nread != dst.bytes_total()`)
    /// is if the file is shorter than `start+dst.len()`.
    ///
    /// This is unlike [`std::os::unix::fs::FileExt::read_exact_at`] which returns an
    /// [`std::io::ErrorKind::UnexpectedEof`] error if the file is shorter than `start+dst.len()`.
    ///
    /// No guarantees are made about the remaining bytes in `dst` in case of a short read.
    async fn read_exact_at_eof_ok<B: IoBufAlignedMut + Send>(
        &self,
        start: u64,
        dst: Slice<B>,
        ctx: &RequestContext,
    ) -> std::io::Result<(Slice<B>, usize)>;
}

/// A logical read from [`File`]. See [`Self::new`].
pub struct LogicalRead<B: Buffer> {
    pos: u64,
    state: RwLockRefCell<LogicalReadState<B>>,
}

enum LogicalReadState<B: Buffer> {
    NotStarted(B),
    Ongoing(B),
    Ok(B),
    Error(Arc<std::io::Error>),
    Undefined,
}

impl<B: Buffer> LogicalRead<B> {
    /// Create a new [`LogicalRead`] from [`File`] of the data in the file in range `[ pos, pos + buf.cap() )`.
    pub fn new(pos: u64, buf: B) -> Self {
        Self {
            pos,
            state: RwLockRefCell::new(LogicalReadState::NotStarted(buf)),
        }
    }
    pub fn into_result(self) -> Option<Result<B, Arc<std::io::Error>>> {
        match self.state.into_inner() {
            LogicalReadState::Ok(buf) => Some(Ok(buf)),
            LogicalReadState::Error(e) => Some(Err(e)),
            LogicalReadState::NotStarted(_) | LogicalReadState::Ongoing(_) => None,
            LogicalReadState::Undefined => unreachable!(),
        }
    }
}

/// The buffer into which a [`LogicalRead`] result is placed.
pub trait Buffer: std::ops::Deref<Target = [u8]> {
    /// Immutable.
    fn cap(&self) -> usize;
    /// Changes only through [`Self::extend_from_slice`].
    fn len(&self) -> usize;
    /// Panics if the total length would exceed the initialized capacity.
    fn extend_from_slice(&mut self, src: &[u8]);
}

/// The minimum alignment and size requirement for disk offsets and memory buffer size for direct IO.
const DIO_CHUNK_SIZE: usize = 512;

/// If multiple chunks need to be read, merge adjacent chunk reads into batches of max size `MAX_CHUNK_BATCH_SIZE`.
/// (The unit is the number of chunks.)
const MAX_CHUNK_BATCH_SIZE: usize = {
    let desired = 128 * 1024; // 128k
    if desired % DIO_CHUNK_SIZE != 0 {
        panic!("MAX_CHUNK_BATCH_SIZE must be a multiple of DIO_CHUNK_SIZE")
        // compile-time error
    }
    desired / DIO_CHUNK_SIZE
};

/// Execute the given logical `reads` against `file`.
/// The results are placed in the buffers of the [`LogicalRead`]s.
/// Retrieve the results by calling [`LogicalRead::into_result`] on each [`LogicalRead`].
///
/// The [`LogicalRead`]s must be freshly created using [`LogicalRead::new`] when calling this function.
/// Otherwise, this function panics.
pub async fn execute<'a, I, F, B>(file: &F, reads: I, ctx: &RequestContext)
where
    I: IntoIterator<Item = &'a LogicalRead<B>>,
    F: File,
    B: Buffer + IoBufMut + Send,
{
    // Terminology:
    // logical read = a request to read an arbitrary range of bytes from `file`; byte-level granularity
    // chunk = we conceptually divide up the byte range of `file` into DIO_CHUNK_SIZEs ranges
    // interest = a range within a chunk that a logical read is interested in; one logical read gets turned into many interests
    // physical read = the read request we're going to issue to the OS; covers a range of chunks; chunk-level granularity

    // Preserve a copy of the logical reads for debug assertions at the end
    #[cfg(debug_assertions)]
    let (reads, assert_logical_reads) = {
        let (reads, assert) = reads.into_iter().tee();
        (reads, Some(Vec::from_iter(assert)))
    };
    #[cfg(not(debug_assertions))]
    let (reads, assert_logical_reads): (_, Option<Vec<&'a LogicalRead<B>>>) = (reads, None);

    // Plan which parts of which chunks need to be appended to which buffer
    let mut by_chunk: BTreeMap<u64, Vec<Interest<B>>> = BTreeMap::new();
    struct Interest<'a, B: Buffer> {
        logical_read: &'a LogicalRead<B>,
        offset_in_chunk: u64,
        len: u64,
    }
    for logical_read in reads {
        let LogicalRead { pos, state } = logical_read;
        let mut state = state.borrow_mut();

        // transition from NotStarted to Ongoing
        let cur = std::mem::replace(&mut *state, LogicalReadState::Undefined);
        let req_len = match cur {
            LogicalReadState::NotStarted(buf) => {
                if buf.len() != 0 {
                    panic!("The `LogicalRead`s that are passed in must be freshly created using `LogicalRead::new`");
                }
                // buf.cap() == 0 is ok

                // transition into Ongoing state
                let req_len = buf.cap();
                *state = LogicalReadState::Ongoing(buf);
                req_len
            }
            x => panic!("must only call with fresh LogicalReads, got another state, leaving Undefined state behind state={x:?}"),
        };

        // plan which chunks we need to read from
        let mut remaining = req_len;
        let mut chunk_no = *pos / (DIO_CHUNK_SIZE.into_u64());
        let mut offset_in_chunk = pos.into_usize() % DIO_CHUNK_SIZE;
        while remaining > 0 {
            let remaining_in_chunk = std::cmp::min(remaining, DIO_CHUNK_SIZE - offset_in_chunk);
            by_chunk.entry(chunk_no).or_default().push(Interest {
                logical_read,
                offset_in_chunk: offset_in_chunk.into_u64(),
                len: remaining_in_chunk.into_u64(),
            });
            offset_in_chunk = 0;
            chunk_no += 1;
            remaining -= remaining_in_chunk;
        }
    }

    // At this point, we could iterate over by_chunk, in chunk order,
    // read each chunk from disk, and fill the buffers.
    // However, we can merge adjacent chunks into batches of MAX_CHUNK_BATCH_SIZE
    // so we issue fewer IOs = fewer roundtrips = lower overall latency.
    struct PhysicalRead<'a, B: Buffer> {
        start_chunk_no: u64,
        nchunks: usize,
        dsts: Vec<PhysicalInterest<'a, B>>,
    }
    struct PhysicalInterest<'a, B: Buffer> {
        logical_read: &'a LogicalRead<B>,
        offset_in_physical_read: u64,
        len: u64,
    }
    let mut physical_reads: Vec<PhysicalRead<B>> = Vec::new();
    let mut by_chunk = by_chunk.into_iter().peekable();
    loop {
        let mut last_chunk_no = None;
        let to_merge: Vec<(u64, Vec<Interest<B>>)> = by_chunk
            .peeking_take_while(|(chunk_no, _)| {
                if let Some(last_chunk_no) = last_chunk_no {
                    if *chunk_no != last_chunk_no + 1 {
                        return false;
                    }
                }
                last_chunk_no = Some(*chunk_no);
                true
            })
            .take(MAX_CHUNK_BATCH_SIZE)
            .collect(); // TODO: avoid this .collect()
        let Some(start_chunk_no) = to_merge.first().map(|(chunk_no, _)| *chunk_no) else {
            break;
        };
        let nchunks = to_merge.len();
        let dsts = to_merge
            .into_iter()
            .enumerate()
            .flat_map(|(i, (_, dsts))| {
                dsts.into_iter().map(
                    move |Interest {
                              logical_read,
                              offset_in_chunk,
                              len,
                          }| {
                        PhysicalInterest {
                            logical_read,
                            offset_in_physical_read: i
                                .checked_mul(DIO_CHUNK_SIZE)
                                .unwrap()
                                .into_u64()
                                + offset_in_chunk,
                            len,
                        }
                    },
                )
            })
            .collect();
        physical_reads.push(PhysicalRead {
            start_chunk_no,
            nchunks,
            dsts,
        });
    }
    drop(by_chunk);

    // Execute physical reads and fill the logical read buffers
    // TODO: pipelined reads; prefetch;
    let get_io_buffer = |nchunks| IoBufferMut::with_capacity(nchunks * DIO_CHUNK_SIZE);
    for PhysicalRead {
        start_chunk_no,
        nchunks,
        dsts,
    } in physical_reads
    {
        let all_done = dsts
            .iter()
            .all(|PhysicalInterest { logical_read, .. }| logical_read.state.borrow().is_terminal());
        if all_done {
            continue;
        }
        let read_offset = start_chunk_no
            .checked_mul(DIO_CHUNK_SIZE.into_u64())
            .expect("we produce chunk_nos by dividing by DIO_CHUNK_SIZE earlier");
        let io_buf = get_io_buffer(nchunks).slice_full();
        let req_len = io_buf.len();
        let (io_buf_slice, nread) = match file.read_exact_at_eof_ok(read_offset, io_buf, ctx).await
        {
            Ok(t) => t,
            Err(e) => {
                let e = Arc::new(e);
                for PhysicalInterest { logical_read, .. } in dsts {
                    *logical_read.state.borrow_mut() = LogicalReadState::Error(Arc::clone(&e));
                    // this will make later reads for the given LogicalRead short-circuit, see top of loop body
                }
                continue;
            }
        };
        let io_buf = io_buf_slice.into_inner();
        assert!(
            nread <= io_buf.len(),
            "the last chunk in the file can be a short read, so, no =="
        );
        let io_buf = &io_buf[..nread];
        for PhysicalInterest {
            logical_read,
            offset_in_physical_read,
            len,
        } in dsts
        {
            let mut logical_read_state_borrow = logical_read.state.borrow_mut();
            let logical_read_buf = match &mut *logical_read_state_borrow {
                LogicalReadState::NotStarted(_) => {
                    unreachable!("we transition it into Ongoing at function entry")
                }
                LogicalReadState::Ongoing(buf) => buf,
                LogicalReadState::Ok(_) | LogicalReadState::Error(_) => {
                    continue;
                }
                LogicalReadState::Undefined => unreachable!(),
            };
            let range_in_io_buf = std::ops::Range {
                start: offset_in_physical_read as usize,
                end: offset_in_physical_read as usize + len as usize,
            };
            assert!(range_in_io_buf.end >= range_in_io_buf.start);
            if range_in_io_buf.end > nread {
                let msg = format!(
                    "physical read returned EOF where this logical read expected more data in the file: offset=0x{read_offset:x} req_len=0x{req_len:x} nread=0x{nread:x} {:?}",
                    &*logical_read_state_borrow
                );
                logical_read_state_borrow.transition_to_terminal(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    msg,
                )));
                continue;
            }
            let data = &io_buf[range_in_io_buf];

            // Copy data from io buffer into the logical read buffer.
            // (And in debug mode, validate that the buffer impl adheres to the Buffer trait spec.)
            let pre = if cfg!(debug_assertions) {
                Some((logical_read_buf.len(), logical_read_buf.cap()))
            } else {
                None
            };
            logical_read_buf.extend_from_slice(data);
            let post = if cfg!(debug_assertions) {
                Some((logical_read_buf.len(), logical_read_buf.cap()))
            } else {
                None
            };
            match (pre, post) {
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => unreachable!(),
                (Some((pre_len, pre_cap)), Some((post_len, post_cap))) => {
                    assert_eq!(pre_len + len as usize, post_len);
                    assert_eq!(pre_cap, post_cap);
                }
            }

            if logical_read_buf.len() == logical_read_buf.cap() {
                logical_read_state_borrow.transition_to_terminal(Ok(()));
            }
        }
    }

    if let Some(assert_logical_reads) = assert_logical_reads {
        for logical_read in assert_logical_reads {
            assert!(logical_read.state.borrow().is_terminal());
        }
    }
}

impl<B: Buffer> LogicalReadState<B> {
    fn is_terminal(&self) -> bool {
        match self {
            LogicalReadState::NotStarted(_) | LogicalReadState::Ongoing(_) => false,
            LogicalReadState::Ok(_) | LogicalReadState::Error(_) => true,
            LogicalReadState::Undefined => unreachable!(),
        }
    }
    fn transition_to_terminal(&mut self, err: std::io::Result<()>) {
        let cur = std::mem::replace(self, LogicalReadState::Undefined);
        let buf = match cur {
            LogicalReadState::Ongoing(buf) => buf,
            x => panic!("must only call in state Ongoing, got {x:?}"),
        };
        *self = match err {
            Ok(()) => LogicalReadState::Ok(buf),
            Err(e) => LogicalReadState::Error(Arc::new(e)),
        };
    }
}

impl<B: Buffer> std::fmt::Debug for LogicalReadState<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        #[allow(unused)]
        struct BufferDebug {
            len: usize,
            cap: usize,
        }
        impl<'a> From<&'a dyn Buffer> for BufferDebug {
            fn from(buf: &'a dyn Buffer) -> Self {
                Self {
                    len: buf.len(),
                    cap: buf.cap(),
                }
            }
        }
        match self {
            LogicalReadState::NotStarted(b) => {
                write!(f, "NotStarted({:?})", BufferDebug::from(b as &dyn Buffer))
            }
            LogicalReadState::Ongoing(b) => {
                write!(f, "Ongoing({:?})", BufferDebug::from(b as &dyn Buffer))
            }
            LogicalReadState::Ok(b) => write!(f, "Ok({:?})", BufferDebug::from(b as &dyn Buffer)),
            LogicalReadState::Error(e) => write!(f, "Error({:?})", e),
            LogicalReadState::Undefined => write!(f, "Undefined"),
        }
    }
}

#[derive(Debug)]
struct RwLockRefCell<T>(RwLock<T>);
impl<T> RwLockRefCell<T> {
    fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }
    fn borrow(&self) -> impl std::ops::Deref<Target = T> + '_ {
        self.0.try_read().unwrap()
    }
    fn borrow_mut(&self) -> impl std::ops::DerefMut<Target = T> + '_ {
        self.0.try_write().unwrap()
    }
    fn into_inner(self) -> T {
        self.0.into_inner().unwrap()
    }
}

impl Buffer for Vec<u8> {
    fn cap(&self) -> usize {
        self.capacity()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        if self.len() + src.len() > self.cap() {
            panic!("Buffer capacity exceeded");
        }
        Vec::extend_from_slice(self, src);
    }
}

#[cfg(test)]
#[allow(clippy::assertions_on_constants)]
mod tests {
    use rand::Rng;

    use crate::{
        context::DownloadBehavior, task_mgr::TaskKind,
        virtual_file::owned_buffers_io::slice::SliceMutExt,
    };

    use super::*;
    use std::{cell::RefCell, collections::VecDeque};

    struct InMemoryFile {
        content: Vec<u8>,
    }

    impl InMemoryFile {
        fn new_random(len: usize) -> Self {
            Self {
                content: rand::thread_rng()
                    .sample_iter(rand::distributions::Standard)
                    .take(len)
                    .collect(),
            }
        }
        fn test_logical_read(&self, pos: u64, len: usize) -> TestLogicalRead {
            let expected_result = if pos as usize + len > self.content.len() {
                Err("InMemoryFile short read".to_string())
            } else {
                Ok(self.content[pos as usize..pos as usize + len].to_vec())
            };
            TestLogicalRead::new(pos, len, expected_result)
        }
    }

    #[test]
    fn test_in_memory_file() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let file = InMemoryFile::new_random(10);
        let test_read = |pos, len| {
            let buf = IoBufferMut::with_capacity_zeroed(len);
            let fut = file.read_exact_at_eof_ok(pos, buf.slice_full(), &ctx);
            use futures::FutureExt;
            let (slice, nread) = fut
                .now_or_never()
                .expect("impl never awaits")
                .expect("impl never errors");
            let mut buf = slice.into_inner();
            buf.truncate(nread);
            buf
        };
        assert_eq!(&test_read(0, 1), &file.content[0..1]);
        assert_eq!(&test_read(1, 2), &file.content[1..3]);
        assert_eq!(&test_read(9, 2), &file.content[9..]);
        assert!(test_read(10, 2).is_empty());
        assert!(test_read(11, 2).is_empty());
    }

    impl File for InMemoryFile {
        async fn read_exact_at_eof_ok<B: IoBufMut + Send>(
            &self,
            start: u64,
            mut dst: Slice<B>,
            _ctx: &RequestContext,
        ) -> std::io::Result<(Slice<B>, usize)> {
            let dst_slice: &mut [u8] = dst.as_mut_rust_slice_full_zeroed();
            let nread = {
                let req_len = dst_slice.len();
                let len = std::cmp::min(req_len, self.content.len().saturating_sub(start as usize));
                if start as usize >= self.content.len() {
                    0
                } else {
                    dst_slice[..len]
                        .copy_from_slice(&self.content[start as usize..start as usize + len]);
                    len
                }
            };
            rand::Rng::fill(&mut rand::thread_rng(), &mut dst_slice[nread..]); // to discover bugs
            Ok((dst, nread))
        }
    }

    #[derive(Clone)]
    struct TestLogicalRead {
        pos: u64,
        len: usize,
        expected_result: Result<Vec<u8>, String>,
    }

    impl TestLogicalRead {
        fn new(pos: u64, len: usize, expected_result: Result<Vec<u8>, String>) -> Self {
            Self {
                pos,
                len,
                expected_result,
            }
        }
        fn make_logical_read(&self) -> LogicalRead<Vec<u8>> {
            LogicalRead::new(self.pos, Vec::with_capacity(self.len))
        }
    }

    async fn execute_and_validate_test_logical_reads<I, F>(
        file: &F,
        test_logical_reads: I,
        ctx: &RequestContext,
    ) where
        I: IntoIterator<Item = TestLogicalRead>,
        F: File,
    {
        let (tmp, test_logical_reads) = test_logical_reads.into_iter().tee();
        let logical_reads = tmp.map(|tr| tr.make_logical_read()).collect::<Vec<_>>();
        execute(file, logical_reads.iter(), ctx).await;
        for (logical_read, test_logical_read) in logical_reads.into_iter().zip(test_logical_reads) {
            let actual = logical_read.into_result().expect("we call execute()");
            match (actual, test_logical_read.expected_result) {
                (Ok(actual), Ok(expected)) if actual == expected => {}
                (Err(actual), Err(expected)) => {
                    assert_eq!(actual.to_string(), expected);
                }
                (actual, expected) => panic!("expected {expected:?}\nactual {actual:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_blackbox() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let cs = DIO_CHUNK_SIZE;
        let cs_u64 = cs.into_u64();

        let file = InMemoryFile::new_random(10 * cs);

        let test_logical_reads = vec![
            file.test_logical_read(0, 1),
            // adjacent to logical_read0
            file.test_logical_read(1, 2),
            // gap
            // spans adjacent chunks
            file.test_logical_read(cs_u64 - 1, 2),
            // gap
            //  tail of chunk 3, all of chunk 4, and 2 bytes of chunk 5
            file.test_logical_read(3 * cs_u64 - 1, cs + 2),
            // gap
            file.test_logical_read(5 * cs_u64, 1),
        ];
        let num_test_logical_reads = test_logical_reads.len();
        let test_logical_reads_perms = test_logical_reads
            .into_iter()
            .permutations(num_test_logical_reads);

        // test all orderings of LogicalReads, the order shouldn't matter for the results
        for test_logical_reads in test_logical_reads_perms {
            execute_and_validate_test_logical_reads(&file, test_logical_reads, &ctx).await;
        }
    }

    #[tokio::test]
    #[should_panic]
    async fn test_reusing_logical_reads_panics() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let file = InMemoryFile::new_random(DIO_CHUNK_SIZE);
        let a = file.test_logical_read(23, 10);
        let logical_reads = vec![a.make_logical_read()];
        execute(&file, &logical_reads, &ctx).await;
        // reuse pancis
        execute(&file, &logical_reads, &ctx).await;
    }

    struct RecorderFile<'a> {
        recorded: RefCell<Vec<RecordedRead>>,
        file: &'a InMemoryFile,
    }

    struct RecordedRead {
        pos: u64,
        req_len: usize,
        res: Vec<u8>,
    }

    impl<'a> RecorderFile<'a> {
        fn new(file: &'a InMemoryFile) -> RecorderFile<'a> {
            Self {
                recorded: Default::default(),
                file,
            }
        }
    }

    impl File for RecorderFile<'_> {
        async fn read_exact_at_eof_ok<B: IoBufAlignedMut + Send>(
            &self,
            start: u64,
            dst: Slice<B>,
            ctx: &RequestContext,
        ) -> std::io::Result<(Slice<B>, usize)> {
            let (dst, nread) = self.file.read_exact_at_eof_ok(start, dst, ctx).await?;
            self.recorded.borrow_mut().push(RecordedRead {
                pos: start,
                req_len: dst.bytes_total(),
                res: Vec::from(&dst[..nread]),
            });
            Ok((dst, nread))
        }
    }

    #[tokio::test]
    async fn test_logical_reads_to_same_chunk_are_merged_into_one_chunk_read() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let file = InMemoryFile::new_random(2 * DIO_CHUNK_SIZE);

        let a = file.test_logical_read(DIO_CHUNK_SIZE.into_u64(), 10);
        let b = file.test_logical_read(DIO_CHUNK_SIZE.into_u64() + 30, 20);

        let recorder = RecorderFile::new(&file);

        execute_and_validate_test_logical_reads(&recorder, vec![a, b], &ctx).await;

        let recorded = recorder.recorded.borrow();
        assert_eq!(recorded.len(), 1);
        let RecordedRead { pos, req_len, .. } = &recorded[0];
        assert_eq!(*pos, DIO_CHUNK_SIZE.into_u64());
        assert_eq!(*req_len, DIO_CHUNK_SIZE);
    }

    #[tokio::test]
    async fn test_max_chunk_batch_size_is_respected() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let file = InMemoryFile::new_random(4 * MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE);

        // read the 10th byte of each chunk 3 .. 3+2*MAX_CHUNK_BATCH_SIZE
        assert!(3 < MAX_CHUNK_BATCH_SIZE, "test assumption");
        assert!(10 < DIO_CHUNK_SIZE, "test assumption");
        let mut test_logical_reads = Vec::new();
        for i in 3..3 + MAX_CHUNK_BATCH_SIZE + MAX_CHUNK_BATCH_SIZE / 2 {
            test_logical_reads
                .push(file.test_logical_read(i.into_u64() * DIO_CHUNK_SIZE.into_u64() + 10, 1));
        }

        let recorder = RecorderFile::new(&file);

        execute_and_validate_test_logical_reads(&recorder, test_logical_reads, &ctx).await;

        let recorded = recorder.recorded.borrow();
        assert_eq!(recorded.len(), 2);
        {
            let RecordedRead { pos, req_len, .. } = &recorded[0];
            assert_eq!(*pos as usize, 3 * DIO_CHUNK_SIZE);
            assert_eq!(*req_len, MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE);
        }
        {
            let RecordedRead { pos, req_len, .. } = &recorded[1];
            assert_eq!(*pos as usize, (3 + MAX_CHUNK_BATCH_SIZE) * DIO_CHUNK_SIZE);
            assert_eq!(*req_len, MAX_CHUNK_BATCH_SIZE / 2 * DIO_CHUNK_SIZE);
        }
    }

    #[tokio::test]
    async fn test_batch_breaks_if_chunk_is_not_interesting() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        assert!(MAX_CHUNK_BATCH_SIZE > 10, "test assumption");
        let file = InMemoryFile::new_random(3 * DIO_CHUNK_SIZE);

        let a = file.test_logical_read(0, 1); // chunk 0
        let b = file.test_logical_read(2 * DIO_CHUNK_SIZE.into_u64(), 1); // chunk 2

        let recorder = RecorderFile::new(&file);

        execute_and_validate_test_logical_reads(&recorder, vec![a, b], &ctx).await;

        let recorded = recorder.recorded.borrow();

        assert_eq!(recorded.len(), 2);
        {
            let RecordedRead { pos, req_len, .. } = &recorded[0];
            assert_eq!(*pos, 0);
            assert_eq!(*req_len, DIO_CHUNK_SIZE);
        }
        {
            let RecordedRead { pos, req_len, .. } = &recorded[1];
            assert_eq!(*pos, 2 * DIO_CHUNK_SIZE.into_u64());
            assert_eq!(*req_len, DIO_CHUNK_SIZE);
        }
    }

    struct ExpectedRead {
        expect_pos: u64,
        expect_len: usize,
        respond: Result<Vec<u8>, String>,
    }

    struct MockFile {
        expected: RefCell<VecDeque<ExpectedRead>>,
    }

    impl Drop for MockFile {
        fn drop(&mut self) {
            assert!(
                self.expected.borrow().is_empty(),
                "expected reads not satisfied"
            );
        }
    }

    macro_rules! mock_file {
        ($($pos:expr , $len:expr => $respond:expr),* $(,)?) => {{
            MockFile {
                expected: RefCell::new(VecDeque::from(vec![$(ExpectedRead {
                    expect_pos: $pos,
                    expect_len: $len,
                    respond: $respond,
                }),*])),
            }
        }};
    }

    impl File for MockFile {
        async fn read_exact_at_eof_ok<B: IoBufMut + Send>(
            &self,
            start: u64,
            mut dst: Slice<B>,
            _ctx: &RequestContext,
        ) -> std::io::Result<(Slice<B>, usize)> {
            let ExpectedRead {
                expect_pos,
                expect_len,
                respond,
            } = self
                .expected
                .borrow_mut()
                .pop_front()
                .expect("unexpected read");
            assert_eq!(start, expect_pos);
            assert_eq!(dst.bytes_total(), expect_len);
            match respond {
                Ok(mocked_bytes) => {
                    let len = std::cmp::min(dst.bytes_total(), mocked_bytes.len());
                    let dst_slice: &mut [u8] = dst.as_mut_rust_slice_full_zeroed();
                    dst_slice[..len].copy_from_slice(&mocked_bytes[..len]);
                    rand::Rng::fill(&mut rand::thread_rng(), &mut dst_slice[len..]); // to discover bugs
                    Ok((dst, len))
                }
                Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
            }
        }
    }

    #[tokio::test]
    async fn test_mock_file() {
        // Self-test to ensure the relevant features of mock file work as expected.

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let mock_file = mock_file! {
            0    , 512 => Ok(vec![0; 512]),
            512  , 512 => Ok(vec![1; 512]),
            1024 , 512 => Ok(vec![2; 10]),
            2048,  1024 => Err("foo".to_owned()),
        };

        let buf = IoBufferMut::with_capacity(512);
        let (buf, nread) = mock_file
            .read_exact_at_eof_ok(0, buf.slice_full(), &ctx)
            .await
            .unwrap();
        assert_eq!(nread, 512);
        assert_eq!(&buf.into_inner()[..nread], &[0; 512]);

        let buf = IoBufferMut::with_capacity(512);
        let (buf, nread) = mock_file
            .read_exact_at_eof_ok(512, buf.slice_full(), &ctx)
            .await
            .unwrap();
        assert_eq!(nread, 512);
        assert_eq!(&buf.into_inner()[..nread], &[1; 512]);

        let buf = IoBufferMut::with_capacity(512);
        let (buf, nread) = mock_file
            .read_exact_at_eof_ok(1024, buf.slice_full(), &ctx)
            .await
            .unwrap();
        assert_eq!(nread, 10);
        assert_eq!(&buf.into_inner()[..nread], &[2; 10]);

        let buf = IoBufferMut::with_capacity(1024);
        let err = mock_file
            .read_exact_at_eof_ok(2048, buf.slice_full(), &ctx)
            .await
            .err()
            .unwrap();
        assert_eq!(err.to_string(), "foo");
    }

    #[tokio::test]
    async fn test_error_on_one_chunk_read_fails_only_dependent_logical_reads() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let test_logical_reads = vec![
            // read spanning two batches
            TestLogicalRead::new(
                DIO_CHUNK_SIZE.into_u64() / 2,
                MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE,
                Err("foo".to_owned()),
            ),
            // second read in failing chunk
            TestLogicalRead::new(
                (MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE).into_u64() + DIO_CHUNK_SIZE.into_u64() - 10,
                5,
                Err("foo".to_owned()),
            ),
            // read unaffected
            TestLogicalRead::new(
                (MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE).into_u64()
                    + 2 * DIO_CHUNK_SIZE.into_u64()
                    + 10,
                5,
                Ok(vec![1; 5]),
            ),
        ];
        let (tmp, test_logical_reads) = test_logical_reads.into_iter().tee();
        let test_logical_read_perms = tmp.permutations(test_logical_reads.len());

        for test_logical_reads in test_logical_read_perms {
            let file = mock_file!(
                0, MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE => Ok(vec![0; MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE]),
                (MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE).into_u64(), DIO_CHUNK_SIZE => Err("foo".to_owned()),
                (MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE + 2*DIO_CHUNK_SIZE).into_u64(), DIO_CHUNK_SIZE => Ok(vec![1; DIO_CHUNK_SIZE]),
            );
            execute_and_validate_test_logical_reads(&file, test_logical_reads, &ctx).await;
        }
    }

    struct TestShortReadsSetup {
        ctx: RequestContext,
        file: InMemoryFile,
        written: u64,
    }
    fn setup_short_chunk_read_tests() -> TestShortReadsSetup {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        assert!(DIO_CHUNK_SIZE > 20, "test assumption");
        let written = (2 * DIO_CHUNK_SIZE - 10).into_u64();
        let file = InMemoryFile::new_random(written as usize);
        TestShortReadsSetup { ctx, file, written }
    }

    #[tokio::test]
    async fn test_short_chunk_read_from_written_range() {
        // Test what happens if there are logical reads
        // that start within the last chunk, and
        // the last chunk is not the full chunk length.
        //
        // The read should succeed despite the short chunk length.
        let TestShortReadsSetup { ctx, file, written } = setup_short_chunk_read_tests();

        let a = file.test_logical_read(written - 10, 5);
        let recorder = RecorderFile::new(&file);

        execute_and_validate_test_logical_reads(&recorder, vec![a], &ctx).await;

        let recorded = recorder.recorded.borrow();
        assert_eq!(recorded.len(), 1);
        let RecordedRead { pos, req_len, res } = &recorded[0];
        assert_eq!(*pos, DIO_CHUNK_SIZE.into_u64());
        assert_eq!(*req_len, DIO_CHUNK_SIZE);
        assert_eq!(res, &file.content[DIO_CHUNK_SIZE..(written as usize)]);
    }

    #[tokio::test]
    async fn test_short_chunk_read_and_logical_read_from_unwritten_range() {
        // Test what happens if there are logical reads
        // that start within the last chunk, and
        // the last chunk is not the full chunk length, and
        // the logical reads end in the unwritten range.
        //
        // All should fail with UnexpectedEof and have the same IO pattern.
        async fn the_impl(offset_delta: i64) {
            let TestShortReadsSetup { ctx, file, written } = setup_short_chunk_read_tests();

            let offset = u64::try_from(
                i64::try_from(written)
                    .unwrap()
                    .checked_add(offset_delta)
                    .unwrap(),
            )
            .unwrap();
            let a = file.test_logical_read(offset, 5);
            let recorder = RecorderFile::new(&file);
            let a_vr = a.make_logical_read();
            execute(&recorder, vec![&a_vr], &ctx).await;

            // validate the LogicalRead result
            let a_res = a_vr.into_result().unwrap();
            let a_err = a_res.unwrap_err();
            assert_eq!(a_err.kind(), std::io::ErrorKind::UnexpectedEof);

            // validate the IO pattern
            let recorded = recorder.recorded.borrow();
            assert_eq!(recorded.len(), 1);
            let RecordedRead { pos, req_len, res } = &recorded[0];
            assert_eq!(*pos, DIO_CHUNK_SIZE.into_u64());
            assert_eq!(*req_len, DIO_CHUNK_SIZE);
            assert_eq!(res, &file.content[DIO_CHUNK_SIZE..(written as usize)]);
        }

        the_impl(-1).await; // start == length - 1
        the_impl(0).await; // start == length
        the_impl(1).await; // start == length + 1
    }

    // TODO: mixed: some valid, some UnexpectedEof

    // TODO: same tests but with merges
}
