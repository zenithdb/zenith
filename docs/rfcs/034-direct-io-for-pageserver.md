# Direct IO For Pageserver

## Summary

This document is a proposal and implementation plan for direct IO in Pageserver.

## Terminology / Glossary

**kernel page cache**: the kernel's page cache is a write-back cache for filesystem contents.
The cached unit is memory-page-sized & aligned chunks of the files that are being cached (typically 4k).
The cache lives in kernel memory and is not directly accessible through userspace.

**Buffered IO**: the application's read/write system calls go through the kernel page cache.
For example, a 10 byte sized read or write to offset 5000 in a file will load the file contents
at offset `[4096,8192)` into a free page in the kernel page cache. If necessary, it will evict
other pages to make room (cf eviction). Then, the kernel performs a memory-to-memory copy of 10 bytes
from/to the offset `4` (`5000 = 4096 + 4`) within the cached page. If it's a write, the kernel keeps
track of the fact that the page is now "dirty" in some ancillary structure.

**Writeback**: a buffered read/write syscall returns after the memory-to-memory copy. The moficiations
made by e.g. write system calls are not even *issued* to disk, let alone durable. Instead, the kernel
asynchronously writes back dirtied pages based on a variety of conditions. For us, the most relevant
ones are a) explicit request by userspace (`fsync`) and b) memory pressure.

**Memory pressure**: the kernel page cache is a best effort service and a user of spare memory capacity.
If there is no free memory, the kernel page allocator will take pages used by page cache to satisfy allocations.
Before reusing a page like that, the page has to be written back (writeback, see above).
The far-reaching consequence of this is that **any allocation of anonymous memory can do IO** if the only
way to get that memory is by eviction & re-using a dirty page cache page.
Notably, this includes a simple `malloc` in userspace, because eventually that boils down to `mmap(..., MAP_ANON, ...)`.
I refer to this effect as the "malloc latency backscatter" caused by buffered IO.

**Direct IO** allows application's read/write system calls to bypass the kernel page cache. The filesystem
is still involved because it is ultimately in charge of mapping the concept of files & offsets within them
to sectors on block devices. Typically, the filesystem poses size and alignment requirements for memory buffers
and file offsets (statx `Dio_mem_align` / `Dio_offset_align`), see [this gist](https://gist.github.com/problame/1c35cac41b7cd617779f8aae50f97155). The IO operations will fail at runtime if the alignment requirements
are not met.

**"buffered" vs "direct"**: the central distinction between buffered and direct IO is about who allocates and
fills the IO buffers, and who controls when exactly the IOs are issued. In buffered IO, it's the syscall handlers,
kernel page cache, and memory management subsystems (cf "writeback"). In direct IO, all of it is done by
the application.
It takes more effort by the application to program with direct instead of buffered IO.
The return is precise control over and a clear distinction between consumption/modification of memory vs disk.

**Pageserver PageCache**: Pageserver has an additioanl `PageCache` (referred to as PageCache from here on, as opposed to "kernel page cache").
Its caching unit is 8KiB which is the Postgres page size.
Currently, it is tiny (128MiB), very much like Postgres's `shared_buffers`.
A miss in PageCache is filled from the filesystem using buffered IO, issued through the `VirtualFile` layer in Pageserver.

**VirtualFile** is Pageserver's abstraction for file IO, very similar to the faciltiy in Postgres that bears the same name.
Its historical purpose appears to be working around open file descriptor limitations, which is practically irrelevant on Linux.
However, the faciltiy in Pageserver is useful as an intermediary layer for metrics and abstracts over the different kinds of
IO engines that Pageserver supports (`std-fs` vs `tokio-epoll-uring`).

## History Of Caching In Pageserver

For multiple years, Pageserver's `PageCache` was used for all data path read _and write_ IO.
It performed write-back to the kernel using buffered IO.

We converted it into a read-only cache of immutable data in [PR 4994](https://github.com/neondatabase/neon/pull/4994).

The introduction of `tokio-epoll-uring` required converting the code base to used owned IO buffers.
The `PageCache` pages are usable as owned IO buffers.

We then introduced vectored `Timeline::get` (cf RFC 30).
The implementation bypasses PS `PageCache` for delta and image layer data block reads.
(The disk btree embedded in delta & image layers is still `PageCache`'d).

Most recently, and still ongoing, is [Epic: Bypass PageCache for user data blocks #7386](https://github.com/neondatabase/neon/issues/7386).
The goal there is to eliminate the remaining caching of user data blocks in PS `PageCache`.

The outcome of the above will be that
1. all data blocks are read through VirtualFile and
2. all indirect blocks (=disk btree blocks) are cached in the PS `PageCache`.
The norm will be very low baseline replacement rates in PS `PageCache`.
High baseline replacement rates will be treated as a signal of resource exhaustion (page cache insufficient to host working set of the PS).
It will be remediated by the storage controller, migrating tenants away to relieve pressure.
(Such a migration mechanism in storage controller is not part of this project.)

In the future, we may elminate the `PageCache` even for indirect blocks.
For example with an LRU cache that has as unit the entire disk btree content
instead of individual blocks.

## Motivation

Even though we have eliminated PS `PageCache` complexities and overheads, we are still using the kernel page cache for all IO.

In this RFC, we propose switching to direct IO and lay out a plan to do it.

The motivation for using direct IO:

Predictable VirtualFile operation latencies.
    * for reads: currently kernel page cache hit/miss determines fast/slow
    * for appends: immediate back-pressure from disk instead of kernel page cache
    * for in-place updates: we don't do in-place updates in Pageserver
    * file fsync: will become practically constant cost because no writeback needs to happen

Predictabile latencies, generally.
    * avoid *malloc latency backscatter* caused by buffered writes (see glossary section)

Efficiency
* Direct IO avoids one layer of memory-to-memory copy.
* We already do not rely / do not want to rely on the kernel page cache for batching of small IOs into bigger ones:
    * writes: we do large streaming writes and/or have implemented batching in userspace.
    * reads:
    * intra-request: vectored get (RFC 30) takes care of merging reads => no block is read twice
    * inter-request, e.g., getpage request for adjacent pages last-modified at nearly the same time
        * (ideally these would come in as one vectored get request)
        * generally, we accept making such reads *predictably* slow rather than *maybe* fast,
            depending on how busy the kernel page cache is.

Explicitness & Tangibility of resource usage.
* It is desriable and valuable to be *explicit* about the main resources we use. For example:
* We can build true observability of resource usage ("what tenant is causing the actual IOs that are sent to the disk?").
* We can build accounting & QoS by implementing an IO scheduler that is tenant aware.

## Definition of Done

All IOs of the Pageserver data path use direct IO, thereby bypassing the kernel page cache.

In particular, the "data path" includes the wal ingest path and anything on the `Timline::get` / `Timline::get_vectored` path.

The production Pageserver config are tuned such that we get equivalent hit rates for the indirect blocks in layers (disk btree blocks) in the PS PageCache compared to what we previously got from the kernel page cache.

The CPU utilization is equivalent or ideally lower.

There are no regressions to ingest latency.

Getpage & basebackup latencies under high memory pressure are equivalent to when we used with kernel page cache.
Getpage & basebackup latencies under low memory pressure will be worse than when we used kernel page cache, but they are predictable, i.e., proportional to number of layers & blocks visited per layer.

## Non-Goals

We're not eliminating the remaining use of PS `PageCache` as part of this work.

## Impacted Components

Pageserver.

## Proposed Implementation

The work breaks down into the following high-level items:

* Risk assessment: determine that our production filesystem (ext4) and Linux kernel version allows mixing direct IO and buffered IO.
* Alignment requirements: make all VirtualFile follow IO alignment requirements (`Dio_mem_align` / `Dio_offset_align`).
* Add Pageserver config option to configure direct vs buffered IO.
* Determine new production configuration for PS PageCache size: when we roll out direct IO, it needs to hold the working set of indirect blocks.
* Performance evaluation, esp avoiding regressions.

The risk assessment is to understand
1. the impact of an implementation bug where we issue some but not all IOs using direct IO, as well as
2. the degree to which this project can be safely partially completed, i.e., if we cannot convert all code paths in the time alotted.

The bulk of the design & coding work is to ensure adherence to the alignment requirements.

Our automated benchmarks are insufficient to rule out performance regressions.
Manual benchmarking / new automated benchmarks will be required for the last two items (new PS PageCache size, avoiding regressions).
The metrics we care about were already listed in the "Definition of Done" section of this document.
More details on benchmarking later in this doc (Phase 3).

### Meeting Direct IO Alignment Requirements

We need to fix all the places where we do tiny and/or unaligned IOs.
Otherwise the kernel will fail the operation with an error.
We can implement a fallback to buffered IO for a transitory period, to avoid user impact.
But the **goal is to systematically ensure that we issue properly aligned IOs to the kernel/filesystem**.

Ideally, we'd use the Rust type system to compile-time-ensure that we only use VirtualFile with aligned buffers.
Feasibility of this will be explored early in the project.

An alternative is to add runtime checks and potentially a runtime fallback to buffered IO so we avoid user-facing downtime.

Genearlly, this work is open-ended (=> hard to estimate!).
It is a fixpoint iteration on the code base until all the places are fixed.
The runtime-check based approach is more amenable to doing this incrementally over many commits.
The value of a type-system-based approach can still be realized retroactively, and it will avoid regressions.

From some [early scoping experiments in January](https://www.notion.so/neondatabase/2024-01-30-benchmark-tokio-epoll-uring-less-Page-Cache-O_DIRECT-request-local-page-cache-aa026802b5214c58b17518d7f6a4219b?pvs=4),
we know the broad categories of changes required:

- Tiny IOs
    - example: writes: blob_io BUFFERED=false writer for ImageLayer
    - example reads: blob_io / vectored_blob_io
    - We have to move the IO buffer from inside the kernel into userspace. The perf upside is huge because we avoid the syscalls.
    - Will very likely be caught by runtime checking.
    - recipe for writes: use streaming IO abstractions that do IO using aligned buffers (see below)
    - recipe for reads: shot-lived IO buffers from buffer pool (see below)

- Larger IOs that are unaligned
    - typical case for this would be a Vec or Bytes that’s short-lived and used as an IoBuf / IoBufMut
    - These are not guaranteed to be sufficiently aligned, and often are not.
    - => need to replace with buffers that are guaranteed aligned
    - recipe:
      - generally these short-lived buffers should have a bounded size, it's a pre-existing design flaw if they don't
      - if they have bounded size: can use buffer pool (see below)
      - unbounded size: try hard to convert these to bounded size or better use streaming IO (see below)
      - generally, unbounded size buffers are an accepted risk to timely completion of this project

- *Accidentally* aligned IOs
    - Like `Larger IOs` section above, but, for some reason, they're aligned.
    - The runtime-check won't detect them.
    - example: current PageCache slots are sometimes aligned
    - recipe: ???
      - for PageCache slots: malloc the page cache slots are with correct algignment.

### Buffer Pool

The **buffer pool** mentioned to above will be a load-bearing component.
Its basic function is to provide callers with a memory buffer of adequate alignment and size (statx `Dio_mem_align` / `Dio_offset_align`).
Callers `get()` a buffer from the pool. Size is specified at `get` time and is fixed (not growable).
Callers own the buffer and are responsible for filling it with valid data.
They then use it to perform the IO.
Either the IO completes and returns the buffer, or the caller loses interest, which hands over ownership to tokio-epoll-uring until IO completion.
The buffer may be re-used, but eventually it gets dropped.
The drop handler returns the buffer to the buffer pool.

The buffer pool enforces a maximum amount of IO memory by stalling `get()` calls if all buffers are in use.
This ensures `page_cache + buffer_pools + slop < user memory` where slop is all other memory allocations.

The buffer pool buffers can optionally be wrapped by the **streaming IO abstraction** in `owned_bufers_io::write` for use as the IO buffer.
This guarantees that the streaming IOs are issued from aligned buffers.

The tricky part is buffers whose size isn't know ahead of time.
The buffer pool can't provide such buffers.
One workaround is to use slop space (such as a Vec) to collect all the data, then memcpy it into buffer pool buffers like so:
```rust
let vec = ... /* code that produces variable amount of data */;
for chunk in vec.chunks(bufpool.buffer_size()) {
    let buf = bufpool.get();
    assert_eq!(buf.len(), bufpool.buffer_size());
    buf.copy_from_slice(chunk);
    file.write_at(..., buf, ...);
}
```
However, the `vec` in that code still needs to be sized in multiples of the filesystem block size.
The best way to ensure this is to completely refactor to `owned_bufers_io::write`, which also avoids the double-copying.

If we **have** to do writes of non-block-size-multiple length, the solution is to do read-modify-write for the unaligned parts.
We don't have infrastructure for this yet.
It would be best to avoid this, and from my scoping work in January, I cannot remember a need for it.

In the future, we might want to use [io_uring registered buffers](https://unixism.net/loti/ref-iouring/io_uring_register.html).
It's out of reach at this time because we use tokio-epoll-uring in thread-local executor mode, meaning we'd have to register
each buffer with all thread-local executors. However, above API requirements for the buffer pool implicitly require the buffer
handle that's returned by `get()` to be a custom smart pointer type. We will be able to extend it in the future to include the
io_uring registered buffer index without having to touch the entire code base.

## Execution

### Phase 1
In this phase we build a bunch of foundational pieces. The work is parallelizable to some extend.

* Explore type-system level way to find all unaligned IO/s
    * idea: create custom IO buffer marker traits / types , e.g. extend IoBuf / IoBufMut to IoBufAligned and IoBufMutAligned.
    * could take this as a general opportunity to clean up the owned buffers APIs
* Runtime-check for alignment requirements
* Perf simulation mode: pad VirtualFile op latencies to typical NVMe latencies
    * Such low latencies are tricky to precisely simulate, as, e.g., tokio doesn’t guarantee that timer resolution.
    * Maybe do a fake direct IO to some fake file in addition to the buffered IO? Doubles amount of tokio-epoll-uring traffic but it’s probably closest to reality.
    * Can we make this safely usable in production?
* Pageserver config changes to expose the new mdoes:
  ```rust
    ...
    virtual_file_direct_io: enum  {
        #[default]
        Disabled,
        Evaluate {
            check_alignment: no | log | error
            pad_timing: enum {
                No,
                TokioSleep,
                FakeFile { path: PathBuf }
            }
        },
        Enabled {
            on_alignment_error: error | fallback_to_buffered
        }
    }
    ...
  ```
* VirtualFile API to support direct IO
    * What's better: Require all callers to be explicit vs just always do direct IO?
* Buffer pool design & implementation
  * Explore designs / prior art to avoid contention on the global buffer pool
  * No implicit global state, create the instance in `main()` and pass it through the app. `RequestContext` is the way to go.
  * Explore further `RequestContext` integration: two-staged pool, with a tiny pool in the `RequestContext`
    to avoid contention on the global pool.
  * Should be able to draw from PS PageCache as a last resort mechanism to avoid OOMs
    (PageCache thrashing will alert operators!)
    * Longer-term, should have model of worst-case / p9X peak buffer usage per request
      and admit not more requests than what configured buffer pool size allows.
      Out of scope of this project, though.


## Phase 2
In this phase, we do the bulk of the coding work, leveraging the runtime check to get feedback.
Also, we use the performance simulator mode to get a worst-case estimate on the perf impact.

* Leverage runtime check for alignment (= monitor for its `warn!` logs)
  - in regress test CI => matrix build like we did for tokio-epoll-uring/vectored get/compaction algorithms
  - in staging
  - in benchmarks (pre-prod, nightly staging)
  - in production?

* Find & fix unaligned IOs.
  * See section `Meeting Direct IO Alignment Requirements`
  * This is the bulk of the work, and it's hard to estimate because we may have to refactor
    existing code away from bad practices such as unbounded allocation / not using streaming IO.

* Use performance simulator mode to get worst-case estimate for perf impact **early**
  * in manual testing on a developer-managed EC2 instance
  * in staging / pre-prod => work with QA team

## Phase 3: Performance
Functionally we're ready, now we have to understand the performance impact and ensure there are no regressions.
Also, we left room for optimization with the buffer pool implementation so let's improve there as well.

* Perf testing to validate perf requirements listed in "Definition of Done" section

* Understand where the bottlenecks are.
  * Manual testing is advisable for this => recommended to set up an EC2 instance with
    a local Grafana + Prometheus + node_exporter stack.
  * This work is time-consuming and open-ended. Get help if inexperienced.

Pagebench, pgbench, and nightly prodlike cloudbench, are workload *drivers*.
They are
* sufficient for producing the metrics listed in "Definition of Done",
* representative enough to detect severe regressions,
* expose bottlenecks.

However, we do not have sufficient automation for
* creating high memory pressure secenario (e.g. with cgroups)
* quantifying and recording before-and-after resource consumption (*CPU utilization, memory, IO*)
* recording pageserver metrics.
Hence, diligent perf testing will require **setting up a manually managed testbench in EC2** that resembles prod,
with a local prometheus + grafana stack + node_exporter +scraping of the local pageserver.
In the past, I have found having such a testbench to be most effective and flexible for diligent benchmarking.

For the high memory pressure configuration, it might make sense to extend `neon_local` to manage a cgroup hierarchy.
