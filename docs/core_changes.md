# Postgres core changes

This lists all the changes that have been made to the PostgreSQL
source tree, as a somewhat logical set of patches. The long-term goal
is to eliminate all these changes, by submitting patches to upstream
and refactoring code into extensions, so that you can run unmodified
PostgreSQL against Neon storage.

In Neon, we run PostgreSQL in the compute nodes, but we also run a special WAL redo process in the
page server. We currently use the same binary for both, with --wal-redo runtime flag to launch it in
the WAL redo mode. Some PostgreSQL changes are needed in the compute node, while others are just for
the WAL redo process.

In addition to core PostgreSQL changes, there is a Neon extension in contrib/neon, to hook into the
smgr interface. Once all the core changes have been submitted to upstream or eliminated some other
way, the extension could live outside the postgres repository and build against vanilla PostgreSQL.

Below is a list of all the PostgreSQL source code changes, categorized into changes needed for
compute, and changes needed for the WAL redo process:

# Changes for Compute node

## Add t_cid to heap WAL records

```
 src/backend/access/heap/heapam.c                            |   26 +-
 src/include/access/heapam_xlog.h                            |    6 +-
```

We have added a new t_cid field to heap WAL records. This changes the WAL record format, making Neon WAL format incompatible with vanilla PostgreSQL!

### Problem we're trying to solve

The problem is that the XLOG_HEAP_INSERT record does not include the command id of the inserted row. And same with deletion/update. So in the primary, a row is inserted with current xmin + cmin. But in the replica, the cmin is always set to 1. That works in PostgreSQL, because the command id is only relevant to the inserting transaction itself. After commit/abort, no one cares about it anymore. But with Neon, we rely on WAL replay to reconstruct the page, even while the original transaction is still running.

### How to get rid of the patch

Bite the bullet and submit the patch to PostgreSQL, to add the t_cid to the WAL records. It makes the WAL records larger, which could make this unpopular in the PostgreSQL community. However, it might simplify some logical decoding code; Andres Freund briefly mentioned in PGCon 2022 discussion on Heikki's Neon presentation that logical decoding currently needs to jump through some hoops to reconstruct the same information.


### Alternatives
Perhaps we could write an extra WAL record with the t_cid information, when a page is evicted that contains rows that were touched a transaction that's still running. However, that seems very complicated.

## ginfast.c

```
diff --git a/src/backend/access/gin/ginfast.c b/src/backend/access/gin/ginfast.c
index e0d9940946..2d964c02e9 100644
--- a/src/backend/access/gin/ginfast.c
+++ b/src/backend/access/gin/ginfast.c
@@ -285,6 +285,17 @@ ginHeapTupleFastInsert(GinState *ginstate, GinTupleCollector *collector)
                memset(&sublist, 0, sizeof(GinMetaPageData));
                makeSublist(index, collector->tuples, collector->ntuples, &sublist);

+               if (metadata->head != InvalidBlockNumber)
+               {
+                       /*
+                        * ZENITH: Get buffer before XLogBeginInsert() to avoid recursive call
+                        * of XLogBeginInsert(). Reading a new buffer might evict a dirty page from
+                        * the buffer cache, and if that page happens to be an FSM or VM page, zenith_write()
+                        * will try to WAL-log an image of the page.
+                        */
+                       buffer = ReadBuffer(index, metadata->tail);
+               }
+
                if (needWal)
                        XLogBeginInsert();

@@ -316,7 +327,6 @@ ginHeapTupleFastInsert(GinState *ginstate, GinTupleCollector *collector)
                        data.prevTail = metadata->tail;
                        data.newRightlink = sublist.head;

-                       buffer = ReadBuffer(index, metadata->tail);
                        LockBuffer(buffer, GIN_EXCLUSIVE);
                        page = BufferGetPage(buffer);
```

The problem is explained in the comment above

### How to get rid of the patch

Can we stop WAL-logging FSM or VM pages? Or delay the WAL logging until we're out of the critical
section or something.

Maybe some bigger rewrite of FSM and VM would help to avoid WAL-logging FSM and VM page images?


## Mark index builds that use buffer manager without logging explicitly

```
 src/backend/access/gin/gininsert.c                          |    7 +
 src/backend/access/gist/gistbuild.c                         |   15 +-
 src/backend/access/spgist/spginsert.c                       |    8 +-

also some changes in src/backend/storage/smgr/smgr.c
```

When a GIN index is built, for example, it is built by inserting the entries into the index more or
less normally, but without WAL-logging anything. After the index has been built, we iterate through
all pages and write them to the WAL. That doesn't work for Neon, because if a page is not WAL-logged
and is evicted from the buffer cache, it is lost. We have an check to catch that in the Neon
extension. To fix that, we've added a few functions to track explicitly when we're performing such
an operation: `smgr_start_unlogged_build`, `smgr_finish_unlogged_build_phase_1` and
`smgr_end_unlogged_build`.


### How to get rid of the patch

I think it would make sense to be more explicit about that in PostgreSQL too. So extract these
changes to a patch and post to pgsql-hackers.


## Track last-written page LSN

```
 src/backend/commands/dbcommands.c                           |   17 +-

Also one call to SetLastWrittenPageLSN() in spginsert.c, maybe elsewhere too
```

Whenever a page is evicted from the buffer cache, we remember its LSN, so that we can use the same
LSN in the GetPage@LSN request when reading the page back from the page server. The value is
conservative: it would be correct to always use the last-inserted LSN, but it would be slow because
then the page server would need to wait for the recent WAL to be streamed and processed, before
responding to any GetPage@LSN request.

The last-written page LSN is mostly tracked in the smgrwrite() function, without core code changes,
but there are a few exceptions where we've had to add explicit calls to the Neon-specific
SetLastWrittenPageLSN() function.

There's an open PR to track the LSN in a more-fine grained fashion:
https://github.com/neondatabase/postgres/pull/177

PostgreSQL v15 introduces a new method to do CREATE DATABASE that WAL-logs the database instead of
relying copying files and checkpoint. With that method, we probably won't need any special handling.
The old method is still available, though.

### How to get rid of the patch

Wait until v15?


## Cache relation sizes

The Neon extension contains a little cache for smgrnblocks() and smgrexists() calls, to avoid going
to the page server every time. It might be useful to cache those in PostgreSQL, maybe in the
relcache? (I think we do cache nblocks in relcache already, check why that's not good enough for
Neon)


## Use buffer manager when extending VM or FSM

```
 src/backend/storage/freespace/freespace.c                   |   14 +-
 src/backend/access/heap/visibilitymap.c                     |   15 +-

diff --git a/src/backend/access/heap/visibilitymap.c b/src/backend/access/heap/visibilitymap.c
index e198df65d8..addfe93eac 100644
--- a/src/backend/access/heap/visibilitymap.c
+++ b/src/backend/access/heap/visibilitymap.c
@@ -652,10 +652,19 @@ vm_extend(Relation rel, BlockNumber vm_nblocks)
        /* Now extend the file */
        while (vm_nblocks_now < vm_nblocks)
        {
-               PageSetChecksumInplace((Page) pg.data, vm_nblocks_now);
+               /*
+                * ZENITH: Initialize VM pages through buffer cache to prevent loading
+                * them from pageserver.
+                */
+               Buffer  buffer = ReadBufferExtended(rel, VISIBILITYMAP_FORKNUM, P_NEW,
+                                                                                       RBM_ZERO_AND_LOCK, NULL);
+               Page    page = BufferGetPage(buffer);
+
+               PageInit((Page) page, BLCKSZ, 0);
+               PageSetChecksumInplace(page, vm_nblocks_now);
+               MarkBufferDirty(buffer);
+               UnlockReleaseBuffer(buffer);

-               smgrextend(rel->rd_smgr, VISIBILITYMAP_FORKNUM, vm_nblocks_now,
-                                  pg.data, false);
                vm_nblocks_now++;
        }
```

### Problem we're trying to solve

???

### How to get rid of the patch

Maybe this would be a reasonable change in PostgreSQL too?


## Allow startup without reading checkpoint record

In Neon, the compute node is stateless. So when we are launching compute node, we need to provide
some dummy PG_DATADIR. Relation pages can be requested on demand from page server. But Postgres
still need some non-relational data: control and configuration files, SLRUs,...  It is currently
implemented using basebackup (do not mix with pg_basebackup) which is created by pageserver. It
includes in this tarball config/control files, SLRUs and required directories.

As pageserver does not have the original WAL segments, the basebackup tarball includes an empty WAL
segment to bootstrap the WAL writing, but it doesn't contain the checkpoint record.  There are some
changes in xlog.c, to allow starting the compute node without reading the last checkpoint record
from WAL.

This includes code to read the `zenith.signal` file, which tells the startup code the LSN to start
at. When the `zenith.signal` file is present, the startup uses that LSN instead of the last
checkpoint's LSN. The system is known to be consistent at that LSN, without any WAL redo.


### How to get rid of the patch

???


### Alternatives

Include a fake checkpoint record in the tarball. Creating fake WAL is a bit risky, though; I'm
afraid it might accidentally get streamed to the safekeepers and overwrite or corrupt the real WAL.

## Disable sequence caching

```
diff --git a/src/backend/commands/sequence.c b/src/backend/commands/sequence.c
index 0415df9ccb..9f9db3c8bc 100644
--- a/src/backend/commands/sequence.c
+++ b/src/backend/commands/sequence.c
@@ -53,7 +53,9 @@
  * so we pre-log a few fetches in advance. In the event of
  * crash we can lose (skip over) as many values as we pre-logged.
  */
-#define SEQ_LOG_VALS   32
+/* Zenith XXX: to ensure sequence order of sequence in Zenith we need to WAL log each sequence update. */
+/* #define SEQ_LOG_VALS        32 */
+#define SEQ_LOG_VALS   0
```

Due to performance reasons Postgres don't want to log each fetching of a value from a sequence, so
it pre-logs a few fetches in advance. In the event of crash we can lose (skip over) as many values
as we pre-logged. But with Neon, because page with sequence value can be evicted from buffer cache,
we can get a gap in sequence values even without crash.

### How to get rid of the patch

Maybe we can just remove it, and accept the gaps. Or add some special handling for sequence
relations in the Neon extension, to WAL log the sequence page when it's about to be evicted. It
would be weird if the sequence moved backwards though, think of PITR.

Or add a GUC for the amount to prefix to PostgreSQL, and force it to 1 in Neon.


## Walproposer

```
 src/Makefile                                                |    1 +
 src/backend/replication/libpqwalproposer/Makefile           |   37 +
 src/backend/replication/libpqwalproposer/libpqwalproposer.c |  416 ++++++++++++
 src/backend/postmaster/bgworker.c                           |    4 +
 src/backend/postmaster/postmaster.c                         |    6 +
 src/backend/replication/Makefile                            |    4 +-
 src/backend/replication/walproposer.c                       | 2350 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 src/backend/replication/walproposer_utils.c                 |  402 +++++++++++
 src/backend/replication/walreceiver.c                       |    7 +
 src/backend/replication/walsender.c                         |  320 ++++++---
 src/backend/storage/ipc/ipci.c                              |    6 +
 src/include/replication/walproposer.h                       |  565 ++++++++++++++++
```

WAL proposer is communicating with safekeeper and ensures WAL durability by quorum writes.  It is
currently implemented as patch to standard WAL sender.

### How to get rid of the patch

Refactor into an extension. Submit hooks or APIs into upstream if necessary.

@MMeent did some work on this already: https://github.com/neondatabase/postgres/pull/96

## Ignore unexpected data beyond EOF in bufmgr.c

```
@@ -922,11 +928,14 @@ ReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
                 */
                bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);
                if (!PageIsNew((Page) bufBlock))
-                       ereport(ERROR,
+               {
+                        // XXX-ZENITH
+                        MemSet((char *) bufBlock, 0, BLCKSZ);
+                        ereport(DEBUG1,
                                        (errmsg("unexpected data beyond EOF in block %u of relation %s",
                                                        blockNum, relpath(smgr->smgr_rnode, forkNum)),
                                         errhint("This has been seen to occur with buggy kernels; consider updating your system.")));
-
+               }
                /*
                 * We *must* do smgrextend before succeeding, else the page will not
                 * be reserved by the kernel, and the next P_NEW call will decide to
```

PostgreSQL is a bit sloppy with extending relations. Usually, the relation is extended with zeros
first, then the page is filled, and finally the new page WAL-logged. But if multiple backends extend
a relation at the same time, the pages can be WAL-logged in different order.

I'm not sure what scenario exactly required this change in Neon, though.

### How to get rid of the patch

Submit patches to pgsql-hackers, to tighten up the WAL-logging around relation extension. It's a bit
confusing even in PostgreSQL. Maybe WAL log the intention to extend first, then extend the relation,
and finally WAL-log that the extension succeeded.

## Make smgr interface available to extensions

```
 src/backend/storage/smgr/smgr.c                             |  203 +++---
 src/include/storage/smgr.h                                  |   72 +-
```

### How to get rid of the patch

Submit to upstream. This could be useful for the Disk Encryption patches too, or for compression.


## Added relpersistence argument to smgropen()

```
 src/backend/access/heap/heapam_handler.c                    |    2 +-
 src/backend/catalog/storage.c                               |   10 +-
 src/backend/commands/tablecmds.c                            |    2 +-
 src/backend/storage/smgr/md.c                               |    4 +-
 src/include/utils/rel.h                                     |    3 +-
```

Neon needs to treat unlogged relations differently from others, so the smgrread(), smgrwrite() etc.
implementations need to know the 'relpersistence' of the relation. To get that information where
it's needed, we added the 'relpersistence' field to smgropen().

### How to get rid of the patch

Maybe 'relpersistence' would be useful in PostgreSQL for debugging purposes? Or simply for the
benefit of extensions like Neon. Should consider this in the patch to make smgr API usable to
extensions.

## Alternatives

Currently in Neon, unlogged tables live on local disk in the compute node, and are wiped away on
compute node restart. One alternative would be to instead WAL-log even unlogged tables, essentially
ignoring the UNLOGGED option. Or prohibit UNLOGGED tables completely. But would we still need the
relpersistence argument to handle index builds? See item on "Mark index builds that use buffer
manager without logging explicitly".

## Use smgr and dbsize_hook for size calculations

```
 src/backend/utils/adt/dbsize.c                              |   61 +-
```

In PostgreSQL, the rel and db-size functions scan the data directory directly. That won't work in Neon.

### How to get rid of the patch

Send patch to PostgreSQL, to use smgr API functions for relation size calculation instead. Maybe as
part of the general smgr API patch.



# WAL redo process changes

Pageserver delegates complex WAL decoding duties to Postgres, which means that the latter might fall
victim to carefully designed malicious WAL records and start doing harmful things to the system.  To
prevent this, the redo functions are executed in a separate process that is sandboxed with Linux
Secure Computing mode (see seccomp(2) man page).

As an alternative to having a separate WAL redo process, we could rewrite all redo handlers in Rust
This is infeasible. However, it would take a lot of effort to rewrite them, ensure that you've done
the rewrite correctly, and once you've done that, it would be a lot of ongoing maintenance effort to
keep the rewritten code in sync over time, across new PostgreSQL versions. That's why we want to
leverage PostgreSQL code.

Another alternative would be to harden all the PostgreSQL WAL redo functions so that it would be
safe to call them directly from Rust code, without needing the security sandbox. That's not feasible
for similar reasons as rewriting them in Rust.


## Don't replay change in XLogReadBufferForRedo that are not for the target page we're replaying

```
 src/backend/access/gin/ginxlog.c                            |   19 +-

Also some changes in xlog.c and xlogutils.c

Example:

@@ -415,21 +416,27 @@ ginRedoSplit(XLogReaderState *record)
        if (!isLeaf)
                ginRedoClearIncompleteSplit(record, 3);

-       if (XLogReadBufferForRedo(record, 0, &lbuffer) != BLK_RESTORED)
+       action = XLogReadBufferForRedo(record, 0, &lbuffer);
+       if (action != BLK_RESTORED && action != BLK_DONE)
                elog(ERROR, "GIN split record did not contain a full-page image of left page");
```

### Problem we're trying to solve

In PostgreSQL, if a WAL redo function calls XLogReadBufferForRead() for a page that has a full-page
image, it always succeeds. However, Neon WAL redo process is only concerned about replaying changes
to a single page, so replaying any changes for other pages is a waste of cycles. We have modified
XLogReadBufferForRead() to return BLK_DONE for all other pages, to avoid the overhead. That is
unexpected by code like the above.

### How to get rid of the patch

Submit the changes to upstream, hope the community accepts them. There's no harm to PostgreSQL from
these changes, although it doesn't have any benefit either.

To make these changes useful to upstream PostgreSQL, we could implement a feature to look ahead the
WAL, and detect truncated relations. Even in PostgreSQL, it is a waste of cycles to replay changes
to pages that are later truncated away, so we could have XLogReadBufferForRedo() return BLK_DONE or
BLK_NOTFOUND for pages that are known to be truncated away later in the WAL stream.

### Alternatives

Maybe we could revert this optimization, and restore pages other than the target page too.

## Add predefined_sysidentifier flag to initdb

```
 src/backend/bootstrap/bootstrap.c                           |   13 +-
 src/bin/initdb/initdb.c                                     |    4 +

And some changes in xlog.c
```

This is used to help with restoring a database when you have all the WAL, all the way back to
initdb, but no backup. You can reconstruct the missing backup by running initdb again, with the same
sysidentifier.


### How to get rid of the patch

Ignore it. This is only needed for disaster recovery, so once we've eliminated all other Postgres
patches, we can just keep it around as a patch or as separate branch in a repo.


# Not currently committed but proposed

## Disable ring buffer buffer manager strategies

### Why?

Postgres tries to avoid cache flushing by bulk operations (copy, seqscan, vacuum,...).
Even if there are free space in buffer cache, pages may be evicted.
Negative effect of it can be somehow compensated by file system cache, but in Neon,
cost of requesting page from page server is much higher.

### Alternatives?

Instead of just prohibiting ring buffer we may try to implement more flexible eviction policy,
for example copy evicted page from ring buffer to some other buffer if there is free space
in buffer cache.

## Disable marking page as dirty when hint bits are set.

### Why?

Postgres has to modify page twice: first time when some tuple is updated and second time when
hint bits are set. Wal logging hint bits updates requires FPI which significantly increase size of WAL.

### Alternatives?

Add special WAL record for setting page hints.

## Prefetching

### Why?

As far as pages in Neon are loaded on demand, to reduce node startup time
and also speedup some massive queries we need some mechanism for bulk loading to
reduce page request round-trip overhead.

Currently Postgres is supporting prefetching only for bitmap scan.
In Neon we should also use prefetch for sequential and index scans, because the OS is not doing it for us.
For sequential scan we could prefetch some number of following pages. For index scan we could prefetch pages
of heap relation addressed by TIDs.

## Prewarming

### Why?

Short downtime (or, in other words, fast compute node restart time) is one of the key feature of Zenith.
But overhead of request-response round-trip for loading pages on demand can make started node warm-up quite slow.
We can capture state of compute node buffer cache and send bulk request for this pages at startup.
