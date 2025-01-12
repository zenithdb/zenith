# 
Created on ..
Implemented on ..

## Summary

Data in large tenants is split up between multiple pageservers according to key hashes, as
introduced in the [sharding RFC](031-sharding-static.md) and [shard splitting RFC](032-shard-splitting.md).

Whereas currently we send all WAL to all pageserver shards, and each shard filters out the data that it needs,
in this RFC we add a mechanism to filter the WAL on the safekeeper, so that each shard receives
only the data it needs.

This will place some extra CPU load on the safekeepers, in exchange for reducing the network bandwidth
for ingesting WAL back to scaling as O(1) with shard count, rather than O(N_shards).

## Motivation

1. Large databases require higher shard counts.  Whereas currently we run with up to 8 shards for tenants
with a few TB of storage, the next order of magnitude capacity increase will require tens of shards, such
that sending all WAL to all shards is impractical in terms of bandwidth.
2. For contemporary database sizes (~2TB), the pageserver is the bottleneck for ingest: since each
   shard has to decode and process the whole WAL, sharding doesn't full relieve this bottleneck.  To achieve significantly higher ingest speeds, we need to filter the WAL earlier so that each pageserver
   only has to process relevant parts.

## Non Goals (if relevant)

We do not seek to introduce multiple WALs per timeline, or to share the work of handling a timeline's
WAL across safekeepers (beyond simple 3x replication).  This RFC may be thought of as an incremental
move of the ingestion bottleneck up the stack: instead of high write rates bottlenecking on the
pageserver, they will bottleneck on the safekeeper.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Safekeeper, pageserver.

There will be no control plane or storage controller coordination needed, as pageservers will directly
indicate their sharding parameters to the safekeeper when subscribing for WAL.

## Proposed implementation

Terminology:
- "Data pages" refers to postgres relation blocks, and SLRU blocks.
- "Metadata pages" refers to everything else the pageserver stores, such as relation sizes and
  directories of relations.

### Phase 1: Refactor ingest

Currently, pageserver ingest code is structured approximately as follows:
1. `handle_walreceiver_connection` reads a stream of binary WAL records off a network
   socket
2. `WalIngest::ingest_record` to translate the record into a series of page-level modifications
3. `DatadirModification` accumulates page updates from several `ingest_record` calls, and when
   its `commit()` method is called, flushes these into a Timeline's open `InMemoryLayer`.

This process currently assumes access to a pageserver `Timeline` throughout `ingest_record` and
from `DatadirModification`, which is used to do read-modify-write cycles on metadata pages
such as relation sizes and the master DBDIR page.  It also assumes that records are ingested
strictly one after the other: they cannot be ingested in parallel because each record assumes
that earlier records' changes have already been applied to `Timeline`.

This code will be refactored to disentangle the simple, fast decode of relation page writes
from the more complex logic for updating internal metadata.  An intermediate representation
called `ProcessedWalIngest` will be introduced.  This is similar to the internal state of
a `DatadirModification`, but does not require access to a Timeline.  Instead of storing
metadata updates as materialized writes to pages, it will accumulate these as abstract operations,
for example rather than including a write to a relation size key, this structure will include
an operation that indicates "Update relation _foo_'s size to the max of its current value and
_bar_", such that these may be applied layer to a real Timeline.

The `DatadirModification` will be aware of the `EphemeralFile` format, so that as it accumulates
simple page writes of relation blocks, it can write them directly into a buffer in the serialized
format.  This will avoid the need to later deserialize/reserialize this data when passing the
structure between safekeeper and pageserver.

The new pipeline will be:
1. `handle_walreceiver_connection` reads a stream of binary WAL records off a network
2. A `ProcessedWalIngest` is generated from the incoming WAL records.  This may be done
   in a pipelined way (to parallelize the work of this phase of ingest), and does not
   require a reference to a Timeline.
3. The logic that is current spread between `WalIngest` and `DatadirModification` for updating
   metadata will be refactored to consume the metadata operations from the `ProcessedWalIngest`
   and turn them into literal writes to metadata pages.  This part must be done sequentially.
4. The resulting buffer of metadata page writes is combined with the buffer of relation block
   writes, and written into the `InMemoryLayer`.

### Phase 2: Decode & filter on safekeeper

In the previous phase, the ingest code was modified to be able to do most of its work without access to
a Timeline: this first stage of ingest simply converts a series of binary wal records into
a buffer of relation/SLRU page writes, and a buffer of abstract metadata writes.

The modified ingest code may be transplanted from pageserver to safekeeper (probably via a
shared crate).  The safekeeper->pageserver network protocol is modified to:
 - in subscription requests, send the `ShardIdentity` from the pageserver to the safekeeper
 - in responses, transmit a `ProcessedWalIngest` instead of a raw `WalRecord`.
 - use the `ShardIdentity` to filter the `ProcessedWalIngest` to relevant content for
   the subscribing shard before transmitting it.

The overall behavior of the pageserver->safekeeper interaction remains the same, in terms of
consistent LSN feedback, and connection management.  Only the payload of the subscriptions
changes, to express an LSN range of WAL as a filtered `ProcessedWalIngest` instead of the
raw data.

The ingest code on the pageserver can now skip the part where it does the first phase of
processing, as it will receive pre-processed data off the wire.

TODO: define batching on the safekeeper: will we do the same batching as on the pageserfer
today?  (i.e. insteadof sending one ProcessedWalIngest per WalRecord, we would batch
many WalRecords)

### Phase 3: Shared safekeeper decode cursor for all shards

In the previous phase, the initial processing of WAL was moved to the safekeeper, but it is still
done once for each shard: this will generate O(N_shards) CPU work on the safekeeper.

To avoid this, we will introduce a cursor concept for handling the WAL on the safekeeper: under
normal operation, the WAL will be read from disk and decoded _once_, and the per-shard `ProcessedWalIngest` objects will be buffered, ready for the relevant shards to subscribe
and receive them.

The main challenge to implementing such cursor schemes is that pageserver shards may not all
subscribe in a nicely synchronized way, and they may go backwards. We may make two useful simplifying
assumptions:
1. that there is little value in letting some shards proceed far ahead of others, since whichever
   shard has the lowest ingested LSN is likely to act as a brake on postgres's progress the next
   time it happens to request a page from that shard.
2. that shards requesting older LSNs (i.e. going backwards) is relatively rare, so it's okay if
   it's a bit slower.

Exploiting those assumptions, we may use a simplified design in which there is a single Cursor
for each safekeeper Timeline, and this cursor maintains an LSN-range-limited buffer of recently
ingested data for each shard which is subscribing on this safekeeper.  When a read arrives:
- for an LSN that is already in the cursor's buffer, it will be returned immediately
- for an LSN ahead of the cursor's state, it will wait for the cursor to advance
- for an LSN that the cursor has already moved past, we will go read the relevant part
  of the WAL and re-ingest it to serve the response, while also registering this shard
  with the Cursor so that the Cursor won't move forward until this shard catches up.

Complications/edge cases:
- Pageserver HA/migration: as each shard may have multiple attached locations
which are each consuming the WAL.  To handle this gracefully, we can let the pageserver transmit
its generation number while subscribing to the WAL.  The highest generation-ed requests will 
be interpreted as sufficient for the Cursor to advance, but older generation-ed requests will
be serviced in the same way as reads to old LSNs (unless the LSN they request happens to already
be buffered in the cursor).
- Pageservers switching connections between safekeepers: a pageserver may restart, or give up on a connection
  and decide to go read from a different safekeeper, and the original connection may not be
  cleanly closed.  We must avoid Cursors getting stuck waiting for dead pageserver connections. We
  can apply a TTL mechanism if we modify the pageserver's logic for sending `PageserverFeedback` messages
  to send one periodically as a heartbeat.


## Deployment

TODO: discuss whether we will deploy phase 2 before phase 3 -- is it safe to put the
O(N_shards) CPU load on safekeepers?

TODO: discuss why/whether it is safe after phase 3 to put extra CPU load on safekeepers: how
much load is it, and how will we know whether we are doing harm?

TODO: describe memory footprint per timeline on safekeepers .vs current memory situation

## Optimizations

### Handling Laggard Shards

Phase 3 describes how we can re-use a single buffer on the safekeeper in order to serve subscriptions from
multiple shards. On the happy path we can indeed use a single cursor to serve all shards of one tenant subscribed
to the same safekeeper. This section proposes how to handle cases when one or more shards attempt to subscribe
at LSNs in the past. This may happen during live migrations and pageserver restarts/crashes.

Things to keep in mind when thinking about this:
* safekeepers keep data on disk until it has been sent to the pageserver
* timeline (and implicitly timeline + shard) safekeeper assingments are not stable
* shard might jump back by a couple hundred MiB when restarting/migrating
* advancing only some of the shards is not useful

Laggard shards are problematic because the cursor merging described in Phase 3 cannot be applied.
This means that the safekeeper will have to hydrate WAL from S3 in order to serve the laggard.

The core proposal here is to make safekeepers aware of the current `remote_consistent_lsn`
of each (tenant, timeline, shard) tuple. This allows more advanced shards to stop pushing
data into the pageserver until all shards are caught up.

More specifically, we can use the storage broker to gossip this information across the safekeeper
cluster. Safekeepers aleady subscribe to updates of `SafekeeperTimelineInfo` from the storage broker.
This struct already contains everything we need apart from the shard identifier. With the shard identifier
included, each safekeeper can have a clear picture of the current WAL location of each shard.

We can implement a simple catch-up protocol:
* We define the "shard X cursor" as the `remote_consistent_lsn` of shard X
* We define the `high_watermark_cursor_lsn` as the `remote_consistent_lsn` of the most advanced shard.
* If safekeeper A detects that the cursor of any given shard X is lagging by more than a configurable
threshold (call it `max_shard_cursor_lag`), then it records the current `high_watermark_cursor_lsn`
in memory and stops pushing data to the pageserver for any shard with a cursor greater or equal to
`high_watermark_cursor_lsn`. This rule also applies for new subscriptions from the pageserver.

Broker outages are already bad for the ingest path, but this proposed change would make any outage
worse. To work around this we can:
1. Make the catch-up protocol optional. If safekeepers detect broker unavailability, then the
`high_watermark_cursor_lsn` gating does not apply.
2. Make the storage broker highly available. We've already done this for the storage controller
(rolling update, leadership, proxying from stepped down to new leader).
3. Do peer to peer safekeeper communication.

#### Tweaking the Safekeeper Selection Algorithm

If the broker becomes aware of the `remote_consistent_lsn` of each shard we can make the safekeeper
selection algorithm used on the pageserver smarter by aplying the following heuristic:
Given a choice between two safekeepers, pick the one with an active cursor that has the biggest
chance of being merged with the cursor of the new shard. If we already have a laggy shard with
a cursor at LSN 10k and the pageserver wants WAL for another laggy shard starting from LSN 11k,
then we can pick the safekeeper serving the cursor at LSN 10k.


### Pipelining first ingest phase

The first ingest phase is a stateless transformation of a binary WAL record into a pre-processed
output per shard.  To put multiple CPUs to work, we may pipeline this processing up to some defined buffer
depth.

### 

## Reliability, failure modes and corner cases (if relevant)

## Interaction/Sequence diagram (if relevant)

## Scalability (if relevant)

## Security implications (if relevant)

## Unresolved questions (if relevant)




## Alternatives considered

### Give safekeepers enough state to fully decode WAL

In this RFC, we only do the first phase of ingest on the safekeeper, because this is
the phase that is stateless.  Subsequent changes then happen on the pageserver, with
access to the `Timeline` state.

We could do more work on the safekeeper if we transmitted metadata state to the safekeeper
when subscribing to the WAL: for example, by telling the safekeeper all the relation sizes,
so that it could then generate all the metadata writes for relation sizes.

We avoid doing this for several reasons:
1. Complexity: it's a more invasive protocol change
2. Decoupling: having the safekeeper understand the `ProcessedWalIngest` already somewhat
   infects it with knowledge of the pageserver, but this is mainly an abstract structure
   that describes postgres writes.  However, if we taught the safekeeper about the exact
   way that pageserver deals with metadata keys, this would be a much tighter coupling.
3. Load: once the WAL has been processed to the point that it can be split between shards,
   it is preferable to share out work on the remaining shards rather than adding extra CPU
   load to the safekeeper.

### Do pre-processing on the compute instead of the safekeeper

Since our first stage of ingest is stateless, it could be done at any stage in the pipeline,
all the way up to the compute.

We choose not to do this, because it is useful for the safekeeper to store the raw WAL rather
than just the preprocessed WAL:
- The safekeeper still needs to be able to serve raw WAL back to postgres for e.g. physical replication
- It simplifies our paxos implementation to have the offset in the write log be literally
  the same as the LSN

### Do wal pre-processing on shard 0 or a separate service, send it to other shards from there

If we wanted to keep the safekeepers as entirely pure stores of raw WAL bytes, then
we could do the initial decode and shard-splitting in some other location:
- Shard 0 could subscribe to the full WAL and then send writes to other shards
- A new intermediate service between the safekeeper and pageserver could do the splitting.

So why not?
- Clearly there is more infrastructure involved here compared with doing it inline on the safekeeper.
- Safekeepers already have very light CPU load: typical cloud instances shapes with appropriate
  disks for the safekeepers effectively have "free" CPU resources.
- Doing extra work on shard 0 would complicate scheduling of shards on pageservers, because
  shard 0 would have significantly higher CPU load under write workloads than other shards.

## Future

### Perhaps: Serve reads from the safekeeper/eliminate pageserver InMemoryLayer

Now that we are doing some pre-processing of the WAL on the safekeeper and storing
a buffer of some recent reads in Cursor, it would be possible to serve some reads
from the safekeeper directly.  It's unclear whether this is a desirable change: it would
only be a net benefit if it enabled us to avoid doing some work on the pageserver.

Ultimately, if the safekeeper served recent reads, we could build entire L0 layers on the safekeeper
and upload them to S3, saving the pageserver the disk bandwidth of writing ephemeral layers
and then flushing them to L0s, since ephemeral layers are approximately just extents of WAL, post-processed
into a readable form.

