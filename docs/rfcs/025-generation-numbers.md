# Pageserver: split-brain safety for remote storage through generation numbers

## Summary

A scheme of logical "generation numbers" for tenant attachment to pageservers is proposed, along with
changes to the remote storage format to include these generation numbers in S3 keys.

Using the control plane as the issuer of these generation numbers enables strong anti-split-brain
properties in the pageserver cluster without implementing a consensus mechanism directly
in the pageservers.

## Motivation

Currently, the pageserver's remote storage format does not provide a mechanism for addressing
split brain conditions that may happen when replacing a node or when migrating
a tenant from one pageserver to another.

From a remote storage perspective, a split brain condition occurs whenever two nodes both think
they have the same tenant attached, and both can write to S3. This can happen in the case of a
network partition, pathologically long delays (e.g. suspended VM), or software bugs.

In the current deployment model, control plane guarantees that a tenant is attached to one
pageserver at a time, thereby ruling out split-brain conditions resulting from dual
attachment (however, there is always the risk of a control plane bug). This control
plane guarantee prevents robust response to failures, as if a pageserver is unresponsive
we may not detach from it. The mechanism in this RFC fixes this, by making it safe to
attach to a new, different pageserver even if an unresponsive pageserver may be running.

Futher, lack of safety during split-brain conditions blocks two important features where occasional
split-brain conditions are part of the design assumptions:

- seamless tenant migration ([RFC PR](https://github.com/neondatabase/neon/pull/5029))
- automatic pageserver instance failure handling (aka "failover") (RFC TBD)

### Prior art

- 020-pageserver-s3-coordination.md
- 023-the-state-of-pageserver-tenant-relocation.md
- 026-pageserver-s3-mvcc.md

This RFC has broad similarities to the proposal to implement a MVCC scheme in
S3 object names, but this RFC avoids a general purpose transaction scheme in
favour of more specialized "generations" that work like a transaction ID that
always has the same lifetime as a pageserver process or tenant attachment, whichever
is shorter.

## Requirements

- Accommodate storage backends with no atomic or fencing capability (i.e. work within
  S3's limitation that there are no atomics and clients can't be fenced)
- Don't depend on any STONITH or node fencing in the compute layer (i.e. we will not
  assume that we can reliably kill and EC2 instance and have it die)
- Scoped per-tenant, not per-pageserver; for _seamless tenant migration_, we need
  per-tenant granularity, and for _failover_, we likely want to spread the workload
  of the failed pageserver instance to a number of peers, rather than monolithically
  moving the entire workload to another machine.
  We do not rule out the latter case, but should not constrain ourselves to it.

## Design Tenets

These are not requirements, but are ideas that guide the following design:

- Avoid implementing another consensus system: we already have a strongly consistent
  database in the control plane that can do atomic operations where needed, and we also
  have a Paxos implementation in the safekeeper.
- Avoiding locking in to specific models of how failover will work (e.g. do not assume that
  all the tenants on a pageserver will fail over as a unit).
- Be strictly correct when it comes to data integrity. Occasional failures of availability
  are tolerable, occasional data loss is not.

## Non Goals

The changes in this RFC intentionally isolate the design decision of how to define
logical generations numbers and object storage format in a way that is somewhat flexible with
respect to how actual orchestration of failover works.

This RFC intentionally does not cover:

- Failure detection
- Orchestration of failover
- Standby modes to keep data ready for fast migration
- Intentional multi-writer operation on tenants (multi-writer scenarios are assumed to be transient split-brain situations).
- Sharding.

The interaction between this RFC and those features is discussed in [Appendix B](#appendix-b-interoperability-with-other-features)

## Impacted Components

pageserver, control plane, safekeeper (optional)

## Implementation Part 1: Correctness

### Summary

- A per-tenant **generation number** is introduced to uniquely identifying tenant attachments to pageserver processes.

  - This generation number increments each time the control plane modifies a tenant (`Project`)'s assigned pageserver, or when the assigned pageserver restarts.
  - the control plane is the authority for generation numbers: only it may
    increment a generation number.

- **Object keys are suffixed** with the generation number
- **Safety for multiply-attached tenants** is provided by the
  generation number in the object key: the competing pageservers will not
  try to write to the same keys.
- **Safety in split brain for multiple nodes running with
  the same node ID** is provided by the pageserver calling out to the control plane
  on startup, to re-attach and thereby increment the generations of any attached tenants
- **Safety for deletions** is achieved by deferring the DELETE from S3 to a point in time where the deleting node has validated with control plane that no attachment with a higher generation has a reference to the to-be-DELETEd key.
- **The control plane is used to issue generation numbers** to avoid the need for
  a built-in consensus system in the pageserver, although this could in principle
  be changed without changing the storage format.

### Generation numbers

A generation number is associated with each tenant in the control plane,
and each time the attachment status of the tenant changes, this is incremented.
Changes in attachment status include:

- Attaching the tenant to a different pageserver
- A pageserver restarting, and "re-attaching" its tenants on startup

These increments of attachment generation provide invariants we need to avoid
split-brain issues in storage:

- If two pageservers have the same tenant attached, the attachments are guaranteed to have different generation numbers, because the generation would increment
  while attaching the second one.
- If there are multiple pageservers running with the same node ID, all the attachments on all pageservers are guaranteed to have different generation numbers, because the generation would increment
  when the second node started and re-attached its tenants.

As long as the infrastructure does not transparently replace an underlying
physical machine, we are totally safe. See the later [unsafe case](#unsafe-case-on-badly-behaved-infrastructure) section for details.

### Object Key Changes

#### Generation suffix

All object keys (layer objects and index objects) will contain the attachment
generation as a [suffix](#why-a-generation-suffix-rather-than-prefix).
This suffix is the primary mechanism for protecting against split-brain situations, and
enabling safe multi-attachment of tenants:

- Two pageservers running with the same node ID (e.g. after a failure, where there is
  some rogue pageserver still running) will not try to write to the same objects, because at startup they will have re-attached tenants and thereby incremented
  generation numbers.
- Multiple attachments (to different pageservers) of the same tenant will not try to write to the same objects, as each attachment would have a distinct generation.

The generation is appended in hex format (8 byte string representing
u32), to all our existing key names. A u32's range limit would permit
27 restarts _per second_ over a 5 year system lifetime: orders of magnitude more than
is realistic.

The exact meaning of the generation suffix can evolve over time if necessary, for
example if we chose to implement a failover mechanism internally to the pageservers
rather than going via the control plane. The storage format just sees it as a number,
with the only semantic property being that the highest numbered index is the latest.

#### Index changes

Since object keys now include a generation suffix, the index of these keys must also be updated. IndexPart currently stores keys and LSNs sufficient to reconstruct key names: this would be extended to store the generation as well.

This will increase the size of the file, but only modestly: layers are already encoded as
their string-ized form, so the overhead is about 10 bytes per layer. This will be less if/when
the index storage format is migrated to a binary format from JSON.

#### Visibility

_This section doesn't describe code changes, but extends on the consequences of the
object key changes given above_

##### Visibility of objects to pageservers

Pageservers can of course list objects in S3 at any time, but in practice their
visible set is based on the contents of their LayerMap, which is initialized
from the `index_part.json.???` that they load.

Starting with the `index_part` from the most recent previous generation
(see [loading index_part](#finding-the-remote-indices-for-timelines)), a pageserver
initially has visibility of all the objects that were referenced in the loaded index.
These objects are guaranteed to remain visible until the current generation is
superseded, via pageservers in older generations avoiding deletions (see [deletion](#deletion)).

The "most recent previous generation" is _not_ necessarily the most recent
in terms of walltime, it is the one that is readable at the time a new generation
starts. Consider the following sequence of a tenant being re-attached to different
pageserver nodes:

- Create + attach on PS1 in generation 1
- PS1 Do some work, write out index_part.json-0001
- Attach to PS2 in generation 2
- Read index_part.json-0001
- PS2 starts doing some work...
- Attach to PS3 in generation 3
- Read index_part.json-0001
- **...PS2 finishes its work: now it writes index_part.json-0002**
- PS3 writes out index_part.json-0003

In the above sequence, the ancestry of indices is:

```
0001 -> 0002
     |
     -> 0003
```

This is not an issue for safety: if the 0002 references some object that is
not in 0001, then 0003 simply does not see it, and will re-do whatever
work was required (e.g. ingesting WAL or doing compaction). Objects referenced
by only the 0002 index will never be read by future attachment generations, and
will eventually be cleaned up by a scrub (see [scrubbing](#cleaning-up-orphan-objects-scrubbing)).

##### Visibility of LSNs to clients

Because index_part.json is now written with a generation suffix, which data
is visible depends on which generation the reader is operating in:

- If one was passively reading from S3 from outside of a pageserver, the
  visibility of data would depend on which index_part.json-<generation> file
  one had chosen to read from.
- If two pageservers have the same tenant attached, they may have different
  data visible as they're independently replaying the WAL, and maintaining
  independent LayerMaps that are written to independent index_part.json files.
  Data does not have to be remotely committed to be visible.
- For a pageserver writing with a stale generation, historic LSNs
  remain readable until another pageserver (with a higher generation suffix)
  decides to execute GC deletions. At this point, we may think of the stale
  attachment's generation as having logically ended: during its existence
  the generation had a consistent view of the world.
- For a newly attached pageserver, its highest visible LSN may appears to
  go backwards with respect to an earlier attachment, if that earlier
  attachment had not uploaded all data to S3 before the new attachment.

### Deletion

#### Generation number validation

While writes are de-conflicted by writers always using their own generation number in the key,
deletions are slightly more challenging: if a pageserver A is isolated, and the true active node is
pageserver B, then it is dangerous for A to do any object deletions, even of objects that it wrote
itself, because pageserver's B metadata might reference those objects.

We solve this by inserting a "generation validation" step between the write of a remote index
that un-links a particular object from the index, and the actual deletion of the object, such
that deletions strictly obey the following ordering:

1. Write out index_part.json: this guarantees that any subsequent reader of the metadata will
   not try and read the object we unlinked.
2. Call out to control plane to validate that the generation which we use for our attachment is still the latest.
3. If step 2 passes, it is safe to delete the object. Why? The check-in with control plane
   together with our visibility rules guarantees that any later generation
   will use either the exact `index_part.json` that we uploaded in step 1, or a successor
   of it; not an earlier one. In both cases, the `index_part.json` doesn't reference the
   key we are deleting anymore, so, the key is invisible to any later attachment generation.
   Hence it's safe to delete it.

Note that at step 2 we are only confirming that deletions of objects _no longer referenced
by the specific `index_part.json` written in step 1_ are safe. If we were attempting other deletions concurrently,
these would need their own generation validation step.

If step 2 fails, we may leak the object. This is safe, but has a cost: see [scrubbing](#cleaning-up-orphan-objects-scrubbing). We may avoid this entirely outside of node
failures, if we do proper flushing of deletions on clean shutdown and clean migration.

To avoid doing a huge number of control plane requests to perform generation validation,
validation of many tenants will be done in a single request, and deletions will be queued up
prior to validation: see [Persistent deletion queue](#persistent-deletion-queue) for more.

#### `remote_consistent_lsn` updates

Remote objects are not the only kind of deletion the pageserver does: it also indirectly deletes
WAL data, by feeding back remote_consistent_lsn to safekeepers, as a signal to the safekeepers that
they may drop data below this LSN.

For the same reasons that deletion of objects must be guarded by an attachment generation number
validation step, updates to `remote_consistent_lsn` are subject to the same rules, using
an ordering as follows:

1. upload the index_part that covers data up to LSN `L0` to S3
2. Call out to control plane to validate that the generation which we use for our attachment is still the latest.
3. advance the `remote_consistent_lsn` that we advertise to the safekeepers to `L0`

If step 2 fails, then the `remote_consistent_lsn` advertised
to safekeepers will not advance again until a pageserver
with the latest generation is ready to do so.

**Note:** at step 3 we are not advertising the _latest_ remote_consistent_lsn, we are
advertising the value in the index_part that we uploaded in step 1. This provides
a strong ordering guarantee.

Internally to the pageserver, each timeline will have two remote_consistent_lsn values: the one that
reflects its latest write to remote storage, and the one that reflects the most
recent validation of generation number. It is only the latter value that may
be advertised to the outside world (i.e. to the safekeeper).

The control plane remains unaware of `remote_consistent_lsn`: it only has to validate
the freshness of generation numbers, thereby granting the pageserver permission to
share the information with the safekeeper.

For convenience, in subsequent sections and RFCs we will use "deletion" to mean both deletion
of objects in S3, and updates to the `remote_consistent_lsn`, as updates to the remote consistent
LSN are de-facto deletions done via the safekeeper, and both kinds of deletion are subject to
the same generation validation requirement.

### Pageserver attach/startup changes

#### Attachment

Calls to `/v1/tenant/{tenant_id}/attach` are augmented with an additional
`generation` field in the body.

The pageserver does not persist this: a generation is only good for the lifetime
of a process.

#### Finding the remote indices for timelines

Because index files are now suffixed with generation numbers, the pageserver
cannot always GET the remote index in one request, because it can't always
know a-priori what the latest remote index is.

Typically, the most recent generation to write an index would be our own
generation minus 1. However, this might not be the case: the previous
node might have started and acquired a generation number, and then crashed
before writing out a remote index.

In the general case and as a fallback, the pageserver may list all the `index_part.json`
files for a timeline, sort them by generation, and pick the highest that is `<=`
its current generation for this attachment. The tenant should never load an index
with an attachment generation _newer_ than its own.
These two rules combined ensure that objects written by later generations are never visible to earlier generations.

Note that if a given attachment picks an index part from an earlier generation (say n-2), but crashes & restarts before it writes its own generation's index part, next time it tries to pick an index part there may be an index part from generation n-1.
It would pick the n-1 index part in that case, because it's sorted higher than the previous one from generation n-2.
So, above rules guarantee no determinism in selecting the index part.
are allowed to be attached with stale attachment generations during a multiply-attached
phase in a migration, and in this instance if the old location's pageserver restarts,
it should not try and load the newer generation's index.

To summarize, on starting a timeline, the pageserver will:

1. Issue a GET for index_part.json-<my generation - 1>
2. If 1 failed, issue a ListObjectsv2 request for index_part.json\* and
   pick the newest.

One could optimize this further by using the control plane to record specifically
which generation most recently wrote an index_part.json, if necessary, to increase
the probability of finding the index_part.json in one GET. One could also improve
the chances by having pageservers proactively write out index_part.json after they
get a new generation ID.

#### Re-attachment on startup

On startup, the pageserver will call out to an new control plane `/re-attach`
API (see [Generation API](#generation-api)). This returns a list of
tenants that should be attached to the pageserver, and their generation numbers, which
the control plane will increment before returning.

The pageserver should still scan its local disk on startup, but should _delete_
any local content for tenants not indicated in the `/re-attach` response: their
absence is an implicit detach operation.

**Note** if a tenant is omitted from the re-attach response, its local disk content
will be deleted. This will change in subsequent work, when the control plane gains
the concept of a secondary/standby location: a node with local content may revert
to this status and retain some local content.

#### Cleaning up previous generations' remote indices

Deletion of old indices is not necessary for correctness, although it is necessary
to avoid the ListObjects fallback in the previous section becoming ever more expensive.

Once the new attachment has written out its index_part.json, it may asynchronously clean up historic index_part.json
objects that were found.

We may choose to implement this deletion either as an explicit step after we
write out index_part for the first time in a pageserver's lifetime, or for
simplicity just do it periodically as part of the background scrub (see [scrubbing](#cleaning-up-orphan-objects-scrubbing));

### Control Plane Changes

#### Store generations for attaching tenants

- The `Project` table must store the generation number for use when
  attaching the tenant to a new pageserver.
- The `/v1/tenant/:tenant_id/attach` pageserver API will require the generation number,
  which the control plane can supply by simply incrementing the `Project`'s
  generation number each time the tenant is attached to a different server: the same database
  transaction that changes the assigned pageserver should also change the generation number.

#### Generation API

This section describes an API that could be provided directly by the control plane,
or built as a separate microservice. In earlier parts of the RFC, when we
discuss the control plane providing generation numbers, we are referring to this API.

The API endpoints used by the pageserver to acquire and validate generation
numbers are quite simple, and only require access to some persistent and
linerizable storage (such as a database).

Building this into the control plane is proposed as a least-effort option to exploit existing infrastructure and implement generation number issuance in the same transaction that mandates it (i.e., the transaction that updates the `Project` assignment to another pageserver).
However, this is not mandatory: this "Generation Number Issuer" could
be built as a microservice. In practice, we will write such a miniature service
anyway, to enable E2E pageserver/compute testing without control plane.

The endpoints required by pageservers are:

##### `/re-attach`

- Request: `{node_id: <u32>}`
- Response:
  - 200 `{tenants: [{id: <TenantId>, gen: <u32>}]}`
  - 404: unknown node_id
  - (Future: 429: flapping detected, perhaps nodes are fighting for the same node ID,
    or perhaps this node was in a retry loop)
  - (On unknown tenants, omit tenant from `tenants` array)
- Server behavior: query database for which tenants should be attached to this pageserver.
  - for each tenant that should be attached, increment the attachment generation and
    include the new generation in the response
- Client behavior:
  - for all tenants in the response, activate with the new generation number
  - for any local disk content _not_ referenced in the response, act as if we
    had been asked to detach it (i.e. delete local files)

**Note** the `node_id` in this request will change in future if we move to ephemeral
node IDs, to be replaced with some correlation ID that helps the control plane realize
if a process is running with the same storage as a previous pageserver process (e.g.
we might use EC instance ID, or we might just write some UUID to the disk the first
time we use it)

##### `/validate`

- Request: `{'tenants': [{tenant: <tenant id>, attach_gen: <gen>}, ...]}'`
- Response:
  - 200 `{'tenants': [{tenant: <tenant id>, status: <bool>}...]}`
  - (On unknown tenants, omit tenant from `tenants` array)
- Purpose: enable the pageserver to discover for the given attachments whether they are still the latest.
- Server behavior: this is a read-only operation: simply compare the generations in the request with
  the generations known to the server, and set status to `true` if they match.
- Client behavior: clients must not do deletions within a tenant's remote data until they have
  received a response indicating the generation they hold for the attachment is current.

#### Use of `/load` and `/ignore` APIs

Because the pageserver will be changed to only attach tenants on startup
based on the control plane's response to a `/re-attach` request, the load/ignore
APIs no longer make sense in their current form.

The `/load` API becomes functionally equivalent to attach, and will be removed:
any location that used `/load` before should just attach instead.

The `/ignore` API is equivalent to detaching, but without deleting local files.

### Timeline/Branch creation & deletion

All of the previous arguments for safety have described operations within
a timeline, where we may describe a sequence that includes updates to
index_part.json, and where reads and writes are coming from a postgres
endpoint (writes via the safekeeper).

Creating or destroying timeline is a bit different, because writes
are coming from the control plane.

We must be safe against scenarios such as:

- A tenant is attached to pageserver B while pageserver A is
  in the middle of servicing an RPC from the control plane to
  create or delete a tenant.
- A pageserver A has been sent a timeline creation request
  but becomes unresponsive. The tenant is attached to a
  different pageserver B, and the timeline creation request
  is sent there too.

#### Timeline Creation

If some very slow node tries to do a timeline creation _after_
a more recent generation node has already created the timeline
and written some data into it, that must not cause harm. This
is provided in timeline creations by the way all the objects
within the timeline's remote path include a generation suffix:
a slow node in an old generation that attempts to "create" a timeline
that already exists will just emit an index_part.json with
an old generation suffix.

Timeline IDs are never reused, so we don't have
to worry about the case of create/delete/create cycles. If they
were reused during a disaster recovery "un-delete" of a timeline,
that special case can be handled by calling out to all available pageservers
to check that they return 404 for the timeline, and to flush their
deletion queues in case they had any deletions pending from the
timeline.

The above makes it safe for control plane to change the assignment of
tenant to pageserver in control plane while a timeline creation is ongoing.
The reason is that the creation request against the new assigned pageserver
uses a new generation number. However, care must be taken by control plane
to ensure that a "timeline creation successul" response from some pageserver
is checked for the pageserver's generation for that timeline's tenant still being the latest.
If it is not the latest, the response does not constitute a successful timeline creation.
It is acceptable to discard such responses, the scrubber will clean up the S3 state.
It is better to issue a timelien deletion request to the stale attachment.

#### Timeline Deletion

Tenant/timeline deletion operations are exempt from generation validation
on deletes, and therefore don't have to go through the same deletion
queue as GC/compaction layer deletions. This is because once a
delete is issued by the control plane, it is a promise that the
control plane will keep trying until the deletion is done, so even stale
pageservers are permitted to go ahead and delete the objects.

The implications of this for control plane are:

- During timeline/tenant deletion, the control plane must wait for the deletion to
  be truly complete (status 404) and also handle the case where the pageserver
  becomes unavailable, either by waiting for a replacement with the same node_id,
  or by *re-attaching the tenant elsewhere.

- The control plane must persist its intent to delete
  a timeline/tenant before issuing any RPCs, and then once it starts, it must
  keep retrying until the tenant/timeline is gone. This is already handled
  by using a persistent `Operation` record that is retried indefinitely.

Timeline deletion may result in a special kind of object leak, where
the latest generation attachment completes a deletion (including erasing
all objects in the timeline path), but some slow/partitioned node is
writing into the timeline path with a stale generation number. This would
not be caught by any per-timeline scrubbing (see [scrubbing](#cleaning-up-orphan-objects-scrubbing)), since scrubbing happens on the
attached pageserver, and once the timeline is deleted it isn't attached anywhere.
This scenario should be pretty rare, and the control plane can make it even
rarer by ensuring that if a tenant is in a multi-attached state (e.g. during
migration), we wait for that to complete before processing the deletion. Beyond
that, we may implement some other top-level scrub of timelines in
an external tool, to identify any tenant/timeline paths that are not found
in the control plane database.

#### Examples

- Deletion, node restarts partway through:
  - By the time we returned 202, we have written a remote delete marker
  - Any subsequent incarnation of the same node_id will see the remote
    delete marker and continue to process the deletion
  - If the original pageserver is lost permanently and no replacement
    with the same node_id is available, then the control plane must recover
    by re-attaching the tenant to a different node.
- Creation, node becomes unresponsive partway through.
  - Control plane will see HTTP request timeout, keep re-issuing
    request to whoever is the latest attachment point for the tenant
    until it succeeds.
  - Stale nodes may be trying to execute timeline creation: they will
    write out index_part.json files with
    stale attachment generation: these will be eventually cleaned up
    by the same mechanism as other old indices.

### Unsafe case on badly behaved infrastructure

This section is only relevant if running on a different environment
than EC2 machines with ephemeral disks.

If we ever run pageservers on infrastructure that might transparently restart
a pageserver while leaving an old process running (e.g. a VM gets rescheduled
without the old one being fenced), then there is a risk of corruption, when
the control plane attaches the tenant, as follows:

- If the control plane sends an `/attach` request to node A, then node A dies
  and is replaced, and the control plane's retries the request without
  incrementing that attachment ID, then it could end up with two physical nodes
  both using the same generation number.
- This is not an issue when using EC2 instances with ephemeral storage, as long
  as the control plane never reuses a node ID, but it would need re-examining
  if running on different infrastructure.
- To robustly protect against this class of issue, we would either:
  - add a "node generation" to distinguish between different processes holding the
    same node_id.
  - or, dispense with static node_id entirely and issue an ephemeral ID to each
    pageserver process when it starts.

## Implementation Part 2: Optimizations

### Persistent deletion queue

Between writing our a new index_part.json that doesn't reference an object,
and executing the deletion, an object passes through a window where it is
only referenced in memory, and could be leaked if the pageserver is stopped
uncleanly. That introduces conflicting incentives: on the one hand, we would
like to delay and batch deletions to
1. minimize the cost of the mandatory validations calls to control plane, and
2. minimize cost for DeleteObjects requests.
On the other hand we would also like to minimize leakage by executing
deletions promptly.

To resolve this, we may make the deletion queue persistent
and then executing these in the background at a later time.

_Note: The deletion queue's reason for existence is optimization rather than correctness,
so there is a lot of flexibility in exactly how the it should work,
as long as it obeys the rule to validate generations before executing deletions,
so the following details are not essential to the overall RFC._

#### Scope

The deletion queue will be global per pageserver, not per-tenant. There
are several reasons for this choice:

- Use the queue as a central point to coalesce validation requests to the
  control plane: this avoids individual `Timeline` objects ever touching
  the control plane API, and avoids them having to know the rules about
  validating deletions. This separation of concerns will avoid burdening
  the already many-LoC `Timeline` type with even more responsibility.
- Decouple the deletion queue from Tenant attachment lifetime: we may
  "hibernate" an inactive tenant by tearing down its `Tenant`/`Timeline`
  objects in the pageserver, without having to wait for deletions to be done.
- Amortize the cost of I/O for the persistent queue, instead of having many
  tiny queues.
- Coalesce deletions into a smaller number of larger DeleteObjects calls

Because of the cost of doing I/O for persistence, and the desire to coalesce
generation validation requests across tenants, and coalesce deletions into
larger DeleteObjects requests, there will be one deletion queue per pageserver
rather than one per tenant. This has the added benefit that when deactivating
a tenant, we do not have to drain their deletion queue: deletions can proceed
for a tenant whose main `Tenant` object has been torn down.

#### Flow of deletion

The flow of a deletion is becomes:

1. Need for deletion of an object (=> layer file) is identified.
2. Unlink the object from all the places that reference it (=> `index_part.json`).
3. Enqueue the deletion to a persistent queue.
   Each entry is `tenant_id, attachment_generation, S3 key`.
4. Validate & execute in batches:
  4.1 For a batch of entries, call into control plane.
  4.2 For the subset of entries that passed validation, execute a `DeleteObjects` S3 DELETE request for their S3 keys.

As outlined in the Part 1 on correctness, it is critical that deletions are only
executed once the key is not referenced anywhere in S3.
This property is obviously upheld by the scheme above.

#### We Accept Object Leakage In Acceptable Circumcstances

If we crash in the flow above between (2) and (3), we lose track of unreferenced object.
Further, enqueuing a single to the persistent queue may not be durable immediately to amortize cost of flush to disk.
This is acceptable for now, it can be caught by [the scrubber](#cleaning-up-orphan-objects-scrubbing).

There are various measures we can take to improve this in the future.
1. Cap amount of time until enqueued entry becomes durable (timeout for flush-to-tisk)
2. Proactively flush:
    - On graceful shutdown, as we anticipate that some or
      all of our attachments may be re-assigned while we are offline.
    - On tenant detach.
3. For each entry, keep track of whether it has passed (2).
   Only admit entries to (4) one they have passed (2).
   This requires re-writing / two queue entries (intent, commit) per deletion.

The important take-away with any of the above is that it's not
disastrous to leak objects in exceptional circumstances.

#### Operations that may skip the queue

Deletions of an entire timeline are [exempt](#Timeline-Deletion) from generation number validation. Once the
control plane sends the deletion request, there is no requirement to retain the readability
of any data within the timeline, and all objects within the timeline path may be deleted
at any time from the control plane's deletion request onwards.

Since deletions of smaller timelines won't have enough objects to compose a full sized
DeleteObjects request, it is still useful to send these through the last part of the
deletion pipeline to coalesce with other executing deletions: to enable this, the
deletion queue should expose two input channels: one for deletions that must be
processed in a generation-aware way, and a fast path for timeline deletions, where
that fast path may skip validation and the persistent queue.

### Cleaning up orphan objects (scrubbing)

An orphan object is any object which is no longer referenced by a running node or by metadata.

Examples of how orphan objects arise:

- A node PUTs a layer object, then crashes before it writes the
  index_part.json that references that layer.
- A stale node carries on running for some time, and writes out an unbounded number of
  objects while it believes itself to be the rightful writer for a tenant.
- A pageserver crashes between un-linking an object from the index, and persisting
  the object to its deletion queue.

Orphan objects are functionally harmless, but have a small cost due to S3 capacity consumed. We
may clean them up at some time in the future, but doing a ListObjectsv2 operation and cross
referencing with the latest metadata to identify objects which are not referenced.

Scrubbing will be done only by an attached pageserver (not some third party process), and deletions requested during scrub will go through the same
validation as all other deletions: the attachment generation must be
fresh. This avoids the possibility of a stale pageserver incorrectly
thinking than an object written by a newer generation is stale, and deleting
it.

It is not strictly necessary that scrubbing be done by an attached
pageserver: it could also be done externally. However, an external
scrubber would still require the same validation procedure that
a pageserver's deletion queue performs, before actually erasing
objects.

## Operational impact

### Availability

Coordination of generation numbers via the control plane introduce a dependency for certain
operations:

1. Starting new pageservers (or activating pageservers after a restart)
2. Executing enqueued deletions
3. Advertising updated `remote_consistent_lsn` to enable WAL trimming

Item 1. would mean that some in-place restarts that previously would have resumed service even if the control plane were
unavailable, will now not resume service to users until the control plane is available. We could
avoid this by having a timeout on communication with the control plane, and after some timeout,
resume service with the previous generation numbers (assuming this was persisted to disk). However,
this is unlikely to be needed as the control plane is already an essential & highly available component. Also, having a node reuse an old generation number would complicate
reasoning about the system, as it would break the invariant that a generation number uniquely identifies
a tenant's attachment to a given pageserver _process_: it would merely identify the tenant's attachment
to the pageserver _machine_ or its _on-disk-state_.

Item 2. is a non-issue operationally: it's harmless to delay deletions, the only impact of objects pending deletion is
the S3 capacity cost.

Item 3. could be an issue if safekeepers are low on disk space and the control plane is unavailable for a long time. If this became an issue,
we could adjust the safekeeper to delete segments from local disk sooner, as soon as they're uploaded to S3, rather than waiting for
remote_consistent_lsn to advance.

For a managed service, the general approach should be to make sure we are monitoring & respond fast enough
that control plane outages are bounded in time.

There is also the fact that control plane runs in a single region.
The latency for distant regions is not a big concern for us because all request types added by this RFC are either infrequent or not in the way of the data path.
However, we lose region isolation for the operations listed above.
The ongoing work to split console and control will give us per-region control plane, and all operations in this RFC can be handled by these per-region control planes.
With that in mind, we accept the trade-offs outlined in this paragraph.

We will also implement an "escape hatch" config generation numbers, where in a major disaster outage,
we may manually run pageservers with a hand-selected generation number, so that we can bring them online
independently of a control plane.

### Rollout

Although there is coupling between components, we may deploy most of the new data plane components
independently of the control plane: initially they can just use a static generation number.

#### Phase 1

The pageserver is deployed with some special config to:

- Always act like everything is generation 1 and do not wait for a control plane issued generation on attach
- Skip the places in deletion and remote_consistent_lsn updates where we would call into control plane

#### Phase 2

The control plane changes are deployed: control plane will now track and increment generation numbers.

#### Phase 3

The pageserver is deployed with its control-plane-dependent changes enabled: it will now require
the control plane to service re-attach requests on startup, and handle generation
validation requests.

### On-disk backward compatibility

Backward compatibility with existing data is straightforward:

- When reading the index, we may assume that any layer whose metadata doesn't include
  generations will have a path without generation suffix.
- When locating the index file on attachment, we may use the "fallback" listing path
  and if there is only an index without generation suffix, that is the one we load.

It is not necessary to re-write existing layers: even new index files will be able
to represent generation-less layers.

### On-disk forward compatibility

We will do a two phase rollout, probably over multiple releases because we will naturally
have some of the read-side code ready before the overall functionality is ready:

1. Deploy pageservers which understand the new index format and generation suffixes
   in keys, but do not write objects with generation numbers in the keys.
2. Deploy pageservers that write objects with generation numbers in the keys.

Old pageservers will be oblivious to generation numbers. That means that they can't
read objects with generation numbers in the name. This is why we must
first step must deploy the ability to read, before the second step
starts writing them.

# Frequently Asked Questions

## Why a generation _suffix_ rather than _prefix_?

The choice is motivated by object listing, since one can list by prefix but not
suffix.

In [finding remote indices](#finding-the-remote-indices-for-timelines), we rely
on being able to do a prefix listing for `<tenant>/<timeline>/index_part.json*`.
That relies on the prefix listing.

The converse case of using a generation prefix and listing by generation is
not needed: one could imagine listing by generation while scrubbing (so that
a particular generation's layers could be scrubbed), but this is not part
of normal operations, and the [scrubber](#cleaning-up-orphan-objects-scrubbing) probably won't work that way anyway.

## Wouldn't it be simpler to have a separate deletion queue per timeline?

Functionally speaking, we could. That's how RemoteTimelineClient currently works,
but this approach does not map well to a long-lived persistent queue with
generation validation.

Anything we do per-timeline generates tiny random I/O, on a pageserver with
tens of thousands of timelines operating: to be ready for high scale, we should:

- A) Amortize costs where we can (e.g. a shared deletion queue)
- B) Expect to put tenants into a quiescent state while they're not
  busy: i.e. we shouldn't keep a tenant alive to service its deletion queue.

This was discussed in the [scope](#scope) part of the deletion queue section.

# Appendix A: Examples of use in high availability/failover

The generation numbers proposed in this RFC are adaptable to a variety of different
failover scenarios and models. The sections below sketch how they would work in practice.

### In-place restart of a pageserver

"In-place" here means that the restart is done before any other element in the system
has taken action in response to the node being down.

- After restart, the node issues a re-attach request to the control plane, and
  receives new generation numbers for all its attached tenants.
- Tenants may be activated with the generation number in the re-attach response.
- If any of its attachments were in fact stale (i.e. had be reassigned to another
  node while this node was offline), then
  - the re-attach response will inform the tenant about this by not including
    the tenant of this by _not_ incrementing the generation for that attachment.
  - This will implicitly block deletions in the tenant, but as an optimization
    the pageserver should also proactively stop doing S3 uploads when it notices this stale-generation state.
  - The control plane is expected to eventually detach this tenant from the
    pageserver.

If the control plane does not include a tenant in the re-attach response,
but there is still local state for the tenant in the filesystem, the pageserver
deletes the local state in response and does not load/active the tenant.
See the [earlier section on pageserver startup](#pageserver-attachstartup-changes) for details.
Control plane can use this mechanism to clean up a pageserver that has been
down for so long that all its tenants were migrated away before it came back
up again and asked for re-attach.

### Failure of a pageserver

In this context, read "failure" as the most ambiguous possible case, where
a pageserver is unavailable to clients and control plane, but may still be executing and talking
to S3.

#### Case A: re-attachment to other nodes

1. Let's say node 0 becomes unresponsive in a cluster of three nodes 0, 1, 2.
2. Some external mechanism notices that the node is unavailable and initiates
   movement of all tenants attached to that node to a different node according
   to some distribution rule.
   In this example, it would mean incrementing the generation
   of all tenants that were attached to node 0, as each tenant's assigned pageserver changes.
3. A tenant which is now attached to node 1 will _also_ still be attached to node
   0, from the perspective of node 0. Node 0 will still be using its old generation,
   node 1 will be using a newer generation.
4. S3 writes will continue from nodes 0 and 1: there will be an index_part.json-00000001
   \_and\* an index_part.json-00000002. Objects written under the old suffix
   after the new attachment was created do not matter from the rest of the system's
   perspective: the endpoints are reading from the new attachment location. Objects
   written by node 0 are just garbage that can be cleaned up at leisure. Node 0 will
   not do any deletions because it can't synchronize with control plane, or if it could,
   its deletion queue processing would get errors for the validation requests.

#### Case B: direct node replacement with same node_id and drive

This is the scenario we would experience if running pageservers in some dynamic
VM/container environment that would auto-replace a given node_id when it became
unresponsive, with the node's storage supplied by some network block device
that is attached to the replacement VM/container.

1. Let's say node 0 fails, and there may be some other peers but they aren't relevant.
2. Some external mechanism notices that the node is unavailable, and creates
   a "new node 0" (Node 0b) which is a physically separate server. The original node 0
   (Node 0a) may still be running, because we do not assume the environment fences nodes.
3. On startup, node 0b re-attaches and gets higher generation numbers for
   all tenants.
4. S3 writes continue from nodes 0a and 0b, but the writes do not collide due to different
   generation in the suffix, and the writes from node 0a are not visible to the rest
   of the system because endpoints are reading only from node 0b.

# Appendix B: interoperability with other features

## Sharded Keyspace

The design in this RFC maps neatly to a sharded keyspace design where subsets of the key space
for a tenant are assigned to different pageservers:

- the "unit of work" for attachments becomes something like a TenantShard rather than a Tenant
- TenantShards get generation numbers just as Tenants do.
- Write workload (ingest, compaction) for a tenant is spread out across pageservers via
  TenantShards, but each TenantShard still has exactly one valid writer at a time.

## Read replicas

_This section is about a passive reader of S3 pageserver state, not a postgres
read replica_

For historical reads to LSNs below the remote persistent LSN, any node may act as a reader at any
time: remote data is logically immutable data, and the use of deferred deletion in this RFC helps
mitigate the fact that remote data is not _physically_ immutable (i.e. the actual data for a given
page moves around as compaction happens).

A read replica needs to be aware of generations in remote data in order to read the latest
metadata (find the index_part.json with the latest suffix). It may either query this
from the control plane, or find it with ListObjectsv2 request

## Seamless migration

To make tenant migration totally seamless, we will probably want to intentionally double-attach
a tenant briefly, serving reads from the old node while waiting for the new node to be ready.

This RFC enables that double-attachment: two nodes may be attached at the same time, with the migration destination
having a higher generation number. The old node will be able to ingest and serve reads, but not
do any deletes. The new node's attachment must also avoid deleting layers that the old node may
still use. A new piece of state
will be needed for this in the control plane's definition of an attachment.

## Warm secondary locations

To enable faster tenant movement after a pageserver is lost, we will probably want to spend some
disk capacity on keeping standby locations populated with local disk data.

There's no conflict between this RFC and that: implementing warm secondary locations on a per-tenant basis
would be a separate change to the control plane to store standby location(s) for a tenant. Because
the standbys do not write to S3, they do not need to be assigned generation numbers. When a tenant is
re-attached to a standby location, that would increment the tenant attachment generation and this
would work the same as any other attachment change, but with a warm cache.

## Ephemeral node IDs

This RFC intentionally avoids changing anything fundamental about how pageservers are identified
and registered with the control plane, to avoid coupling the implementation of pageserver split
brain protection with more fundamental changes in the management of the pageservers.

Moving to ephemeral node IDs would provide an extra layer of
resilience in the system, as it would prevent the control plane
accidentally attaching to two physical nodes with the same
generation, if somehow there were two physical nodes with
the same node IDs (currently we rely on EC2 guarantees to
eliminate this scenario). With ephemeral node IDs, there would be
no possibility of that happening, no matter the behavior of
underlying infrastructure.

Nothing fundamental in the pageserver's handling of generations needs to change to handle ephemeral node IDs, since we hardly use the
`node_id` anywhere. The `/re-attach` API would be extended
to enable the pageserver to obtain its ephemeral ID, and provide
some correlation identifier (e.g. EC instance ID), to help the
control plane re-attach tenants to the same physical server that
previously had them attached.
