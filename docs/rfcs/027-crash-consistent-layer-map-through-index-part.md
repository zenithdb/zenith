
# Crash-Consistent Layer Map Updates By Leveraging `index_part.json`

* Created on: Aug 23, 2023
* Author: Christian Schwarz

## Summary

This RFC describes a simple scheme to make layer map updates crash consistent by leveraging the `index_part.json` in remote storage.
Without such a mechanism, crashes can induce certain edge cases in which broadly held assumptions about system invariants don't hold.

## Motivation

### Background

We can currently easily make complex, atomic updates to the layer map by means of an RwLock.
If we crash or restart pageserver, we reconstruct the layer map from:
1. local timeline directory contents
2. remote `index_part.json` contents.

The function that is responsible for this is called `Timeline::load_layer_map()`.
The reconciliation process's behavior is the following:
* local-only files will become part of the layer map as local-only layers and rescheduled for upload
* For a file name that, by its name, is present locally and in the remote `index_part.json`, but where the local file has a different size (future: checksum) than the remote file, we will delete the local file and leave the remote file as a `RemoteLayer` in the layer map.

### The Problem

There are are cases where we need to make an atomic update to the layer map that involves **more than one layer**.
The best example is compaction, where we need to insert the L1 layers generated from the L0 layers, and remove the L0 layers.
As stated above, making the update to the layer map in atomic way is trivial.
But, there is no system call API to make an atomic update to a directory that involves more than one file rename and deletion.
Currently, we issue the system calls one by one and hope we don't crash.

What happens if we crash and restart in the middle of that system call sequence?
We will reconstruct the layer map according to the reconciliation process, taking as input whatever transitory state the timeline directory ended up in.

We cannot roll back or complete the timeline directory update during which we crashed, because we keep no record of the changes we plan to make.

### Problem's Implications For Compaction

The implications of the above are primarily problematic for compaction.
Specifically, the part of it that compacts L0 layers into L1 layers.

Remember that compaction takes a set of L0 layers and reshuffles the delta records in them into L1 layer files.
Once the L1 layer files are written to disk, it atomically removes the L0 layers from the layer map and adds the L1 layers to the layer map.
It then deletes the L0 layers locally, and schedules an upload of the L1 layers and and updated index part.

If we crash before deleting L0s, but after writing out L1s, the next compaction after restart will re-digest the L0s and produce new L1s.
This means the compaction after restart will **overwrite** the previously written L1s.
Currently we also schedule an S3 upload of the overwritten L1.

If the compaction algorithm doesn't change between the two compaction runs, is deterministic, and uses the same set of L0s as input, then the second run will produce identical L1s and the overwrites will go unnoticed.

*However*:
1. the file size of the overwritten L1s may not be identical, and
2. the bit pattern of the overwritten L1s may not be identical, and,
3. in the future, we may want to make the compaction code non-determinstic, influenced by past access patterns, or otherwise change it, resulting in L1 overwrites with a different set of delta records than before the overwrite

The items above are a problem for the [split-brain protection RFC](https://github.com/neondatabase/neon/pull/4919) because it assumes that layer files in S3 are only ever deleted, but never replaced (overPUTted).

For example, if an unresponsive node A becomes active again after control plane has relocated the tenant to a new node B, the node A may overwrite some L1s.
But node B based its world view on the version of node A's `index_part.json` from _before_ the overwrite.
That earlier `index_part.json`` contained the file size of the pre-overwrite L1.
If the overwritten L1 has a different file size, node B will refuse to read data from the overwritten L1.
Effectively, the data in the L1 has become inaccessible to node B.
If node B already uploaded an index part itself, all subsequent attachments will use node B's index part, and run into the same probem.

If we ever introduce checksums instead of checking just the file size, then a mismatching bit pattern (2) will cause similar problems.

In case of (1) and (2), where we know that the logical content of the layers is still the same, we can recover by manually patching the `index_part.json` of the new node to the overwritten L1's file size / checksum.

But if (3) ever happens, the logical content may be different, and, we could have truly lost data.

Given the above considerations, we should avoid making correctness of split-brain protection dependent on overwrites preserving _logical_ layer file contents.
**It is a much cleaner separation of concerns to require that layer files are truly immutable in S3, i.e., PUT once and then only DELETEd, never overwritten (overPUTted).**

## Design

Instead of reconciling a layer map from local timeline directory contents and remote index part, this RFC proposes to view the remote index part as authoritative during timeline load.
Local layer files will be recognized if they match what's listed in remote index part, and removed otherwise.

During **timeline load**, the only thing that matters is the remote index part content.
Essentially, timeline load becomes much like attach, except we don't need to prefix-list the remote timelines.
The local timeline dir's `metadata` file does not matter.
The layer files in the local timeline dir are seen as a nice-to-have cache of layer files that are in the remote index part.
Any layer files in the local timeline dir that aren't in the remote index part are removed during startup.
The `Timeline::load_layer_map()` no longer "merges" local timeline dir contents with the remote index part.
Instead, it treats the remote index part as the authoritative layer map.
If the local timeline dir contains a layer that is in the remote index part, that's nice, and we'll reuse it if file size (and in the future, check sum) match what's stated in the index part.
If it doesn't match, we remove the file from the local timeline dir.

After load, **at runtime**, nothing changes compared to what we did before this RFC.
The procedure for single- and multi-object changes is reproduced here for reference:
* For any new layers that the change adds:
  * Write them to a temporary location.
  * While holding layer map lock:
    * Move them to the final location.
    * Insert into layer map.
* Make the S3 changes.
  We won't reproduce the remote timeline client method calls here because these are subject to change.
  Instead we reproduce the sequence of s3 changes that must result for a given single-/multi-object change:
    * PUT layer files inserted by the change.
    * PUT an index part that has insertions and deletions of the change.
    * DELETE the layer files that are deleted by the change.

Note that it is safe for the DELETE to be deferred arbitrarily.
* If it never happens, we leak the object, but, that's not a correctness concern.
* As of #4938, we don't schedule the remote timeline client operation for deletion immediately, but, only when we drop the `LayerInner`.
* With the [split-brain protection RFC](https://github.com/neondatabase/neon/pull/4919), the deletions will be written to deletion queue for processing when it's safe to do so (see the RFC for details).

## How This Solves The Problem

If we crash before we've finished the S3 changes, then timeline load will reset layer map to the state that's in the S3 index part.
The S3 change sequence above is obviously crash-consistent.
If we crash before the index part PUT, then we leak the inserted layer files to S3.
If we crash after the index part PUT, we leak the to-be-DELETEd layer files to S3.
Leaking is fine, it's a pre-existing condition and not addressed in this RFC.

Multi-object changes that previously created and removed files in timeline dir are now atomic because the layer map updates are atomic and crash consistent:
* atomic layer map update at runtime, currently by using an RwLock in write mode
* atomic `index_part.json` update in S3, as per guarantee that S3 PUT is atomic
* local timeline dir state:
  * irrelevant for layer map content => irrelevant for atomic updates / crash consistency
  * if we crash after index part PUT, local layer files will be used, so, no on-demand downloads neede for them
  * if we crash before index part PUT, local layer files will be deleted

## Trade-Offs

### Fundamental

If we crash before finishing the index part PUT, we lose all the work that hasn't reached the S3 `index_part.json`:
* wal ingest: we lose not-yet-uploaded L0s; load on the **safekeepers** + work for pageserver
* compaction: we lose the entire compaction iteration work; need to re-do it again
* gc: no change to what we have today

If the work is still deemed necessary after restart, the restarted restarted pageserver will re-do this work.
The amount of work to be re-do is capped to the lag of S3 changes to the local changes.
Assuming upload queue allows for unlimited queue depth (that's what it does today), this means:
* on-demand downloads that were needed to do the work: are likely still present, not lost
* wal ingest: currently unbounded
* L0 => L1 compaction: CPU time proportional to `O(sum(L0 size))` and upload work proportional to `O()`
  * Compaction threshold is 10 L0s and each L0 can be up to 256M in size. Target size for L1 is 128M.
  * In practive, most L0s are tiny due to 10minute `DEFAULT_CHECKPOINT_TIMEOUT`.
* image layer generation: CPU time `O(sum(input data))` + upload work `O(sum(new image layer size))`
  * I have no intuition how expensive / long-running it is in reality.
* gc: `update_gc_info`` work (not substantial, AFAIK)

To limit the amount of lost upload work, and ingest work, we can limit the upload queue depth (see suggestions in the next sub-section).
However, to limit the amount of lost CPU work, we would need a way to make make the compaction/image-layer-generation algorithms interruptible & resumable.
We aren't there yet, the need for it is tracked by ([#4580](https://github.com/neondatabase/neon/issues/4580)).
However, this RFC is not constraining the design space either.

### Practical

#### Pageserver Restarts

Pageserver crashes are very rare ; it would likely be acceptable to re-do the lost work in that case.
However, regular pageserver restart happen frequently, e.g., during weekly deploys.

In general, pageserver restart faces the problem of tenants that "take too long" to shut down.
They are a problem because other tenants that shut down quickly are unavailble while we wait for the slow tenants to shut down.
We currently allot 10 seconds for graceful shutdown until we SIGKILL the pageserver process (as per `pageserver.service` unit file).
A longer budget would expose tenants that are done early to a longer downtime.
A short budget would risk throwing away more work that'd have to be re-done after restart.

In the context of this RFC, killing the process would mean losing the work that hasn't made it to S3.
We can mitigate this problem as follows:
0. initially, by accepting that we need to do the work again
1. short-term, introducing measures to cap the amount of in-flight work:

   - cap upload queue length, use backpressure to slow down compaction
   - disabling compaction/image-layer-generation X minutes before `systemctl restart pageserver`
   - introducing a read-only shutdown state for tenants that are fast to shut down;
     that state would be equivalent to the state of a tenant in hot standby / readonly mode.

2. mid term, by not restarting pageserver in place, but using [*seamless tenant migration*](https://github.com/neondatabase/neon/pull/5029) to drain a pageserver's tenants before we restart it.

#### `disk_consistent_lsn` can go backwards

`disk_consistent_lsn` can go backwards across restarts if we crash before we've finished the index part PUT.
Nobody should care about it, because the only thing that matters is `remote_consistent_lsn`.
Compute certainly doesn't care about `disk_consistent_lsn`.


## Side-Effects Of This Design

* local `metadata` is basically reduced to a cache of which timelines exist for this tenant; i.e., we can avoid a `ListObjects` requests for a tenant's timelines during tenant load.

## Limitations

Multi-object changes that span multiple timelines aren't covered by this RFC.
That's fine because we currently don't need them, as evidenced by the absence
of a Pageserver operation that holds multiple timelines' layer map lock at a time.

## Impacted components

Primarily pageservers.

Safekeepers will experience more load when we need to re-ingest WAL because we've thrown away work.
No changes to safekeepers are needed.

## Alternatives considered

### Alternative 1: WAL

We could have a local WAL for timeline dir changes, as proposed here https://github.com/neondatabase/neon/issues/4418 and partially implemented here https://github.com/neondatabase/neon/pull/4422 .
The WAL would be used to
1. make multi-object changes atomic
2. replace `reconcile_with_remote()` reconciliation: scheduling of layer upload would be part of WAL replay.

The WAL is appealing in a local-first world, but, it's much more complex than the design described above:
* New on-disk state to get right.
* Forward- and backward-compatibility development costs in the future.

### Alternative 2: Flow Everything Through `index_part.json`

We could have gone to the other extreme and **only** update the layer map whenever we've PUT `index_part.json`.
I.e., layer map would always be the last-persisted S3 state.
That's axiomatically beautiful, not least because it fully separates the layer file production and consumption path (=> [layer file spreading proposal](https://www.notion.so/neondatabase/One-Pager-Layer-File-Spreading-Christian-eb6b64182a214e11b3fceceee688d843?pvs=4)).
And it might make hot standbys / read-only pageservers less of a special case in the future.

But, I have some uncertainties with regard to WAL ingestion, because it needs to be able to do some reads for the logical size feedback to safekeepers.

And it's silly that we wouldn't be able to use the results of compaction or image layer generation before we're done with the upload.

Lastly, a temporarily clogged-up upload queue (e.g. S3 is down) shouldn't immediately render ingestion unavailable.

### Alternative 3: Sequence Numbers For Layers

Instead of what's proposed in this RFC, we could use unique numbers to identify layer files:

```
# before
tenants/$tenant/timelines/$timeline/$key_and_lsn_range
# after
tenants/$tenant/timelines/$timeline/$layer_file_id-$key_and_lsn_range
```

To guarantee uniqueness, the unqiue number is a sequence number, stored in `index_part.json`.

This alternative does not solve atomic layer map updates.
In our crash-during-compaction scenario above, the compaction run after the crash will not overwrite the L1s, but write/PUT new files with new sequence numbers.
In fact, this alternative makes it worse because the data is now duplicated in the not-overwritten and overwritten L1 layer files.
We'd need to write a deduplication pass that checks if perfectly overlapping layers have identical contents.

However, this alternative is appealing because it systematically prevents overwrites at a lower level than this RFC.

So, this alternative is sufficient for the needs of the split-brain safety RFC (immutable layer files locally and in S3).
But it doesn't solve the problems with crash-during-compaction outlined earlier in this RFC, and in fact, makes it much more accute.
The proposed design in this RFC addresses both.

So, if this alternative sounds appealing, we should implement the proposal in this RFC first, then implement this alternative on top.
That way, we avoid a phase where the crash-during-compaction problem is accute.

## Related issues

- https://github.com/neondatabase/neon/issues/4749
- https://github.com/neondatabase/neon/issues/4418
  - https://github.com/neondatabase/neon/pull/4422
- https://github.com/neondatabase/neon/issues/5077
- https://github.com/neondatabase/neon/issues/4088
  - (re)resolutions:
    - https://github.com/neondatabase/neon/pull/4696
    - https://github.com/neondatabase/neon/pull/4094
      - https://neondb.slack.com/archives/C033QLM5P7D/p1682519017949719

Note that the test case introduced in https://github.com/neondatabase/neon/pull/4696/files#diff-13114949d1deb49ae394405d4c49558adad91150ba8a34004133653a8a5aeb76 will produce L1s with the same logical content, but, as outlined in the last paragraph of the _Problem Statement_ section above, we don't want to make that  assumption in order to fix the problem.


## Implementation Plan

1. Remove support for `remote_storage=None`, because we now rely on the existence of an index part.

    - The nasty part here is to fix all the tests that fiddle with the local timeline directory.
      Possibly they are just irrelevant with this change, but, each case will require inspection.

2. Implement the design above.

    - Initially, ship without the mitigations for restart and accept we will do some work twice.
    - Measure the impact and implement one of the mitigations.

