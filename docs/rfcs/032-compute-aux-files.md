# Postgres aux files storage

In the current Neon architecture, compute nodes can only persist data through WAL replication to storage nodes. This method covers most of the data that Postgres persists, but not all. Exceptions include:

* Replication slots
* Postgres statistics files (`pg_stat/pgstat.stat`)
* `pg_stat_statements` disk state
* `pg_prewarm` disk state

It may also be beneficial to store observability data, such as compute metrics and page access histograms, in the per-endpoint storage.

## Aux file types

The mentioned files have different requirements regarding durability, exhibit different I/O patterns, vary in expected size, and have different desired behaviors with respect to point-in-time recovery and read-only replicas.

### Replication Slots

Replication slots persist data about the replication receiver on the replication source. In the case of chain replication, these files could be modified by a replica (because it may have another attached replica). The ability to modify this state on a replica is the main reason for this data to be kept in a separate file instead of in the usual relation (ignoring hint bits, replicas can't modify relation data).

* Stale file: Not allowed, as Postgres might have already pruned WAL or vacuumed old catalog entries (relevant in the case of logical replication).
* Lost file: Postgres will start, but replication will not be able to proceed.
* Point-in-time recovery (PITR): Resetting a slot to a previous point in time is meaningful in that it allows replication to continue from that point. However, it's hard to envision a use case where this would be useful, as the replica has to be reset in a similar fashion. Considering that a stale slot will cause WAL and catalog to grow without garbage collection, we decided to delete slots upon branch creation. This decision aligns with Postgres documentation's suggestion to omit `pg_replslot` from backups.

There are several types of files:

* Slot state, `pg_replslot/<slot_name>/state`, fixed size, about 200 bytes; updated on checkpoint.
* LogicalRewriteHeap, `pg_logical/mappings/map-*`, size varies with the size of the mapping; updated on checkpoint.
* SnapBuild state, `pg_logical/snapshots/*`, fixed structure plus an array of transaction IDs, size can reach up to kilobytes; updated on checkpoint or with running_xact WAL record.
* ReplicationOrigin, `pg_logical/replorigin_checkpoint`, fixed size, capped at 10 bytes; updated on checkpoint.

An important point is that these files could be modified on the replica in cases of chain replication or logical decoding at standby. Our approach with WAL logging comes at the expense of not being able to support chain replication and logical decoding at standby.

### PGStat: Postgres' cumulative statistics system

* Stale file: Not acceptable, as Postgres discards it on a dirty stop.
* Lost file: Postgres will work, but there may be a potential impact on query plans' quality and autovacuum performance.
* PITR: Resetting pgstat is sensible if we can retrieve a relevant version of the file. Since dump/restore operations are only performed on stop/start, we might need to modify Postgres code to dump the file periodically and handle an outdated file on start, which seems more practical than trying to WAL-log changes to stats.

Files are written only on shutdown and read only on startup.  Their size depends on the number of relations and columns, typically ranging from 100KB to 1MB, but can exceed that.  In my tests, the database had stats file around 1KB and it compressed 10x with gzip.

Stored in `pg_stat/pgstat.stat`.
### Files written by extensions

Potentially any extension can write files upon shutdown that it may want to retrieve upon startup.
PostgreSQL extensions, like pg_stat_statements, can manage persistent data across server restarts by writing to files during shutdown and reading from them during startup. This is achieved through PostgreSQL's support for extension hooks and background workers that can execute custom code at specific points in the server's lifecycle, including startup and shutdown.

*Writing Data on Shutdown:* Extensions can use the shmem_exit hook to register a function that PostgreSQL will call during its shutdown sequence. In the case of pg_stat_statements, it registers a function that serializes the collected SQL statement statistics to a file (e.g., pg_stat/pg_stat_statements.stat). This serialization process involves writing the data to a disk in a format that the extension can later read.

See 
https://github.com/neondatabase/postgres/blob/f7ea954989a2e7901f858779cff55259f203479a/contrib/pg_stat_statements/pg_stat_statements.c#L556

*Reading Data on Startup:* Upon server startup, after the extension is loaded, it can read the previously saved file to restore the statistics. This is typically done by registering a shmem_startup_hook in the _PG_init function of the extension, which is called when the extension is loaded into the PostgreSQL server process. The extension checks for the presence of its data file and loads it to initialize its in-memory data structures with the saved statistics.

See
https://github.com/neondatabase/postgres/blob/f7ea954989a2e7901f858779cff55259f203479a/contrib/pg_stat_statements/pg_stat_statements.c#L461


*Ensuring Data Integrity:* To ensure data integrity across unexpected shutdowns, extensions like pg_stat_statements can also leverage PostgreSQL's WAL (Write-Ahead Logging) for critical data that must survive crashes. However, for performance statistics, a simple file-based persistence mechanism is often sufficient and involves less overhead.

The following important extensions are supported by Neon and we could either provide an extension specific mechanism to survive their AUX files or write a generic mechanism that can support more extensions in their hooks.
#### pg_stat_statements

* Stale file: Not currently supported, but could be added if desired.
* Lost file: Results in lost stats, but everything else will work.

The file is only saved on a clean shutdown and read on startup. The maximum size depends on the `pgss_max` setting (the maximum amount of tracked queries), with the default value being 5000. Using `sqlsmith` to generate random SQL queries and fill the statement cache to 4967 entries, the pg_stat/pg_stat_statements.stat file was 12MB and compressed to 2.1MB with gzip.

#### pg_prewarm

No stale file support currently, but it could be added if needed.

This data is only saved on a clean shutdown and read on startup, stored in the `autoprewarm.blocks` file. The size is `NBuffers * 20 bytes`. `NBuffers` is the number of 8KB pages in shared buffers. Thus, for 16GB shared buffers, this file would be approximately 40MB and will grow linearly with the size of the shared buffers. We should note that in Neon the size of shared buffers might change between supend and start due to autoscaling.

## Summary

Files associated with replication slots differ from others in two main aspects:
1. Durability Requirement: The durability requirement for replication slot files is higher. Losing a replication slot file means it cannot be regenerated, and replication will consequently break
2. Access Pattern: Access to these files occurs while Postgres is running, not just at startup and shutdown.

For statistics files, the stakes for durability are lower since these files can be regenerated, albeit with a performance cost.

For all of that files Point-In-Time Recovery (PITR) is not strictly necessary. It would be a nice-to-have feature but is not essential.

## Possible storages

### s3fs

Mounting an `/<branch_id>/<endpoint_id>` directory into each running compute node is a viable option.

Pros:

* Eliminates the need to modify Postgres code with special file access routines for WAL logging on write or some API access on read/write.
* The file system API provides built-in access laziness, meaning prewarm, stats, and PGSS files don't need to be downloaded before database startup (background workers can read them as needed).

Cons:

* Mount time adds to start latency. Some slot files must be downloaded for Postgres to start, requiring careful batch access to prevent slow starts with thousands of small files. TODO: A detailed list of files that can block start needs to be compiled. TODO: check if snapshot files need to be read at startup.

It not clear how to exactly mount S3 in our setup. We should restrict access to only a certain prefix, e.g. `/<branch_id>/<endpoint_id>`. Then we have to mount it somehow. AWS has `s3-csi`, but it is mounted on pod start and with our approach of pre-created computes we don't know endpoint/tenant during pod start. So we have to manage mount inside pod. That means setting `cap_admin` on pod. So not sure how to do that with pod. On the othe hand things are easier with NeonVM: in each pod we have VM and we have root access to it, so we can mount S3.
*Alternative:*

* create one filesystem for all tenants but encrypt its content with tenant-specific keys (using stackable encryption system like eCryptFS that can be mounted on top of CSI filesystem)
* mount the filesystem on pod start
* configure each tenant with the eCryptFS encryption key for its prefix
* on compute start the tenant mounts the eCryptFS for its' prefix files 
TODO: Determine how to share credentials with the VM and restrict access to a specific prefix.

Note: S3FS is not suited for general-purpose file systems due to its limitations, such as no in-place updates and high latency. However, it is suitable for the files discussed here.

### EBS

EBS is per-Availability Zone (AZ), introducing a dependency on AZ for branch/endpoint, which is undesirable.

### EFS

Built on top of NFS, EFS could be a potential solution but shares similar mounting issues with S3FS. Restricting access to specific paths is more challenging, and past issues with Vector on EFS leading to corrupted files (possibly due to Vector) make it a less favorable option.

### Logical messages + basebackup

This is our current approach.

Pros:
* Eliminates the need for an additional service, whether internal or external.

Cons:
* Lacks start-up laziness as all files are loaded via base backup
* Does not support file updates on replicas, which are necessary for chain replication and logical decoding on standby

### Centralized service

Pros:
* Flexible

Cons:
* Requires instrumentation for all access, both reads and writes, complicating the implementation.
