## How pg_upgrade works in upstream postgres:

0. USER: Initdb new version cluster and prepare it (install extension shared object files to the right path, adjust authentication and config)
1. USER: stop both clusters
2. USER run pg_upgade

3. pg_upgrade: pg_dump of old cluster

```
start_postmaster(&old_cluster)

get_db_and_rel_infos(&old_cluster);
init_tablespaces();
get_loadable_libraries();

// some other checks

generate_old_dump();

stop_postmaster()

```

4. pg_upgrade: check and prepare new cluster

```
start_postmaster(&new_cluster);
check_new_cluster();
    get_db_and_rel_infos(&new_cluster);
	check_new_cluster_is_empty();
	check_loadable_libraries();

set_locale_and_encoding();

// run `vacuumdb --all --analyze` and  `vacuumdb --all --freeze`
prepare_new_cluster();

stop_postmaster();

```

5. pg_upgrade: handle SLRU

```
copy_xact_xlog_xid()
    copy pg_xact files
    set the next transaction id and epoch of the new cluster 

    copy multixact files
    Setting next multixact ID and offset for new cluster
```

6. pg_upgrade: pg_restore to new cluster

```
    // Note that postgres starts in a special --binary-upgrade mode.
	start_postmaster(&new_cluster, true);

	prepare_new_globals();

    // Basically pg_restore, but postgres runs in --binary-upgrade mode
    // and preserves OIDs and relfilenodes of imported objects
	create_new_objects();

	stop_postmaster();
```

7. pg_upgrade: transfer data files


#### Storage compatibility:

1. OID and relfilenode:
pg_upgrade ensures that all assignments of `pg_class.oid` (and `relfilenode`) so toast oids are the same between old and new clusters. 
It also controls assignments of `pg_tablespace.oid`, `pg_type.oid`, `pg_enum.oid` and `pg_authid.oid`.

To do this, it starts postgres in (undocumented) --binary-upgrade mode.

2. Page layout of relation and SLRU

Postgres aims to maintain backward compatibility of page layout and change it lazily in the new version.
In exceptional cases, it provides a function to run during pg_upgrade the page layout.

We need to watch for such changes and probably reimplement them ourselves.

3. Control file, XLOG format
may change, but we use version-specific WAL-redo, so this is not a problem.


## How we can make it work in neon:


#### TLDR:
1. create a new version branch from the old one
2. run pg_upgrade in the background to prepare the new version catalog
3. import the new version catalog into the new branch
4. ?????
5. profit

#### What we have in prototype:
- WAL-redo is version-specific and we can redo WAL using correct binary for page chain before and after the branch_lsn. No known issues here.

- This example uses v14 -> v15 upgrade. I don't see any restrictions on the number of versions we can skip during the upgrade. pg_upgrade should be able to handle it. Our wal-redo mechanism should be able to handle it too.

- We can simplify pg_upgrade sequence a lot, because we can control data visibility using branch LSN. We don't need to stop old cluster for a long time. Plus, we don't need data transfer step, just access the history.

- We have working prototype of pg_upgrade service, where we spin old_cluster as neon_local compute connected to pagesever and new_cluster as just postgres process.

The tricky part is to figure out the value of import_lsn (S) for the new version branch, 
set it in all the right places and make the switch atomic.

#### Let's set the terminology:
```

v14 branch ---------------------|*****X
                                |
v15 initdb + v15 pg_restore  I====R
                                |
v15 branch                      |~~~~~S--------->

```

`|` - branch_lsn. We don't really use it anywhere in the pg_upgrade branching, just use as a reference point.

`I` - initdb_lsn of a new branch. We don't use it anywhere.

`R` - LSN of a new version branch pg_restore catalog import. Let's call it the `restore_lsn`.
We don't care about the history before restore_lsn, becaues we import timeline into branch as a snapshot.
The restore_lsn may be both smaller and larger than the branch_lsn, depending on how active the old branch was and how many objects were created in the new branch by pg_restore (width of `====` on picture). 

`S` - `start_lsn` of a new version branch. 
We upload the prepared snapshot of the new version branch to the storage as of lsn `S`.
This is the similar mechanism that we use to import new main timeline into the storage, but here we skip all non-catalog relation files - new branch will read their content from the old branch.

`****` - writes that happened in v14 during the upgrade process. v15 must be able to read them. 

`X` - final lsn of the old branch. It must be smaller than S. We must somehow restrict writes to the old branch after the version switch. Otherwise we will have data between S and X that will not be readable from the new branch.

#### Complicated steps:

##### How to figure out the value of `S`:
- It must be larger than branch_lsn `|` to kepp the history line correct.
- It must be larger than `R` to ensure that we can read the new objects created by pg_restore. Because otherwise the pages of the new branch will contain LSNs from the future and XLOG flush will break badly.
- It must be larger than `X` to ensure that we don't miss any changes that happened in the old branch during the upgrade.

- IIUC the correct value is `S = MAXALIGN(X+1)`, because we cannot import new timeline at LSN that exists in parent. 
**Need review from the storage team here** to understand how hard this restriction is and if there is any problem with +1.

##### Where to set `S` for the new branch:

- We need to use it as a start_lsn for the new Timeline struct
- We need to set it in the control file of the new branch.
- We need to set in in the Checkpoint record of the new branch.

##### How to make the switch atomic:

- We need to restrict writes to the old branch to get the `X` value and use it as `S` for the new branch.
- IIUC, we need to do this right before the import. This means that timeline import time is customer visible pg_upgrade downtime. Can we somehow make it shorter? What is our target downtime for the upgrade? **Need storage team help here** 


## Known technical issues:

- We mantain relsize cache in neon. We need to copy it over to the new cluster during upgrade.

- Fix import of the new version catalog into the new branch. We need to distinguish between catalog and non-catalog files and only skip the latter.
Now this is done in a very hacky way, that will not work if any catalog files changed their relfilenode before the upgrade.
**Need compute (postgres) team help here**

- We need to update XIDs in the new version controlfile and skip SLRU files during timeline import. See copy_xact_xlog_xid() for details.

- Proablby we need to pass some options to pg_upgrade. Same options that we use for initdb and start (i.e. locale)

- We need to teach storage to handle the gap in the LSN space between the old branch end and new branch start for the scenario where `restore_lsn R` > `branch_lsn |`. See `~~~~~` on the picture, imagine case, where it is larger than `*****`.
**Need storage team help here** to estimate the complexity of this task.

- We need to run the "pg_upgrade microservice" somewhere.
Possible options: 
    - special mode of compute image (requires multi-version compute image).
    - separate microservice running on pageserver machine. Synchronization with old running compute will be complicated here.

## Open questions:

**Need product, storage, compute and cplane teams help here**

- Do we want to implement a full switch to the new version branch? Or do we want to start with "Test new version in a branch" feature? 
Do we see product value in this feature?
It is simpler to implenent, but we need to ensure that users won't use this new-version branch in production. How to do this?

- Do we want to switch main endpoint to use upgraded branch? What if we upgraded from non-main branch?

- How to restrict write access to the old branch after the siwtch?

- How to communicate this pg_upgade switch to the user in UI / API?

- How can we test the data correctness after the upgrade?

- At what moment should we run recommended post-upgrade scripts?

- How to handle extensions? This issue boils down to multi-version compute image.
