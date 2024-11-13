# AUX file v2

## Summary

This is a retrospective RFC describing a new storage strategy for AUX files.

## Motivation

The original aux file storage strategy stores everything in a single `AUX_FILES_KEY`.
Every time the compute node streams a `neon-file` record to the pageserver, it will
update the aux file hash map, and then write the serialized hash map into the key.
This creates serious space bloat. There was a fix to log delta records (i.e., update
a key in the hash map) to the aux file key. In this way, the pageserver only stores
the deltas at each of the LSNs. However, this improved v1 storage strategy still
requires us to store everything in an aux file cache in memory, because we cannot
fetch a single key (or file) from the compound `AUX_FILES_KEY`.

### Prior art

For storing large amount of small files, we can use a key-value store where the key
is the filename and the value is the file content.

## Requirements

- No space bloat, fixed space amplification.
- No write bloat, fixed write amplification.

## Impacted Components

pageserver

## Sparse Keyspace

In pageserver, we had assumed the keyspaces are always contiguous. For example, if the keyspace 0x0000-0xFFFF
exists in the pageserver, every single key in the key range would exist in the storage. Based on the prior
assumption, there are code that traverses the keyspace by iterating every single key.

```rust
loop {
    // do something
    key = key.next();
}
```

If a keyspace is very large, for example, containing `2^64` keys, this loop will take infinite time to run.
Therefore, we introduce the concept of sparse keyspace in this RFC. For a sparse keyspace, not every key would
exist in the key range. Developers should not attempt to iterate every single key in the keyspace. Instead,
they should fetch all the layer files in the key range, and then do a merge of them.

In aux file v2, we store aux files within the sparse keyspace of the prefix `AUX_KEY_PREFIX`.

## AUX v2 Keyspace and Key Mapping

Pageserver uses fixed-size keys. The key is 128b. In order to store files of arbitrary filenames into the
keyspace, we assign a predetermined prefix based on the directory storing the aux file, and use the FNV hash
of the filename for the rest bits of the key. The encoding scheme is defined in `encode_aux_file_key`.

For example, `pg_logical/mappings/test1` will be encoded as:

```
62 0000 01 01 7F8B83D94F7081693471ABF91C
^ aux prefix
        ^ assigned prefix of pg_logical/
           ^ assigned prefix of mappings/
              ^ 13B FNV hash of test1
   ^ not used due to key representation
```

The prefixes of the directories should be assigned every time we add a new type of aux file into the storage within `aux_file.rs`. For all directories without an assigned prefix, it will be put into the `0xFFFF` keyspace.

Note that inside pageserver, there are two representations of the keys: the 18B full key representation
and the 16B compact key representation. For the 18B representation, some fields have restricted ranges
of values. Therefore, the aux keys only use the 16B compact portion of the full key.

It is possible that two files get mapped to the same key due to hash collision. Therefore, the value of
each of the aux key is an array that contains all filenames and file content that should be stored in
this key.

We use `Value::Image` to store the aux keys. Therefore, page reconstruction works in the same way as before,
and we do not need addition code to support reconstructing the value. We simply get the latest image from
the storage.

## Inbound Logical Replication Key Mapping

For inbound logical replication, Postgres needs the `replorigin_checkpoint` file to store the data.
This file not directly stored in the pageserver using the aux v2 mechanism. It is constructed during
generating the basebackup by scanning the `REPL_ORIGIN_KEY_PREFIX` keyspace.

## Sparse Keyspace Read Path

There are two places we need to read the aux files from the pageserver:

* On the write path, when the compute node adds an aux file to the pageserver, we will retrieve the key from the storage, append the file to the hashed key, and write it back. The current `get` API already supports that.
*  We use the vectored get API to retrieve all aux files during generating the basebackup. Because we need to scan a sparse keyspace, we slightly modified the vectored get path. The vectorized API used to always attempt to retrieve every single key within the requested key range, and therefore, we modified it in a way that keys within `NON_INHERITED_SPARSE_RANGE` will not trigger missing key error. Furthermore, as aux file reads usually need all layer files intersecting with that key range within the branch and cover a big keyspace, it incurs large overhead for tracking keyspaces that have not been read. Therefore, for sparse keyspaces, we [do not track](https://github.com/neondatabase/neon/pull/9631) `ummapped_keyspace`.

## Compaction and Image Layer Generation

With the add of sparse keyspaces, we also modified the compaction code to accommodate the fact that sparse keyspaces do not have every single key stored in the storage.

* L0 compaction: we modified the hole computation code so that it can handle sparse keyspaces when computing holes.
* Image layer creation: instead of calling `key.next()` and getting/reconstructing images for every single key, we use the vectored get API to scan all keys in the keyspace at a given LSN. Image layers are only created if there are too many delta layers between the latest LSN and the last image layer we generated for sparse keyspaces. The created image layer always cover the full aux key range for now, and could be optimized later.

## Migration

We decided not to make the new aux storage strategy (v1) compatible with the original one (v1). One feasible way of doing a seamless migration is to store new data in aux v2 while old data in aux v1, but this complicates file deletions. We want all users to start with a clean state with no aux files in the storage, and therefore, we need to do manual migrations for users using aux v1 by using the [migration script](https://github.com/neondatabase/aux_v2_migration).

During the period of migration, we store the aux policy in the `index_part.json` file. When a tenant is attached
with no policy set, the pageserver will scan the aux file keyspaces to identify the current aux policy being used (v1 or v2).

If a timeline has aux v1 files stored, it will use aux file policy v1 unless we do a manual migration for them. Otherwise, the default aux file policy for new timelines is aux v2. Users enrolled in logical replication before we set aux v2 as default use aux v1 policy. Users who tried setting up inbound replication (which was not supported at that time) may also create some file entries in aux v1 store, even if they did not enroll in the logical replication testing program.

The code for aux v2 migration is in https://github.com/neondatabase/aux_v2_migration. The toolkit scans all projects with logical replication enabled. For all these projects, it put the computes into maintenance mode (suspend all of then), call the migration API to switch the aux file policy on the pageserver (which drops all replication states), and restart all the computes.
