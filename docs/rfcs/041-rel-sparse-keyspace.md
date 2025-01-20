# Sparse Keyspace for Relation Directories

## Summary

This is an RFC describing a new storage strategy for storing relation directories.

## Motivation

Postgres maintains a directory structure for databases and relations. In Neon, we store these information
by serializing the directory data in a single key (see `pgdatadir_mapping.rs`).

```
// DbDir:
// 00 00000000 00000000 00000000 00   00000000

// RelDir:
// 00 SPCNODE  DBNODE   00000000 00   00000001 (Postgres never uses relfilenode 0)
```

We have a dedicated structure on the ingestion path to serialize the relation directory into this single key.

```rust
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct RelDirectory {
    // Set of relations that exist. (relfilenode, forknum)
    //
    // TODO: Store it as a btree or radix tree or something else that spans multiple
    // key-value pairs, if you have a lot of relations
    pub(crate) rels: HashSet<(Oid, u8)>,
}
```

The current codebase has the following two access patterns for the relation directory.

1. Check if a relation exists.
2. List all relations.
3. Create/drop a relation.

For (1), we currently have to get the reldir key, deserialize it, and check whether the relation exists in the
hash set. For (2), we get the reldir key and the hash set. For (3), we need first to get
and deserialize the key, add the new relation record to the hash set, and then serialize it and write it back.

If we have 100k relations in a database, we would have a 100k-large hash set. Then, every
relation created and dropped would have deserialized and serialized this 100k-large hash set. This makes the
relation create/drop process to be quadratic. When we check if a relation exists in the ingestion path,
we would have to deserialize this super big 100k-large key before checking if a single relation exists.

In this RFC, we will propose a new way to store the reldir data in the sparse keyspace and propose how
to seamlessly migrate users to use the new keyspace.

The PoC patch is implemented in [PR10316](https://github.com/neondatabase/neon/pull/10316).

## Key Mapping

We will use the recently introduced sparse keyspace to store actual data. Sparse keyspace was proposed in
[038-aux-file-v2.md](038-aux-file-v2.md). The original reldir has one single value of `HashSet<(Oid, u8)>`
for each of the databases (identified as `spcnode, dbnode`). We encode the `Oid` (`relnode, forknum`),
into the key.

```
(REL_DIR_KEY_PREFIX, spcnode, dbnode, relnode, forknum, 1) -> deleted
(REL_DIR_KEY_PREFIX, spcnode, dbnode, relnode, forknum, 1) -> exists
```

Assume all reldir data are stored in this new keyspace; the 3 reldir operations we mentioned before can be
implemented as follows.

1. Check if a relation exists: check if the key maps to "exists".
2. List all relations: scan the sprase keyspace over the `rel_dir_key_prefix`. Extract relnode and forknum from the key.
3. Create/drop a relation: write "exists" or "deleted" to the corresponding key of the relation. The delete tombstone will
   be removed during image layer generation upon compaction.

Note that "exists" and "deleted" will be encoded as a single byte as two variants of an enum.
The mapping is implemented as `rel_tag_sparse_key` in the PoC patch.

## Changes to Sparse Keyspace

Previously, we only used sparse keyspaces for the aux files, which did not carry over when branching. The reldir
information needs to be preserved from the parent branch to the child branch. Therefore, the read path needs
to be updated accordingly to accommodate such "inherited sparse keys". This is done in
[PR#10313](https://github.com/neondatabase/neon/pull/10313).

## Coexistence of the Old and New Keyspaces

Migrating to the new keyspace will be done gradually: when we flip a config item to enable the new reldir keyspace, the
ingestion path will start to write to the new keyspace and the old reldir data will be kept in the old one. The read
path needs to combine the data from both keyspaces.

Theoretically, we could do a rewrite at the startup time that scans all relation directories and copies that data into the
new keyspace. However, this could take a long time, especially if we have thousands of tenants doing the migration
process simultaneously after the pageserver restarts. Therefore, we propose the coexistence strategy so that the
migration can happen seamlessly and imposes no potential downtime for the user.

With the coexistence assumption, the 3 reldir operations will be implemented as follows:

1. Check if a relation exists
   - Check the new keyspace if the key maps to any value. If it maps to "exists" or "deleted", directly
    return it to the user.
   - Otherwise, deserialize the old reldir key and get the result.
2. List all relations: scan the sparse keyspace over the `rel_dir_key_prefix` and deserialize the old reldir key.
   Combine them to obtain the final result.
3. Create/drop a relation: write "exists" or "deleted" to the corresponding key of the relation into the new keyspace.
   - We assume no overwrite of relations will happen (i.e., the user won't create a relation at the same Oid). This will be implemented as a runtime check.
   - For relation creation, we add `sparse_reldir_tableX -> exists` to the keyspace.
   - For relation drop, we first check if the relation is recorded in the old keyspace. If yes, we deserialize the old reldir key,
    remove the relation, and then write it back. Otherwise, we put `sparse_reldir_tableX -> deleted` to the keyspace.
   - The delete tombstone will be removed during image layer generation upon compaction.

This process ensures that the transition will not introduce any downtime and all new updates are written to the new keyspace. The total
amount of data in the storage would be `O(relations_modifications)` and we can guarantee `O(current_relations)` after compaction.
There could be some relations that exist in the old reldir key for a long time. Refer to the "Full Migration" section on how to deal
with them.

We will introduce a config item and an index_part record to record the current status of the migration process.

* Config item `enable_reldir_v2`: controls whether the ingestion path writes the reldir info into the new keyspace.
* `index_part.json` field `reldir_v2_status`: whether the timeline has written any key into the new reldir keyspace.

If `enable_reldir_v2` is set to `true` and the timeline ingests the first key into the new reldir keyspace, it will update
`index_part.json` to set `reldir_v2_status` to `Status::Migrating`. Even if `enable_reldir_v2` gets flipped back to
`false` (i.e., when the pageserver restarts and such config isn't persisted), the read/write path will still
read/write to the new keyspace to avoid data inconsistency.

## Full Migration

This won't be implemented in the project's first phase but might be implemented in the future. Having both v1 and
v2 existing in the system would force us to keep the code to deserialize the old reldir key forever. To entirely deprecate this
code path, we must ensure the timeline has no old reldir data.

We can trigger a special image layer generation process at the gc-horizon. The generated image layers will cover several keyspaces:
the old reldir key in each of the databases, and the new reldir sparse keyspace. It will remove the old reldir key while
copying them into the corresponding keys in the sparse keyspace in the resulting image. This special process happens in
the background during compaction. For example, assume this special process is triggered at LSN 0/180. The `create_image_layers`
process discovers the following keys at this LSN.

```
db1/reldir_key -> (table 1, table 2, table 3)
...db1 rel keys
db2/reldir_key -> (table 4, table 5, table 6)
...db2 rel keys
sparse_reldir_db2_table7 -> exists
sparse_reldir_db1_table8 -> deleted
```

It will generate the following keys:

```
db1/reldir_key -> ()
...db1 rel keys
db2/reldir_key -> ()
...db2 rel keys

-- start image layer for the sparse keyspace at sparse_reldir_prefix
sparse_reldir_db1_table1 -> exists
sparse_reldir_db1_table2 -> exists
sparse_reldir_db1_table3 -> exists
sparse_reldir_db2_table4 -> exists
sparse_reldir_db2_table5 -> exists
sparse_reldir_db2_table6 -> exists
sparse_reldir_db2_table7 -> exists
-- end image layer for the sparse keyspace at sparse_reldir_prefix+1

# The `sparse_reldir_db1_table8` key gets dropped as part of the image layer generation code for the sparse keyspace.
# Note that the read path will stop reading if a key is not found in the image layer covering the key range so there
# are no correctness issue.
```

Once we verified that no pending modifications to the old reldir exists in the delta/image layers above the gc-horizon,
we can mark `reldir_v2_status` in the `index_part.json` to `Status::Migrated`, and the read path won't need to read from
the old reldir anymore. We can do a vectored read to get the full key history of the old reldir key and ensure there
are no more images above the gc-horizon.

The migration process can be proactively triggered across all attached/detached tenants to help us fully remove the old reldir code.

## Next Steps

### Consolidate Relation Size Keys

We have relsize at the end of all relation nodes.

```
// RelSize:
// 00 SPCNODE  DBNODE   RELNODE  FORK FFFFFFFF
```

This means that computing logical size requires us to do several single-key gets across the keyspace,
potentially requiring downloading many layer files. We could consolidate them into a single
keyspace, improving logical size calculation performance.

### Migrate DBDir Keys

We assume the number of databases created by the users will be small, and therefore, the current way
of storing the database directory would be acceptable. In the future, we could also migrate DBDir keys into
the sparse keyspace to support large amount of databases.
