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

On the write path, we have a dedicated structure to serialize the relation directory into this single key.

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

For (1), we currently have to get the reldir key, deserialize it, and check if the relation exists in the
hashset or not. For (2), we simply get the reldir key and get the hashset. For (3), we need to first get
and deserialize the key, add the new relation record to the hashset, and then serialize it and write it back.

Consider the case we have 100k relations in a database, then we would have a 100k-large hashset. Then, every
relation create and drop would have deserialized and serialized this 100k-large hashset. This makes the
relation create/drop process to be quadratic. And when we check if a relation exists in the ingestion path,
we would have to deserialize this super big 100k-large key before checking if a single relation exists.

In this RFC, we will propose a new way to store the reldir data in the sparse keyspace, and propose how
to seamlessly migrate users to use the new keyspace.

The PoC patch is implemented in [PR10316](https://github.com/neondatabase/neon/pull/10316).

## Key Mapping

We will use the recently-introduced sparse keyspace to store reldir data. Sparse keyspace was proposed in
[038-aux-file-v2.md](038-aux-file-v2.md). The original reldir has one single value of `HashSet<(Oid, u8)>`
for each of the database (identified as `spcnode, dbnode`). We simply encode the `Oid`, which is `relnode, forknum`,
into the key.

```
(REL_DIR_KEY_PREFIX, spcnode, dbnode, relnode, forknum, 1) -> deleted
(REL_DIR_KEY_PREFIX, spcnode, dbnode, relnode, forknum, 1) -> exists
```

Assume all reldir data are stored in this new keyspace, the 3 reldir operations we mentioned before can be
implemented as follows.

1. Check if a relation exists: check if the key maps to "exists".
2. List all relations: do a sprase keyspace scan over the `rel_dir_key_prefix`. Extract relnode and forknum from the key.
3. Create/drop a relation: write "exists" or "deleted" to the corresponding key of the relation.

The mapping is implemented as `rel_tag_sparse_key` in the PoC patch.

## Changes to Sparse Keyspace

Previously, we only use sparse keyspaces for the aux files, which does not carry over when branching. The reldir
information needs to be preserved from the parent branch to the child branch, and therefore, the read path needs
to be updated accordingly to accomodate such "inherited sparse keys". This is done in
[PR#10313](https://github.com/neondatabase/neon/pull/10313).

## Coexistence of the Old and New Keyspaces

Migrating to the new keyspace will be done gradually: when we flip a config item to enable the new reldir keyspace, the
ingestion path will start to write to the new keyspace, and the old reldir data will be kept in the old one. The read
path needs to combine the data from both keyspaces.

In theory, we could do a rewrite at the startup time that scans all relation directories and copy that data into the
new keyspace. However, this could take a long time, especially if we have thousands of tenants doing the migration
process at the same time after the pageserver restarts. Therefore, we propose the co-existence strategy so that the
migration can happen seamlessly and imposes no potential downtime for the user.

With the co-existence assumption, the 3 reldir operations will be implemented as follows:

1. Check if a relation exists
  - Check the new keyspace if the key maps to any value. If it maps to "exists" or "deleted", directly
    return it to the user.
  - Otherwise, deserialize the old reldir key and get the result.
2. List all relations: do a sprase keyspace scan over the `rel_dir_key_prefix`, and deserialize the old reldir key.
   combine them to obtain the final result.
3. Create/drop a relation: write "exists" or "deleted" to the corresponding key of the relation into the new keyspace.
  - Alternatively, when a user removes a relation when the new keyspace is enabled but that relation is created in the
    old reldir key, we can both remove it from the old reldir key and write the delete tombstone in the new keyspace.

We will introduce a config item and an index_part record to record the current status of the migration process.

* Config item `enable_reldir_v2`: controls whether the ingestion path writes the reldir info into the new keyspace.
* `index_part.json` field `reldir_v2_status`: whether the timeline has written any key into the new reldir keyspace.

If `enable_reldir_v2` is set to `true` and the timeline ingests the first key into the new reldir keyspace, it will update
`index_part.json` to set `reldir_v2_status` to `Status::Migrating`. Even if `enable_reldir_v2` gets flipped back to
`false` (i.e., when the pageserver restarts and such config isn't persisted), the read/write path will still
read/write to the new keyspace to avoid data inconsistency.

## Full Migration

This won't be implemented in the first phase of the project, but might be implemented in the future. Having both v1 and
v2 existing in the system would have us keep the code to deserialize the old reldir key forever. To fully deprecate this
code path, we need to ensure the timeline does not have any old reldir data.

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
sparse_reldir_db1_table1 -> deleted
sparse_reldir_db2_table7 -> exists
```

It will generate the following keys in the new image layer:

```
db1/reldir_key -> (table 1, table 2, table 3) + migrated
...db1 rel keys
db2/reldir_key -> (table 4, table 5, table 6) + migrated
...db2 rel keys
sparse_reldir_db1_table1 -> deleted (keep it until the keyspace gets fully migrated)
sparse_reldir_db1_table2 -> exists
sparse_reldir_db1_table3 -> exists
sparse_reldir_db2_table4 -> exists
sparse_reldir_db2_table5 -> exists
sparse_reldir_db2_table6 -> exists
sparse_reldir_db2_table7 -> exists
```

Once we verified that no pending modifications to the old reldir exists in the delta/image layers above the gc-horizon,
we can mark `reldir_v2_status` in the `index_part.json` to `Status::Migrated`, and the read path won't need to read from
the old reldir anymore. Furthermore, image layer generation can drop all keys marked as `deleted` (we need to keep these
deleted keys before the migration is fully completed in case they get created in the old reldir key but deleted in the new
sparse keyspace).

## Next Steps

### Consolidate Relation Size Keys

We have relsize at the end of all relation nodes.

```
// RelSize:
// 00 SPCNODE  DBNODE   RELNODE  FORK FFFFFFFF
```

This means that computing logical size requires us to do several single-key gets across the keyspace,
which potentially requires downloading a lot of layer files. We could consolidate them into a single
keyspace, therefore improving the performance of logical size calculation.

### Migrate DBDir Keys

We assume the number of databases created by the users will be small, and therefore the currently way
of storing database directory would be fine. In the future, we could also migrate DBDir keys into
the sparse keyspace to support large amount of databases.