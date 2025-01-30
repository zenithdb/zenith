# Synthetic size

## How to get the data

Pageserver provides a HTTP API for getting the synthetic size of a tenant
along with the data that was used to calculate it. Usage examples:

1. This query returns the synthetic size of the tenant, along with the "raw" data
That is returned in the `segments` and `timeline_inputs` fields.

```
curl localhost:9898/v1/tenant/5e1de642394b00a0a583a088e8276b98/synthetic_size | jq
```

2. If `inputs_only=true` is passed, the response will contain only the raw data.
Actual synthetic size is not calculated.

```
curl localhost:9898/v1/tenant/5e1de642394b00a0a583a088e8276b98/synthetic_size?inputs_only=true | jq

```

3. 'retention_period' is a cutoff (in bytes) that overrides the cutoff that is used in the size calculation.
Note, that override is applied only if provided `retnention_period` is shorter than the real cutoff.

```
curl localhost:9898/v1/tenant/5e1de642394b00a0a583a088e8276b98/synthetic_size?retention_period=1048576 | jq
```

4. If header `Accept: text/html` is passed, the response will be in HTML format.
The HTML contains a json with the same data as in the previous examples + SVG diagram of the tenant timelines.

```
curl -H "Accept: text/html" localhost:9898/v1/tenant/5e1de642394b00a0a583a088e8276b98/synthetic_size > ./size.html |  google-chrome ./size.html
```

## Overview

Neon storage has copy-on-write branching, which makes it difficult to
answer the question "how large is my database"? To give one reasonable
answer, we calculate _synthetic size_ for a project.

The calculation is called "synthetic", because it is based purely on
the user-visible logical size, which is the size that you would see on
a standalone PostgreSQL installation, and the amount of WAL, which is
also the same as what you'd see on a standalone PostgreSQL, for the
same set of updates.

The synthetic size does *not* depend on the actual physical size
consumed in the storage, or implementation details of the Neon storage
like garbage collection, compaction and compression.  There is a
strong *correlation* between the physical size and the synthetic size,
but the synthetic size is designed to be independent of the
implementation details, so that any improvements we make in the
storage system simply reduce our COGS. And vice versa: any bugs or bad
implementation where we keep more data than we would need to, do not
change the synthetic size or incur any costs to the user.

The synthetic size is calculated for the whole project. It is not
straightforward to attribute size to individual branches. See [What is
the size of an individual branch?](#what-is-the-size-of-an-individual-branch)
for a discussion of those difficulties.

The synthetic size is designed to:

- Take into account the copy-on-write nature of the storage. For
  example, if you create a branch, it doesn't immediately add anything
  to the synthetic size. It starts to affect the synthetic size only
  as it diverges from the parent branch.

- Be independent of any implementation details of the storage, like
  garbage collection, remote storage, or compression.

## Terms & assumptions

- logical size is the size of a branch *at a given point in
  time*. It's the total size of all tables in all databases, as you
  see with "\l+" in psql for example, plus the Postgres SLRUs and some
  small amount of metadata. Note that currently, Neon does not include
  the SLRUs and metadata in the logical size. Refer to the comment in
  [`get_current_logical_size_non_incremental()`](/pageserver/src/pgdatadir_mapping.rs#L813-L814).

- a "point in time" is defined as an LSN value. You can convert a
  timestamp to an LSN, but the storage internally works with LSNs.

- PITR horizon can be set per-branch.

- PITR horizon can be set as a time interval, e.g. 5 days or hours, or
  as amount of WAL, in bytes.  If it's given as a time interval, it's
  converted to an LSN for the calculation.

- PITR horizon can be set to 0, if you don't want to retain any history.

## Calculation

Inputs to the calculation are:
- logical size of the database at different points in time,
- amount of WAL generated, and
- the PITR horizon settings

The synthetic size is based on an idealistic model of the storage
system, where we pretend that the storage consists of two things:
- snapshots, containing a full snapshot of the database, at a given
  point in time, and
- WAL.

In the simple case that the project contains just one branch (main),
and a fixed PITR horizon, the synthetic size is the sum of:

- the logical size of the branch *at the beginning of the PITR
  horizon*, i.e. at the oldest point that you can still recover to, and
- the size of the WAL covering the PITR horizon.

The snapshot allows you to recover to the beginning of the PITR
horizon, and the WAL allows you to recover from that point to any
point within the horizon.

```
                             WAL
   -----------------------#########>
                          ^
                       snapshot

Legend:
  ##### PITR horizon. This is the region that you can still access
        with Point-in-time query and you can still create branches
        from.
  ----- history that has fallen out of the PITR horizon, and can no
        longer be accessed
```

NOTE: This is not how the storage system actually works! The actual
implementation is also based on snapshots and WAL, but the snapshots
are taken for individual database pages and ranges of pages rather
than the whole database, and it is much more complicated. This model
is a reasonable approximation, however, to make the synthetic size a
useful proxy for the actual storage consumption.


## Example: Data is INSERTed

For example, let's assume that your database contained 10 GB of data
at the beginning of the PITR horizon, and you have since then inserted
5 GB of additional data into it. The additional insertions of 5 GB of
data consume roughly 5 GB of WAL. In that case, the synthetic size is:

> 10 GB (snapshot) +  5 GB (WAL) = 15 GB

If you now set the PITR horizon on the project to 0, so that no
historical data is retained, then the beginning PITR horizon would be
at the end of the branch, so the size of the snapshot would be
calculated at the end of the branch, after the insertions. Then the
synthetic size is:

> 15 GB (snapshot) + 0 GB (WAL) = 15 GB.

In this case, the synthetic size is the same, regardless of the PITR horizon,
because all the history consists of inserts. The newly inserted data takes
up the same amount of space, whether it's stored as part of the logical
snapshot, or as WAL. (*)

(*) This is a rough approximation. In reality, the WAL contains
headers and other overhead, and on the other hand, the logical
snapshot includes empty space on pages, so the size of insertions in
WAL can be smaller or greater than the size of the final table after
the insertions. But in most cases, it's in the same ballpark.

## Example: Data is DELETEd

Let's look at another example:

Let's start again with a database that contains 10 GB of data. Then,
you DELETE 5 GB of the data, and run VACUUM to free up the space, so
that the logical size of the database is now only 5 GB.

Let's assume that the WAL for the deletions and the vacuum take up
100 MB of space. In that case, the synthetic size of the project is:

> 10 GB (snapshot) + 100 MB (WAL) = 10.1 GB

This is much larger than the logical size of the database after the
deletions (5 GB). That's because the system still needs to retain the
deleted data, because it's still accessible to queries and branching
in the PITR window.

If you now set the PITR horizon to 0 or just wait for time to pass so
that the data falls out of the PITR horizon, making the deleted data
inaccessible, the synthetic size shrinks:

> 5 GB (snapshot) + 0 GB (WAL) = 5 GB


# Branching

Things get more complicated with branching. Branches in Neon are
copy-on-write, which is also reflected in the synthetic size.

When you create a branch, it doesn't immediately change the synthetic
size at all. The branch point is within the PITR horizon, and all the
data needed to recover to that point in time needs to be retained
anyway.

However, if you make modifications on the branch, the system needs to
keep the WAL of those modifications. The WAL is included in the
synthetic size.

## Example: branch and INSERT

Let's assume that you again start with a 10 GB database.
On the main branch, you insert 2 GB of data. Then you create
a branch at that point, and insert another 3 GB of data on the
main branch, and 1 GB of data on the child branch

```
  child                 +#####>
                        |
                        |    WAL
  main    ---------###############>
                   ^
                snapshot
```

In this case, the synthetic size consists of:
- the snapshot at the beginning of the PITR horizon (10 GB)
- the WAL on the main branch (2 GB + 3 GB = 5 GB)
- the WAL on the child branch (1 GB)

Total: 16 GB

# Diverging branches

If there is only a small amount of changes in the database on the
different branches, as in the previous example, the synthetic size
consists of a snapshot before the branch point, containing all the
shared data, and the WAL on both branches. However, if the branches
diverge a lot, it is more efficient to store a separate snapshot of
branches.

## Example: diverging branches

You start with a 10 GB database. You insert 5 GB of data on the main
branch. Then you create a branch, and immediately delete all the data
on the child branch and insert 5 GB of new data to it. Then you do the
same on the main branch. Let's assume
that the PITR horizon requires keeping the last 1 GB of WAL on the
both branches.

```
                              snapshot
                                  v     WAL
  child                 +---------##############>
                        |
                        |
  main     -------------+---------##############>
                                  ^     WAL
                              snapshot
```

In this case, the synthetic size consists of:
- snapshot at the beginning of the PITR horizon on the main branch (4 GB)
- WAL on the main branch (1 GB)
- snapshot at the beginning of the PITR horizon on the child branch (4 GB)
- last 1 GB of WAL on the child branch (1 GB)

Total: 10 GB

The alternative way to store this would be to take only one snapshot
at the beginning of branch point, and keep all the WAL on both
branches.  However, the size with that method would be larger, as it
would require one 10 GB snapshot, and 5 GB + 5 GB of WAL. It depends
on the amount of changes (WAL) on both branches, and the logical size
at the branch point, which method would result in a smaller synthetic
size. On each branch point, the system performs the calculation with
both methods, and uses the method that is cheaper, i.e. the one that
results in a smaller synthetic size.

One way to think about this is that when you create a branch, it
starts out as a thin branch that only stores the WAL since the branch
point.  As you modify it, and the amount of WAL grows, at some point
it becomes cheaper to store a completely new snapshot of the branch
and truncate the WAL.


# What is the size of an individual branch?

Synthetic size is calculated for the whole project, and includes all
branches. There is no such thing as the size of a branch, because it
is not straightforward to attribute the parts of size to individual
branches.

## Example: attributing size to branches

(copied from https://github.com/neondatabase/neon/pull/2884#discussion_r1029365278)

Imagine that you create two branches, A and B, at the same point from
main branch, and do a couple of small updates on both branches. Then
six months pass, and during those six months the data on the main
branch churns over completely multiple times. The retention period is,
say 1 month.

```
                      +------> A
                     /
--------------------*-------------------------------> main
                     \
                      +--------> B
```

In that situation, the synthetic tenant size would be calculated based
on a "logical snapshot" at the branch point, that is, the logical size
of the database at that point. Plus the WAL on branches A and B. Let's
say that the snapshot size is 10 GB, and the WAL is 1 MB on both
branches A and B. So the total synthetic storage size is 10002
MB. (Let's ignore the main branch for now, that would be just added to
the sum)

How would you break that down per branch? I can think of three
different ways to do it, and all of them have their own problems:

### Subtraction method

For each branch, calculate how much smaller the total synthetic size
would be, if that branch didn't exist. In other words, how much would
you save if you dropped the branch. With this method, the size of
branches A and B is 1 MB.

With this method, the 10 GB shared logical snapshot is not included
for A nor B. So the size of all branches is not equal to the total
synthetic size of the tenant. If you drop branch A, you save 1 MB as
you'd expect, but also the size of B suddenly jumps from 1 MB to 10001
MB, which might feel surprising.

### Division method

Divide the common parts evenly across all branches that need
them. With this method, the size of branches A and B would be 5001 MB.

With this method, the sum of all branches adds up to the total
synthetic size. But it's surprising in other ways: if you drop branch
A, you might think that you save 5001 MB, but in reality you only save
1 MB, and the size of branch B suddenly grows from 5001 to 10001 MB.

### Addition method

For each branch, include all the snapshots and WAL that it depends on,
even if some of them are shared by other branches. With this method,
the size of branches A and B would be 10001 MB.

The surprise with this method is that the sum of all the branches is
larger than the total synthetic size. And if you drop branch A, the
total synthetic size doesn't fall by 10001 MB as you might think.

# Alternatives

A sort of cop-out method would be to show the whole tree of branches
graphically, and for each section of WAL or logical snapshot, display
the size of that section. You can then see which branches depend on
which sections, which sections are shared etc. That would be good to
have in the UI anyway.

Or perhaps calculate per-branch numbers using the subtraction method,
and in addition to that, one more number for "shared size" that
includes all the data that is needed by more than one branch.

## Which is the right method?

The bottom line is that it's not straightforward to attribute the
synthetic size to individual branches. There are things we can do, and
all of those methods are pretty straightforward to implement, but they
all have their own problems. What makes sense depends a lot on what
you want to do with the number, what question you are trying to
answer.
