# Read-only replicas

We want to be able to spin-up read-only compute nodes.

## Scope of this feature

- We want to be able to spin-up multiple read-only replicas.
- We should be able to spin-up RO node, if primary is inactive.

- We don't want "snapshot" read-only computes at fixed LSN. Users can use branches for that.
- We don't need replica promotion. For failower we rely on quick primary node restarts.

- Implement it for v15 first.

## Design

### Replica creation

1. At the moment of endpoint creation, user should be able to specify that they want to create a read-only endpoint.
This setting will be passed via compute spec to `compute_ctl`. The replica will be created in the same region as primary (and all project's branches).

Q: Should we allow to specify region for replica? It will affect latency and costs.

2. `compute_ctl` will spin up a read-only replica.

3. replica then connects to safekeepers and starts streaming WAL from them
It would be good to connect to the safekeeper in the same AZ to reduce network latency.

### Configuration & UI

We take the same approach as with primary nodes and don't expose any configuration knobs to users.
It may be useful to add a dashboard to show replica lag.

### Replication

- Replica receives WAL stream from safekeeper to keep up with primary.
- Replica is connected to pageserver to receive page images.

There are two cases of WAL apply:

- page is already in buffer cache.
In this case, replica should update page's `LastWriteLSN` and apply the WAL record to the page.

NOTE: We must either hold buffer lock (pin) or update `LastWriteLSN` before applying WAL record. Otherwise concurrent process may request a stale page from pageserver.

- page is not in buffer cache
In this case, replica should update `LastWriteLSN` and ignore the WAL record.

NOTE: We must be careful about atomicity of operations that modify multiple pages. See buffer lock coupling/crabbing ( https://github.com/neondatabase/neon/issues/383) for details. Most likely everything will work fine, but we need to test it properly.


To request page, replica should use current WAL applyLSN to make sure that we don't get pages from the future.


## Safekeepers

- safekeepers should be able to stream WAL to multiple replicas and handle their feedback.

- lagging replica can make safekeepers to keep a lot of WAL.
If they can offload WAL to S3 and download it back on demand, it will be fine.
