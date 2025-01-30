# Safekeeper peer recovery

## A problem and proposed solution

Currently, on start walproposer preserves WAL for all safekeepers: on start it
determines the horizon beyond which all safekeepers received WAL, downloads
missing part to pg_wal and holds WAL since this horizon ever since. This is
problematic, because

1. If one safekeeper is down and/or lagging, pg_wal eventually explodes -- we intentionally don't have much space on computes.
2. If one safekeeper is down and/or lagging it makes compute start longer.

Proposed solution is to teach safekeepers to fetch WAL directly from peers,
respecting consensus rules. Namely,
- On start, walproposer won't download WAL at all -- it will have it only since
  writing position. As WAL grows it should also keep some fixed number of
  latest segments (~20) to provide gradual switch from peer recovery to walproposer
  streaming under load; it can be achieved by setting wal_keep_size or
  implemented separately.
- Whenever safekeeper through peer communication discovers that 1) it lacks WAL compared to some 
  peer and 2) walproposer streaming is not active, it starts recovery. Recovery ends when either there 
  is nothing more to fetch or streaming walproposer is discovered.

## Details

### Correctness

The idea is simple: recovery process imitates actions of donor's last_log_term
== donor's term leader. That is, sk A will fetch WAL from sk B if
1) B's (last_log_term, LSN) is higher than A's (last_log_term, LSN) *and* 
2) A's term <= B's term -- otherwise append request can't be accepted. 
3) B's term == B's last_log_term -- to ensure that such a leader was ever elected in 
   the first place.

Note that not always such configuration is possible. e.g. in scenario
A 1.1
B 1.1 2.1
C 1.1 3.1

where (x.y) is (term, LSN) pair if A voted for term 4 and B and C haven't (their
terms are 3 and 2 respectively), then A can't pull from B nor from C. IOW, we
need elected authoritative leader to determine to correct log sequence. However,
such scenario is unlikely and will be fixed by walproposer voting once it
appears, so we can ignore it for now, and add elections on safekeepers side
later if needed.

Just like a normal leader, recovery would first truncate WAL and only then start
inserting.

### Start/stop criterion

Recovery shouldn't prevent actively streaming compute -- we don't skip records,
so if recovery inserts something after walproposer push, next will error out.
OTOH, for better availability recovery should finish its job aligning all
safekeepers even if compute is missing. So I propose to track on safekeeper
existence of streaming compute. Recovery should kick in if 1) there is something
to pull and 2) streaming compute doesn't exist. On each insert, compute presence
is checked and recovery is terminated if it appeared. It also terminates if
there is nothing more to pull.

This should be good enough, though not bullet proof: in theory we can imagine
recovery starting regularly before streaming started and inserting something
after. Such loop is very unlikely though, we can add more heuristics if it shows
up.

## Alternatives

An entirely different direction would be more granular WAL managing on computes
-- don't hold a lot, but download and pass on demand to stale safekeepers. It
seems of comparable complexity, but writing rust is more pleasant and less
postgres version dependant.