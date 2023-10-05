# Storage messaging

Safekeepers need to communicate to each other to
* Trim WAL on safekeepers;
* Decide on which SK should push WAL to the S3;
* Decide on when to shut down SK<->pageserver connection;
* Understand state of each other to perform peer recovery;

Pageservers need to communicate to safekeepers to decide which SK should provide
WAL to the pageserver.

This is an iteration on [015-storage-messaging](https://github.com/neondatabase/neon/blob/main/docs/rfcs/015-storage-messaging.md) describing current situation,
potential performance issue and ways to address it.

## Background

What we have currently is very close to etcd variant described in
015-storage-messaging. Basically, we have single `SkTimelineInfo` message
periodically sent by all safekeepers to etcd for each timeline.
* Safekeepers subscribe to it to learn status of peers (currently they subscribe to
  'everything', but they can and should fetch data only for timelines they hold).
* Pageserver subscribes to it (separate watch per timeline) to learn safekeepers
  positions; based on that, it decides from which safekeepers to pull WAL.

Also, safekeepers use etcd elections API to make sure only single safekeeper
offloads WAL.

It works, and callmemaybe is gone. However, this has a performance
hazard. Currently deployed etcd can do about 6k puts per second (using its own
`benchmark` tool); on my 6 core laptop, while running on tmpfs, this gets to
35k. Making benchmark closer to our usage [etcd watch bench](https://github.com/arssher/etcd-client/blob/watch-bench/examples/watch_bench.rs),
I get ~10k received messages per second with various number of publisher-subscribers
(laptop, tmpfs). Diving this by 12 (3 sks generate msg, 1 ps + 3 sk consume them) we
get about 800 active timelines, if message is sent each second. Not extremely
low, but quite reachable.

A lot of idle watches seem to be ok though -- which is good, as pageserver
subscribes to all its timelines regardless of their activity.

Also, running etcd with fsyncs disabled is messy -- data dir must be wiped on
each restart or there is a risk of corruption errors.

The reason is etcd making much more than what we need; it is a fault tolerant
store with strong consistency, but I claim all we need here is just simplest pub
sub with best effort delivery, because
* We already have centralized source of truth for long running data, like which
  tlis are on which nodes  -- the console.
* Momentary data (safekeeper/pageserver progress) doesn't make sense to persist.
  Instead of putting each change to broker, expecting it to reliably deliver it
  is better to just have constant flow of data for active timelines: 1) they
  serve as natural heartbeats -- if node can't send, we shouldn't pull WAL from
  it 2) it is simpler -- no need to track delivery to/from the broker.
  Moreover, latency here is important: the faster we obtain fresh data, the
  faster we can switch to proper safekeeper after failure.
* As for WAL offloading leader election, it is trivial to achieve through these
  heartbeats -- just take suitable node through deterministic rule (min node
  id).  Once network is stable, this is a converging process (well, except
  complicated failure topology, but even then making it converge is not
  hard). Such elections bear some risk of several offloaders running
  concurrently for a short period of time, but that's harmless.

  Generally, if one needs strong consistency, electing leader per se is not
  enough; it must be accompanied with number (logical clock ts), checked at
  every action to track causality. s3 doesn't provide CAS, so it can't
  differentiate old/new leader, this must be solved differently.

  We could use etcd CAS (its most powerful/useful primitive actually) to issue
  these leader numbers (and e.g. prefix files in s3), but currently I don't see
  need for that.


Obviously best effort pub sub is much more simpler and performant; the one proposed is

## gRPC broker

I took tonic and [prototyped](https://github.com/neondatabase/neon/blob/asher/neon-broker/broker/src/broker.rs) the replacement of functionality we currently use
with grpc streams and tokio mpsc channels. The implementation description is at the file header.

It is just 500 lines of code and core functionality is complete. 1-1 pub sub
gives about 120k received messages per second; having multiple subscribers in
different connecitons quickly scales to 1 million received messages per second.
I had concerns about many concurrent streams in single connection, but 2^20
subscribers still work (though eat memory, with 10 publishers 20GB are consumed;
in this implementation each publisher holds full copy of all subscribers). There
is `bench.rs` nearby which I used for testing.

`SkTimelineInfo` is wired here, but another message can be added (e.g. if
pageservers want to communicate with each other) with templating.

### Fault tolerance

Since such broker is stateless, we can run it under k8s. Or add proxying to
other members, with best-effort this is simple.

### Security implications

Communication happens in a private network that is not exposed to users;
additionaly we can add auth to the broker.

## Alternative: get existing pub-sub

We could take some existing pub sub solution, e.g. RabbitMQ, Redis. But in this
case IMV simplicity of our own outweights external dependency costs (RabbitMQ is
much more complicated and needs VM; Redis Rust client maintenance is not
ideal...). Also note that projects like CockroachDB and TiDB are based on gRPC
as well.

## Alternative: direct communication

Apart from being transport, broker solves one more task: discovery, i.e. letting
safekeepers and pageservers find each other. We can let safekeepers know, for
each timeline, both other safekeepers for this timeline and pageservers serving
it. In this case direct communication is possible:
 - each safekeeper pushes to each other safekeeper status of timelines residing
   on both of them, letting remove WAL, decide who offloads, decide on peer
   recovery;
 - each safekeeper pushes to each pageserver status of timelines residing on
   both of them, letting pageserver choose from which sk to pull WAL;

It was mostly described in [014-safekeeper-gossip](https://github.com/neondatabase/neon/blob/main/docs/rfcs/014-safekeepers-gossip.md), but I want to recap on that.

The main pro is less one dependency: less moving parts, easier to run Neon
locally/manually, less places to monitor. Fault tolerance for broker disappears,
no kuber or something. To me this is a big thing.

Also (though not a big thing) idle watches for inactive timelines disappear:
naturally safekeepers learn about compute connection first and start pushing
status to pageserver(s), notifying it should pull.

Importantly, I think that eventually knowing and persisting peers and
pageservers on safekeepers is inevitable:
- Knowing peer safekeepers for the timeline is required for correct
  automatic membership change -- new member set must be hardened on old
  majority before proceeding. It is required to get rid of sync-safekeepers
  as well (peer recovery up to flush_lsn).
- Knowing pageservers where the timeline is attached is needed to
  1. Understand when to shut down activity on the timeline, i.e. push data to
     the broker. We can have a lot of timelines sleeping quietly which
	 shouldn't occupy resources.
  2. Preserve WAL for these (currently we offload to s3 and take it from there,
     but serving locally is better, and we get one less condition on which WAL
     can be removed from s3).

I suppose this membership data should be passed to safekeepers directly from the
console because
1. Console is the original source of this data, conceptually this is the
   simplest way (rather than passing it through compute or something).
2. We already have similar code for deleting timeline on safekeepers
   (and attaching/detaching timeline on pageserver), this is a typical
   action -- queue operation against storage node and execute it until it
   completes (or timeline is dropped).

Cons of direct communication are
- It is more complicated: each safekeeper should maintain set of peers it talks
  to, and set of timelines for each such peer -- they ought to be multiplexed
  into single connection.
- Totally, we have O(n^2) connections instead of O(n) with broker schema
  (still O(n) on each node). However, these are relatively stable, async and
  thus not very expensive, I don't think this is a big problem. Up to 10k
  storage nodes I doubt connection overhead would be noticeable.

I'd use gRPC for direct communication, and in this sense gRPC based broker is a
step towards it.
