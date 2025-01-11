# Context

In Neon, one host runs a lot of compute nodes. Most of the compute
nodes are fairly small, 1-2 vCPUs and 1-2 GB of memory. But some
compute nodes can be larger, and we also want to autoscale from small
to large, and back again, without shutting down the compute.

    +----------------------------+
    |      Host                  |
    | +------------------------+ |
    | |    Container VM        | |               +------------+
    | | +--------------------+ | |               |            |
    | | |  Postgres          | | |               | Pageserver |
    | | |                    | | |               |            |
    | | |  ################  | | |               |            |
    | | |  # shared_      #  | | |               +------------+
    | | |  # buffers      #  | | |
    | | |  ################  | | |
    | | |                    | | |
    | | +--------------------+ | |
    | +------------------------+ |
    +----------------------------+

Each Postgres runs in a qemu VM, which in turn runs in a kubernetes
pod. Kubernetes manages the placement of these Postgres compute nodes
to hosts.

# Problem

PostgreSQL normally relies heavily on the kernel page cache for
performance. PostgreSQL has its own buffer cache, configured by the
shared_buffers setting, but the usual advice is to set shared_buffers
to around 10-20% of the overall system RAM available, leaving the rest
for the kernel page cache. However with Neon, the I/O operations don't
go through the kernel filesystem layer, so we bypass the kernel page
cache and rely solely on the Postgres shared buffer cache for caching
in the compute node.

Because we don't make use of the kernel page cache, we have to either
set shared_buffers larger than you would with normal PostgreSQL, or
you send a lot more I/O requests to the pageserver than you otherwise
would. However in PostgreSQL, shared_buffers setting cannot be changed
while the server is running.

Furthermore, we have fast local SSDs available in the compute hosts
that we could also utilize for caching.



# Solution 1: Scale shared buffers

This solution consists of:

- Core PostgreSQL changes to allow changing shared_buffers on the fly

- New code to orchestrate changing the memory size of the VM, and tell
  PostgreSQL to change the shared_buffers setting accordingly.

- New code to Postgres that measures current shared buffer cache usage
  to determine what the "cache pressure" is, i.e. how useful it would
  be to have a larger shared buffer cache. This could be in an
  extension.
  
- A new governor in the host that chooses which VM to allocate
  how much memory.

Picture:

    +----------------------------+
    |      Host                  |
    | +------------------------+ |
    | |    Container VM        | |               +------------+
    | | +--------------------+ | |               |            |
    | | |  Postgres          | | |               | Pageserver |
    | | |                    | | |               |            |
    | | |  ################  | | |               |            |
    | | |  # shared_      #  | | |               +------------+
    | | |  # buffers      #  | | |
    | | |  #              #  | | |
    | | |  .    |         .  | | |
    | | |  .    |         .  | | |
    | | |  .    V         .  | | |
    | | |  ################  | | |
    | | |                    | | |
    | | +--------------------+ | |
    | +------------------------+ |
    +----------------------------+

Pros:

- best possible performance for the cached data

Cons:

- Scales only memory, cannot take advantage of local SSDs in host machine
- Needs explicit operations to scale. Won't dynamicaly share resources
  between tenants, we'll need to start a resizing process to change
  the allocations.
- Needs patches to core PostgreSQL


# Alternative 2: Local filesystem cache

Add code to Postgres Neon extension to use a local file on disk for
caching.  When a page is evicted from Postgres buffer cache, write it
to the local file, and read it back if it's requested again. Rely on
kernel page cache to keep the most hot part of that file in memory.

Like in solution 1, need a governor in the host to allocate the local
disk for each VM, and orchestration to scale it up and down.


    +----------------------------+
    |      Host                  |
    | +------------------------+ |
    | |    Container VM        | |               +------------+
    | | +--------------------+ | |               |            |
    | | |  Postgres          | | |               | Pageserver |
    | | |                    | | |               |            |
    | | |  ################  | | |               |            |
    | | |  # shared_      #  | | |               +------------+
    | | |  # buffers      #  | | |
    | | |  ################  | | |
    | | |                    | | |
    | | |  ################  | | |
    | | |  # Local FS     #  | | |
    | | |  # cache        #  | | |
    | | |  #              #  | | |
    | | |  ################  | | |
    | | |                    | | |
    | | +--------------------+ | |
    | +------------------------+ |
    +----------------------------+

Pros:

- No PostgreSQL core changes required
- Automatically takes advantage of local SSDs, not just memory

Cons:

- Needs explicit operations to change the size of the cache file
- Need support for live migration of the filesystem


Question:
How is the page cache shared between the host kernel and the VMs? Does
each VM maintain their own page cache? I think that depends on the
filesystem and qemu driver we choose. If we use a raw block device and
let the VM manage it, I believe the VM will maintain the page
cache. But if we use a driver to access the host filesystem directly,
or use something like NFS, I'm not sure.


# Alternative 3: Host cache

Implement a new host cache process that intercepts all the I/O
requests from all VMs running on the host, and manages a local cache.
Postgres can communicate with the host cache using TCP, or via custom
virtio driver.

    +----------------------------+
    |      Host                  |
    |                            |
    |   ######################   |
    |   #                    #   |
    |   # shared host cache  #   |
    |   #                    #   |
    |   #                    #   |
    |   #                    #   |
    |   #                    #   |
    |   ######################   |
    |                            |
    | +------------------------+ |
    | |    Container VM        | |               +------------+
    | | +--------------------+ | |               |            |
    | | |  Postgres          | | |               | Pageserver |
    | | |                    | | |               |            |
    | | |  ################  | | |               |            |
    | | |  # shared_      #  | | |               +------------+
    | | |  # buffers      #  | | |
    | | |  ################  | | |
    | | |                    | | |
    | | +--------------------+ | |
    | +------------------------+ |
    +----------------------------+

Pros:
- dynamic sharing between all tenants (one cache and eviction policy for all)
- No PostgreSQL core changes required
- Takes advantage of local SSDs, not just memory

Cons:

- Whole new component to write


One way to achieve this would be to collocate the pageserver on the
host itself. That would eliminate the network roundtrip between
Postgres and the pageserver, effectively making the pageserver itself
be the host shared cache.
