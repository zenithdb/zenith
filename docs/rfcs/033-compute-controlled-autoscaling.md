# Compute controlled autoscaling

## Summary

The proposal is to move the responsibility for deciding what the
desired size of a VM is, from the autoscaler agent to the VM monitor.

## Motivation

Currently, the decision to upscale or downscale a VM is made outside
the VM, in the autoscaler-agent, based on the load average and memory
consumption. In addition to that, the VM can override that decision by
making an explicit upscale request, which will bump up the VM size for
a certain period of time.

Moving the decision-making to the compute has several benefits:

- It allows the compute to explicitly request a certain size, which
  can be useful for testing and debugging purposes. We can expose that
  as SQL-callable functions to provide an "escape hatch" for the cases
  where the automatic algorithm doesn't work well.

- It makes it possible for the compute to upscale _before_ making a
  large memory allocation, for example, avoiding OOM. We battled with
  this problem with pgvector, which makes one giant dynamic shared
  memory allocation at CREATE INDEX, which would immediately OOM if
  the instance wasn't already scaled up to accommodate it. We mostly
  solved it by enabling swap and setting dynamic_shared_memory=mmap,
  but it's fiddly. Explicitly scaling up would be more robust.

- It's simpler. Even with no change to the actual algorithm used, it
  is more straightforward to measure the CPU and memory usage directly
  in the VM. The vm-monitor can read the statistics directly from the
  OS, while the agent needs to receive them through Vector. Also, we
  already had the "emergency upscale" request codepath that the
  vm-monitor could use to request immediate upscale.  This proposal
  eliminates that case as a separate thing, all downscale/upscale
  requests follow the same path.

- We can improve the scaling algorithm to take into account more
  information about query plans and resource usage, without having to
  expose all the information to the agent outside the VM.

- Organizationally, the compute team has the PostgreSQL expertise to
  improve the algorithm for choosing the optimal compute size in the
  long run.


## Scope / Non Goals

This proposal is only about choosing the *desired* size of the
VM. There can be reasons that the agent / scheduler cannot grant the
desired resources, e.g if the host is overbooked. Downscaling to the
desired size might also not be possible, because of an inbalance in
Linux memory zones, or if there is a sudden spike in memory usage
after the desired size was last calculated. This proposal doesn't
change what happens in those cases.

What happens between the autoscaler agent, neonvm controller, and to
resize the VM is out of scope. Scheduling VMs on nodes is also out of
scope. Choosing the desired size of a VM is a purely VM-local
decision, and doesn't take into account overall system load.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

VM monitor (libs/vm_monitor) and autoscaling agent.

## Proposed implementation

The current VM monitor <-> autoscaling agent protocol includes an
UpscaleRequest message. The VM monitor sends that message to request
upscaling when memory is running low. The proposal is to add a new
ScaleRequest message, which is similar to UpscaleRequest, but tells
the agent directly what the desired CPU and memory size of the VM
is. When the autoscaler agent receives that message, it tries to
upscale/downscale the VM to the size specified in the message, and
disables theusual metrics-based algorithm.

### Backwards compatibility

This proposal adds a new ScaleRequest message, but doesn't modify the
old message types. The migration path is to deploy the support for the
new message in the autoscaler agent first. After that, new computes
can start using a new version of the VM monitor, which issues
ScaleRequest messages. After all old computes have expired, the
metrics-based algorithm and the code to handle the old UpscaleRequest
message can be removed from the autocaler agent.

### Reliability, failure modes and corner cases

The VM monitor's and autoscaler agent's idea of what the desired and
current VM size is might go out of sync, e.g. if the autoscaler agent
is restarted or some messages are lost. Or if upscaling or downscaling
fails for some reason.  The VM monitor will periodically resend the
ScaleRequest if the actual size of the VM doesn't match the desired
size.

### Interaction/Sequence diagram

Currently, before this proposal, there are two ways that upscaling /
downscaling can be initiated. It can be initiated by an algorithm that
runs in the autoscaler agent, which makes the decision based on metrics
received from Vector running inside the VM:

Current metrics-based scaling:


+--------+   (1) metrics    +------------+
| Vector |  ------------->  | Autoscaler |
+--------+                  | agent      |
                            +------------+
			           |
			           |
           (2) UpscaleNotification |
               / DownscaleRequest  |
			           |
+------------+		           |
| VM monitor |  <------------------/
+------------+
                                   |
                                   |
+------+      (3) QMP plug/unplug  |
| QEMU | <-------------------------/
+------+

The second way to initiate upscaling (but not downscaling!), is that
the VM monitor requests upscaling by sending an UpscaleRequest message
to the agent. The upscale request overrides the metrics-based algorithm's
decision for some period of time:

+------------+   (1) UpscaleRequest
| VM monitor |  <------------------\
+------------+                     |
                                   V
                            +------------+
                            | Autoscaler |
                            | agent      |
                            +------------+
                                   |
+------+      (2) QMP hotplug      |
| QEMU | <-------------------------/
+------+

The proposal is to replace the above two mechanisms with a new
mechanism that is very similar to the old UpscaleRequest message. The
difference is that the new ScaleRequest message can initiate immediate
upscaling but also downscaling, and when the new mechanism is used,
the metrics-based algorithm in the agent is disabled for the VM, and
upscaling/downscaling is only initiated when a new ScaleRequest is
received from the VM monitor.


+------------+   (1) ScaleRequest
| VM monitor |  <------------------\
+------------+                     |
                                   V
                            +------------+
                            | Autoscaler |
                            | agent      |
                            +------------+
                                   |
+------+      (2) QMP hotplug      |
| QEMU | <-------------------------/
+------+


### Scalability (if relevant)

n/a

### Security implications (if relevant)

This doesn't open any new communication paths.

### Unresolved questions (if relevant)

## Alternative implementation (if relevant)

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)
