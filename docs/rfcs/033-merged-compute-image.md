## Merged compute image
https://github.com/neondatabase/neon/issues/6685

### Motivation:
It's hard to manage compute pools for 3 Postgres versions.  
(we have a compute image for each version of Postgres (currently, it's 3 for neonVM and 3 for k8s pods; eventually, we will have only neonVMs)).
We can try putting all Postgres versions into a single image, which should dramatically improve pool usage.

### TODO
#### Compute code changes:

1. Create merged compute image https://github.com/neondatabase/neon/pull/6808
2. Pass compute version in spec from control-plane
3. Change path to the postgres in compute_ctl. Now it is not specified explicitly.
   `compute_ctl` has `pgbin` and `pgdata` arguments, now they are used only in tests.
3.  Make changes to custom_extension code - fix path handling.

#### Control-plane changes:
1. Pass compute version in spec from control-plane
2. Remove old logic of VM pools management

#### Prewarm changes:
Currently, for pooled VMs, we prewarm postgres to improve cold start speed
```
    // If this is a pooled VM, prewarm before starting HTTP server and becoming
    // available for binding. Prewarming helps Postgres start quicker later,
    // because QEMU will already have it's memory allocated from the host, and
    // the necessary binaries will already be cached.
```

Prewarm = initdb + start postgres + rm pgdata

Q: How should we do prewarm, if we don't know in adwance, what version of postgres will be used?
I see two options:
- use versioned pgdata directories and run prewarm operations for all existing versions.
- chose "default_version" for each pooled VM and run prewarm. Try to start compute in pooled VM with matching version, in case it doesn't exist, spin compute in any existing VM. Start will be slower, because it is not prewarmed.

#### Extensions support
To support merged compute image (image, containing all supported versions of postgres),
we need to offload extensions from the image. We can implement this using "custom extensions" mechanism.

Custom extensions changes:
1. We need to move all extensions from main compute image file to the build-custom-extensions repo
2. We need to generate spec for all public extensions and pass it to compute image
Spec contains information about files in the extension and paths,
and also content of the control file. Currently it is set manually per-user, for single users that use "rare" custom extensions. We need to improve spec passing.
For public extensions, we can embed this spec into compute image: use artifact from build-custom-extension CI step and put it into compute image.
  
3. We need to test performance of the extension downloading and ensure that it doesn't affect cold starts (with proxy the speed should be fine).
4. Note that in this task we are not trying to solve extension versioning issue and assume that all extensions are mapped to compute images 1-1 as they are now.
 
#### Test changes:
- This is general functionality and will be covered by e2e tests.
- We will need to add test for extensions, to ensure that they are available for every new compute version. Don't need to run extension regression tests here. Just ensure that `CREATE EXTENSION ext;` works.