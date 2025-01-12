# Postgres Bundle for Pageserver

Created on 2024-06-17

## Summary

This RFC defines the responsibilities of Compute and Storage team regarding the
build & deployment of the Postgres code that Pageserver must run
(`initdb`, `postgres --wal-redo`).

## Motivation

Pageserver has to run Postgres binaries to do its job, specifically

* `initdb`
* `postgres --wal-redo` mode

Currently there is **no clear ownership** of
* how these binaries are built
  * including, critically, dynamic linkage against other libraries such as `libicu`
* what build of the binaries ends up running on Pageservers
* how the binaries and runtime dependencies (e.g., shared libraries) are delivered to Pageservers

Further, these binaries have dependencies (e.g., libicu) which
1. prevent the Storage team from switching Pageserver distro and/or version, and
2. some dependencies impact compatibility between Storage and Compute (e.g., [libicu version impacts collation incompatibilty](https://github.com/neondatabase/neon/pull/8074))
3. some dependencies can cause database corruption if updated carelessly (locale => libc)

## Why Is This Worth Solving

1. Clearly defined ownership generally boosts execution speed & bug triage.
   * Example for why execution speed matters: CVE in dependency => who takes care of patching & updating.
2. Centralize understanding of risks involved with some dependencies.
   Currently, there is no team clearly responsible for assessing / tracking the risks. As a reminder from previous section, these are
   * runtime incompatibilities
   * database corruption

Also, it is an unlock for additional future value, see "Future Work" section.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Pageserver (neon.git)
Compute (neon.git)
Deployment process (aws.git)

## Design

The basic interface between Compute and Storage team is as follows:

* Compute team publishes a "bundle" of the binaries required by Pageserver
* Storage team uses a pinned bundle in the Pageserver build process
* Storage team code review is required to update the pinned version

The "bundle" provides an interface agreed upon by Compute and Storage teams to run
* for each supported Postgres version at Neon (v14, v15, v16, ...)
  * the `initdb` process
    * behaving like a vanilla Postgres `initdb`
  * `postgres --wal-redo` mode process
    * following the walredo protocol specified elsewhere

The bundle is self-contained, i.e., it behaves the same way on any Linux system.
The only ambient runtime dependency is the Linux kernel.
The minimum Linux kernel version is 5.10.

### Variant 1: bundle = fully statically linked binaries
The "bundle" is a tarball of fully statically linked binaries

```
v14/initdb
v14/postgres
v15/initdb
v15/postgres
v16/initdb
v16/postgres
...
```

The directory structure is part of the interface.

### Variant 2: bundle = chrooted directory

The "bundle" is a tarball that contains all sorts of files, plus a launcher script.

```
LAUNCHER
storage
storage/does
storage/does/not
storage/does/not/care
```

To launch `initdb` or `postgres --wal-redo`, the Pageserver does
1. fork child process
2. `chroot` into the extracted directory
3. inside the chroot, run `/LAUNCHER VERSION PG_BINARY [FLAGS...]`
4. The `LAUNCHER` script sets up library search paths, etc, and then `exec`s the correct binary

We acknowledge this is half-way reinventing OCI + linux containers.
However, our needs are much simpler than what OCI & Docker provide.
Specifically, we do not want Pageserver to be runtime-dependent on e.g. Docker as the launcher.

The `chroot` is to enforce that the "bundle" be self-contained.
The special path `/inout` int he bundle is reserved, e.g., for `initdb` output.

### Variant 3: ???

Your design here, feedback welcome.

## Security implications

It's an improvement because a single team (Compute) will be responsible for runtime dependencies.

## Implementation & Rollout

Storage and Compute teams agree on a bundle definition.

Compute team changes their build process to produce both
1. existing: compute image / vm compute image
2. existing: pg_install tarball (currently built by `neon.git:Dockerfile`)
2. new: the bundle

Storage makes `neon.git` Pageserver changes to support using bundle (behind feature flag).
With feature flag disabled, existing `pg_install` tarball is used instead.

Storage & infra make `aws.git` changes to deploy bundle to pageservers, with feature flag disabled.

Storage team does gradual rollout.

Storage & infra teams remove support for `pg_install`, delete it from the nodes (experimentation in staging to ensure no hidden runtime deps!)

Compute team stops producing `pg_install` tarball.


## Future Work

We know that we can easily make pageserver fully statically linked.
Together with the self-contained "bundle" proposed above, Pageserver can then be deployed to different OSes.
For example, we have been entertaining the idea of trying Amazon Linux instead of Debian for Pageserver.
That experiment would be a lot simpler.
