# Neon

Neon is a serverless open-source alternative to AWS Aurora Postgres. It separates storage and compute and substitutes the PostgreSQL storage layer by redistributing data across a cluster of nodes.

## Quick start
Try the [Neon Free Tier](https://neon.tech/docs/introduction/technical-preview-free-tier/) to create a serverless Postgres instance. Then connect to it with your preferred Postgres client (psql, dbeaver, etc) or use the online [SQL Editor](https://neon.tech/docs/get-started-with-neon/query-with-neon-sql-editor/). See [Connect from any application](https://neon.tech/docs/connect/connect-from-any-app/) for connection instructions.

Alternatively, compile and run the project [locally](#running-local-installation).

## Architecture overview

A Neon installation consists of compute nodes and the Neon storage engine. Compute nodes are stateless PostgreSQL nodes backed by the Neon storage engine.

The Neon storage engine consists of two major components:
- Pageserver. Scalable storage backend for the compute nodes.
- Safekeepers. The safekeepers form a redundant WAL service that received WAL from the compute node, and stores it durably until it has been processed by the pageserver and uploaded to cloud storage.

See developer documentation in [/docs/SUMMARY.md](/docs/SUMMARY.md) for more information.

## Running local installation


#### Installing dependencies on Linux
1. Install build dependencies and other applicable packages

* On Ubuntu or Debian, this set of packages should be sufficient to build the code:
```bash
apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang pkg-config libpq-dev etcd cmake postgresql-client protobuf-compiler
```
* On Fedora, these packages are needed:
```bash
dnf install flex bison readline-devel zlib-devel openssl-devel \
  libseccomp-devel perl clang cmake etcd postgresql postgresql-contrib protobuf-compiler
```

2. [Install Rust](https://www.rust-lang.org/tools/install)
```
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Installing dependencies on OSX (12.3.1)
1. Install XCode and dependencies
```
xcode-select --install
brew install protobuf etcd openssl flex bison
```

2. [Install Rust](https://www.rust-lang.org/tools/install)
```
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

3. Install PostgreSQL Client
```
# from https://stackoverflow.com/questions/44654216/correct-way-to-install-psql-without-full-postgres-on-macos
brew install libpq
brew link --force libpq
```

#### Rustc version

The project uses [rust toolchain file](./rust-toolchain.toml) to define the version it's built with in CI for testing and local builds.

This file is automatically picked up by [`rustup`](https://rust-lang.github.io/rustup/overrides.html#the-toolchain-file) that installs (if absent) and uses the toolchain version pinned in the file.

rustup users who want to build with another toolchain can use [`rustup override`](https://rust-lang.github.io/rustup/overrides.html#directory-overrides) command to set a specific toolchain for the project's directory.

non-rustup users most probably are not getting the same toolchain automatically from the file, so are responsible to manually verify their toolchain matches the version in the file.
Newer rustc versions most probably will work fine, yet older ones might not be supported due to some new features used by the project or the crates.

#### Building on Linux

1. Build neon and patched postgres
```
# Note: The path to the neon sources can not contain a space.

git clone --recursive https://github.com/neondatabase/neon.git
cd neon

# The preferred and default is to make a debug build. This will create a
# demonstrably slower build than a release build. For a release build,
# use "BUILD_TYPE=release make -j`nproc`"

make -j`nproc`
```

#### Building on OSX

1. Build neon and patched postgres
```
# Note: The path to the neon sources can not contain a space.

git clone --recursive https://github.com/neondatabase/neon.git
cd neon

# The preferred and default is to make a debug build. This will create a
# demonstrably slower build than a release build. For a release build,
# use "BUILD_TYPE=release make -j`sysctl -n hw.logicalcpu`"

make -j`sysctl -n hw.logicalcpu`
```

#### Dependency installation notes
To run the `psql` client, install the `postgresql-client` package or modify `PATH` and `LD_LIBRARY_PATH` to include `pg_install/bin` and `pg_install/lib`, respectively.

To run the integration tests or Python scripts (not required to use the code), install
Python (3.9 or higher), and install python3 packages using `./scripts/pysync` (requires [poetry](https://python-poetry.org/)) in the project directory.


#### Running neon database
1. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .neon with proper paths to binaries and data
# Later that would be responsibility of a package install script
> ./target/debug/neon_local init
Starting pageserver at '127.0.0.1:64000' in '.neon'.
pageserver started, pid: 2545906
Successfully initialized timeline de200bd42b49cc1814412c7e592dd6e9
Stopped pageserver 1 process with pid 2545906

# start pageserver and safekeeper
> ./target/debug/neon_local start
Starting etcd broker using "/usr/bin/etcd"
etcd started, pid: 2545996
Starting pageserver at '127.0.0.1:64000' in '.neon'.
pageserver started, pid: 2546005
Starting safekeeper at '127.0.0.1:5454' in '.neon/safekeepers/sk1'.
safekeeper 1 started, pid: 2546041

# start postgres compute node
> ./target/debug/neon_local pg start main
Starting new postgres (v14) main on timeline de200bd42b49cc1814412c7e592dd6e9 ...
Extracting base backup to create postgres instance: path=.neon/pgdatadirs/tenants/9ef87a5bf0d92544f6fafeeb3239695c/main port=55432
Starting postgres node at 'host=127.0.0.1 port=55432 user=cloud_admin dbname=postgres'

# check list of running postgres instances
> ./target/debug/neon_local pg list
 NODE  ADDRESS          TIMELINE                          BRANCH NAME  LSN        STATUS
 main  127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main         0/16B5BA8  running
```

2. Now, it is possible to connect to postgres and run some queries:
```text
> psql -p55432 -h 127.0.0.1 -U cloud_admin postgres
postgres=# CREATE TABLE t(key int primary key, value text);
CREATE TABLE
postgres=# insert into t values(1,1);
INSERT 0 1
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

3. And create branches and run postgres on them:
```sh
# create branch named migration_check
> ./target/debug/neon_local timeline branch --branch-name migration_check
Created timeline 'b3b863fa45fa9e57e615f9f2d944e601' at Lsn 0/16F9A00 for tenant: 9ef87a5bf0d92544f6fafeeb3239695c. Ancestor timeline: 'main'

# check branches tree
> ./target/debug/neon_local timeline list
(L) main [de200bd42b49cc1814412c7e592dd6e9]
(L) ┗━ @0/16F9A00: migration_check [b3b863fa45fa9e57e615f9f2d944e601]

# start postgres on that branch
> ./target/debug/neon_local pg start migration_check --branch-name migration_check
Starting new postgres migration_check on timeline b3b863fa45fa9e57e615f9f2d944e601 ...
Extracting base backup to create postgres instance: path=.neon/pgdatadirs/tenants/9ef87a5bf0d92544f6fafeeb3239695c/migration_check port=55433
Starting postgres node at 'host=127.0.0.1 port=55433 user=cloud_admin dbname=postgres'

# check the new list of running postgres instances
> ./target/debug/neon_local pg list
 NODE             ADDRESS          TIMELINE                          BRANCH NAME      LSN        STATUS
 main             127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main             0/16F9A38  running
 migration_check  127.0.0.1:55433  b3b863fa45fa9e57e615f9f2d944e601  migration_check  0/16F9A70  running

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p55433 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1

# check that the new change doesn't affect the 'main' postgres
> psql -p55432 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

4. If you want to run tests afterward (see below), you must stop all the running of the pageserver, safekeeper, and postgres instances
   you have just started. You can terminate them all with one command:
```sh
> ./target/debug/neon_local stop
```

## Running tests

Ensure your dependencies are installed as described [here](https://github.com/neondatabase/neon#dependency-installation-notes).

```sh
git clone --recursive https://github.com/neondatabase/neon.git

make

./scripts/pytest
```

## Documentation

[/docs/](/docs/) Contains a top-level overview of all available markdown documentation.

- [/docs/sourcetree.md](/docs/sourcetree.md) contains overview of source tree layout.

To view your `rustdoc` documentation in a browser, try running `cargo doc --no-deps --open`

See also README files in some source directories, and `rustdoc` style documentation comments.

Other resources:

- [SELECT 'Hello, World'](https://neon.tech/blog/hello-world/): Blog post by Nikita Shamgunov on the high level architecture
- [Architecture decisions in Neon](https://neon.tech/blog/architecture-decisions-in-neon/): Blog post by Heikki Linnakangas
- [Neon: Serverless PostgreSQL!](https://www.youtube.com/watch?v=rES0yzeERns): Presentation on storage system by Heikki Linnakangas in the CMU Database Group seminar series

### Postgres-specific terms

Due to Neon's very close relation with PostgreSQL internals, numerous specific terms are used.
The same applies to certain spelling: i.e. we use MB to denote 1024 * 1024 bytes, while MiB would be technically more correct, it's inconsistent with what PostgreSQL code and its documentation use.

To get more familiar with this aspect, refer to:

- [Neon glossary](/docs/glossary.md)
- [PostgreSQL glossary](https://www.postgresql.org/docs/14/glossary.html)
- Other PostgreSQL documentation and sources (Neon fork sources can be found [here](https://github.com/neondatabase/postgres))

## Join the development

- Read `CONTRIBUTING.md` to learn about project code style and practices.
- To get familiar with a source tree layout, use [/docs/sourcetree.md](/docs/sourcetree.md).
- To learn more about PostgreSQL internals, check http://www.interdb.jp/pg/index.html
