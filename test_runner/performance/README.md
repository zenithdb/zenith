# Running locally

First make a release build. The profiling flag is optional, used only for tests that
generate flame graphs. The `-s` flag just silences a lot of output, and makes it
easier to see if you have compile errors without scrolling up.
`BUILD_TYPE=release CARGO_BUILD_FLAGS="--features=profiling" make -s -j8`

NOTE: the `profiling` flag only works on linux because we use linux-specific
libc APIs like `libc::timer_t`.

Then run the tests
`NEON_BIN=./target/release poetry run pytest test_runner/performance"`

Some handy pytest flags for local development:
- `-x` tells pytest to stop on first error
- `-s` shows test output
- `-k` selects a test to run
- `--timeout=0` disables our default timeout of 300s (see `setup.cfg`)

# What performance tests do we have and how we run them

Performance tests are built using the same infrastructure as our usual python integration tests. There are some extra fixtures that help to collect performance metrics, and to run tests against both vanilla PostgreSQL and Neon for comparison.

## Tests that are run against local installation

Most of the performance tests run against a local installation. This is not very representative of a production environment. Firstly, Postgres, safekeeper(s) and the pageserver have to share CPU and I/O resources, which can add noise to the results. Secondly, network overhead is eliminated.

In the CI, the performance tests are run in the same environment as the other integration tests. We don't have control over the host that the CI runs on, so the environment may vary widely from one run to another, which makes the results across different runs noisy to compare.

## Remote tests

There are a few tests that marked with `pytest.mark.remote_cluster`. These tests do not set up a local environment, and instead require a libpq connection string to connect to. So they can be run on any Postgres compatible database. Currently, the CI runs these tests on our staging and captest environments daily. Those are not an isolated environments, so there can be noise in the results due to activity of other clusters.

## Noise

All tests run only once. Usually to obtain more consistent performance numbers, a test should be repeated multiple times and the results be aggregated, for example by taking min, max, avg, or median.

## Results collection

Local test results for main branch, and results of daily performance tests, are stored in a neon project deployed in production environment. There is a Grafana dashboard that visualizes the results. Here is the [dashboard](https://observer.zenith.tech/d/DGKBm9Jnz/perf-test-results?orgId=1). The main problem with it is the unavailability to point at particular commit, though the data for that is available in the database. Needs some tweaking from someone who knows Grafana tricks.

There is also an inconsistency in test naming. Test name should be the same across platforms, and results can be differentiated by the platform field. But currently, platform is sometimes included in test name because of the way how parametrization works in pytest. I.e. there is a platform switch in the dashboard with neon-local-ci and neon-staging variants. I.e. some tests under neon-local-ci value for a platform switch are displayed as `Test test_runner/performance/test_bulk_insert.py::test_bulk_insert[vanilla]` and `Test test_runner/performance/test_bulk_insert.py::test_bulk_insert[neon]` which is highly confusing.
