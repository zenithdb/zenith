## Neon test runner

This directory contains integration tests.

Prerequisites:
- Correctly configured Python, see [`/docs/sourcetree.md`](/docs/sourcetree.md#using-python)
- Neon and Postgres binaries
    - See the root [README.md](/README.md) for build directions
    - Tests can be run from the git tree; or see the environment variables
      below to run from other directories.
- The neon git repo, including the postgres submodule
  (for some tests, e.g. `pg_regress`)
- Some tests (involving storage nodes coordination) require etcd installed. Follow
  [`the guide`](https://etcd.io/docs/v3.5/install/) to obtain it.

### Test Organization

Regression tests are in the 'regress' directory. They can be run in
parallel to minimize total runtime. Most regression test sets up their
environment with its own pageservers and safekeepers (but see
`TEST_SHARED_FIXTURES`).

'pg_clients' contains tests for connecting with various client
libraries. Each client test uses a Dockerfile that pulls an image that
contains the client, and connects to PostgreSQL with it. The client
tests can be run against an existing PostgreSQL or Neon installation.

'performance' contains performance regression tests. Each test
exercises a particular scenario or workload, and outputs
measurements. They should be run serially, to avoid the tests
interfering with the performance of each other. Some performance tests
set up their own Neon environment, while others can be run against an
existing PostgreSQL or Neon environment.

### Running the tests

There is a wrapper script to invoke pytest: `./scripts/pytest`.
It accepts all the arguments that are accepted by pytest.
Depending on your installation options pytest might be invoked directly.

Test state (postgres data, pageserver state, and log files) will
be stored under a directory `test_output`.

You can run all the tests with:

`./scripts/pytest`

If you want to run all the tests in a particular file:

`./scripts/pytest test_pgbench.py`

If you want to run all tests that have the string "bench" in their names:

`./scripts/pytest -k bench`

To run tests in parellel we utilize `pytest-xdist` plugin. By default everything runs single threaded. Number of workers can be specified with `-n` argument:

`./scripts/pytest -n4`

By default performance tests are excluded. To run them explicitly pass performance tests selection to the script:

`./scripts/pytest test_runner/performance`

Useful environment variables:

`NEON_BIN`: The directory where neon binaries can be found.
`POSTGRES_DISTRIB_DIR`: The directory where postgres distribution can be found.
Since pageserver supports several postgres versions, `POSTGRES_DISTRIB_DIR` must contain
a subdirectory for each version with naming convention `v{PG_VERSION}/`.
Inside that dir, a `bin/postgres` binary should be present.
`DEFAULT_PG_VERSION`: The version of Postgres to use,
This is used to construct full path to the postgres binaries.
Format is 2-digit major version nubmer, i.e. `DEFAULT_PG_VERSION="14"`
`TEST_OUTPUT`: Set the directory where test state and test output files
should go.
`TEST_SHARED_FIXTURES`: Try to re-use a single pageserver for all the tests.
`NEON_PAGESERVER_OVERRIDES`: add a `;`-separated set of configs that will be passed as
`--pageserver-config-override=${value}` parameter values when neon_local cli is invoked
`RUST_LOG`: logging configuration to pass into Neon CLI

Let stdout, stderr and `INFO` log messages go to the terminal instead of capturing them:
`./scripts/pytest -s --log-cli-level=INFO ...`
(Note many tests capture subprocess outputs separately, so this may not
show much.)

Exit after the first test failure:
`./scripts/pytest -x ...`
(there are many more pytest options; run `pytest -h` to see them.)

### Writing a test

Every test needs a Neon Environment, or NeonEnv to operate in. A Neon Environment
is like a little cloud-in-a-box, and consists of a Pageserver, 0-N Safekeepers, and
compute Postgres nodes. The connections between them can be configured to use JWT
authentication tokens, and some other configuration options can be tweaked too.

The easiest way to get access to a Neon Environment is by using the `neon_simple_env`
fixture. The 'simple' env may be shared across multiple tests, so don't shut down the nodes
or make other destructive changes in that environment. Also don't assume that
there are no tenants or branches or data in the cluster. For convenience, there is a
branch called `empty`, though. The convention is to create a test-specific branch of
that and load any test data there, instead of the 'main' branch.

For more complicated cases, you can build a custom Neon Environment, with the `neon_env`
fixture:

```python
def test_foobar(neon_env_builder: NeonEnvBuilder):
    # Prescribe the environment.
    # We want to have 3 safekeeper nodes, and use JWT authentication in the
    # connections to the page server
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.set_pageserver_auth(True)

    # Now create the environment. This initializes the repository, and starts
    # up the page server and the safekeepers
    env = neon_env_builder.init_start()

    # Run the test
    ...
```

For more information about pytest fixtures, see https://docs.pytest.org/en/stable/fixture.html

At the end of a test, all the nodes in the environment are automatically stopped, so you
don't need to worry about cleaning up. Logs and test data are preserved for the analysis,
in a directory under `../test_output/<testname>`

### Before submitting a patch
Ensure that you pass all [obligatory checks](/docs/sourcetree.md#obligatory-checks).

Also consider:

* Writing a couple of docstrings to clarify the reasoning behind a new test.
* Adding more type hints to your code to avoid `Any`, especially:
  * For fixture parameters, they are not automatically deduced.
  * For function arguments and return values.
