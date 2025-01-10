# How to run the `pg_regress` tests on a cloud Neon instance.

* Create a Neon project on staging.
* Grant the superuser privileges to the DB user.
* (Optional) create a branch for testing
* Configure the endpoint by updating the control-plane database with the following settings:
  * `Timeone`: `America/Los_Angeles`
  * `DateStyle`: `Postgres,MDY`
  * `compute_query_id`: `off`
* Checkout the actual `Neon` sources
* Patch the sql and expected files for the specific PostgreSQL version, e.g. for v17:
```bash
$ cd vendor/postgres-v17
$ patch -p1 <../../compute/patches/cloud_regress_pg17.patch
```
* Set the environment variable `BENCHMARK_CONNSTR` to the connection URI of your project.
* Set the environment variable `PG_VERSION` to the version of your project.
* Run 
```bash
$ pytest -m remote_cluster -k cloud_regress
```