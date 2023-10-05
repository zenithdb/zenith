# Docker images of Neon

## Images

Currently we build two main images:

- [neondatabase/neon](https://hub.docker.com/repository/docker/zenithdb/zenith) — image with pre-built `pageserver`, `safekeeper` and `proxy` binaries and all the required runtime dependencies. Built from [/Dockerfile](/Dockerfile).
- [neondatabase/compute-node](https://hub.docker.com/repository/docker/zenithdb/compute-node) — compute node image with pre-built Postgres binaries from [neondatabase/postgres](https://github.com/neondatabase/postgres).

And additional intermediate image:

- [neondatabase/compute-tools](https://hub.docker.com/repository/docker/neondatabase/compute-tools) — compute node configuration management tools.

## Building pipeline

We build all images after a successful `release` tests run and push automatically to Docker Hub with two parallel CI jobs

1. `neondatabase/compute-tools` and `neondatabase/compute-node`

2. `neondatabase/neon`

## Docker Compose example

You can see a [docker compose](https://docs.docker.com/compose/) example to create a neon cluster in [/docker-compose/docker-compose.yml](/docker-compose/docker-compose.yml). It creates the following containers.

- pageserver x 1
- safekeeper x 3
- storage_broker x 1
- compute x 1
- MinIO x 1        # This is Amazon S3 compatible object storage

### How to use

1. create containers

You can specify version of neon cluster using following environment values.
- PG_VERSION: postgres version for compute (default is 14)
- TAG: the tag version of [docker image](https://registry.hub.docker.com/r/neondatabase/neon/tags) (default is latest), which is tagged in [CI test](/.github/workflows/build_and_test.yml)
```
$ cd docker-compose/
$ docker-compose down   # remove the containers if exists
$ PG_VERSION=15 TAG=2937 docker-compose up --build -d  # You can specify the postgres and image version
Creating network "dockercompose_default" with the default driver
Creating docker-compose_storage_broker_1       ... done
(...omit...)
```

2. connect compute node
```
$ echo "localhost:55433:postgres:cloud_admin:cloud_admin" >> ~/.pgpass
$ chmod 600 ~/.pgpass
$ psql -h localhost -p 55433 -U cloud_admin
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

3. If you want to see the log, you can use `docker-compose logs` command.
```
# check the container name you want to see
$ docker ps
CONTAINER ID   IMAGE                                              COMMAND                  CREATED         STATUS         PORTS                                                                                                                                  NAMES
d6968a5ae912   dockercompose_compute                              "/shell/compute.sh"      5 minutes ago   Up 5 minutes   0.0.0.0:3080->3080/tcp, 0.0.0.0:55433->55433/tcp                                                                                       dockercompose_compute_1
(...omit...)

$ docker logs -f dockercompose_compute_1
2022-10-21 06:15:48.757 GMT [56] LOG:  connection authorized: user=cloud_admin database=postgres application_name=psql
2022-10-21 06:17:00.307 GMT [56] LOG:  [NEON_SMGR] libpagestore: connected to 'host=pageserver port=6400'
(...omit...)
```

4. If you want to see durable data in MinIO which is s3 compatible storage

Access http://localhost:9001 and sign in.

- Username: `minio`
- Password: `password`

You can see durable pages and WAL data in `neon` bucket.
