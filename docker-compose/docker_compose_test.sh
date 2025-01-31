#!/bin/bash

# A basic test to ensure Docker images are built correctly.
# Build a wrapper around the compute, start all services and runs a simple SQL query.
# Repeats the process for all currenly supported Postgres versions.

# Implicitly accepts `REPOSITORY` and `TAG` env vars that are passed into the compose file
# Their defaults point at DockerHub `neondatabase/neon:latest` image.`,
# to verify custom image builds (e.g pre-published ones).
#
# A test script for postgres extensions
# Currently supports only v16
#
set -eux -o pipefail

COMPOSE_FILE='docker-compose.yml'
cd $(dirname $0)
COMPUTE_CONTAINER_NAME=docker-compose-compute-1
TEST_CONTAINER_NAME=docker-compose-neon-test-extensions-1
PSQL_OPTION="-h localhost -U cloud_admin -p 55433 -d postgres"

cleanup() {
    echo "show container information"
    docker ps
    echo "stop containers..."
    docker compose --profile test-extensions -f $COMPOSE_FILE down
}

for pg_version in ${TEST_VERSION_ONLY-14 15 16 17}; do
    pg_version=${pg_version/v/}
    echo "clean up containers if exists"
    cleanup
    PG_TEST_VERSION=$((pg_version < 16 ? 16 : pg_version))
    PG_VERSION=$pg_version PG_TEST_VERSION=$PG_TEST_VERSION docker compose --profile test-extensions -f $COMPOSE_FILE up --quiet-pull --build -d

    echo "wait until the compute is ready. timeout after 60s. "
    cnt=0
    while sleep 3; do
        # check timeout
        cnt=`expr $cnt + 3`
        if [ $cnt -gt 60 ]; then
            echo "timeout before the compute is ready."
            exit 1
        fi
        if docker compose --profile test-extensions -f $COMPOSE_FILE logs "compute_is_ready" | grep -q "accepting connections"; then
            echo "OK. The compute is ready to connect."
            echo "execute simple queries."
            docker exec $COMPUTE_CONTAINER_NAME /bin/bash -c "psql $PSQL_OPTION"
            break
        fi
    done

    if [ $pg_version -ge 16 ]; then
        docker cp ext-src $TEST_CONTAINER_NAME:/
        # This is required for the pg_hint_plan test, to prevent flaky log message causing the test to fail
        # It cannot be moved to Dockerfile now because the database directory is created after the start of the container
        echo Adding dummy config
        docker exec $COMPUTE_CONTAINER_NAME touch /var/db/postgres/compute/compute_ctl_temp_override.conf
        # The following block copies the files for the pg_hintplan test to the compute node for the extension test in an isolated docker-compose environment
        TMPDIR=$(mktemp -d)
        docker cp $TEST_CONTAINER_NAME:/ext-src/pg_hint_plan-src/data $TMPDIR/data
        docker cp $TMPDIR/data $COMPUTE_CONTAINER_NAME:/ext-src/pg_hint_plan-src/
        rm -rf $TMPDIR
        # Prepare for the PostGIS test
        docker exec $COMPUTE_CONTAINER_NAME mkdir -p /tmp/pgis_reg/pgis_reg_tmp
        TMPDIR=$(mktemp -d)
        docker cp $TEST_CONTAINER_NAME:/ext-src/postgis-src/raster/test $TMPDIR
        docker cp $TEST_CONTAINER_NAME:/ext-src/postgis-src/regress/00-regress-install $TMPDIR
        docker exec $COMPUTE_CONTAINER_NAME mkdir -p /ext-src/postgis-src/raster /ext-src/postgis-src/regress /ext-src/postgis-src/regress/00-regress-install
        docker cp $TMPDIR/test $COMPUTE_CONTAINER_NAME:/ext-src/postgis-src/raster/test
        docker cp $TMPDIR/00-regress-install $COMPUTE_CONTAINER_NAME:/ext-src/postgis-src/regress
        rm -rf $TMPDIR

        # The following block does the same for the contrib/file_fdw test
        TMPDIR=$(mktemp -d)
        docker cp $TEST_CONTAINER_NAME:/postgres/contrib/file_fdw/data $TMPDIR/data
        docker cp $TMPDIR/data $COMPUTE_CONTAINER_NAME:/postgres/contrib/file_fdw/data
        rm -rf $TMPDIR
        # Apply patches
        cat ../compute/patches/contrib_pg${pg_version}.patch | docker exec -i $TEST_CONTAINER_NAME bash -c "(cd /postgres && patch -p1)"
        # We are running tests now
        rm -f testout.txt testout_contrib.txt
        docker exec -e USE_PGXS=1 -e SKIP=timescaledb-src,rdkit-src,pgx_ulid-src,pgtap-src,pg_tiktoken-src,pg_jsonschema-src,kq_imcx-src,wal2json_2_5-src \
        $TEST_CONTAINER_NAME /run-tests.sh /ext-src | tee testout.txt && EXT_SUCCESS=1 || EXT_SUCCESS=0
        docker exec -e SKIP=start-scripts,postgres_fdw,ltree_plpython,jsonb_plpython,jsonb_plperl,hstore_plpython,hstore_plperl,dblink,bool_plperl \
        $TEST_CONTAINER_NAME /run-tests.sh /postgres/contrib | tee testout_contrib.txt && CONTRIB_SUCCESS=1 || CONTRIB_SUCCESS=0
        if [ $EXT_SUCCESS -eq 0 ] || [ $CONTRIB_SUCCESS -eq 0 ]; then
            CONTRIB_FAILED=
            FAILED=
            [ $EXT_SUCCESS -eq 0 ] && FAILED=$(tail -1 testout.txt | awk '{for(i=1;i<=NF;i++){print "/ext-src/"$i;}}')
            [ $CONTRIB_SUCCESS -eq 0 ] && CONTRIB_FAILED=$(tail -1 testout_contrib.txt | awk '{for(i=0;i<=NF;i++){print "/postgres/contrib/"$i;}}')
            for d in $FAILED $CONTRIB_FAILED; do
                dn="$(basename $d)"
                rm -rf $dn
                mkdir $dn
                docker cp $TEST_CONTAINER_NAME:$d/regression.diffs $dn || [ $? -eq 1 ]
                docker cp $TEST_CONTAINER_NAME:$d/regression.out $dn || [ $? -eq 1 ]
                cat $dn/regression.out $dn/regression.diffs || true
                rm -rf $dn
            done
            if echo $FAILED | grep -q postgis-src; then
              docker exec $TEST_CONTAINER_NAME cat /tmp/pgis_reg/regress_log || true
            fi
        rm -rf $FAILED
        exit 1
        fi
    fi
done
