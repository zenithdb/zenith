#!/bin/bash
set -eux -o pipefail
cd "$(dirname "${0}")"
# Takes a variable name as argument. The result is stored in that variable.
generate_id() {
    local -n resvar=$1
    printf -v resvar '%08x%08x%08x%08x' $SRANDOM $SRANDOM $SRANDOM $SRANDOM
}
if [ -z ${OLDTAG+x} ] || [ -z ${NEWTAG+x} ] || [ -z "${OLDTAG}" ] || [ -z "${NEWTAG}" ]; then
  echo OLDTAG and NEWTAG must be defined
  exit 1
fi
export PG_VERSION=${PG_VERSION:-16}
function wait_for_ready {
  TIME=0
  while ! docker compose logs compute_is_ready | grep -q "accepting connections" && [ ${TIME} -le 300 ] ; do
    ((TIME += 1 ))
    sleep 1
  done
  if [ ${TIME} -gt 300 ]; then
    echo Time is out.
    exit 2
  fi
}
function create_extensions() {
  for ext in ${1}; do
    docker compose exec neon-test-extensions psql -X -v ON_ERROR_STOP=1 -d contrib_regression -c "CREATE EXTENSION IF NOT EXISTS ${ext}"
  done
}
EXTENSIONS='[
{"extname": "plv8", "extdir": "plv8-src"},
{"extname": "vector", "extdir": "pgvector-src"},
{"extname": "unit", "extdir": "postgresql-unit-src"},
{"extname": "hypopg", "extdir": "hypopg-src"},
{"extname": "rum", "extdir": "rum-src"},
{"extname": "ip4r", "extdir": "ip4r-src"},
{"extname": "prefix", "extdir": "prefix-src"},
{"extname": "hll", "extdir": "hll-src"},
{"extname": "pg_cron", "extdir": "pg_cron-src"},
{"extname": "pg_uuidv7", "extdir": "pg_uuidv7-src"},
{"extname": "roaringbitmap", "extdir": "pg_roaringbitmap-src"},
{"extname": "semver", "extdir": "pg_semver-src"},
{"extname": "pg_ivm", "extdir": "pg_ivm-src"}
]'
EXTNAMES=$(echo ${EXTENSIONS} | jq -r '.[].extname' | paste -sd ' ' -)
TAG=${NEWTAG} docker compose --profile test-extensions up --quiet-pull --build -d
wait_for_ready
docker compose exec neon-test-extensions psql -c "DROP DATABASE IF EXISTS contrib_regression"
docker compose exec neon-test-extensions psql -c "CREATE DATABASE contrib_regression"
create_extensions "${EXTNAMES}"
query="select json_object_agg(extname,extversion) from pg_extension where extname in ('${EXTNAMES// /\',\'}')"
new_vers=$(docker compose exec neon-test-extensions psql -Aqt -d contrib_regression -c "$query")
docker compose --profile test-extensions down
TAG=${OLDTAG} docker compose --profile test-extensions up --quiet-pull --build -d --force-recreate
wait_for_ready
docker compose cp  ext-src neon-test-extensions:/
docker compose exec neon-test-extensions psql -c "DROP DATABASE IF EXISTS contrib_regression"
docker compose exec neon-test-extensions psql -c "CREATE DATABASE contrib_regression"
create_extensions "${EXTNAMES}"
query="select pge.extname from pg_extension pge join (select key as extname, value as extversion from json_each_text('${new_vers}')) x on pge.extname=x.extname and pge.extversion <> x.extversion"
exts=$(docker compose exec neon-test-extensions psql -Aqt -d contrib_regression -c "$query")
if [ -z "${exts}" ]; then
  echo "No extensions were upgraded"
else
  tenant_id=$(docker compose exec neon-test-extensions psql -Aqt -c "SHOW neon.tenant_id")
  timeline_id=$(docker compose exec neon-test-extensions psql -Aqt -c "SHOW neon.timeline_id")
  for ext in ${exts}; do
    echo Testing ${ext}...
    EXTDIR=$(echo ${EXTENSIONS} | jq -r '.[] | select(.extname=="'${ext}'") | .extdir')
    generate_id new_timeline_id
    PARAMS=(
        -sbf
        -X POST
        -H "Content-Type: application/json"
        -d "{\"new_timeline_id\": \"${new_timeline_id}\", \"pg_version\": ${PG_VERSION}, \"ancestor_timeline_id\": \"${timeline_id}\"}"
        "http://127.0.0.1:9898/v1/tenant/${tenant_id}/timeline/"
    )
    result=$(curl "${PARAMS[@]}")
    echo $result | jq .
    TENANT_ID=${tenant_id} TIMELINE_ID=${new_timeline_id} TAG=${OLDTAG} docker compose down compute compute_is_ready
    COMPUTE_TAG=${NEWTAG} TAG=${OLDTAG} TENANT_ID=${tenant_id} TIMELINE_ID=${new_timeline_id} docker compose up --quiet-pull -d --build compute compute_is_ready
    wait_for_ready
    TID=$(docker compose exec neon-test-extensions psql -Aqt -c "SHOW neon.timeline_id")
    if [ ${TID} != ${new_timeline_id} ]; then
      echo Timeline mismatch
      exit 1
    fi
    docker compose exec neon-test-extensions psql -d contrib_regression -c "\dx ${ext}"
    docker compose exec neon-test-extensions sh -c /ext-src/${EXTDIR}/test-upgrade.sh
    docker compose exec neon-test-extensions psql -d contrib_regression -c "alter extension ${ext} update"
    docker compose exec neon-test-extensions psql -d contrib_regression -c "\dx ${ext}"
  done
fi
