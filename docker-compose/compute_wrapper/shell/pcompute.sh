#!/bin/bash
set -eux

# Generate a random tenant or timeline ID
#
# Takes a variable name as argument. The result is stored in that variable.
generate_id() {
    local -n resvar=$1
    printf -v resvar '%08x%08x%08x%08x' $SRANDOM $SRANDOM $SRANDOM $SRANDOM
}

PG_VERSION=${PG_VERSION:-14}

SPEC_FILE_ORG=/var/db/postgres/specs/spec.json
SPEC_FILE=/tmp/spec.json

echo "Waiting pageserver become ready."
while ! nc -z pageserver 6400; do
     sleep 1;
done
echo "Page server is ready."

echo "Create a tenant and timeline"
generate_id tenant_id
PARAMS=(
     -X PUT
     -H "Content-Type: application/json"
     -d "{\"mode\": \"AttachedSingle\", \"generation\": 1, \"tenant_conf\": {}}"
     "http://pageserver:9898/v1/tenant/${tenant_id}/location_config"
)
result=$(curl "${PARAMS[@]}")
echo $result | jq .

generate_id timeline_id
PARAMS=(
     -sbf
     -X POST
     -H "Content-Type: application/json"
     -d "{\"new_timeline_id\": \"${timeline_id}\", \"pg_version\": ${PG_VERSION}}"
     "http://pageserver:9898/v1/tenant/${tenant_id}/timeline/"
)
result=$(curl "${PARAMS[@]}")
echo $result | jq .

echo "Overwrite tenant id and timeline id in spec file"
sed "s/TENANT_ID/${tenant_id}/" ${SPEC_FILE_ORG} > ${SPEC_FILE}
sed -i "s/TIMELINE_ID/${timeline_id}/" ${SPEC_FILE}

cat ${SPEC_FILE}

echo "Start compute node"
/usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute \
     -C "postgresql://cloud_admin@localhost:55433/postgres"  \
     -b /usr/local/bin/postgres                              \
     -S ${SPEC_FILE}
