#!/bin/bash

if [ -z "$DB_CONNSTR" ]; then
    echo "DB_CONNSTR is not set"
    exit 1
fi

# Create a temporary table for JSON data
psql $DB_CONNSTR -c 'DROP TABLE IF EXISTS tmp_json'
psql $DB_CONNSTR -c 'CREATE TABLE tmp_json (data jsonb)'

for file in ./result/*.json; do
    echo "$file"
    SK_ID=$(jq '.config.id' $file)
    echo "SK_ID: $SK_ID"
    jq -c ".timelines[] |  . + {\"sk_id\": $SK_ID}" $file | psql $DB_CONNSTR -c "\\COPY tmp_json (data) FROM STDIN"
done

TABLE_NAME=$1

if [ -z "$TABLE_NAME" ]; then
    echo "TABLE_NAME is not set, skipping conversion to table with typed columns"
    echo "Usage: ./upload.sh TABLE_NAME"
    exit 0
fi

psql $DB_CONNSTR <<EOF
CREATE TABLE $TABLE_NAME AS
SELECT
  (data->>'sk_id')::int AS sk_id,
  (data->>'tenant_id') AS tenant_id,
  (data->>'timeline_id') AS timeline_id,
  (data->'memory'->>'active')::bool AS active,
  (data->'memory'->>'flush_lsn')::int AS flush_lsn,
  (data->'memory'->'mem_state'->>'backup_lsn')::int AS backup_lsn,
  (data->'memory'->'mem_state'->>'commit_lsn')::int AS commit_lsn,
  (data->'memory'->'mem_state'->>'peer_horizon_lsn')::int AS peer_horizon_lsn,
  (data->'memory'->'mem_state'->>'remote_consistent_lsn')::int AS remote_consistent_lsn,
  (data->'memory'->>'write_lsn')::int AS write_lsn,
  (data->'memory'->>'num_computes')::int AS num_computes,
  (data->'memory'->>'epoch_start_lsn')::int AS epoch_start_lsn,
  (data->'memory'->>'last_removed_segno')::int AS last_removed_segno,
  (data->'memory'->>'is_cancelled')::bool AS is_cancelled,
  (data->'control_file'->>'backup_lsn')::int AS disk_backup_lsn,
  (data->'control_file'->>'commit_lsn')::int AS disk_commit_lsn,
  (data->'control_file'->'acceptor_state'->>'term')::int AS disk_term,
  (data->'control_file'->>'local_start_lsn')::int AS local_start_lsn,
  (data->'control_file'->>'peer_horizon_lsn')::int AS disk_peer_horizon_lsn,
  (data->'control_file'->>'timeline_start_lsn')::int AS timeline_start_lsn,
  (data->'control_file'->>'remote_consistent_lsn')::int AS disk_remote_consistent_lsn
FROM tmp_json
EOF
