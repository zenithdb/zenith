#!/usr/bin/env bash

set -exuo pipefail

TENANT="abcdefabcdefabcdefabcdefabcdefab"
TIMELINE="01234567890123456789012345678901"

# Loop until we hit an error.
while true; do
    # Remove current cluster, if any.
    if [[ -d .neon ]]; then
        cargo neon --release stop
        rm -rf .neon
    fi

    # Create cluster.
    cargo neon --release init
    cp pageserver.toml .neon/pageserver_1/pageserver.toml
    cargo neon --release start

    # Create tenant and endpoint.
    cargo neon --release tenant create --set-default --tenant-id $TENANT --timeline-id $TIMELINE
    cargo neon --release endpoint create main
    cp postgresql.conf .neon/endpoints/main/postgresql.conf
    cargo neon --release endpoint start main

    # Run pgbench init.
    createdb -p 55432 -U cloud_admin pgbench
    psql -p 55432 -U cloud_admin -d pgbench -f schema.sql
    pgbench -p 55432 -U cloud_admin -d pgbench --initialize --init-steps=G -s 500

    # Compact tenant.
    for SHARD in 0008 0108 0208 0308 0408 0508 0608 0708; do
        curl -XPUT "localhost:9898/v1/tenant/$TENANT-$SHARD/timeline/$TIMELINE/compact?force_l0_compaction=true&force_image_layer_creation=true&wait_until_scheduled_compaction_done=true"
        echo
        echo "Compacted $SHARD"
    done

    # Check for errors.
    if rg -i 'could not find data for key|panic' .neon/**/*.log; then
        echo "boom"
        exit 1
    fi
done