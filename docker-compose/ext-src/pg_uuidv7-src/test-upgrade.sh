#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --dbname=contrib_regression  002_uuid_generate_v7 003_uuid_v7_to_timestamptz 004_uuid_timestamptz_to_v7 005_uuid_v7_to_timestamp 006_uuid_timestamp_to_v7