#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --max-connections=86 --schedule test/schedule/main.sch   --schedule test/build/run.sch --dbname contrib_regression --use-existing