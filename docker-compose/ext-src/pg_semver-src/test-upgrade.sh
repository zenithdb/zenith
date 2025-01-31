#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --dbname=contrib_regression base corpus