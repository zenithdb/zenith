#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --dbname=contrib_regression prefix falcon explain queries