#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --dbname=contrib_regression ip4r ip4r-softerr ip4r-v11