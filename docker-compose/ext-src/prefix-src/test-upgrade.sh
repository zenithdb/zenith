#!/bin/sh
set -ex
cd "$(dirname ${0})"
/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --dbname=contrib_regression prefix falcon explain queries