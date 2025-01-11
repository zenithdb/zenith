#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --dbname=contrib_regression base corpus