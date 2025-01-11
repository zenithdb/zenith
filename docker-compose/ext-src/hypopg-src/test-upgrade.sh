#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/usr/local/pgsql/bin' --use-existing --inputdir=test --dbname=contrib_regression hypopg hypo_brin hypo_index_part hypo_include hypo_hash hypo_hide_index