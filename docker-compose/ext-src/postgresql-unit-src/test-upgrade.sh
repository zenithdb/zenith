#!/bin/sh
set -ex
cd "$(dirname ${0})"
/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/usr/local/pgsql/bin' --use-existing --dbname=contrib_regression extension tables unit binary unicode prefix units time temperature functions language_functions round derived compare aggregate iec custom crosstab convert