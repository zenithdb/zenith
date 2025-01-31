#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin' --use-existing --dbname=contrib_regression extension tables unit binary unicode prefix units time temperature functions language_functions round derived compare aggregate iec custom crosstab convert