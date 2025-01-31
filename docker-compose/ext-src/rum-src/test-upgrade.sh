#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin' --use-existing --dbname=contrib_regression rum rum_validate rum_hash ruminv timestamp orderby orderby_hash altorder altorder_hash limits int2 int4 int8 float4 float8 money oid time timetz date interval macaddr inet cidr text varchar char bytea bit varbit numeric rum_weight expr array