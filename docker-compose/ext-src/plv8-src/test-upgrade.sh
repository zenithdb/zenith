#!/bin/sh
set -ex
cd "$(dirname ${0})"
/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/usr/local/pgsql/bin'  --use-existing --dbname=contrib_regression plv8 plv8-errors scalar_args inline json startup_pre startup varparam json_conv jsonb_conv window guc es6 arraybuffer composites currentresource startup_perms bytea find_function_perms memory_limits reset show array_spread regression dialect bigint procedure