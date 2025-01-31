#!/bin/bash
set -ex
cd "$(dirname "${0}")"
dropdb --if-exists contrib_regression
createdb contrib_regression
PGXS="$(dirname "$(pg_config --pgxs)" )"
REGRESS="${PGXS}/../test/regress/pg_regress"
TESTDIR="test"
TESTS=$(ls "${TESTDIR}/sql" | sort )
TESTS=${TESTS//\.sql/}
psql -v ON_ERROR_STOP=1 -f test/fixtures.sql -d contrib_regression
${REGRESS} --use-existing --dbname=contrib_regression --inputdir=${TESTDIR} ${TESTS}

