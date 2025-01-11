#!/bin/sh
set -ex
cd "$(dirname ${0})"
/usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/usr/local/pgsql/bin' --inputdir=test --use-existing --dbname=contrib_regression bit btree cast copy halfvec hnsw_bit hnsw_halfvec hnsw_sparsevec hnsw_vector ivfflat_bit ivfflat_halfvec ivfflat_vector sparsevec vector_type