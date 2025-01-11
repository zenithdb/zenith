\echo Use "ALTER EXTENSION neon UPDATE TO '1.6'" to load this file. \quit

CREATE FUNCTION get_prewarm_info(out total_chunks integer, out curr_chunk integer, out prewarmed_pages integer, out skipped_pages integer)
RETURNS record
AS 'MODULE_PATHNAME', 'get_prewarm_info'
LANGUAGE C STRICT
PARALLEL SAFE;

CREATE FUNCTION get_local_cache_state(max_chunks integer default null)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_local_cache_state'
LANGUAGE C
PARALLEL UNSAFE;

CREATE FUNCTION prewarm_local_cache(state bytea)
RETURNS void
AS 'MODULE_PATHNAME', 'prewarm_local_cache'
LANGUAGE C STRICT
PARALLEL UNSAFE;



