\echo Use "ALTER EXTENSION neon UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION neon_get_stats()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_stats'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE VIEW neon_stats AS
	SELECT P.* FROM neon_get_stats() AS P (ns_key text, ns_value text);
