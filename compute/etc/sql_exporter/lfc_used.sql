SELECT COALESCE(lfc_value, 0) AS lfc_used FROM neon.neon_lfc_stats WHERE lfc_key = 'file_cache_used';
