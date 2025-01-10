DO $$
DECLARE
    can_execute boolean;
BEGIN
    SELECT has_function_privilege('neon_superuser', oid, 'execute')
       INTO can_execute
       FROM pg_proc
       WHERE proname = 'pg_show_replication_origin_status'
           AND pronamespace = 'pg_catalog'::regnamespace;
    IF NOT can_execute THEN
        RAISE EXCEPTION 'neon_superuser cannot execute pg_show_replication_origin_status';
    END IF;
END $$;
