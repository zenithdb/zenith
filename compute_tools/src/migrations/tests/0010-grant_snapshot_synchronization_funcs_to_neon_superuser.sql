DO $$
DECLARE
    can_execute boolean;
BEGIN
    SELECT bool_and(has_function_privilege('neon_superuser', oid, 'execute'))
       INTO can_execute
       FROM pg_proc
       WHERE proname IN ('pg_export_snapshot', 'pg_log_standby_snapshot')
           AND pronamespace = 'pg_catalog'::regnamespace;
    IF NOT can_execute THEN
        RAISE EXCEPTION 'neon_superuser cannot execute both pg_export_snapshot and pg_log_standby_snapshot';
    END IF;
END $$;
