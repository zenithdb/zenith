DO $$
DECLARE
    bypassrls boolean;
BEGIN
    SELECT rolbypassrls INTO bypassrls FROM pg_roles WHERE rolname = 'neon_superuser';
    IF NOT bypassrls THEN
        RAISE EXCEPTION 'neon_superuser cannot bypass RLS';
    END IF;
END $$;
