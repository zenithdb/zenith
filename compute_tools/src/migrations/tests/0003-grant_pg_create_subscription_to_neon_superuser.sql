DO $$
BEGIN
    IF (SELECT current_setting('server_version_num')::numeric < 160000) THEN
        RETURN;
    END IF;

    IF NOT (SELECT pg_has_role('neon_superuser', 'pg_create_subscription', 'member')) THEN
        RAISE EXCEPTION 'neon_superuser cannot execute pg_create_subscription';
    END IF;
END $$;
