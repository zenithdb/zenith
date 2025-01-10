DO $$
DECLARE
    monitor record;
BEGIN
    SELECT pg_has_role('neon_superuser', 'pg_monitor', 'member') AS member,
            admin_option AS admin
        INTO monitor
        FROM pg_auth_members
        WHERE roleid = 'pg_monitor'::regrole
            AND member = 'pg_monitor'::regrole;

    IF NOT monitor.member THEN
        RAISE EXCEPTION 'neon_superuser is not a member of pg_monitor';
    END IF;

    IF NOT monitor.admin THEN
        RAISE EXCEPTION 'neon_superuser cannot grant pg_monitor';
    END IF;
END $$;
