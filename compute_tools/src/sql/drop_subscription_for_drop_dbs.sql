DO $$
DECLARE
    subname TEXT;
BEGIN
    FOR subname IN SELECT pg_subscription.subname FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = {datname_str}) LOOP
        EXECUTE format('ALTER SUBSCRIPTION %I DISABLE;', subname);
        EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE);', subname);
        EXECUTE format('DROP SUBSCRIPTION %I;', subname);
    END LOOP;
END;
$$;
