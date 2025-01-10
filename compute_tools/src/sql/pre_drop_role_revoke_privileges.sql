SET SESSION ROLE neon_superuser;

DO $$
DECLARE
    schema TEXT;
    revoke_query TEXT;
BEGIN
    FOR schema IN
        SELECT schema_name
        FROM information_schema.schemata
        -- So far, we only had issues with 'public' schema. Probably, because we do some additional grants,
        -- e.g., make DB owner the owner of 'public' schema automatically (when created via API).
        -- See https://github.com/neondatabase/cloud/issues/13582 for the context.
        -- Still, keep the loop because i) it efficiently handles the case when there is no 'public' schema,
        -- ii) it's easy to add more schemas to the list if needed.
        WHERE schema_name IN ('public')
    LOOP
        revoke_query := format(
            'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM {role_name} GRANTED BY neon_superuser;',
            schema
        );

        EXECUTE revoke_query;
    END LOOP;
END;
$$;

RESET ROLE;
