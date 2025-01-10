-- create a schema that simulates Neon control plane operations table
-- however use partitioned operations tables with many (e.g. 500) child partition tables per table
-- in summary we create multiple of these partitioned operations tables (with 500 childs each) - until we reach the requested number of tables


-- first we need some other tables that can be referenced by the operations table

--  Table for branches
CREATE TABLE public.branches (
    id text PRIMARY KEY
);

-- Table for endpoints
CREATE TABLE public.endpoints (
    id text PRIMARY KEY
);

-- Table for projects
CREATE TABLE public.projects (
    id text PRIMARY KEY
);

INSERT INTO public.branches (id)
VALUES ('branch_1');

-- Insert one row into endpoints
INSERT INTO public.endpoints (id)
VALUES ('endpoint_1');

-- Insert one row into projects
INSERT INTO public.projects (id)
VALUES ('project_1');

-- now we create a procedure that can create n operations tables
-- we do that in a procedure to save roundtrip latency when scaling the test to many tables
-- prefix is the base table name, e.g. 'operations_scale_1000' if we create 1000 tables
CREATE OR REPLACE PROCEDURE create_partitioned_tables(prefix text, n INT)
LANGUAGE plpgsql AS $$
DECLARE
    table_name TEXT;  -- Variable to hold table names dynamically
    i INT;            -- Counter for the loop
BEGIN
    -- Loop to create n partitioned tables
    FOR i IN 1..n LOOP
        table_name := format('%s_%s', prefix, i);

        -- Create the partitioned table
        EXECUTE format(
            'CREATE TABLE public.%s (
                project_id character varying NOT NULL,
                id uuid NOT NULL,
                status integer,
                action character varying NOT NULL,
                error character varying,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                updated_at timestamp with time zone NOT NULL DEFAULT now(),
                spec jsonb,
                retry_at timestamp with time zone,
                failures_count integer DEFAULT 0,
                metadata jsonb NOT NULL DEFAULT ''{}''::jsonb,
                executor_id text NOT NULL,
                attempt_duration_ms integer,
                metrics jsonb DEFAULT ''{}''::jsonb,
                branch_id text,
                endpoint_id text,
                next_operation_id uuid,
                compute_id text,
                connection_attempt_at timestamp with time zone,
                concurrency_key text,
                queue_id text,
                CONSTRAINT %s_pkey PRIMARY KEY (id, created_at),
                CONSTRAINT %s_branch_id_fk FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
                CONSTRAINT %s_endpoint_id_fk FOREIGN KEY (endpoint_id) REFERENCES endpoints(id) ON DELETE CASCADE,
                CONSTRAINT %s_next_operation_id_fk FOREIGN KEY (next_operation_id, created_at) REFERENCES %s(id, created_at),
                CONSTRAINT %s_project_id_fk FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
            ) PARTITION BY RANGE (created_at)',
            table_name, table_name, table_name, table_name, table_name, table_name, table_name
        );

        -- Add indexes for the partitioned table
        EXECUTE format('CREATE INDEX index_%s_on_next_operation_id ON public.%s (next_operation_id)', table_name, table_name);
        EXECUTE format('CREATE INDEX index_%s_on_project_id ON public.%s (project_id)', table_name, table_name);
        EXECUTE format('CREATE INDEX %s_branch_id ON public.%s (branch_id)', table_name, table_name);
        EXECUTE format('CREATE INDEX %s_branch_id_created_idx ON public.%s (branch_id, created_at)', table_name, table_name);
        EXECUTE format('CREATE INDEX %s_created_at_idx ON public.%s (created_at)', table_name, table_name);
        EXECUTE format('CREATE INDEX %s_created_at_project_id_id_cond_idx ON public.%s (created_at, project_id, id)', table_name, table_name);
        EXECUTE format('CREATE INDEX %s_endpoint_id ON public.%s (endpoint_id)', table_name, table_name);
        EXECUTE format(
            'CREATE INDEX %s_for_redo_worker_idx ON public.%s (executor_id) WHERE status <> 1',
            table_name, table_name
        );
        EXECUTE format(
            'CREATE INDEX %s_project_id_status_index ON public.%s ((project_id::text), status)',
            table_name, table_name
        );
        EXECUTE format(
            'CREATE INDEX %s_status_not_finished ON public.%s (status) WHERE status <> 1',
            table_name, table_name
        );
        EXECUTE format('CREATE INDEX %s_updated_at_desc_idx ON public.%s (updated_at DESC)', table_name, table_name);
        EXECUTE format(
            'CREATE INDEX %s_with_failures ON public.%s (failures_count) WHERE failures_count > 0',
            table_name, table_name
        );
    END LOOP;
END;
$$;

-- next we create a procedure that can add the child partitions (one per day) to each of the operations tables
CREATE OR REPLACE PROCEDURE create_operations_partitions(
    table_name TEXT, 
    start_date DATE,
    end_date DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    counter INT := 0;  -- Counter to track the number of tables created in the current transaction
BEGIN
    partition_date := start_date;

    -- Create partitions in batches
    WHILE partition_date < end_date LOOP
        partition_name := format('%s_%s', table_name, to_char(partition_date,'YYYY_MM_DD'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS public.%s PARTITION OF public.%s
             FOR VALUES FROM (''%s'') TO (''%s'')',
            partition_name,
            table_name,
            partition_date,
            partition_date + INTERVAL '1 day'
        );

        counter := counter + 1;

        -- Commit and reset counter after every 100 partitions
        IF counter >= 100 THEN
            COMMIT;
            counter := 0;  -- Reset the counter
        END IF;

        -- Advance to the next day
        partition_date := partition_date + INTERVAL '1 day';
    END LOOP;

    -- Final commit for remaining partitions
    IF counter > 0 THEN
        COMMIT;
    END IF;

    -- Insert synthetic rows into each partition
    EXECUTE format(
        'INSERT INTO %I (
            project_id,
            branch_id,
            endpoint_id,
            id,
            status,
            action,
            created_at,
            updated_at,
            spec,
            metadata,
            executor_id,
            failures_count
        )
        SELECT 
            ''project_1'',                                   -- project_id
            ''branch_1'',                                    -- branch_id
            ''endpoint_1'',                                  -- endpoint_id
            ''e8bba687-0df9-4291-bfcd-7d5f6aa7c158'',          -- unique id
            1,                                               -- status
            ''SYNTHETIC_ACTION'',                            -- action
            gs::timestamp + interval ''0 ms'',               -- created_at
            gs::timestamp + interval ''1 minute'',           -- updated_at
            ''{"key": "value"}'',                            -- spec (JSONB)
            ''{"metadata_key": "metadata_value"}'',          -- metadata (JSONB)
            ''executor_1'',                                  -- executor_id
            0                                                -- failures_count
        FROM generate_series(%L, %L::DATE - INTERVAL ''1 day'', INTERVAL ''1 day'') AS gs',
        table_name, start_date, end_date
    );
    
    -- Commit the inserted rows
    COMMIT;
END;
$$;

-- we can now create partitioned tables using something like
-- CALL create_partitioned_tables('operations_scale_1000' ,10);

-- and we can create the child partitions for a table using something like
-- CALL create_operations_partitions(
--     'operations_scale_1000_1',
--     '2000-01-01',            -- Start date
--     ('2000-01-01'::DATE + INTERVAL '1 day' * 500)::DATE  -- End date (start date + number of days)
-- );
