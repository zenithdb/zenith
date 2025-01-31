-- ========================================================
-- Schema for reproducing production pageserver workload on
-- on staging using pgbench.
-- We modify the pgbench schema to be more web-app like:
--   - add text and jsonb columns
-- our biggest tenant currently has 1 TB logical data size
-- so we need to scale up the pgbench schema to 1 TB
-- scale factor of 5000 is 73 GB according to 
-- https://www.cybertec-postgresql.com/en/a-formula-to-calculate-pgbench-scaling-factor-for-target-db-size/
-- so we need to scale up to 14x5000 = 70000 scale factor
-- this creates 100000x70000 rows = 7 billion rows
-- thus we need to use the big_int type for pgbench_accounts.aid
-- ========================================================


DROP TABLE IF EXISTS pgbench_accounts CASCADE;
DROP TABLE IF EXISTS pgbench_branches CASCADE;
DROP TABLE IF EXISTS pgbench_history CASCADE;
DROP TABLE IF EXISTS pgbench_tellers CASCADE;

--
-- Name: pgbench_accounts
--

CREATE TABLE pgbench_accounts (
    aid bigint NOT NULL,
    bid integer,
    abalance integer,
    filler character(84),
    -- more web-app like columns
    text_column_plain TEXT  DEFAULT repeat('NeonIsCool', 5),
    jsonb_column_extended JSONB  DEFAULT ('{ "tell everyone": [' || repeat('{"Neon": "IsCool"},',9) || ' {"Neon": "IsCool"}]}')::jsonb
)
WITH (fillfactor='100');

--
-- Name: pgbench_branches; 
--

CREATE TABLE pgbench_branches (
    bid integer NOT NULL,
    bbalance integer,
    filler character(88),
    -- more web-app like columns
    text_column_plain TEXT  DEFAULT repeat('NeonIsCool', 5),
    jsonb_column_extended JSONB  DEFAULT ('{ "tell everyone": [' || repeat('{"Neon": "IsCool"},',9) || ' {"Neon": "IsCool"}]}')::jsonb
)
WITH (fillfactor='100');

--
-- Name: pgbench_history; Type: TABLE; Schema: public; Owner: peterbendel
--

CREATE TABLE pgbench_history (
    tid integer,
    bid integer,
    aid bigint,
    delta integer,
    mtime timestamp without time zone,
    filler character(22),
    text_column_plain TEXT  DEFAULT repeat('NeonIsCool', 5)
);


--
-- Name: pgbench_tellers; Type: TABLE; Schema: public; Owner: peterbendel
--

CREATE TABLE pgbench_tellers (
    tid integer NOT NULL,
    bid integer,
    tbalance integer,
    filler character(84),
    -- more web-app like columns
    text_column_plain TEXT  DEFAULT repeat('NeonIsCool', 5),
    jsonb_column_extended JSONB  DEFAULT ('{ "tell everyone": [' || repeat('{"Neon": "IsCool"},',9) || ' {"Neon": "IsCool"}]}')::jsonb
)
WITH (fillfactor='100');

--
-- Name: pgbench_accounts pgbench_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: peterbendel
--

ALTER TABLE ONLY pgbench_accounts
    ADD CONSTRAINT pgbench_accounts_pkey PRIMARY KEY (aid);

--
-- Name: pgbench_branches pgbench_branches_pkey; Type: CONSTRAINT; Schema: public; Owner: peterbendel
--

ALTER TABLE ONLY pgbench_branches
    ADD CONSTRAINT pgbench_branches_pkey PRIMARY KEY (bid);

--
-- Name: pgbench_tellers pgbench_tellers_pkey; Type: CONSTRAINT; Schema: public; Owner: peterbendel
--

ALTER TABLE ONLY pgbench_tellers
    ADD CONSTRAINT pgbench_tellers_pkey PRIMARY KEY (tid);


-- ========================================================
-- We create one foreign key to enable account history queries
-- in the read-only part of our workload.
-- SELECT * FROM pgbench_history WHERE aid = xxx;
-- ========================================================

--
-- Name: pgbench_history pgbench_history_aid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: peterbendel
--

ALTER TABLE ONLY public.pgbench_history
    ADD CONSTRAINT pgbench_history_aid_fkey FOREIGN KEY (aid) REFERENCES public.pgbench_accounts(aid);

-- ========================================================
-- also add an index to enable point queries using aid on 
-- pgbench_history
-- ========================================================

CREATE INDEX idx_pbgbench_history_aid ON pgbench_history(aid);
