-- This file was generated using the `spec/dummy` Rails app that was removed
-- before v3.0.0.
--
--     cd spec/dummy; bundle; bundle exec rake db:migrate
--     pg_dump -h 127.0.0.1 -U postgcres -W part_test
--
-- https://github.com/jobseekerltd/partitioned/blob/v2.1.0/spec/dummy

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


ALTER TABLE public.schema_migrations OWNER TO postgres;

-- COPY public.schema_migrations (version) FROM stdin;
-- \.

CREATE UNIQUE INDEX unique_schema_migrations ON public.schema_migrations USING btree (version);

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;
