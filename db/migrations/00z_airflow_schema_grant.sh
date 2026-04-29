#!/bin/bash
# Grant airflow_app CREATE on public schema in the airflow database.
# Runs after 00_create_databases.sql via docker-entrypoint-initdb.d.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow <<-SQL
    GRANT ALL ON SCHEMA public TO airflow_app;
    ALTER SCHEMA public OWNER TO airflow_app;
SQL
