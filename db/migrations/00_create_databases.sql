-- Creates the airflow metadata database and separate users for isolation
CREATE DATABASE airflow;

CREATE USER ods_app WITH PASSWORD 'ods';
GRANT ALL PRIVILEGES ON DATABASE ods_dev TO ods_app;

CREATE USER airflow_app WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_app;
