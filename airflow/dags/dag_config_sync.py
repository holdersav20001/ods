import os
import sys

import pendulum
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from common.connector_provisioner import provision_all_from_db
from common.yaml_loader import discover_dataset_yaml_paths, sync_to_db

DATASETS_DIR = os.environ.get('DATASETS_DIR', '/opt/airflow/datasets')
PG_DSN = os.environ.get('PIPELINE_PG_DSN', 'host=postgres port=5432 dbname=ods_dev user=ods password=ods')


def run_sync():
    conn = psycopg2.connect(PG_DSN)
    failed = []
    paths = discover_dataset_yaml_paths(DATASETS_DIR)
    try:
        for path in paths:
            try:
                sync_to_db(path, conn)
            except Exception as e:
                print(f"sync failed for {path}: {e}")
                failed.append(path)
    finally:
        conn.close()
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(paths)} dataset YAMLs failed to sync")


def run_provision_connectors():
    conn = psycopg2.connect(PG_DSN)
    try:
        results = provision_all_from_db(conn)
        for key, created in results.items():
            print(f"  {'CREATED' if created else 'EXISTS '}: {key}")
    finally:
        conn.close()


with DAG(
    dag_id='dag_config_sync',
    start_date=pendulum.datetime(2026, 4, 28, tz='UTC'),
    schedule=None,
    catchup=False,
    tags=['ods', 'config'],
):
    sync = PythonOperator(task_id='sync_yaml_to_db', python_callable=run_sync)
    provision = PythonOperator(task_id='provision_connectors', python_callable=run_provision_connectors)
    sync >> provision
