import os, glob
import pendulum, psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, os.path.dirname(__file__))
from common.yaml_loader import sync_to_db

DATASETS_DIR = os.environ.get('DATASETS_DIR', '/opt/airflow/datasets')
PG_DSN = os.environ.get('PIPELINE_PG_DSN', 'host=postgres port=5432 dbname=ods user=postgres password=postgres')

def run_sync():
    conn = psycopg2.connect(PG_DSN)
    failed = []
    paths = sorted(glob.glob(f'{DATASETS_DIR}/**/*.yaml', recursive=True))
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

with DAG(
    dag_id='dag_config_sync',
    start_date=pendulum.datetime(2026, 4, 28, tz='UTC'),
    schedule=None,
    catchup=False,
    tags=['ods', 'config'],
):
    PythonOperator(task_id='sync_yaml_to_db', python_callable=run_sync)
