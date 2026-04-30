-- Migration 15: add event_type + attempt_number to run_stage_log
-- Makes the append-only contract explicit — consumers filter on event_type
-- rather than inferring completion from status != 'running'

ALTER TABLE pipeline.run_stage_log
    ADD COLUMN IF NOT EXISTS event_type     VARCHAR,
    ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 1;

-- Fast terminal-event lookup: WHERE run_id=X AND event_type IN ('stage_completed','stage_failed','stage_skipped')
CREATE INDEX IF NOT EXISTS run_stage_log_event_idx
    ON pipeline.run_stage_log (run_id, event_type);

COMMENT ON COLUMN pipeline.run_stage_log.event_type IS
    'stage_started | stage_completed | stage_failed | stage_skipped | stage_warned';

COMMENT ON COLUMN pipeline.run_stage_log.attempt_number IS
    'Retry attempt counter. First attempt = 1. Incremented on retry.';

-- Migration 15b: correlation fields — link stage rows to Airflow/CloudWatch
ALTER TABLE pipeline.run_stage_log
    ADD COLUMN IF NOT EXISTS airflow_dag_id  VARCHAR,
    ADD COLUMN IF NOT EXISTS airflow_run_id  VARCHAR,
    ADD COLUMN IF NOT EXISTS spark_app_id    VARCHAR;

-- Jump-to-CloudWatch: filter log streams by spark_app_id prefix
-- Jump-to-Airflow:   /dags/{airflow_dag_id}/dagRuns/{airflow_run_id}
CREATE INDEX IF NOT EXISTS run_stage_log_spark_idx
    ON pipeline.run_stage_log (spark_app_id)
    WHERE spark_app_id IS NOT NULL;

COMMENT ON COLUMN pipeline.run_stage_log.airflow_dag_id IS
    'Airflow DAG id. Jump: /dags/{airflow_dag_id}/dagRuns/{airflow_run_id}';
COMMENT ON COLUMN pipeline.run_stage_log.airflow_run_id IS
    'Airflow run id (dag_run.run_id). Matches Airflow UI dag run key.';
COMMENT ON COLUMN pipeline.run_stage_log.spark_app_id IS
    'Spark application id (spark.sparkContext.applicationId). Filter CloudWatch log streams.';
