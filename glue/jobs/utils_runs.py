# glue/jobs/utils_runs.py
"""Run-header and stage-row writes (pipeline.run / pipeline.stage)."""

from utils_bootstrap import *  # noqa: F401,F403  ensure ods_pipeline on sys.path


def upsert_run_header(
    pg_dsn,
    *,
    run_id,
    pipeline_type,
    domain,
    dataset,
    business_date,
    file_id=None,
    kafka_topic=None,
    config_version_id=None,
    schema_version_id=None,
    parents=None,
) -> None:
    import psycopg2

    from ods_pipeline import runs

    with psycopg2.connect(pg_dsn) as conn:
        runs.start(
            conn,
            run_id=run_id,
            pipeline_type=pipeline_type,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            file_id=file_id,
            kafka_topic=kafka_topic,
            config_version_id=config_version_id,
            schema_version_id=schema_version_id,
            parents=parents,
        )


def update_run_fields(pg_dsn, run_id, **fields) -> None:
    import psycopg2

    from ods_pipeline import runs

    with psycopg2.connect(pg_dsn) as conn:
        runs.update(conn, run_id, **fields)


def write_stage_row(
    pg_dsn,
    *,
    run_id,
    stage,
    status,
    event_type=None,
    attempt_number=1,
    input_ref=None,
    output_ref=None,
    record_count_in=None,
    record_count_out=None,
    metrics=None,
    error=None,
    airflow_dag_id=None,
    airflow_run_id=None,
    spark_app_id=None,
) -> None:
    import psycopg2

    from ods_pipeline import stages

    kwargs = {
        "run_id": run_id,
        "stage": stage,
        "attempt_number": attempt_number,
        "input_ref": input_ref,
        "output_ref": output_ref,
        "record_count_in": record_count_in,
        "record_count_out": record_count_out,
        "metrics": metrics,
        "error": error,
        "airflow_dag_id": airflow_dag_id,
        "airflow_run_id": airflow_run_id,
        "spark_app_id": spark_app_id,
    }
    with psycopg2.connect(pg_dsn) as conn:
        if status == "running":
            stages.start(conn, **kwargs)
        else:
            stages.finish(conn, status=status, event_type=event_type, **kwargs)
