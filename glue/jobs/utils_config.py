# glue/jobs/utils_config.py
"""Dataset config loader — reads pipeline.dataset_config from Postgres."""


def load_dataset_config(conn, domain: str, dataset: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, domain, dataset, filename_pattern, target_topic,
                   schema_id, schema_version, key_fields, dq_rules,
                   data_classification, version,
                   is_canonical, canonical_topic, canonical_schema_id,
                   transform_yaml_path, source_type,
                   COALESCE(raw_format, 'csv'),
                   COALESCE(source_config, '{}'::jsonb),
                   postgres_target_table, s3_curated_path, write_mode,
                   COALESCE(delivery, 'file_pipeline'),
                   COALESCE(recon_tolerance_records, 0),
                   COALESCE(recon_tolerance_pct, 0)
            FROM pipeline.dataset_config
            WHERE domain = %s AND dataset = %s AND active = TRUE
            """,
            (domain, dataset),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"No active config for {domain}/{dataset}")
    cols = [
        "id", "domain", "dataset", "filename_pattern", "target_topic",
        "schema_id", "schema_version", "key_fields", "dq_rules",
        "data_classification", "version", "is_canonical",
        "canonical_topic", "canonical_schema_id", "transform_yaml_path",
        "source_type", "raw_format", "source_config",
        "postgres_target_table", "s3_curated_path", "write_mode",
        "delivery", "recon_tolerance_records", "recon_tolerance_pct",
    ]
    return dict(zip(cols, row))
