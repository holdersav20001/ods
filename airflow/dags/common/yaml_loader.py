import hashlib, json, yaml


# Source-config keys we never persist to dataset_config.source_config.
# secret_ref names are kept; resolved secret values are looked up at
# poll time from the env / Airflow Secrets Backend and never stored.
_SECRET_KEYS = frozenset({"token", "password", "client_secret", "api_key"})


def load_dataset_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def compute_hash(cfg: dict) -> str:
    canonical = json.dumps(cfg, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _scrub_secrets(value):
    """Recursively drop any key in _SECRET_KEYS so we never persist secret
    material into pipeline.dataset_config.source_config. Only the
    ``secret_ref`` name is allowed through."""
    if isinstance(value, dict):
        return {
            k: _scrub_secrets(v)
            for k, v in value.items()
            if k not in _SECRET_KEYS
        }
    if isinstance(value, list):
        return [_scrub_secrets(v) for v in value]
    return value


def sync_to_db(path: str, pg_conn) -> None:
    try:
        cfg = load_dataset_yaml(path)
        h = compute_hash(cfg)
        source_type = cfg.get('source_type', 's3_batch')
        delivery = cfg.get('delivery', 'file_pipeline')
        # filename_pattern is required for s3_batch / file pattern; api_pull
        # datasets have no upstream filename and pass NULL after migration 24.
        filename_pattern = cfg.get('filename_pattern')
        if source_type == 's3_batch' and not filename_pattern:
            raise ValueError(
                f"dataset_config {cfg.get('domain')}/{cfg.get('dataset')} "
                f"with source_type='s3_batch' requires filename_pattern"
            )
        # target_topic is required for Kafka-leg deliveries. direct_postgres
        # has no Kafka leg, so accept NULL there.
        target_topic = cfg.get('target_topic')
        if delivery in ('file_pipeline', 'direct_kafka') and not target_topic:
            raise ValueError(
                f"dataset_config {cfg.get('domain')}/{cfg.get('dataset')} "
                f"with delivery={delivery!r} requires target_topic"
            )
        raw_format = cfg.get('raw_format', 'csv')
        source_config = _scrub_secrets(cfg.get('source', {})) or {}
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT config_yaml_hash FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s",
                (cfg['domain'], cfg['dataset']),
            )
            row = cur.fetchone()
            if row and row[0] == h:
                return
            cur.execute(
                """
                INSERT INTO pipeline.dataset_config
                    (domain, dataset, source_type, filename_pattern, target_topic,
                     schema_id, key_fields, dq_rules, schema_def,
                     postgres_target_table, s3_curated_path,
                     config_version_id, config_yaml_hash, config_pinned_at,
                     recon_tolerance_records, recon_tolerance_pct, write_mode,
                     is_canonical, canonical_topic, canonical_schema_id,
                     transform_yaml_path, raw_format, source_config, delivery)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s, 1,%s,NOW(), %s,%s, %s,
                        %s,%s,%s,%s, %s,%s, %s)
                ON CONFLICT (domain, dataset) DO UPDATE SET
                    source_type=EXCLUDED.source_type,
                    filename_pattern=EXCLUDED.filename_pattern,
                    target_topic=EXCLUDED.target_topic,
                    key_fields=EXCLUDED.key_fields,
                    dq_rules=EXCLUDED.dq_rules,
                    schema_def=EXCLUDED.schema_def,
                    postgres_target_table=EXCLUDED.postgres_target_table,
                    s3_curated_path=EXCLUDED.s3_curated_path,
                    config_version_id=pipeline.dataset_config.config_version_id + 1,
                    config_yaml_hash=EXCLUDED.config_yaml_hash,
                    config_pinned_at=NOW(),
                    recon_tolerance_records=EXCLUDED.recon_tolerance_records,
                    recon_tolerance_pct=EXCLUDED.recon_tolerance_pct,
                    write_mode=EXCLUDED.write_mode,
                    is_canonical=EXCLUDED.is_canonical,
                    canonical_topic=EXCLUDED.canonical_topic,
                    canonical_schema_id=EXCLUDED.canonical_schema_id,
                    transform_yaml_path=EXCLUDED.transform_yaml_path,
                    raw_format=EXCLUDED.raw_format,
                    source_config=EXCLUDED.source_config,
                    delivery=EXCLUDED.delivery
                """,
                (
                    cfg['domain'], cfg['dataset'], source_type,
                    filename_pattern, target_topic,
                    cfg.get('schema_id', f"{cfg['domain']}.{cfg['dataset']}"),
                    json.dumps(cfg['key_fields']),
                    json.dumps(cfg.get('dq_rules', {})), json.dumps(cfg.get('schema_def', {})),
                    cfg.get('postgres_target_table'), cfg.get('s3_curated_path'),
                    h,
                    int(cfg.get('recon_tolerance_records', 0)),
                    float(cfg.get('recon_tolerance_pct', 0)),
                    cfg.get('write_mode', 'upsert'),
                    bool(cfg.get('is_canonical', True)),
                    cfg.get('canonical_topic'),
                    cfg.get('canonical_schema_id'),
                    cfg.get('transform_yaml_path'),
                    raw_format,
                    json.dumps(source_config),
                    cfg.get('delivery', 'file_pipeline'),
                ),
            )
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
