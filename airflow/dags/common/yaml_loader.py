import hashlib, json, yaml

def load_dataset_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def compute_hash(cfg: dict) -> str:
    canonical = json.dumps(cfg, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()

def sync_to_db(path: str, pg_conn) -> None:
    try:
        cfg = load_dataset_yaml(path)
        h = compute_hash(cfg)
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
                     recon_tolerance_records, recon_tolerance_pct)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s, 1,%s,NOW(), %s,%s)
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
                    recon_tolerance_pct=EXCLUDED.recon_tolerance_pct
                """,
                (
                    cfg['domain'], cfg['dataset'], cfg.get('source_type', 's3_batch'),
                    cfg['filename_pattern'], cfg['target_topic'],
                    f"{cfg['domain']}.{cfg['dataset']}", json.dumps(cfg['key_fields']),
                    json.dumps(cfg.get('dq_rules', {})), json.dumps(cfg.get('schema_def', {})),
                    cfg['postgres_target_table'], cfg['s3_curated_path'],
                    h,
                    int(cfg.get('recon_tolerance_records', 0)),
                    float(cfg.get('recon_tolerance_pct', 0)),
                ),
            )
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
