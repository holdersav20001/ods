import hashlib
import json
import os
import sys

import yaml

# Make ods_pipeline importable from this DAG file even when the
# Airflow scheduler runs us with a stripped sys.path.
_HERE = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_HERE, "..", "..", "..")),
    os.path.abspath(os.path.join(_HERE, "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

from ods_pipeline.config import (  # noqa: E402
    check_no_filename_pattern_overlap,
    validate_dataset_config,
)

# Source-config keys we never persist to dataset_config.source_config.
# secret_ref names are kept; resolved secret values are looked up at
# poll time from the env / Airflow Secrets Backend and never stored.
_SECRET_KEYS = frozenset({"token", "password", "client_secret", "api_key"})
_COMPONENT_FILES = {
    "source.yaml",
    "contract.yaml",
    "quality.yaml",
    "transform.yaml",
    "delivery.yaml",
    "reconciliation.yaml",
}


def _read_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base: dict, overlay: dict) -> dict:
    out = dict(base)
    for key, value in overlay.items():
        if (
            key in out
            and isinstance(out[key], dict)
            and isinstance(value, dict)
        ):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _compose_component(filename: str, component: dict) -> dict:
    if filename == "quality.yaml":
        return {"dq_rules": component.get("dq_rules", component)}
    if filename == "reconciliation.yaml":
        tolerances = component.get("tolerances", {})
        composed = {"reconciliation": component}
        if "records" in tolerances:
            composed["recon_tolerance_records"] = tolerances["records"]
        if "pct" in tolerances:
            composed["recon_tolerance_pct"] = tolerances["pct"]
        return composed
    if filename == "contract.yaml" and "fields" in component:
        component = dict(component)
        component["schema_def"] = {"fields": component.pop("fields")}
    return component


def _component_paths(dataset_yaml_path: str, cfg: dict) -> list[str]:
    base_dir = os.path.dirname(dataset_yaml_path)
    refs = cfg.get("refs")
    if isinstance(refs, dict):
        return [
            os.path.join(base_dir, ref)
            for ref in refs.values()
            if isinstance(ref, str)
        ]
    return [
        os.path.join(base_dir, filename)
        for filename in sorted(_COMPONENT_FILES)
        if os.path.exists(os.path.join(base_dir, filename))
    ]


def _is_split_dataset_yaml(path: str, cfg: dict) -> bool:
    if os.path.basename(path) == "dataset.yaml":
        return True
    refs = cfg.get("refs")
    return isinstance(refs, dict) and bool(refs)


def discover_dataset_yaml_paths(base_dir: str) -> list[str]:
    """Return syncable dataset YAML entrypoints under ``base_dir``.

    Split configs use ``dataset.yaml`` as the entrypoint; sibling component
    YAMLs are loaded by :func:`load_dataset_yaml` and are not synced directly.
    Legacy single-file configs remain valid.
    """
    paths: list[str] = []
    for root, _dirs, files in os.walk(base_dir):
        file_set = set(files)
        if "dataset.yaml" in file_set:
            paths.append(os.path.join(root, "dataset.yaml"))
            continue
        for filename in files:
            if filename.endswith((".yaml", ".yml")) and filename not in _COMPONENT_FILES:
                paths.append(os.path.join(root, filename))
    return sorted(paths)


def load_dataset_yaml(path: str) -> dict:
    cfg = _read_yaml(path)
    if not _is_split_dataset_yaml(path, cfg):
        return cfg

    cfg = {k: v for k, v in cfg.items() if k != "refs"}
    for component_path in _component_paths(path, cfg):
        component = _read_yaml(component_path)
        filename = os.path.basename(component_path)
        cfg = _deep_merge(cfg, _compose_component(filename, component))
    return cfg


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


def _active_peers(pg_conn) -> list[dict]:
    """Pull the active s3_batch peers so the overlap check has data."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain, dataset, source_type, filename_pattern,
                   COALESCE(delivery, 'file_pipeline')
              FROM pipeline.dataset_config
             WHERE active = TRUE
            """
        )
        return [
            {"domain": d, "dataset": ds, "source_type": st,
             "filename_pattern": fp, "delivery": dlv}
            for d, ds, st, fp, dlv in cur.fetchall()
        ]


def sync_to_db(path: str, pg_conn) -> None:
    try:
        cfg = load_dataset_yaml(path)
        # Fail BEFORE the INSERT so impossible combinations are caught
        # at sync time with a single operator-readable message rather
        # than discovered six hours later in a failing run.
        validate_dataset_config(cfg)
        # Cross-dataset overlap: two datasets with regex-overlapping
        # filename_patterns would cause dag_drop_to_raw to register
        # the same physical SFTP file twice. Catch at sync time.
        check_no_filename_pattern_overlap(cfg, _active_peers(pg_conn))
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
