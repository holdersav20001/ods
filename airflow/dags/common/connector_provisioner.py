"""Auto-provision Kafka Connect JDBC sink connectors from dataset_config.

Reads all active s3_batch datasets that have a postgres_target_table and
target_topic, then creates a JDBC sink connector if one does not already exist.
write_mode controls insert behaviour:
  - upsert  → insert.mode=upsert, pk.mode=record_value (default)
  - append  → insert.mode=insert, pk.mode=none
  - replace → insert.mode=upsert, pk.mode=record_value (same as upsert;
              replace semantics enforced at ingest time by deleting the
              business-date partition before writing)
"""
from __future__ import annotations

import json
import os

import requests


CONNECT_URL = os.environ.get("CONNECT_URL", "http://kafka-connect:8083")
_PG_JDBC    = os.environ.get("JDBC_URL",       "jdbc:postgresql://postgres:5432/ods_dev")
_PG_USER    = os.environ.get("POSTGRES_USER",   "ods")
_PG_PASS    = os.environ.get("POSTGRES_PASSWORD", "ods")
_SR_URL     = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def connector_name(domain: str, dataset: str) -> str:
    return f"jdbc-sink-{domain}-{dataset}".replace("_", "-")


def _build_config(target_topic: str, postgres_table: str,
                  write_mode: str, key_fields: list[str]) -> dict:
    cfg = {
        "connector.class":                        "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max":                              "1",
        "topics":                                 target_topic,
        "connection.url":                         _PG_JDBC,
        "connection.user":                        _PG_USER,
        "connection.password":                    _PG_PASS,
        "auto.create":                            "false",
        "auto.evolve":                            "true",
        "table.name.format":                      postgres_table,
        "value.converter":                        "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url":    _SR_URL,
        "key.converter":                          "org.apache.kafka.connect.storage.StringConverter",
    }
    if write_mode == "append":
        cfg["insert.mode"] = "insert"
        cfg["pk.mode"]     = "none"
    else:
        cfg["insert.mode"] = "upsert"
        cfg["pk.mode"]     = "record_value"
        cfg["pk.fields"]   = ",".join(key_fields) if key_fields else "id"
    return cfg


def provision_connector(domain: str, dataset: str, target_topic: str,
                        postgres_table: str, write_mode: str,
                        key_fields: list[str]) -> bool:
    """Create connector if absent. Returns True when a new connector was created."""
    name = connector_name(domain, dataset)
    r = requests.get(f"{CONNECT_URL}/connectors/{name}", timeout=5)
    if r.status_code == 200:
        return False

    body = {"name": name, "config": _build_config(
        target_topic, postgres_table, write_mode, key_fields
    )}
    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=10,
    )
    resp.raise_for_status()
    print(f"Provisioned connector: {name} ({write_mode})")
    return True


def provision_all_from_db(pg_conn) -> dict[str, bool]:
    """Provision connectors for all active s3_batch datasets with a target table."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT domain, dataset, COALESCE(canonical_topic, target_topic) AS sink_topic,
                   postgres_target_table,
                   COALESCE(write_mode, 'upsert'), key_fields
            FROM pipeline.dataset_config
            WHERE active = TRUE
              AND source_type = 's3_batch'
              AND postgres_target_table IS NOT NULL
              AND COALESCE(canonical_topic, target_topic) IS NOT NULL
              AND COALESCE(canonical_topic, target_topic) != ''
        """)
        rows = cur.fetchall()

    results: dict[str, bool] = {}
    for domain, dataset, topic, table, write_mode, key_fields_raw in rows:
        if isinstance(key_fields_raw, str):
            key_fields = json.loads(key_fields_raw)
        else:
            key_fields = key_fields_raw or []
        try:
            created = provision_connector(domain, dataset, topic, table,
                                          write_mode, key_fields)
            results[f"{domain}/{dataset}"] = created
        except Exception as exc:
            print(f"WARNING: connector provision failed for {domain}/{dataset}: {exc}")
            results[f"{domain}/{dataset}"] = False
    return results
