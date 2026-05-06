# glue/jobs/ods_merge.py
"""
ODS Glue merge job — staged slot tables → wide Postgres target.

Reads all slot staging tables for a given business_date, performs a
full-outer join, writes to the wide target table, and records per-column
lineage in merge_contribution_log.

Usage:
    spark-submit ods_merge.py \
        --merge_run_id <uuid5> \
        --domain       insurance \
        --dataset      policies_enriched \
        --business_date 2026-06-01
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2
from utils import update_run_fields, upsert_run_header, write_stage_row

# Slot definitions: which staging table owns which columns.
# Derived from dataset_config at runtime; also encoded here as fallback.
SLOT_COLUMNS = {
    "core":        ["status", "premium", "effective_date"],
    "enrichment":  ["agent_code", "postcode", "risk_score", "channel"],
}


def _get_pg_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "ods_dev"),
        user=os.environ.get("POSTGRES_USER", "ods"),
        password=os.environ.get("POSTGRES_PASSWORD", "ods"),
    )


def _pg_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'ods_dev')} "
        f"user={os.environ.get('POSTGRES_USER', 'ods')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'ods')}"
    )


def _load_slot_defs(conn, domain: str, merge_dataset: str) -> list[dict]:
    """Return slot definitions ordered by slot_name."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT slot_name, dataset, staging_table
            FROM pipeline.dataset_config
            WHERE domain=%s AND merge_dataset=%s AND active=TRUE
            ORDER BY slot_name
            """,
            (domain, merge_dataset),
        )
        rows = cur.fetchall()
    if not rows:
        raise ValueError(f"No slot configs found for {domain}/{merge_dataset}")
    return [{"slot_name": r[0], "dataset": r[1], "staging_table": r[2]} for r in rows]


def _get_latest_slot_run(conn, domain: str, slot_dataset: str,
                         business_date: str) -> tuple[str | None, str | None]:
    """Return (run_id, file_id) for most recent succeeded stage run for this slot+bd."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.run_id::text, r.file_id::text
            FROM pipeline.run_log r
            JOIN pipeline.dataset_config dc ON dc.domain=r.domain AND dc.dataset=r.dataset
            WHERE r.domain=%s AND r.dataset=%s
              AND r.business_date=%s AND r.status='succeeded'
              AND r.pipeline_type='stage'
            ORDER BY r.started_at DESC LIMIT 1
            """,
            (domain, slot_dataset, business_date),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def _get_s3_raw_path(conn, run_id: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s3_raw_path FROM pipeline.file_catalogue WHERE file_id="
            "(SELECT file_id FROM pipeline.run_log WHERE run_id=%s LIMIT 1)",
            (run_id,),
        )
        row = cur.fetchone()
    return row[0] if row else f"unknown (run_id={run_id})"


def _read_staging(conn, staging_table: str, business_date: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {staging_table} WHERE _ods_business_date=%s",
            (business_date,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _merge_slots(core_rows: list[dict], enrich_rows: list[dict]) -> list[dict]:
    """Full-outer join on policy_id."""
    core_map = {r["policy_id"]: r for r in core_rows}
    enrich_map = {r["policy_id"]: r for r in enrich_rows}
    all_keys = set(core_map) | set(enrich_map)
    merged = []
    for key in sorted(all_keys):
        c = core_map.get(key, {})
        e = enrich_map.get(key, {})
        merged.append({
            "policy_id":      key,
            "status":         c.get("status"),
            "premium":        c.get("premium"),
            "effective_date": c.get("effective_date"),
            "agent_code":     e.get("agent_code"),
            "postcode":       e.get("postcode"),
            "risk_score":     e.get("risk_score"),
            "channel":        e.get("channel"),
            "_ods_run_id_core":   c.get("_ods_run_id"),
            "_ods_run_id_enrich": e.get("_ods_run_id"),
        })
    return merged


def _write_wide(conn, rows: list[dict], merge_run_id: str, business_date: str) -> int:
    if not rows:
        return 0
    cols = [
        "policy_id", "status", "premium", "effective_date",
        "agent_code", "postcode", "risk_score", "channel",
        "_ods_merge_run_id", "_ods_run_id_core", "_ods_run_id_enrich",
        "_ods_business_date", "_ods_merged_at",
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(f'"{c}"' for c in cols)
    update_set = ", ".join(
        f'"{c}"=EXCLUDED."{c}"'
        for c in cols if c != "policy_id"
    )

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM ods.policies_enriched WHERE _ods_business_date=%s",
            (business_date,),
        )
        data = [
            (
                row["policy_id"],
                row.get("status"),
                row.get("premium"),
                row.get("effective_date"),
                row.get("agent_code"),
                row.get("postcode"),
                row.get("risk_score"),
                row.get("channel"),
                merge_run_id,
                row.get("_ods_run_id_core"),
                row.get("_ods_run_id_enrich"),
                business_date,
                None,  # _ods_merged_at — DEFAULT NOW()
            )
            for row in rows
        ]
        # Remove _ods_merged_at from the explicit columns; let DEFAULT handle it
        short_cols = cols[:-1]
        short_ph = ", ".join(["%s"] * len(short_cols))
        short_col_list = ", ".join(f'"{c}"' for c in short_cols)
        short_update = ", ".join(
            f'"{c}"=EXCLUDED."{c}"' for c in short_cols if c != "policy_id"
        )
        cur.executemany(
            f'INSERT INTO ods.policies_enriched ({short_col_list}) '
            f'VALUES ({short_ph}) '
            f'ON CONFLICT (policy_id) DO UPDATE SET {short_update}, _ods_merged_at=NOW()',
            [d[:-1] for d in data],
        )
    conn.commit()
    return len(data)


def _write_contribution(conn, merge_run_id: str, slot_name: str, slot_run_id: str,
                        file_id: str | None, s3_raw_path: str,
                        columns_written: list[str], record_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.merge_contribution_log
                (merge_run_id, slot_name, slot_run_id, file_id, s3_raw_path,
                 columns_written, record_count)
            VALUES (%s,%s,%s,%s,%s, %s,%s)
            """,
            (merge_run_id, slot_name, slot_run_id,
             file_id, s3_raw_path, columns_written, record_count),
        )
    conn.commit()


def run(merge_run_id: str, domain: str, dataset: str, business_date: str) -> int:
    pg = _get_pg_conn()
    pg_dsn = _pg_dsn()

    # ── Idempotency: already succeeded? ───────────────────────────────────
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.merge_run_log "
            "WHERE merge_run_id=%s",
            (merge_run_id,),
        )
        existing = cur.fetchone()

    if existing and existing[0] == "succeeded":
        print(f"Merge {merge_run_id} already succeeded — skipping.", file=sys.stderr)
        return 0

    # ── Bootstrap merge_run_log + run_log ────────────────────────────────
    with pg.cursor() as cur:
        if existing:
            cur.execute(
                "UPDATE pipeline.merge_run_log SET status='running', error_summary=NULL "
                "WHERE merge_run_id=%s",
                (merge_run_id,),
            )
        else:
            cur.execute(
                """
                INSERT INTO pipeline.merge_run_log
                    (merge_run_id, domain, dataset, business_date, status)
                VALUES (%s,%s,%s,%s,'running')
                """,
                (merge_run_id, domain, dataset, business_date),
            )
    pg.commit()

    upsert_run_header(pg_dsn, run_id=merge_run_id, pipeline_type="merge",
                      domain=domain, dataset=dataset, business_date=business_date)

    current_stage = "merge_read"
    try:
        # ── Load slot definitions ──────────────────────────────────────
        slot_defs = _load_slot_defs(pg, domain, dataset)
        slot_data: dict[str, list[dict]] = {}
        slot_meta: dict[str, dict] = {}

        write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_read", status="running")

        for slot in slot_defs:
            sname = slot["slot_name"]
            rows = _read_staging(pg, slot["staging_table"], business_date)
            slot_data[sname] = rows
            slot_run_id, file_id = _get_latest_slot_run(pg, domain, slot["dataset"], business_date)
            s3_raw = _get_s3_raw_path(pg, slot_run_id) if slot_run_id else "unknown"
            slot_meta[sname] = {
                "run_id": slot_run_id,
                "file_id": file_id,
                "s3_raw_path": s3_raw,
                "count": len(rows),
            }

        write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_read",
                        status="succeeded",
                        record_count_in=sum(m["count"] for m in slot_meta.values()))

        # ── Merge ─────────────────────────────────────────────────────
        current_stage = "merge_write"
        core_rows = slot_data.get("core", [])
        enrich_rows = slot_data.get("enrichment", [])
        merged = _merge_slots(core_rows, enrich_rows)

        written = _write_wide(pg, merged, merge_run_id, business_date)

        write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_write",
                        status="succeeded",
                        output_ref="ods.policies_enriched",
                        record_count_out=written)

        # ── Lineage ───────────────────────────────────────────────────
        current_stage = "lineage"
        for slot in slot_defs:
            sname = slot["slot_name"]
            meta = slot_meta[sname]
            columns = SLOT_COLUMNS.get(sname, [])
            if meta["run_id"]:
                _write_contribution(
                    pg, merge_run_id, sname,
                    meta["run_id"], meta["file_id"],
                    meta["s3_raw_path"], columns, meta["count"],
                )

        # ── Finalise ──────────────────────────────────────────────────
        with pg.cursor() as cur:
            cur.execute(
                "UPDATE pipeline.merge_run_log "
                "SET status='succeeded', ended_at=NOW(), record_count_out=%s "
                "WHERE merge_run_id=%s",
                (written, merge_run_id),
            )
        pg.commit()
        update_run_fields(pg_dsn, merge_run_id,
                          status="succeeded",
                          record_count_published=written)
        pg.close()
        return 0

    except Exception as exc:
        msg = str(exc)
        try:
            with pg.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline.merge_run_log "
                    "SET status='failed', ended_at=NOW(), error_summary=%s "
                    "WHERE merge_run_id=%s",
                    (msg, merge_run_id),
                )
            pg.commit()
            update_run_fields(pg_dsn, merge_run_id, status="failed", error_summary=msg)
            write_stage_row(pg_dsn, run_id=merge_run_id, stage=current_stage,
                            status="failed", error=msg)
        except Exception:
            pass
        pg.close()
        print(f"FATAL: {msg}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--merge_run_id", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--business_date", required=True)
    args = parser.parse_args()
    sys.exit(run(args.merge_run_id, args.domain, args.dataset, args.business_date))
