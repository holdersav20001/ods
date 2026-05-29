# glue/jobs/ods_merge.py
"""
ODS Glue merge job — staged slot tables → wide Postgres target.

Reads all slot staging tables for a given business_date, performs a
full-outer join, writes to the wide target table stamped with a single
_ods_lineage_link_id, and records one lineage_edge row per contributing
slot under that lineage_link (each edge carries the slot_name).

After migration 36 the dedicated merge_run_log and merge_contribution_log
tables are gone — run_log + lineage_link + lineage_edge cover the same
information uniformly with the rest of the platform.

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
import uuid

import psycopg2
from utils import update_run_fields, upsert_run_header, write_stage_row

import ods_pipeline  # noqa: E402  — lineage_link helper + autonomous run lookup

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
        # Preferred: file_catalogue via run_log.file_id (set when the stage
        # job registers the file). Fall back to run_stage_log.input_ref —
        # ods_stage records the s3 path there even when it does not register
        # the file in file_catalogue (e.g. legacy multi-file merge fixtures).
        cur.execute(
            "SELECT s3_raw_path FROM pipeline.file_catalogue WHERE file_id="
            "(SELECT file_id FROM pipeline.run_log WHERE run_id=%s LIMIT 1)",
            (run_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        cur.execute(
            "SELECT input_ref FROM pipeline.run_stage_log "
            "WHERE run_id=%s AND input_ref IS NOT NULL "
            "ORDER BY started_at LIMIT 1",
            (run_id,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else f"unknown (run_id={run_id})"


def _read_staging(conn, staging_table: str, business_date: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {staging_table} WHERE _ods_business_date=%s",
            (business_date,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _merge_slots(core_rows: list[dict], enrich_rows: list[dict]) -> list[dict]:
    """Full-outer join on policy_id.

    Migration 36: the legacy ``_ods_run_id_core`` / ``_ods_run_id_enrich``
    columns are gone. Slot contributions are recorded in
    ``pipeline.lineage_edge`` (one row per slot) under a single
    ``lineage_link_id`` that is stamped on every output row.
    """
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
        })
    return merged


def _write_wide(
    conn,
    rows: list[dict],
    lineage_link_id: str,
    business_date: str,
) -> int:
    """Write merged rows to ods.policies_enriched stamped with one
    lineage_link_id.

    Migration 36 contract: every row carries _ods_lineage_link_id as the
    sole lineage handle. Walk back to contributing slot runs via
    pipeline.lineage_edge.lineage_link_id.
    """
    if not rows:
        return 0
    short_cols = [
        "policy_id", "status", "premium", "effective_date",
        "agent_code", "postcode", "risk_score", "channel",
        "_ods_lineage_link_id", "_ods_business_date",
    ]
    short_ph = ", ".join(["%s"] * len(short_cols))
    short_col_list = ", ".join(f'"{c}"' for c in short_cols)
    short_update = ", ".join(
        f'"{c}"=EXCLUDED."{c}"' for c in short_cols if c != "policy_id"
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
            lineage_link_id,
            business_date,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM ods.policies_enriched WHERE _ods_business_date=%s",
            (business_date,),
        )
        cur.executemany(
            f'INSERT INTO ods.policies_enriched ({short_col_list}) '
            f'VALUES ({short_ph}) '
            f'ON CONFLICT (policy_id) DO UPDATE SET {short_update}, '
            f'_ods_merged_at=NOW()',
            data,
        )
    conn.commit()
    return len(data)


# NOTE: _write_contribution removed in migration 36. The merge_contribution_log
# table no longer exists. Slot contributions are now recorded as
# pipeline.lineage_edge rows under one shared lineage_link_id; the slot role
# travels on lineage_edge.slot_name. See ods_pipeline.lineage.write_link.


def run(merge_run_id: str, domain: str, dataset: str, business_date: str) -> int:
    """Run the multi-source merge.

    Migration 36: merge_run_log and merge_contribution_log are gone. The
    merge run is a regular run_log row with pipeline_type='merge'.
    Idempotency is enforced by checking run_log.status for the same
    merge_run_id (deterministically derived by the caller).
    """
    pg = _get_pg_conn()
    pg_dsn = _pg_dsn()

    # ── Idempotency: already succeeded? ───────────────────────────────────
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.run_log WHERE run_id=%s",
            (merge_run_id,),
        )
        existing = cur.fetchone()

    if existing and existing[0] == "succeeded":
        print(f"Merge {merge_run_id} already succeeded — skipping.",
              file=sys.stderr)
        return 0

    # ── Bootstrap run_log only (merge_run_log removed in migration 36) ──
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
            stage_run_id, file_id = _get_latest_slot_run(
                pg, domain, slot["dataset"], business_date
            )
            # Prefer a canonicalize run if one ran AFTER the stage; this
            # surfaces the per-slot canonicalize node in dashboards.
            canon_run_id = ods_pipeline.runs.latest_succeeded_run(
                pg, file_id=file_id, pipeline_type="canonicalize"
            ) if file_id else None
            upstream_run = canon_run_id or stage_run_id
            s3_raw = _get_s3_raw_path(pg, stage_run_id) if stage_run_id else "unknown"
            slot_meta[sname] = {
                "run_id": upstream_run,
                "stage_run_id": stage_run_id,
                "canon_run_id": canon_run_id,
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

        # Mint the lineage_link_id BEFORE writing rows so every target row
        # carries the same handle. Helper accepts pre-minted id (see
        # ods_pipeline.lineage.write_link).
        lineage_link_id = str(uuid.uuid4())
        written = _write_wide(pg, merged, lineage_link_id, business_date)

        write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_write",
                        status="succeeded",
                        output_ref="ods.policies_enriched",
                        record_count_out=written)

        # ── Lineage: one lineage_link + N edges (one per slot) ───────
        current_stage = "lineage"
        contributions = []
        for slot in slot_defs:
            sname = slot["slot_name"]
            meta = slot_meta[sname]
            if not meta["run_id"]:
                continue
            contributions.append({
                "upstream_run_id": meta["run_id"],
                "source_file_id":  meta["file_id"],
                "source_ref":      meta["s3_raw_path"],
                "slot_name":       sname,
                "record_count":    meta["count"],
                "edge_type":       "slot_to_merged",
            })

        ods_pipeline.lineage.write_link(
            pg,
            lineage_link_id=lineage_link_id,
            consumer_run_id=merge_run_id,
            edge_type="merge_to_postgres",
            target_ref="ods.policies_enriched",
            record_count=written,
            contributions=contributions or [{
                # Defensive: write_link requires >=1 contribution. If no slot
                # rows exist we still record an "empty" edge to make the
                # lineage_link discoverable.
                "upstream_run_id": None,
                "source_file_id":  None,
                "source_ref":      None,
                "slot_name":       "empty",
                "record_count":    0,
                "edge_type":       "slot_to_merged",
            }],
        )

        # ── Finalise (merge_run_log dropped in migration 36) ─────────
        update_run_fields(pg_dsn, merge_run_id,
                          status="succeeded",
                          record_count_target=written)
        pg.close()
        return 0

    except Exception as exc:
        msg = str(exc)
        try:
            update_run_fields(pg_dsn, merge_run_id, status="failed",
                              error_summary=msg)
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
