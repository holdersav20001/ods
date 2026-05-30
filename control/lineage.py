"""Lineage wrappers over cp.write_lineage_link and cp.write_link_then_rows.

target_ref / edges / rows are Python objects; psycopg sends them as jsonb via
the Jsonb wrapper.
"""
from psycopg.types.json import Jsonb


def _validate_target_ref(target_ref) -> None:
    """P2 (Codex) — enforce the target_ref CONTRACT client-side for clear errors,
    as defense in depth ahead of the DB target_ref_contract CHECK (012).

    A link's output identity is a contract, not a convention: target_ref MUST be
    a dict carrying a non-empty `path` (str), a non-empty `content_hash` (str),
    and a present `version` key. Raise ValueError with a precise message on any
    violation so callers fail fast and legibly instead of getting an opaque
    CheckViolation from Postgres.
    """
    if not isinstance(target_ref, dict):
        raise ValueError(
            f"target_ref must be a dict with path/content_hash/version, "
            f"got {type(target_ref).__name__}")
    path = target_ref.get("path")
    if not isinstance(path, str) or path == "":
        raise ValueError("target_ref.path must be a non-empty string")
    content_hash = target_ref.get("content_hash")
    if not isinstance(content_hash, str) or content_hash == "":
        raise ValueError("target_ref.content_hash must be a non-empty string")
    if "version" not in target_ref:
        raise ValueError("target_ref must carry a 'version' key")


def write_link(conn, *, consumer_run_id, edge_type, target_ref, record_count,
               edges, sink_type=None, transform_version=None, commit=True) -> str:
    _validate_target_ref(target_ref)
    link_id = conn.execute(
        "SELECT cp.write_lineage_link(%s,%s,%s,%s,%s,%s,%s)",
        [consumer_run_id, edge_type, Jsonb(target_ref), record_count,
         Jsonb(edges), sink_type, transform_version],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(link_id)


def write_link_then_rows(conn, *, consumer_run_id, edge_type, target_ref,
                         record_count, edges, rows, sink_type=None,
                         transform_version=None, source_file_id=None,
                         commit=True) -> str:
    """Write the link+edges, THEN the target rows, in one transaction.

    `source_file_id` (P10-D / §4) is stamped onto each row's _ods_source_file_id
    for row-level file attribution. Pass it ONLY when the rows map cleanly to ONE
    source file (single-file ingest->sink path); leave None for aggregate/
    merge-derived rows (the column stays NULL — do not pretend an aggregate came
    from one file).
    """
    _validate_target_ref(target_ref)
    link_id = conn.execute(
        "SELECT cp.write_link_then_rows(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        [consumer_run_id, edge_type, Jsonb(target_ref), record_count,
         Jsonb(edges), Jsonb(rows), sink_type, transform_version,
         source_file_id],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(link_id)


def write_trigger(conn, *, triggered_run_id, trigger_source, commit=True) -> str:
    """Record an ORCHESTRATION trigger as a non-provenance lineage link.

    H-trigger mechanism: when one workflow triggers another run, that causal
    edge is real history but it is NOT data provenance — it must NEVER appear on
    a trace-to-raw walk. We record it as an 'orchestrates' lineage_link
    (edge_type='orchestrates', is_provenance=false in cp.edge_type), so
    cp.v_provenance — which joins edge_type ON is_provenance — structurally
    excludes it. The link still exists in cp.lineage_link / cp.lineage_edge for
    audit ("what kicked this off"), it just cannot pollute lineage.

    The link is written through the SAME sanctioned primitive
    (cp.write_lineage_link) as every other edge — no hand-built rows. The
    triggered run is the CONSUMER of the orchestrates edge; target_ref carries a
    synthetic trigger:// path discriminated by the triggered run id (its
    content_hash), so repeated triggers of the same run are idempotent.

    Returns the orchestrates link_id.
    """
    return write_link(
        conn,
        consumer_run_id=triggered_run_id,
        edge_type="orchestrates",
        target_ref={
            "path": f"trigger://{trigger_source}",
            "content_hash": triggered_run_id,
            "version": 1,
        },
        record_count=0,
        edges=[{
            "edge_type": "orchestrates",
            "source_ref": {"trigger_source": trigger_source},
            "record_count": 0,
        }],
        commit=commit,
    )
