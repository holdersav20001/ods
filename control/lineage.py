"""Lineage wrappers over cp.write_lineage_link and cp.write_link_then_rows.

target_ref / edges / rows are Python objects; psycopg sends them as jsonb via
the Jsonb wrapper.
"""
from psycopg.types.json import Jsonb


def write_link(conn, *, consumer_run_id, edge_type, target_ref, record_count,
               edges, sink_type=None, transform_version=None, commit=True) -> str:
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
                         transform_version=None, commit=True) -> str:
    link_id = conn.execute(
        "SELECT cp.write_link_then_rows(%s,%s,%s,%s,%s,%s,%s,%s)",
        [consumer_run_id, edge_type, Jsonb(target_ref), record_count,
         Jsonb(edges), Jsonb(rows), sink_type, transform_version],
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
        },
        record_count=0,
        edges=[{
            "edge_type": "orchestrates",
            "source_ref": {"trigger_source": trigger_source},
            "record_count": 0,
        }],
        commit=commit,
    )
