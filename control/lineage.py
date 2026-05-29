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
