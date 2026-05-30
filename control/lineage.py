"""Lineage wrappers over cp.write_lineage_link and cp.write_link_then_rows.

target_ref / edges / rows are Python objects; psycopg sends them as jsonb via
the Jsonb wrapper.

NAMING (spec docs/specs/2026-05-30-output-link-input-edge-rename.md, Option B):
  output_link             = what a run produced  (physical: cp.lineage_link;
                            new-name read view: cp.output_link)
  input_edge              = what that output was made from (physical:
                            cp.lineage_edge; new-name read view: cp.input_edge)
  upstream_output_link_id = a previous output used as input (physical column:
                            upstream_lineage_link_id)

The PREFERRED new API is ``write_output_link`` / ``write_output_then_rows``,
which take ``inputs`` (list of dicts keyed with the NEW name
``upstream_output_link_id``). They translate that key to the physical
``upstream_lineage_link_id`` and delegate to the existing write path. The OLD
names ``write_link`` / ``write_link_then_rows`` (taking ``edges`` keyed with
``upstream_lineage_link_id``) keep working unchanged for existing callers.
"""
from psycopg.types.json import Jsonb


# New-name -> physical-name translation for input/edge dict keys (Option B).
# Only the upstream pointer was renamed; every other input key is unchanged.
_INPUT_KEY_TRANSLATION = {
    "upstream_output_link_id": "upstream_lineage_link_id",
}


def _translate_inputs(inputs):
    """Translate new-name ``inputs`` dicts into physical-name ``edges`` dicts.

    Each input may use the new key ``upstream_output_link_id``; it is mapped to
    the physical ``upstream_lineage_link_id``. All other keys (source_file_id,
    edge_type, input_slot, source_ref, record_count, upstream_run_id) pass
    through unchanged. A dict that already uses the physical key is accepted as-is
    (translation is idempotent), but supplying BOTH the new and old key for the
    same input is a conflict and raises.
    """
    edges = []
    for item in inputs:
        edge = {}
        for key, value in item.items():
            physical = _INPUT_KEY_TRANSLATION.get(key, key)
            if physical in edge:
                raise ValueError(
                    f"input supplies both new and old key for {physical!r}; "
                    f"use only upstream_output_link_id")
            edge[physical] = value
        edges.append(edge)
    return edges


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


def write_output_link(conn, *, consumer_run_id, edge_type, target_ref,
                      record_count, inputs, sink_type=None,
                      transform_version=None, commit=True) -> str:
    """Record ONE produced output (an output_link) and its input edges.

    PREFERRED new-name API (spec §"API / Wrapper Changes"). ``inputs`` is a list
    of input-edge dicts using the NEW key ``upstream_output_link_id`` (a previous
    output used as input), plus the unchanged keys ``source_file_id``,
    ``edge_type``, ``input_slot``, ``source_ref``, ``record_count``,
    ``upstream_run_id``. The inputs are translated to physical-name edges and the
    existing sanctioned write path (cp.write_lineage_link via ``write_link``) is
    used. Returns the output_link_id (the produced output's id).
    """
    return write_link(
        conn,
        consumer_run_id=consumer_run_id,
        edge_type=edge_type,
        target_ref=target_ref,
        record_count=record_count,
        edges=_translate_inputs(inputs),
        sink_type=sink_type,
        transform_version=transform_version,
        commit=commit,
    )


def write_output_then_rows(conn, *, consumer_run_id, edge_type, target_ref,
                           record_count, inputs, rows, sink_type=None,
                           transform_version=None, source_file_id=None,
                           commit=True) -> str:
    """Record an output_link + its input edges, THEN the target rows, atomically.

    PREFERRED new-name API. Same ``inputs`` translation as ``write_output_link``;
    delegates to the existing ``write_link_then_rows`` (cp.write_link_then_rows),
    which stamps each target row with the output id under BOTH
    ``_ods_lineage_link_id`` and (where the target table carries it)
    ``_ods_output_link_id``. Returns the output_link_id.
    """
    return write_link_then_rows(
        conn,
        consumer_run_id=consumer_run_id,
        edge_type=edge_type,
        target_ref=target_ref,
        record_count=record_count,
        edges=_translate_inputs(inputs),
        rows=rows,
        sink_type=sink_type,
        transform_version=transform_version,
        source_file_id=source_file_id,
        commit=commit,
    )


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
