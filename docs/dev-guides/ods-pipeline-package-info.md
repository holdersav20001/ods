# ods_pipeline Package Source

This file contains the `ods_pipeline` Python package source, written out file by file so it can be used on another computer where the repository is not available.

Important: `ods_pipeline` depends on the repo-local `ods_ingestion_control` package for database function calls. If you want this package to run elsewhere, copy/install `ods_ingestion_control` as well and make sure the repo root or package parent directory is on `PYTHONPATH`.

## File Tree

```text
ods_pipeline\__init__.py
ods_pipeline\_db.py
ods_pipeline\config\__init__.py
ods_pipeline\config\validator.py
ods_pipeline\dlq.py
ods_pipeline\events.py
ods_pipeline\files.py
ods_pipeline\ingest\__init__.py
ods_pipeline\ingest\api_pull\__init__.py
ods_pipeline\ingest\api_pull\archive.py
ods_pipeline\ingest\api_pull\auth.py
ods_pipeline\ingest\api_pull\cursors.py
ods_pipeline\ingest\api_pull\linkage.py
ods_pipeline\ingest\api_pull\poller.py
ods_pipeline\ingest\api_pull\watermark.py
ods_pipeline\ingest\api_pull_kafka\__init__.py
ods_pipeline\ingest\api_pull_kafka\loop.py
ods_pipeline\ingest\api_pull_kafka\runner.py
ods_pipeline\lineage.py
ods_pipeline\messages.py
ods_pipeline\metadata.py
ods_pipeline\models.py
ods_pipeline\offsets.py
ods_pipeline\ops\__init__.py
ods_pipeline\ops\__main__.py
ods_pipeline\ops\dlq.py
ods_pipeline\ops\runs.py
ods_pipeline\patterns\__init__.py
ods_pipeline\patterns\api_pull.py
ods_pipeline\patterns\base.py
ods_pipeline\patterns\event.py
ods_pipeline\patterns\file.py
ods_pipeline\publish.py
ods_pipeline\reconciliation.py
ods_pipeline\runs.py
ods_pipeline\stages.py
```

## Source Files

### `ods_pipeline\__init__.py`

```python
"""ODS Pipeline control-plane client.

Usage
-----
::

    import ods_pipeline

    with ods_pipeline.connect(dsn) as conn:
        file_id = ods_pipeline.files.upsert(conn, domain="insurance", ...)
        ods_pipeline.runs.start(conn, run_id=run_id, ...)
        ods_pipeline.stages.write(conn, run_id=run_id, stage=ods_pipeline.Stage.RAW_READ, ...)
        ods_pipeline.lineage.write_edge(conn, child_run_id=run_id, ...)
        ods_pipeline.runs.finish(conn, run_id=run_id, status="succeeded")

    ods_pipeline.events.produce("run_succeeded", run_id=run_id, ...)
"""

from ods_pipeline import (
    dlq,
    events,
    files,
    lineage,
    messages,
    metadata,
    offsets,
    publish,
    reconciliation,
    runs,
    stages,
)
from ods_pipeline._db import build_dsn, connect
from ods_pipeline.models import TERMINAL_STATUSES, RunStatus, Stage, StageEvent

__all__ = [
    # connection helpers
    "build_dsn",
    "connect",
    # sub-modules
    "files",
    "runs",
    "stages",
    "lineage",
    "reconciliation",
    "events",
    "metadata",
    "offsets",
    "messages",
    "dlq",
    "publish",
    # constants
    "Stage",
    "StageEvent",
    "RunStatus",
    "TERMINAL_STATUSES",
]
```

### `ods_pipeline\_db.py`

```python
"""Connection helpers for the ODS pipeline control-plane client."""
from __future__ import annotations

import os

import psycopg2


def build_dsn(dsn: str | None = None) -> str:
    """Return a libpq DSN string from explicit arg or environment variables.

    Precedence:
      1. ``dsn`` argument
      2. ``PIPELINE_PG_DSN`` env var
      3. ``PG_DSN`` env var
      4. Individual ``POSTGRES_*`` env vars
    """
    if dsn:
        return dsn
    for key in ("PIPELINE_PG_DSN", "PG_DSN"):
        val = os.environ.get(key)
        if val:
            return val
    host = os.environ.get("POSTGRES_HOST")
    if not host:
        raise ValueError(
            "No Postgres DSN configured. "
            "Set PIPELINE_PG_DSN, PG_DSN, or POSTGRES_HOST."
        )
    return (
        f"host={host} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'ods_dev')} "
        f"user={os.environ.get('POSTGRES_USER', 'ods')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'ods')}"
    )


def connect(dsn: str | None = None) -> psycopg2.extensions.connection:
    """Open and return a psycopg2 connection.

    Use as a context manager::

        with ods_pipeline.connect(dsn) as conn:
            ods_pipeline.stages.write(conn, ...)
    """
    return psycopg2.connect(build_dsn(dsn))
```

### `ods_pipeline\config\__init__.py`

```python
"""Dataset config validation helpers.

Centralises pre-INSERT checks the YAML loader runs against
``pipeline.dataset_config`` rows so impossible combinations (e.g.
``delivery=direct_postgres`` paired with a ``target_topic``, or
``write_mode=upsert`` with empty ``key_fields``) fail at sync time
with an operator-readable message instead of silently producing
nonsense at run time.
"""
from ods_pipeline.config.validator import (
    DatasetConfigError,
    check_no_filename_pattern_overlap,
    validate_dataset_config,
)

__all__ = [
    "DatasetConfigError",
    "check_no_filename_pattern_overlap",
    "validate_dataset_config",
]
```

### `ods_pipeline\config\validator.py`

```python
"""Pre-sync validation of dataset_config YAML.

Catches impossible combinations BEFORE they reach
``pipeline.dataset_config`` so:

  - operators see the error in the dag_config_sync output, not in a
    failed run six hours later;
  - ``yaml_loader.sync_to_db`` does not need to repeat the rules
    inline â€” the validator module owns the contract;
  - new combos can be added by extending one function with one rule
    + one unit test, no SQL changes.

Two functions:

  - ``validate_dataset_config(cfg)``  â€” pure-Python single-row checks.
    No I/O. Reusable from a future ``odscli config validate``.
  - ``check_no_filename_pattern_overlap(cfg, peers)`` â€” given a row
    + the active peers (a list of dicts pulled from
    ``pipeline.dataset_config``), reject regex collisions across
    different ``(domain, dataset)`` tuples that would cause
    ``dag_drop_to_raw`` to register the same physical file under two
    delivery routes.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Mapping


class DatasetConfigError(ValueError):
    """Raised when a dataset_config row violates a structural invariant."""


_ALLOWED_DELIVERIES = {"file_pipeline", "direct_kafka", "direct_postgres"}
_ALLOWED_SOURCE_TYPES = {"s3_batch", "api_pull", "cdc", "event"}
_ALLOWED_WRITE_MODES = {"upsert", "append", "replace"}
_ALLOWED_DQ_SECTIONS = {"hard_blocks", "soft_warns"}


def _qualified(cfg: Mapping[str, Any]) -> str:
    return f"{cfg.get('domain', '<no-domain>')}/{cfg.get('dataset', '<no-dataset>')}"


def _validate_dq_rules_shape(name: str, cfg: Mapping[str, Any]) -> None:
    dq_rules = cfg.get("dq_rules") or {}
    if not isinstance(dq_rules, Mapping):
        raise DatasetConfigError(f"{name}: dq_rules must be a mapping")

    legacy_sections = {"hard", "soft"} & set(dq_rules)
    if legacy_sections:
        raise DatasetConfigError(
            f"{name}: dq_rules uses legacy section(s) "
            f"{sorted(legacy_sections)}; use hard_blocks/soft_warns"
        )

    unknown_sections = set(dq_rules) - _ALLOWED_DQ_SECTIONS
    if unknown_sections:
        raise DatasetConfigError(
            f"{name}: dq_rules has unknown section(s) "
            f"{sorted(unknown_sections)}; expected hard_blocks/soft_warns"
        )

    for section in _ALLOWED_DQ_SECTIONS:
        rules = dq_rules.get(section, [])
        if not isinstance(rules, list):
            raise DatasetConfigError(f"{name}: dq_rules.{section} must be a list")
        for rule in rules:
            if not isinstance(rule, Mapping):
                raise DatasetConfigError(
                    f"{name}: dq_rules.{section} entries must be mappings"
                )
            if "column" in rule or "min_pct" in rule:
                raise DatasetConfigError(
                    f"{name}: dq_rules.{section} uses legacy keys "
                    "'column'/'min_pct'; use runtime keys 'field' or "
                    "'fields'/'threshold'"
                )


def validate_dataset_config(cfg: Mapping[str, Any]) -> None:
    """Validate ``cfg`` (parsed YAML) against the dataset_config contract.

    Raises :class:`DatasetConfigError` with a single, operator-readable
    message identifying the dataset and the broken rule. Does not log
    or print; callers decide how to surface failures.
    """
    if not isinstance(cfg, Mapping):
        raise DatasetConfigError(
            f"dataset_config must be a mapping, got {type(cfg).__name__}"
        )

    domain = cfg.get("domain")
    dataset = cfg.get("dataset")
    if not domain or not dataset:
        raise DatasetConfigError(
            "dataset_config requires non-empty 'domain' and 'dataset'"
        )

    name = _qualified(cfg)
    source_type = cfg.get("source_type", "s3_batch")
    delivery = cfg.get("delivery", "file_pipeline")
    is_canonical = bool(cfg.get("is_canonical", True))
    write_mode = cfg.get("write_mode", "upsert")
    key_fields = cfg.get("key_fields") or []
    target_topic = cfg.get("target_topic")
    canonical_topic = cfg.get("canonical_topic")
    transform_yaml_path = cfg.get("transform_yaml_path")
    filename_pattern = cfg.get("filename_pattern")

    # Enum domains.
    if source_type not in _ALLOWED_SOURCE_TYPES:
        raise DatasetConfigError(
            f"{name}: unknown source_type={source_type!r}; "
            f"expected one of {sorted(_ALLOWED_SOURCE_TYPES)}"
        )
    if delivery not in _ALLOWED_DELIVERIES:
        raise DatasetConfigError(
            f"{name}: unknown delivery={delivery!r}; "
            f"expected one of {sorted(_ALLOWED_DELIVERIES)}"
        )
    if write_mode not in _ALLOWED_WRITE_MODES:
        raise DatasetConfigError(
            f"{name}: unknown write_mode={write_mode!r}; "
            f"expected one of {sorted(_ALLOWED_WRITE_MODES)}"
        )

    # Source-type / file-pattern coherence.
    if source_type == "s3_batch" and not filename_pattern:
        raise DatasetConfigError(
            f"{name}: source_type='s3_batch' requires filename_pattern"
        )
    if source_type != "s3_batch" and filename_pattern:
        raise DatasetConfigError(
            f"{name}: filename_pattern only applies to source_type='s3_batch'"
        )

    # Delivery-specific topic requirements.
    if delivery in {"file_pipeline", "direct_kafka"} and not target_topic:
        raise DatasetConfigError(
            f"{name}: delivery={delivery!r} requires target_topic"
        )
    if delivery == "direct_postgres" and target_topic:
        raise DatasetConfigError(
            f"{name}: delivery='direct_postgres' must not set target_topic "
            f"(no Kafka leg). Got target_topic={target_topic!r}"
        )
    if delivery == "direct_postgres" and canonical_topic:
        raise DatasetConfigError(
            f"{name}: delivery='direct_postgres' must not set canonical_topic"
        )

    # Canonicalize â†’ transform_yaml_path requirement, except direct_postgres
    # which may run inline canonicalize OR pre-canonicalized curated.
    if not is_canonical and not transform_yaml_path:
        raise DatasetConfigError(
            f"{name}: is_canonical=false requires transform_yaml_path"
        )

    # Write-mode / key-field coherence.
    if write_mode == "upsert" and not key_fields:
        raise DatasetConfigError(
            f"{name}: write_mode='upsert' requires non-empty key_fields"
        )
    if write_mode == "replace" and not key_fields:
        raise DatasetConfigError(
            f"{name}: write_mode='replace' requires non-empty key_fields "
            f"(slot identity)"
        )

    # api_pull source block â€” runtime requirements that the runner / poller
    # would otherwise discover only when called.
    # Probe-string overlap with self is allowed (one filename matches
    # only the dataset that owns it); cross-dataset overlap is checked
    # by ``check_no_filename_pattern_overlap`` when peers are known.

    _validate_dq_rules_shape(name, cfg)

    if source_type == "api_pull":
        source = cfg.get("source") or {}
        if not isinstance(source, Mapping):
            raise DatasetConfigError(
                f"{name}: source_type='api_pull' requires a 'source' mapping"
            )
        if not source.get("url"):
            raise DatasetConfigError(
                f"{name}: source_type='api_pull' requires source.url"
            )
        cursor = source.get("cursor") or {}
        if not cursor.get("style"):
            raise DatasetConfigError(
                f"{name}: source_type='api_pull' requires source.cursor.style"
            )
        auth = source.get("auth") or {}
        auth_type = auth.get("type", "none")
        if auth_type not in {"none", "bearer", "basic", "mtls"}:
            raise DatasetConfigError(
                f"{name}: unknown source.auth.type={auth_type!r}"
            )
        if auth_type == "bearer" and not auth.get("secret_ref"):
            raise DatasetConfigError(
                f"{name}: source.auth.type='bearer' requires secret_ref "
                f"(env var name; never the token itself)"
            )
        # Defence-in-depth: never persist literal token strings even if a
        # YAML accidentally inlines one.
        for forbidden in ("token", "password", "client_secret", "api_key"):
            if forbidden in auth:
                raise DatasetConfigError(
                    f"{name}: source.auth must not contain {forbidden!r} "
                    f"directly; use secret_ref instead"
                )


# ---------------------------------------------------------------------------
# Cross-dataset overlap (called by yaml_loader before INSERT)
# ---------------------------------------------------------------------------


# A handful of probe filenames covering the formats this codebase ingests.
# The overlap check matches each peer's ``filename_pattern`` against probes
# generated from the candidate's pattern; if any probe matches BOTH, the
# regexes share input and `dag_drop_to_raw` would register the same SFTP
# file under two distinct ``(domain, dataset)`` rows.
_PROBE_FILENAMES: tuple[str, ...] = (
    # Common shapes produced by the patterns in patterns/insurance/.
    "policies_20260501.csv",
    "risk_20260501.csv",
    "events_20260501.csv",
    "country_codes_20260501.csv",
    "claims_20260501.csv",
)


def _generate_probes(pattern: str) -> list[str]:
    """Synthesise sample filenames a regex would accept.

    Pure heuristic â€” substitutes named groups with sensible defaults
    and returns the literal-stripped form. Used only for cross-dataset
    overlap detection so a precise regex parser isn't required.
    """
    if not pattern:
        return []
    probes: list[str] = []
    # Replace named-group YYYYMMDD captures with a known date.
    candidate = re.sub(
        r"\(\?P<bd>[^)]+\)",
        "20260501",
        pattern,
    )
    # Replace any other named group with a token.
    candidate = re.sub(r"\(\?P<[^>]+>[^)]+\)", "x", candidate)
    # Strip anchors + escapes.
    candidate = candidate.replace("^", "").replace("$", "").replace("\\.", ".")
    candidate = candidate.replace("\\d{8}", "20260501")
    candidate = candidate.replace(r"\d{8}", "20260501")
    probes.append(candidate)
    probes.extend(_PROBE_FILENAMES)
    return probes


def check_no_filename_pattern_overlap(
    cfg: Mapping[str, Any],
    peers: Iterable[Mapping[str, Any]],
) -> None:
    """Reject the candidate if its ``filename_pattern`` matches a sample
    that another active dataset's pattern ALSO matches (excluding
    same ``(domain, dataset)``).

    Why: ``dag_drop_to_raw`` walks the SFTP listing and, for each file,
    iterates every active ``s3_batch`` dataset's regex. Two regexes
    that accept the same filename â†’ same physical byte stream
    registered as two distinct ``file_catalogue`` rows under two
    distinct deliveries. T2 recon then double-counts and the file
    fans into both downstream DAGs. The protection is regex
    disjointness across active s3_batch datasets.

    Heuristic only: synthesises probe filenames from each pattern and
    runs every other pattern against them. False positives possible
    on heavily-anchored regexes, false negatives possible on patterns
    that accept only filenames not in the probe set. For production-
    grade isolation, use distinct SFTP folders per delivery.
    """
    if cfg.get("source_type", "s3_batch") != "s3_batch":
        return
    pattern = cfg.get("filename_pattern")
    if not pattern:
        return

    name_self = (cfg.get("domain"), cfg.get("dataset"))
    self_delivery = cfg.get("delivery", "file_pipeline")
    self_probes = _generate_probes(pattern)
    if not self_probes:
        return
    self_re = re.compile(pattern)

    for peer in peers:
        if peer.get("source_type", "s3_batch") != "s3_batch":
            continue
        peer_pattern = peer.get("filename_pattern")
        if not peer_pattern:
            continue
        if (peer.get("domain"), peer.get("dataset")) == name_self:
            continue
        # Two datasets sharing the SAME delivery route are an accepted
        # dual-write pattern: e.g. policies (current) + policies_history
        # both read the same file and write to different downstream
        # tables via different Kafka topics. The reviewer concern is
        # about CROSS-route collisions (file_pipeline + direct_postgres)
        # where the file would be ingested through two divergent
        # pipelines. Skip same-delivery peers.
        peer_delivery = peer.get("delivery", "file_pipeline")
        if peer_delivery == self_delivery:
            continue
        try:
            peer_re = re.compile(peer_pattern)
        except re.error:
            continue
        # Generate probes from BOTH patterns so we don't miss collisions
        # in either direction.
        probes = list(set(self_probes + _generate_probes(peer_pattern)))
        for probe in probes:
            if self_re.match(probe) and peer_re.match(probe):
                raise DatasetConfigError(
                    f"{name_self[0]}/{name_self[1]}: filename_pattern "
                    f"{pattern!r} overlaps with peer "
                    f"{peer.get('domain')}/{peer.get('dataset')!r} "
                    f"pattern {peer_pattern!r} (both accept {probe!r}). "
                    f"dag_drop_to_raw would register the same SFTP file "
                    f"under two distinct deliveries. Disambiguate the "
                    f"regexes (anchors, separator chars) or split SFTP "
                    f"folders per dataset."
                )
```

### `ods_pipeline\dlq.py`

```python
"""DLQ helpers shared by file, canonicalize, and message/API flows."""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

from ods_pipeline import metadata

_log = logging.getLogger(__name__)


def s3_prefix(
    *,
    env: str,
    domain: str,
    dataset: str,
    stage: str,
    run_id: str,
    business_date: str | None = None,
) -> str:
    """Return the standard S3 prefix for failed records/events."""
    date_part = business_date or "unknown"
    return (
        f"s3://ods-dlq-{env}/{domain}/{dataset}/{stage}/"
        f"date={date_part}/run_id={run_id}/"
    )


def envelope(
    *,
    payload: Mapping[str, Any],
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    error_type: str,
    error_message: str,
    stage: str,
    source_metadata: Mapping[str, Any] | None = None,
    source_message_id: str | None = None,
    source_event_id: str | None = None,
    source_request_id: str | None = None,
    source_batch_id: str | None = None,
    file_id: str | None = None,
    business_date: str | None = None,
    replayable: bool = True,
) -> dict[str, Any]:
    """Build a JSON/JSONL-friendly DLQ envelope."""
    source_metadata = dict(source_metadata or {})
    correlation = {
        "_ods_source_message_id": source_message_id or source_metadata.get("_ods_source_message_id"),
        "_ods_source_event_id": source_event_id or source_metadata.get("_ods_source_event_id"),
        "_ods_source_request_id": source_request_id or source_metadata.get("_ods_source_request_id"),
        "_ods_source_batch_id": source_batch_id or source_metadata.get("_ods_source_batch_id"),
    }
    result = {
        **{k: v for k, v in correlation.items() if v},
        "_ods_file_id": file_id or source_metadata.get("_ods_file_id"),
        "_ods_run_id": run_id,
        "_ods_domain": domain,
        "_ods_dataset": dataset,
        "_ods_business_date": business_date or source_metadata.get("_ods_business_date"),
        "_ods_source_application": source_application,
        "_ods_failed_stage": stage,
        "_ods_error_type": error_type,
        "_ods_error_message": error_message,
        "_ods_replayable": replayable,
        "payload": dict(payload),
    }
    if not result.get("_ods_file_id"):
        metadata.require_message_correlation(result, context="DLQ envelope")
    metadata.require_fields(
        result,
        (
            "_ods_run_id",
            "_ods_domain",
            "_ods_dataset",
            "_ods_source_application",
            "_ods_failed_stage",
            "_ods_error_type",
            "_ods_error_message",
            "payload",
        ),
        context="DLQ envelope",
    )
    return {key: value for key, value in result.items() if value is not None}


class DlqWriter:
    """Persist DLQ envelopes to S3 with bounded exponential-backoff retry.

    The writer is intentionally small: callers (Glue jobs, the message/API
    service, replay tooling) hand it a fully-formed envelope built via
    :func:`envelope` and a ``stage`` (matching the prefix layout from
    :func:`s3_prefix`). It computes the canonical key, JSON-encodes, and
    PUTs to S3, retrying transient errors with exp backoff (1s, 2s, 4s, 8s,
    16s by default; configurable). Permanent errors raise immediately.

    The S3 key is::

        s3://<dlq-bucket>/<domain>/<dataset>/<stage>/date=<bd>/run_id=<rid>/<attempt>.json

    where ``attempt`` is the per-write counter the caller supplies, so multiple
    failed records from the same run land at distinct keys.
    """

    DEFAULT_BACKOFF = (1.0, 2.0, 4.0, 8.0, 16.0)

    def __init__(
        self,
        s3_client,
        *,
        env: str = "local",
        backoff: tuple[float, ...] = DEFAULT_BACKOFF,
        sleep=time.sleep,
    ) -> None:
        self._s3 = s3_client
        self._env = env
        self._backoff = backoff
        self._sleep = sleep

    def write(
        self,
        envelope_dict: Mapping[str, Any],
        *,
        stage: str,
        attempt: int,
    ) -> str:
        """Write ``envelope_dict`` to S3 and return the resulting ``s3://`` URI.

        Raises the last underlying exception if every retry fails. Caller is
        expected to surface the failure to the run ledger; we do not write
        run_log here to keep the writer side-effect-isolated.
        """
        prefix = s3_prefix(
            env=self._env,
            domain=str(envelope_dict["_ods_domain"]),
            dataset=str(envelope_dict["_ods_dataset"]),
            stage=stage,
            run_id=str(envelope_dict["_ods_run_id"]),
            business_date=envelope_dict.get("_ods_business_date"),
        )
        bucket, key = _split_s3_uri(f"{prefix}{attempt}.json")
        body = json.dumps(envelope_dict, default=str).encode("utf-8")

        last_exc: Exception | None = None
        for attempt_idx in range(len(self._backoff) + 1):
            try:
                self._s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                )
                return f"s3://{bucket}/{key}"
            except Exception as exc:  # noqa: BLE001 â€” retry intentionally broad
                last_exc = exc
                if attempt_idx >= len(self._backoff):
                    break
                delay = self._backoff[attempt_idx]
                _log.warning(
                    "DlqWriter put_object failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt_idx + 1,
                    len(self._backoff) + 1,
                    delay,
                    exc,
                )
                self._sleep(delay)
        assert last_exc is not None  # for type checkers
        raise last_exc


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"not an s3 URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def replay_request(
    *,
    dlq_uri: str,
    run_id: str,
    domain: str,
    dataset: str,
    target_stage: str,
    reason: str,
) -> dict[str, Any]:
    """Return a small structured object operators can pass to replay tooling."""
    return {
        "dlq_uri": dlq_uri,
        "source_run_id": run_id,
        "domain": domain,
        "dataset": dataset,
        "target_stage": target_stage,
        "reason": reason,
    }
```

### `ods_pipeline\events.py`

```python
"""Publish pipeline run lifecycle events to ods.pipeline.run-events (Kafka + Postgres)."""
from __future__ import annotations

import datetime
import json
import os
import sys

import ods_ingestion_control as control
from ods_pipeline._db import build_dsn

TOPIC = "ods.pipeline.run-events"
_SUBJECT = f"{TOPIC}-value"

SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "RunEvent",
    "namespace": "com.aviva.ods.pipeline",
    "fields": [
        {"name": "run_id",                 "type": "string"},
        {"name": "event_type",             "type": "string"},
        {"name": "pipeline_type",          "type": ["null", "string"],  "default": None},
        {"name": "domain",                 "type": "string"},
        {"name": "dataset",                "type": "string"},
        {"name": "business_date",          "type": "string"},
        {"name": "status",                 "type": "string"},
        {"name": "record_count_source",    "type": ["null", "int"],     "default": None},
        {"name": "record_count_dq_pass",   "type": ["null", "int"],     "default": None},
        {"name": "record_count_dq_fail",   "type": ["null", "int"],     "default": None},
        {"name": "record_count_published", "type": ["null", "int"],     "default": None},
        {"name": "kafka_topic",            "type": ["null", "string"],  "default": None},
        {"name": "kafka_offset_end",       "type": ["null", "long"],    "default": None},
        {"name": "error_summary",          "type": ["null", "string"],  "default": None},
        {"name": "occurred_at",            "type": "string"},
        {"name": "file_id",                "type": ["null", "string"],  "default": None},
        {"name": "s3_raw_path",            "type": ["null", "string"],  "default": None},
        {"name": "s3_curated_path",        "type": ["null", "string"],  "default": None},
        {"name": "file_md5",               "type": ["null", "string"],  "default": None},
        {"name": "kafka_offset_start",     "type": ["null", "long"],    "default": None},
        {"name": "stages",                 "type": ["null", "string"],  "default": None},
    ],
})


def produce(
    event_type: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    status: str,
    *,
    pipeline_type: str | None = None,
    record_count_source: int | None = None,
    record_count_dq_pass: int | None = None,
    record_count_dq_fail: int | None = None,
    record_count_published: int | None = None,
    kafka_topic: str | None = None,
    kafka_offset_end: int | None = None,
    error_summary: str | None = None,
    file_id: str | None = None,
    s3_raw_path: str | None = None,
    s3_curated_path: str | None = None,
    file_md5: str | None = None,
    kafka_offset_start: int | None = None,
    stages: str | None = None,
) -> None:
    """Emit a run lifecycle event to Postgres (always) and Kafka (best-effort)."""
    payload = {
        "run_id":                 str(run_id),
        "event_type":             event_type,
        "pipeline_type":          pipeline_type,
        "domain":                 domain,
        "dataset":                dataset,
        "business_date":          str(business_date) if business_date else "",
        "status":                 status,
        "record_count_source":    int(record_count_source)    if record_count_source    is not None else None,
        "record_count_dq_pass":   int(record_count_dq_pass)   if record_count_dq_pass   is not None else None,
        "record_count_dq_fail":   int(record_count_dq_fail)   if record_count_dq_fail   is not None else None,
        "record_count_published": int(record_count_published) if record_count_published is not None else None,
        "kafka_topic":            kafka_topic,
        "kafka_offset_end":       int(kafka_offset_end)       if kafka_offset_end       is not None else None,
        "error_summary":          error_summary,
        "occurred_at":            datetime.datetime.utcnow().isoformat(),
        "file_id":                file_id,
        "s3_raw_path":            s3_raw_path,
        "s3_curated_path":        s3_curated_path,
        "file_md5":               file_md5,
        "kafka_offset_start":     int(kafka_offset_start)     if kafka_offset_start     is not None else None,
        "stages":                 stages,
    }

    # Always write to Postgres regardless of Kafka outcome
    _write_pg(payload)

    # Kafka publish is best-effort â€” failure must never block the pipeline
    try:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        sr_url   = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
        bootstrap = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            os.environ.get("KAFKA_BOOTSTRAP", "broker:29092"),
        )
        sr = SchemaRegistryClient({"url": sr_url})
        serializer = AvroSerializer(sr, SCHEMA_STR)
        producer   = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
        producer.produce(
            topic=TOPIC,
            key=run_id.encode(),
            value=serializer(payload, SerializationContext(TOPIC, MessageField.VALUE)),
        )
        producer.flush()
    except Exception as exc:
        print(f"[ods_pipeline.events] WARNING: Kafka publish failed: {exc}", file=sys.stderr)


def _write_pg(payload: dict) -> None:
    """Write *payload* to ``pipeline.run_events``.  Non-fatal on error."""
    try:
        dsn = build_dsn()
    except ValueError:
        return  # No Postgres configured â€” skip silently

    try:
        import psycopg2
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            # Auto-fetch stage details from run_stage_log if not supplied
            stages_json = payload.get("stages")
            if stages_json is None:
                try:
                    cur.execute(
                        """
                        SELECT stage, status, started_at, ended_at,
                               input_ref, output_ref,
                               record_count_in, record_count_out,
                               metrics, error
                          FROM pipeline.run_stage_log
                         WHERE run_id = %s
                         ORDER BY started_at
                        """,
                        (payload["run_id"],),
                    )
                    rows = cur.fetchall()
                    if rows:
                        cols = [
                            "stage", "status", "started_at", "ended_at",
                            "input_ref", "output_ref",
                            "record_count_in", "record_count_out",
                            "metrics", "error",
                        ]
                        stages_json = json.dumps([
                            {
                                c: (v.isoformat() if hasattr(v, "isoformat") else v)
                                for c, v in zip(cols, row)
                            }
                            for row in rows
                        ])
                except Exception:
                    pass  # stages remain None â€” non-fatal

            control.record_run_event(
                conn,
                run_id=payload["run_id"],
                event_type=payload["event_type"],
                pipeline_type=payload["pipeline_type"],
                domain=payload["domain"],
                dataset=payload["dataset"],
                business_date=payload["business_date"],
                status=payload["status"],
                record_count_source=payload["record_count_source"],
                record_count_dq_pass=payload["record_count_dq_pass"],
                record_count_dq_fail=payload["record_count_dq_fail"],
                record_count_published=payload["record_count_published"],
                kafka_topic=payload["kafka_topic"],
                kafka_offset_end=payload["kafka_offset_end"],
                error_summary=payload["error_summary"],
                occurred_at=payload["occurred_at"],
                file_id=payload.get("file_id"),
                s3_raw_path=payload.get("s3_raw_path"),
                s3_curated_path=payload.get("s3_curated_path"),
                file_md5=payload.get("file_md5"),
                kafka_offset_start=payload.get("kafka_offset_start"),
                stages=stages_json,
                commit=False,
            )
    except Exception as exc:
        print(f"[ods_pipeline.events] WARNING: PG write failed: {exc}", file=sys.stderr)
```

### `ods_pipeline\files.py`

```python
"""pipeline.file_catalogue and pipeline.file_state operations."""
from __future__ import annotations

import ods_ingestion_control as control


def upsert(
    conn,
    *,
    file_id: str | None = None,
    domain: str,
    dataset: str,
    business_date: str,
    file_md5: str,
    s3_raw_path: str | None = None,
    sftp_path: str | None = None,
    s3_curated_path: str | None = None,
    file_size_bytes: int | None = None,
    source_row_count: int | None = None,
    state: str = "ingested",
    last_run_id: str | None = None,
) -> str:
    """Upsert ``file_catalogue`` keyed on ``(domain, dataset, s3_raw_path)``.

    Returns the canonical ``file_id`` UUID string for this landed raw file.
    ``file_md5`` remains a content fingerprint; it is not the identity because
    different files can legitimately have identical content.
    """
    return control.register_file(
        conn,
        domain=domain,
        dataset=dataset,
        business_date=str(business_date),
        file_md5=file_md5,
        s3_raw_path=s3_raw_path,
        sftp_path=sftp_path,
        s3_curated_path=s3_curated_path,
        file_size_bytes=file_size_bytes,
        source_row_count=source_row_count,
        state=state,
        last_run_id=last_run_id,
        file_id=file_id,
    )


def update_catalogue(conn, file_id: str, **fields) -> None:
    """Update arbitrary columns on a ``file_catalogue`` row by ``file_id``."""
    if not fields:
        return
    allowed = {
        "state",
        "s3_curated_path",
        "source_row_count",
        "last_run_id",
    }
    invalid = set(fields) - allowed
    if invalid:
        raise ValueError(f"Unknown file_catalogue fields: {sorted(invalid)}")
    control.update_file_catalogue(conn, file_id=file_id, **fields)


def set_state(
    conn,
    s3_path: str,
    run_id: str,
    status: str,
    *,
    record_count: int | None = None,
    error_reason: str | None = None,
) -> None:
    """Upsert ``pipeline.file_state`` for *s3_path*."""
    control.set_file_state(
        conn,
        s3_path=s3_path,
        run_id=run_id,
        status=status,
        record_count=record_count,
        error_reason=error_reason,
    )


def get_state(conn, s3_path: str) -> str | None:
    """Return the current status string for *s3_path*, or ``None`` if not found."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None
```

### `ods_pipeline\ingest\__init__.py`

```python
"""ODS ingest entry-points outside the Glue Spark jobs.

Currently exposes the api_pull poller, used by dag_api_pull to fetch a
batch of records from an external HTTP API and archive them to S3.
"""
```

### `ods_pipeline\ingest\api_pull\__init__.py`

```python
"""API pull poller â€” fetch external HTTP API on a schedule and archive
records as gzipped JSONL on S3.

Public surface kept small on purpose; dag_api_pull only needs:

    poll_and_archive(...)         the one-shot batch poller
    ArchivedBatch                 the typed return value
    AuthProvider, BearerAuth      auth Protocol + bearer impl
    Cursor, build_cursor          cursor Protocol + factory
    WatermarkStore                pending/committed cursor store

Each strategy lives in its own module so adding etag/offset/full_replace
later is one new file, not a refactor.
"""
from __future__ import annotations

from ods_pipeline.ingest.api_pull.archive import ArchivedBatch, write_jsonl_archive
from ods_pipeline.ingest.api_pull.auth import AuthProvider, BearerAuth, build_auth
from ods_pipeline.ingest.api_pull.cursors import Cursor, build_cursor
from ods_pipeline.ingest.api_pull.linkage import (
    TRIGGERED_BY_API_PULL_EDGE,
    derive_dag_ingest_parent_run_id,
    ingest_status_for_api_pull_run,
)
from ods_pipeline.ingest.api_pull.poller import poll_and_archive
from ods_pipeline.ingest.api_pull.watermark import WatermarkStore

__all__ = [
    "ArchivedBatch",
    "AuthProvider",
    "BearerAuth",
    "Cursor",
    "TRIGGERED_BY_API_PULL_EDGE",
    "WatermarkStore",
    "build_auth",
    "build_cursor",
    "derive_dag_ingest_parent_run_id",
    "ingest_status_for_api_pull_run",
    "poll_and_archive",
    "write_jsonl_archive",
]
```

### `ods_pipeline\ingest\api_pull\archive.py`

```python
"""Gzipped JSONL archive writer for api_pull.

Each fetched record is wrapped in the standard ODS metadata envelope
(_ods_run_id, _ods_source_request_id, _ods_source_application, ...)
before being written to S3 as one line of gzipped JSONL. The downstream
Glue ingestion job reads JSONL via spark.read.json and applies the same
schema validation and DQ pipeline as the file pattern.

Identity: the archive is keyed by (domain, dataset, business_date,
run_id) â€” one immutable object per poll. Replay is safe because the
file_catalogue upsert is keyed on s3_raw_path.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ods_pipeline import metadata as _metadata


@dataclass
class ArchivedBatch:
    """Outcome of a single api_pull poll.

    ``no_changes=True`` means the source returned 0 records (or 304); the
    DAG should mark the run skipped, not trigger dag_ingest, and leave
    the watermark unchanged.
    """

    domain: str
    dataset: str
    business_date: str
    source_application: str
    s3_uri: str
    s3_bucket: str
    s3_key: str
    file_md5: str
    file_size_bytes: int
    record_count: int
    page_count: int
    old_cursor_value: str | None
    new_cursor_value: str | None
    source_request_id: str
    no_changes: bool = False


def _envelope(
    *,
    record: Mapping[str, Any],
    run_id: str,
    source_application: str,
    domain: str,
    dataset: str,
    business_date: str,
    source_request_id: str,
    cursor_value: str | None,
    archive_uri: str,
    schema_id: str,
    schema_version: int | str,
) -> dict[str, Any]:
    """One JSONL line: ODS metadata envelope wrapping the source record."""
    meta = _metadata.message_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        source_request_id=source_request_id,
    )
    envelope = _metadata.archive_envelope(
        payload=dict(record),
        metadata=meta,
        schema_id=schema_id,
        schema_version=schema_version,
        archive_s3_uri=archive_uri,
    )
    envelope["_ods_business_date"] = business_date
    envelope["_ods_source_cursor"] = cursor_value
    return envelope


def _gzip_jsonl(lines: Sequence[Mapping[str, Any]]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        for line in lines:
            gz.write(json.dumps(line, default=str, sort_keys=True).encode("utf-8"))
            gz.write(b"\n")
    return buffer.getvalue()


def write_jsonl_archive(
    *,
    s3_client,
    bucket: str,
    records: Sequence[Mapping[str, Any]],
    domain: str,
    dataset: str,
    business_date: str,
    run_id: str,
    source_application: str,
    source_request_id: str,
    cursor_value: str | None,
    schema_id: str,
    schema_version: int | str = 1,
    page_count: int = 1,
    old_cursor_value: str | None = None,
    new_cursor_value: str | None = None,
) -> ArchivedBatch:
    """Wrap ``records`` in ODS envelopes, gzip as JSONL, and put to S3.

    Returns ArchivedBatch with file_md5, size and S3 location so the caller
    can register the archive in pipeline.file_catalogue and write lineage.
    """
    s3_key = (
        f"api_pull/{domain}/{dataset}/"
        f"date={business_date}/run_id={run_id}.jsonl.gz"
    )
    s3_uri = f"s3://{bucket}/{s3_key}"

    if not records:
        return ArchivedBatch(
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_application=source_application,
            s3_uri=s3_uri,
            s3_bucket=bucket,
            s3_key=s3_key,
            file_md5="",
            file_size_bytes=0,
            record_count=0,
            page_count=page_count,
            old_cursor_value=old_cursor_value,
            new_cursor_value=new_cursor_value,
            source_request_id=source_request_id,
            no_changes=True,
        )

    enveloped = [
        _envelope(
            record=record,
            run_id=run_id,
            source_application=source_application,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_request_id=source_request_id,
            cursor_value=cursor_value,
            archive_uri=s3_uri,
            schema_id=schema_id,
            schema_version=schema_version,
        )
        for record in records
    ]
    body = _gzip_jsonl(enveloped)
    file_md5 = hashlib.md5(body).hexdigest()
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/x-jsonlines",
        ContentEncoding="gzip",
    )
    return ArchivedBatch(
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_application=source_application,
        s3_uri=s3_uri,
        s3_bucket=bucket,
        s3_key=s3_key,
        file_md5=file_md5,
        file_size_bytes=len(body),
        record_count=len(records),
        page_count=page_count,
        old_cursor_value=old_cursor_value,
        new_cursor_value=new_cursor_value,
        source_request_id=source_request_id,
        no_changes=False,
    )
```

### `ods_pipeline\ingest\api_pull\auth.py`

```python
"""HTTP auth providers for the api_pull poller.

A provider is anything that mutates a ``requests.Session`` so subsequent
requests carry the correct credentials. Bearer is implemented for slice 1;
``none`` is allowed for stub/test sources. ``basic``, ``mtls`` and OAuth
plug into the same Protocol â€” adding one is one new class + one
``build_auth`` branch.

Secret material is never read from ``source_config``. It is looked up by
``secret_ref`` from the environment (Airflow Secrets Backend mounts as
env vars). This keeps tokens out of the dataset_config table.
"""
from __future__ import annotations

import os
from typing import Mapping, Protocol

import requests


class AuthProvider(Protocol):
    """Anything that knows how to attach credentials to a Session."""

    type: str

    def apply(self, session: requests.Session) -> None: ...


class NoAuth:
    """No-op auth â€” used by tests and public stub APIs."""

    type = "none"

    def apply(self, session: requests.Session) -> None:  # noqa: D401 â€” Protocol impl
        return None


class BearerAuth:
    """``Authorization: Bearer <token>`` from a named env var.

    The env var name comes from ``secret_ref`` in source_config. Missing or
    blank token raises immediately so we fail fast on misconfig rather
    than emit unauthenticated requests.
    """

    type = "bearer"

    def __init__(self, secret_ref: str, *, env: Mapping[str, str] | None = None):
        if not secret_ref:
            raise ValueError("bearer auth requires a non-empty secret_ref")
        env = env if env is not None else os.environ
        token = env.get(secret_ref)
        if not token:
            raise ValueError(
                f"bearer auth secret_ref={secret_ref!r} not set in environment"
            )
        self._token = token

    def apply(self, session: requests.Session) -> None:
        session.headers["Authorization"] = f"Bearer {self._token}"


def build_auth(
    auth_config: Mapping[str, object] | None,
    *,
    env: Mapping[str, str] | None = None,
) -> AuthProvider:
    """Construct an AuthProvider from the ``source.auth`` block in YAML."""
    if not auth_config:
        return NoAuth()
    auth_type = str(auth_config.get("type", "none")).lower()
    if auth_type == "none":
        return NoAuth()
    if auth_type == "bearer":
        secret_ref = str(auth_config.get("secret_ref", ""))
        return BearerAuth(secret_ref, env=env)
    raise ValueError(
        f"unsupported auth.type={auth_type!r}; "
        f"slice 1 supports 'none' and 'bearer' only"
    )
```

### `ods_pipeline\ingest\api_pull\cursors.py`

```python
"""Cursor strategies for the api_pull poller.

A Cursor is the small piece of state that decides:
  - what request parameters carry the watermark forward,
  - how to walk pages within one poll,
  - what the next watermark should be after a successful poll.

Slice 1 implements ``since_timestamp`` only. ``etag``, ``offset`` and
``full_replace`` plug in by adding new classes and one ``build_cursor``
branch â€” the poller body never branches on style.

since_timestamp:
    request:    GET ?{request_param}={committed_or_initial}
    paging:     follow RFC 5988 Link rel=next; if absent stop after one page.
    watermark:  max({response_field}) over fetched records, ISO-8601 string.
                If the page is empty, the cursor is left unchanged so the
                next poll re-issues the same window.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence


@dataclass
class CursorRequest:
    """One HTTP request the poller should issue.

    The poller does not decide URLs â€” Cursors do. ``url`` may be an absolute
    next-page link (link_header paging) or the configured base URL with the
    cursor query string applied (first page).
    """

    url: str
    params: dict[str, str]


class Cursor(Protocol):
    """Strategy that drives one poll of one dataset.

    A Cursor is constructed once per poll with the committed watermark and
    is consumed by the poller via ``initial_request``, ``next_request``
    and ``advance``. It carries no I/O â€” pure state machine.
    """

    style: str

    def initial_request(self) -> CursorRequest: ...

    def next_request(
        self,
        last_response_headers: Mapping[str, str],
        last_response_body: Any,
    ) -> CursorRequest | None:
        """Return the next page request, or None if paging is exhausted."""

    def advance(self, all_records: Sequence[Mapping[str, Any]]) -> str | None:
        """Compute the new watermark value after a successful poll.

        ``None`` means leave the committed cursor unchanged (e.g. empty page,
        304 Not Modified, full_replace one-shot).
        """


class SinceTimestampCursor:
    """``?{request_param}={cursor}`` watermark with optional Link paging."""

    style = "since_timestamp"

    def __init__(
        self,
        *,
        base_url: str,
        request_param: str,
        response_field: str,
        committed_value: str | None,
        initial_value: str,
        page_style: str = "link_header",
        extra_params: Mapping[str, str] | None = None,
    ):
        if not request_param:
            raise ValueError("since_timestamp cursor requires request_param")
        if not response_field:
            raise ValueError("since_timestamp cursor requires response_field")
        if page_style not in {"link_header", "none"}:
            raise ValueError(
                f"since_timestamp cursor: page.style={page_style!r} not supported "
                f"in slice 1; expected 'link_header' or 'none'"
            )
        self._base_url = base_url
        self._request_param = request_param
        self._response_field = response_field
        self._cursor_value = committed_value or initial_value
        self._page_style = page_style
        self._extra_params = dict(extra_params or {})

    def initial_request(self) -> CursorRequest:
        params = {**self._extra_params, self._request_param: self._cursor_value}
        return CursorRequest(url=self._base_url, params=params)

    def next_request(
        self,
        last_response_headers: Mapping[str, str],
        last_response_body: Any,
    ) -> CursorRequest | None:
        if self._page_style != "link_header":
            return None
        link_header = last_response_headers.get("Link") or last_response_headers.get("link")
        if not link_header:
            return None
        next_url = _parse_next_link(link_header)
        if not next_url:
            return None
        # Subsequent pages carry the cursor in the Link URL itself; do not
        # re-apply request_param so we don't double-stamp the query string.
        return CursorRequest(url=next_url, params={})

    def advance(self, all_records: Sequence[Mapping[str, Any]]) -> str | None:
        if not all_records:
            return None
        max_value: str | None = None
        for record in all_records:
            value = record.get(self._response_field)
            if value is None:
                continue
            value_s = str(value)
            if max_value is None or value_s > max_value:
                max_value = value_s
        return max_value


def build_cursor(
    source: Mapping[str, Any],
    *,
    committed_value: str | None,
) -> Cursor:
    """Construct the Cursor for a dataset's ``source`` config block."""
    cursor_cfg = source.get("cursor") or {}
    style = str(cursor_cfg.get("style", "since_timestamp")).lower()
    if style == "since_timestamp":
        page_cfg = source.get("page") or {}
        return SinceTimestampCursor(
            base_url=str(source["url"]),
            request_param=str(cursor_cfg.get("request_param", "updated_since")),
            response_field=str(cursor_cfg.get("response_field", "updated_at")),
            committed_value=committed_value,
            initial_value=str(cursor_cfg.get("initial", "")),
            page_style=str(page_cfg.get("style", "link_header")),
        )
    raise ValueError(
        f"unsupported cursor.style={style!r}; "
        f"slice 1 supports 'since_timestamp' only"
    )


def _parse_next_link(link_header: str) -> str | None:
    """Pull the ``rel=\"next\"`` URL out of an RFC 5988 Link header.

    Format example::

        Link: <https://api.example/items?page=2>; rel="next", <...>; rel="prev"
    """
    for part in link_header.split(","):
        segments = [s.strip() for s in part.split(";") if s.strip()]
        if not segments:
            continue
        url_segment = segments[0]
        if not (url_segment.startswith("<") and url_segment.endswith(">")):
            continue
        url = url_segment[1:-1]
        for attr in segments[1:]:
            if attr.replace(" ", "").lower() in {'rel="next"', "rel=next"}:
                return url
    return None
```

### `ods_pipeline\ingest\api_pull\linkage.py`

```python
"""Run-linkage helpers for api_pull <-> dag_ingest.

The api_pull control-plane needs to look up the EXACT dag_ingest parent
run launched by a given api_pull poll, not "the latest s3_batch run for
this file_id" or "any run carrying our edge". Two scenarios drove the
two-layer match below:

  1. A replay of the same ``file_id`` with a different api_pull poll
     would match a "latest by file_id" lookup. Fixed by requiring the
     ``triggered_by_api_pull`` edge in ``run_log.parents``.
  2. TriggerDagRunOperator retries, manual re-triggers, or bugs could
     produce TWO rows that both carry the ``triggered_by_api_pull``
     edge for the same ``api_pull_run_id``. A "latest by edge" lookup
     could then promote/clear the wrong cursor. Fixed by requiring the
     caller (dag_api_pull) to pre-mint a deterministic
     ``expected_parent_run_id`` and matching it as the run_log primary
     key. PK uniqueness eliminates ambiguity by definition. The edge
     check stays as defence-in-depth.

If the caller cannot supply ``expected_parent_run_id`` (legacy / test
paths), the lookup falls back to JSONB containment but returns ``None``
when more than one row matches â€” "ambiguous, do not promote".

Lives outside the Airflow DAG module so plain pytest (no Airflow
runtime) can exercise the SQL contract.
"""
from __future__ import annotations

import json
import uuid

TRIGGERED_BY_API_PULL_EDGE = "triggered_by_api_pull"


def derive_dag_ingest_parent_run_id(api_pull_run_id: str) -> str:
    """Compute the deterministic ``run_id`` that dag_ingest.init_run will
    use as its parent run when triggered by this api_pull poll.

    Both ends derive the same value from the same api_pull_run_id so
    no extra column or coordination state is needed. UUID5 namespacing
    keeps it stable across processes / restarts and collision-free with
    randomly-minted UUIDs.
    """
    return str(
        uuid.uuid5(uuid.NAMESPACE_OID, f"api_pull:{api_pull_run_id}")
    )


def ingest_status_for_api_pull_run(
    conn,
    api_pull_run_id: str,
    *,
    expected_parent_run_id: str | None = None,
) -> str | None:
    """Return the status of the exact dag_ingest parent run launched by
    THIS api_pull poll, or ``None`` if no unambiguous match exists.

    Behaviour:

      * If ``expected_parent_run_id`` is supplied, look up by PK and
        verify the ``triggered_by_api_pull`` edge is present in
        ``run_log.parents``. Returns the status iff both match.
      * Otherwise, fall back to JSONB containment on the edge alone.
        If MORE THAN ONE row matches, return ``None`` â€” the lookup is
        ambiguous and the caller (finalise_watermark) MUST NOT promote
        or clear the watermark on an ambiguous result.
    """
    needle = json.dumps([{
        "run_id": api_pull_run_id,
        "edge_type": TRIGGERED_BY_API_PULL_EDGE,
    }])
    with conn.cursor() as cur:
        if expected_parent_run_id:
            # Exact-PK match. PK uniqueness in run_log makes the result
            # at most one row; the edge check rejects same-PK rows that
            # somehow lack the api_pull linkage.
            cur.execute(
                """
                SELECT status
                  FROM pipeline.run_log
                 WHERE run_id = %s::uuid
                   AND pipeline_type = 's3_batch'
                   AND parents @> %s::jsonb
                """,
                (expected_parent_run_id, needle),
            )
            row = cur.fetchone()
            return row[0] if row else None

        # Fallback: edge-only lookup. Pull at most 2 rows so we can
        # detect ambiguity without scanning the full table.
        cur.execute(
            """
            SELECT status
              FROM pipeline.run_log
             WHERE pipeline_type = 's3_batch'
               AND parents @> %s::jsonb
             ORDER BY started_at DESC NULLS LAST, run_id::text DESC
             LIMIT 2
            """,
            (needle,),
        )
        rows = cur.fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        return None  # ambiguous â€” caller must NOT promote/clear
    return rows[0][0]
```

### `ods_pipeline\ingest\api_pull\poller.py`

```python
"""api_pull HTTP poller.

Single public entry-point: :func:`poll_and_archive`. Builds an HTTP
session with the configured auth, walks pages via the configured cursor
strategy, accumulates all records into one logical batch, and writes
that batch to S3 as one immutable gzipped JSONL archive. Watermark
read/record_pending lifecycle is owned by the caller (dag_api_pull) so
locking and the post-trigger promote/clear_pending hooks live in one
place.
"""
from __future__ import annotations

import uuid
from typing import Any, Mapping, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ods_pipeline.ingest.api_pull.archive import ArchivedBatch, write_jsonl_archive
from ods_pipeline.ingest.api_pull.auth import AuthProvider, build_auth
from ods_pipeline.ingest.api_pull.cursors import Cursor, CursorRequest, build_cursor


def _build_session(
    *,
    auth: AuthProvider,
    timeout_seconds: float,
    retries: int,
) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers["Accept"] = "application/json"
    session.request = _with_default_timeout(session.request, timeout_seconds)  # type: ignore[assignment]
    auth.apply(session)
    return session


def _with_default_timeout(method, timeout_seconds: float):
    """Wrap Session.request so callers always get a default timeout."""

    def wrapper(*args, **kwargs):
        kwargs.setdefault("timeout", timeout_seconds)
        return method(*args, **kwargs)

    return wrapper


def _records_from_body(body: Any) -> Sequence[Mapping[str, Any]]:
    """Accept ``[{...}]`` or ``{"items": [...]}`` / ``{"data": [...]}``.

    Avoids hard-coding a wire shape; api_pull integrations vary. Anything
    else is treated as zero records â€” the caller decides how to surface it.
    """
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("items", "data", "records", "results"):
            value = body.get(key)
            if isinstance(value, list):
                return value
    return []


def poll_and_archive(
    *,
    dataset_config: Mapping[str, Any],
    s3_client,
    archive_bucket: str,
    committed_cursor_value: str | None,
    run_id: str,
    business_date: str,
    session: requests.Session | None = None,
    cursor: Cursor | None = None,
    env: Mapping[str, str] | None = None,
) -> ArchivedBatch:
    """Run one HTTP poll for a dataset and archive results to S3.

    Wiring:
      - ``dataset_config`` carries domain, dataset, schema_id and the
        ``source`` block (URL, auth, cursor, page, retries, timeout).
      - ``committed_cursor_value`` is the watermark to issue this poll
        from. ``None`` falls back to the cursor's configured ``initial``.
      - ``s3_client``, ``session`` and ``cursor`` are injectable so tests
        can drive the function without real network or S3.

    Returns ArchivedBatch. ``no_changes=True`` means the source had
    nothing new â€” the DAG should skip dag_ingest and leave the watermark
    unchanged.
    """
    domain = str(dataset_config["domain"])
    dataset = str(dataset_config["dataset"])
    source = dict(dataset_config.get("source") or {})
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    schema_id = str(dataset_config.get("schema_id", f"{domain}.{dataset}"))
    schema_version = dataset_config.get("schema_version", 1)
    timeout_seconds = float(source.get("timeout_seconds", 30))
    retries = int(source.get("retries", 3))

    if session is None:
        auth = build_auth(source.get("auth"), env=env)
        session = _build_session(
            auth=auth,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )

    if cursor is None:
        cursor = build_cursor(source, committed_value=committed_cursor_value)

    source_request_id = str(uuid.uuid4())
    all_records: list[Mapping[str, Any]] = []
    request: CursorRequest | None = cursor.initial_request()
    page_count = 0

    while request is not None:
        response = session.get(request.url, params=request.params or None)
        page_count += 1
        if response.status_code == 304:
            # Etag/no-change short-circuit. Slice 1 doesn't issue
            # If-None-Match yet, but we still honour 304 if a source emits it.
            break
        response.raise_for_status()
        body = response.json() if response.content else None
        records = _records_from_body(body)
        all_records.extend(records)
        request = cursor.next_request(response.headers, body)

    new_cursor_value = cursor.advance(all_records)

    return write_jsonl_archive(
        s3_client=s3_client,
        bucket=archive_bucket,
        records=all_records,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        run_id=run_id,
        source_application=source_application,
        source_request_id=source_request_id,
        cursor_value=committed_cursor_value,
        schema_id=schema_id,
        schema_version=schema_version,
        page_count=page_count,
        old_cursor_value=committed_cursor_value,
        new_cursor_value=new_cursor_value,
    )
```

### `ods_pipeline\ingest\api_pull\watermark.py`

```python
"""Two-phase watermark store for api_pull.

The api_pull control-plane separates ``pending`` and ``committed`` cursors:

  1. read_committed()        return the cursor to issue the next poll from
  2. lock(run_id)            advisory-style lock so concurrent DAG runs do
                             not double-poll the same dataset
  3. record_pending()        after S3 archive succeeds, stage the new cursor
                             alongside the run_id that owns it
  4. promote()               called by dag_api_pull's post-trigger sensor
                             once the downstream dag_ingest run succeeds
  5. clear_pending()         called when the downstream run failed or was
                             aborted; committed cursor is left untouched

The split guarantees no records are dropped if dag_ingest fails after the
poll succeeds: the same window is re-issued on the next schedule.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class WatermarkRow:
    domain: str
    dataset: str
    source_application: str
    cursor_type: str
    committed_cursor_value: str | None
    pending_cursor_value: str | None
    pending_run_id: str | None
    last_successful_run_id: str | None
    locked: bool


class WatermarkStore:
    """Thin DAO over ``pipeline.api_pull_watermark``.

    All methods take an open psycopg2 connection. Each call opens its own
    short-lived cursor and commits â€” these are control-plane writes that
    must not piggy-back on the caller's data transaction.
    """

    def __init__(self, conn):
        self._conn = conn

    # ------------------------------------------------------------------ read

    def read(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        cursor_type: str,
    ) -> WatermarkRow:
        """Return the current row, inserting a fresh one with NULL cursors
        if none exists for this (domain, dataset, source_application)."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.api_pull_watermark
                    (domain, dataset, source_application, cursor_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (domain, dataset, source_application) DO NOTHING
                """,
                (domain, dataset, source_application, cursor_type),
            )
            cur.execute(
                """
                SELECT cursor_type, committed_cursor_value, pending_cursor_value,
                       pending_run_id::text, last_successful_run_id::text,
                       locked_at IS NOT NULL
                  FROM pipeline.api_pull_watermark
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (domain, dataset, source_application),
            )
            row = cur.fetchone()
        self._conn.commit()
        existing_type, committed, pending, pending_run, last_run, locked = row
        return WatermarkRow(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            cursor_type=existing_type,
            committed_cursor_value=committed,
            pending_cursor_value=pending,
            pending_run_id=pending_run,
            last_successful_run_id=last_run,
            locked=bool(locked),
        )

    # ------------------------------------------------------------------ lock

    def try_lock(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
    ) -> bool:
        """Set ``locked_at=NOW()`` only if currently NULL.

        Returns True if the caller now holds the lock for this dataset.
        Caller must release via ``unlock`` even on failure.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET locked_at=NOW(),
                       pending_run_id=%s,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                   AND locked_at IS NULL
                """,
                (run_id, domain, dataset, source_application),
            )
            acquired = cur.rowcount == 1
        self._conn.commit()
        return acquired

    def unlock(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
    ) -> None:
        """Release the dataset lock. Pending cursor is left untouched â€”
        promote/clear_pending owns that lifecycle."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET locked_at=NULL,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (domain, dataset, source_application),
            )
        self._conn.commit()

    # --------------------------------------------------------------- pending

    def record_pending(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
        new_cursor_value: str,
    ) -> None:
        """Stage a new cursor against ``run_id``.

        Called only after the S3 archive write succeeded. promote() will
        move this value into ``committed_cursor_value`` once dag_ingest
        for ``run_id`` finishes successfully.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET pending_cursor_value=%s,
                       pending_run_id=%s,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (new_cursor_value, run_id, domain, dataset, source_application),
            )
        self._conn.commit()

    def promote(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
    ) -> bool:
        """Move pending -> committed iff ``run_id`` still owns the pending
        cursor. Returns True if a row was promoted."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET committed_cursor_value=pending_cursor_value,
                       last_successful_run_id=%s,
                       pending_cursor_value=NULL,
                       pending_run_id=NULL,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                   AND pending_run_id::text=%s
                """,
                (run_id, domain, dataset, source_application, run_id),
            )
            promoted = cur.rowcount == 1
        self._conn.commit()
        return promoted

    def clear_pending(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
    ) -> None:
        """Discard the pending cursor without touching committed.

        Called when dag_ingest failed for ``run_id``. The next poll re-issues
        the same window, so records are preserved.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET pending_cursor_value=NULL,
                       pending_run_id=NULL,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                   AND pending_run_id::text=%s
                """,
                (domain, dataset, source_application, run_id),
            )
        self._conn.commit()
```

### `ods_pipeline\ingest\api_pull_kafka\__init__.py`

```python
"""Direct API â†’ Kafka api_pull runner.

Companion to ods_pipeline.ingest.api_pull. The file-pipeline shape
polls and archives to S3 JSONL, then triggers dag_ingest. The
direct-Kafka shape polls and publishes per-record Avro to the raw
Kafka topic; a Kafka Connect S3 sink writes the archive in parallel
and a JDBC sink writes Postgres rows. Same control-plane primitives
(run_log, stages, lineage_edge, reconciliation_log,
api_pull_watermark) â€” different downstream shape.

Public entrypoints:
  * ``run_once`` performs one poll/publish cycle. The Airflow
    dispatcher in ``dag_api_pull`` invokes it per scheduled tick.
  * ``run_loop`` wraps ``run_once`` in a long-running, stop-event
    aware poller for sub-tick latency datasets. See
    ``dag_api_pull_kafka_continuous`` for the Airflow surface.
"""
from __future__ import annotations

from ods_pipeline.ingest.api_pull_kafka.loop import run_loop
from ods_pipeline.ingest.api_pull_kafka.runner import (
    PublishedBatch,
    build_avro_producer,
    build_envelope,
    run_once,
)

__all__ = [
    "PublishedBatch",
    "build_avro_producer",
    "build_envelope",
    "run_once",
    "run_loop",
]
```

### `ods_pipeline\ingest\api_pull_kafka\loop.py`

```python
"""Long-running runner mode for the direct-Kafka api_pull slice.

The scheduled DAG (``dag_api_pull``) calls :func:`run_once` once per
Airflow tick. For datasets where sub-tick latency matters, this module
wraps ``run_once`` in a tight loop that respects an external stop
signal (Airflow heartbeat, SIGTERM, etc).

Design contract â€” see ``docs/api-pull-direct-kafka-design.md`` Â§
Components â†’ "Long-running poller":

  * One :func:`run_loop` invocation owns one ``(domain, dataset)``.
  * Each iteration re-reads ``committed_cursor_value`` from the
    watermark store so a parallel finalise step that promoted the
    cursor is reflected immediately.
  * Transient errors from ``run_once`` (HTTP 5xx surfaced as
    ``RuntimeError``, transient kafka delivery RuntimeErrors) are
    swallowed and retried on the next tick. Config / validator errors
    (``ValueError``, ``KeyError``) are unrecoverable and propagate so
    the operator restarts cleanly with a fresh config.
  * The producer (and HTTP session, when applicable) is reused across
    iterations; ``run_once`` accepts an injected ``producer`` so we
    only pay the broker handshake / Schema Registry round-trip once.

The module deliberately does **not** import psycopg2 or
``confluent_kafka`` at module import time; both are produced via the
factories supplied by the caller so unit tests stay infra-free.
"""
from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from threading import Event
from typing import Any

from ods_pipeline.ingest.api_pull_kafka.runner import (
    PublishedBatch,
    build_avro_producer,
    run_once,
)

logger = logging.getLogger(__name__)

# Sentinel exception types treated as unrecoverable. ``run_once`` raises
# RuntimeError for transient kafka / HTTP issues; ValueError / KeyError /
# TypeError indicate the dataset_config or schema is malformed and a
# retry will not help.
_FATAL_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ValueError,
    KeyError,
    TypeError,
)

_DEFAULT_POLL_INTERVAL_SECONDS = 5.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_loop(
    dataset_config: Mapping[str, Any],
    *,
    kafka_bootstrap: str,
    schema_registry_url: str,
    schema_str: str,
    watermark_store_factory: Callable[[], Any],
    stop_event: Event | None = None,
    env: Mapping[str, str] | None = None,
    sleep: Callable[[float], None] | None = None,
    producer: Any | None = None,
) -> None:
    """Run the direct-Kafka poller in a loop until ``stop_event`` fires.

    Parameters
    ----------
    dataset_config
        Same shape consumed by :func:`run_once` plus the optional
        ``source.poll_interval_seconds`` knob (default 5s) and the
        ``source.continuous`` selector handled by the caller.
    kafka_bootstrap, schema_registry_url, schema_str
        Forwarded to the Avro producer factory on first iteration.
    watermark_store_factory
        Zero-arg callable returning an object with ``read(...)`` and
        ``record_pending(...)`` methods compatible with
        :class:`ods_pipeline.ingest.api_pull.WatermarkStore`. The
        callable is invoked once per iteration so the underlying
        psycopg2 connection can be refreshed if the loop runs for hours.
    stop_event
        :class:`threading.Event` set by the surrounding runtime
        (Airflow operator, signal handler) to request a clean exit.
    env
        Forwarded to ``run_once`` for auth provider env-var lookup.
    sleep
        Injection point for unit tests â€” defaults to :func:`time.sleep`.
    producer
        Optional pre-built confluent_kafka producer. When omitted the
        loop builds one from ``kafka_bootstrap`` / ``schema_registry_url``
        / ``schema_str`` on the first iteration and reuses it for the
        lifetime of the loop.

    Raises
    ------
    ValueError, KeyError, TypeError
        Propagated from ``run_once`` when configuration is malformed.
        The supervising operator should treat these as unrecoverable
        and not auto-restart without operator review.
    """
    domain = str(dataset_config["domain"])
    dataset = str(dataset_config["dataset"])
    source = dict(dataset_config.get("source") or {})
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    cursor_style = str((source.get("cursor") or {}).get("style", "since_timestamp"))
    poll_interval = float(
        source.get("poll_interval_seconds", _DEFAULT_POLL_INTERVAL_SECONDS)
    )
    if poll_interval <= 0:
        raise ValueError(
            f"poll_interval_seconds must be positive, got {poll_interval!r}"
        )

    stop = stop_event if stop_event is not None else Event()
    do_sleep = sleep if sleep is not None else time.sleep

    if producer is None:
        producer = build_avro_producer(
            kafka_bootstrap=kafka_bootstrap,
            schema_registry_url=schema_registry_url,
            schema_str=schema_str,
        )

    iteration = 0
    logger.info(
        "api_pull_kafka.loop.start domain=%s dataset=%s poll_interval=%.2fs",
        domain,
        dataset,
        poll_interval,
    )

    try:
        while not stop.is_set():
            iteration += 1
            run_id = str(uuid.uuid4())
            business_date = datetime.now(timezone.utc).date().isoformat()

            store = watermark_store_factory()
            try:
                watermark = store.read(
                    domain=domain,
                    dataset=dataset,
                    source_application=source_application,
                    cursor_type=cursor_style,
                )
                committed = getattr(watermark, "committed_cursor_value", None)
            finally:
                # Best-effort close â€” store may wrap a connection.
                _maybe_close(store)

            try:
                published: PublishedBatch = run_once(
                    dataset_config=dataset_config,
                    committed_cursor_value=committed,
                    run_id=run_id,
                    business_date=business_date,
                    producer=producer,
                    env=env,
                )
            except _FATAL_EXCEPTIONS:
                logger.exception(
                    "api_pull_kafka.loop.fatal domain=%s dataset=%s iter=%d",
                    domain,
                    dataset,
                    iteration,
                )
                raise
            except Exception as exc:  # noqa: BLE001 â€” transient, retry next tick
                logger.warning(
                    "api_pull_kafka.loop.transient domain=%s dataset=%s "
                    "iter=%d error=%s",
                    domain,
                    dataset,
                    iteration,
                    exc,
                )
                _wait(do_sleep, poll_interval, stop)
                continue

            if published.no_changes:
                logger.info(
                    "api_pull_kafka.loop.no_changes domain=%s dataset=%s iter=%d "
                    "ts=%s",
                    domain,
                    dataset,
                    iteration,
                    _now_iso(),
                )
            else:
                if published.new_cursor_value is not None:
                    store2 = watermark_store_factory()
                    try:
                        store2.record_pending(
                            domain=domain,
                            dataset=dataset,
                            source_application=source_application,
                            run_id=run_id,
                            new_cursor_value=published.new_cursor_value,
                        )
                    finally:
                        _maybe_close(store2)
                logger.info(
                    "api_pull_kafka.loop.published domain=%s dataset=%s "
                    "iter=%d records=%d topic=%s new_cursor=%s",
                    domain,
                    dataset,
                    iteration,
                    published.record_count,
                    published.target_topic,
                    published.new_cursor_value,
                )

            _wait(do_sleep, poll_interval, stop)
    finally:
        logger.info(
            "api_pull_kafka.loop.exit domain=%s dataset=%s iterations=%d",
            domain,
            dataset,
            iteration,
        )
        # Drain producer best-effort so in-flight messages aren't lost
        # on operator shutdown. Producer creation is owned here when not
        # injected, so we own teardown too.
        try:
            flush = getattr(producer, "flush", None)
            if callable(flush):
                flush(timeout=5)
        except Exception:  # noqa: BLE001
            pass


def _wait(sleep_fn: Callable[[float], None], seconds: float, stop: Event) -> None:
    """Sleep ``seconds`` but break early when ``stop`` is set.

    The injected ``sleep_fn`` is normally :func:`time.sleep`; tests
    pass a fake clock that records the requested durations.
    """
    if stop.is_set():
        return
    sleep_fn(seconds)


def _maybe_close(store: Any) -> None:
    closer = getattr(store, "close", None)
    if callable(closer):
        try:
            closer()
        except Exception:  # noqa: BLE001
            pass
```

### `ods_pipeline\ingest\api_pull_kafka\runner.py`

```python
"""Direct API â†’ Kafka api_pull runner â€” one poll/publish cycle.

Reuses the polling primitives from ``ods_pipeline.ingest.api_pull``
(:func:`build_auth`, :func:`build_cursor`) plus an idempotent +
transactional Avro Kafka producer. Output is one Avro message per
source record on the dataset's raw Kafka topic; the message envelope
mirrors the file-pipeline ODS metadata block so downstream consumers
(canonicalize, JDBC sink, S3 sink) see identical correlation fields.

This module does **not** touch the Postgres control-plane. The DAG
dispatcher (``dag_api_pull``) is responsible for ``run_log``,
``run_stage_log``, ``lineage_edge``, ``reconciliation_log`` and the
``api_pull_watermark`` lifecycle. Keeping I/O concerns split makes the
runner unit-testable without psycopg2.
"""
from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ods_pipeline import metadata as _metadata
from ods_pipeline.ingest.api_pull.auth import AuthProvider, build_auth
from ods_pipeline.ingest.api_pull.cursors import Cursor, CursorRequest, build_cursor

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class PublishedBatch:
    """Outcome of one direct-Kafka poll/publish cycle.

    ``no_changes=True`` means the source returned 0 records or 304 Not
    Modified. The DAG should mark the run skipped, not advance the
    watermark, and skip the produce step's recon and lineage writes.
    """

    domain: str
    dataset: str
    source_application: str
    run_id: str
    target_topic: str
    record_count: int
    page_count: int
    old_cursor_value: str | None
    new_cursor_value: str | None
    source_request_id: str
    offset_start_by_partition: dict[int, int] = field(default_factory=dict)
    offset_end_by_partition: dict[int, int] = field(default_factory=dict)
    no_changes: bool = False


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def _build_session(*, auth: AuthProvider, timeout_seconds: float, retries: int) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers["Accept"] = "application/json"
    original = session.request

    def _with_timeout(*args, **kwargs):
        kwargs.setdefault("timeout", timeout_seconds)
        return original(*args, **kwargs)

    session.request = _with_timeout  # type: ignore[assignment]
    auth.apply(session)
    return session


def _records_from_body(body: Any) -> Sequence[Mapping[str, Any]]:
    """Accept ``[{...}]`` or ``{"items": [...]}`` etc. Same semantics as
    the file-pipeline poller so wire-shape changes are not required when
    a dataset is moved between delivery modes."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("items", "data", "records", "results"):
            value = body.get(key)
            if isinstance(value, list):
                return value
    return []


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def build_envelope(
    *,
    record: Mapping[str, Any],
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    source_request_id: str,
    business_date: str,
    cursor_value: str | None,
    schema_id: str,
    schema_version: int | str,
) -> dict[str, Any]:
    """Build the per-record Avro envelope.

    Top-level fields are the source record keys (flat). ODS metadata
    fields are stamped alongside so the JDBC sink can persist origin
    correlation without nested types.
    """
    meta = _metadata.message_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        source_request_id=source_request_id,
    )
    envelope: dict[str, Any] = {**dict(record)}
    envelope["_ods_run_id"] = run_id
    envelope["_ods_business_date"] = business_date
    envelope["_ods_source_request_id"] = meta.get("_ods_source_request_id")
    envelope["_ods_source_message_id"] = meta.get("_ods_source_message_id")
    envelope["_ods_source_event_id"] = meta.get("_ods_source_event_id")
    envelope["_ods_source_batch_id"] = meta.get("_ods_source_batch_id")
    envelope["_ods_source_application"] = source_application
    envelope["_ods_source_cursor"] = cursor_value
    envelope["_ods_archive_s3_uri"] = None  # written by the S3 sink later
    envelope["_ods_domain"] = domain
    envelope["_ods_dataset"] = dataset
    envelope["_ods_file_id"] = None  # synthetic file_id is the DAG's job
    envelope["_ods_ingested_at"] = meta.get("_ods_ingested_at")
    envelope["_ods_schema_id"] = schema_id
    envelope["_ods_schema_version"] = (
        int(schema_version) if isinstance(schema_version, (int, str)) and str(schema_version).isdigit()
        else None
    )
    envelope["_ods_kafka_partition"] = None  # filled in by delivery callback
    envelope["_ods_kafka_offset"] = None
    return envelope


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


def build_avro_producer(
    *,
    kafka_bootstrap: str,
    schema_registry_url: str,
    schema_str: str,
    transactional_id: str | None = None,
):
    """Construct an idempotent Avro :class:`SerializingProducer`.

    Lazy import so the runner module imports cleanly in environments
    without confluent_kafka (unit tests use the injected ``producer``
    parameter on :func:`run_once`).
    """
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import StringSerializer

    registry = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(registry, schema_str)
    config = {
        "bootstrap.servers": kafka_bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 5,
    }
    if transactional_id:
        config["transactional.id"] = transactional_id
    return SerializingProducer(config)


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------


def run_once(
    *,
    dataset_config: Mapping[str, Any],
    kafka_bootstrap: str | None = None,
    schema_registry_url: str | None = None,
    schema_str: str | None = None,
    committed_cursor_value: str | None,
    run_id: str,
    business_date: str,
    session: requests.Session | None = None,
    cursor: Cursor | None = None,
    producer: Any = None,
    env: Mapping[str, str] | None = None,
) -> PublishedBatch:
    """Execute one poll/publish cycle.

    Walks pages via the configured cursor, builds Avro envelopes per
    record, produces them to the dataset's raw topic, and waits for
    delivery. Returns the per-partition offset window and the new
    cursor value. If the source returns no records, returns
    ``no_changes=True`` and produces nothing.

    Injectables (``session``, ``cursor``, ``producer``) keep the runner
    unit-testable without HTTP / Kafka / Schema Registry.
    """
    domain = str(dataset_config["domain"])
    dataset = str(dataset_config["dataset"])
    source = dict(dataset_config.get("source") or {})
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    target_topic = str(dataset_config["target_topic"])
    schema_id = str(dataset_config.get("schema_id", f"{domain}.{dataset}"))
    schema_version = dataset_config.get("schema_version", 1)
    timeout_seconds = float(source.get("timeout_seconds", 30))
    retries = int(source.get("retries", 3))

    if session is None:
        auth = build_auth(source.get("auth"), env=env)
        session = _build_session(
            auth=auth,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )
    if cursor is None:
        cursor = build_cursor(source, committed_value=committed_cursor_value)

    source_request_id = str(uuid.uuid4())
    all_records: list[Mapping[str, Any]] = []
    request: CursorRequest | None = cursor.initial_request()
    page_count = 0

    while request is not None:
        response = session.get(request.url, params=request.params or None)
        page_count += 1
        if response.status_code == 304:
            break
        response.raise_for_status()
        body = response.json() if response.content else None
        all_records.extend(_records_from_body(body))
        request = cursor.next_request(response.headers, body)

    new_cursor_value = cursor.advance(all_records)

    if not all_records:
        return PublishedBatch(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            run_id=run_id,
            target_topic=target_topic,
            record_count=0,
            page_count=page_count,
            old_cursor_value=committed_cursor_value,
            new_cursor_value=None,
            source_request_id=source_request_id,
            no_changes=True,
        )

    if producer is None:
        if not (kafka_bootstrap and schema_registry_url and schema_str):
            raise ValueError(
                "producer must be supplied OR kafka_bootstrap + "
                "schema_registry_url + schema_str must be provided"
            )
        producer = build_avro_producer(
            kafka_bootstrap=kafka_bootstrap,
            schema_registry_url=schema_registry_url,
            schema_str=schema_str,
        )

    delivered_offsets: dict[int, int] = {}
    delivered_starts: dict[int, int] = {}
    delivery_errors: list[str] = []

    def _on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(str(err))
            return
        partition = int(msg.partition())
        offset = int(msg.offset())
        delivered_starts.setdefault(partition, offset)
        if partition not in delivered_offsets or delivered_offsets[partition] < offset:
            delivered_offsets[partition] = offset

    for record in all_records:
        envelope = build_envelope(
            record=record,
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            source_request_id=source_request_id,
            business_date=business_date,
            cursor_value=committed_cursor_value,
            schema_id=schema_id,
            schema_version=schema_version,
        )
        producer.produce(
            topic=target_topic,
            key=str(envelope["_ods_run_id"]),
            value=envelope,
            on_delivery=_on_delivery,
        )

    remaining = producer.flush(timeout=30)
    if remaining:
        raise RuntimeError(
            f"{remaining} kafka message(s) not delivered for run_id={run_id}"
        )
    if delivery_errors:
        raise RuntimeError(
            f"kafka delivery errors for run_id={run_id}: {delivery_errors[:3]}"
        )

    # End-offset = max delivered offset + 1 (next-message convention).
    offset_end = {p: o + 1 for p, o in delivered_offsets.items()}

    return PublishedBatch(
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        run_id=run_id,
        target_topic=target_topic,
        record_count=len(all_records),
        page_count=page_count,
        old_cursor_value=committed_cursor_value,
        new_cursor_value=new_cursor_value,
        source_request_id=source_request_id,
        offset_start_by_partition=dict(delivered_starts),
        offset_end_by_partition=offset_end,
        no_changes=False,
    )
```

### `ods_pipeline\lineage.py`

```python
"""pipeline.lineage_edge operations."""
from __future__ import annotations

import ods_ingestion_control as control


def write_edge(
    conn,
    *,
    child_run_id: str,
    edge_type: str,
    parent_run_id: str | None = None,
    parent_file_id: str | None = None,
    source_ref: str | None = None,
    target_ref: str | None = None,
    record_count: int | None = None,
) -> None:
    """Insert one row into ``pipeline.lineage_edge``.

    Either ``parent_run_id`` or ``parent_file_id`` (or both) should be supplied.

    Common *edge_type* values (use these constants in callers):
      * ``"raw_to_curated"``   â€” S3 raw â†’ S3 curated (written by ingestion job)
      * ``"curated_to_kafka"`` â€” S3 curated â†’ Kafka topic (written by publish job)
      * ``"curated_to_postgres"`` â€” S3 curated â†’ Postgres table
    """
    if parent_run_id is None and parent_file_id is None:
        raise ValueError(
            "write_edge requires at least one of parent_run_id or parent_file_id"
        )
    control.write_lineage_edge(
        conn,
        child_run_id=child_run_id,
        edge_type=edge_type,
        parent_run_id=parent_run_id,
        parent_file_id=parent_file_id,
        source_ref=source_ref,
        target_ref=target_ref,
        record_count=record_count,
    )
```

### `ods_pipeline\messages.py`

```python
"""Control-plane helpers for message/API ingestion flows.

Stateless contract (since 2026-05-07)
-------------------------------------

Both :func:`start_run` and :func:`record_result` commit per write â€” the
caller no longer wraps them in a transaction. The strict write order in
``record_result`` (stages â†’ archive â†’ reconciliation â†’ run.status) means
the dashboard never observes a ``status='succeeded'`` run without its
proof rows already landed: succeeded is the LAST commit.

For mid-flight failures the caller's own ``except`` block (or the
:func:`ods_pipeline.stages.stage_scope` context manager) is responsible
for writing the ``stage_failed`` row and calling
:func:`ods_pipeline.runs.update` with ``status='failed'``. If even those
handlers don't run (process killed mid-flight), the heartbeat-staleness
janitor (``airflow.dags.dag_run_janitor``) closes the orphan row.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from ods_pipeline import metadata, reconciliation, runs, stages
from ods_pipeline.models import PATTERN_CORRELATION_FIELD, PatternType, Stage, StageEvent


def _caller_must_rollback_on_exception() -> None:
    """Caller-managed transaction contract.

    ``record_result`` (below) calls ``runs.update``, ``stages.write``,
    ``stages.finish``, and ``reconciliation.write_check`` with
    ``commit=False``. The CALLER MUST wrap the invocation in a try/except
    and call ``conn.rollback()`` on exception, otherwise the open
    transaction stays open and partially-written rows remain in pg_locks.

    Recommended pattern::

        try:
            messages.record_result(conn, ...)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    """


def correlate(
    message: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    pattern_type: str,
) -> bool:
    """Return True if ``message`` belongs to the given ``context`` for ``pattern_type``.

    Each ingestion pattern has its own correlation field (see
    :data:`ods_pipeline.models.PATTERN_CORRELATION_FIELD`). The function:

    - looks up the field for the pattern,
    - returns True if both message and context carry the same non-empty value,
    - returns True when context has no correlation set (broadcast / "all
      messages of this pattern"),
    - falls back to ``_ods_run_id`` cross-check for the FILE pattern only,
      preserving the legacy two-key match in
      :func:`glue.jobs.canonicalize.matches_context`.

    Raises ``ValueError`` for unknown ``pattern_type``.
    """
    if pattern_type not in PatternType.ALL:
        raise ValueError(
            f"unknown pattern_type {pattern_type!r}; "
            f"expected one of {sorted(PatternType.ALL)}"
        )
    field = PATTERN_CORRELATION_FIELD[pattern_type]
    ctx_value = context.get(field) or context.get(field.removeprefix("_ods_"))
    msg_value = message.get(field)

    if pattern_type == PatternType.FILE:
        # Legacy: file pattern allows correlation by run_id as a secondary key.
        ctx_run = context.get("_ods_run_id") or context.get("run_id") or context.get("parent_run_id")
        msg_run = message.get("_ods_run_id")
        if ctx_value and msg_value and str(ctx_value) == str(msg_value):
            return True
        if ctx_run and msg_run and str(ctx_run) == str(msg_run):
            return True
        # No context = broadcast.
        return not ctx_value and not ctx_run

    if not ctx_value:
        return True  # broadcast / no correlation set
    return msg_value is not None and str(ctx_value) == str(msg_value)


def _correlation_metadata(
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    correlation: Mapping[str, Any],
) -> dict[str, Any]:
    message_meta = metadata.message_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        source_message_id=correlation.get("_ods_source_message_id")
        or correlation.get("source_message_id"),
        source_event_id=correlation.get("_ods_source_event_id")
        or correlation.get("source_event_id"),
        source_request_id=correlation.get("_ods_source_request_id")
        or correlation.get("source_request_id"),
        source_batch_id=correlation.get("_ods_source_batch_id")
        or correlation.get("source_batch_id"),
    )
    return {
        key: message_meta.get(key)
        for key in metadata.MESSAGE_CORRELATION_FIELDS
        if message_meta.get(key)
    }


def start_run(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    correlation: Mapping[str, Any],
    business_date: str | None = None,
    kafka_topic: str | None = None,
    expected_count: int | None = None,
    parents: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Start a message/API run and open the receive stage.

    ``correlation`` must include at least one of:
    ``source_message_id``, ``source_event_id``, ``source_request_id``, or
    ``source_batch_id``.  The helper intentionally writes only existing
    pipeline tables; payload storage remains S3/Kafka responsibility.
    """
    correlation_meta = _correlation_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        correlation=correlation,
    )
    parent_payload = list(parents or [])
    parent_payload.append({
        "edge_type": "message_correlation",
        "source_application": source_application,
        **correlation_meta,
    })
    runs.start(
        conn,
        run_id=run_id,
        pipeline_type="message_api",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        kafka_topic=kafka_topic,
        parents=parent_payload,
    )
    stages.start(
        conn,
        run_id=run_id,
        stage=Stage.MESSAGE_RECEIVE,
        record_count_in=expected_count,
        metrics={
            "source_application": source_application,
            "correlation": correlation_meta,
        },
    )
    return correlation_meta


def record_result(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_count: int,
    published_count: int,
    validation_fail_count: int = 0,
    dlq_count: int = 0,
    archive_count: int | None = None,
    business_date: str | None = None,
    kafka_topic: str | None = None,
    dlq_ref: str | None = None,
    archive_ref: str | None = None,
    extra_detail: Mapping[str, Any] | None = None,
) -> str:
    """Close a message/API run with count reconciliation and stage facts.

    Stateless write order (2026-05-07): every helper call below commits
    independently. Order matters â€” recon row writes BEFORE the run flips
    to terminal status so the dashboard rule "succeeded â‡’ recon row
    present" always holds. If the process dies mid-call the next-minute
    janitor closes the still-running row; the partial stage rows that
    landed remain durable so operators can see exactly where the run
    stopped.
    """
    accepted_count = int(source_count) - int(validation_fail_count) - int(dlq_count)
    discrepancy = int(published_count) - accepted_count
    archive_discrepancy = (
        None if archive_count is None else int(archive_count) - int(source_count)
    )
    ok = discrepancy == 0 and (archive_discrepancy in (None, 0))
    status = "succeeded" if ok else "failed"

    stages.finish(
        conn,
        run_id=run_id,
        stage=Stage.MESSAGE_RECEIVE,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        record_count_in=source_count,
        record_count_out=source_count,
        commit=True,
    )
    stages.write(
        conn,
        run_id=run_id,
        stage=Stage.MESSAGE_VALIDATE,
        status="warned" if validation_fail_count else "succeeded",
        event_type=StageEvent.WARNED if validation_fail_count else StageEvent.COMPLETED,
        record_count_in=source_count,
        record_count_out=source_count - validation_fail_count,
        metrics={"validation_fail_count": validation_fail_count},
        commit=True,
    )
    if dlq_count:
        stages.write(
            conn,
            run_id=run_id,
            stage=Stage.DLQ_WRITE,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            output_ref=dlq_ref,
            record_count_in=dlq_count,
            record_count_out=dlq_count,
            commit=True,
        )
    if archive_count is not None:
        stages.write(
            conn,
            run_id=run_id,
            stage=Stage.MESSAGE_ARCHIVE,
            status="succeeded" if archive_discrepancy == 0 else "failed",
            event_type=StageEvent.COMPLETED if archive_discrepancy == 0 else StageEvent.FAILED,
            output_ref=archive_ref,
            record_count_in=source_count,
            record_count_out=archive_count,
            error=None if archive_discrepancy == 0 else f"archive discrepancy={archive_discrepancy}",
            commit=True,
        )

    detail = {
        "source_count": source_count,
        "validation_fail_count": validation_fail_count,
        "dlq_count": dlq_count,
        "accepted_count": accepted_count,
        "published_count": published_count,
        "archive_count": archive_count,
        "archive_discrepancy": archive_discrepancy,
        **dict(extra_detail or {}),
    }
    reconciliation.write_check(
        conn,
        check_type="message_batch_count",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=accepted_count,
        kafka_count=published_count,
        status="ok" if ok else "failed",
        detail=json.dumps(detail, sort_keys=True),
        commit=True,
    )
    stages.write(
        conn,
        run_id=run_id,
        stage=Stage.RECON_MESSAGE,
        status="succeeded" if ok else "failed",
        event_type=StageEvent.COMPLETED if ok else StageEvent.FAILED,
        record_count_in=accepted_count,
        record_count_out=published_count,
        metrics=detail,
        error=None if ok else f"message reconciliation mismatch={discrepancy}",
        commit=True,
    )
    runs.update(
        conn,
        run_id,
        commit=True,
        status=status,
        record_count_source=source_count,
        record_count_dq_fail=validation_fail_count + dlq_count,
        record_count_published=published_count,
        kafka_topic=kafka_topic,
        error_summary=None if ok else "message/API reconciliation failed",
    )
    return status
```

### `ods_pipeline\metadata.py`

```python
"""ODS metadata contracts for file, message, canonical, and archive records."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping

FILE_RECORD_FIELDS: tuple[str, ...] = (
    "_ods_file_id",
    "_ods_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_business_date",
    "_ods_source_application",
    "_ods_ingested_at",
)

MESSAGE_CORRELATION_FIELDS: tuple[str, ...] = (
    "_ods_source_message_id",
    "_ods_source_event_id",
    "_ods_source_request_id",
    "_ods_source_batch_id",
)

MESSAGE_RECORD_FIELDS: tuple[str, ...] = (
    *MESSAGE_CORRELATION_FIELDS,
    "_ods_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_source_application",
    "_ods_ingested_at",
)

CANONICAL_FILE_RECORD_FIELDS: tuple[str, ...] = (
    "_ods_file_id",
    "_ods_raw_run_id",
    "_ods_canonicalize_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_business_date",
    "_ods_source_application",
    "_ods_ingested_at",
)

CANONICAL_MESSAGE_RECORD_FIELDS: tuple[str, ...] = (
    *MESSAGE_CORRELATION_FIELDS,
    "_ods_raw_run_id",
    "_ods_canonicalize_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_source_application",
    "_ods_ingested_at",
)

HISTORY_TABLE_FIELDS: tuple[str, ...] = (
    "_ods_run_id",
    "_ods_business_date",
    "_ods_ingested_at",
)

FILE_HISTORY_TABLE_FIELDS: tuple[str, ...] = (
    *HISTORY_TABLE_FIELDS,
    "_ods_file_id",
)

MESSAGE_HISTORY_TABLE_FIELDS: tuple[str, ...] = (
    *HISTORY_TABLE_FIELDS,
    *MESSAGE_CORRELATION_FIELDS,
)

ARCHIVE_ENVELOPE_FIELDS: tuple[str, ...] = (
    *MESSAGE_CORRELATION_FIELDS,
    "_ods_source_application",
    "_ods_domain",
    "_ods_dataset",
    "_ods_ingested_at",
    "_ods_schema_id",
    "_ods_schema_version",
    "_ods_run_id",
    "payload",
)


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp for ODS metadata fields."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalise_value(value: Any) -> Any:
    """Normalise common Python values into JSON/Avro friendly metadata values."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def missing_fields(record: Mapping[str, Any], required_fields: tuple[str, ...]) -> list[str]:
    """Return required fields that are absent, ``None``, or blank strings."""
    missing: list[str] = []
    for field in required_fields:
        value = record.get(field)
        if value is None or value == "":
            missing.append(field)
    return missing


def require_fields(
    record: Mapping[str, Any],
    required_fields: tuple[str, ...],
    *,
    context: str = "record",
) -> None:
    """Raise ``ValueError`` if *record* misses any required ODS metadata fields."""
    missing = missing_fields(record, required_fields)
    if missing:
        raise ValueError(f"{context} missing required ODS metadata fields: {missing}")


def require_message_correlation(record: Mapping[str, Any], *, context: str = "message") -> None:
    """Require at least one stable source correlation key for message/API records."""
    if not any(record.get(field) for field in MESSAGE_CORRELATION_FIELDS):
        raise ValueError(
            f"{context} must include at least one source correlation key: "
            f"{list(MESSAGE_CORRELATION_FIELDS)}"
        )


def file_metadata(
    *,
    file_id: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | date,
    source_application: str,
    ingested_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Build the standard metadata block for file based records."""
    metadata = {
        "_ods_file_id": file_id,
        "_ods_run_id": run_id,
        "_ods_domain": domain,
        "_ods_dataset": dataset,
        "_ods_business_date": normalise_value(business_date),
        "_ods_source_application": source_application,
        "_ods_ingested_at": normalise_value(ingested_at) if ingested_at else utc_now_iso(),
    }
    require_fields(metadata, FILE_RECORD_FIELDS, context="file metadata")
    return metadata


def message_metadata(
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    source_message_id: str | None = None,
    source_event_id: str | None = None,
    source_request_id: str | None = None,
    source_batch_id: str | None = None,
    ingested_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Build the standard metadata block for message/API records."""
    metadata = {
        "_ods_source_message_id": source_message_id,
        "_ods_source_event_id": source_event_id,
        "_ods_source_request_id": source_request_id,
        "_ods_source_batch_id": source_batch_id,
        "_ods_run_id": run_id,
        "_ods_domain": domain,
        "_ods_dataset": dataset,
        "_ods_source_application": source_application,
        "_ods_ingested_at": normalise_value(ingested_at) if ingested_at else utc_now_iso(),
    }
    require_fields(
        metadata,
        ("_ods_run_id", "_ods_domain", "_ods_dataset", "_ods_source_application", "_ods_ingested_at"),
        context="message metadata",
    )
    require_message_correlation(metadata)
    return metadata


def canonical_metadata(
    source_metadata: Mapping[str, Any],
    *,
    canonicalize_run_id: str,
    raw_run_id: str | None = None,
) -> dict[str, Any]:
    """Preserve source correlation metadata and add canonicalization run metadata."""
    metadata = {
        key: source_metadata.get(key)
        for key in (
            "_ods_file_id",
            *MESSAGE_CORRELATION_FIELDS,
            "_ods_domain",
            "_ods_dataset",
            "_ods_business_date",
            "_ods_source_application",
            "_ods_ingested_at",
        )
        if source_metadata.get(key) is not None
    }
    metadata["_ods_raw_run_id"] = raw_run_id or source_metadata.get("_ods_run_id")
    metadata["_ods_canonicalize_run_id"] = canonicalize_run_id
    if metadata.get("_ods_file_id"):
        require_fields(metadata, CANONICAL_FILE_RECORD_FIELDS, context="canonical file metadata")
    else:
        require_message_correlation(metadata, context="canonical message metadata")
        require_fields(
            metadata,
            (
                "_ods_raw_run_id",
                "_ods_canonicalize_run_id",
                "_ods_domain",
                "_ods_dataset",
                "_ods_source_application",
                "_ods_ingested_at",
            ),
            context="canonical message metadata",
        )
    return metadata


def archive_envelope(
    *,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any],
    schema_id: str,
    schema_version: int | str,
    archive_s3_uri: str | None = None,
) -> dict[str, Any]:
    """Build a JSONL-friendly S3 archive envelope for complex events."""
    envelope = {
        key: metadata.get(key)
        for key in (
            *MESSAGE_CORRELATION_FIELDS,
            "_ods_source_application",
            "_ods_domain",
            "_ods_dataset",
            "_ods_ingested_at",
            "_ods_run_id",
        )
        if metadata.get(key) is not None
    }
    envelope["_ods_schema_id"] = schema_id
    envelope["_ods_schema_version"] = schema_version
    if archive_s3_uri is not None:
        envelope["_ods_archive_s3_uri"] = archive_s3_uri
    envelope["payload"] = dict(payload)
    require_message_correlation(envelope, context="archive envelope")
    require_fields(
        envelope,
        (
            "_ods_source_application",
            "_ods_domain",
            "_ods_dataset",
            "_ods_ingested_at",
            "_ods_schema_id",
            "_ods_schema_version",
            "_ods_run_id",
            "payload",
        ),
        context="archive envelope",
    )
    return envelope
```

### `ods_pipeline\models.py`

```python
"""Canonical constants for the pipeline control-plane schema."""
from __future__ import annotations


class PatternType:
    """Ingestion pattern types. Each has its own correlation field.

    Used by ``ods_pipeline.messages.correlate`` and the future
    ``ods_pipeline.patterns.IngestionPattern`` registry (T12).
    """

    FILE  = "file"   # raw file ingestion -> correlate by _ods_file_id
    CDC   = "cdc"    # change data capture -> correlate by _ods_change_lsn
    API   = "api"    # synchronous request  -> correlate by _ods_source_request_id
    EVENT = "event"  # async event stream   -> correlate by _ods_source_event_id

    ALL: frozenset[str] = frozenset({"file", "cdc", "api", "event"})


#: Maps pattern type to the canonical correlation field on the message envelope.
PATTERN_CORRELATION_FIELD: dict[str, str] = {
    PatternType.FILE:  "_ods_file_id",
    PatternType.CDC:   "_ods_change_lsn",
    PatternType.API:   "_ods_source_request_id",
    PatternType.EVENT: "_ods_source_event_id",
}


class Stage:
    """Valid values for ``pipeline.run_stage_log.stage``."""

    RAW_READ        = "raw_read"         # ingestion: read CSV/file from S3 raw
    RAW_POLL        = "raw_poll"         # api_pull: HTTP poll source API for records
    SCHEMA_VALIDATE = "schema_validate"  # validate columns against schema registry
    DQ_CHECK        = "dq_check"         # data quality rules evaluation
    CURATED_WRITE   = "curated_write"    # write Parquet to S3 curated
    CURATED_READ    = "curated_read"     # publish: read curated Parquet
    KAFKA_CONSUME   = "kafka_consume"    # canonicalize: bounded raw-topic consume
    CANONICAL_TRANSFORM = "canonical_transform"  # raw-shape -> canonical-shape
    KAFKA_PUBLISH   = "kafka_publish"    # produce Avro messages to Kafka topic
    MESSAGE_RECEIVE = "message_receive"  # message/API: receive request/batch/window
    MESSAGE_VALIDATE = "message_validate"  # message/API: validate source events
    MESSAGE_ARCHIVE = "message_archive"  # message/API: write S3 archive envelopes
    DLQ_WRITE       = "dlq_write"         # write failed records/events to DLQ
    RECON_T0        = "recon_t0"         # T0 offset reconciliation check
    RECON_T1        = "recon_t1"         # raw-topic -> canonical-topic reconciliation
    RECON_MESSAGE   = "recon_message"    # message/API count reconciliation
    SINK_PG_WAIT    = "sink_pg_wait"     # wait for JDBC sink to consume offsets
    SINK_S3_WAIT    = "sink_s3_wait"     # wait for S3 sink to consume offsets
    FINALISE        = "finalise"         # DAG finalise: mark run succeeded

    @classmethod
    def all_values(cls) -> frozenset[str]:
        return frozenset(
            v for k, v in vars(cls).items()
            if not k.startswith("_") and isinstance(v, str)
        )


class StageEvent:
    """Valid values for ``pipeline.run_stage_log.event_type``.

    Filtering convention:
        terminal events  = stage_completed | stage_failed | stage_skipped | stage_warned
        in-progress      = stage_started
    """

    STARTED   = "stage_started"
    COMPLETED = "stage_completed"
    FAILED    = "stage_failed"
    SKIPPED   = "stage_skipped"
    WARNED    = "stage_warned"   # completed with warnings (e.g. DQ soft blocks)
    HEARTBEAT = "stage_heartbeat"  # long-running stage liveness ping (R8)

    TERMINAL: frozenset[str] = frozenset(
        {"stage_completed", "stage_failed", "stage_skipped", "stage_warned"}
    )


class RunStatus:
    """Valid values for ``pipeline.run_log.status``."""

    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    PARTIAL   = "partial"


#: Statuses that close a run (set ``ended_at``).
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.PARTIAL}
)

#: Fields that may be inserted into ``pipeline.glue_job_log`` by
#: ``glue.jobs.utils.write_job_log``. Any caller-supplied key outside this set
#: is rejected before SQL composition to prevent identifier injection.
ALLOWED_JOB_LOG_FIELDS: frozenset[str] = frozenset({
    "run_id",
    "job_name",
    "pipeline_type",
    "domain",
    "dataset",
    "source_path",
    "target_path",
    "business_date",
    "status",
    "record_count",
    "error_reason",
    "error_detail",
    "config_version",
    "config_snapshot",
})


#: Fields that may be updated on ``pipeline.run_log``.
ALLOWED_RUN_FIELDS: frozenset[str] = frozenset({
    "status",
    "record_count_source",
    "record_count_dq_pass",
    "record_count_dq_fail",
    "record_count_published",
    "kafka_topic",
    "kafka_offset_start",
    "kafka_offset_end",
    "config_version_id",
    "schema_version_id",
    "parents",
    "runtime_context",
    "error_summary",
    "file_id",
    "business_date",
})
```

### `ods_pipeline\offsets.py`

```python
"""Kafka offset helpers for partition-safe reconciliation and sink waits."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

OffsetMap = dict[int, int]
OffsetRangeMap = dict[int, tuple[int, int]]


def persist_ranges(
    conn,
    *,
    run_id: str,
    stage: str,
    topic: str,
    ranges: Mapping[int, tuple[int, int]],
    commit: bool = True,
) -> int:
    """Write per-partition offset ranges to ``pipeline.run_kafka_offsets``.

    Returns the number of rows inserted/updated.

    Designed to be called inside the SAME transaction as the run-status update
    so a crash between Kafka transaction commit and Postgres commit leaves a
    detectable inconsistency (no offset rows + run still 'running').

    ``commit=True`` (default) preserves single-call ergonomics; ``commit=False``
    lets a caller (B8 exactly-once flow) own the transaction boundary and
    commit alongside ``runs.update``.
    """
    if not ranges:
        if commit:
            conn.commit()
        return 0
    rows = [
        (run_id, stage, topic, int(partition), int(start), int(end))
        for partition, (start, end) in ranges.items()
    ]
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO pipeline.run_kafka_offsets
                    (run_id, stage, topic, partition, offset_start, offset_end)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, stage, topic, partition)
                DO UPDATE SET
                    offset_start = EXCLUDED.offset_start,
                    offset_end   = EXCLUDED.offset_end,
                    recorded_at  = now()
                """,
                rows,
            )
        if commit:
            conn.commit()
        return len(rows)
    except Exception:
        if commit:
            conn.rollback()
        raise


def read_ranges(conn, *, run_id: str, stage: str | None = None) -> dict[str, OffsetRangeMap]:
    """Return ``{topic: {partition: (start, end)}}`` for a run (optionally a stage).

    Empty dict if no offsets recorded.  Used by B8 resume-time idempotency
    check and T8 dashboards.
    """
    where = "run_id = %s"
    params: tuple[Any, ...] = (run_id,)
    if stage is not None:
        where += " AND stage = %s"
        params = (run_id, stage)
    out: dict[str, OffsetRangeMap] = {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT topic, partition, offset_start, offset_end
              FROM pipeline.run_kafka_offsets
             WHERE {where}
            """,
            params,
        )
        for topic, partition, offset_start, offset_end in cur.fetchall():
            out.setdefault(topic, {})[int(partition)] = (int(offset_start), int(offset_end))
    return out


def has_recorded_offsets(conn, *, run_id: str, stage: str) -> bool:
    """Cheap existence check used by B8 resume-time idempotency in ``runs.start``.

    Returns True if any ``run_kafka_offsets`` row exists for the
    ``(run_id, stage)`` pair â€” i.e. a prior attempt's Kafka transaction
    committed AND its offsets were persisted to Postgres in the same tx.
    Caller should treat this as 'republish would duplicate; skip'.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pipeline.run_kafka_offsets
             WHERE run_id = %s AND stage = %s
             LIMIT 1
            """,
            (run_id, stage),
        )
        return cur.fetchone() is not None


def normalise_offset_map(raw: Mapping[Any, Any] | str | None) -> OffsetMap:
    """Return ``{partition: offset}`` with integer keys and values."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    return {int(partition): int(offset) for partition, offset in raw.items()}


def normalise_range_map(raw: Mapping[Any, Any] | str | None) -> OffsetRangeMap:
    """Return ``{partition: (start, end)}`` from dict/list JSON shapes."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    ranges: OffsetRangeMap = {}
    for partition, value in raw.items():
        if isinstance(value, Mapping):
            start, end = value["start"], value["end"]
        else:
            start, end = value[0], value[1]
        ranges[int(partition)] = (int(start), int(end))
    return ranges


def range_count(ranges: Mapping[Any, Any] | str | None) -> int:
    """Count records represented by per-partition offset ranges."""
    return sum(
        max(0, end - start)
        for start, end in normalise_range_map(ranges).values()
    )


def delta_count(start_offsets: Mapping[Any, Any], end_offsets: Mapping[Any, Any]) -> int:
    """Count records between two per-partition high-watermark snapshots."""
    starts = normalise_offset_map(start_offsets)
    ends = normalise_offset_map(end_offsets)
    return sum(
        max(0, ends.get(partition, 0) - starts.get(partition, 0))
        for partition in set(starts) | set(ends)
    )


def ranges_from_offsets(start_offsets: Mapping[Any, Any], end_offsets: Mapping[Any, Any]) -> OffsetRangeMap:
    """Build per-partition ranges from two offset snapshots."""
    starts = normalise_offset_map(start_offsets)
    ends = normalise_offset_map(end_offsets)
    return {
        partition: (starts.get(partition, 0), ends.get(partition, 0))
        for partition in sorted(set(starts) | set(ends))
    }


def jsonable_offset_map(offsets: Mapping[Any, Any] | None) -> dict[str, int]:
    """Return an offset map that serializes cleanly to JSON."""
    return {
        str(partition): offset
        for partition, offset in sorted(normalise_offset_map(offsets).items())
    }


def jsonable_range_map(ranges: Mapping[Any, Any] | None) -> dict[str, dict[str, int]]:
    """Return a range map that serializes cleanly to JSON."""
    return {
        str(partition): {"start": start, "end": end}
        for partition, (start, end) in sorted(normalise_range_map(ranges).items())
    }


def partitions_consumed(committed_offsets: Mapping[Any, Any], target_offsets: Mapping[Any, Any]) -> bool:
    """True only when every target partition has reached its target offset."""
    committed = normalise_offset_map(committed_offsets)
    targets = normalise_offset_map(target_offsets)
    if not targets:
        return False
    return all(committed.get(partition, -1) >= target for partition, target in targets.items())


class OffsetTracker:
    """Capture broker-confirmed `(partition, offset)` per delivered Kafka message.

    Designed for use with confluent_kafka's transactional producer (B5/B6).
    The producer's per-message ``on_delivery`` callback runs on the librdkafka
    poll thread, so the callback MUST NOT raise.  Errors are accumulated in
    ``errors`` for the caller to inspect after ``producer.flush()``.

    Recon contract: ``len(rows_attempted) == tracker.delivered_count`` proves
    every queued ``produce()`` resulted in a broker-acknowledged write inside
    the open transaction. Safer than ``offset_end - offset_start`` because it
    is immune to other producers writing to the same topic concurrently.

    Usage:

        tracker = OffsetTracker()
        for row in rows:
            producer.produce(topic, key=k, value=v, on_delivery=tracker.on_delivery)
        producer.flush()
        if tracker.errors:
            raise RuntimeError(f"{len(tracker.errors)} delivery failures")
        assert tracker.delivered_count == len(rows)
    """

    def __init__(self) -> None:
        # {partition: [offset, ...]} â€” order is delivery order, not produce order.
        self._delivered: dict[int, list[int]] = {}
        self.errors: list[str] = []

    def on_delivery(self, err, msg) -> None:  # noqa: D401 â€” librdkafka callback shape
        """confluent_kafka per-message delivery callback. Must not raise."""
        if err is not None:
            self.errors.append(str(err))
            return
        try:
            partition = int(msg.partition())
            offset = int(msg.offset())
        except Exception as exc:  # pragma: no cover â€” defensive
            self.errors.append(f"unparseable delivery report: {exc}")
            return
        self._delivered.setdefault(partition, []).append(offset)

    @property
    def delivered_count(self) -> int:
        """Total number of broker-acknowledged messages across all partitions."""
        return sum(len(offsets) for offsets in self._delivered.values())

    def per_partition_counts(self) -> dict[int, int]:
        """``{partition: delivered_count}``."""
        return {partition: len(offsets) for partition, offsets in self._delivered.items()}

    def per_partition_ranges(self) -> OffsetRangeMap:
        """``{partition: (min_offset, max_offset+1)}`` for delivered messages.

        ``end`` is exclusive (matches the rest of this module's range
        convention).  Empty partitions are omitted.
        """
        return {
            partition: (min(offsets), max(offsets) + 1)
            for partition, offsets in self._delivered.items()
            if offsets
        }
```

### `ods_pipeline\ops\__init__.py`

```python
"""Operator-facing CLI: ``python -m ods_pipeline.ops <subcmd>``."""
```

### `ods_pipeline\ops\__main__.py`

```python
"""Entry point: ``python -m ods_pipeline.ops``.

Dispatches to subcommand modules. Argparse-only â€” no external deps.
"""
from __future__ import annotations

import argparse
import sys

from ods_pipeline.ops import dlq as dlq_cmd
from ods_pipeline.ops import runs as runs_cmd


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ods_pipeline.ops")
    sub = parser.add_subparsers(dest="cmd", required=True)
    dlq_cmd.register(sub.add_parser("dlq", help="DLQ inspection + replay"))
    runs_cmd.register(sub.add_parser("runs", help="replay / rerun runs"))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "dlq":
        return dlq_cmd.dispatch(args)
    if args.cmd == "runs":
        return runs_cmd.dispatch(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
```

### `ods_pipeline\ops\dlq.py`

```python
"""DLQ subcommand: list / show / replay.

Operators inspect S3 DLQ records and replay specific envelopes back through
the canonical pipeline. Replay creates a new ``run_log`` row linked via
``lineage_edge`` (``edge_type='replay'``, ``parent_run_id=<original failed
run>``) so the original evidence is preserved.

Designed to be unit-testable: side-effects (S3, Kafka, Postgres) are passed
in as already-constructed clients via the dependency injection in
``_DlqOps``. ``dispatch()`` builds default clients from env when invoked
from the CLI; tests construct ``_DlqOps`` directly.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections import Counter
from collections.abc import Iterable
from typing import Any

# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------

def register(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="dlq_cmd", required=True)

    p_list = sub.add_parser("list", help="counts by domain/dataset/run/stage")
    p_list.add_argument("--domain")
    p_list.add_argument("--dataset")
    p_list.add_argument("--prefix", help="restrict to specific S3 prefix")

    p_show = sub.add_parser("show", help="dump envelope JSON for a key")
    p_show.add_argument("s3_uri")

    p_replay = sub.add_parser("replay", help="re-publish a DLQ record")
    p_replay.add_argument("s3_uri")
    p_replay.add_argument("--target-topic", required=True,
                          help="Kafka topic to republish into")
    p_replay.add_argument("--dry-run", action="store_true",
                          help="print intended action; do not produce/write")


def dispatch(args) -> int:  # pragma: no cover â€” thin glue
    import boto3

    s3 = boto3.client("s3")
    pg = _connect_pg()
    producer = None  # lazily init only on replay
    bucket = _default_bucket()
    ops = _DlqOps(s3=s3, pg_conn=pg, bucket=bucket,
                  producer_factory=lambda: _make_producer())
    if args.dlq_cmd == "list":
        for line in ops.list(domain=args.domain, dataset=args.dataset,
                              prefix=args.prefix):
            print(line)
        return 0
    if args.dlq_cmd == "show":
        print(json.dumps(ops.show(args.s3_uri), indent=2, default=str))
        return 0
    if args.dlq_cmd == "replay":
        result = ops.replay(args.s3_uri, target_topic=args.target_topic,
                            dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))
        return 0
    print(f"unknown subcommand {args.dlq_cmd!r}", file=sys.stderr)
    return 2


# ---------------------------------------------------------------------------
# Core logic â€” DI-friendly
# ---------------------------------------------------------------------------

class _DlqOps:
    """Stateless wrapper. All side-effect collaborators are injected."""

    def __init__(self, *, s3, pg_conn, bucket: str,
                 producer_factory=None):
        self._s3 = s3
        self._pg = pg_conn
        self._bucket = bucket
        self._producer_factory = producer_factory

    # --- list -----------------------------------------------------------

    def list(self, *, domain: str | None = None, dataset: str | None = None,
             prefix: str | None = None) -> Iterable[str]:
        keys = list(self._iter_keys(domain=domain, dataset=dataset,
                                    prefix=prefix))
        if not keys:
            yield "(no DLQ records found)"
            return
        breakdown: Counter = Counter()
        for key in keys:
            parts = key.split("/")
            # layout: <domain>/<dataset>/<stage>/date=.../run_id=.../<n>.json
            if len(parts) >= 5:
                d, ds, stage = parts[0], parts[1], parts[2]
                run_id = next((p.split("=", 1)[1] for p in parts
                               if p.startswith("run_id=")), "?")
                breakdown[(d, ds, stage, run_id)] += 1
        yield f"total: {len(keys)} record(s)"
        yield "domain/dataset/stage/run_id\tcount"
        for (d, ds, stage, run_id), count in sorted(breakdown.items()):
            yield f"{d}/{ds}/{stage}/{run_id}\t{count}"

    # --- show -----------------------------------------------------------

    def show(self, s3_uri: str) -> dict[str, Any]:
        bucket, key = _split_s3_uri(s3_uri)
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())

    # --- replay ---------------------------------------------------------

    def replay(self, s3_uri: str, *, target_topic: str,
               dry_run: bool = False) -> dict[str, Any]:
        envelope = self.show(s3_uri)
        original_run = envelope.get("_ods_run_id")
        domain = envelope.get("_ods_domain")
        dataset = envelope.get("_ods_dataset")
        payload = envelope.get("payload", {})
        replay_run_id = str(uuid.uuid4())
        result = {
            "replay_run_id": replay_run_id,
            "original_run_id": original_run,
            "target_topic": target_topic,
            "domain": domain,
            "dataset": dataset,
            "dry_run": dry_run,
        }
        if dry_run:
            result["status"] = "dry-run"
            return result
        # Real replay: open new run, link via lineage, produce, commit
        from ods_pipeline import lineage, runs
        runs.start(self._pg, run_id=replay_run_id, pipeline_type="dlq_replay",
                   domain=domain or "unknown",
                   dataset=dataset or "unknown",
                   business_date=envelope.get("_ods_business_date"),
                   parents=[original_run] if original_run else None)
        if original_run:
            lineage.write_edge(self._pg, child_run_id=replay_run_id,
                               parent_run_id=original_run,
                               edge_type="replay",
                               source_ref=s3_uri,
                               target_ref=f"kafka://{target_topic}",
                               record_count=1)
        producer = self._producer_factory()
        producer.produce(topic=target_topic,
                         value=json.dumps(payload).encode("utf-8"))
        producer.flush()
        result["status"] = "replayed"
        return result

    # --- helpers --------------------------------------------------------

    def _iter_keys(self, *, domain: str | None, dataset: str | None,
                   prefix: str | None):
        if prefix is not None:
            search = prefix
        else:
            parts = []
            if domain:
                parts.append(domain)
            if dataset:
                parts.append(dataset)
            search = "/".join(parts) + "/" if parts else ""
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=search):
            for obj in page.get("Contents", []):
                yield obj["Key"]


# ---------------------------------------------------------------------------
# helpers (also used by CLI builder)
# ---------------------------------------------------------------------------

def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 URI: {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3 URI: {uri!r}")
    return bucket, key


def _default_bucket() -> str:  # pragma: no cover
    import os
    env = os.environ.get("ENV", "local")
    return f"ods-dlq-{env}"


def _connect_pg():  # pragma: no cover
    import os

    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5440")),
        dbname=os.environ.get("PG_DB", "ods_dev"),
        user=os.environ.get("PG_USER", "ods"),
        password=os.environ.get("PG_PASSWORD", "ods"),
    )


def _make_producer():  # pragma: no cover
    import os

    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP",
                                                          "localhost:9092")})
```

### `ods_pipeline\ops\runs.py`

```python
"""Replay / rerun subcommand for python -m ods_pipeline.ops.

Two operations:
  - replay --file-id <uuid>   restart all runs that touched a file
  - rerun  --run-id  <uuid>   restart one specific run

Both operations:
  1. Look up the original run + dataset metadata from run_log
  2. Allocate a replay_request_id for operator/audit correlation
  3. Trigger the appropriate Airflow DAG via REST (or print intended action
     in --dry-run)
  4. The DAG creates the actual run rows and links lineage using
     replay_of_run_id from conf. Old run + its evidence are NEVER mutated.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Any


def register(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="runs_cmd", required=True)
    p_replay = sub.add_parser("replay", help="restart runs for a file")
    p_replay.add_argument("--file-id", required=True)
    p_replay.add_argument("--dry-run", action="store_true")

    p_rerun = sub.add_parser("rerun", help="restart a specific run")
    p_rerun.add_argument("--run-id", required=True)
    p_rerun.add_argument("--dry-run", action="store_true")


def dispatch(args) -> int:  # pragma: no cover â€” thin glue
    pg = _connect_pg()
    airflow = _make_airflow_client()
    ops = _RunsOps(pg_conn=pg, airflow=airflow)
    if args.runs_cmd == "replay":
        out = ops.replay_file(args.file_id, dry_run=args.dry_run)
    elif args.runs_cmd == "rerun":
        out = ops.rerun(args.run_id, dry_run=args.dry_run)
    else:
        print(f"unknown subcommand {args.runs_cmd!r}", file=sys.stderr)
        return 2
    print(json.dumps(out, indent=2, default=str))
    return 0


class _RunsOps:
    """Stateless wrapper. Postgres + Airflow client injected for testability."""

    def __init__(self, *, pg_conn, airflow):
        self._pg = pg_conn
        self._airflow = airflow

    # ---------------------------------------------------------------
    # rerun: keyed by run_id
    # ---------------------------------------------------------------
    def rerun(self, run_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        original = self._lookup_run(run_id)
        if original is None:
            raise LookupError(f"run_id not found: {run_id}")
        return self._launch_replay(original, source="rerun", dry_run=dry_run)

    # ---------------------------------------------------------------
    # replay: keyed by file_id (one or more original runs)
    # ---------------------------------------------------------------
    def replay_file(self, file_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        original = self._lookup_replay_candidate_by_file(file_id)
        if not original:
            raise LookupError(f"no runs found for file_id={file_id}")
        return {
            "file_id": file_id,
            "replay": self._launch_replay(original, source="replay", dry_run=dry_run),
        }

    # ---------------------------------------------------------------
    # internals
    # ---------------------------------------------------------------
    def _lookup_run(self, run_id: str):
        with self._pg.cursor() as cur:
            cur.execute(
                """
                SELECT run_id::text, pipeline_type, domain, dataset,
                       business_date::text, file_id::text, kafka_topic
                  FROM pipeline.run_log WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        keys = ("run_id", "pipeline_type", "domain", "dataset",
                "business_date", "file_id", "kafka_topic")
        return dict(zip(keys, row))

    def _lookup_replay_candidate_by_file(self, file_id: str):
        with self._pg.cursor() as cur:
            cur.execute(
                """
                SELECT run_id::text, pipeline_type, domain, dataset,
                       business_date::text, file_id::text, kafka_topic
                  FROM pipeline.run_log
                 WHERE file_id = %s::uuid
                 ORDER BY CASE pipeline_type
                            WHEN 's3_batch' THEN 0
                            WHEN 'file' THEN 1
                            WHEN 'ingestion' THEN 2
                            WHEN 'publish_raw' THEN 3
                            WHEN 'publish' THEN 4
                            WHEN 'canonicalize' THEN 5
                            ELSE 9
                          END,
                          started_at DESC
                 LIMIT 1
                """,
                (file_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        keys = ("run_id", "pipeline_type", "domain", "dataset",
                "business_date", "file_id", "kafka_topic")
        return dict(zip(keys, row))

    def _launch_replay(self, original, *, source: str,
                       dry_run: bool) -> dict[str, Any]:
        replay_request_id = str(uuid.uuid4())
        result = {
            "source": source,
            "replay_request_id": replay_request_id,
            "original_run_id": original["run_id"],
            "pipeline_type": original["pipeline_type"],
            "domain": original["domain"],
            "dataset": original["dataset"],
            "business_date": original["business_date"],
            "file_id": original["file_id"],
            "dry_run": dry_run,
        }
        if dry_run:
            result["status"] = "dry-run"
            return result
        dag_id = _dag_for(original["pipeline_type"])
        self._airflow.trigger_dag(
            dag_id=dag_id,
            conf={"replay_request_id": replay_request_id,
                  "replay_of_run_id": original["run_id"],
                  "file_id": original["file_id"],
                  "domain": original["domain"],
                  "dataset": original["dataset"],
                  "business_date": original["business_date"]},
        )
        result["status"] = "triggered"
        result["dag_id"] = dag_id
        return result


def _dag_for(pipeline_type: str) -> str:
    return {
        "file":          "dag_ingest",
        "message_api":   "dag_event_api",
        "dlq_replay":    "dag_dlq_replay",
    }.get(pipeline_type, "dag_ingest")


# ---------------------------------------------------------------------------
# CLI helpers (production-only paths)
# ---------------------------------------------------------------------------

def _connect_pg():  # pragma: no cover
    import os

    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5440")),
        dbname=os.environ.get("PG_DB", "ods_dev"),
        user=os.environ.get("PG_USER", "ods"),
        password=os.environ.get("PG_PASSWORD", "ods"),
    )


def _make_airflow_client():  # pragma: no cover
    import os

    import requests

    base = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
    auth = (
        os.environ.get("AIRFLOW_USER", "airflow"),
        os.environ.get("AIRFLOW_PASSWORD", "airflow"),
    )

    class _AirflowClient:
        def trigger_dag(self, *, dag_id: str, conf: dict[str, Any]) -> dict:
            resp = requests.post(
                f"{base}/api/v1/dags/{dag_id}/dagRuns",
                json={"conf": conf},
                auth=auth,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()

    return _AirflowClient()
```

### `ods_pipeline\patterns\__init__.py`

```python
"""Ingestion pattern registry (A4 / T12).

Each pattern type (file, cdc, api, event) has a concrete `IngestionPattern`
declaring its stages, topics, sinks, recon checks and correlation field.
Consumers (DAG factory, Glue entrypoints, event_api) look up patterns
by name from the registry to drive execution.
"""
from ods_pipeline.patterns import api_pull as _api_pull  # noqa: F401 â€” registers patterns
from ods_pipeline.patterns import event as _event  # noqa: F401 â€” registers patterns
from ods_pipeline.patterns import file as _file  # noqa: F401 â€” registers patterns
from ods_pipeline.patterns.base import (
    PATTERNS,
    IngestionPattern,
    get,
    register,
)

__all__ = ["IngestionPattern", "PATTERNS", "get", "register"]
```

### `ods_pipeline\patterns\api_pull.py`

```python
"""API pull ingestion pattern.

Airflow polls an external HTTP API on a schedule, archives the response
as gzipped JSONL to S3, registers the archive in pipeline.file_catalogue,
and triggers the existing dag_ingest pipeline (raw_read -> Kafka publish
-> canonicalize -> JDBC sink). Cursor commit follows a two-phase
pending/committed split so downstream failure does not lose data.

The registration mirrors the file/event patterns: a declarative
IngestionPattern is the single source of truth for stages, topics,
sinks, recon checks and YAML config.
"""
from __future__ import annotations

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns.base import IngestionPattern, register

# Stages emitted by the api_pull control-plane (poller + dag_api_pull).
# Downstream stages (raw_read, schema_validate, ...) are emitted by the
# triggered dag_ingest run and are not duplicated here.
_API_PULL_STAGES = (
    Stage.RAW_POLL,
    Stage.MESSAGE_ARCHIVE,
    Stage.RECON_MESSAGE,
    Stage.FINALISE,
)


API_PULL_DEMO = register(IngestionPattern(
    name="insurance.api_pull_demo",
    pattern_type=PatternType.API,
    stages=_API_PULL_STAGES,
    topics=(
        "ods.insurance.api_pull_demo",
        "ods.insurance.api_pull_demo.canonical",
    ),
    sinks=("jdbc-sink-api-pull-demo",),
    recon_checks=("api_pull_archive_count",),
    yaml_config="patterns/insurance/api_pull_demo.yaml",
))
```

### `ods_pipeline\patterns\base.py`

```python
"""IngestionPattern dataclass + registry."""
from __future__ import annotations

from dataclasses import dataclass

from ods_pipeline.models import PATTERN_CORRELATION_FIELD, PatternType, Stage


@dataclass(frozen=True)
class IngestionPattern:
    """Declarative spec of an ingestion flow.

    Pattern types:
      - ``file``  â€” SFTP/S3 file -> Glue ingestion -> raw Kafka -> canonical Kafka -> sink
      - ``cdc``   â€” DB CDC stream -> raw Kafka -> canonical Kafka -> sink
      - ``api``   â€” synchronous request -> S3 archive + raw Kafka -> canonical -> sink
      - ``event`` â€” async event stream -> raw Kafka -> canonical -> sink

    The ``correlation_field`` is derived from ``pattern_type`` so callers
    cannot accidentally desync them.
    """

    name: str
    pattern_type: str
    stages: tuple[str, ...]
    topics: tuple[str, ...]
    sinks: tuple[str, ...]
    recon_checks: tuple[str, ...] = ()
    yaml_config: str | None = None

    def __post_init__(self):
        if self.pattern_type not in PatternType.ALL:
            raise ValueError(
                f"unknown pattern_type {self.pattern_type!r}; "
                f"must be one of {sorted(PatternType.ALL)}"
            )
        unknown_stages = set(self.stages) - Stage.all_values()
        if unknown_stages:
            raise ValueError(f"unknown stages: {sorted(unknown_stages)}")

    @property
    def correlation_field(self) -> str:
        """Canonical envelope correlation field for this pattern type."""
        return PATTERN_CORRELATION_FIELD[self.pattern_type]


# Module-level registry. Patterns register themselves at import time.
PATTERNS: dict[str, IngestionPattern] = {}


def register(pattern: IngestionPattern) -> IngestionPattern:
    """Register ``pattern`` in the global registry. Re-register replaces."""
    PATTERNS[pattern.name] = pattern
    return pattern


def get(name: str) -> IngestionPattern:
    """Return pattern by name. Raises KeyError if unknown."""
    if name not in PATTERNS:
        raise KeyError(
            f"no pattern registered named {name!r}; "
            f"known: {sorted(PATTERNS)}"
        )
    return PATTERNS[name]
```

### `ods_pipeline\patterns\event.py`

```python
"""Event-pattern demo registration (T16).

First non-file IngestionPattern. Proves the IngestionPattern abstraction
generalises beyond the file pattern by composing the same control-plane
primitives (runs, stages, lineage, recon) for an HTTP-trigger source.
"""
from __future__ import annotations

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns.base import IngestionPattern, register

EVENT_DEMO = register(IngestionPattern(
    name="insurance.event_demo",
    pattern_type=PatternType.EVENT,
    stages=(
        Stage.MESSAGE_RECEIVE,
        Stage.MESSAGE_VALIDATE,
        Stage.MESSAGE_ARCHIVE,
        Stage.KAFKA_PUBLISH,
        Stage.CANONICAL_TRANSFORM,
        Stage.RECON_MESSAGE,
        Stage.SINK_PG_WAIT,
        Stage.FINALISE,
    ),
    topics=("ods.insurance.events", "ods.insurance.events.canonical"),
    sinks=("jdbc-sink-events",),
    recon_checks=("recon_message_count",),
    yaml_config="patterns/insurance/event_demo.yaml",
))
```

### `ods_pipeline\patterns\file.py`

```python
"""File-based ingestion patterns (risk, policies).

Both flow: SFTP -> S3 raw -> Glue ingestion (CSV -> Parquet, DQ) ->
publish raw Kafka -> canonicalize -> publish canonical Kafka -> JDBC
sink(s) -> recon.

These declarations replace the implicit "Glue + DAG knows what to do"
convention with explicit metadata that DAG factory + Glue entrypoint
can consume.
"""
from __future__ import annotations

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns.base import IngestionPattern, register

_FILE_STAGES = (
    Stage.RAW_READ,
    Stage.SCHEMA_VALIDATE,
    Stage.DQ_CHECK,
    Stage.CURATED_WRITE,
    Stage.CURATED_READ,
    Stage.KAFKA_PUBLISH,
    Stage.KAFKA_CONSUME,
    Stage.CANONICAL_TRANSFORM,
    Stage.RECON_T0,
    Stage.RECON_T1,
    Stage.SINK_PG_WAIT,
    Stage.FINALISE,
)


POLICIES = register(IngestionPattern(
    name="insurance.policies",
    pattern_type=PatternType.FILE,
    stages=_FILE_STAGES,
    topics=("ods.insurance.policies",),
    sinks=("jdbc-sink-policies", "jdbc-sink-policy-history"),
    recon_checks=("t0_publish_count", "t1_canonical_count", "dual_sink_parity"),
    yaml_config="patterns/insurance/policies.yaml",
))


RISK = register(IngestionPattern(
    name="insurance.risk",
    pattern_type=PatternType.FILE,
    stages=_FILE_STAGES,
    topics=("ods.insurance.risk", "ods.insurance.risk-canonical"),
    sinks=("jdbc-sink-risk",),
    recon_checks=("t0_publish_count", "t1_canonical_count"),
    yaml_config="patterns/insurance/risk.yaml",
))
```

### `ods_pipeline\publish.py`

```python
"""Reusable Kafka transactional-publish lifecycle (B5/B6).

Extracted from ``glue/jobs/ods_s3_publish.py`` so the same lifecycle can be
exercised by:

* ``ods_s3_publish.py``  â€” current production caller.
* ``ods_pipeline/messages.py`` callers â€” once T6 / T16 land message-pattern
  publishing.
* The unit test ``tests/unit/test_publish_transactional.py`` â€” exercises the
  exact production code path, so reverting the Glue caller would fail the
  test suite (no tautology â€” the helper is load-bearing).

Design contract:

* The caller owns ``producer = Producer(config)`` and ``producer.close()``
  (typically inside its own ``finally``). This helper does NOT close the
  producer because the caller may want to reuse the same Producer across
  multiple invocations or wrap close ordering with other cleanup.
* The caller passes a fresh ``OffsetTracker``; on success the tracker is
  populated with broker-acknowledged ``(partition, offset)`` per delivered
  message; ``tracker.delivered_count`` is the recon source-of-truth.
* The caller passes ``payloads`` â€” any iterable; each item is forwarded to
  ``on_send(producer, payload, on_delivery=tracker.on_delivery)``. The
  ``on_send`` callback is the integration point: production callers serialise
  Avro and call ``producer.produce(...)``; tests use a one-line lambda.

Lifecycle (single transaction):
    init_transactions  â†’  begin_transaction
                          â†’  for payload: on_send(...)
                          â†’  flush
                          â†’  if tracker.errors: raise
                          â†’  commit_transaction
    Any exception in the produce loop or in flush  â†’  abort_transaction
                                                    â†’  raise
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from ods_pipeline.offsets import OffsetTracker


def _default_on_send(producer, payload, *, on_delivery):
    """Default per-payload sender used when caller doesn't supply one.

    Expects ``payload`` to be a mapping with ``topic``, ``key``, ``value`` â€”
    matches the lightweight test fixture shape. Production callers ALWAYS
    pass their own ``on_send`` because they need to serialise Avro and
    compute message keys.
    """
    producer.produce(
        topic=payload["topic"],
        key=payload.get("key"),
        value=payload.get("value"),
        on_delivery=on_delivery,
    )


def publish_with_transaction(
    producer,
    payloads: Iterable[Any],
    tracker: OffsetTracker,
    *,
    on_send: Callable[..., None] | None = None,
) -> None:
    """Run one transactional publish: init â†’ begin â†’ produce* â†’ commit/abort.

    Caller owns ``producer.close()`` (typically in their own ``finally``).
    Raises whatever the produce loop raises, after first calling
    ``producer.abort_transaction()``. If ``tracker.errors`` is non-empty
    after ``producer.flush()``, raises ``RuntimeError`` describing the first
    failure (and aborts).
    """
    sender = on_send or _default_on_send
    producer.init_transactions()
    producer.begin_transaction()
    try:
        for payload in payloads:
            sender(producer, payload, on_delivery=tracker.on_delivery)
        producer.flush()
        if tracker.errors:
            raise RuntimeError(
                f"{len(tracker.errors)} kafka delivery failures; "
                f"first: {tracker.errors[0]}"
            )
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
        raise
```

### `ods_pipeline\reconciliation.py`

```python
"""pipeline.reconciliation_log operations."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from psycopg2 import sql

import ods_ingestion_control as control


def write_check(
    conn,
    *,
    check_type: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    source_count: int | None = None,
    kafka_count: int | None = None,
    postgres_count: int | None = None,
    status: str,
    detail: str | None = None,
    window_start=None,
    window_end=None,
    commit: bool = True,
) -> None:
    """Insert a row into ``pipeline.reconciliation_log``.

    Computes ``discrepancy_count`` and ``discrepancy_pct`` automatically:
      * If *source_count* and *kafka_count* both supplied:
        ``discrepancy = kafka_count - source_count``
      * If *kafka_count* and *postgres_count* both supplied:
        ``discrepancy = postgres_count - kafka_count``

    ``commit``: when True (default), the helper commits its own transaction.
    When False, the caller owns the surrounding tx (used by atomic
    ``record_result`` flow â€” B4).
    """
    discrepancy: int | None = None
    if source_count is not None and kafka_count is not None:
        discrepancy = (kafka_count or 0) - (source_count or 0)
    elif kafka_count is not None and postgres_count is not None:
        discrepancy = (postgres_count or 0) - (kafka_count or 0)

    pct: float | None = None
    if discrepancy is not None and source_count:
        pct = round(100.0 * discrepancy / source_count, 4)

    control.write_reconciliation_check(
        conn,
        check_type=check_type,
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=None if business_date is None else str(business_date),
        source_count=source_count,
        kafka_count=kafka_count,
        postgres_count=postgres_count,
        status=status,
        detail=detail,
        window_start=window_start,
        window_end=window_end,
        commit=commit,
    )


# Default dual-sink table pairs per dataset.
# Each entry: dataset -> (current_table, history_table, run_id_column).
DUAL_SINK_TABLES: dict[str, tuple[str, str, str]] = {
    "policies": ("ods.insurance_policy",
                 "ods.insurance_policy_history",
                 "_ods_run_id"),
}


# dataset -> (current_table, history_table, key_columns, compare_columns, order_column)
CURRENT_HISTORY_TABLES: dict[str, tuple[str, str, tuple[str, ...], tuple[str, ...], str]] = {
    "policies": (
        "ods.insurance_policy",
        "ods.insurance_policy_history",
        ("policy_id",),
        ("status", "premium", "effective_date", "_ods_file_id", "_ods_run_id"),
        "_ods_ingested_at",
    ),
}


def _split_table_ref(table_ref: str) -> tuple[str, str]:
    schema, sep, table = table_ref.partition(".")
    if not sep or not schema.isidentifier() or not table.isidentifier():
        raise ValueError(f"table reference must be schema.table: {table_ref!r}")
    return schema, table


def _safe_column(column: str) -> str:
    if not column.isidentifier():
        raise ValueError(f"illegal column name: {column!r}")
    return column


def check_dual_sink_parity(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    pairs: Mapping[str, tuple[str, str, str]] | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """Compare row counts between current and history sinks for ``run_id``.

    Returns the reconciliation summary and writes a corresponding row to
    ``pipeline.reconciliation_log`` with ``check_type='dual_sink_parity'``.

    History sink lag is the most common source of divergence between the
    upsert (current) and append (history) JDBC sinks reading from the same
    canonical topic. If the history connector is paused, restart-lagging,
    or schema-evolution-blocked, the current state can advance without a
    matching history row, leaving the audit trail incomplete.

    Status:
      - ``ok``        â€” counts match (delta == 0)
      - ``pending``   â€” at least one sink has no rows yet (likely lag)
      - ``failed``    â€” both sinks have rows but counts diverge
    """
    pairs_to_check = pairs or DUAL_SINK_TABLES
    table_pair = pairs_to_check.get(dataset)
    if not table_pair:
        return {"status": "skipped", "reason": f"no dual-sink pair registered for {dataset}"}
    current_table, history_table, run_col = table_pair
    current_schema, current_name = _split_table_ref(current_table)
    history_schema, history_name = _split_table_ref(history_table)
    run_col = _safe_column(run_col)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} = %s").format(
                sql.Identifier(current_schema),
                sql.Identifier(current_name),
                sql.Identifier(run_col),
            ),
            (run_id,),
        )
        current_count = int(cur.fetchone()[0])
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} = %s").format(
                sql.Identifier(history_schema),
                sql.Identifier(history_name),
                sql.Identifier(run_col),
            ),
            (run_id,),
        )
        history_count = int(cur.fetchone()[0])

    delta = history_count - current_count
    if current_count == 0 or history_count == 0:
        status = "pending"
        detail = (f"current={current_count}, history={history_count}; "
                  "one or both sinks empty â€” likely consumer lag")
    elif delta == 0:
        status = "ok"
        detail = f"current={current_count}, history={history_count}"
    else:
        status = "failed"
        detail = (f"current={current_count}, history={history_count}, "
                  f"delta={delta}")

    write_check(
        conn,
        check_type="dual_sink_parity",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=current_count,
        postgres_count=history_count,
        status=status,
        detail=detail,
        commit=commit,
    )
    return {
        "status": status,
        "current_count": current_count,
        "history_count": history_count,
        "delta": delta,
        "detail": detail,
    }


def compare_history_vs_current(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    tables: Mapping[str, tuple[str, str, tuple[str, ...], tuple[str, ...], str]] | None = None,
    sample_limit: int = 20,
    commit: bool = True,
) -> dict[str, Any]:
    """Compare current-state rows to latest matching history rows.

    Count parity can prove both sinks received the same number of rows, but it
    cannot prove the current table holds the same values as the append history
    table's latest row per business key. This check builds a latest-history
    view with ``row_number()`` and compares configured columns using
    ``IS DISTINCT FROM`` so NULLs are handled correctly.
    """
    registry = tables or CURRENT_HISTORY_TABLES
    table_config = registry.get(dataset)
    if not table_config:
        return {"status": "skipped", "reason": f"no current/history pair registered for {dataset}"}

    current_table, history_table, key_columns, compare_columns, order_column = table_config
    current_schema, current_name = _split_table_ref(current_table)
    history_schema, history_name = _split_table_ref(history_table)
    key_columns = tuple(_safe_column(column) for column in key_columns)
    compare_columns = tuple(_safe_column(column) for column in compare_columns)
    order_column = _safe_column(order_column)
    if not key_columns:
        raise ValueError("at least one key column is required")
    if not compare_columns:
        raise ValueError("at least one compare column is required")

    key_join = sql.SQL(" AND ").join(
        sql.SQL("c.{col} = h.{col}").format(col=sql.Identifier(column))
        for column in key_columns
    )
    diff_predicate = sql.SQL(" OR ").join(
        sql.SQL("c.{col} IS DISTINCT FROM h.{col}").format(col=sql.Identifier(column))
        for column in compare_columns
    )
    key_json = sql.SQL(", ").join(
        sql.SQL("{literal}, c.{col}").format(
            literal=sql.Literal(column),
            col=sql.Identifier(column),
        )
        for column in key_columns
    )
    value_json = sql.SQL(", ").join(
        sql.SQL("{literal}, jsonb_build_object('current', c.{col}, 'history', h.{col})").format(
            literal=sql.Literal(column),
            col=sql.Identifier(column),
        )
        for column in compare_columns
    )
    partition_by = sql.SQL(", ").join(sql.Identifier(column) for column in key_columns)
    history_filters = [sql.SQL("{} = %s").format(sql.Identifier("_ods_business_date"))]
    current_filters = [sql.SQL("{} = %s").format(sql.Identifier("_ods_business_date"))]
    params: list[Any] = [business_date, business_date, sample_limit]
    if business_date is None:
        history_filters = [sql.SQL("%s IS NULL")]
        current_filters = [sql.SQL("%s IS NULL")]

    query = sql.SQL(
        """
        WITH latest_history AS (
            SELECT *
              FROM (
                    SELECT h.*,
                           row_number() OVER (
                               PARTITION BY {partition_by}
                               ORDER BY {order_col} DESC NULLS LAST
                           ) AS rn
                      FROM {history_schema}.{history_table} h
                     WHERE {history_where}
                   ) ranked
             WHERE rn = 1
        ),
        current_rows AS (
            SELECT *
              FROM {current_schema}.{current_table} c
             WHERE {current_where}
        ),
        mismatches AS (
            SELECT jsonb_build_object({key_json}) AS key,
                   jsonb_build_object({value_json}) AS differences
              FROM current_rows c
              JOIN latest_history h ON {key_join}
             WHERE {diff_predicate}
        ),
        missing_history AS (
            SELECT jsonb_build_object({key_json}) AS key
              FROM current_rows c
              LEFT JOIN latest_history h ON {key_join}
             WHERE h.{first_key} IS NULL
        ),
        counts AS (
            SELECT
                (SELECT COUNT(*) FROM current_rows) AS current_count,
                (SELECT COUNT(*) FROM latest_history) AS history_count,
                (SELECT COUNT(*) FROM mismatches) AS mismatch_count,
                (SELECT COUNT(*) FROM missing_history) AS missing_history_count
        )
        SELECT counts.current_count,
               counts.history_count,
               counts.mismatch_count,
               counts.missing_history_count,
               COALESCE((
                   SELECT jsonb_agg(sample)
                     FROM (
                           SELECT jsonb_build_object(
                                      'type', 'mismatch',
                                      'key', key,
                                      'differences', differences
                                  ) AS sample
                             FROM mismatches
                            LIMIT %s
                          ) s
               ), '[]'::jsonb) ||
               COALESCE((
                   SELECT jsonb_agg(sample)
                     FROM (
                           SELECT jsonb_build_object(
                                      'type', 'missing_history',
                                      'key', key
                                  ) AS sample
                             FROM missing_history
                            LIMIT %s
                          ) s
               ), '[]'::jsonb) AS samples
          FROM counts
        """
    ).format(
        partition_by=partition_by,
        order_col=sql.Identifier(order_column),
        history_schema=sql.Identifier(history_schema),
        history_table=sql.Identifier(history_name),
        current_schema=sql.Identifier(current_schema),
        current_table=sql.Identifier(current_name),
        history_where=sql.SQL(" AND ").join(history_filters),
        current_where=sql.SQL(" AND ").join(current_filters),
        key_join=key_join,
        diff_predicate=diff_predicate,
        key_json=key_json,
        value_json=value_json,
        first_key=sql.Identifier(key_columns[0]),
    )
    params.append(sample_limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        current_count, history_count, mismatch_count, missing_history_count, samples = cur.fetchone()

    issue_count = int(mismatch_count) + int(missing_history_count)
    status = "ok" if issue_count == 0 else "failed"
    detail_payload = {
        "current_count": int(current_count),
        "history_count": int(history_count),
        "mismatch_count": int(mismatch_count),
        "missing_history_count": int(missing_history_count),
        "samples": samples,
    }
    write_check(
        conn,
        check_type="current_history_row_value",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=int(history_count),
        postgres_count=int(current_count),
        status=status,
        detail=json.dumps(detail_payload, default=str, sort_keys=True),
        commit=commit,
    )
    return {"status": status, **detail_payload}
```

### `ods_pipeline\runs.py`

```python
"""pipeline.run_log operations."""
from __future__ import annotations

import ods_ingestion_control as control
from ods_pipeline.models import ALLOWED_RUN_FIELDS


def _normalise(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def start(
    conn,
    *,
    run_id: str,
    pipeline_type: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    file_id: str | None = None,
    kafka_topic: str | None = None,
    config_version_id=None,
    schema_version_id=None,
    parents=None,
    runtime_context=None,
) -> None:
    """Insert a new ``run_log`` row with ``status='running'``.

    Duplicate ``run_id`` is allowed only when the supplied identifying
    metadata matches the existing row.  This preserves idempotency for retries
    while surfacing accidental reuse across different pipeline types/files.
    """
    try:
        control.start_run(
            conn,
            run_id=run_id,
            pipeline_type=pipeline_type,
            domain=domain,
            dataset=dataset,
            business_date=str(business_date) if business_date is not None else None,
            file_id=file_id,
            kafka_topic=kafka_topic,
            config_version_id=config_version_id,
            schema_version_id=schema_version_id,
            parents=parents,
            runtime_context=runtime_context,
        )
    except Exception as exc:
        if "already exists with different metadata" in str(exc):
            raise ValueError(str(exc)) from exc
        raise


def update(conn, run_id: str, *, commit: bool = True, **fields) -> None:
    """Update arbitrary ``run_log`` fields for *run_id*.

    Terminal status (succeeded / failed / partial) automatically sets
    ``ended_at = COALESCE(ended_at, NOW())``.

    Raises ``ValueError`` for unknown field names.

    ``commit``: when True (default), the helper commits its own transaction â€”
    behaviour preserved for all existing callers.  When False, the caller is
    responsible for the surrounding transaction (used by
    ``ods_pipeline.messages.record_result`` for atomic multi-write flows).
    """
    if not fields:
        return
    invalid = set(fields) - ALLOWED_RUN_FIELDS
    if invalid:
        raise ValueError(f"Unknown run_log fields: {sorted(invalid)}")
    cols = list(fields.keys())
    # Defence in depth: every key MUST be a bare identifier *and* whitelisted.
    # ALLOWED_RUN_FIELDS is the authoritative gate; isidentifier() is a belt-
    # and-braces guard against future whitelist additions that contain unsafe
    # characters by mistake.
    for c in cols:
        if not (isinstance(c, str) and c.isidentifier() and c in ALLOWED_RUN_FIELDS):
            raise ValueError(f"Illegal run_log field name: {c!r}")
    control.patch_run(conn, run_id=run_id, fields=fields, commit=commit)


def finish(
    conn,
    run_id: str,
    status: str,
    error_summary: str | None = None,
) -> None:
    """Convenience wrapper: mark a run terminal and optionally record error."""
    kw: dict = {"status": status}
    if error_summary is not None:
        kw["error_summary"] = error_summary
    update(conn, run_id, **kw)


class LineageInvariantError(RuntimeError):
    """Raised by ``finalise`` when run state violates the lineage contract."""


def finalise(conn, run_id: str, *, commit: bool = True) -> None:
    """Validate lineage closure invariants before marking a run succeeded.

    Asserts:
      1. If ``record_count_published > 0``, at least one ``lineage_edge`` row
         exists with ``child_run_id = run_id`` (no orphan published runs).
      2. No non-terminal ``run_stage_log`` rows exist for ``run_id`` â€” every
         opened stage must have been closed.

    On violation, marks the run ``failed`` with an explanatory
    ``error_summary`` and raises :class:`LineageInvariantError`.

    Closes architectural risk A3.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(record_count_published, 0)
              FROM pipeline.run_log
             WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise LineageInvariantError(f"run_id {run_id} not found")
        published = int(row[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*) FROM pipeline.lineage_edge
             WHERE child_run_id = %s
            """,
            (run_id,),
        )
        edges = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT stage, attempt_number FROM pipeline.run_stage_log
             WHERE run_id = %s
               AND event_type NOT IN ('stage_completed','stage_failed',
                                      'stage_skipped','stage_warned')
            """,
            (run_id,),
        )
        open_stages = cur.fetchall()

    failures: list[str] = []
    if published > 0 and edges == 0:
        failures.append(
            f"published={published} but no lineage_edge rows; orphaned run"
        )
    if open_stages:
        names = ", ".join(f"{s}#{a}" for s, a in open_stages)
        failures.append(f"non-terminal stages remain: {names}")

    if failures:
        summary = "lineage invariant violated: " + " | ".join(failures)
        update(conn, run_id, status="failed",
               error_summary=summary, commit=commit)
        raise LineageInvariantError(summary)
```

### `ods_pipeline\stages.py`

```python
"""pipeline.run_stage_log operations.

Stateless control-plane contract
--------------------------------

Every helper here defaults to ``commit=True`` â€” each stage row is its own
durable checkpoint. Long pipelines therefore expose live progress on the
operator dashboard and survive worker death without leaving an open
transaction holding row locks.

The trade-off is that the application code must explicitly write a
``stage_failed`` row in its exception handler. The :func:`stage_scope`
context manager below packages that discipline so callers can't forget:

    >>> with stage_scope(conn, run_id=run_id, stage=Stage.MESSAGE_RECEIVE,
    ...                  record_count_in=1):
    ...     do_work()                    # success â†’ stage_completed
    ...                                  # raise   â†’ stage_failed

If even the failure write fails (DB flapping, connection dead) the
context manager swallows the secondary error so the original work
exception still propagates; a heartbeat-staleness janitor closes any
``status='running'`` rows it leaves behind.
"""
from __future__ import annotations

from contextlib import contextmanager

import ods_ingestion_control as control
from ods_pipeline.models import StageEvent


def write(
    conn,
    *,
    run_id: str,
    stage: str,
    status: str,
    event_type: str | None = None,
    attempt_number: int = 1,
    input_ref: str | None = None,
    output_ref: str | None = None,
    record_count_in: int | None = None,
    record_count_out: int | None = None,
    metrics: dict | None = None,
    error: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> None:
    """Append one row to ``pipeline.run_stage_log``.

    ``started_at`` is always set to ``NOW()``.
    ``ended_at`` is set to ``NOW()`` only for terminal events
    (completed / failed / skipped / warned).  For ``stage_started`` /
    ``status='running'`` it is left NULL so the open interval is
    queryable.

    Idempotency (B4, migration 19): inserting a ``stage_started`` row that
    duplicates an existing open attempt is a no-op (partial unique index
    ``run_stage_log_started_unique``).  Terminal events remain append-only.

    ``commit``: when True (default), the helper commits its own transaction.
    When False, the caller owns the surrounding tx (used by atomic
    ``record_result`` flow).
    """
    control.write_stage_event(
        conn,
        run_id=run_id,
        stage=stage,
        status=status,
        event_type=event_type,
        attempt_number=attempt_number,
        input_ref=input_ref,
        output_ref=output_ref,
        record_count_in=record_count_in,
        record_count_out=record_count_out,
        metrics=metrics,
        error=error,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
        commit=commit,
    )


def next_attempt_number(conn, *, run_id: str, stage: str) -> int:
    """Return the next free ``attempt_number`` for ``(run_id, stage)``.

    Stateless control-plane retries leave durable stage rows from the
    failed attempts; the unique index on ``(run_id, stage, event_type,
    attempt_number)`` requires the new attempt to use ``MAX+1`` rather
    than always 1.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(attempt_number), 0) + 1
              FROM pipeline.run_stage_log
             WHERE run_id=%s AND stage=%s
            """,
            (run_id, stage),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 1


def start(
    conn,
    *,
    run_id: str,
    stage: str,
    attempt_number: int | None = None,
    **kwargs,
) -> int:
    """Open a stage attempt with ``status='running'`` and ``ended_at=NULL``.

    When ``attempt_number`` is omitted, :func:`next_attempt_number` is
    consulted so retries land on a fresh row instead of colliding on the
    unique index. Returns the attempt number used so the caller can pass
    it to a later :func:`finish` for the same attempt.
    """
    return control.start_stage(
        conn,
        run_id=run_id,
        stage=stage,
        attempt_number=attempt_number,
        input_ref=kwargs.get("input_ref"),
        record_count_in=kwargs.get("record_count_in"),
        metrics=kwargs.get("metrics"),
        airflow_dag_id=kwargs.get("airflow_dag_id"),
        airflow_run_id=kwargs.get("airflow_run_id"),
        spark_app_id=kwargs.get("spark_app_id"),
    )


def finish(
    conn,
    *,
    run_id: str,
    stage: str,
    status: str,
    event_type: str | None = None,
    attempt_number: int = 1,
    input_ref: str | None = None,
    output_ref: str | None = None,
    record_count_in: int | None = None,
    record_count_out: int | None = None,
    metrics: dict | None = None,
    error: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> None:
    """Close the latest open stage row, falling back to append if none exists.

    Concurrency contract (B3): the open-row claim uses
    ``SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`` and the subsequent
    UPDATE-or-INSERT runs in the same transaction (single ``conn.commit()``
    at the end).  Two concurrent ``finish`` calls for the same
    ``(run_id, stage, attempt_number)`` therefore resolve as: the lock
    holder UPDATEs the open row, the loser sees no claimable row and
    INSERTs a fresh terminal row â€” never a silent no-op or double-update.
    """
    control.finish_stage(
        conn,
        run_id=run_id,
        stage=stage,
        status=status,
        event_type=event_type,
        attempt_number=attempt_number,
        input_ref=input_ref,
        output_ref=output_ref,
        record_count_in=record_count_in,
        record_count_out=record_count_out,
        metrics=metrics,
        error=error,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
        commit=commit,
    )


@contextmanager
def stage_scope(
    conn,
    *,
    run_id: str,
    stage: str,
    record_count_in: int | None = None,
    input_ref: str | None = None,
    metrics: dict | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    truncate_error: int = 500,
):
    """Context manager that opens a stage on entry and closes it on exit.

    Stateless-control-plane contract:

    * On entry: writes ``stage_started`` (with auto-incremented
      ``attempt_number``) and commits. Live on the dashboard immediately.
    * On clean exit: writes ``stage_completed``. The caller may yield a
      result dict via ``ctx.set_result(...)`` â€” its keys merge into the
      finish call so ``output_ref``, ``record_count_out``, etc. land on
      the same row.
    * On ``s.skip(reason)``: clean exit, but writes ``stage_skipped``
      instead of ``stage_completed``. Use when a stage legitimately ran
      but had nothing to do (api_pull saw no changes, file_pipeline saw
      an empty file). ``reason`` is folded into the row's ``metrics`` so
      the dashboard can surface why.
    * On exception: writes ``stage_failed`` with the exception message
      (truncated to ``truncate_error`` chars) and re-raises the original.
      If the failure write itself fails, swallow that secondary error â€”
      the heartbeat janitor will close the row.

    Usage:

        with stage_scope(conn, run_id=rid, stage=Stage.MESSAGE_RECEIVE) as s:
            count = do_work()
            if count == 0:
                s.skip("no_changes")
            else:
                s.set_result(record_count_out=count, output_ref="s3://...")
    """
    attempt = start(
        conn,
        run_id=run_id,
        stage=stage,
        record_count_in=record_count_in,
        input_ref=input_ref,
        metrics=metrics,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
    )
    box: dict = {
        "record_count_out": None,
        "output_ref": None,
        "metrics": None,
    }
    state: dict = {
        "outcome": "succeeded",   # succeeded | skipped | warned
        "reason": None,           # narrative for skipped / warned
    }

    class _Scope:
        attempt_number = attempt

        def set_result(self, **fields) -> None:
            for key, value in fields.items():
                if key not in box:
                    raise KeyError(
                        f"stage_scope only accepts {sorted(box)}; got {key!r}"
                    )
                box[key] = value

        def skip(self, reason: str | None = None) -> None:
            """Mark the stage as skipped on clean exit.

            Idempotent: a second ``skip()`` overwrites the reason. A
            subsequent exception still wins (skip is for clean exits
            only).
            """
            state["outcome"] = "skipped"
            state["reason"] = reason

        def warn(self, reason: str | None = None) -> None:
            """Mark the stage as warned on clean exit.

            Use when work completed but produced soft-failure signals
            (DQ rules fired, partial DLQ writes, schema drift below the
            blocking threshold). Distinct from ``skip`` (which means
            "ran but had nothing to do") and ``raise`` (which means
            "failed hard"). ``reason`` lands on the stage row's
            ``error`` column so the dashboard can surface it next to
            other failure signals.
            """
            state["outcome"] = "warned"
            state["reason"] = reason

    try:
        yield _Scope()
    except BaseException as exc:
        # BaseException catches SystemExit / KeyboardInterrupt â€” those
        # also leave the stage row stuck if we don't close it.
        try:
            finish(
                conn,
                run_id=run_id,
                stage=stage,
                status="failed",
                event_type=StageEvent.FAILED,
                attempt_number=attempt,
                record_count_in=record_count_in,
                record_count_out=box["record_count_out"],
                output_ref=box["output_ref"],
                metrics=box["metrics"],
                error=str(exc)[:truncate_error] if str(exc) else type(exc).__name__,
                airflow_dag_id=airflow_dag_id,
                airflow_run_id=airflow_run_id,
                spark_app_id=spark_app_id,
            )
        except Exception:
            # Last-ditch â€” heartbeat janitor catches any orphaned row.
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    else:
        # Map clean-exit outcomes to status / event_type. ``skip`` /
        # ``warn`` reasons fold into the appropriate column (metrics
        # for skip â€” non-failure signal; error for warn â€” failure
        # signal that wasn't blocking).
        finish_metrics = box["metrics"]
        finish_error: str | None = None
        outcome = state["outcome"]
        if outcome == "skipped":
            if state["reason"] is not None:
                finish_metrics = dict(finish_metrics or {})
                finish_metrics.setdefault("skip_reason", state["reason"])
            finish_status = "skipped"
            finish_event = StageEvent.SKIPPED
        elif outcome == "warned":
            finish_status = "warned"
            finish_event = StageEvent.WARNED
            if state["reason"] is not None:
                finish_error = state["reason"]
        else:
            finish_status = "succeeded"
            finish_event = StageEvent.COMPLETED
        finish(
            conn,
            run_id=run_id,
            stage=stage,
            status=finish_status,
            event_type=finish_event,
            attempt_number=attempt,
            record_count_in=record_count_in,
            record_count_out=box["record_count_out"],
            output_ref=box["output_ref"],
            metrics=finish_metrics,
            error=finish_error,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
            spark_app_id=spark_app_id,
        )
```
