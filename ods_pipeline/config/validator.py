"""Pre-sync validation of dataset_config YAML.

Catches impossible combinations BEFORE they reach
``pipeline.dataset_config`` so:

  - operators see the error in the dag_config_sync output, not in a
    failed run six hours later;
  - ``yaml_loader.sync_to_db`` does not need to repeat the rules
    inline — the validator module owns the contract;
  - new combos can be added by extending one function with one rule
    + one unit test, no SQL changes.

The validator is data-only and pure-Python — no psycopg2, no I/O —
so unit tests run fast and downstream tooling (a future
``odscli config validate`` command, IDE pre-commit) can reuse it.
"""
from __future__ import annotations

from typing import Any, Mapping


class DatasetConfigError(ValueError):
    """Raised when a dataset_config row violates a structural invariant."""


_ALLOWED_DELIVERIES = {"file_pipeline", "direct_kafka", "direct_postgres"}
_ALLOWED_SOURCE_TYPES = {"s3_batch", "api_pull", "cdc", "event"}
_ALLOWED_WRITE_MODES = {"upsert", "append", "replace"}


def _qualified(cfg: Mapping[str, Any]) -> str:
    return f"{cfg.get('domain', '<no-domain>')}/{cfg.get('dataset', '<no-dataset>')}"


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

    # Canonicalize → transform_yaml_path requirement, except direct_postgres
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

    # api_pull source block — runtime requirements that the runner / poller
    # would otherwise discover only when called.
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
