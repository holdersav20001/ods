"""Pre-sync validation of dataset_config YAML.

Catches impossible combinations BEFORE they reach
``pipeline.dataset_config`` so:

  - operators see the error in the dag_config_sync output, not in a
    failed run six hours later;
  - ``yaml_loader.sync_to_db`` does not need to repeat the rules
    inline — the validator module owns the contract;
  - new combos can be added by extending one function with one rule
    + one unit test, no SQL changes.

Two functions:

  - ``validate_dataset_config(cfg)``  — pure-Python single-row checks.
    No I/O. Reusable from a future ``odscli config validate``.
  - ``check_no_filename_pattern_overlap(cfg, peers)`` — given a row
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
    # Probe-string overlap with self is allowed (one filename matches
    # only the dataset that owns it); cross-dataset overlap is checked
    # by ``check_no_filename_pattern_overlap`` when peers are known.

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

    Pure heuristic — substitutes named groups with sensible defaults
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
    that accept the same filename → same physical byte stream
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
