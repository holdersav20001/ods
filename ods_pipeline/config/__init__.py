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
    validate_dataset_config,
)

__all__ = ["DatasetConfigError", "validate_dataset_config"]
