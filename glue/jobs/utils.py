# glue/jobs/utils.py
"""Public surface for glue jobs.

Internals split into focused modules (R11):

- ``utils_bootstrap`` — sys.path setup so ``ods_pipeline`` is importable.
- ``utils_data``     — pure data helpers (``extract_business_date``, ``generate_message_key``).
- ``utils_config``   — dataset config loader (``load_dataset_config``).
- ``utils_state``    — file-state I/O (``set_file_state``, ``get_file_state``).
- ``utils_runs``     — run-header / stage-row writes
  (``upsert_run_header``, ``update_run_fields``, ``write_stage_row``).
- ``utils_jobs``     — glue_job_log writes (``write_job_log``).

This shim re-exports each function so existing callers continue to use
``from utils import ...``. The spark-submit ``--py-files`` arg lists each
sibling module so they ship to workers alongside ``utils.py``.
"""
import utils_bootstrap  # noqa: F401  side-effect: extends sys.path
from utils_config import load_dataset_config
from utils_data import extract_business_date, generate_message_key
from utils_jobs import write_job_log
from utils_runs import update_run_fields, upsert_run_header, write_stage_row
from utils_state import get_file_state, set_file_state

__all__ = [
    "extract_business_date",
    "generate_message_key",
    "load_dataset_config",
    "write_job_log",
    "set_file_state",
    "get_file_state",
    "upsert_run_header",
    "update_run_fields",
    "write_stage_row",
]
