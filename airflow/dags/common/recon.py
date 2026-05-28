from __future__ import annotations

import os
import sys
from dataclasses import dataclass

# Ensure ods_pipeline is importable both locally and in Airflow.
_COMMON_DIR = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_COMMON_DIR, "..", "..")),
    os.path.abspath(os.path.join(_COMMON_DIR, "..", "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import ods_pipeline


@dataclass
class T0Result:
    passed: bool
    discrepancy: int
    source_count: int
    accounted_count: int

def t0_check_publish(conn, *, run_id, domain, dataset, business_date,
                     source_count, kafka_offset_start, kafka_offset_end):
    accounted_count = (kafka_offset_end or 0) - (kafka_offset_start or 0)
    discrepancy = accounted_count - source_count
    passed = discrepancy == 0
    ods_pipeline.reconciliation.write_check(
        conn,
        check_type='t0_publish_count',
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=source_count,
        accounted_count=accounted_count,
        status='ok' if passed else 'failed',
        detail=None if passed else f'discrepancy={discrepancy}',
    )
    return T0Result(passed, discrepancy, source_count, accounted_count)
