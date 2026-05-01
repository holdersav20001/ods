from __future__ import annotations
import os, sys
from dataclasses import dataclass

# Ensure repo root is importable so ods_pipeline package is found
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import ods_pipeline

@dataclass
class T0Result:
    passed: bool
    discrepancy: int
    source_count: int
    kafka_count: int

def t0_check_publish(conn, *, run_id, domain, dataset, business_date,
                     source_count, kafka_offset_start, kafka_offset_end):
    kafka_count = (kafka_offset_end or 0) - (kafka_offset_start or 0)
    discrepancy = kafka_count - source_count
    passed = discrepancy == 0
    ods_pipeline.reconciliation.write_check(
        conn,
        check_type='t0_publish_count',
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=source_count,
        kafka_count=kafka_count,
        status='ok' if passed else 'failed',
        detail=None if passed else f'discrepancy={discrepancy}',
    )
    return T0Result(passed, discrepancy, source_count, kafka_count)
