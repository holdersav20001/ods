"""P4 item 9 — adapter contract: the REQUIRED call sequence a real sink job
must follow.

This is the ONE place mocks are correct. Every other test runs against the real
DB because the property under test IS database behaviour. Here the property is a
CALL-SEQUENCE contract on the client: a sink job MUST write the lineage link
BEFORE any target row — "Postgres write is last", enforced in the harness via
write_link_then_rows. The harness proves the CLIENT honours this; it does NOT
prove that a real (future) Spark sink job calls the client in the right order.

This contract test pins that ordering with unittest.mock: it wraps the client
calls and asserts write_link / write_link_then_rows is invoked before any
target-row write. A real adapter is then conformant iff it passes this same
sequence assertion.
"""
import unittest.mock as mock


class _SinkJobUnderTest:
    """A stand-in for a real sink adapter. The CORRECT adapter writes the link
    (link-then-rows) before stamping target rows. We assert the order of calls
    on the injected client.
    """
    def __init__(self, client):
        self.client = client

    def run(self, *, consumer_run_id, edges, rows):
        # CONTRACT: link FIRST (and, here, the sanctioned link-then-rows
        # primitive does the row write atomically AFTER the link).
        self.client.write_link_then_rows(
            consumer_run_id=consumer_run_id,
            edge_type="canonical_to_sink",
            target_ref={"content_hash": "h"},
            record_count=len(rows),
            edges=edges, rows=rows, sink_type="postgres")


class _BadSinkJob:
    """A NON-conformant adapter that writes target rows BEFORE the link."""
    def __init__(self, client):
        self.client = client

    def run(self, *, consumer_run_id, edges, rows):
        self.client.write_target_rows(rows)            # WRONG: rows first
        self.client.write_link_then_rows(
            consumer_run_id=consumer_run_id,
            edge_type="canonical_to_sink",
            target_ref={"content_hash": "h"},
            record_count=len(rows), edges=edges, rows=rows,
            sink_type="postgres")


def test_sink_job_writes_link_before_rows():
    """A conformant sink job calls write_link_then_rows (the link-first
    primitive) and never a raw row write before it."""
    client = mock.MagicMock()
    manager = mock.Mock()
    manager.attach_mock(client.write_link_then_rows, "link_then_rows")
    manager.attach_mock(client.write_target_rows, "rows")

    job = _SinkJobUnderTest(client)
    job.run(consumer_run_id="r1", edges=[{"e": 1}], rows=[{"k": 1}])

    names = [c[0] for c in manager.mock_calls]
    # The FIRST (and only) write is the link-then-rows primitive; no bare row
    # write precedes it.
    assert "link_then_rows" in names
    if "rows" in names:
        assert names.index("link_then_rows") < names.index("rows"), \
            "target rows written before the lineage link — contract violated"
    client.write_link_then_rows.assert_called_once()


def test_bad_sink_job_is_caught_by_the_contract():
    """Proof the assertion has teeth: a job that writes rows BEFORE the link
    FAILS the same ordering check."""
    client = mock.MagicMock()
    manager = mock.Mock()
    manager.attach_mock(client.write_link_then_rows, "link_then_rows")
    manager.attach_mock(client.write_target_rows, "rows")

    job = _BadSinkJob(client)
    job.run(consumer_run_id="r1", edges=[{"e": 1}], rows=[{"k": 1}])

    names = [c[0] for c in manager.mock_calls]
    assert names.index("rows") < names.index("link_then_rows")
    # The contract check (link-before-rows) would REJECT this ordering:
    contract_holds = names.index("link_then_rows") < names.index("rows")
    assert not contract_holds, "bad ordering should violate the contract"
