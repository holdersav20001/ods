"""Cursor strategy unit tests for slice 1 (since_timestamp)."""
from __future__ import annotations

import pytest

from ods_pipeline.ingest.api_pull.cursors import (
    SinceTimestampCursor,
    build_cursor,
)


def _build(committed=None, page_style="link_header"):
    return SinceTimestampCursor(
        base_url="https://api.example/items",
        request_param="updated_since",
        response_field="updated_at",
        committed_value=committed,
        initial_value="2026-01-01T00:00:00Z",
        page_style=page_style,
    )


def test_initial_request_uses_committed_when_present():
    cursor = _build(committed="2026-04-01T00:00:00Z")
    req = cursor.initial_request()
    assert req.url == "https://api.example/items"
    assert req.params == {"updated_since": "2026-04-01T00:00:00Z"}


def test_initial_request_falls_back_to_initial_when_no_committed():
    cursor = _build(committed=None)
    req = cursor.initial_request()
    assert req.params == {"updated_since": "2026-01-01T00:00:00Z"}


def test_link_header_paging_returns_next_url():
    cursor = _build()
    cursor.initial_request()
    headers = {
        "Link": '<https://api.example/items?page=2>; rel="next", '
                '<https://api.example/items?page=99>; rel="last"',
    }
    req = cursor.next_request(headers, [])
    assert req is not None
    assert req.url == "https://api.example/items?page=2"
    assert req.params == {}


def test_link_header_no_next_returns_none():
    cursor = _build()
    headers = {"Link": '<https://api.example/items?page=99>; rel="last"'}
    assert cursor.next_request(headers, []) is None


def test_link_header_missing_returns_none():
    cursor = _build()
    assert cursor.next_request({}, []) is None


def test_page_style_none_never_pages():
    cursor = _build(page_style="none")
    headers = {"Link": '<https://api.example/items?page=2>; rel="next"'}
    assert cursor.next_request(headers, []) is None


def test_advance_returns_max_response_field():
    cursor = _build()
    records = [
        {"updated_at": "2026-04-01T00:00:00Z"},
        {"updated_at": "2026-04-03T00:00:00Z"},
        {"updated_at": "2026-04-02T00:00:00Z"},
    ]
    assert cursor.advance(records) == "2026-04-03T00:00:00Z"


def test_advance_empty_records_returns_none():
    cursor = _build()
    assert cursor.advance([]) is None


def test_advance_skips_records_without_field():
    cursor = _build()
    records = [
        {"id": 1},
        {"updated_at": "2026-04-02T00:00:00Z"},
    ]
    assert cursor.advance(records) == "2026-04-02T00:00:00Z"


def test_build_cursor_constructs_since_timestamp():
    source = {
        "url": "https://api.example/items",
        "cursor": {
            "style": "since_timestamp",
            "request_param": "since",
            "response_field": "modified_at",
            "initial": "2026-01-01T00:00:00Z",
        },
        "page": {"style": "link_header"},
    }
    cursor = build_cursor(source, committed_value=None)
    assert cursor.style == "since_timestamp"
    req = cursor.initial_request()
    assert req.params == {"since": "2026-01-01T00:00:00Z"}


def test_build_cursor_unsupported_style_raises():
    with pytest.raises(ValueError, match="cursor.style"):
        build_cursor(
            {"url": "x", "cursor": {"style": "etag"}},
            committed_value=None,
        )


def test_since_timestamp_requires_request_param():
    with pytest.raises(ValueError, match="request_param"):
        SinceTimestampCursor(
            base_url="x",
            request_param="",
            response_field="updated_at",
            committed_value=None,
            initial_value="2026-01-01",
        )


def test_since_timestamp_unsupported_page_style():
    with pytest.raises(ValueError, match="page.style"):
        SinceTimestampCursor(
            base_url="x",
            request_param="since",
            response_field="updated_at",
            committed_value=None,
            initial_value="2026-01-01",
            page_style="cursor_field",
        )
