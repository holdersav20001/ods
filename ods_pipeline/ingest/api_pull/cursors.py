"""Cursor strategies for the api_pull poller.

A Cursor is the small piece of state that decides:
  - what request parameters carry the watermark forward,
  - how to walk pages within one poll,
  - what the next watermark should be after a successful poll.

Slice 1 implements ``since_timestamp`` only. ``etag``, ``offset`` and
``full_replace`` plug in by adding new classes and one ``build_cursor``
branch — the poller body never branches on style.

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

    The poller does not decide URLs — Cursors do. ``url`` may be an absolute
    next-page link (link_header paging) or the configured base URL with the
    cursor query string applied (first page).
    """

    url: str
    params: dict[str, str]


class Cursor(Protocol):
    """Strategy that drives one poll of one dataset.

    A Cursor is constructed once per poll with the committed watermark and
    is consumed by the poller via ``initial_request``, ``next_request``
    and ``advance``. It carries no I/O — pure state machine.
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
