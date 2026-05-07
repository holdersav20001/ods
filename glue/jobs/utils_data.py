# glue/jobs/utils_data.py
"""Pure data helpers — no I/O, no DB, no spark dependency."""
import hashlib
import re
from datetime import date


def extract_business_date(filename: str, pattern: str) -> date:
    match = re.search(pattern, filename)
    if not match:
        raise ValueError(
            f"Cannot extract business_date from {filename!r} using pattern {pattern!r}"
        )
    s = match.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def generate_message_key(key_fields: list, row: dict) -> str:
    parts = "|".join(str(row.get(f, "")) for f in sorted(key_fields))
    return hashlib.sha256(parts.encode()).hexdigest()
