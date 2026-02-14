import re
from typing import Tuple

REDACTED = "***REDACTED***"

def redact_value(value: str, mode: str = "strict") -> str:
    if value is None:
        return value
    if mode == "strict":
        return REDACTED
    # balanced: keep small hints (length + last 2 chars) if safe-ish
    v = str(value)
    if len(v) <= 4:
        return REDACTED
    return f"{REDACTED}({len(v)} chars..{v[-2:]})"

def redact_snippet(snippet: str, mode: str = "strict") -> str:
    if not snippet:
        return snippet
    # avoid leaking; just redact whole snippet in strict
    return snippet if mode == "balanced" else REDACTED
