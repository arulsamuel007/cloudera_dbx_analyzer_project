from __future__ import annotations

from pathlib import Path
from typing import Any


def count_lines_words(text: str) -> tuple[int, int]:
    if text is None:
        return 0, 0
    lines = text.splitlines()
    words = 0
    for ln in lines:
        words += len([w for w in ln.strip().split() if w])
    return len(lines), words


def compute_counts(repo_root: Path, files_index: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Adds line/word counts to each files_index entry in a cross-platform way.

    Expected input shape (minimum):
      - entry["path"] relative to repo_root
      - entry["detected_type"]

    Adds/updates:
      - lines_count
      - words_count
      - parse_status (sets to 'skipped_large' or 'read_error' when needed)
    """
    for f in files_index:
        rel = f.get("path")
        if not rel:
            f["lines_count"] = 0
            f["words_count"] = 0
            continue

        p = repo_root / rel

        # Skip binary-ish types quickly (best-effort)
        ext = p.suffix.lower()
        if ext in {".jar", ".class", ".zip", ".tar", ".gz", ".7z", ".parquet", ".orc", ".avro",
                   ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".docx", ".pptx", ".xlsx"}:
            f["lines_count"] = 0
            f["words_count"] = 0
            f.setdefault("parse_status", "skipped_binary")
            continue

        # If size_bytes exists and is too big, don't fully read
        size_bytes = f.get("size_bytes")
        if isinstance(size_bytes, int) and size_bytes > 10 * 1024 * 1024:  # 10 MB default cap
            f["lines_count"] = 0
            f["words_count"] = 0
            f["parse_status"] = "skipped_large"
            continue

        try:
            text = p.read_text(encoding="utf-8", errors="replace")
            lc, wc = count_lines_words(text)
            f["lines_count"] = lc
            f["words_count"] = wc
            f.setdefault("parse_status", "ok")
        except Exception:
            f["lines_count"] = 0
            f["words_count"] = 0
            f["parse_status"] = "read_error"

    return files_index
