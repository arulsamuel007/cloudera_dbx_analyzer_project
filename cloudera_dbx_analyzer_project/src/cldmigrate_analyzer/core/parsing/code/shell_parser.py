import re
from typing import Dict, List

CALL_RE = re.compile(r"\b(?:bash|sh|source|\.)\s+([^\s;]+)")
SPARK_SUBMIT_RE = re.compile(r"\bspark-submit\b([^\n]+)")
HIVE_RE = re.compile(r"\b(?:beeline|hive)\b([^\n]+)")
IMPALA_RE = re.compile(r"\bimpala-shell\b([^\n]+)")

def extract_calls(text: str) -> Dict[str, List[str]]:
    calls = [m.group(1) for m in CALL_RE.finditer(text or "")]
    return {
        "scripts": sorted(set(calls)),
        "spark_submit": [m.group(0).strip() for m in SPARK_SUBMIT_RE.finditer(text or "")],
        "hive_calls": [m.group(0).strip() for m in HIVE_RE.finditer(text or "")],
        "impala_calls": [m.group(0).strip() for m in IMPALA_RE.finditer(text or "")],
    }
