import re
from typing import List

IMPORT_RE = re.compile(r"^\s*import\s+([\w\.]+)", re.MULTILINE)

def extract_imports(text: str) -> List[str]:
    out = []
    for m in IMPORT_RE.finditer(text or ""):
        out.append(m.group(1))
    return sorted(set(out))
