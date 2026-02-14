import re
from typing import List, Tuple

IMPORT_RE = re.compile(r"^\s*(?:from\s+([\w\.]+)\s+import|import\s+([\w\.]+))", re.MULTILINE)

def extract_imports(text: str) -> List[str]:
    out = []
    for m in IMPORT_RE.finditer(text or ""):
        mod = m.group(1) or m.group(2)
        if mod:
            out.append(mod.split('.')[0])
    return sorted(set(out))
