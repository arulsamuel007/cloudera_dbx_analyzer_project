from typing import Dict

def parse_properties(text: str) -> Dict[str,str]:
    out: Dict[str,str] = {}
    for line in (text or "").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("!"):
            continue
        if "=" in s:
            k,v = s.split("=",1)
            out[k.strip()] = v.strip()
        elif ":" in s:
            k,v = s.split(":",1)
            out[k.strip()] = v.strip()
    return out
