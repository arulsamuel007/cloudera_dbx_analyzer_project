import json
from typing import Dict, List, Tuple

def parse_zeppelin(text: str) -> Tuple[Dict, str]:
    # Zeppelin notes can be JSON with paragraphs
    try:
        obj = json.loads(text)
    except Exception as e:
        return {}, f"json_parse_error:{e}"
    paras = obj.get("paragraphs", [])
    code_blocks = []
    for p in paras:
        txt = p.get("text") or ""
        code_blocks.append(txt)
    return {"paragraph_count": len(paras), "code_blocks": code_blocks}, ""
