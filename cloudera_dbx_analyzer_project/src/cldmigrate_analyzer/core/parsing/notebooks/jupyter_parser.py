import json
from typing import Dict, List, Tuple

def parse_ipynb(text: str) -> Tuple[Dict, str]:
    try:
        nb = json.loads(text)
    except Exception as e:
        return {}, f"json_parse_error:{e}"
    cells = nb.get("cells", [])
    code_cells = []
    for c in cells:
        if c.get("cell_type") == "code":
            src = c.get("source", [])
            if isinstance(src, list):
                src = "".join(src)
            code_cells.append(src)
    return {"code_cells": code_cells, "cell_count": len(cells)}, ""
