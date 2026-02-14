from pathlib import Path
from typing import Tuple

def load_head_text(path: str, max_bytes: int = 64_000) -> Tuple[str, str]:
    p = Path(path)
    try:
        with p.open("rb") as f:
            data = f.read(max_bytes)
    except Exception as e:
        return "", f"read_error:{e}"

    # try utf-8 first then latin1 fallback
    for enc in ("utf-8","utf-8-sig","latin-1"):
        try:
            return data.decode(enc), ""
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore"), "decode_warn"
