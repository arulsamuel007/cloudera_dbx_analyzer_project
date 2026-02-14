import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

@dataclass
class OozieBundle:
    name: Optional[str]
    coordinators: List[str] = field(default_factory=list)

def parse_bundle(xml_text: str) -> Tuple[Optional[OozieBundle], str]:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        return None, f"xml_parse_error:{e}"
    name = root.attrib.get("name")
    coords: List[str] = []
    for c in root.findall(".//{*}coordinator"):
        app = c.findtext("{*}app-path")
        if app:
            coords.append(app.strip())
    return OozieBundle(name=name, coordinators=coords), ""

# -------------------------------------------------------------------
# File-based wrapper for pipeline
# -------------------------------------------------------------------

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

def parse_bundle_xml(path: Path) -> Dict[str, Any]:
    """Pipeline entrypoint: read bundle XML and return JSON-serializable dict."""
    try:
        xml_text = Path(path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return {"source_file": str(path), "parse_status": f"read_error:{e}", "name": None}

    b, err = parse_bundle(xml_text)
    if b is None:
        return {"source_file": str(path), "parse_status": err or "parse_error", "name": None}

    d = asdict(b)
    d["source_file"] = str(path)
    d["parse_status"] = "ok"
    return d
