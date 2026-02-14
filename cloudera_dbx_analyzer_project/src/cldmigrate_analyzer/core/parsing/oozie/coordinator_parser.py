import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

@dataclass
class OozieDataset:
    name: str
    uri_template: str

@dataclass
class OozieCoordinator:
    name: Optional[str]
    frequency: Optional[str]
    start: Optional[str]
    end: Optional[str]
    timezone: Optional[str]
    workflow_app_path: Optional[str]
    datasets: List[OozieDataset] = field(default_factory=list)
    properties: Dict[str,str] = field(default_factory=dict)

def parse_coordinator(xml_text: str) -> Tuple[Optional[OozieCoordinator], str]:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        return None, f"xml_parse_error:{e}"

    name = root.attrib.get("name")
    frequency = root.attrib.get("frequency")
    start = root.attrib.get("start")
    end = root.attrib.get("end")
    timezone = root.attrib.get("timezone")

    props: Dict[str,str] = {}
    for p in root.findall(".//{*}configuration/{*}property"):
        pn = p.findtext("{*}name")
        pv = p.findtext("{*}value")
        if pn:
            props[pn] = pv or ""

    wf_app_path = root.findtext(".//{*}action/{*}workflow/{*}app-path")

    datasets: List[OozieDataset] = []
    for ds in root.findall(".//{*}datasets/{*}dataset"):
        dsn = ds.attrib.get("name","")
        uri = ds.findtext("{*}uri-template") or ""
        datasets.append(OozieDataset(name=dsn, uri_template=uri))

    return OozieCoordinator(
        name=name, frequency=frequency, start=start, end=end, timezone=timezone,
        workflow_app_path=wf_app_path, datasets=datasets, properties=props
    ), ""

# -------------------------------------------------------------------
# File-based wrapper for pipeline
# -------------------------------------------------------------------

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

def parse_coordinator_xml(path: Path) -> Dict[str, Any]:
    """Pipeline entrypoint: read coordinator XML and return JSON-serializable dict."""
    try:
        xml_text = Path(path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return {"source_file": str(path), "parse_status": f"read_error:{e}", "name": None}

    coord, err = parse_coordinator(xml_text)
    if coord is None:
        return {"source_file": str(path), "parse_status": err or "parse_error", "name": None}

    d = asdict(coord)
    d["source_file"] = str(path)
    d["parse_status"] = "ok"
    return d
