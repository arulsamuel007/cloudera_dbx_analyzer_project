import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

@dataclass
class OozieAction:
    name: str
    action_type: str
    main: Optional[str] = None
    args: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    archives: List[str] = field(default_factory=list)
    job_xmls: List[str] = field(default_factory=list)
    subworkflow_app_path: Optional[str] = None

@dataclass
class OozieWorkflow:
    name: Optional[str]
    actions: List[OozieAction]
    has_fork_join: bool
    has_decision: bool
    globals: Dict[str,str] = field(default_factory=dict)

def _strip_ns(tag: str) -> str:
    return tag.split("}",1)[-1] if "}" in tag else tag

def parse_workflow(xml_text: str) -> Tuple[Optional[OozieWorkflow], str]:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        return None, f"xml_parse_error:{e}"

    wf_name = root.attrib.get("name")
    actions: List[OozieAction] = []
    has_fork_join = False
    has_decision = False

    # globals
    globals_kv: Dict[str,str] = {}
    for g in root.findall(".//{*}global/{*}configuration/{*}property"):
        name = g.findtext("{*}name")
        val = g.findtext("{*}value")
        if name:
            globals_kv[name] = val or ""

    for node in root.iter():
        t = _strip_ns(node.tag).lower()
        if t in ("fork","join"):
            has_fork_join = True
        if t == "decision":
            has_decision = True

    for a in root.findall(".//{*}action"):
        aname = a.attrib.get("name","")
        # determine action type by first child element inside action
        action_type = "unknown"
        main = None
        args: List[str] = []
        files: List[str] = []
        archives: List[str] = []
        job_xmls: List[str] = []
        subwf = None

        for child in list(a):
            ct = _strip_ns(child.tag).lower()
            if ct in ("ok","error"):
                continue
            action_type = ct
            # heuristics for main script/class
            main = child.findtext("{*}script") or child.findtext("{*}job-tracker") or child.findtext("{*}class")
            # args
            for arg in child.findall(".//{*}arg"):
                if arg.text:
                    args.append(arg.text.strip())
            # files/archives
            for f in child.findall(".//{*}file"):
                if f.text: files.append(f.text.strip())
            for ar in child.findall(".//{*}archive"):
                if ar.text: archives.append(ar.text.strip())
            for jx in child.findall(".//{*}job-xml"):
                if jx.text: job_xmls.append(jx.text.strip())
            # subworkflow app-path
            if ct == "sub-workflow":
                subwf = child.findtext("{*}app-path")
            break

        actions.append(OozieAction(
            name=aname, action_type=action_type, main=main, args=args,
            files=files, archives=archives, job_xmls=job_xmls, subworkflow_app_path=subwf
        ))

    return OozieWorkflow(name=wf_name, actions=actions, has_fork_join=has_fork_join, has_decision=has_decision, globals=globals_kv), ""

# -------------------------------------------------------------------
# File-based wrapper for pipeline
# -------------------------------------------------------------------

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

def parse_workflow_xml(path: Path) -> Dict[str, Any]:
    """
    Pipeline entrypoint: read workflow.xml from disk, parse, and return a JSON-serializable dict.
    """
    try:
        xml_text = Path(path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return {"source_file": str(path), "parse_status": f"read_error:{e}", "name": None, "actions": []}

    wf, err = parse_workflow(xml_text)
    if wf is None:
        return {"source_file": str(path), "parse_status": err or "parse_error", "name": None, "actions": []}

    d = asdict(wf)
    d["source_file"] = str(path)
    d["parse_status"] = "ok"
    # Normalize keys to what the rest of the pipeline expects
    if "action_type" in (d.get("actions") or [{}])[0]:
        # convert dataclass field names
        for a in d.get("actions", []):
            a["type"] = a.pop("action_type", a.get("type"))
    return d
