import re
from pathlib import Path
from typing import Dict, List, Tuple

OOZIE_WORKFLOW_RE = re.compile(r"<\s*workflow-app\b", re.IGNORECASE)
OOZIE_COORD_RE = re.compile(r"<\s*coordinator-app\b", re.IGNORECASE)
OOZIE_BUNDLE_RE = re.compile(r"<\s*bundle-app\b", re.IGNORECASE)

def classify(path: str, head_text: str) -> Tuple[str, List[str]]:
    p = Path(path)
    name = p.name.lower()
    ext = p.suffix.lower()
    signals: List[str] = []

    # Oozie by filename
    if name == "workflow.xml":
        return "oozie_workflow_xml", ["filename:workflow.xml"]
    if name == "coordinator.xml":
        return "oozie_coordinator_xml", ["filename:coordinator.xml"]
    if name == "bundle.xml":
        return "oozie_bundle_xml", ["filename:bundle.xml"]

    # by extension
    mapping = {
        ".properties":"properties",
        ".props":"properties",
        ".json":"json",
        ".yml":"yaml",
        ".yaml":"yaml",
        ".ini":"ini_conf",
        ".cfg":"ini_conf",
        ".conf":"ini_conf",
        ".sql":"sql",
        ".hql":"hql",
        ".q":"hql",
        ".ddl":"sql_ddl_dml",
        ".dml":"sql_ddl_dml",
        ".pig":"pig",
        ".sh":"shell",
        ".bash":"shell",
        ".ksh":"shell",
        ".py":"python",
        ".scala":"scala",
        ".java":"java",
        ".ipynb":"notebook_jupyter",
        ".zpln":"notebook_zeppelin",
        ".md":"text_doc",
        ".txt":"text_doc",
        ".xml":"xml",
    }
    t = mapping.get(ext, "unknown")

    # signature refinement
    ht = head_text or ""
    if t in ("xml","unknown"):
        if OOZIE_WORKFLOW_RE.search(ht):
            return "oozie_workflow_xml", signals+["sig:workflow-app"]
        if OOZIE_COORD_RE.search(ht):
            return "oozie_coordinator_xml", signals+["sig:coordinator-app"]
        if OOZIE_BUNDLE_RE.search(ht):
            return "oozie_bundle_xml", signals+["sig:bundle-app"]
        if ht.strip().startswith("<?xml"):
            return "xml_generic", signals+["sig:xml"]

    # sql refinement for impala/hive
    if t in ("sql","hql","sql_ddl_dml"):
        if re.search(r"(?i)\binvalidate\s+metadata\b|\bcompute\s+stats\b|\brefresh\b", ht):
            return "impala_sql", signals+["sig:impala"]
        if re.search(r"(?i)\bset\s+hive\.|\bmsck\s+repair\b|\bcreate\s+temporary\s+function\b", ht):
            return "hql", signals+["sig:hive"]

    # build files
    if name == "pom.xml":
        return "build_maven", ["filename:pom.xml"]
    if name in ("build.gradle","settings.gradle"):
        return "build_gradle", [f"filename:{name}"]
    if name == "build.sbt":
        return "build_sbt", ["filename:build.sbt"]
    if name in ("requirements.txt","pipfile","poetry.lock"):
        return "build_python_deps", [f"filename:{name}"]
    return t, signals
