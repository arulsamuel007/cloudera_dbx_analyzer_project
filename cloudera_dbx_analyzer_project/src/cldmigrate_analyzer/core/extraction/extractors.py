import re
from typing import Dict, List, Tuple
from ...utils.redaction import redact_value

VAR_RE = re.compile(r"\$\{([^}]+)\}")
WFCONF_RE = re.compile(r"\$\{\s*wf:conf\('([^']+)'\)\s*\}")
COORD_RE = re.compile(r"\$\{\s*coord:[^}]+\}")

SQL_FROM_RE = re.compile(r"(?is)\bfrom\s+([\w\.\$\{\}]+)")
SQL_JOIN_RE = re.compile(r"(?is)\bjoin\s+([\w\.\$\{\}]+)")
SQL_INSERT_RE = re.compile(r"(?is)\binsert\s+(?:into|overwrite)\s+(?:table\s+)?([\w\.\$\{\}]+)")
SQL_CREATE_RE = re.compile(r"(?is)\bcreate\s     +table\s+(?:if\s+not\s+exists\s+)?([\w\.\$\{\}]+)")
SQL_MERGE_RE = re.compile(r"(?is)\bmerge\s+into\s+([\w\.\$\{\}]+)")

STREAMING_RE = re.compile(r"(?i)\breadStream\b|\bwriteStream\b|spark\.streaming|structured\s+streaming|kafka", re.MULTILINE)
DYNAMIC_SQL_RE = re.compile(r"\+\s*['\"]\s*(select|insert|create)\b|format\(|f['\"]\s*select\b", re.IGNORECASE)

def extract_variables(text: str) -> List[str]:
    vars_ = set()
    for m in VAR_RE.finditer(text or ""):
        vars_.add(m.group(1).strip())
    for m in WFCONF_RE.finditer(text or ""):
        vars_.add(f"wf:conf('{m.group(1)}')")
    if COORD_RE.search(text or ""):
        vars_.add("coord:expression")
    return sorted(vars_)

def extract_sql_lineage(text: str) -> Dict[str,List[str]]:
    src = set()
    tgt = set()
    for r in (SQL_FROM_RE, SQL_JOIN_RE):
        for m in r.finditer(text or ""):
            src.add(m.group(1).strip())
    for r in (SQL_INSERT_RE, SQL_CREATE_RE, SQL_MERGE_RE):
        for m in r.finditer(text or ""):
            tgt.add(m.group(1).strip())
    return {"sources": sorted(src), "targets": sorted(tgt)}

def has_streaming(text: str) -> bool:
    return bool(STREAMING_RE.search(text or ""))

def has_dynamic_sql(text: str) -> bool:
    return bool(DYNAMIC_SQL_RE.search(text or ""))

def find_patterns(text: str, patterns: List[str]) -> List[str]:
    out = []
    for p in patterns:
        try:
            rx = re.compile(p)
        except re.error:
            continue
        for m in rx.finditer(text or ""):
            val = m.group(0)
            out.append(val)
    return out

from pathlib import Path
from typing import Any, Dict, List, Tuple

def scan_repo_patterns(repo_root: Path, files_index: List[Dict[str, Any]], patterns: Dict[str, Any]) -> Dict[str, Any]:
    """
    Repo-level pattern scan (connections/urls/paths) producing the same structure used in report.
    patterns is the dict returned by config.loader.load_patterns():
      {secrets:..., connections:..., paths:..., languages:...}
    """
    con = (patterns or {}).get("connections", {}) or {}
    pth = (patterns or {}).get("paths", {}) or {}

    jdbc_patterns = con.get("jdbc_patterns", []) or []
    url_patterns = con.get("url_patterns", []) or []
    kafka_patterns = con.get("kafka_bootstrap_patterns", []) or []
    storage_patterns = pth.get("storage_path_patterns", []) or []

    # compile regex safely
    def _compile_many(rx_list: List[str]) -> List[Tuple[str, Any]]:
        out = []
        import re
        for p in rx_list:
            try:
                out.append((p, re.compile(p)))
            except re.error:
                continue
        return out

    jdbc_rx = _compile_many(jdbc_patterns)
    url_rx = _compile_many(url_patterns)
    kafka_rx = _compile_many(kafka_patterns)
    storage_rx = _compile_many(storage_patterns)

    findings = {
        "jdbc_strings": [],
        "urls": [],
        "kafka_bootstrap_hints": [],
        "storage_paths": [],
        "jdbc_count": 0,
        "url_count": 0,
        "kafka_bootstrap_count": 0,
        "storage_path_count": 0,
    }

    def _scan_lines(rel_path: str, text: str):
        lines = text.splitlines()
        for i, line in enumerate(lines, start=1):
            # JDBC
            for _, rx in jdbc_rx:
                for m in rx.finditer(line):
                    findings["jdbc_strings"].append(
                        {"value": m.group(0), "file": rel_path, "line": i, "confidence": "high"}
                    )
            # URLs
            for _, rx in url_rx:
                for m in rx.finditer(line):
                    findings["urls"].append(
                        {"value": m.group(0), "file": rel_path, "line": i, "confidence": "high"}
                    )
            # Kafka bootstrap (prefer capture group 1 if present)
            for _, rx in kafka_rx:
                for m in rx.finditer(line):
                    v = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
                    findings["kafka_bootstrap_hints"].append(
                        {"value": v, "file": rel_path, "line": i, "confidence": "high"}
                    )
            # Storage paths
            for _, rx in storage_rx:
                for m in rx.finditer(line):
                    findings["storage_paths"].append(
                        {"value": m.group(0), "file": rel_path, "line": i, "confidence": "high"}
                    )

    for f in files_index or []:
        rel = f.get("path")
        if not rel:
            continue
        p = repo_root / rel
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        _scan_lines(rel, text)

    findings["jdbc_count"] = len(findings["jdbc_strings"])
    findings["url_count"] = len(findings["urls"])
    findings["kafka_bootstrap_count"] = len(findings["kafka_bootstrap_hints"])
    findings["storage_path_count"] = len(findings["storage_paths"])
    return findings


from pathlib import Path
from typing import Any, Dict, List, Tuple

def scan_repo_patterns(repo_root: Path, files_index: List[Dict[str, Any]], patterns: Dict[str, Any]) -> Dict[str, Any]:
    """
    Repo-level pattern scan (connections/urls/paths) producing the same structure used in report.
    patterns is the dict returned by config.loader.load_patterns():
      {secrets:..., connections:..., paths:..., languages:...}
    """
    con = (patterns or {}).get("connections", {}) or {}
    pth = (patterns or {}).get("paths", {}) or {}

    jdbc_patterns = con.get("jdbc_patterns", []) or []
    url_patterns = con.get("url_patterns", []) or []
    kafka_patterns = con.get("kafka_bootstrap_patterns", []) or []
    storage_patterns = pth.get("storage_path_patterns", []) or []

    # compile regex safely
    def _compile_many(rx_list: List[str]) -> List[Tuple[str, Any]]:
        out = []
        import re
        for p in rx_list:
            try:
                out.append((p, re.compile(p)))
            except re.error:
                continue
        return out

    jdbc_rx = _compile_many(jdbc_patterns)
    url_rx = _compile_many(url_patterns)
    kafka_rx = _compile_many(kafka_patterns)
    storage_rx = _compile_many(storage_patterns)

    findings = {
        "jdbc_strings": [],
        "urls": [],
        "kafka_bootstrap_hints": [],
        "storage_paths": [],
        "jdbc_count": 0,
        "url_count": 0,
        "kafka_bootstrap_count": 0,
        "storage_path_count": 0,
    }

    def _scan_lines(rel_path: str, text: str):
        lines = text.splitlines()
        for i, line in enumerate(lines, start=1):
            # JDBC
            for _, rx in jdbc_rx:
                for m in rx.finditer(line):
                    findings["jdbc_strings"].append(
                        {"value": m.group(0), "file": rel_path, "line": i, "confidence": "high"}
                    )
            # URLs
            for _, rx in url_rx:
                for m in rx.finditer(line):
                    findings["urls"].append(
                        {"value": m.group(0), "file": rel_path, "line": i, "confidence": "high"}
                    )
            # Kafka bootstrap (prefer capture group 1 if present)
            for _, rx in kafka_rx:
                for m in rx.finditer(line):
                    v = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
                    findings["kafka_bootstrap_hints"].append(
                        {"value": v, "file": rel_path, "line": i, "confidence": "high"}
                    )
            # Storage paths
            for _, rx in storage_rx:
                for m in rx.finditer(line):
                    findings["storage_paths"].append(
                        {"value": m.group(0), "file": rel_path, "line": i, "confidence": "high"}
                    )

    for f in files_index or []:
        rel = f.get("path")
        if not rel:
            continue
        p = repo_root / rel
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        _scan_lines(rel, text)

    findings["jdbc_count"] = len(findings["jdbc_strings"])
    findings["url_count"] = len(findings["urls"])
    findings["kafka_bootstrap_count"] = len(findings["kafka_bootstrap_hints"])
    findings["storage_path_count"] = len(findings["storage_paths"])
    return findings


from pathlib import Path
from typing import Any, Dict, List

def extract_sql_lineage_repo(repo_root: Path, files_index: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Repo-level lineage extraction wrapper.
    Calls existing extract_sql_lineage(text) per file, and attaches evidence_file.
    """
    out: List[Dict[str, Any]] = []

    for f in files_index or []:
        rel = f.get("path")
        if not rel:
            continue

        # Only scan likely query/code files; keep it safe and fast
        t = (f.get("detected_type") or "").lower()
        if t not in {
            "sql", "hql", "impala_sql", "oozie_workflow_xml", "oozie_coordinator_xml",
            "notebook_zeppelin", "notebook_jupyter", "python", "scala", "java", "shell", "xml_generic"
        }:
            continue

        p = repo_root / rel
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        try:
            recs = extract_sql_lineage(text)  # <-- your existing function (takes 1 arg)
        except Exception:
            continue

        if not recs:
            continue

        for r in recs:
            if isinstance(r, dict):
                rr = dict(r)
                rr.setdefault("evidence_file", rel)
                out.append(rr)

    return out

from pathlib import Path
from typing import Any, Dict, List

def extract_variables_repo(repo_root: Path, files_index: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Repo-level variable extraction wrapper.
    Calls existing extract_variables(text) per file and merges results.
    """
    merged = {
        "placeholders": {},   # var -> count
        "examples": {},       # var -> example strings (limited)
        "by_file": {},        # file -> [vars]
    }

    def _add_var(var: str, example: str | None, rel: str):
        merged["placeholders"][var] = merged["placeholders"].get(var, 0) + 1
        if example and var not in merged["examples"]:
            merged["examples"][var] = example
        merged["by_file"].setdefault(rel, [])
        if var not in merged["by_file"][rel]:
            merged["by_file"][rel].append(var)

    for f in files_index or []:
        rel = f.get("path")
        if not rel:
            continue

        # Scan broadly; variables appear everywhere (xml, props, py, sql, etc.)
        p = repo_root / rel
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        try:
            res = extract_variables(text)  # <-- existing single-text function
        except Exception:
            continue

        # Support a few possible return shapes safely
        if isinstance(res, dict):
            # if res already has list/set under common keys
            vars_list = None
            for k in ("variables", "placeholders", "vars"):
                if k in res and isinstance(res[k], (list, set, tuple)):
                    vars_list = list(res[k])
                    break
            if vars_list is None:
                # if dict is var->count or var->example
                # treat keys as variable names
                vars_list = [k for k in res.keys() if isinstance(k, str)]

            for v in vars_list:
                if isinstance(v, str) and v:
                    _add_var(v, None, rel)

        elif isinstance(res, (list, set, tuple)):
            for v in res:
                if isinstance(v, str) and v:
                    _add_var(v, None, rel)

        elif isinstance(res, str):
            # single var
            _add_var(res, None, rel)

    merged["total_unique"] = len(merged["placeholders"])
    merged["total_occurrences"] = sum(merged["placeholders"].values())
    return merged


from typing import Any, Dict, List

def has_streaming_repo(files_index: List[Dict[str, Any]], workflows_blob: Dict[str, Any]) -> bool:
    """
    Repo-level wrapper for streaming detection.
    Uses existing has_streaming(text) if available; otherwise scans workflow/actions.
    """
    # Try workflow hints first (kafka, spark streaming, flink etc.)
    wf_texts = []
    for wf in (workflows_blob or {}).get("workflows", []):
        for act in wf.get("actions", []):
            for k in ("main", "script", "class", "args"):
                v = act.get(k)
                if isinstance(v, str) and v:
                    wf_texts.append(v)

    combined = "\n".join(wf_texts)

    try:
        # If existing has_streaming expects a single string
        if combined:
            if has_streaming(combined):
                return True
    except TypeError:
        pass
    except Exception:
        pass

    # Fall back: scan file_index types or names for streaming hints
    streaming_tokens = ("kafka", "sparkstream", "spark streaming", "structuredstream", "flink", "kinesis")
    for f in files_index or []:
        p = (f.get("path") or "").lower()
        if any(tok in p for tok in streaming_tokens):
            return True

    return False

from pathlib import Path
from typing import Any, Dict, List

def has_dynamic_sql_repo(repo_root: Path, files_index: List[Dict[str, Any]]) -> bool:
    """
    Repo-level dynamic SQL detector.
    Uses existing has_dynamic_sql(text) by scanning text content of likely files.
    """
    for f in files_index or []:
        rel = f.get("path")
        if not rel:
            continue

        # Limit to likely code/query/config files
        t = (f.get("detected_type") or "").lower()
        if t not in {
            "sql", "hql", "impala_sql", "python", "scala", "java", "shell",
            "oozie_workflow_xml", "oozie_coordinator_xml", "xml_generic",
            "properties", "ini_conf", "notebook_zeppelin", "notebook_jupyter"
        }:
            continue

        p = repo_root / rel
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        try:
            if has_dynamic_sql(text):
                return True
        except Exception:
            # if regex fails for any reason, just keep scanning
            continue

    return False

