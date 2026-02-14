from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Any, Optional


_VAR_RE = re.compile(r"\$\{([^}]+)\}")


@dataclass
class VarDef:
    name: str
    value: str
    defined_in: str  # file path
    kind: str        # properties|oozie_conf|maven_props


def parse_properties_text(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
        elif ":" in line:
            k, v = line.split(":", 1)
        else:
            continue
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def parse_maven_properties(pom_text: str) -> Dict[str, str]:
    # very lightweight: grabs <properties> <a>v</a> ... </properties>
    props: Dict[str, str] = {}
    m = re.search(r"<properties>(.*?)</properties>", pom_text, flags=re.DOTALL | re.IGNORECASE)
    if not m:
        return props
    block = m.group(1)
    for tag, val in re.findall(r"<([a-zA-Z0-9_.-]+)>(.*?)</\1>", block, flags=re.DOTALL):
        v = re.sub(r"\s+", " ", val.strip())
        if tag and v:
            props[tag.strip()] = v
    return props


def parse_oozie_configuration(xml_text: str) -> Dict[str, str]:
    # Extract <property><name>k</name><value>v</value></property>
    props: Dict[str, str] = {}
    for name, val in re.findall(
        r"<property>\s*<name>\s*(.*?)\s*</name>\s*<value>\s*(.*?)\s*</value>\s*</property>",
        xml_text,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        k = re.sub(r"\s+", " ", name.strip())
        v = re.sub(r"\s+", " ", val.strip())
        if k:
            props[k] = v
    return props


def merge_definitions(def_lists: Iterable[Iterable[VarDef]]) -> Tuple[Dict[str, VarDef], Dict[str, List[VarDef]]]:
    chosen: Dict[str, VarDef] = {}
    all_defs: Dict[str, List[VarDef]] = {}
    # precedence: properties > oozie_conf > maven_props (tunable)
    precedence = {"properties": 3, "oozie_conf": 2, "maven_props": 1}

    for defs in def_lists:
        for d in defs:
            all_defs.setdefault(d.name, []).append(d)
            if d.name not in chosen:
                chosen[d.name] = d
            else:
                if precedence.get(d.kind, 0) > precedence.get(chosen[d.name].kind, 0):
                    chosen[d.name] = d
    return chosen, all_defs


def resolve_string(s: str, lookup: Dict[str, str], max_depth: int = 10) -> Tuple[str, List[str]]:
    unresolved: List[str] = []
    cur = s

    for _ in range(max_depth):
        changed = False

        def repl(m: re.Match) -> str:
            nonlocal changed
            key = m.group(1)
            if key in lookup:
                changed = True
                return lookup[key]
            else:
                if key not in unresolved:
                    unresolved.append(key)
                return m.group(0)

        nxt = _VAR_RE.sub(repl, cur)
        cur = nxt
        if not changed:
            break

    # any remaining placeholders
    for k in _VAR_RE.findall(cur):
        if k not in unresolved:
            unresolved.append(k)

    return cur, unresolved


def build_definitions_from_repo(files_index: List[Dict[str, Any]], repo_root: Path) -> List[VarDef]:
    defs: List[VarDef] = []
    for f in files_index:
        p = repo_root / f["path"]
        ftype = f.get("detected_type", "")
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        if ftype in ("properties", "ini_conf") or p.name.endswith(".properties"):
            d = parse_properties_text(text)
            for k, v in d.items():
                defs.append(VarDef(name=k, value=v, defined_in=str(p), kind="properties"))

        # pick up oozie configs inside workflow/coordinator xml too
        if ftype.startswith("oozie_") or (p.suffix.lower() == ".xml"):
            d = parse_oozie_configuration(text)
            for k, v in d.items():
                defs.append(VarDef(name=k, value=v, defined_in=str(p), kind="oozie_conf"))

        if p.name.lower() == "pom.xml" or ftype == "build_maven":
            d = parse_maven_properties(text)
            for k, v in d.items():
                defs.append(VarDef(name=k, value=v, defined_in=str(p), kind="maven_props"))

    return defs


def resolve_repository(
    files_index: List[Dict[str, Any]],
    repo_root: Path,
    raw_findings: Dict[str, Any],
    raw_workflows: Dict[str, Any],
    raw_lineage: List[Dict[str, Any]],
) -> Dict[str, Any]:
    # 1) build definitions
    defs = build_definitions_from_repo(files_index, repo_root)
    chosen, all_defs = merge_definitions([defs])
    lookup = {k: v.value for k, v in chosen.items()}

    # 2) resolve selected data blobs
    # resolve in findings evidence values
    resolved_findings = raw_findings.copy()
    unresolved_hits: List[Dict[str, Any]] = []

    def _resolve_evidence_list(lst: List[Dict[str, Any]], field: str) -> None:
        for item in lst:
            val = item.get(field)
            if isinstance(val, str):
                new_val, un = resolve_string(val, lookup)
                item[field] = new_val
                if un:
                    unresolved_hits.append(
                        {"kind": "finding", "what": field, "unresolved": un, "file": item.get("file"), "line": item.get("line")}
                    )

    for k in ("jdbc_strings", "urls", "storage_paths", "kafka_bootstrap_hints"):
        if k in resolved_findings and isinstance(resolved_findings[k], list):
            _resolve_evidence_list(resolved_findings[k], "value")

    # resolve in workflows/coordinators
    resolved_workflows = raw_workflows.copy()
    for wf in resolved_workflows.get("workflows", []):
        for field in ("app_path", "workflow_path"):
            if isinstance(wf.get(field), str):
                wf[field], un = resolve_string(wf[field], lookup)
                if un:
                    unresolved_hits.append({"kind": "workflow", "what": field, "unresolved": un, "file": wf.get("source_file")})
        for act in wf.get("actions", []):
            for field in ("main", "script", "class", "args"):
                if isinstance(act.get(field), str):
                    act[field], un = resolve_string(act[field], lookup)
                    if un:
                        unresolved_hits.append({"kind": "action", "what": field, "unresolved": un, "file": wf.get("source_file")})

    for coord in resolved_workflows.get("coordinators", []):
        for field in ("frequency", "start", "end", "timezone", "workflow_app_path"):
            if isinstance(coord.get(field), str):
                coord[field], un = resolve_string(coord[field], lookup)
                if un:
                    unresolved_hits.append({"kind": "coordinator", "what": field, "unresolved": un, "file": coord.get("source_file")})

    # resolve lineage strings
    resolved_lineage: List[Dict[str, Any]] = []
    for rec in raw_lineage:
        r = rec.copy()
        for field in ("source_name", "target_name"):
            if isinstance(r.get(field), str):
                r[field], un = resolve_string(r[field], lookup)
                if un:
                    unresolved_hits.append({"kind": "lineage", "what": field, "unresolved": un, "file": r.get("evidence_file")})
        resolved_lineage.append(r)

    # 3) compute resolved/partial/unresolved variable sets
    resolved_vars: List[Dict[str, Any]] = []
    partially: List[Dict[str, Any]] = []

    # variables seen in repo (from unresolved.json OR from extraction in files_index)
    # Here we rebuild seen vars from placeholders in findings + workflows + lineage
    seen = set()
    vars_seen=set()
    def _collect_from_str(s):
        if not s:
            return
        # args can be list[str] from parsed XML; handle both
        if isinstance(s, (list, tuple, set)):
            for item in s:
                _collect_from_str(item)
            return
        if not isinstance(s, (str, bytes)):
            return
        for k in _VAR_RE.findall(s):
            vars_seen.add(k)


    # findings
    for k in ("jdbc_strings", "urls", "storage_paths", "kafka_bootstrap_hints"):
        for item in raw_findings.get(k, []):
            _collect_from_str(item.get("value"))

    # workflows / coordinators
    for wf in raw_workflows.get("workflows", []):
        _collect_from_str(wf.get("app_path"))
        for act in wf.get("actions", []):
            _collect_from_str(act.get("main"))
            _collect_from_str(act.get("script"))
            _collect_from_str(act.get("class"))
            _collect_from_str(act.get("args"))

    for coord in raw_workflows.get("coordinators", []):
        _collect_from_str(coord.get("frequency"))
        _collect_from_str(coord.get("workflow_app_path"))

    # lineage
    for rec in raw_lineage:
        _collect_from_str(rec.get("source_name"))
        _collect_from_str(rec.get("target_name"))

    # Add all keys defined too
    for k in lookup.keys():
        seen.add(k)

    # Resolve variables themselves (including nested definitions)
    final_lookup: Dict[str, str] = {}
    unresolved_vars: Dict[str, List[str]] = {}
    for k in sorted(seen):
        if k in lookup:
            v, un = resolve_string(lookup[k], lookup)
            final_lookup[k] = v
            if un:
                unresolved_vars[k] = un
        else:
            unresolved_vars[k] = ["<no_definition_found>"]

    for k, v in final_lookup.items():
        if k in unresolved_vars:
            partially.append(
                {
                    "name": k,
                    "raw_value": lookup.get(k, ""),
                    "resolved_value": v,
                    "unresolved_parts": unresolved_vars[k],
                    "definitions": [
                        {"value": d.value, "defined_in": d.defined_in, "kind": d.kind}
                        for d in all_defs.get(k, [])
                    ],
                }
            )
        else:
            resolved_vars.append(
                {
                    "name": k,
                    "value": v,
                    "definitions": [
                        {"value": d.value, "defined_in": d.defined_in, "kind": d.kind}
                        for d in all_defs.get(k, [])
                    ],
                }
            )

    unresolved_list = []
    for k, why in unresolved_vars.items():
        if k in final_lookup:
            continue  # partial already tracked
        unresolved_list.append(
            {
                "name": k,
                "reason": why,
                "definitions_found": [
                    {"value": d.value, "defined_in": d.defined_in, "kind": d.kind}
                    for d in all_defs.get(k, [])
                ],
            }
        )

    return {
        "resolved_variables": resolved_vars,
        "partially_resolved_variables": partially,
        "still_unresolved_variables": unresolved_list,
        "resolved_findings": resolved_findings,
        "resolved_workflows": resolved_workflows,
        "resolved_lineage": resolved_lineage,
        "unresolved_hits": unresolved_hits,
    }
