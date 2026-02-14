from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List

from ..discovery.repo_scanner import scan_repository
from ..metrics.counts import compute_counts
from ..parsing.oozie.workflow_parser import parse_workflow_xml
from ..parsing.oozie.coordinator_parser import parse_coordinator_xml
from ..parsing.oozie.bundle_parser import parse_bundle_xml
from ..extraction.extractors import (
    scan_repo_patterns,
    extract_sql_lineage_repo,
    extract_variables_repo,
    has_streaming_repo,
    has_dynamic_sql_repo,
)
# NEW: Import database schema parser
from ..extraction.database_schema_parser import extract_databases_from_repository
from ..dependency.graph import build_dependency_graph
from ..metrics.complexity import score_repository
from ..resolution.resolver import resolve_repository
from ...reporting.render import render_html_report


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def analyze_repository(
    input_dir: str,
    output_run_dir: str,
    patterns: Dict[str, Any],
    rubric: Dict[str, Any],
    max_file_mb: int = 10,
    include_globs: List[str] | None = None,
    exclude_globs: List[str] | None = None,
    log: Any | None = None,
) -> None:
    t0 = time.time()
    repo_root = Path(input_dir)

    out_dir = Path(output_run_dir)
    artifacts_dir = out_dir / "artifacts"
    logs_dir = out_dir / "logs"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # 1) scan + classify
    files_index = scan_repository(
        repo_root=repo_root,
        max_file_mb=max_file_mb,
        include_globs=include_globs or [],
        exclude_globs=exclude_globs or [],
        patterns=patterns,
    )

    # 2) counts
    files_index = compute_counts(repo_root, files_index)

    # 3) parse oozie xmls
    workflows: List[Dict[str, Any]] = []
    coordinators: List[Dict[str, Any]] = []
    bundles: List[Dict[str, Any]] = []

    for f in files_index:
        p = repo_root / f["path"]
        ftype = f.get("detected_type")
        if ftype == "oozie_workflow_xml":
            workflows.append(parse_workflow_xml(p))
        elif ftype == "oozie_coordinator_xml":
            coordinators.append(parse_coordinator_xml(p))
        elif ftype == "oozie_bundle_xml":
            bundles.append(parse_bundle_xml(p))

    workflows_blob = {"workflows": workflows, "coordinators": coordinators, "bundles": bundles}
    _write_json(artifacts_dir / "workflows.json", workflows_blob)

    # 4) pattern findings + lineage + variables (first pass)
    findings = scan_repo_patterns(repo_root, files_index, patterns)
    _write_json(artifacts_dir / "findings.json", findings)

    lineage = extract_sql_lineage_repo(repo_root, files_index)
    _write_json(artifacts_dir / "lineage.json", lineage)

    variables = extract_variables_repo(repo_root, files_index)
    # keep for backward compatibility
    _write_json(artifacts_dir / "variables.json", variables)

    # NEW STEP 4.5) Extract database and schema information
    if log:
        log.info("Extracting database and schema information...")
    
    database_context = extract_databases_from_repository(repo_root, files_index)
    _write_json(artifacts_dir / "database_context.json", database_context)
    
    if log:
        db_summary = database_context.get("summary", {})
        log.info(
            f"Found {db_summary.get('total_databases', 0)} databases, "
            f"{db_summary.get('total_source_table_refs', 0)} source table refs, "
            f"{db_summary.get('total_target_table_refs', 0)} target table refs"
        )

    # 5) dependency graph
    dep_graph = build_dependency_graph(repo_root, files_index, workflows_blob)
    _write_json(artifacts_dir / "dependency_graph.json", dep_graph)

    # 6) complexity (now with database context)
    complexity = score_repository(
        files_index, 
        workflows_blob, 
        findings, 
        lineage, 
        rubric,
        database_context=database_context  # NEW: Pass database context
    )
    _write_json(artifacts_dir / "complexity.json", complexity)

    # 7) repo summary (enhanced with database stats)
    repo_summary = {
        "repo_root": str(repo_root),
        "generated_at_epoch": int(time.time()),
        "file_count": len(files_index),
        "workflow_count": len(workflows),
        "coordinator_count": len(coordinators),
        "bundle_count": len(bundles),
        "has_streaming": has_streaming_repo(files_index, workflows_blob),
        "has_dynamic_sql": has_dynamic_sql_repo(repo_root, files_index),
        # NEW: Add database statistics to summary
        "database_count": database_context.get("summary", {}).get("total_databases", 0),
        "source_table_refs": database_context.get("summary", {}).get("total_source_table_refs", 0),
        "target_table_refs": database_context.get("summary", {}).get("total_target_table_refs", 0),
        "elapsed_seconds": round(time.time() - t0, 2),
    }
    _write_json(artifacts_dir / "repo_summary.json", repo_summary)

    _write_json(artifacts_dir / "files_index.json", files_index)

    # 8) resolution pass
    resolved = resolve_repository(
        files_index=files_index,
        repo_root=repo_root,
        raw_findings=findings,
        raw_workflows=workflows_blob,
        raw_lineage=lineage,
    )
    _write_json(artifacts_dir / "resolved.json", resolved.get("resolved_variables", []))
    _write_json(artifacts_dir / "partially_resolved.json", resolved.get("partially_resolved_variables", []))
    _write_json(artifacts_dir / "unresolved.json", resolved.get("still_unresolved_variables", []))

    # also write resolved versions for rendering
    _write_json(artifacts_dir / "findings_resolved.json", resolved.get("resolved_findings", findings))
    _write_json(artifacts_dir / "workflows_resolved.json", resolved.get("resolved_workflows", workflows_blob))
    _write_json(artifacts_dir / "lineage_resolved.json", resolved.get("resolved_lineage", lineage))

    # 9) HTML report (render with resolved blobs AND database context)
    render_html_report(
        out_path=out_dir / "report.html",
        repo_summary=repo_summary,
        files_index=files_index,
        workflows=resolved.get("resolved_workflows", workflows_blob),
        findings=resolved.get("resolved_findings", findings),
        lineage=resolved.get("resolved_lineage", lineage),
        complexity=complexity,
        resolved_vars=resolved.get("resolved_variables", []),
        partial_vars=resolved.get("partially_resolved_variables", []),
        unresolved_vars=resolved.get("still_unresolved_variables", []),
        database_context=database_context,  # NEW: Pass database context to report
    )
