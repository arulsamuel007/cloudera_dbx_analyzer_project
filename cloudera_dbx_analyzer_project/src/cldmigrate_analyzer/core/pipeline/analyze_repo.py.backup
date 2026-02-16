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
# Database/Schema Parser
from ..extraction.database_schema_parser import extract_databases_from_repository
# SQL Complexity Analyzer
from ..extraction.sql_complexity_analyzer import analyze_repository_sql_complexity
from ..dependency.graph import build_dependency_graph
from ..metrics.complexity import score_repository
from ..resolution.resolver import resolve_repository
from ...reporting.render import render_html_report


def _write_json(path: Path, obj: Any) -> None:
    """Write object to JSON file with pretty formatting"""
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
    """
    Comprehensive repository analysis with all analyzers.
    
    This pipeline orchestrates multiple analysis modules and ensures
    all outputs are captured for future reuse:
    
    1. File scanning and classification
    2. Oozie workflow parsing
    3. Pattern detection (JDBC, URLs, etc.)
    4. SQL lineage extraction
    5. Database/schema extraction (NEW)
    6. SQL complexity analysis (NEW)
    7. Dependency graph building
    8. Complexity scoring (enhanced with DB context)
    9. Variable resolution
    10. HTML report generation
    
    All intermediate results are saved as JSON artifacts for:
    - Future analysis
    - External tool integration
    - Historical comparison
    - Custom reporting
    """
    t0 = time.time()
    repo_root = Path(input_dir)

    out_dir = Path(output_run_dir)
    artifacts_dir = out_dir / "artifacts"
    logs_dir = out_dir / "logs"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    if log:
        log.info("=" * 80)
        log.info("Starting Comprehensive Repository Analysis")
        log.info("=" * 80)

    # ============================================
    # STEP 1: Repository Scanning & Classification
    # ============================================
    if log:
        log.info("Step 1/10: Scanning repository and classifying files...")
    
    files_index = scan_repository(
        repo_root=repo_root,
        max_file_mb=max_file_mb,
        include_globs=include_globs or [],
        exclude_globs=exclude_globs or [],
        patterns=patterns,
    )
    
    if log:
        log.info(f"  Found {len(files_index)} files")

    # ============================================
    # STEP 2: File Metrics
    # ============================================
    if log:
        log.info("Step 2/10: Computing file metrics...")
    
    files_index = compute_counts(repo_root, files_index)
    _write_json(artifacts_dir / "files_index.json", files_index)

    # ============================================
    # STEP 3: Oozie Workflow Parsing
    # ============================================
    if log:
        log.info("Step 3/10: Parsing Oozie workflows...")
    
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

    workflows_blob = {
        "workflows": workflows,
        "coordinators": coordinators,
        "bundles": bundles
    }
    _write_json(artifacts_dir / "workflows.json", workflows_blob)
    
    if log:
        log.info(f"  Workflows: {len(workflows)}, Coordinators: {len(coordinators)}, Bundles: {len(bundles)}")

    # ============================================
    # STEP 4: Pattern Detection
    # ============================================
    if log:
        log.info("Step 4/10: Detecting patterns (JDBC, URLs, Kafka, paths)...")
    
    findings = scan_repo_patterns(repo_root, files_index, patterns)
    _write_json(artifacts_dir / "findings.json", findings)
    
    if log:
        log.info(f"  JDBC: {findings.get('jdbc_count', 0)}, URLs: {findings.get('url_count', 0)}, "
                f"Kafka: {findings.get('kafka_bootstrap_count', 0)}")

    # ============================================
    # STEP 5: Basic SQL Lineage
    # ============================================
    if log:
        log.info("Step 5/10: Extracting SQL lineage...")
    
    lineage = extract_sql_lineage_repo(repo_root, files_index)
    _write_json(artifacts_dir / "lineage.json", lineage)
    
    if log:
        log.info(f"  Lineage entries: {len(lineage)}")

    # ============================================
    # STEP 6: Database & Schema Extraction (NEW)
    # ============================================
    if log:
        log.info("Step 6/10: Extracting database and schema information...")
    
    database_context = extract_databases_from_repository(repo_root, files_index)
    _write_json(artifacts_dir / "database_context.json", database_context)
    
    if log:
        db_summary = database_context.get("summary", {})
        log.info(f"  Databases: {db_summary.get('total_databases', 0)}, "
                f"Source tables: {db_summary.get('total_source_table_refs', 0)}, "
                f"Target tables: {db_summary.get('total_target_table_refs', 0)}")

    # ============================================
    # STEP 7: SQL Complexity Analysis (NEW)
    # ============================================
    if log:
        log.info("Step 7/10: Analyzing SQL complexity...")
    
    sql_complexity_summary = analyze_repository_sql_complexity(repo_root, files_index)
    _write_json(artifacts_dir / "sql_complexity_analysis.json", sql_complexity_summary)
    
    if log:
        log.info(f"  Queries analyzed: {sql_complexity_summary.get('queries_analyzed', 0)}")
        log.info(f"  Avg complexity score: {sql_complexity_summary.get('average_complexity_score', 0)}")
        dist = sql_complexity_summary.get('complexity_distribution', {})
        log.info(f"  Distribution - Simple: {dist.get('simple', 0)}, "
                f"Moderate: {dist.get('moderate', 0)}, "
                f"Complex: {dist.get('complex', 0)}, "
                f"Very Complex: {dist.get('very_complex', 0)}")

    # ============================================
    # STEP 8: Variable Extraction
    # ============================================
    if log:
        log.info("Step 8/10: Extracting variables...")
    
    variables = extract_variables_repo(repo_root, files_index)
    _write_json(artifacts_dir / "variables.json", variables)

    # ============================================
    # STEP 9: Dependency Graph
    # ============================================
    if log:
        log.info("Step 9/10: Building dependency graph...")
    
    dep_graph = build_dependency_graph(repo_root, files_index, workflows_blob)
    _write_json(artifacts_dir / "dependency_graph.json", dep_graph)

    # ============================================
    # STEP 10: Complexity Scoring (Enhanced)
    # ============================================
    if log:
        log.info("Step 10/10: Computing complexity scores...")
    
    complexity = score_repository(
        files_index,
        workflows_blob,
        findings,
        lineage,
        rubric,
        database_context=database_context,  # Pass DB context for enhanced scoring
        sql_complexity_summary=sql_complexity_summary  # Pass SQL complexity
    )
    _write_json(artifacts_dir / "complexity.json", complexity)

    # ============================================
    # STEP 11: Enhanced Repository Summary
    # ============================================
    if log:
        log.info("Generating repository summary...")
    
    repo_summary = {
        "repo_root": str(repo_root),
        "generated_at_epoch": int(time.time()),
        "file_count": len(files_index),
        
        # Workflow stats
        "workflow_count": len(workflows),
        "coordinator_count": len(coordinators),
        "bundle_count": len(bundles),
        
        # Feature flags
        "has_streaming": has_streaming_repo(files_index, workflows_blob),
        "has_dynamic_sql": has_dynamic_sql_repo(repo_root, files_index),
        
        # Database stats
        "database_count": database_context.get("summary", {}).get("total_databases", 0),
        "source_table_refs": database_context.get("summary", {}).get("total_source_table_refs", 0),
        "target_table_refs": database_context.get("summary", {}).get("total_target_table_refs", 0),
        
        # SQL complexity stats
        "sql_queries_analyzed": sql_complexity_summary.get("queries_analyzed", 0),
        "avg_sql_complexity": sql_complexity_summary.get("average_complexity_score", 0),
        "complex_queries_count": (
            sql_complexity_summary.get("complexity_distribution", {}).get("complex", 0) +
            sql_complexity_summary.get("complexity_distribution", {}).get("very_complex", 0)
        ),
        
        # Performance metrics
        "elapsed_seconds": round(time.time() - t0, 2),
    }
    _write_json(artifacts_dir / "repo_summary.json", repo_summary)

    # ============================================
    # STEP 12: Variable Resolution
    # ============================================
    if log:
        log.info("Resolving variables...")
    
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

    # Write resolved versions
    _write_json(artifacts_dir / "findings_resolved.json", resolved.get("resolved_findings", findings))
    _write_json(artifacts_dir / "workflows_resolved.json", resolved.get("resolved_workflows", workflows_blob))
    _write_json(artifacts_dir / "lineage_resolved.json", resolved.get("resolved_lineage", lineage))

    # ============================================
    # STEP 13: Master Manifest (NEW)
    # ============================================
    # Create a master manifest that lists all artifacts
    manifest = {
        "version": "2.0",
        "generated_at": int(time.time()),
        "repository": str(repo_root),
        "analysis_duration_seconds": repo_summary["elapsed_seconds"],
        "artifacts": {
            "core": {
                "files_index.json": "Complete file inventory with classifications",
                "repo_summary.json": "High-level repository statistics",
            },
            "workflows": {
                "workflows.json": "Parsed Oozie workflows, coordinators, and bundles",
                "dependency_graph.json": "Workflow and file dependencies",
            },
            "patterns": {
                "findings.json": "JDBC, URLs, Kafka, storage paths",
                "findings_resolved.json": "Findings with resolved variables",
            },
            "database": {
                "database_context.json": "Database, schema, and table inventory",
                "lineage.json": "Basic SQL lineage (source/target tables)",
                "lineage_resolved.json": "Lineage with resolved variables",
            },
            "sql_analysis": {
                "sql_complexity_analysis.json": "Comprehensive SQL complexity metrics",
            },
            "variables": {
                "variables.json": "Extracted variables",
                "resolved.json": "Fully resolved variables",
                "partially_resolved.json": "Partially resolved variables",
                "unresolved.json": "Unresolved variables",
            },
            "complexity": {
                "complexity.json": "Repository complexity scoring",
            },
        },
        "summary": repo_summary,
    }
    _write_json(artifacts_dir / "MANIFEST.json", manifest)

    # ============================================
    # STEP 14: HTML Report Generation
    # ============================================
    if log:
        log.info("Generating comprehensive HTML report...")
    
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
        database_context=database_context,  # Database inventory
        sql_complexity_summary=sql_complexity_summary,  # SQL complexity analysis
    )

    if log:
        log.info("=" * 80)
        log.info("Analysis Complete!")
        log.info(f"Report: {out_dir / 'report.html'}")
        log.info(f"Artifacts: {artifacts_dir}")
        log.info(f"Manifest: {artifacts_dir / 'MANIFEST.json'}")
        log.info("=" * 80)
