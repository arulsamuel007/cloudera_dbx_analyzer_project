from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape


def render_html_report(
    out_path: Path,
    repo_summary: Dict[str, Any],
    files_index: List[Dict[str, Any]],
    workflows: Dict[str, Any],
    findings: Dict[str, Any],
    lineage: List[Dict[str, Any]],
    complexity: Dict[str, Any],
    resolved_vars: List[Dict[str, Any]],
    partial_vars: List[Dict[str, Any]],
    unresolved_vars: List[Dict[str, Any]],
    database_context: Dict[str, Any] = None,
    sql_complexity_summary: Dict[str, Any] = None,  # NEW: SQL complexity parameter
) -> None:
    """
    Render comprehensive HTML report with all analysis results.
    
    Args:
        out_path: Path where HTML report will be written
        repo_summary: High-level repository statistics
        files_index: Complete file inventory
        workflows: Parsed Oozie workflows, coordinators, bundles
        findings: Pattern detection results (JDBC, URLs, etc.)
        lineage: SQL lineage information
        complexity: Repository complexity scores
        resolved_vars: Fully resolved variables
        partial_vars: Partially resolved variables
        unresolved_vars: Unresolved variables
        database_context: Database/schema inventory (optional)
        sql_complexity_summary: SQL complexity analysis (optional)
    """
    templates_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    
    # Use comprehensive template if it exists, otherwise fall back to basic
    try:
        tpl = env.get_template("report_comprehensive_v2.html.j2")
    except:
        try:
            tpl = env.get_template("report.html.j2")
        except:
            raise RuntimeError("No report template found")

    html = tpl.render(
        repo=repo_summary,
        files=files_index,
        workflows=workflows.get("workflows", []),
        coordinators=workflows.get("coordinators", []),
        bundles=workflows.get("bundles", []),
        findings=findings,
        lineage=lineage,
        complexity=complexity,
        resolved_vars=resolved_vars,
        partial_vars=partial_vars,
        unresolved_vars=unresolved_vars,
        database_context=database_context or {},
        sql_complexity_summary=sql_complexity_summary or {},  # NEW: Pass SQL complexity
    )
    out_path.write_text(html, encoding="utf-8")
