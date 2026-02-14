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
    database_context: Dict[str, Any] = None,  # NEW: Database context parameter
) -> None:
    templates_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template("report.html.j2")

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
        database_context=database_context or {},  # NEW: Pass database context to template
    )
    out_path.write_text(html, encoding="utf-8")
