from typing import Dict, List, Tuple
from ...models.schema import Complexity

def classify_level(score: int, thresholds: Dict[str,int]) -> str:
    if score <= thresholds.get("low_max",39):
        return "low"
    if score <= thresholds.get("medium_max",69):
        return "medium"
    return "high"

def score_item(item_id: str, dims: Dict[str,int], thresholds: Dict[str,int], reasons: List[str], flags: List[str]) -> Complexity:
    total = sum(dims.values())
    lvl = classify_level(total, thresholds)
    return Complexity(item_id=item_id, level=lvl, total_score=total, dimension_scores=dims, top_reasons=reasons[:7], risk_flags=flags)


from dataclasses import asdict
from typing import Any

def score_repository(
    files_index: List[Dict[str, Any]],
    workflows_blob: Dict[str, Any],
    findings: Dict[str, Any],
    lineage: List[Dict[str, Any]],
    rubric: Dict[str, Any],
    database_context: Dict[str, Any] = None,
    sql_complexity_summary: Dict[str, Any] = None,  # NEW: SQL complexity parameter
) -> Dict[str, Any]:
    """
    Compute comprehensive complexity score for the repository.
    
    Now includes:
    - Database/schema complexity
    - SQL query complexity
    - All existing metrics
    """
    pts = (rubric or {}).get("points", {}) or {}
    thresholds = (rubric or {}).get("thresholds", {}) or {}

    # 1) repo-wide signals
    languages = set()
    for f in files_index or []:
        dt = (f.get("detected_type") or "").lower()
        if dt in ("python","scala","java","pig","hql","sql","shell","notebook_jupyter","notebook_zeppelin","impala"):
            languages.add(dt)

    # findings counts
    secrets_n = len((findings or {}).get("secrets", []) or [])
    conns_n = len((findings or {}).get("connections", []) or [])
    urls_n = len((findings or {}).get("urls", []) or []) + len((findings or {}).get("url_patterns", []) or [])
    jdbc_n = len((findings or {}).get("jdbc", []) or []) + len((findings or {}).get("jdbc_patterns", []) or [])

    streaming_flag = False
    dynamic_sql_flag = False
    for f in files_index or []:
        if f.get("has_streaming"):
            streaming_flag = True
        if f.get("has_dynamic_sql"):
            dynamic_sql_flag = True

    # 2) repo overview complexity
    repo_dims: Dict[str,int] = {"orchestration": 0, "technology": 0, "data": 0, "operational": 0, "security": 0}
    repo_reasons: List[str] = []
    repo_flags: List[str] = []

    wfs = (workflows_blob or {}).get("workflows", []) or []
    coords = (workflows_blob or {}).get("coordinators", []) or []

    # orchestration: workflows/actions/control flow
    action_count = 0
    control_flow = 0
    subwf = 0
    for wf in wfs:
        actions = wf.get("actions", []) or []
        action_count += len(actions)
        if wf.get("has_fork_join"): control_flow += 1
        if wf.get("has_decision"): control_flow += 1
        for a in actions:
            if a.get("subworkflow_app_path"): subwf += 1

    if action_count:
        repo_dims["orchestration"] += action_count * int(pts.get("oozie_action", 2))
        repo_reasons.append(f"Oozie actions detected: {action_count}")
    if control_flow:
        repo_dims["orchestration"] += control_flow * int(pts.get("oozie_control_flow", 8))
        repo_reasons.append(f"Control-flow nodes (fork/join/decision): {control_flow}")
    if subwf:
        repo_dims["orchestration"] += subwf * int(pts.get("oozie_subworkflow", 6))
        repo_reasons.append(f"Sub-workflows detected: {subwf}")

    # technology: multi-language + external systems
    if len(languages) >= 2:
        repo_dims["technology"] += int(pts.get("multi_language_bonus", 10))
        repo_reasons.append(f"Multiple languages detected: {', '.join(sorted(languages))}")
    if conns_n or jdbc_n:
        repo_dims["technology"] += int(pts.get("external_system_bonus", 5))
        repo_reasons.append(f"External connections/JDBC hints: connections={conns_n}, jdbc={jdbc_n}")

    # data: lineage volume + dynamic SQL
    if lineage:
        repo_dims["data"] += min(25, len(lineage))
        repo_reasons.append(f"SQL lineage entries: {len(lineage)}")
    if dynamic_sql_flag:
        repo_dims["data"] += int(pts.get("dynamic_sql_bonus", 10))
        repo_flags.append("dynamic_sql")
        repo_reasons.append("Dynamic SQL patterns detected")

    # Database complexity factors
    if database_context:
        db_summary = database_context.get("summary", {})
        num_databases = db_summary.get("total_databases", 0)
        num_source_refs = db_summary.get("total_source_table_refs", 0)
        num_target_refs = db_summary.get("total_target_table_refs", 0)
        
        if num_databases >= 5:
            repo_dims["data"] += int(pts.get("multi_database_bonus", 10))
            repo_reasons.append(f"Multiple databases detected: {num_databases}")
            repo_flags.append("multi_database")
        
        if num_source_refs > 50 or num_target_refs > 20:
            repo_dims["data"] += int(pts.get("high_table_usage_bonus", 8))
            repo_reasons.append(f"High table usage: {num_source_refs} reads, {num_target_refs} writes")
        
        files_by_db = database_context.get("files_by_database", {})
        cross_db_files = sum(1 for db_list in files_by_db.values() if len(db_list) > 1)
        if cross_db_files > 0:
            repo_dims["data"] += min(10, cross_db_files)
            repo_reasons.append(f"Cross-database activity detected in multiple files")
            repo_flags.append("cross_database")

    # SQL Complexity factors (NEW)
    if sql_complexity_summary:
        queries_analyzed = sql_complexity_summary.get("queries_analyzed", 0)
        avg_complexity = sql_complexity_summary.get("average_complexity_score", 0)
        dist = sql_complexity_summary.get("complexity_distribution", {})
        complex_count = dist.get("complex", 0) + dist.get("very_complex", 0)
        
        # Add points based on SQL complexity
        if queries_analyzed > 0:
            # Average complexity factor
            if avg_complexity > 50:
                repo_dims["data"] += int(pts.get("high_sql_complexity_bonus", 15))
                repo_reasons.append(f"High average SQL complexity: {avg_complexity:.1f}")
                repo_flags.append("complex_sql")
            elif avg_complexity > 30:
                repo_dims["data"] += int(pts.get("moderate_sql_complexity_bonus", 8))
                repo_reasons.append(f"Moderate SQL complexity: {avg_complexity:.1f}")
            
            # Complex query count factor
            if complex_count > 10:
                repo_dims["data"] += int(pts.get("many_complex_queries_bonus", 10))
                repo_reasons.append(f"Many complex SQL queries: {complex_count}")
            elif complex_count > 5:
                repo_dims["data"] += int(pts.get("some_complex_queries_bonus", 5))
                repo_reasons.append(f"Some complex SQL queries: {complex_count}")
            
            # SQL-specific risk flags
            risk_flags = sql_complexity_summary.get("risk_flag_summary", {})
            if risk_flags.get("correlated_subqueries", 0) > 5:
                repo_dims["data"] += 8
                repo_flags.append("correlated_subqueries")
                repo_reasons.append("Multiple correlated subqueries detected")
            
            if risk_flags.get("cross_join", 0) > 0:
                repo_dims["data"] += 10
                repo_flags.append("cross_joins")
                repo_reasons.append("CROSS JOINs detected (performance risk)")
            
            if risk_flags.get("many_joins", 0) > 3:
                repo_dims["data"] += 6
                repo_flags.append("many_joins")
                repo_reasons.append("Queries with many JOINs detected")

    # operational: streaming + schedule density
    if streaming_flag:
        repo_dims["operational"] += int(pts.get("streaming_bonus", 15))
        repo_flags.append("streaming")
        repo_reasons.append("Streaming hints detected (Kafka/Spark streaming/etc.)")
    
    high_freq = 0
    for c in coords:
        freq = (c.get("frequency") or "").lower()
        if "minute" in freq or freq.strip() in ("5","10","15","30"):
            high_freq += 1
    if high_freq:
        repo_dims["operational"] += high_freq * int(pts.get("schedule_high_freq_bonus", 8))
        repo_reasons.append(f"High-frequency coordinators: {high_freq}")

    # security: secrets and URLs (tokens)
    if secrets_n:
        repo_dims["security"] += int(pts.get("secrets_bonus", 15))
        repo_flags.append("secrets")
        repo_reasons.append(f"Potential secrets detected: {secrets_n}")
    if urls_n:
        repo_dims["security"] += min(10, urls_n)
        repo_reasons.append(f"URLs detected: {urls_n}")

    repo_item = score_item("repo_overview", repo_dims, thresholds, repo_reasons, repo_flags)

    # 3) per-workflow complexity items
    items = [asdict(repo_item)]
    for wf in wfs:
        wf_name = wf.get("name") or wf.get("source_file") or "workflow"
        dims = {"orchestration": 0, "technology": 0, "data": 0, "operational": 0, "security": 0}
        reasons: List[str] = []
        flags: List[str] = []

        acts = wf.get("actions", []) or []
        dims["orchestration"] += len(acts) * int(pts.get("oozie_action", 2))
        if wf.get("has_fork_join") or wf.get("has_decision"):
            dims["orchestration"] += int(pts.get("oozie_control_flow", 8))
            reasons.append("Has fork/join/decision control flow")
        subc = sum(1 for a in acts if a.get("subworkflow_app_path"))
        if subc:
            dims["orchestration"] += subc * int(pts.get("oozie_subworkflow", 6))
            reasons.append(f"Sub-workflows: {subc}")
        reasons.append(f"Actions: {len(acts)}")
        items.append(asdict(score_item(f"workflow:{wf_name}", dims, thresholds, reasons, flags)))

    for c in coords:
        c_name = c.get("name") or c.get("source_file") or "coordinator"
        dims = {"orchestration": 0, "technology": 0, "data": 0, "operational": 0, "security": 0}
        reasons: List[str] = []
        flags: List[str] = []
        freq = (c.get("frequency") or "")
        if freq:
            reasons.append(f"frequency={freq}")
            if "minute" in freq.lower():
                dims["operational"] += int(pts.get("schedule_high_freq_bonus", 8))
                reasons.append("High-frequency schedule")
        if c.get("workflow_app_path"):
            dims["orchestration"] += 4
            reasons.append("Triggers workflow-app")
        items.append(asdict(score_item(f"coordinator:{c_name}", dims, thresholds, reasons, flags)))

    return {
        "repo_level": repo_item.level,
        "repo_score": repo_item.total_score,
        "items": items,
    }
