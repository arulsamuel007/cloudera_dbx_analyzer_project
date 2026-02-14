from dataclasses import asdict
from typing import Any, Dict, List, Optional
from ...models.schema import Graph, GraphEdge

def ensure_node(g: Graph, node_id: str, node_type: str, meta: Optional[Dict[str,Any]]=None):
    if node_id not in g.nodes:
        g.nodes[node_id] = {"id": node_id, "type": node_type, "meta": meta or {}}

def add_edge(g: Graph, src: str, dst: str, edge_type: str, confidence: str="med", evidence: Optional[str]=None):
    g.edges.append(GraphEdge(src=src, dst=dst, edge_type=edge_type, confidence=confidence, evidence=evidence))

# -------------------------------------------------------------------
# Compatibility wrapper expected by pipeline
# -------------------------------------------------------------------

def build_dependency_graph(repo_root, files_index, workflows_blob):
    """
    Minimal dependency graph builder for Phase-1.

    Builds nodes/edges from:
      - workflows_blob["workflows"]
      - workflows_blob["coordinators"]
      - file references inside workflow actions (main/script/args)
      - coordinator workflow_app_path
    """
    g = Graph(nodes={}, edges=[])

    # Index valid repo files for quick existence check (relative paths)
    repo_files = set()
    for f in files_index or []:
        p = f.get("path")
        if p:
            repo_files.add(p.replace("\\", "/"))

    # Helpers
    def _node_id(prefix: str, val: str) -> str:
        return f"{prefix}:{val}"

    def _as_str(x):
        return x if isinstance(x, str) else None

    # Workflows
    for wf in (workflows_blob or {}).get("workflows", []):
        wf_name = wf.get("name") or wf.get("id") or "workflow"
        wf_src = wf.get("source_file") or wf.get("file") or ""
        wf_node = _node_id("workflow", wf_name)

        ensure_node(g, wf_node, "workflow", {"source_file": wf_src})

        # app path / workflow path reference
        for key in ("app_path", "workflow_path"):
            v = _as_str(wf.get(key))
            if v:
                ensure_node(g, _node_id("path", v), "path", {"kind": key})
                add_edge(g, wf_node, _node_id("path", v), "references", evidence=f"{key}={v}")

        # actions
        for act in wf.get("actions", []):
            act_name = act.get("name") or act.get("type") or "action"
            act_node = _node_id("action", f"{wf_name}/{act_name}")
            ensure_node(g, act_node, "action", {"type": act.get("type"), "source_file": wf_src})
            add_edge(g, wf_node, act_node, "contains", evidence=f"workflow={wf_name}")

            for key in ("main", "script", "class", "args"):
                v = _as_str(act.get(key))
                if v:
                    ensure_node(g, _node_id("ref", v), "reference", {"kind": key})
                    add_edge(g, act_node, _node_id("ref", v), "references", evidence=f"{key}={v}")

    # Coordinators
    for c in (workflows_blob or {}).get("coordinators", []):
        c_name = c.get("name") or c.get("id") or "coordinator"
        c_src = c.get("source_file") or c.get("file") or ""
        c_node = _node_id("coordinator", c_name)

        ensure_node(g, c_node, "coordinator", {
            "source_file": c_src,
            "frequency": c.get("frequency"),
            "start": c.get("start"),
            "end": c.get("end"),
            "timezone": c.get("timezone"),
        })

        wf_app = _as_str(c.get("workflow_app_path"))
        if wf_app:
            ensure_node(g, _node_id("path", wf_app), "path", {"kind": "workflow_app_path"})
            add_edge(g, c_node, _node_id("path", wf_app), "triggers", evidence=f"workflow_app_path={wf_app}")

    return asdict(g)


