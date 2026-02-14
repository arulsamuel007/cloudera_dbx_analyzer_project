import os
import fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Set, Tuple

@dataclass
class ScanConfig:
    include_globs: List[str]
    exclude_globs: List[str]
    skip_extensions: Set[str]
    follow_symlinks: bool
    max_file_bytes: int

def _match_any(path: str, patterns: List[str]) -> bool:
    for pat in patterns:
        if fnmatch.fnmatch(path, pat) or fnmatch.fnmatch(os.path.basename(path), pat):
            return True
    return False

def iter_files(root: str, cfg: ScanConfig) -> Iterator[str]:
    root_path = Path(root)
    for dirpath, dirnames, filenames in os.walk(root_path, followlinks=cfg.follow_symlinks):
        rel_dir = os.path.relpath(dirpath, root_path)
        rel_dir = "" if rel_dir == "." else rel_dir

        # prune excluded directories
        pruned = []
        for d in list(dirnames):
            rel = os.path.join(rel_dir, d).replace("\\", "/")
            if _match_any(rel + "/", cfg.exclude_globs):
                pruned.append(d)
        for d in pruned:
            dirnames.remove(d)

        for f in filenames:
            full = os.path.join(dirpath, f)
            rel = os.path.join(rel_dir, f).replace("\\", "/")
            ext = os.path.splitext(f)[1].lower()
            if ext in cfg.skip_extensions:
                continue
            if cfg.include_globs and not _match_any(rel, cfg.include_globs):
                continue
            if _match_any(rel, cfg.exclude_globs):
                continue
            try:
                st = os.stat(full)
            except OSError:
                continue
            yield full
            
def _detect_type_simple(full_path: str) -> str:
    """
    Lightweight detector (cross-platform) to support Phase-1.
    This avoids YAML regex issues and keeps the pipeline running.
    """
    p = Path(full_path)
    name = p.name.lower()
    ext = p.suffix.lower()

    # Oozie XMLs by common filenames
    if name == "workflow.xml":
        return "oozie_workflow_xml"
    if name == "coordinator.xml" or name.endswith("-coord.xml") or "coordinator" in name:
        # best-effort; real namespace sniffing happens later if you add it
        return "oozie_coordinator_xml" if ext == ".xml" else "xml_generic"
    if name == "bundle.xml":
        return "oozie_bundle_xml"

    # Common types
    if ext in (".properties", ".props"):
        return "properties"
    if ext in (".yml", ".yaml"):
        return "yaml"
    if ext == ".json":
        return "json"
    if ext in (".ini", ".cfg", ".conf"):
        return "ini_conf"
    if ext in (".sql", ".ddl", ".dml"):
        return "sql"
    if ext in (".hql", ".q"):
        return "hql"
    if ext == ".pig":
        return "pig"
    if ext in (".sh", ".bash", ".ksh"):
        return "shell"
    if ext == ".py":
        return "python"
    if ext == ".scala":
        return "scala"
    if ext == ".java":
        return "java"
    if ext == ".ipynb":
        return "notebook_jupyter"
    if ext == ".xml":
        # generic xml (oozie-specific already captured above by filename)
        return "xml_generic"

    # Build
    if name == "pom.xml":
        return "build_maven"
    if name in ("build.gradle", "settings.gradle"):
        return "build_gradle"
    if name == "build.sbt":
        return "build_sbt"
    if name == "requirements.txt":
        return "build_python_deps"

    return "other"


def scan_repository(
    repo_root: Path,
    max_file_mb: int = 10,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    patterns: dict | None = None,
):
    """
    Compatibility wrapper expected by the pipeline.
    Returns a list of dicts with minimal fields needed downstream.
    """
    include_globs = include_globs or []
    exclude_globs = exclude_globs or []

    skip_ext = {
        ".jar", ".class", ".so", ".dll", ".exe",
        ".zip", ".tar", ".gz", ".7z",
        ".parquet", ".orc", ".avro", ".snappy", ".zst",
        ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".docx", ".pptx", ".xlsx",
    }

    cfg = ScanConfig(
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        skip_extensions=skip_ext,
        follow_symlinks=False,
        max_file_bytes=max_file_mb * 1024 * 1024,
    )

    out = []
    root_str = str(repo_root)

    for full in iter_files(root_str, cfg):
        p = Path(full)
        try:
            rel = p.relative_to(repo_root).as_posix()
        except Exception:
            # fallback
            rel = str(p).replace("\\", "/")

        try:
            size = p.stat().st_size
        except Exception:
            size = None

        out.append(
            {
                "path": rel,
                "detected_type": _detect_type_simple(full),
                "parse_status": "ok",
                "size_bytes": size,
            }
        )
    return out
