import yaml
from pathlib import Path
from typing import Any, Dict

def load_yaml(path: Path) -> Dict[str,Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def load_defaults(pkg_root: Path) -> Dict[str,Any]:
    return load_yaml(pkg_root / "config" / "defaults.yml")

def load_patterns(pkg_root: Path) -> Dict[str,Any]:
    base = pkg_root / "config" / "patterns"
    return {
        "secrets": load_yaml(base / "secrets.yml"),
        "connections": load_yaml(base / "connections.yml"),
        "paths": load_yaml(base / "paths.yml"),
        "languages": load_yaml(base / "languages.yml"),
    }

def load_rubric(pkg_root: Path) -> Dict[str,Any]:
    base = pkg_root / "config" / "rubrics"
    return load_yaml(base / "complexity.yml")
