import re
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath, PurePosixPath
from typing import Optional, Tuple

_DBFS_URI = re.compile(r"^dbfs:/+", re.IGNORECASE)
_WIN_DRIVE = re.compile(r"^[A-Za-z]:\\")
_UNC = re.compile(r"^\\\\[^\\]+")

@dataclass(frozen=True)
class NormalizedPath:
    raw: str
    kind: str  # windows|posix
    fs_path: str  # path usable for local filesystem operations on the running OS (best-effort)

def normalize_input_path(p: str) -> NormalizedPath:
    if not p:
        raise ValueError("Input path is empty")
    raw = p.strip().strip('"').strip("'")

    # Databricks dbfs uri -> /dbfs/
    if _DBFS_URI.match(raw):
        norm = "/dbfs/" + _DBFS_URI.sub("", raw).lstrip("/")
        return NormalizedPath(raw=raw, kind="posix", fs_path=norm)

    # If looks like windows path, preserve semantics for parent computation
    if _WIN_DRIVE.match(raw) or _UNC.match(raw):
        wp = PureWindowsPath(raw)
        return NormalizedPath(raw=raw, kind="windows", fs_path=str(wp))

    # Otherwise treat as posix
    pp = PurePosixPath(raw)
    return NormalizedPath(raw=raw, kind="posix", fs_path=str(pp))

def compute_default_output_dir(input_dir: str, output_folder_name: str = "output_files") -> str:
    np = normalize_input_path(input_dir)
    if np.kind == "windows":
        parent = PureWindowsPath(np.fs_path).parent
        return str(parent / output_folder_name)
    parent = PurePosixPath(np.fs_path).parent
    return str(parent / output_folder_name)

def to_runtime_fs_path(p: str) -> str:
    # For now, only normalize dbfs:/ to /dbfs/. Volumes and /Workspace are already posix-like.
    return normalize_input_path(p).fs_path
