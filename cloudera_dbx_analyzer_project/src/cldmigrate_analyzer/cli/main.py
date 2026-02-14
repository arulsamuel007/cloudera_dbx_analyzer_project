import argparse
import os
from pathlib import Path
from datetime import datetime

from ..runtime.paths import compute_default_output_dir, to_runtime_fs_path
from ..utils.logging import setup_logger
from ..config.loader import load_defaults, load_patterns, load_rubric
from ..core.pipeline.analyze_repo import analyze_repository


def _parse_globs(s: str):
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts


def main():
    parser = argparse.ArgumentParser(
        prog="cloudera_dbx_analyzer",
        description="Analyze a Cloudera/Hadoop repository for Databricks migration (Phase-1).",
    )
    parser.add_argument("input_dir", help="Repository root directory to analyze")
    parser.add_argument(
        "--output",
        help="Output directory (defaults to <parent_of_input>/output_files)",
        default=None,
    )
    parser.add_argument(
        "--run-name",
        help="Run folder name under output root (defaults to timestamp)",
        default=None,
    )
    parser.add_argument(
        "--max-file-mb",
        type=int,
        default=None,
        help="Max file size (MB) to fully read/parse; larger files are sample-scanned",
    )
    parser.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Follow symlinks during scan (default off)",
    )
    parser.add_argument(
        "--include",
        default="",
        help="Comma-separated glob patterns to include, e.g. '*.xml,*.hql'",
    )
    parser.add_argument(
        "--exclude",
        default="",
        help="Comma-separated glob patterns to exclude (in addition to defaults)",
    )
    parser.add_argument(
        "--redaction-mode",
        choices=["strict", "balanced"],
        default=None,
        help="Redaction mode for secrets/credentials",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Do not keep JSON artifacts (HTML still written)",
    )
    parser.add_argument(
        "--unresolved-only",
        action="store_true",
        help="(Currently informational) Unresolved-focused outputs (report already includes unresolved sections)",
    )
    parser.add_argument("--html-title", default=None, help="(Not used in current renderer; report title is fixed)")
    parser.add_argument("--log-level", default="INFO", help="Log level (INFO/DEBUG/WARN/ERROR)")

    args = parser.parse_args()

    # Normalize paths for runtime FS access (dbfs:/ -> /dbfs/)
    input_dir = to_runtime_fs_path(args.input_dir)
    if not os.path.isdir(input_dir):
        raise SystemExit(
            f"Input directory does not exist or is not a directory: {args.input_dir} (normalized: {input_dir})"
        )

    pkg_root = Path(__file__).resolve().parents[1]  # cldmigrate_analyzer/
    defaults = load_defaults(pkg_root)
    patterns = load_patterns(pkg_root)
    rubric = load_rubric(pkg_root)

    # Merge CLI overrides into defaults (we map them to analyze_repository args below)
    if args.max_file_mb is not None:
        defaults["max_file_mb"] = args.max_file_mb
    if args.follow_symlinks:
        defaults["follow_symlinks"] = True
    if args.redaction_mode:
        defaults["redaction_mode"] = args.redaction_mode

    inc = _parse_globs(args.include)
    exc = _parse_globs(args.exclude)
    if inc:
        defaults["include_globs"] = inc
    if exc:
        defaults["exclude_globs"] = list(defaults.get("exclude_globs") or []) + exc

    output_root = args.output or compute_default_output_dir(args.input_dir, "output_files")
    output_root = to_runtime_fs_path(output_root)

    run_name = args.run_name or datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = Path(output_root) / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logger(run_dir / "logs" / "analyzer.log", level=args.log_level)
    logger.info("Starting analysis")
    logger.info("Input: %s", args.input_dir)
    logger.info("Normalized input: %s", input_dir)
    logger.info("Output: %s", str(run_dir))

    # New pipeline API (writes artifacts + report.html itself)
    analyze_repository(
        input_dir=input_dir,
        output_run_dir=str(run_dir),
        patterns=patterns,
        rubric=rubric,
        max_file_mb=int(defaults.get("max_file_mb") or 10),
        include_globs=list(defaults.get("include_globs") or []),
        exclude_globs=list(defaults.get("exclude_globs") or []),
        log=logger,
    )

    out_html = run_dir / "report.html"
    logger.info("Report written: %s", str(out_html))

    if args.unresolved_only:
        logger.info("--unresolved-only is noted; unresolved sections are already included in report/artifacts.")

    if args.no_json:
        # Remove artifacts folder if user asked
        art = run_dir / "artifacts"
        if art.exists():
            for p in art.glob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                art.rmdir()
            except Exception:
                pass
        logger.info("JSON artifacts disabled (--no-json).")

    logger.info("Done.")
    return 0
