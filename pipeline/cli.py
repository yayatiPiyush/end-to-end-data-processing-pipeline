"""Command-line entry point.

    python -m pipeline                 # run with config/pipeline.yaml
    python -m pipeline run --dry-run   # validate and report, write nothing
    python -m pipeline rules           # print the validation rule catalogue

Exit codes: 0 success, 1 unexpected error, 2 config/ingestion error,
3 quality gate failed, 4 integrity check failed.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

from .config import load_config
from .errors import PipelineError
from .logging_utils import setup_logging
from .runner import run_pipeline
from .validate import rule_catalogue


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m pipeline",
                                description="E-commerce orders data pipeline")
    p.add_argument("command", nargs="?", default="run", choices=["run", "rules"])
    p.add_argument("--config", help="path to YAML config (default: config/pipeline.yaml)")
    p.add_argument("--customers", help="override customers input file")
    p.add_argument("--orders", help="override orders input file")
    p.add_argument("--db", help="override SQLite database path")
    p.add_argument("--reports-dir", help="override reports directory")
    p.add_argument("--logs-dir", help="override logs directory")
    p.add_argument("--as-of", type=date.fromisoformat, help="validation reference date (YYYY-MM-DD)")
    p.add_argument("--dry-run", action="store_true", help="run every stage except the database write")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        cfg = load_config(args.config)
    except PipelineError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return exc.exit_code
    cfg = cfg.with_overrides(customers=args.customers, orders=args.orders,
                             database=args.db, reports_dir=args.reports_dir,
                             logs_dir=args.logs_dir)
    if args.as_of:
        cfg = replace(cfg, as_of_date=args.as_of)

    if args.command == "rules":
        print("| Entity | Rule | Severity | Meaning |\n|---|---|---|---|")
        for row in rule_catalogue(cfg):
            print("| " + " | ".join(row) + " |")
        return 0

    setup_logging(cfg.paths.logs_dir, args.log_level)
    try:
        stats = run_pipeline(cfg, dry_run=args.dry_run)
    except PipelineError as exc:
        return exc.exit_code
    except Exception:
        return 1
    for name, e in stats.get("entities", {}).items():
        load = e.get("load", {})
        print(f"{name:<10} read={e['lines_read']:<5} valid={e['unique_valid']:<5} "
              f"rejected={e['unreadable'] + e['rejected']:<4} duplicates={e['exact_duplicates'] + e['superseded_versions']:<4} "
              f"inserted={load.get('inserted', '-')} updated={load.get('updated', '-')} "
              f"unchanged={load.get('unchanged', '-')}")
    report_path = cfg.paths.reports_dir / "data_quality_report.md"
    try:
        report_path = report_path.relative_to(Path.cwd())
    except ValueError:
        pass
    print(f"status={stats['status']}  report={report_path}")
    return 0
