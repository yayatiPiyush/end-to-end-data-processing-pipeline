"""Rough throughput benchmark: replicate the sample orders N times with fresh
order ids and time each stage (first load, then an idempotent re-run).

    python scripts/benchmark.py --factor 30      # ~96k order rows
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import replace
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.config import load_config  # noqa: E402
from pipeline.logging_utils import setup_logging  # noqa: E402
from pipeline.runner import run_pipeline  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--factor", type=int, default=30)
    args = ap.parse_args()
    setup_logging(None, "WARNING")
    base = replace(load_config(), as_of_date=date(2026, 10, 21))
    lines = base.paths.orders.read_text(encoding="utf-8").splitlines()
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        big = tmp / "orders_big.csv"
        out = [lines[0]]
        for k in range(args.factor):
            for line in lines[1:]:
                # Shift each id by k*5000 so copies never collide (sample ids are < 5000).
                oid, rest = line.split(",", 1)
                if oid.startswith("O") and oid[1:].isdigit():
                    oid = f"O{k * 5000 + int(oid[1:]):06d}"
                out.append(f"{oid},{rest}")
        big.write_text("\n".join(out) + "\n", encoding="utf-8")
        cfg = base.with_overrides(orders=big, database=tmp / "b.db", reports_dir=tmp / "r")
        print(f"order rows: {len(out) - 1:,}")
        for label in ("first load", "re-run"):
            s = run_pipeline(cfg)
            o = s["entities"]["orders"]
            rate = o["lines_read"] / s["duration_seconds"]
            print(f"{label:<11} total {s['duration_seconds']:.2f}s  ({rate:,.0f} rows/s)  stages: "
                  + ", ".join(f"{k}={v:.2f}s" for k, v in s["stage_seconds"].items())
                  + f"  load={o['load']}")


if __name__ == "__main__":
    main()
