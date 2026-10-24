"""Demonstrate behaviour across repeated executions on the full sample data.

Runs the pipeline five times against a throw-away database and prints a
Markdown table of what each run did:

1. first load
2. identical re-run                       -> nothing written, same fingerprints
3. changed input (3 orders updated upstream, 2 new orders)
4. replay of the original, older extract  -> stale versions refused
5. re-run of the changed input            -> no-op again

    python scripts/rerun_evidence.py
"""

from __future__ import annotations

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

CHANGES = {   # order_id -> new status; updated_at is moved forward
    "O000002": "delivered",
    "O000010": "returned",
    "O000020": "cancelled",
}
NEW_ROWS = [
    "O900001,C00001,2026-09-30,SKU-001,1,10.00,USD,pending,2026-09-30T12:00:00",
    "O900002,C00002,2026-09-30,SKU-002,2,$20.00,usd,Pending,2026-09-30T12:00:00",
]


def changed_orders(src: Path, dst: Path) -> None:
    lines = src.read_text(encoding="utf-8").splitlines()
    out, done = [lines[0]], set()
    for line in lines[1:]:
        f = line.split(",")
        if f[0] in CHANGES and f[0] not in done and len(f) == 9:
            f[7], f[8] = CHANGES[f[0]], "2026-10-01T00:00:00"
            done.add(f[0])
            line = ",".join(f)
        out.append(line)
    dst.write_text("\n".join(out + NEW_ROWS) + "\n", encoding="utf-8")


def main() -> None:
    setup_logging(None, "WARNING")
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        base = replace(load_config(), as_of_date=date(2026, 10, 21)).with_overrides(
            database=tmp / "w.db", reports_dir=tmp / "reports")
        changed = tmp / "orders_changed.csv"
        changed_orders(base.paths.orders, changed)
        steps = [
            ("1. first load", base),
            ("2. identical re-run", base),
            ("3. changed input", base.with_overrides(orders=changed)),
            ("4. replay original (older) extract", base),
            ("5. re-run changed input", base.with_overrides(orders=changed)),
        ]
        print("| Run | Entity | Inserted | Updated | Unchanged | Skipped stale | Rows in table | Fingerprint |")
        print("|---|---|---|---|---|---|---|---|")
        for label, cfg in steps:
            s = run_pipeline(cfg)
            for entity in ("customers", "orders"):
                ld = s["entities"][entity]["load"]
                print(f"| {label} | {entity} | {ld['inserted']} | {ld['updated']} | {ld['unchanged']} | "
                      f"{ld['skipped_stale']} | {s['tables'][entity]} | `{s['fingerprints'][entity]}` |")


if __name__ == "__main__":
    main()
