"""Stage 5 - Reporting.

Every run (successful or not) produces:

* ``reports/runs/<run_id>.json``   machine-readable statistics, kept per run
* ``reports/latest.json``          the same, for the most recent run
* ``reports/data_quality_report.md`` human-readable summary of the latest run
"""

from __future__ import annotations

import json
from pathlib import Path


def to_json(obj: object) -> str:
    return json.dumps(obj, indent=2, default=str, sort_keys=False)


def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "-"


def _table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


def render_markdown(stats: dict) -> str:
    ents = stats.get("entities", {})
    lines = [
        "# Data Quality Report",
        "",
        f"- **Run:** `{stats['run_id']}`  ",
        f"- **Status:** {stats['status']}" + (f" - {stats['error']}" if stats.get("error") else ""),
        f"- **Started:** {stats['started_at']} (duration {stats.get('duration_seconds', '?')} s)",
        f"- **As-of date for validation:** {stats.get('as_of_date')}",
        "",
    ]
    if not ents:
        lines += ["The run failed before any records were processed.", ""]
        return "\n".join(lines)

    lines += ["## Record flow", "",
              "Every input line ends up in exactly one bucket; *reconciled* confirms the buckets add up.", ""]
    keys = [("lines_read", "Lines read"), ("unreadable", "Unreadable (ingestion reject)"),
            ("exact_duplicates", "Exact duplicates dropped"), ("rejected", "Rejected by validation"),
            ("superseded_versions", "Older versions superseded"), ("unique_valid", "Unique valid records")]
    names = list(ents)
    rows = [[label] + [f"{ents[n][k]} ({_pct(ents[n][k], ents[n]['lines_read'])})" for n in names]
            for k, label in keys]
    rows.append(["Reject rate (excl. exact duplicates)"] + [f"{ents[n]['reject_rate']:.1%}" for n in names])
    rows.append(["Reconciled"] + ["yes" if ents[n]["reconciled"] else "**NO**" for n in names])
    lines += [_table(["Stage"] + names, rows), ""]

    if any("load" in ents[n] for n in names):
        lines += ["## Load result", "",
                  _table(["Outcome"] + names,
                         [[k] + [ents[n].get("load", {}).get(k, "-") for n in names]
                          for k in ("inserted", "updated", "unchanged", "skipped_stale")]
                         + [["quarantined (distinct)"] + [ents[n].get("quarantined_distinct", "-") for n in names]]),
                  ""]

    lines += ["## Corrections applied to loaded records", "",
              "A record is *corrected* when at least one value had to be normalized to load it.", ""]
    for n in names:
        e = ents[n]
        lines.append(f"**{n}**: {e['rows_with_corrections']} of {e['unique_valid']} records "
                     f"({_pct(e['rows_with_corrections'], e['unique_valid'])}) corrected")
        if e["corrections"]:
            lines += ["", _table(["Correction", "Records"], [[k, v] for k, v in e["corrections"].items()])]
        lines.append("")

    lines += ["## Warnings on loaded records", "",
              "Loaded, with the bad field set to NULL (or `unknown`) and the rule name stored in `dq_flags`.", ""]
    for n in names:
        e = ents[n]
        lines.append(f"**{n}**: {e['rows_with_warnings']} records flagged")
        if e["warnings"]:
            lines += ["", _table(["Rule", "Records"], [[k, v] for k, v in e["warnings"].items()])]
        lines.append("")

    lines += ["## Rejection reasons", "",
              "Counts are per rule; one record can fail several rules. Rejected records are in the "
              "`quarantine` table with their raw content and every reason.", ""]
    for n in names:
        e = ents[n]
        lines.append(f"**{n}**")
        if e["rejection_reasons"]:
            lines += ["", _table(["Reason", "Records"], [[k, v] for k, v in e["rejection_reasons"].items()])]
        else:
            lines.append("none")
        lines.append("")

    if "post_load_checks" in stats:
        lines += ["## Post-load integrity checks", "",
                  _table(["Check", "Violations"], [[k, v] for k, v in stats["post_load_checks"].items()]), ""]
    if "tables" in stats:
        lines += ["## Database state after run", "",
                  _table(["Table", "Rows"], [[k, v] for k, v in stats["tables"].items()]), "",
                  "Content fingerprints (identical across runs means identical data): "
                  + ", ".join(f"`{k}={v}`" for k, v in stats.get("fingerprints", {}).items()), ""]
    lines += ["## Stage timings (seconds)", "",
              _table(["Stage", "Seconds"], [[k, v] for k, v in stats.get("stage_seconds", {}).items()]), ""]
    return "\n".join(lines)


def write_reports(stats: dict, reports_dir: Path) -> list[Path]:
    runs = reports_dir / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    per_run = runs / f"{stats['run_id']}.json"
    latest = reports_dir / "latest.json"
    md = reports_dir / "data_quality_report.md"
    payload = to_json(stats)
    per_run.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    md.write_text(render_markdown(stats), encoding="utf-8")
    return [per_run, latest, md]
