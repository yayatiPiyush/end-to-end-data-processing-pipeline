"""Pipeline orchestration: runs the stages in order and owns run-level concerns
(run id, timing, statistics, circuit breaker, transaction, run record).

    ingest -> drop exact duplicates -> transform -> validate -> resolve versions
           -> quality gate -> load (one transaction + integrity checks) -> report
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from . import dedupe, ingest, report, store, transform, validate
from .config import Config
from .errors import DataQualityError, IntegrityError, PipelineError
from .logging_utils import RUN_ID_FILTER

log = logging.getLogger("pipeline")


@dataclass
class EntityResult:
    """Everything one entity produced in a run, plus its statistics."""

    name: str
    loadable: pd.DataFrame = field(default_factory=pd.DataFrame)
    quarantine: list[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_run_id(now: datetime) -> str:
    return f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


@contextmanager
def _timed(timings: dict, stage: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        timings[stage] = round(timings.get(stage, 0.0) + time.perf_counter() - t0, 4)


def _iso_date(v: object) -> str | None:
    return None if v is None or pd.isna(v) else pd.Timestamp(v).strftime("%Y-%m-%d")


def _iso_ts(v: object) -> str | None:
    return None if v is None or pd.isna(v) else pd.Timestamp(v).strftime("%Y-%m-%dT%H:%M:%S")


def _int(v: object) -> int | None:
    return None if v is None or pd.isna(v) else int(v)


def _txt(v: object) -> str | None:
    return None if v is None or (not isinstance(v, str) and pd.isna(v)) else str(v)


def _flags(v: list) -> str | None:
    return ",".join(v) if v else None


def customer_records(df: pd.DataFrame) -> list[dict]:
    return [{
        "customer_id": r.customer_id, "name": _txt(r.name), "email": _txt(r.email),
        "signup_date": _iso_date(r.signup_date), "city": _txt(r.city),
        "country_code": _txt(r.country_code), "source_updated_at": _iso_ts(r.source_updated_at),
        "dq_flags": _flags(r.dq_flags),
    } for r in df.itertuples(index=False)]


def order_records(df: pd.DataFrame) -> list[dict]:
    return [{
        "order_id": r.order_id, "customer_id": r.customer_id, "order_date": _iso_date(r.order_date),
        "sku": r.sku, "quantity": _int(r.quantity), "unit_price_cents": _int(r.unit_price_cents),
        "total_cents": _int(r.total_cents), "currency": r.currency, "status": r.status,
        "source_updated_at": _iso_ts(r.source_updated_at), "dq_flags": _flags(r.dq_flags),
    } for r in df.itertuples(index=False)]


def _process(entity: str, ing: ingest.IngestResult, key: str, transform_fn,
             rules: list[validate.Rule], cfg: Config, timings: dict) -> EntityResult:
    """Run one entity through dedupe -> transform -> validate -> resolve versions."""
    res = EntityResult(entity)
    with _timed(timings, "transform"):
        raw, n_exact = dedupe.drop_exact_duplicates(ing.frame)
        cleaned = transform_fn(raw, cfg)
    with _timed(timings, "validate"):
        vr = validate.apply_rules(cleaned, rules)
        loadable, n_superseded, n_ties = dedupe.resolve_versions(vr.accepted, key)

    rule_sev = {r.name: r.severity for r in rules}
    reasons = Counter({k: v for k, v in vr.failures.items() if rule_sev[k] == "error"})
    for rej in ing.rejects:
        reasons.update(r.split("(")[0] for r in rej["reasons"])
    # Warnings and corrections are reported for rows actually loaded.
    loaded_warnings = Counter(f for flags in loadable["dq_flags"] for f in flags)
    corrections = Counter(t for tags in loadable["_corrections"] for t in tags)

    src = ing.source_file.name
    res.quarantine = (
        [{"raw": r["_raw"], "reasons": r["reasons"], "source_file": src,
          "source_line": int(r["_source_line"]), "record_key": None} for r in ing.rejects]
        + [{"raw": row["_raw"], "reasons": row["reasons"], "source_file": src,
            "source_line": int(row["_source_line"]), "record_key": row[key]}
           for row in vr.rejected[["_raw", "reasons", "_source_line", key]].to_dict("records")])

    considered = ing.lines_read - n_exact
    rejected_total = len(ing.rejects) + len(vr.rejected)
    res.loadable = loadable
    res.stats = {
        "source_file": src,
        "lines_read": ing.lines_read,
        "unreadable": len(ing.rejects),
        "exact_duplicates": n_exact,
        "rejected": len(vr.rejected),
        "superseded_versions": n_superseded,
        "timestamp_ties": n_ties,
        "unique_valid": len(loadable),
        "reject_rate": round(rejected_total / considered, 4) if considered else 0.0,
        "rows_with_corrections": int(loadable["_corrections"].map(bool).sum()),
        "rows_with_warnings": int(loadable["dq_flags"].map(bool).sum()),
        "corrections": dict(corrections.most_common()),
        "warnings": dict(loaded_warnings.most_common()),
        "rejection_reasons": dict(reasons.most_common()),
    }
    # Reconciliation: every input line is accounted for exactly once.
    s = res.stats
    accounted = s["unreadable"] + s["exact_duplicates"] + s["rejected"] + s["superseded_versions"] + s["unique_valid"]
    s["reconciled"] = accounted == s["lines_read"]
    if not s["reconciled"]:
        raise IntegrityError(f"{entity}: {s['lines_read']} lines read but {accounted} accounted for")
    log.info("%s: read=%d unreadable=%d exact_dup=%d rejected=%d superseded=%d valid=%d (reject rate %.1f%%)",
             entity, s["lines_read"], s["unreadable"], s["exact_duplicates"], s["rejected"],
             s["superseded_versions"], s["unique_valid"], 100 * s["reject_rate"])
    return res


def _quality_gate(results: list[EntityResult], cfg: Config) -> None:
    breaches = [f"{r.name} reject rate {r.stats['reject_rate']:.1%} > "
                f"{cfg.max_reject_rate[r.name]:.1%}"
                for r in results if r.stats["reject_rate"] > cfg.max_reject_rate[r.name]]
    if breaches:
        raise DataQualityError("quality gate failed, nothing was written: " + "; ".join(breaches))


def run_pipeline(cfg: Config, dry_run: bool = False) -> dict:
    """Execute the full pipeline once. Returns the run statistics.

    Raises a :class:`PipelineError` subclass on handled failures; the run is
    still recorded in ``pipeline_runs`` and a report is still written.
    """
    started = _now()
    run_id = _new_run_id(started)
    RUN_ID_FILTER.run_id = run_id
    timings: dict[str, float] = {}
    stats: dict = {"run_id": run_id, "started_at": started.isoformat(timespec="seconds"),
                   "status": "running", "dry_run": dry_run,
                   "as_of_date": cfg.effective_as_of().isoformat(),
                   "config_file": str(cfg.source_file) if cfg.source_file else None,
                   "stage_seconds": timings}
    log.info("run started (dry_run=%s, as_of=%s)", dry_run, stats["as_of_date"])

    conn = store.connect(cfg.paths.database)
    in_tx = False
    try:
        store.init_schema(conn)
        store.start_run(conn, run_id, stats["started_at"], {})

        with _timed(timings, "ingest"):
            cust_in = ingest.read_customers_jsonl(cfg.paths.customers)
            ord_in = ingest.read_orders_csv(cfg.paths.orders)
        stats["inputs"] = {i.entity: {"path": str(i.source_file), "sha256": i.file_sha256}
                           for i in (cust_in, ord_in)}
        conn.execute("UPDATE pipeline_runs SET inputs = json(?) WHERE run_id = ?",
                     (report.to_json(stats["inputs"]), run_id))

        customers = _process("customers", cust_in, "customer_id", transform.transform_customers,
                             validate.customer_rules(cfg), cfg, timings)

        # Orders may reference customers from this run or from earlier runs.
        db_customers = store.known_customers(conn)
        signup = {k: pd.Timestamp(v) for k, v in db_customers.items() if v}
        signup.update({cid: d for cid, d in zip(customers.loadable["customer_id"],
                                                customers.loadable["signup_date"]) if pd.notna(d)})
        known_ids = set(db_customers) | set(customers.loadable["customer_id"])
        orders = _process("orders", ord_in, "order_id", transform.transform_orders,
                          validate.order_rules(cfg, known_ids, signup), cfg, timings)
        stats["entities"] = {"customers": customers.stats, "orders": orders.stats}

        _quality_gate([customers, orders], cfg)

        if dry_run:
            log.info("dry run: validation complete, skipping load")
        else:
            with _timed(timings, "load"):
                conn.execute("BEGIN IMMEDIATE")      # take the write lock up front
                in_tx = True
                for res, records in ((customers, customer_records(customers.loadable)),
                                     (orders, order_records(orders.loadable))):
                    up = store.upsert(conn, res.name, records, run_id)
                    res.stats["load"] = up.as_dict()
                    res.stats["quarantined_distinct"] = store.replace_quarantine(
                        conn, res.name, res.quarantine, run_id)
                    log.info("%s load: %s", res.name, up.as_dict())
                checks = store.run_post_load_checks(conn)
                stats["post_load_checks"] = checks
                failed = {k: v for k, v in checks.items() if v}
                if failed:
                    raise IntegrityError(f"post-load checks failed: {failed}")
                conn.execute("COMMIT")
                in_tx = False

        stats["status"] = "dry_run" if dry_run else "success"
    except BaseException as exc:
        if in_tx:
            conn.execute("ROLLBACK")
            log.error("transaction rolled back; database unchanged by this run")
        stats["status"] = "failed"
        stats["error"] = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, PipelineError):
            log.error("run failed: %s", exc)
        else:
            log.exception("run failed with an unexpected error")
        raise
    finally:
        finished = _now()
        stats["finished_at"] = finished.isoformat(timespec="seconds")
        stats["duration_seconds"] = round((finished - started).total_seconds(), 3)
        try:
            stats["tables"] = store.table_counts(conn)
            stats["fingerprints"] = {t: store.content_fingerprint(conn, t) for t in ("customers", "orders")}
            store.finish_run(conn, run_id, stats["finished_at"], stats["status"], stats, stats.get("error"))
        except Exception:                            # never mask the original error
            log.exception("could not record run outcome in pipeline_runs")
        conn.close()
        try:
            paths = report.write_reports(stats, cfg.paths.reports_dir)
            log.info("report written to %s", paths[-1])
        except Exception:
            log.exception("could not write run report")
        log.info("run finished: status=%s in %.2fs", stats["status"], stats["duration_seconds"])
    return stats
