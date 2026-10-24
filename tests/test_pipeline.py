"""End-to-end tests: golden output, idempotency, changes, failures and recovery."""

import json
import shutil
from dataclasses import replace

import pandas as pd
import pytest

from pipeline import runner, store
from pipeline.cli import main
from pipeline.errors import DataQualityError, IngestionError, IntegrityError

from .conftest import FIXTURES, business_rows, read_table

EXPECTED_ORDER_REJECTS = {
    "O000004": ["customer_unknown"],
    "O000005": ["currency_missing", "order_date_unparseable", "quantity_not_positive", "unit_price_invalid"],
    "O000006": ["currency_unsupported", "order_date_in_future"],
    None: ["order_id_missing"],
    "O000008": ["quantity_not_integer", "unit_price_negative"],
    "O000011": ["customer_unknown"],                 # customer C00004 was malformed JSON
    "O000013": ["order_date_too_old"],
    "O000014": ["sku_invalid"],
    "O000015": ["unit_price_invalid"],               # 3 decimal places
    "O000012": ["wrong_field_count(3!=9)"],          # unreadable CSV row
}


def _expected(name: str) -> pd.DataFrame:
    return pd.read_csv(FIXTURES / name, dtype=str, keep_default_na=False)


def _actual(db, table, columns) -> pd.DataFrame:
    df = read_table(db, table)[columns].sort_values(columns[0]).reset_index(drop=True)
    return df.astype(object).where(df.notna(), "").astype(str)


# --- representative input -> expected output --------------------------------

def test_golden_output(cfg):
    stats = runner.run_pipeline(cfg)
    db = cfg.paths.database
    pd.testing.assert_frame_equal(_actual(db, "customers", store.CUSTOMER_COLUMNS),
                                  _expected("expected_customers.csv"))
    pd.testing.assert_frame_equal(_actual(db, "orders", store.ORDER_COLUMNS),
                                  _expected("expected_orders.csv"))
    assert stats["status"] == "success"


def test_quarantine_contents(cfg):
    runner.run_pipeline(cfg)
    q = read_table(cfg.paths.database, "quarantine")
    orders = q[q.entity == "orders"]
    got = {}
    for _, r in orders.iterrows():
        key = r.record_key if pd.notna(r.record_key) else ("O000012" if "O000012" in r.raw_record else None)
        got[key] = sorted(r.reasons.split(","))
    assert got == {k: sorted(v) for k, v in EXPECTED_ORDER_REJECTS.items()}
    cust = q[q.entity == "customers"]
    assert sorted(cust.reasons) == ["customer_id_missing", "malformed_json(Unterminated string starting at)"]


def test_statistics_reconcile(cfg):
    stats = runner.run_pipeline(cfg)
    c, o = stats["entities"]["customers"], stats["entities"]["orders"]
    assert (c["lines_read"], c["unreadable"], c["exact_duplicates"], c["rejected"],
            c["superseded_versions"], c["unique_valid"]) == (8, 1, 1, 1, 1, 4)
    assert (o["lines_read"], o["unreadable"], o["exact_duplicates"], o["rejected"],
            o["superseded_versions"], o["unique_valid"]) == (18, 1, 1, 9, 1, 6)
    assert c["reconciled"] and o["reconciled"]
    assert o["load"] == {"inserted": 6, "updated": 0, "unchanged": 0, "skipped_stale": 0}
    assert all(v == 0 for v in stats["post_load_checks"].values())


# --- repeated execution ------------------------------------------------------

def test_rerun_is_idempotent(cfg):
    first = runner.run_pipeline(cfg)
    snapshot = {t: business_rows(cfg.paths.database, t) for t in ("customers", "orders")}
    second = runner.run_pipeline(cfg)
    for entity in ("customers", "orders"):
        load = second["entities"][entity]["load"]
        assert load["inserted"] == load["updated"] == load["skipped_stale"] == 0
        assert business_rows(cfg.paths.database, entity) == snapshot[entity]
    assert first["fingerprints"] == second["fingerprints"]
    assert second["tables"]["quarantine"] == first["tables"]["quarantine"]


def test_rerun_after_input_change_updates_only_what_changed(cfg):
    runner.run_pipeline(cfg)
    path = cfg.paths.orders
    path.write_text(path.read_text().replace(
        "O000003,C00003,2025-04-01,SKU-003,1,10.00,EUR,lost?,2025-04-02T00:00:00",
        "O000003,C00003,2025-04-01,SKU-003,1,10.00,EUR,delivered,2025-04-09T00:00:00")
        + "O000099,C00001,2025-08-01,SKU-009,1,9.99,USD,pending,2025-08-02T00:00:00\n")
    stats = runner.run_pipeline(cfg)
    assert stats["entities"]["orders"]["load"] == {"inserted": 1, "updated": 1, "unchanged": 5, "skipped_stale": 0}
    row = read_table(cfg.paths.database, "orders").set_index("order_id").loc["O000003"]
    assert row.status == "delivered" and pd.isna(row.dq_flags)    # warning cleared


def test_replaying_old_extract_does_not_regress_data(cfg, tmp_path):
    old = tmp_path / "old_orders.csv"
    shutil.copy(cfg.paths.orders, old)
    path = cfg.paths.orders
    path.write_text(path.read_text().replace(
        "O000001,C00001,2025-03-01,SKU-001,2,\"$1,299.00\",USD,Shipped,2025-03-02T00:00:00",
        "O000001,C00001,2025-03-01,SKU-001,2,\"$1,299.00\",USD,delivered,2025-03-09T00:00:00"))
    runner.run_pipeline(cfg)
    stats = runner.run_pipeline(cfg.with_overrides(orders=old))
    assert stats["entities"]["orders"]["load"]["skipped_stale"] == 1
    row = read_table(cfg.paths.database, "orders").set_index("order_id").loc["O000001"]
    assert row.status == "delivered"


def test_orphan_order_loads_once_its_customer_arrives(cfg):
    runner.run_pipeline(cfg)
    with open(cfg.paths.customers, "a") as fh:
        fh.write('{"customer_id": "C99999", "name": "Late", "email": "late@example.com", '
                 '"signup_date": "2024-01-01", "address": {"city": "Pune", "country": "IN"}, '
                 '"updated_at": "2025-01-01T00:00:00"}\n')
    stats = runner.run_pipeline(cfg)
    assert stats["entities"]["orders"]["load"]["inserted"] == 1        # O000004
    q = read_table(cfg.paths.database, "quarantine")
    assert "O000004" not in set(q.record_key)                          # left quarantine


def test_customers_from_earlier_runs_satisfy_foreign_keys(cfg, tmp_path):
    runner.run_pipeline(cfg)
    only_c1 = tmp_path / "c.jsonl"
    only_c1.write_text(cfg.paths.customers.read_text().splitlines()[0] + "\n")
    stats = runner.run_pipeline(cfg.with_overrides(customers=only_c1))
    # C00002/C00003/C00005 are not in this run's file but exist in the DB.
    assert stats["entities"]["orders"]["rejection_reasons"]["customer_unknown"] == 2   # same as before
    assert stats["entities"]["orders"]["load"]["unchanged"] == 6


# --- failures and recovery ---------------------------------------------------

def test_missing_input_fails_and_is_recorded(cfg):
    cfg.paths.orders.unlink()
    with pytest.raises(IngestionError):
        runner.run_pipeline(cfg)
    runs = read_table(cfg.paths.database, "pipeline_runs")
    assert runs.status.tolist() == ["failed"] and "not found" in runs.error.iloc[0]
    latest = json.loads((cfg.paths.reports_dir / "latest.json").read_text())
    assert latest["status"] == "failed"


def test_quality_gate_blocks_load_and_writes_nothing(cfg):
    strict = replace(cfg, max_reject_rate={"customers": 1.0, "orders": 0.10})
    with pytest.raises(DataQualityError, match="orders reject rate"):
        runner.run_pipeline(strict)
    assert read_table(cfg.paths.database, "orders").empty
    assert read_table(cfg.paths.database, "customers").empty


def test_failure_mid_load_rolls_back_everything(cfg, monkeypatch):
    runner.run_pipeline(cfg)
    before = {t: business_rows(cfg.paths.database, t) for t in ("customers", "orders")}
    # Change customers so the run *would* write, then crash while loading orders.
    path = cfg.paths.customers
    path.write_text(path.read_text().replace("Pune", "Mumbai"))
    real_upsert = store.upsert

    def flaky(conn, table, records, run_id):
        if table == "orders":
            raise OSError("disk full")
        return real_upsert(conn, table, records, run_id)

    monkeypatch.setattr(store, "upsert", flaky)
    with pytest.raises(OSError):
        runner.run_pipeline(cfg)
    for t in ("customers", "orders"):
        assert business_rows(cfg.paths.database, t) == before[t]     # customers not half-updated
    monkeypatch.setattr(store, "upsert", real_upsert)
    stats = runner.run_pipeline(cfg)                                  # recovery = just rerun
    assert stats["status"] == "success" and stats["entities"]["customers"]["load"]["updated"] == 1


def test_failed_integrity_check_rolls_back(cfg, monkeypatch):
    monkeypatch.setitem(store.POST_LOAD_CHECKS, "always_fails", "SELECT 1")
    with pytest.raises(IntegrityError):
        runner.run_pipeline(cfg)
    assert read_table(cfg.paths.database, "orders").empty


def test_dry_run_writes_no_data(cfg):
    stats = runner.run_pipeline(cfg, dry_run=True)
    assert stats["status"] == "dry_run"
    assert stats["entities"]["orders"]["unique_valid"] == 6
    assert read_table(cfg.paths.database, "orders").empty


# --- CLI ---------------------------------------------------------------------

def _cli_args(cfg):
    return ["--customers", str(cfg.paths.customers), "--orders", str(cfg.paths.orders),
            "--db", str(cfg.paths.database), "--reports-dir", str(cfg.paths.reports_dir),
            "--logs-dir", str(cfg.paths.logs_dir), "--as-of", "2026-01-01"]


def test_cli_success_and_exit_codes(cfg, tmp_path, capsys):
    # The default config's quality gate (15% for orders) rejects the dirty fixture -> exit 3.
    assert main(["run", *_cli_args(cfg)]) == 3
    assert main(["run", *_cli_args(cfg)[4:], "--customers", str(cfg.paths.customers),
                 "--orders", str(tmp_path / "missing.csv")]) == 2
    assert main(["--config", str(tmp_path / "nope.yaml")]) == 2


def test_cli_full_sample_dataset(tmp_path, capsys):
    """The committed sample data passes the real quality gate and reconciles."""
    db = tmp_path / "w.db"
    args = ["--db", str(db), "--reports-dir", str(tmp_path / "r"), "--logs-dir", str(tmp_path / "l"),
            "--as-of", "2026-10-21"]
    assert main(args) == 0
    assert main(args) == 0
    latest = json.loads((tmp_path / "r" / "latest.json").read_text())
    for e in latest["entities"].values():
        assert e["reconciled"]
        assert e["load"]["inserted"] == e["load"]["updated"] == 0      # second run: no-op
    assert "status=success" in capsys.readouterr().out


def test_cli_rules_command(capsys):
    assert main(["rules"]) == 0
    out = capsys.readouterr().out
    assert "customer_unknown" in out and "email_invalid" in out
