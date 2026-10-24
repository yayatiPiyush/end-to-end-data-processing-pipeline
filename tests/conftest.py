from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline.config import load_config
from pipeline.logging_utils import setup_logging

FIXTURES = Path(__file__).parent / "fixtures"
AS_OF = date(2026, 1, 1)


@pytest.fixture(autouse=True)
def _quiet_logging():
    setup_logging(None, "WARNING")


@pytest.fixture
def cfg(tmp_path: Path):
    """Real config file, but every path inside tmp_path, a pinned as-of date and
    the quality gate opened up (the tiny fixtures are deliberately very dirty)."""
    raw = tmp_path / "raw"
    raw.mkdir()
    shutil.copy(FIXTURES / "customers.jsonl", raw / "customers.jsonl")
    shutil.copy(FIXTURES / "orders.csv", raw / "orders.csv")
    c = load_config().with_overrides(
        customers=raw / "customers.jsonl", orders=raw / "orders.csv",
        database=tmp_path / "out" / "test.db", reports_dir=tmp_path / "reports",
        logs_dir=tmp_path / "logs")
    return replace(c, as_of_date=AS_OF, max_reject_rate={"customers": 1.0, "orders": 1.0})


def read_table(db: Path, table: str) -> pd.DataFrame:
    with closing(sqlite3.connect(db)) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table}", conn)


def business_rows(db: Path, table: str) -> list[tuple]:
    """Table contents excluding run metadata, sorted; comparable across runs."""
    df = read_table(db, table).drop(columns=["first_loaded_run_id", "last_changed_run_id"])
    return sorted(df.itertuples(index=False, name=None), key=lambda r: str(r[0]))
