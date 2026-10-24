"""Stage 4 - Storage (SQLite).

Why SQLite: zero setup for a reviewer, yet a real relational engine with
primary keys, foreign keys, CHECK constraints and atomic transactions -
the properties idempotency actually depends on. The SQL is plain enough to
port to PostgreSQL (`INSERT ... ON CONFLICT` has the same syntax there).

Idempotency and uniqueness
--------------------------
* Each table has the source's business key as PRIMARY KEY, so a key can
  exist only once no matter how many times the pipeline runs.
* Every row stores ``row_hash``: a SHA-256 of its business columns. A
  re-run with unchanged input finds identical hashes and writes nothing,
  so the tables are byte-for-byte stable across re-runs.
* Changed rows are updated in place - but only if the incoming
  ``source_updated_at`` is not older than what is stored. Replaying an old
  extract therefore cannot overwrite newer data ("stale" rows are counted
  and skipped).
* All writes of a run happen in one transaction together with the
  post-load integrity checks. Any failure rolls back everything: there is
  never a half-loaded state.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id          TEXT PRIMARY KEY,
    name                 TEXT,
    email                TEXT,
    signup_date          TEXT,               -- ISO date YYYY-MM-DD
    city                 TEXT,
    country_code         TEXT CHECK (country_code IS NULL OR length(country_code) = 2),
    source_updated_at    TEXT,               -- ISO timestamp from the source
    dq_flags             TEXT,               -- comma-separated warning rules, NULL if clean
    row_hash             TEXT NOT NULL,
    first_loaded_run_id  TEXT NOT NULL,
    last_changed_run_id  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id             TEXT PRIMARY KEY,
    customer_id          TEXT NOT NULL REFERENCES customers(customer_id),
    order_date           TEXT NOT NULL,
    sku                  TEXT NOT NULL,
    quantity             INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents     INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    total_cents          INTEGER NOT NULL,
    currency             TEXT NOT NULL CHECK (length(currency) = 3),
    status               TEXT NOT NULL,
    source_updated_at    TEXT,
    dq_flags             TEXT,
    row_hash             TEXT NOT NULL,
    first_loaded_run_id  TEXT NOT NULL,
    last_changed_run_id  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS ix_orders_date ON orders(order_date);

-- Records that failed validation in the most recent run, with every reason.
-- Rebuilt per run from the current snapshot, so a record that is fixed
-- upstream (or whose customer arrives later) leaves quarantine on its own.
CREATE TABLE IF NOT EXISTS quarantine (
    entity        TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,             -- sha256 of the raw record
    record_key    TEXT,
    source_file   TEXT NOT NULL,
    source_line   INTEGER,
    reasons       TEXT NOT NULL,             -- comma-separated rule names
    raw_record    TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    PRIMARY KEY (entity, fingerprint)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        TEXT PRIMARY KEY,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    status        TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'dry_run')),
    inputs        TEXT,                      -- JSON: file paths and sha256
    stats         TEXT,                      -- JSON: full run statistics
    error         TEXT
);

CREATE VIEW IF NOT EXISTS v_order_facts AS
SELECT o.order_id, o.order_date, o.status, o.sku, o.quantity,
       o.unit_price_cents / 100.0 AS unit_price,
       o.total_cents / 100.0      AS total_amount,
       o.currency,
       c.customer_id, c.country_code, c.signup_date,
       o.dq_flags AS order_dq_flags
FROM orders o JOIN customers c USING (customer_id);
"""

CUSTOMER_COLUMNS = ["customer_id", "name", "email", "signup_date", "city",
                    "country_code", "source_updated_at", "dq_flags"]
ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "sku", "quantity",
                 "unit_price_cents", "total_cents", "currency", "status",
                 "source_updated_at", "dq_flags"]
TABLES = {"customers": ("customer_id", CUSTOMER_COLUMNS),
          "orders": ("order_id", ORDER_COLUMNS)}


@dataclass
class UpsertStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped_stale: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: we issue BEGIN/COMMIT ourselves, so the
    # transaction boundary is explicit in the code rather than implicit.
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version not in (0, SCHEMA_VERSION):
        raise RuntimeError(f"database schema version {version} is not supported "
                           f"(expected {SCHEMA_VERSION}); run a migration first")
    conn.executescript(SCHEMA)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def row_hash(record: dict, columns: Sequence[str]) -> str:
    payload = json.dumps([record.get(c) for c in columns], default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _chunks(items: list, size: int = 500) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def known_customers(conn: sqlite3.Connection) -> dict[str, str | None]:
    """customer_id -> signup_date for customers loaded by earlier runs."""
    return {r["customer_id"]: r["signup_date"]
            for r in conn.execute("SELECT customer_id, signup_date FROM customers")}


def upsert(conn: sqlite3.Connection, table: str, records: list[dict], run_id: str) -> UpsertStats:
    """Insert new keys, update changed ones, leave identical ones untouched.

    Must be called inside an open transaction.
    """
    key, columns = TABLES[table]
    stats = UpsertStats()
    existing: dict[str, tuple[str, str | None]] = {}
    keys = [r[key] for r in records]
    for chunk in _chunks(keys):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
                f"SELECT {key}, row_hash, source_updated_at FROM {table} WHERE {key} IN ({marks})",
                chunk):
            existing[row[0]] = (row[1], row[2])

    to_write = []
    for rec in records:
        h = row_hash(rec, columns)
        prev = existing.get(rec[key])
        if prev is None:
            stats.inserted += 1
        elif prev[0] == h:
            stats.unchanged += 1
            continue
        elif (rec.get("source_updated_at") or "") < (prev[1] or ""):
            stats.skipped_stale += 1
            continue
        else:
            stats.updated += 1
        to_write.append([rec.get(c) for c in columns] + [h, run_id, run_id])

    cols = columns + ["row_hash", "first_loaded_run_id", "last_changed_run_id"]
    updates = ", ".join(f"{c} = excluded.{c}" for c in columns + ["row_hash", "last_changed_run_id"])
    # The WHERE clause repeats the Python-side decision inside the database,
    # so the guarantees hold even if this function is called carelessly.
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
           f"ON CONFLICT({key}) DO UPDATE SET {updates} "
           f"WHERE excluded.row_hash <> {table}.row_hash "
           f"AND COALESCE(excluded.source_updated_at, '') >= COALESCE({table}.source_updated_at, '')")
    conn.executemany(sql, to_write)
    return stats


def replace_quarantine(conn: sqlite3.Connection, entity: str, rows: list[dict], run_id: str) -> int:
    """Replace this entity's quarantine with the current run's rejects.

    Returns the number of distinct quarantined records.
    """
    conn.execute("DELETE FROM quarantine WHERE entity = ?", (entity,))
    conn.executemany(
        "INSERT OR IGNORE INTO quarantine (entity, fingerprint, record_key, source_file, "
        "source_line, reasons, raw_record, run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(entity, hashlib.sha256(r["raw"].encode("utf-8")).hexdigest(), r.get("record_key"),
          r["source_file"], r.get("source_line"), ",".join(r["reasons"]), r["raw"], run_id)
         for r in rows])
    return conn.execute("SELECT COUNT(*) FROM quarantine WHERE entity = ?", (entity,)).fetchone()[0]


# Each check is a query that must return 0. They run inside the load
# transaction; a non-zero result raises and rolls the whole load back.
POST_LOAD_CHECKS = {
    "orders_without_customer":
        "SELECT COUNT(*) FROM orders o LEFT JOIN customers c USING (customer_id) "
        "WHERE c.customer_id IS NULL",
    "order_total_mismatch":
        "SELECT COUNT(*) FROM orders WHERE total_cents <> quantity * unit_price_cents",
    "orders_with_invalid_values":
        "SELECT COUNT(*) FROM orders WHERE quantity <= 0 OR unit_price_cents < 0 "
        "OR order_date IS NULL OR order_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'",
    "customers_with_bad_key":
        "SELECT COUNT(*) FROM customers WHERE customer_id IS NULL OR customer_id <> upper(trim(customer_id))",
}


def run_post_load_checks(conn: sqlite3.Connection) -> dict[str, int]:
    return {name: conn.execute(sql).fetchone()[0] for name, sql in POST_LOAD_CHECKS.items()}


def content_fingerprint(conn: sqlite3.Connection, table: str) -> str:
    """Hash of every row's key and content hash; equal fingerprints across two
    runs prove the table's data is identical. Run-metadata columns are ignored."""
    key, _ = TABLES[table]
    h = hashlib.sha256()
    for row in conn.execute(f"SELECT {key}, row_hash FROM {table} ORDER BY {key}"):
        h.update(f"{row[0]}:{row[1]}\n".encode())
    return h.hexdigest()[:16]


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("customers", "orders", "quarantine", "pipeline_runs")}


def start_run(conn: sqlite3.Connection, run_id: str, started_at: str, inputs: dict) -> None:
    conn.execute("INSERT INTO pipeline_runs (run_id, started_at, status, inputs) VALUES (?, ?, 'running', ?)",
                 (run_id, started_at, json.dumps(inputs)))


def finish_run(conn: sqlite3.Connection, run_id: str, finished_at: str, status: str,
               stats: dict | None, error: str | None = None) -> None:
    conn.execute("UPDATE pipeline_runs SET finished_at = ?, status = ?, stats = ?, error = ? WHERE run_id = ?",
                 (finished_at, status, json.dumps(stats, default=str) if stats else None, error, run_id))
