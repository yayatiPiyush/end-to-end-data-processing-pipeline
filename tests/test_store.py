"""Storage: uniqueness, idempotent upserts, stale-replay protection."""

import sqlite3

import pytest

from pipeline import store


@pytest.fixture
def conn(tmp_path):
    c = store.connect(tmp_path / "t.db")
    store.init_schema(c)
    yield c
    c.close()


def cust(cid="C00001", email="a@example.com", updated="2025-01-01T00:00:00"):
    return {"customer_id": cid, "name": "A", "email": email, "signup_date": "2024-01-01",
            "city": "Pune", "country_code": "IN", "source_updated_at": updated, "dq_flags": None}


def upsert(conn, records, run="r1"):
    conn.execute("BEGIN")
    s = store.upsert(conn, "customers", records, run)
    conn.execute("COMMIT")
    return s.as_dict()


def test_insert_then_unchanged(conn):
    assert upsert(conn, [cust()]) == {"inserted": 1, "updated": 0, "unchanged": 0, "skipped_stale": 0}
    assert upsert(conn, [cust()], "r2") == {"inserted": 0, "updated": 0, "unchanged": 1, "skipped_stale": 0}
    row = conn.execute("SELECT first_loaded_run_id, last_changed_run_id FROM customers").fetchone()
    assert tuple(row) == ("r1", "r1")        # an unchanged row is not touched at all


def test_newer_change_updates_in_place(conn):
    upsert(conn, [cust()])
    stats = upsert(conn, [cust(email="new@example.com", updated="2025-02-01T00:00:00")], "r2")
    assert stats["updated"] == 1
    row = conn.execute("SELECT email, first_loaded_run_id, last_changed_run_id FROM customers").fetchone()
    assert tuple(row) == ("new@example.com", "r1", "r2")
    assert conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 1


def test_replaying_an_older_extract_does_not_overwrite_newer_data(conn):
    upsert(conn, [cust(email="new@example.com", updated="2025-02-01T00:00:00")])
    stats = upsert(conn, [cust(email="old@example.com", updated="2025-01-01T00:00:00")], "r2")
    assert stats["skipped_stale"] == 1
    assert conn.execute("SELECT email FROM customers").fetchone()[0] == "new@example.com"


def test_sql_guard_also_blocks_stale_writes(conn):
    # Even bypassing the Python-side classification, the ON CONFLICT ... WHERE
    # clause refuses to overwrite with an older source timestamp.
    upsert(conn, [cust(email="new@example.com", updated="2025-02-01T00:00:00")])
    conn.execute(
        "INSERT INTO customers (customer_id, email, source_updated_at, row_hash, first_loaded_run_id, "
        "last_changed_run_id) VALUES ('C00001', 'old@example.com', '2025-01-01T00:00:00', 'x', 'r', 'r') "
        "ON CONFLICT(customer_id) DO UPDATE SET email = excluded.email "
        "WHERE COALESCE(excluded.source_updated_at, '') >= COALESCE(customers.source_updated_at, '')")
    assert conn.execute("SELECT email FROM customers").fetchone()[0] == "new@example.com"


def test_primary_key_prevents_duplicates(conn):
    upsert(conn, [cust()])
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO customers (customer_id, row_hash, first_loaded_run_id, last_changed_run_id) "
                     "VALUES ('C00001', 'h', 'r', 'r')")


def test_foreign_key_is_enforced(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO orders (order_id, customer_id, order_date, sku, quantity, unit_price_cents, "
                     "total_cents, currency, status, row_hash, first_loaded_run_id, last_changed_run_id) "
                     "VALUES ('O1', 'C_MISSING', '2025-01-01', 'SKU-001', 1, 100, 100, 'USD', 'pending', "
                     "'h', 'r', 'r')")


def test_row_hash_is_order_sensitive_and_stable():
    a = store.row_hash(cust(), store.CUSTOMER_COLUMNS)
    assert a == store.row_hash(cust(), store.CUSTOMER_COLUMNS)
    assert a != store.row_hash(cust(email="b@example.com"), store.CUSTOMER_COLUMNS)


def test_quarantine_is_replaced_not_appended(conn):
    rej = [{"raw": "x", "reasons": ["r1"], "source_file": "f", "source_line": 1, "record_key": "K"}]
    conn.execute("BEGIN")
    assert store.replace_quarantine(conn, "orders", rej + rej, "run1") == 1   # same raw -> one row
    assert store.replace_quarantine(conn, "orders", rej, "run2") == 1
    assert store.replace_quarantine(conn, "orders", [], "run3") == 0          # fixed upstream -> gone
    conn.execute("COMMIT")


def test_unsupported_schema_version_is_refused(tmp_path):
    c = store.connect(tmp_path / "v.db")
    c.execute("PRAGMA user_version = 99")
    with pytest.raises(RuntimeError, match="schema version"):
        store.init_schema(c)
    c.close()
