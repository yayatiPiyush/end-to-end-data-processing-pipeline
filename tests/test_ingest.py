"""Ingestion: file-level failures stop the run, line-level failures are captured."""

import logging

import pytest

from pipeline import ingest
from pipeline.errors import IngestionError

HEADER = "order_id,customer_id,order_date,sku,quantity,unit_price,currency,status,updated_at\n"
ROW = "O000001,C00001,2025-01-01,SKU-001,1,1.00,USD,pending,2025-01-01T00:00:00\n"


def write(tmp_path, name, content, mode="w"):
    p = tmp_path / name
    if mode == "wb":
        p.write_bytes(content)
    else:
        p.write_text(content, encoding="utf-8")
    return p


def test_missing_file_raises(tmp_path):
    with pytest.raises(IngestionError, match="not found"):
        ingest.read_orders_csv(tmp_path / "nope.csv")


def test_directory_instead_of_file_raises(tmp_path):
    with pytest.raises(IngestionError, match="not a file"):
        ingest.read_orders_csv(tmp_path)


@pytest.mark.parametrize("content", ["", HEADER])
def test_empty_csv_raises(tmp_path, content):
    with pytest.raises(IngestionError):
        ingest.read_orders_csv(write(tmp_path, "o.csv", content))


def test_missing_required_column_raises(tmp_path):
    header = HEADER.replace("currency,", "")
    with pytest.raises(IngestionError, match="currency"):
        ingest.read_orders_csv(write(tmp_path, "o.csv", header + ROW))


def test_header_is_case_and_space_insensitive_and_extra_columns_are_ignored(tmp_path, caplog):
    header = " ORDER_ID ,customer_id,order_date,sku,quantity,unit_price,currency,status,updated_at,coupon\n"
    with caplog.at_level(logging.WARNING):
        res = ingest.read_orders_csv(write(tmp_path, "o.csv", header + ROW.rstrip("\n") + ",SAVE10\n"))
    assert list(res.frame.columns[:9]) == ingest.ORDER_COLUMNS and "coupon" not in res.frame
    assert "unexpected columns" in caplog.text


def test_row_with_wrong_field_count_is_rejected_others_kept(tmp_path):
    res = ingest.read_orders_csv(write(tmp_path, "o.csv", HEADER + ROW + "O2,C1,2025-01-01\n" + ROW))
    assert len(res.frame) == 2
    assert res.rejects[0]["_source_line"] == 3
    assert res.rejects[0]["reasons"][0].startswith("wrong_field_count")
    assert res.lines_read == 3


def test_quoted_field_with_comma_and_newline(tmp_path):
    row = 'O000001,C00001,2025-01-01,SKU-001,1,"$1,299.00",USD,"pend\ning",2025-01-01T00:00:00\n'
    res = ingest.read_orders_csv(write(tmp_path, "o.csv", HEADER + row))
    assert res.frame["unit_price"].iloc[0] == "$1,299.00"
    assert res.rejects == []


def test_values_are_kept_as_raw_strings(tmp_path):
    # pandas' default would turn "N/A" into NaN and "007" into 7, hiding what arrived.
    row = "O000001,C00001,2025-01-01,SKU-001,007,N/A,USD,pending,2025-01-01T00:00:00\n"
    res = ingest.read_orders_csv(write(tmp_path, "o.csv", HEADER + row))
    assert res.frame["quantity"].iloc[0] == "007"
    assert res.frame["unit_price"].iloc[0] == "N/A"


def test_utf8_bom_is_tolerated(tmp_path):
    res = ingest.read_orders_csv(write(tmp_path, "o.csv", ("﻿" + HEADER + ROW).encode("utf-8"), "wb"))
    assert len(res.frame) == 1


def test_invalid_encoding_raises(tmp_path):
    with pytest.raises(IngestionError, match="UTF-8"):
        ingest.read_orders_csv(write(tmp_path, "o.csv", (HEADER + ROW).encode("utf-16"), "wb"))


def test_jsonl_bad_lines_are_captured(tmp_path):
    good = '{"customer_id": "C00001", "updated_at": "2025-01-01T00:00:00", "address": {"country": "US"}}'
    p = write(tmp_path, "c.jsonl", "\n".join([good, '{"customer_id": "C2", "na', "", '[1, 2]', good]) + "\n")
    res = ingest.read_customers_jsonl(p)
    assert len(res.frame) == 2
    assert [r["_source_line"] for r in res.rejects] == [2, 4]
    assert res.rejects[0]["reasons"][0].startswith("malformed_json")
    assert res.rejects[1]["reasons"] == ["not_a_json_object"]
    assert res.frame["country"].iloc[0] == "US"          # nested address flattened


def test_jsonl_missing_optional_fields_become_none(tmp_path):
    p = write(tmp_path, "c.jsonl", '{"customer_id": "C00001", "updated_at": "2025-01-01T00:00:00"}\n')
    row = ingest.read_customers_jsonl(p).frame.iloc[0]
    assert row["email"] is None and row["city"] is None


def test_jsonl_schema_drift_raises(tmp_path):
    # Upstream renamed customer_id -> id: every record lacks it.
    p = write(tmp_path, "c.jsonl", '{"id": "C00001", "updated_at": "x"}\n{"id": "C00002", "updated_at": "x"}\n')
    with pytest.raises(IngestionError, match="schema"):
        ingest.read_customers_jsonl(p)


def test_file_hash_is_recorded(tmp_path):
    res = ingest.read_orders_csv(write(tmp_path, "o.csv", HEADER + ROW))
    assert len(res.file_sha256) == 64
