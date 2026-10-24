import pandas as pd

from pipeline import dedupe


def frame(rows):
    return pd.DataFrame(rows, columns=["order_id", "source_updated_at", "_source_line", "_raw"])


def test_exact_duplicates_keep_first():
    df = frame([("O1", None, 2, "a"), ("O1", None, 3, "a"), ("O2", None, 4, "b")])
    out, n = dedupe.drop_exact_duplicates(df)
    assert n == 1 and out["_source_line"].tolist() == [2, 4]


def test_latest_timestamp_wins_regardless_of_file_order():
    ts = pd.to_datetime
    df = frame([("O1", ts("2025-01-05"), 2, "new"), ("O1", ts("2025-01-01"), 3, "old")])
    out, superseded, ties = dedupe.resolve_versions(df, "order_id")
    assert out["_raw"].tolist() == ["new"] and superseded == 1 and ties == 0


def test_timestamp_tie_falls_back_to_later_line_and_is_counted():
    t = pd.Timestamp("2025-01-01")
    df = frame([("O1", t, 2, "first"), ("O1", t, 9, "second")])
    out, superseded, ties = dedupe.resolve_versions(df, "order_id")
    assert out["_raw"].tolist() == ["second"] and ties == 1


def test_missing_timestamp_loses_to_any_timestamp():
    df = frame([("O1", pd.Timestamp("2025-01-01"), 2, "dated"), ("O1", pd.NaT, 3, "undated")])
    out, _, _ = dedupe.resolve_versions(df, "order_id")
    assert out["_raw"].tolist() == ["dated"]


def test_empty_frame():
    out, s, t = dedupe.resolve_versions(frame([]), "order_id")
    assert out.empty and s == 0 and t == 0
