"""Unit tests for transformation + validation rules on small, targeted frames."""

import pandas as pd

from pipeline import transform, validate

ORDER_DEFAULTS = {"order_id": "O000001", "customer_id": "C00001", "order_date": "2025-03-01",
                  "sku": "SKU-001", "quantity": "1", "unit_price": "10.00", "currency": "USD",
                  "status": "pending", "updated_at": "2025-03-02T00:00:00"}
CUSTOMER_DEFAULTS = {"customer_id": "C00001", "name": "Asha", "email": "a@example.com",
                     "signup_date": "2024-01-01", "city": "Pune", "country": "IN",
                     "updated_at": "2025-01-01T00:00:00"}


def _frame(defaults: dict, rows: list[dict]) -> pd.DataFrame:
    recs = [{**defaults, **r, "_source_line": i + 2, "_raw": str(i)} for i, r in enumerate(rows)]
    return pd.DataFrame(recs).astype("object")


def run_orders(cfg, *rows, known=("C00001",), signup=None):
    df = transform.transform_orders(_frame(ORDER_DEFAULTS, list(rows)), cfg)
    return validate.apply_rules(df, validate.order_rules(cfg, set(known), signup))


def run_customers(cfg, *rows):
    df = transform.transform_customers(_frame(CUSTOMER_DEFAULTS, list(rows)), cfg)
    return validate.apply_rules(df, validate.customer_rules(cfg))


def reasons(result) -> list[list[str]]:
    return result.rejected["reasons"].tolist()


def test_clean_order_passes_without_flags(cfg):
    r = run_orders(cfg, {})
    assert len(r.accepted) == 1 and r.rejected.empty
    assert r.accepted["dq_flags"].iloc[0] == []


def test_order_reports_every_failing_rule_not_just_the_first(cfg):
    r = run_orders(cfg, {"order_date": "not a date", "quantity": "0", "unit_price": "N/A", "currency": ""})
    assert sorted(reasons(r)[0]) == sorted(
        ["order_date_unparseable", "quantity_not_positive", "unit_price_invalid", "currency_missing"])


def test_missing_and_unparseable_are_different_reasons(cfg):
    r = run_orders(cfg, {"quantity": ""}, {"quantity": "two"}, {"unit_price": ""}, {"unit_price": "abc"})
    assert reasons(r) == [["quantity_missing"], ["quantity_not_integer"],
                          ["unit_price_missing"], ["unit_price_invalid"]]


def test_order_date_bounds(cfg):
    # cfg pins as_of to 2026-01-01; min_order_date is 2020-01-01
    r = run_orders(cfg, {"order_date": "2026-01-01"}, {"order_date": "2026-01-02"},
                   {"order_date": "2020-01-01"}, {"order_date": "2019-12-31"})
    assert r.accepted["order_date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-01", "2020-01-01"]
    assert reasons(r) == [["order_date_in_future"], ["order_date_too_old"]]


def test_unknown_customer_is_rejected(cfg):
    r = run_orders(cfg, {"customer_id": "C00002"}, known={"C00001"})
    assert reasons(r) == [["customer_unknown"]]


def test_customer_id_is_normalized_before_lookup(cfg):
    r = run_orders(cfg, {"customer_id": " c00001 "})
    assert r.accepted["customer_id"].tolist() == ["C00001"]


def test_negative_price_rejected_zero_price_allowed(cfg):
    r = run_orders(cfg, {"unit_price": "-1.00"}, {"unit_price": "0.00"})
    assert reasons(r) == [["unit_price_negative"]]
    assert r.accepted["unit_price_cents"].tolist() == [0]


def test_currency_rules(cfg):
    r = run_orders(cfg, {"currency": "eur"}, {"currency": "XYZ"}, {"currency": " "})
    assert r.accepted["currency"].tolist() == ["EUR"]
    assert reasons(r) == [["currency_unsupported"], ["currency_missing"]]


def test_unknown_status_is_kept_as_unknown_and_flagged(cfg):
    r = run_orders(cfg, {"status": "lost?"}, {"status": " SHIPPED "}, {"status": "canceled"})
    assert r.rejected.empty
    assert r.accepted["status"].tolist() == ["unknown", "shipped", "cancelled"]
    assert r.accepted["dq_flags"].tolist() == [["status_unrecognized"], [], []]


def test_total_is_quantity_times_price_in_integer_cents(cfg):
    r = run_orders(cfg, {"quantity": "3", "unit_price": "0.10"})
    assert r.accepted["total_cents"].iloc[0] == 30


def test_order_before_signup_is_a_warning(cfg):
    signup = {"C00001": pd.Timestamp("2025-06-01")}
    r = run_orders(cfg, {"order_date": "2025-05-31"}, signup=signup)
    assert r.rejected.empty
    assert r.accepted["dq_flags"].iloc[0] == ["order_before_signup"]


def test_warnings_are_not_applied_to_rejected_rows(cfg):
    r = run_orders(cfg, {"status": "lost?", "order_id": ""})
    assert reasons(r) == [["order_id_missing"]]          # no warning tag mixed in
    assert "status_unrecognized" not in r.failures


def test_customer_missing_id_rejected(cfg):
    r = run_customers(cfg, {"customer_id": None}, {"customer_id": "X1"})
    assert reasons(r) == [["customer_id_missing"], ["customer_id_invalid_format"]]


def test_invalid_email_is_nulled_and_flagged_not_rejected(cfg):
    r = run_customers(cfg, {"email": "ben@example"})
    assert r.rejected.empty
    assert r.accepted["email"].iloc[0] is None
    assert r.accepted["dq_flags"].iloc[0] == ["email_invalid"]


def test_unrecognized_country_and_bad_signup_are_warnings(cfg):
    r = run_customers(cfg, {"country": "Atlantis", "signup_date": "unknown"})
    assert r.rejected.empty
    assert sorted(r.accepted["dq_flags"].iloc[0]) == ["country_unrecognized", "signup_date_unparseable"]


def test_future_signup_is_nulled(cfg):
    r = run_customers(cfg, {"signup_date": "2099-01-01"})
    assert pd.isna(r.accepted["signup_date"].iloc[0])
    assert r.accepted["dq_flags"].iloc[0] == ["signup_date_in_future"]


def test_corrections_are_tagged(cfg):
    df = transform.transform_customers(_frame(CUSTOMER_DEFAULTS, [
        {"customer_id": " c00001", "email": " A@Example.com", "country": "India", "signup_date": "01/02/2024"},
        {},
    ]), cfg)
    assert sorted(df["_corrections"].iloc[0]) == sorted(
        ["customer_id_normalized", "email_normalized", "country_standardized", "date_reformatted"])
    assert df["_corrections"].iloc[1] == []


def test_rule_catalogue_names_are_unique(cfg):
    names = [(e, r) for e, r, _, _ in validate.rule_catalogue(cfg)]
    assert len(names) == len(set(names))
