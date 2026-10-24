"""Stage 3 - Validation.

Validation rules are declared as data (a list of :class:`Rule` objects), not
buried in if-statements. That gives three things for free:

* every failure carries a stable, machine-readable reason code;
* the report can count failures per rule;
* adding a rule is a one-entry change (see README, "Adding a validation rule").

Severity policy
---------------
``error``  The record cannot be loaded without making something up: its key is
           missing, it points at a customer that does not exist, or a value
           needed for revenue (date, quantity, price, currency) is unusable.
           The whole record goes to quarantine with every failing reason.

``warn``   The record is still useful, but one non-critical field is bad
           (an invalid email, an unrecognized country). The record is loaded,
           the bad field is set to NULL (or a sentinel) by the rule's
           ``on_fail`` action, and the rule name is stored in ``dq_flags`` so
           analysts can filter it out. Rejecting a customer over a typo in
           their email would orphan all of that customer's orders.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Literal

import pandas as pd

from . import clean
from .config import Config

Check = Callable[[pd.DataFrame], pd.Series]          # returns True where the row PASSES
Action = Callable[[pd.DataFrame, pd.Series], None]   # mutates failing rows in place


@dataclass(frozen=True)
class Rule:
    name: str
    severity: Literal["error", "warn"]
    description: str
    check: Check
    on_fail: Action | None = None


@dataclass
class ValidationResult:
    accepted: pd.DataFrame          # with a `dq_flags` list column
    rejected: pd.DataFrame          # with a `reasons` list column
    failures: Counter               # rule name -> number of rows failing it


# --- small helpers to keep rule definitions one-liners ----------------------

def present(col: str) -> Check:
    return lambda df: df[col].notna()


def absent_or(col: str, inner: Check) -> Check:
    """Pass when the value is missing (a separate rule reports that) or ``inner`` passes."""
    return lambda df: df[col].isna() | inner(df)


def raw_blank(col: str) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: clean.normalize_text(df[col]).isna()


def parsed_if_given(col: str, raw_col: str) -> Check:
    """Pass if the raw value was blank or it parsed successfully."""
    return lambda df: raw_blank(raw_col)(df) | df[col].notna()


def nullify(col: str) -> Action:
    def act(df: pd.DataFrame, failing: pd.Series) -> None:
        df.loc[failing, col] = None
    return act


def set_value(col: str, value: object) -> Action:
    def act(df: pd.DataFrame, failing: pd.Series) -> None:
        df.loc[failing, col] = value
    return act


def _date_cmp(col: str, op: Callable[[pd.Series, pd.Timestamp], pd.Series], bound: date) -> Check:
    ts = pd.Timestamp(bound)
    return lambda df: df[col].isna() | op(df[col], ts)


# --- rule catalogues ---------------------------------------------------------

def customer_rules(cfg: Config) -> list[Rule]:
    r = cfg.customers
    as_of = cfg.effective_as_of()
    return [
        Rule("customer_id_missing", "error", "customer_id is blank or absent",
             present("customer_id")),
        Rule("customer_id_invalid_format", "error", f"customer_id must match {r.id_pattern}",
             absent_or("customer_id", lambda df: clean.matches(df["customer_id"], r.id_pattern))),
        Rule("name_missing", "warn", "name is blank; kept as NULL",
             present("name")),
        Rule("email_missing", "warn", "email is blank; kept as NULL",
             lambda df: ~raw_blank("email__raw")(df)),
        Rule("email_invalid", "warn", "email is not shaped like name@domain.tld; set to NULL",
             absent_or("email", lambda df: clean.matches(df["email"], clean.EMAIL_RE)),
             nullify("email")),
        Rule("signup_date_unparseable", "warn", "signup_date matches no configured format; NULL",
             parsed_if_given("signup_date", "signup_date__raw")),
        Rule("signup_date_in_future", "warn", "signup_date is after the as-of date; set to NULL",
             _date_cmp("signup_date", lambda s, t: s <= t, as_of), nullify("signup_date")),
        Rule("country_unrecognized", "warn", "country not in country_map; country_code NULL",
             parsed_if_given("country_code", "country__raw")),
        Rule("updated_at_invalid", "warn", "updated_at missing or not ISO-8601; loses tie-breaks",
             present("source_updated_at")),
    ]


def order_rules(cfg: Config, known_customer_ids: set[str],
                signup_dates: dict[str, pd.Timestamp] | None = None) -> list[Rule]:
    r = cfg.orders
    as_of = cfg.effective_as_of()
    signup_dates = signup_dates or {}

    def known_customer(df: pd.DataFrame) -> pd.Series:
        return df["customer_id"].isin(known_customer_ids)

    def not_before_signup(df: pd.DataFrame) -> pd.Series:
        signup = pd.to_datetime(df["customer_id"].map(signup_dates), errors="coerce")
        return signup.isna() | df["order_date"].isna() | (df["order_date"] >= signup)

    return [
        Rule("order_id_missing", "error", "order_id is blank", present("order_id")),
        Rule("order_id_invalid_format", "error", f"order_id must match {r.id_pattern}",
             absent_or("order_id", lambda df: clean.matches(df["order_id"], r.id_pattern))),
        Rule("customer_id_missing", "error", "customer_id is blank", present("customer_id")),
        Rule("customer_unknown", "error",
             "customer_id not found among valid customers (this run or already loaded)",
             absent_or("customer_id", known_customer)),
        Rule("order_date_missing", "error", "order_date is blank",
             lambda df: ~raw_blank("order_date__raw")(df)),
        Rule("order_date_unparseable", "error", "order_date matches no configured format",
             parsed_if_given("order_date", "order_date__raw")),
        Rule("order_date_in_future", "error", "order_date is after the as-of date",
             _date_cmp("order_date", lambda s, t: s <= t, as_of)),
        Rule("order_date_too_old", "error", f"order_date is before {r.min_order_date}",
             _date_cmp("order_date", lambda s, t: s >= t, r.min_order_date)),
        Rule("sku_invalid", "error", f"sku missing or not matching {r.sku_pattern}",
             lambda df: clean.matches(df["sku"], r.sku_pattern)),
        Rule("quantity_missing", "error", "quantity is blank",
             lambda df: ~raw_blank("quantity__raw")(df)),
        Rule("quantity_not_integer", "error", "quantity is not a whole number",
             parsed_if_given("quantity", "quantity__raw")),
        Rule("quantity_not_positive", "error", "quantity must be >= 1",
             absent_or("quantity", lambda df: df["quantity"].fillna(0) > 0)),
        Rule("unit_price_missing", "error", "unit_price is blank",
             lambda df: ~raw_blank("unit_price__raw")(df)),
        Rule("unit_price_invalid", "error", "unit_price is not a number with <= 2 decimals",
             parsed_if_given("unit_price_cents", "unit_price__raw")),
        Rule("unit_price_negative", "error", "unit_price must be >= 0 (refunds use status)",
             absent_or("unit_price_cents", lambda df: df["unit_price_cents"].fillna(0) >= 0)),
        Rule("currency_missing", "error", "currency is blank; amount is meaningless without it",
             present("currency")),
        Rule("currency_unsupported", "error",
             f"currency not in {sorted(r.valid_currencies)}",
             absent_or("currency", lambda df: df["currency"].isin(r.valid_currencies))),
        Rule("status_unrecognized", "warn", "status not in status_map; stored as 'unknown'",
             present("status"), set_value("status", "unknown")),
        Rule("updated_at_invalid", "warn", "updated_at missing or not ISO-8601; loses tie-breaks",
             present("source_updated_at")),
        Rule("order_before_signup", "warn", "order_date is earlier than the customer's signup_date",
             not_before_signup),
    ]


# --- engine -----------------------------------------------------------------

def apply_rules(df: pd.DataFrame, rules: list[Rule]) -> ValidationResult:
    """Evaluate every rule on every row (no short-circuit), so a rejected
    record lists *all* of its problems, not just the first one found."""
    df = df.copy()
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index, dtype="object")
    flags = pd.Series([[] for _ in range(len(df))], index=df.index, dtype="object")
    failures: Counter = Counter()

    # Errors first: warning actions must not run on rows that will be rejected
    # anyway, and must not influence error checks.
    for rule in sorted(rules, key=lambda r: r.severity != "error"):
        passed = rule.check(df).reindex(df.index).fillna(False).astype(bool)
        failing = ~passed
        if rule.severity == "warn":
            failing &= reasons.map(len) == 0
        n = int(failing.sum())
        if not n:
            continue
        failures[rule.name] += n
        target = reasons if rule.severity == "error" else flags
        for idx in failing[failing].index:
            target.at[idx] = [*target.at[idx], rule.name]
        if rule.on_fail is not None:
            rule.on_fail(df, failing)

    is_rejected = reasons.map(len) > 0
    accepted = df.loc[~is_rejected].copy()
    accepted["dq_flags"] = flags.loc[~is_rejected]
    rejected = df.loc[is_rejected].copy()
    rejected["reasons"] = reasons.loc[is_rejected]
    return ValidationResult(accepted, rejected, failures)


def rule_catalogue(cfg: Config) -> list[tuple[str, str, str, str]]:
    """(entity, rule, severity, description) for docs and `python -m pipeline rules`."""
    rows = [("customers", r.name, r.severity, r.description) for r in customer_rules(cfg)]
    rows += [("orders", r.name, r.severity, r.description) for r in order_rules(cfg, set())]
    return rows
