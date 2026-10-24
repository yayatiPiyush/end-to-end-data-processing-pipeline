"""Stage 2 - Cleaning and transformation (entity-specific).

Composes the generic primitives in :mod:`pipeline.clean` into typed
customer and order frames. For every value that had to be *changed* to
become canonical (e.g. ' $1,299.00' -> 129900 cents, 'usa' -> 'US') the row
records a correction tag in ``_corrections``. Those tags feed the data quality
report so "records corrected" is measured, not estimated.

Raw values of parsed fields are kept alongside as ``<field>__raw`` so the
validation stage can tell *missing* from *present but unparseable*, which
produce different rejection reasons.
"""

from __future__ import annotations

import pandas as pd

from . import clean
from .config import Config


def _strip(s: pd.Series) -> pd.Series:
    return clean.map_obj(s, lambda v: None if clean.is_missing(v) else str(v).strip())


def _tag(tags: pd.Series, mask: pd.Series, name: str) -> None:
    """Append correction ``name`` to the tag list of every row where ``mask``."""
    for idx in mask[mask].index:
        tags.at[idx] = [*tags.at[idx], name]


def _changed(raw: pd.Series, canonical_text: pd.Series) -> pd.Series:
    """Rows where a usable value exists but its raw spelling differed."""
    return canonical_text.notna() & (_strip(raw) != canonical_text)


def _iso(dates: pd.Series) -> pd.Series:
    return clean.map_obj(dates, lambda d: None if pd.isna(d) else d.strftime("%Y-%m-%d"))


def transform_customers(raw: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    rules = cfg.customers
    out = pd.DataFrame(index=raw.index)
    tags = pd.Series([[] for _ in range(len(raw))], index=raw.index, dtype="object")

    out["customer_id"] = clean.normalize_id(raw["customer_id"])
    _tag(tags, _changed(raw["customer_id"], out["customer_id"]), "customer_id_normalized")

    out["name"] = clean.normalize_text(raw["name"])
    _tag(tags, _changed(raw["name"], out["name"]) | (out["name"].notna() & (_strip(raw["name"]) != raw["name"])),
         "name_whitespace")

    out["email"] = clean.normalize_email(raw["email"])
    _tag(tags, _changed(raw["email"], out["email"]), "email_normalized")

    out["signup_date"] = clean.parse_dates(raw["signup_date"], cfg.date_formats)
    _tag(tags, _changed(raw["signup_date"], _iso(out["signup_date"])), "date_reformatted")

    out["city"] = clean.normalize_text(raw["city"])
    out["country_code"] = clean.map_values(raw["country"], rules.country_map, key=clean.country_key)
    _tag(tags, _changed(raw["country"], out["country_code"]), "country_standardized")

    out["source_updated_at"] = clean.parse_timestamp(raw["updated_at"])

    for col in ("customer_id", "email", "signup_date", "country", "updated_at"):
        out[f"{col}__raw"] = raw[col]
    out["_source_line"] = raw["_source_line"]
    out["_raw"] = raw["_raw"]
    out["_corrections"] = tags
    return out


def transform_orders(raw: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    rules = cfg.orders
    out = pd.DataFrame(index=raw.index)
    tags = pd.Series([[] for _ in range(len(raw))], index=raw.index, dtype="object")

    out["order_id"] = clean.normalize_id(raw["order_id"])
    _tag(tags, _changed(raw["order_id"], out["order_id"]), "order_id_normalized")

    out["customer_id"] = clean.normalize_id(raw["customer_id"])
    _tag(tags, _changed(raw["customer_id"], out["customer_id"]), "customer_id_normalized")

    out["order_date"] = clean.parse_dates(raw["order_date"], cfg.date_formats)
    _tag(tags, _changed(raw["order_date"], _iso(out["order_date"])), "date_reformatted")

    out["sku"] = clean.normalize_id(raw["sku"])

    out["quantity"] = clean.parse_int_strict(raw["quantity"])
    qty_text = clean.map_obj(out["quantity"], lambda v: None if pd.isna(v) else str(v))
    _tag(tags, _changed(raw["quantity"], qty_text), "quantity_reformatted")

    out["unit_price_cents"] = clean.parse_money_cents(raw["unit_price"])
    price_text = clean.map_obj(out["unit_price_cents"],
                               lambda v: None if pd.isna(v) else f"{v / 100:.2f}")
    _tag(tags, _changed(raw["unit_price"], price_text), "price_reformatted")

    out["currency"] = clean.normalize_id(raw["currency"])
    _tag(tags, _changed(raw["currency"], out["currency"]), "currency_uppercased")

    status_raw = clean.normalize_text(raw["status"])
    out["status"] = clean.map_values(raw["status"], rules.status_map)
    _tag(tags, _changed(raw["status"], out["status"]), "status_standardized")
    # Unknown spellings are kept for the "unrecognized_status" warning rule.
    out["status__raw"] = status_raw

    out["source_updated_at"] = clean.parse_timestamp(raw["updated_at"])

    qty, price = out["quantity"], out["unit_price_cents"]
    out["total_cents"] = (qty * price).where(qty.notna() & price.notna()).astype("Int64")

    for col in ("order_id", "customer_id", "order_date", "quantity", "unit_price",
                "currency", "updated_at"):
        out[f"{col}__raw"] = raw[col]
    out["_source_line"] = raw["_source_line"]
    out["_raw"] = raw["_raw"]
    out["_corrections"] = tags
    return out
