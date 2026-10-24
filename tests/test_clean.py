"""Unit tests for the reusable cleaning primitives."""

import pandas as pd
import pytest

from pipeline import clean

FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%d %b %Y")


@pytest.mark.parametrize("raw, cents", [
    ("12.50", 1250),
    ("12.5", 1250),
    ("12", 1200),
    ("$1,299.00", 129900),
    ("  49.99 ", 4999),
    ("€7.10", 710),
    ("0.00", 0),
    ("-3.00", -300),          # parsed; rejecting negatives is validation's job
    ("-$3.00", -300),
    ("0.1", 10),
    ("0.29", 29),             # Decimal, not float: 0.29*100 would be 28.999...
])
def test_parse_money_cents_valid(raw, cents):
    assert clean.parse_money_cents(pd.Series([raw])).iloc[0] == cents


@pytest.mark.parametrize("raw", ["", "  ", None, "N/A", "abc", "1.005", "1.2.3", "12,50 EUR", "--1"])
def test_parse_money_cents_invalid_is_missing(raw):
    assert pd.isna(clean.parse_money_cents(pd.Series([raw], dtype="object")).iloc[0])


@pytest.mark.parametrize("raw, expected", [("2", 2), (" 3 ", 3), ("2.0", 2), ("-1", -1), ("0", 0)])
def test_parse_int_strict_valid(raw, expected):
    assert clean.parse_int_strict(pd.Series([raw])).iloc[0] == expected


@pytest.mark.parametrize("raw", ["2.5", "two", "", None, "1e3"])
def test_parse_int_strict_never_truncates(raw):
    assert pd.isna(clean.parse_int_strict(pd.Series([raw], dtype="object")).iloc[0])


def test_parse_dates_supports_each_configured_format():
    s = pd.Series(["2025-03-14", "03/14/2025", "14 Mar 2025"])
    assert clean.parse_dates(s, FORMATS).dt.strftime("%Y-%m-%d").tolist() == ["2025-03-14"] * 3


def test_parse_dates_ambiguous_value_uses_configured_order():
    # 03/04/2025 is 4 March in the US, 3 April elsewhere. Only %m/%d/%Y is
    # configured, so it must be 4 March - never silently guessed.
    assert clean.parse_dates(pd.Series(["03/04/2025"]), FORMATS).iloc[0] == pd.Timestamp("2025-03-04")


@pytest.mark.parametrize("raw", ["not a date", "2025-13-45", "31/31/2024", "", None])
def test_parse_dates_unparseable_is_nat(raw):
    assert pd.isna(clean.parse_dates(pd.Series([raw], dtype="object"), FORMATS).iloc[0])


def test_parse_timestamp_iso_only():
    out = clean.parse_timestamp(pd.Series(["2025-01-01T10:00:00", "yesterday", None], dtype="object"))
    assert out.iloc[0] == pd.Timestamp("2025-01-01 10:00")
    assert out.iloc[1:].isna().all()


def test_normalize_text_collapses_whitespace_and_blanks_to_none():
    out = clean.normalize_text(pd.Series(["  Asha   Rao ", "   ", None, ""], dtype="object"))
    assert out.tolist() == ["Asha Rao", None, None, None]


def test_normalize_id_and_email():
    assert clean.normalize_id(pd.Series([" c00012 "])).iloc[0] == "C00012"
    assert clean.normalize_email(pd.Series(["  A.B@Example.COM "])).iloc[0] == "a.b@example.com"


def test_none_is_preserved_not_turned_into_nan():
    # Regression: Series.map would coerce None to float NaN and break .upper().
    out = clean.normalize_id(pd.Series([None, "a"], dtype="object"))
    assert out.iloc[0] is None and out.iloc[1] == "A"


@pytest.mark.parametrize("raw, code", [
    ("USA", "US"), ("U.S.", "US"), ("united  states", "US"), ("India", "IN"),
    ("uk", "GB"), ("Atlantis", None), ("", None),
])
def test_country_mapping(raw, code):
    mapping = {"usa": "US", "us": "US", "united states": "US", "india": "IN", "uk": "GB"}
    assert clean.map_values(pd.Series([raw]), mapping, key=clean.country_key).iloc[0] == code


@pytest.mark.parametrize("email, ok", [
    ("a@b.co", True), ("first.last+tag@example.com", True),
    ("ben@example", False), ("not-an-email", False), ("a@@b.com", False), ("a b@c.com", False),
])
def test_email_shape(email, ok):
    assert bool(clean.matches(pd.Series([email]), clean.EMAIL_RE).iloc[0]) is ok
