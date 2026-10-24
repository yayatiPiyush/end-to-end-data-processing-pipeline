"""Reusable, dataset-agnostic cleaning primitives.

Every function here is pure: it takes a Series of raw strings and returns a
new Series with the same index. Values that cannot be interpreted become
missing (None / NaT / <NA>) rather than raising; deciding whether a missing
value is acceptable is the job of the validation stage, not of the parser.
This separation is what lets the same primitives serve different datasets.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import pandas as pd

_WS = re.compile(r"\s+")
_MONEY = re.compile(r"^-?\d+(\.\d{1,2})?$")
_INT_LIKE = re.compile(r"^[+-]?\d+(\.0+)?$")
_CURRENCY_SYMBOLS = "$€£₹"
# Pragmatic shape check, not RFC 5322: one @, no spaces, a dot in the domain.
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


def map_obj(s: pd.Series, fn: Callable[[object], object]) -> pd.Series:
    """Element-wise map that keeps ``None`` as ``None``.

    ``Series.map`` lets pandas infer the result dtype, which turns ``None``
    into ``NaN`` (a float). Building an object Series directly avoids that.
    """
    return pd.Series([fn(v) for v in s], index=s.index, dtype="object")


def is_missing(value: object) -> bool:
    return value is None or (not isinstance(value, str) and bool(pd.isna(value)))


def _blank_to_none(value: object) -> object:
    if is_missing(value):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def normalize_text(s: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace; blank -> None."""
    def f(v: object) -> object:
        v = _blank_to_none(v)
        return None if v is None else _WS.sub(" ", str(v).strip())
    return map_obj(s, f)


def normalize_id(s: pd.Series) -> pd.Series:
    """Business keys: trimmed and upper-cased so ' c00012' and 'C00012' match."""
    return map_obj(normalize_text(s), lambda v: None if v is None else v.upper())


def normalize_email(s: pd.Series) -> pd.Series:
    """Trim and lower-case. Shape is validated separately (see EMAIL_RE)."""
    return map_obj(normalize_text(s), lambda v: None if v is None else v.lower())


def map_values(s: pd.Series, mapping: Mapping[str, str],
               key: Callable[[str], str] = lambda v: v.lower()) -> pd.Series:
    """Map raw spellings to canonical values. Unmapped or blank -> None."""
    def f(v: object) -> object:
        v = _blank_to_none(v)
        return None if v is None else mapping.get(key(str(v).strip()))
    return map_obj(s, f)


def country_key(v: str) -> str:
    """'U.S.' -> 'us', ' United  Kingdom ' -> 'united kingdom'."""
    return _WS.sub(" ", v.replace(".", "").strip().lower())


def parse_dates(s: pd.Series, formats: Iterable[str]) -> pd.Series:
    """Parse with an explicit, ordered list of formats; first match wins.

    Returns datetime64 with NaT where nothing matched. We never let pandas
    infer the format, because inference silently swaps day and month on
    ambiguous inputs like 03/04/2025.
    """
    text = normalize_text(s)
    result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in formats:
        todo = result.isna() & text.notna()
        if not todo.any():
            break
        parsed = pd.to_datetime(text[todo], format=fmt, errors="coerce")
        result.loc[todo] = parsed
    return result


def parse_timestamp(s: pd.Series) -> pd.Series:
    """Parse ISO-8601 timestamps; anything else -> NaT."""
    text = normalize_text(s)
    return pd.to_datetime(text, format="ISO8601", errors="coerce")


def _to_cents(v: object) -> object:
    v = _blank_to_none(v)
    if v is None:
        return pd.NA
    t = str(v).strip().lstrip(_CURRENCY_SYMBOLS).replace(",", "").strip()
    if t.startswith("-") and t[1:2] in _CURRENCY_SYMBOLS:      # "-$5.00"
        t = "-" + t[2:]
    if not _MONEY.match(t):
        return pd.NA
    try:
        return int((Decimal(t) * 100).to_integral_value(rounding=ROUND_HALF_UP))
    except InvalidOperation:
        return pd.NA


def parse_money_cents(s: pd.Series) -> pd.Series:
    """'$1,299.00' -> 129900. Integer cents avoid float rounding in totals.

    At most two decimal places are accepted; '12.345' is treated as invalid
    rather than rounded, because silently rounding money hides upstream bugs.
    """
    return pd.Series(pd.array([_to_cents(v) for v in s], dtype="Int64"), index=s.index)


def _to_int(v: object) -> object:
    v = _blank_to_none(v)
    if v is None:
        return pd.NA
    t = str(v).strip()
    return int(float(t)) if _INT_LIKE.match(t) else pd.NA


def parse_int_strict(s: pd.Series) -> pd.Series:
    """'2' and '2.0' -> 2; '2.5', 'two', '' -> <NA>. Never truncates."""
    return pd.Series(pd.array([_to_int(v) for v in s], dtype="Int64"), index=s.index)


def matches(s: pd.Series, pattern: str | re.Pattern) -> pd.Series:
    """Boolean mask: value is present and fully matches ``pattern``."""
    rx = re.compile(pattern) if isinstance(pattern, str) else pattern
    return pd.Series([not is_missing(v) and bool(rx.fullmatch(str(v))) for v in s],
                     index=s.index, dtype=bool)
