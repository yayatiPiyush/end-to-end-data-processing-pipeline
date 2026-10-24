"""Load and validate the YAML configuration into typed, immutable objects.

Validating config up front means a typo in ``pipeline.yaml`` fails in the
first millisecond with a clear message, not halfway through a load.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = REPO_ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class Paths:
    customers: Path
    orders: Path
    database: Path
    reports_dir: Path
    logs_dir: Path


@dataclass(frozen=True)
class OrderRules:
    id_pattern: str
    min_order_date: date
    sku_pattern: str
    valid_currencies: frozenset[str]
    status_map: dict[str, str]


@dataclass(frozen=True)
class CustomerRules:
    id_pattern: str
    country_map: dict[str, str]


@dataclass(frozen=True)
class Config:
    paths: Paths
    date_formats: tuple[str, ...]
    orders: OrderRules
    customers: CustomerRules
    max_reject_rate: dict[str, float]
    as_of_date: date | None = None
    source_file: Path | None = field(default=None, compare=False)

    def effective_as_of(self) -> date:
        return self.as_of_date or date.today()

    def with_overrides(self, **paths: Path | None) -> "Config":
        """Return a copy with some paths replaced (used by the CLI and tests)."""
        changes = {k: Path(v) for k, v in paths.items() if v is not None}
        return replace(self, paths=replace(self.paths, **changes)) if changes else self


def _require(d: dict[str, Any], key: str, where: str) -> Any:
    if key not in d or d[key] is None:
        raise ConfigError(f"missing required config key '{where}{key}'")
    return d[key]


def _resolve(p: str, base: Path) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (base / path)


def _parse_date(value: Any, key: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ConfigError(f"'{key}' must be an ISO date (YYYY-MM-DD), got {value!r}") from exc


def _check_regex(pattern: str, key: str) -> str:
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ConfigError(f"'{key}' is not a valid regular expression: {exc}") from exc
    return pattern


def load_config(path: str | Path | None = None, base_dir: Path | None = None) -> Config:
    """Read ``path`` (default: config/pipeline.yaml) and return a validated Config.

    Relative paths inside the file are resolved against ``base_dir``
    (default: the repository root), so the pipeline works from any cwd.
    """
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    base = base_dir or REPO_ROOT
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {cfg_path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a mapping")

    p = _require(raw, "paths", "")
    paths = Paths(**{k: _resolve(_require(p, k, "paths."), base)
                     for k in ("customers", "orders", "database", "reports_dir", "logs_dir")})

    formats = tuple(_require(raw, "date_formats", ""))
    if not formats:
        raise ConfigError("'date_formats' must list at least one format")

    o = _require(raw, "orders", "")
    status_map = {str(k).strip().lower(): str(v) for k, v in _require(o, "status_map", "orders.").items()}
    orders = OrderRules(
        id_pattern=_check_regex(_require(o, "id_pattern", "orders."), "orders.id_pattern"),
        min_order_date=_parse_date(_require(o, "min_order_date", "orders."), "orders.min_order_date"),
        sku_pattern=_check_regex(_require(o, "sku_pattern", "orders."), "orders.sku_pattern"),
        valid_currencies=frozenset(str(c).upper() for c in _require(o, "valid_currencies", "orders.")),
        status_map=status_map,
    )

    c = _require(raw, "customers", "")
    customers = CustomerRules(
        id_pattern=_check_regex(_require(c, "id_pattern", "customers."), "customers.id_pattern"),
        country_map={str(k).strip().lower(): str(v).upper()
                     for k, v in _require(c, "country_map", "customers.").items()},
    )

    q = _require(raw, "quality", "")
    rates = {str(k): float(v) for k, v in _require(q, "max_reject_rate", "quality.").items()}
    for entity in ("customers", "orders"):
        if entity not in rates:
            raise ConfigError(f"missing 'quality.max_reject_rate.{entity}'")
        if not 0.0 <= rates[entity] <= 1.0:
            raise ConfigError(f"'quality.max_reject_rate.{entity}' must be between 0 and 1")

    as_of = raw.get("as_of_date")
    return Config(
        paths=paths,
        date_formats=formats,
        orders=orders,
        customers=customers,
        max_reject_rate=rates,
        as_of_date=_parse_date(as_of, "as_of_date") if as_of else None,
        source_file=cfg_path,
    )
