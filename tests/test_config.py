"""Configuration is validated up front so mistakes fail fast with a clear message."""

from pathlib import Path

import pytest
import yaml

from pipeline.config import DEFAULT_CONFIG, load_config
from pipeline.errors import ConfigError


def write_cfg(tmp_path: Path, mutate) -> Path:
    data = yaml.safe_load(DEFAULT_CONFIG.read_text())
    mutate(data)
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


def test_default_config_loads():
    cfg = load_config()
    assert cfg.orders.valid_currencies == {"USD", "EUR", "GBP", "INR"}
    assert cfg.customers.country_map["usa"] == "US"
    assert cfg.paths.orders.is_absolute()          # resolved against repo root


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d.pop("paths"), "paths"),
    (lambda d: d["paths"].pop("orders"), "paths.orders"),
    (lambda d: d.__setitem__("date_formats", []), "date_formats"),
    (lambda d: d["orders"].__setitem__("min_order_date", "01/01/2020"), "ISO date"),
    (lambda d: d["orders"].__setitem__("sku_pattern", "[unclosed"), "regular expression"),
    (lambda d: d["quality"]["max_reject_rate"].__setitem__("orders", 1.5), "between 0 and 1"),
    (lambda d: d["quality"]["max_reject_rate"].pop("customers"), "customers"),
])
def test_invalid_config_is_rejected(tmp_path, mutate, message):
    with pytest.raises(ConfigError, match=message):
        load_config(write_cfg(tmp_path, mutate))


def test_not_yaml_mapping(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("- just\n- a list\n")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(p)


def test_malformed_yaml(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("paths: [unclosed\n")
    with pytest.raises(ConfigError, match="YAML"):
        load_config(p)


def test_as_of_date_is_parsed(tmp_path):
    cfg = load_config(write_cfg(tmp_path, lambda d: d.__setitem__("as_of_date", "2026-01-31")))
    assert cfg.effective_as_of().isoformat() == "2026-01-31"
