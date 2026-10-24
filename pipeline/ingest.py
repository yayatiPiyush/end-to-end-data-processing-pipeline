"""Stage 1 - Ingestion.

Reads raw files into DataFrames of *untyped strings* and nothing more. No
cleaning happens here: keeping ingestion dumb means a parsing bug can never
silently change values, and every later stage can see exactly what arrived.

Two failure levels:

* **File-level** problems (missing file, unreadable encoding, empty file,
  required column absent) raise :class:`IngestionError` and stop the run.
  Loading from a half-broken extract is worse than not loading.
* **Line-level** problems (malformed JSON line, CSV row with the wrong number
  of fields) are captured as ingestion rejects with their line number and
  raw text, and the rest of the file is processed.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .errors import IngestionError

log = logging.getLogger(__name__)

ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "sku", "quantity",
                 "unit_price", "currency", "status", "updated_at"]
CUSTOMER_FIELDS = ["customer_id", "name", "email", "signup_date", "city",
                   "country", "updated_at"]
# Fields whose total absence means the upstream schema changed (renamed or
# dropped), as opposed to individual records simply being incomplete.
CUSTOMER_REQUIRED = ["customer_id", "updated_at"]

META_COLUMNS = ["_source_line", "_raw"]


@dataclass
class IngestResult:
    entity: str
    source_file: Path
    file_sha256: str
    frame: pd.DataFrame                                  # parsed rows, strings only
    rejects: list[dict] = field(default_factory=list)    # line-level ingestion failures

    @property
    def lines_read(self) -> int:
        return len(self.frame) + len(self.rejects)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_text(path: Path) -> str:
    if not path.exists():
        raise IngestionError(f"source file not found: {path}")
    if not path.is_file():
        raise IngestionError(f"source path is not a file: {path}")
    try:
        # utf-8-sig transparently strips a BOM added by Excel-style exporters.
        return path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IngestionError(f"{path.name} is not valid UTF-8: {exc}") from exc


def _canonical(record: dict) -> str:
    """Stable JSON text of a record; used for quarantine and exact-duplicate checks."""
    return json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)


def _reject(line_no: int, raw: str, reason: str) -> dict:
    return {"_source_line": line_no, "_raw": raw, "reasons": [reason]}


def read_orders_csv(path: Path) -> IngestResult:
    text = _read_text(path)
    reader = csv.reader(io.StringIO(text, newline=""))  # handles quoted newlines
    try:
        header = [h.strip().lower() for h in next(reader)]
    except StopIteration:
        raise IngestionError(f"{path.name} is empty") from None

    missing = [c for c in ORDER_COLUMNS if c not in header]
    if missing:
        raise IngestionError(f"{path.name} is missing required columns: {missing}")
    extra = [c for c in header if c not in ORDER_COLUMNS]
    if extra:
        log.warning("%s has unexpected columns %s; they will be ignored", path.name, extra)

    rows, rejects = [], []
    for fields in reader:
        line_no = reader.line_num
        if not fields or all(not f.strip() for f in fields):
            continue                                          # blank line
        if len(fields) != len(header):
            rejects.append(_reject(line_no, ",".join(fields),
                                   f"wrong_field_count({len(fields)}!={len(header)})"))
            continue
        record = dict(zip(header, fields))
        record = {c: record[c] for c in ORDER_COLUMNS}
        rows.append({**record, "_source_line": line_no, "_raw": _canonical(record)})

    if not rows and not rejects:
        raise IngestionError(f"{path.name} has a header but no data rows")
    frame = pd.DataFrame(rows, columns=ORDER_COLUMNS + META_COLUMNS).astype(
        {c: "object" for c in ORDER_COLUMNS})
    log.info("ingested %s: %d rows, %d unreadable", path.name, len(frame), len(rejects))
    return IngestResult("orders", path, _sha256(path), frame, rejects)


def _flatten_customer(obj: dict) -> dict:
    """Map the nested CRM record onto flat columns. Missing keys become None."""
    address = obj.get("address") if isinstance(obj.get("address"), dict) else {}
    flat = {
        "customer_id": obj.get("customer_id"),
        "name": obj.get("name"),
        "email": obj.get("email"),
        "signup_date": obj.get("signup_date"),
        "city": address.get("city"),
        "country": address.get("country"),
        "updated_at": obj.get("updated_at"),
    }
    # Keep values as text (or None) so typing happens in one place: cleaning.
    return {k: (None if v is None else str(v)) for k, v in flat.items()}


def read_customers_jsonl(path: Path) -> IngestResult:
    text = _read_text(path)
    rows, rejects = [], []
    seen_keys: set[str] = set()
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            rejects.append(_reject(line_no, line, f"malformed_json({exc.msg})"))
            continue
        if not isinstance(obj, dict):
            rejects.append(_reject(line_no, line, "not_a_json_object"))
            continue
        seen_keys.update(obj)
        if isinstance(obj.get("address"), dict):
            seen_keys.update(obj["address"])
        rows.append({**_flatten_customer(obj), "_source_line": line_no, "_raw": _canonical(obj)})

    if not rows and not rejects:
        raise IngestionError(f"{path.name} is empty")
    absent = [k for k in CUSTOMER_REQUIRED if rows and k not in seen_keys]
    if absent:
        raise IngestionError(
            f"{path.name}: no record contains required field(s) {absent}; "
            "the upstream schema has probably changed")
    frame = pd.DataFrame(rows, columns=CUSTOMER_FIELDS + META_COLUMNS).astype(
        {c: "object" for c in CUSTOMER_FIELDS})
    log.info("ingested %s: %d records, %d unreadable", path.name, len(frame), len(rejects))
    return IngestResult("customers", path, _sha256(path), frame, rejects)
