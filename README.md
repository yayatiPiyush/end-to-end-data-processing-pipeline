# E-commerce Orders Data Pipeline

A reliable, idempotent batch pipeline that ingests messy customer and order exports (JSON Lines + CSV),
cleans and validates them, and loads analysis-ready tables into SQLite, with every rejected record
quarantined and explained.

```bash
pip install -r requirements.txt
python -m pipeline            # the single command that runs everything
```

```
customers  read=423   valid=394   rejected=8    duplicates=21   inserted=394 updated=0 unchanged=0
orders     read=3213  valid=2705  rejected=304  duplicates=204  inserted=2705 updated=0 unchanged=0
status=success  report=reports/data_quality_report.md
```

Run it again and every count moves to `unchanged`: nothing is rewritten.

## What it does

1. **Ingest** `data/raw/customers.jsonl` and `data/raw/orders.csv` as raw strings. Unreadable files stop the
   run; unreadable lines are captured with their line number.
2. **Clean and transform**: drop exact duplicates, normalize ids, emails, countries, statuses, dates
   (three explicit formats) and money (`$1,299.00` → 129900 integer cents), and tag every correction.
3. **Validate** with 29 declarative rules. `error` rules send the record to quarantine with *all* its reasons;
   `warn` rules load it with the bad field set to NULL and the rule recorded in `dq_flags`. Then keep the
   newest valid version of each record, and stop the run if the reject rate exceeds a threshold.
4. **Load** in one transaction: upsert by business key (unchanged rows are not touched, older data never
   overwrites newer), rebuild the quarantine, run integrity checks, commit or roll back.
5. **Report**: per-run JSON statistics and a Markdown data quality report; the run is recorded in
   `pipeline_runs`.

## Deliverables

| Deliverable | Where |
|---|---|
| Source code | [`pipeline/`](pipeline) (package), [`scripts/`](scripts) (data generator, evidence, benchmark) |
| README | this file |
| Architecture overview | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (diagram, stages, data model, run lifecycle) |
| Testing evidence | [docs/TESTING.md](docs/TESTING.md), [`tests/`](tests) (128 tests, 98% coverage) |
| Data quality report | [docs/DATA_QUALITY_REPORT.md](docs/DATA_QUALITY_REPORT.md) (regenerated as `reports/data_quality_report.md` on every run) |
| Technical explanation | [docs/DESIGN.md](docs/DESIGN.md) (decisions, trade-offs, assumptions, limitations, performance) |
| Future improvements | [docs/DESIGN.md#future-improvements-before-production](docs/DESIGN.md#future-improvements-before-production) |
| Dataset and rule catalogue | [docs/DATA.md](docs/DATA.md) |

## Setup

Requires **Python 3.10+**. Tested on 3.10 (pandas 2.1) and 3.13 (pandas 3.0) with identical output.

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt                   # runtime: pandas, PyYAML; tests: pytest, pytest-cov
```

No database server, credentials or network access are needed. The sample input data is committed in
`data/raw/`; to regenerate it (byte-identical, fixed seed): `python scripts/generate_sample_data.py`.

## Running

| Command | Purpose |
|---|---|
| `python -m pipeline` (or `make run`) | Run the full pipeline |
| `python -m pipeline --dry-run` | Ingest, clean, validate and report, but write nothing to the database |
| `python -m pipeline --as-of 2026-10-21` | Pin the "today" used by date rules (reproducible results) |
| `python -m pipeline --orders other.csv --db /tmp/test.db` | Override any input/output path |
| `python -m pipeline rules` | Print the validation rule catalogue |
| `make test` / `make coverage` | Run the tests |
| `python scripts/walkthrough.py` | Run the stages **one at a time**, print what each produced and save each stage's output as CSV in `walkthrough_output/` |
| `python scripts/inspect_db.py` | Health summary of the database (no `sqlite3` CLI needed) |
| `make evidence` | Five consecutive runs showing idempotency, change handling and stale-replay protection |
| `make benchmark` | Throughput on ~96k order rows |

Exit codes: `0` success · `1` unexpected error · `2` config or input file error · `3` quality gate
failed (nothing written) · `4` post-load integrity check failed (rolled back).

### Outputs

| Path | Content |
|---|---|
| `data/processed/warehouse.db` | SQLite: `customers`, `orders`, `quarantine`, `pipeline_runs`, view `v_order_facts` |
| `reports/data_quality_report.md` | Human-readable report of the latest run |
| `reports/latest.json`, `reports/runs/<run_id>.json` | Full statistics per run |
| `logs/pipeline.log` | Log lines tagged with the run id |

Examples:

```bash
sqlite3 data/processed/warehouse.db "SELECT currency, COUNT(*), ROUND(SUM(total_amount), 2) FROM v_order_facts GROUP BY currency"
sqlite3 data/processed/warehouse.db "SELECT reasons, COUNT(*) FROM quarantine GROUP BY reasons ORDER BY 2 DESC LIMIT 5"
sqlite3 data/processed/warehouse.db "SELECT run_id, status, json_extract(stats, '$.entities.orders.load') FROM pipeline_runs"
```

## Configuration

All business rules that are likely to change live in [`config/pipeline.yaml`](config/pipeline.yaml):
input/output paths, accepted date formats, id/SKU patterns, accepted currencies, status and country
mappings, minimum order date, and the quality-gate thresholds (`max_reject_rate`: customers 5%, orders 15%).
The file is validated on load; a typo fails immediately with the key name. Use `--config` to point at another file.

## Project layout

```
pipeline/
  cli.py            command line, exit codes
  runner.py         orchestration: stages, stats, reconciliation, quality gate, transaction
  config.py         typed, validated configuration
  ingest.py         stage 1: read files as raw strings
  clean.py          reusable, dataset-agnostic cleaning primitives
  transform.py      stage 2: entity-specific cleaning + correction tags
  dedupe.py         exact-duplicate removal and version resolution
  validate.py       stage 3: rule engine and rule catalogues
  store.py          stage 4: SQLite schema, upserts, quarantine, integrity checks
  report.py         stage 5: JSON + Markdown reports
  errors.py         exception hierarchy -> exit codes
config/pipeline.yaml
data/raw/           sample input (committed)
scripts/            data generator, stage-by-stage walkthrough, DB inspection, repeated-run evidence, benchmark
tests/              unit + end-to-end tests, hand-written fixtures and expected outputs
docs/               architecture, design, data, testing, data quality report
```

## Extending

**Add a validation rule** — one entry in `customer_rules()` or `order_rules()` in `pipeline/validate.py`.
The check returns `True` where a row passes:

```python
Rule("quantity_too_large", "warn", "quantity above 100 is unusual for retail orders",
     absent_or("quantity", lambda df: df["quantity"].fillna(0) <= 100)),
```

It is automatically evaluated, counted in the report, stored in `dq_flags` / `quarantine`, and listed by
`python -m pipeline rules`. Use severity `error` to reject instead, and `on_fail=nullify("col")` or
`set_value("col", v)` to repair the field for warnings.

**Accept a new spelling** (status, country, currency, date format) — edit `config/pipeline.yaml`; no code change.

**Add a column** — add it to `ORDER_COLUMNS` in `ingest.py`, parse it in `transform.py`, add it to the
table and `ORDER_COLUMNS` in `store.py`. Changing the table means bumping `SCHEMA_VERSION`.

**Add a source** — write a `read_<source>()` in `ingest.py` returning an `IngestResult`, a
`transform_<entity>()` and a rule list, then call `_process()` for it in `runner.py`.

## Key assumptions

Full list and reasoning in [docs/DESIGN.md](docs/DESIGN.md#assumptions). The ones that most affect results:

* Each run receives a **full snapshot**; records missing from a later file are not deleted.
* The source's `updated_at` orders versions of a record; ties go to the later line.
* Slash dates are US `MM/DD/YYYY`.
* An order without a valid key, customer, date, SKU, quantity, price or currency is rejected; a customer is
  only rejected for a missing or malformed id.
* Prices are per unit in the order's currency; no FX conversion.
