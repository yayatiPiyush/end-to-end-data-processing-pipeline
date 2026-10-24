# Technical Explanation

Design decisions, trade-offs, assumptions, limitations and what I would do before production.

## Goals, in priority order

1. **Correct** — never load a value that was guessed; every input record is accounted for.
2. **Safe to re-run** — running twice, or after a crash, leaves the same result as running once.
3. **Explainable** — every rejected or modified record says why, in a form a non-engineer can read.
4. **Simple** — one process, one command, one file database, standard libraries. No framework to learn.

## Key decisions

| Decision | Why | Trade-off accepted |
|---|---|---|
| **Synthetic dataset from a seeded generator** (`scripts/generate_sample_data.py`) | Every defect type is known and its rate is controlled, so the report's numbers can be checked against intent. Fully offline and byte-for-byte reproducible. | Not a real distribution. The pipeline is source-agnostic at the primitive level; a real export is a config change. |
| **Two sources in two formats** (CSV + nested JSON Lines) | Shows schema handling for both flat and nested input. JSON **Lines** rather than one JSON array: a single corrupt record makes one line unreadable instead of the whole file. | Slightly less common than a JSON array. |
| **Ingest everything as raw strings** | Type coercion at read time hides problems (`pandas` turns `"N/A"` into NaN and `"007"` into 7). Keeping raw strings means one place does all interpretation, and the quarantine holds the record exactly as received. | Cleaning code must parse every field explicitly. |
| **Explicit, ordered date formats** | `03/04/2025` is ambiguous. Letting a library guess silently swaps day and month for some rows. The source exports US format, so only `%m/%d/%Y` is configured. | A source that switches to `DD/MM/YYYY` would mis-parse days ≤ 12. Mitigated by the gate (days > 12 fail) and documented as an assumption. |
| **Money as integer cents via `Decimal`** | Floats cannot represent 0.29 exactly; sums drift. Integer cents make `total = qty × price` exact and checkable in SQL. Values with > 2 decimals are rejected, not rounded. | Analysts divide by 100 (the `v_order_facts` view does it). |
| **Declarative validation rules** with two severities | Each failure gets a stable reason code, counted per rule. Adding a rule is one list entry. `error` = record unusable (key, FK, revenue fields). `warn` = record useful but one non-critical field is bad: field set to NULL, rule stored in `dq_flags`. | A home-grown rules engine instead of Great Expectations / Pandera. Chosen for zero dependencies and full control over row-level reasons; swapping later is contained to `validate.py`. |
| **All rules evaluated, no short-circuit** | A rejected record lists *every* problem, so a source owner fixes it once rather than in several rounds. | Slightly more work per row. |
| **Quarantine instead of drop** | Rejected records are kept with raw content, file, line and reasons. Nothing disappears silently, and records can be replayed once fixed. | Quarantine reflects the latest run only (see limitations). |
| **Two-step de-duplication** | Exact duplicates are removed *before* validation (they carry no information and would inflate the reject rate). Versions of the same key are resolved *after* validation, so the newest **valid** version wins, ordered by the source's `updated_at`, ties by file position. | If the newest version is invalid, an older valid one is loaded and the newer one sits in quarantine. The alternative (load nothing) loses a known-good record. |
| **SQLite** | Zero setup for a reviewer, yet real primary keys, foreign keys, `CHECK` constraints and atomic transactions, which is what idempotency depends on. SQL is portable to PostgreSQL (`ON CONFLICT` syntax is identical). | Single writer, not a server. Fine for batch; see future improvements. |
| **Upsert by business key + `row_hash` + stale guard** | PK = source business key, so a key exists once. Unchanged rows (same hash) are not written at all, so tables are byte-stable across re-runs. Updates only apply if the incoming `updated_at` is not older than the stored one, so replaying an old file cannot regress data. The guard exists in Python (for counting) **and** in the SQL `WHERE` clause (for safety). | Hash comparison needs a read of existing keys first (chunked `IN` queries). |
| **One transaction per run, integrity checks inside it** | Customers, orders and quarantine change together or not at all. Integrity checks (FK, totals, value ranges) run before `COMMIT`, so a bug that produces bad rows rolls back instead of publishing them. | A huge load holds one long write transaction. |
| **Quality gate (circuit breaker)** | If > 5% of customers or > 15% of orders are rejected, nothing is written and the run exits with code 3. A sudden spike in rejects usually means an upstream format change, and loading a partial dataset silently is worse than failing loudly. | Thresholds must be tuned per source; a legitimately bad day blocks the load until someone looks. |
| **Count reconciliation** | `read = unreadable + exact duplicates + rejected + superseded + loaded` is asserted on every run. If a future code change drops rows silently, the run fails. | — |
| **Run records, per-run JSON reports, distinct exit codes** | `pipeline_runs` stores status, input file SHA-256 and full statistics, so "what did the 3 a.m. run do, and with which file?" is one query. Exit codes 2/3/4 let a scheduler route "bad input" to the data owner and "bug" to the engineer. | — |
| **YAML config, CLI overrides, no secrets** | Rules that change with the business (currencies, status spellings, country map, thresholds) live in config, not code. SQLite needs no credentials; a server DB would read them from the environment. | — |

## How the design handles the four required scenarios

### Data quality issues

| Issue in the sample data | Handling | Outcome |
|---|---|---|
| Inconsistent formats (dates, `$1,299.00`, `usa`/`U.S.`, `SHIPPED `, ` c00012`) | Normalized in `transform.py`; each change tagged | Loaded; counted under *corrections* |
| Missing critical value (order id, date, quantity, price, currency) | `error` rule | Quarantined with reason |
| Invalid critical value (`two`, `N/A`, `-3.00`, `1.005`, `XYZ`, `2025-13-45`, future date) | `error` rule (distinct from "missing") | Quarantined with reason |
| Invalid non-critical value (email, country, signup date, status) | `warn` rule, field NULLed or set to `unknown` | Loaded, flagged in `dq_flags` |
| Order for a customer that does not exist | `customer_unknown` rule checks this run's valid customers **and** the database | Quarantined; loads automatically once the customer arrives |
| Exact duplicate lines | Dropped before validation | Counted |
| Several versions of one record | Newest valid `updated_at` wins | Older versions counted as superseded |
| Corrupt JSON line, CSV row with wrong field count | Line-level ingestion reject | Quarantined with line number and raw text |

### Repeated executions

The same input produces zero writes and identical table fingerprints (evidence in
[TESTING.md](TESTING.md#repeated-executions)). Changed input updates only the changed keys. An older file
replayed after a newer one is refused row-by-row (`skipped_stale`). Quarantine is rebuilt per run, so it
does not grow on re-runs.

### Processing failures

| Failure | Behaviour | Recovery |
|---|---|---|
| Config missing / invalid | Fails before touching data, exit 2 | Fix config, re-run |
| Input missing, empty, wrong encoding, required column missing | `IngestionError`, exit 2, run recorded as `failed` | Fix or re-deliver the file, re-run |
| Too many rejects | `DataQualityError`, exit 3, nothing written | Inspect the report; fix source or (deliberately) raise threshold |
| Exception during load (disk full, bug, killed process) | `ROLLBACK`, database unchanged, exit 1 | Re-run. Idempotency means no clean-up is ever needed |
| Integrity check fails after load | `ROLLBACK`, exit 4 | Bug in pipeline: fix code, re-run |
| Process killed mid-run | SQLite journal guarantees the open transaction never committed; `pipeline_runs` shows the run stuck in `running` | Re-run |

The test suite injects each of these (`tests/test_pipeline.py`, "failures and recovery").

### Changes in input data

| Change | Behaviour |
|---|---|
| New records | Inserted |
| Updated records with newer `updated_at` | Updated in place; `last_changed_run_id` records which run |
| Older extract replayed | Stale versions skipped, newer data kept |
| Record disappears from the snapshot | Kept (assumption 1: no hard deletes) |
| Extra column added upstream | Logged as a warning, ignored |
| Required column removed or renamed | Run fails fast with the column name (exit 2) |
| Field renamed in JSON (absent from every record) | Run fails fast ("schema has probably changed") |
| New spelling of a status or country | Loaded with a warning flag until the mapping is added to config |
| Format change across the file (e.g. new date format) | Reject rate spikes, quality gate stops the load |
| Database created by a newer schema version | Refused (`PRAGMA user_version` check) instead of guessing |

## Assumptions

1. Each run receives a **full snapshot** of both exports, not a delta. Records missing from a later snapshot
   are not deleted.
2. The source's `updated_at` reflects the order of changes. Ties fall back to file position (later wins).
3. Slash-separated dates are US `MM/DD/YYYY`. Dates have no time zone; `updated_at` timestamps share one zone.
4. One order line = one SKU (the export is already at line-item grain; `order_id` is unique per line).
5. `unit_price` is per unit in the order's own currency. No FX conversion. Zero price is allowed (promotions);
   negative is not (refunds are expressed through `status = returned`).
6. Key formats: customers `C#####`, orders `O######`, SKUs `SKU-###`. Anything else is a data error.
7. A customer is usable without a valid email, name, signup date or country; an order is not usable without
   its key, customer, date, SKU, quantity, price and currency.
8. "Future" is relative to the run date unless `as_of_date` is pinned (it is pinned in tests and docs for
   reproducibility).
9. Quality-gate thresholds (5% / 15%) are set from this source's known defect rate. In reality they would be
   agreed with the data owner.

## Known limitations

* **Memory-bound.** Everything is processed in pandas in memory. ~10k order rows/s end-to-end on the
  benchmark (below); fine up to a few million rows, not beyond.
* **Single writer.** SQLite serializes concurrent runs (`BEGIN IMMEDIATE` + 30 s busy timeout) but does not
  parallelize them.
* **No delete propagation**, by assumption 1.
* **Quarantine shows only the latest run.** History is in `pipeline_runs.stats` and `reports/runs/*.json`
  as counts, not as row-level history.
* **Cascading rejects.** A customer rejected for a missing id causes their orders to be rejected as
  `customer_unknown` (27 orders in the sample). Correct for integrity, but the report has to explain it.
* **Multi-currency revenue cannot be summed** without FX rates.
* **Email validation is a shape check**, not deliverability.
* **Mappings are manual.** New country or status spellings appear as warnings until config is updated.
  Deliberate (visible beats guessed), but it needs an owner.
* **Ambiguous date assumption** (see assumption 3) cannot be verified from the data alone.

## Performance

`python scripts/benchmark.py --factor 30` (96,390 order rows; measured in a small cloud dev container, Python 3.13, pandas 3):

| Run | Total | ingest | transform | validate | load |
|---|---|---|---|---|---|
| First load (81,150 inserted) | 9.7 s | 1.1 s | 3.9 s | 1.6 s | 3.1 s |
| Idempotent re-run (0 written) | 8.4 s | 1.0 s | 3.6 s | 1.5 s | 2.0 s |

**Bottleneck:** `transform`. The parsers apply small Python functions per value (`Decimal` money parsing,
correction tagging) for strictness and clarity. Next steps, in order of payoff: vectorize parsing with
pandas string methods and regex extraction; tag corrections with boolean masks instead of per-row lists;
process in chunks so memory stays flat; or move transformation into DuckDB/Polars. Loading is already
batched (`executemany`, chunked key lookups) and skips unchanged rows entirely.

## Future improvements (before production)

In the order I would do them:

1. **Scheduling, alerting and ownership.** Run under an orchestrator (Airflow, Dagster or even cron + a
   wrapper), alert on non-zero exit codes, route exit 2/3 to the source owner and 1/4 to engineering. The
   pipeline is already a library with a CLI, so the orchestrator only needs to call it.
2. **Volume and drift checks.** Compare row counts and reject rates with the previous successful run
   (stored in `pipeline_runs`) and fail on unexpected drops, e.g. orders falling 50% day-over-day. Today
   only the absolute reject rate is gated.
3. **PostgreSQL (or the team's warehouse) with migrations.** Same SQL, plus Alembic migrations, a staging
   schema, `COPY`-based bulk load, and connection credentials from the environment / a secret manager.
4. **Incremental ingestion and deletes.** A file manifest (path + SHA-256, already captured) to skip files
   already loaded, watermarking on `updated_at`, and an explicit delete/tombstone feed or CDC.
5. **Quarantine workflow.** Keep a history table, add a `reprocess` command, and send owners a daily digest
   of new rejects by reason.
6. **Data contracts.** Agree schemas, formats (ISO dates, explicit currency) and thresholds with the source
   teams; most corrections in the report disappear at the source.
7. **Observability.** Structured JSON logs and metrics (rows in/out, reject rate per rule, duration per stage)
   to a dashboard, so trends are visible before the gate trips.
8. **PII handling.** Names and emails are personal data: restrict access, or hash/tokenize them in the
   analytics tables.
9. **Performance** work described above, driven by real volumes.
10. **Engineering hygiene.** Type checking (mypy), linting (ruff) and a container image in CI alongside the
    existing test job.
