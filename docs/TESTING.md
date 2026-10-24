# Testing and Evaluation

```bash
make test                # or: python -m pytest
make coverage            # with line coverage
python scripts/rerun_evidence.py   # repeated-execution demo on the full sample data
python scripts/benchmark.py        # throughput
```

## Test suite

128 tests, ~3 s, no network, every test uses its own temporary database.

| File | Test cases | What it covers |
|---|---|---|
| `test_clean.py` | 54 | Money → cents (incl. `$1,299.00`, `0.29` exactness, `1.005` refused), strict integers (`2.0` ok, `2.5` refused), each date format, the ambiguous `03/04/2025`, ISO timestamps, whitespace, id/email normalization, country mapping, email shape, `None` never becoming `NaN` (regression) |
| `test_validate.py` | 18 | Every order and customer rule; all reasons reported (no short-circuit); missing vs unparseable are different reasons; date bounds at the exact edges; FK lookup after id normalization; warnings NULL the field but keep the row; warnings never attached to rejected rows; correction tagging |
| `test_ingest.py` | 15 | Missing file, directory, empty file, header-only, missing required column, extra column (warned, dropped), case/space-insensitive header, wrong field count, quoted comma + newline, raw strings preserved (`N/A`, `007`), UTF-8 BOM, UTF-16 refused, malformed JSON line, non-object line, nested address flattening, schema drift in JSONL, file hash |
| `test_dedupe.py` | 5 | Exact duplicates, latest `updated_at` wins regardless of file order, timestamp tie → later line (and counted), missing timestamp loses, empty input |
| `test_store.py` | 9 | Insert → unchanged (row untouched, run ids unchanged), newer change updates in place, stale replay refused (in Python **and** by the SQL guard alone), PK and FK enforced by the database, row hash stability, quarantine replaced not appended, schema-version guard |
| `test_config.py` | 11 | Default config, each class of config error, malformed YAML, `as_of_date` |
| `test_pipeline.py` | 16 | End-to-end: golden output, quarantine contents, count reconciliation, idempotency, changed input, stale replay, late-arriving customer, FK satisfied by earlier runs, missing input, quality gate, crash mid-load, failed integrity check, dry run, CLI exit codes, full sample dataset twice |

## Representative input and expected output

`tests/fixtures/` holds a small, hand-written input where **every line exercises a specific case**, and the
expected tables written by hand (not snapshotted from the pipeline):

* `customers.jsonl` — 8 lines: clean record, padded/upper-case email, lower-case padded id, invalid email,
  `U.S.`, unknown country, missing name, missing id, exact duplicate, newer version, truncated JSON, future signup.
* `orders.csv` — 18 rows: `$1,299.00`, `03/04/2025`, `1.0`, ` 49.5 `, `usd`, `DELIVERED `, unknown status,
  unknown customer, a row failing four rules at once, future date, missing id, two versions of one order, exact
  duplicate, `two`, `-3.00`, `0.10 × 3`, order before signup, order for a customer whose line was corrupt,
  short row, 2019 date, `SKU-1`, `1.005`.
* `expected_customers.csv`, `expected_orders.csv` — the exact rows that must be in the database.

`test_golden_output` compares the database to these files column by column; `test_quarantine_contents`
checks every rejected order and its full list of reasons; `test_statistics_reconcile` checks every count.

## Edge cases and failure scenarios

| Scenario | Test | Expected and verified behaviour |
|---|---|---|
| Input file missing | `test_missing_input_fails_and_is_recorded` | Exit 2, run stored as `failed` with the error, report still written |
| Reject rate above threshold | `test_quality_gate_blocks_load_and_writes_nothing` | Exit 3, both tables empty |
| Crash while loading orders after customers were written | `test_failure_mid_load_rolls_back_everything` | Customers **not** half-updated; a plain re-run then succeeds |
| Integrity check fails | `test_failed_integrity_check_rolls_back` | Rollback, exit 4 |
| Older extract replayed | `test_replaying_old_extract_does_not_regress_data` | `skipped_stale = 1`, newer status kept |
| Customer arrives after its order | `test_orphan_order_loads_once_its_customer_arrives` | Order loads on the next run and leaves quarantine |
| Customer file only has some customers | `test_customers_from_earlier_runs_satisfy_foreign_keys` | Customers already in the DB still satisfy order FKs |
| Upstream renames a JSON field | `test_jsonl_schema_drift_raises` | Fails fast with "schema has probably changed" |
| Upstream adds a CSV column | `test_header_is_case_and_space_insensitive_and_extra_columns_are_ignored` | Warning logged, column ignored |
| Dry run | `test_dry_run_writes_no_data` | Full validation and report, no rows written |

## Repeated executions

Unit level: `test_rerun_is_idempotent` runs twice and asserts zero inserts/updates, identical table
contents (ignoring run ids) and identical fingerprints. `test_cli_full_sample_dataset` does the same through
the CLI on the full sample.

On the full sample data, `python scripts/rerun_evidence.py` produced:

| Run | Entity | Inserted | Updated | Unchanged | Skipped stale | Rows in table | Fingerprint |
|---|---|---|---|---|---|---|---|
| 1. first load | customers | 394 | 0 | 0 | 0 | 394 | `ad23166eea1e0fff` |
| 1. first load | orders | 2705 | 0 | 0 | 0 | 2705 | `101b1509b8e69d96` |
| 2. identical re-run | customers | 0 | 0 | 394 | 0 | 394 | `ad23166eea1e0fff` |
| 2. identical re-run | orders | 0 | 0 | 2705 | 0 | 2705 | `101b1509b8e69d96` |
| 3. changed input | customers | 0 | 0 | 394 | 0 | 394 | `ad23166eea1e0fff` |
| 3. changed input | orders | 2 | 3 | 2702 | 0 | 2707 | `6e93460d4f204c03` |
| 4. replay original (older) extract | customers | 0 | 0 | 394 | 0 | 394 | `ad23166eea1e0fff` |
| 4. replay original (older) extract | orders | 0 | 0 | 2702 | 3 | 2707 | `6e93460d4f204c03` |
| 5. re-run changed input | customers | 0 | 0 | 394 | 0 | 394 | `ad23166eea1e0fff` |
| 5. re-run changed input | orders | 0 | 0 | 2707 | 0 | 2707 | `6e93460d4f204c03` |

Reading it: run 2 writes nothing and the fingerprints match run 1. Run 3 (3 orders re-exported with a new
status, 2 new orders) touches exactly 5 rows. Run 4 replays the original file: the 3 changed orders are refused
as stale, so the newer data survives, and the 2 new orders stay (no deletes, by assumption). Run 5 is a no-op.

## Results

```
$ python -m pytest --cov=pipeline
128 passed in 6.02s

Name                        Stmts   Miss  Cover
-----------------------------------------------
pipeline/clean.py              75      2    97%
pipeline/cli.py                50      2    96%
pipeline/config.py             92      1    99%
pipeline/dedupe.py             21      0   100%
pipeline/ingest.py            104      2    98%
pipeline/report.py             65      0   100%
pipeline/runner.py            145      5    97%
pipeline/store.py              90      0   100%
pipeline/transform.py          67      0   100%
pipeline/validate.py           85      0   100%
-----------------------------------------------
TOTAL                         835     15    98%
```

The suite also passes with `-W error` (no resource or deprecation warnings).

**Portability.** The suite also passes on Python 3.10 with pandas 2.1.4 (the minimum supported
versions), and the full sample run produces the same table fingerprints there as on Python 3.13 / pandas 3.0.5
(`customers=ad23166eea1e0fff`, `orders=101b1509b8e69d96`). CI runs 3.10, 3.12 and 3.13.

**Do the tests actually catch bugs?** Coverage only shows code ran. As a spot check I injected two
plausible bugs by hand and confirmed the suite fails:

| Injected bug | Tests failing |
|---|---|
| Version resolution keeps the *oldest* version instead of the newest | 4 |
| Upsert stops detecting unchanged rows (rewrites every row each run) | 6 |

## Evaluation metrics and what they show

| Metric | Where | What it demonstrates |
|---|---|---|
| **Reconciliation** (read = unreadable + duplicates + rejected + superseded + loaded) | every run, asserted | No record is silently lost or double-counted |
| **Reject rate** per entity, and **rejections per rule** | report, quality gate | Size and cause of data loss; the gate turns it into a stop/go decision |
| **Correction rate** per correction type | report | How much normalization the source needs (input to data contracts) |
| **Warning counts** per rule | report, `dq_flags` | How many loaded records have a known-bad field, so analysts can filter |
| **Inserted / updated / unchanged / skipped stale** | report, `pipeline_runs` | Idempotency and incremental behaviour, run by run |
| **Content fingerprints** | report | Two runs produced byte-identical data |
| **Post-load check violations** (must be 0) | report | Referential integrity and arithmetic consistency of what was committed |
| **Stage timings** and rows/s | report, benchmark | Where time goes and how far the design scales |
