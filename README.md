# E-commerce Orders Data Pipeline

A reliable, idempotent batch pipeline that ingests messy customer and order exports (JSON Lines + CSV), cleans and validates them, and loads analysis-ready tables into SQLite, with every rejected record quarantined and explained.

## Data Issues Addressed

The pipeline tackles several common data quality issues found in the raw customer and order exports:

1. **Inconsistent formats**: The raw data comes in a mix of JSON Lines (customers) and CSV (orders) formats, which need to be parsed and harmonized.
2. **Messy and inconsistent values**: The raw data contains inconsistencies in date formats, money amounts, status codes, and country names. The pipeline cleans and standardizes these values.
3. **Duplicates**: The exports may include exact duplicate records, which need to be identified and dropped.
4. **Invalid or missing data**: Some records may have missing or invalid values for critical fields like customer ID, order ID, date, SKU, quantity, price, or currency. The pipeline validates these fields and quarantines records that fail the checks.
5. **Stale data**: The exports may contain outdated versions of records. The pipeline keeps the newest valid version of each record based on the `updated_at` timestamp.

By addressing these issues, the pipeline ensures that the loaded data is clean, consistent, and ready for analysis.

## What it does

1. **Ingest** `data/raw/customers.jsonl` and `data/raw/orders.csv` as raw strings. Unreadable files stop the run; unreadable lines are captured with their line number.
2. **Clean and transform**: drop exact duplicates, normalize ids, emails, countries, statuses, dates (three explicit formats) and money (`$1,299.00` → 129900 integer cents), and tag every correction.
3. **Validate** with 29 declarative rules. `error` rules send the record to quarantine with *all* its reasons; `warn` rules load it with the bad field set to NULL and the rule recorded in `dq_flags`. Then keep the newest valid version of each record, and stop the run if the reject rate exceeds a threshold.
4. **Load** in one transaction: upsert by business key (unchanged rows are not touched, older data never overwrites newer), rebuild the quarantine, run integrity checks, commit or roll back.
5. **Report**: per-run JSON statistics and a Markdown data quality report; the run is recorded in `pipeline_runs`.

## Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yayatiPiyush/end-to-end-data-processing-pipeline.git
   cd end-to-end-data-processing-pipeline
   ```

2. Create a virtual environment and activate it:

   - For Unix/macOS:
     ```bash
     python -m venv .venv
     source .venv/bin/activate
     ```

   - For Windows (Command Prompt):
     ```bash
     python -m venv .venv
     .venv\Scripts\activate.bat
     ```

   - For Windows (PowerShell):
     ```powershell
     python -m venv .venv
     .venv\Scripts\Activate.ps1
     ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

## Running the Pipeline

To run the full pipeline, execute the following command:
```bash
python -m pipeline
```

This will ingest the sample data from `data/raw/`, clean and validate it, and load the results into a SQLite database.

## Output Files

The pipeline generates the following output files:

- `data/processed/warehouse.db`: The final SQLite database containing the `customers`, `orders`, `quarantine`, and `pipeline_runs` tables.
- `reports/data_quality_report.md`: A human-readable report of the latest run.
- `reports/latest.json` and `reports/runs/<run_id>.json`: Full statistics per run.
- `logs/pipeline.log`: Log lines tagged with the run ID.

Note: The `warehouse.db` file represents the state after the latest successful run of the pipeline. You can use this file to explore the loaded data and run queries. To inspect the `warehouse.db` file, run:
```bash
python scripts\inspect_db.py
```

## Additional Commands

- `python -m pipeline --dry-run`: Ingest, clean, validate, and report, but write nothing to the database.
- `python -m pipeline --as-of YYYY-MM-DD`: Pin the "today" used by date rules for reproducible results.
- `python -m pipeline rules`: Print the validation rule catalogue.
- `make test` or `make coverage`: Run the test suite.
- `python scripts/walkthrough.py`: Run the pipeline stages one at a time, printing the output of each stage and saving intermediate CSVs in `walkthrough_output/`.

## Key assumptions

Full list and reasoning in [docs/DESIGN.md](docs/DESIGN.md#assumptions). The ones that most affect results:

- Each run receives a **full snapshot**; records missing from a later file are not deleted.
- The source's `updated_at` orders versions of a record; ties go to the later line .
- Slash dates are US `MM/DD/YYYY`.
- An order without a valid key, customer, date, SKU, quantity, price or currency is rejected; a customer is only rejected for a missing or malformed id.
- Prices are per unit in the order's currency; no FX conversion.

For more details on the pipeline's architecture, design decisions, deliverables, configuration, project layout, and extensibility, please refer to the documentation in the `docs/` directory.
