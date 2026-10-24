"""Run the pipeline ONE STAGE AT A TIME and show what each stage produced.

`python -m pipeline` runs every stage in one go, which is what you want in
production but hides the intermediate results. This script calls the same
functions the real pipeline uses, in the same order, but stops after each
stage to print a summary plus a few traced example records, and saves that
stage's full output as CSV in `walkthrough_output/` (open them in Excel).

At the end it runs the real orchestrator on the same input and checks that
its numbers match the stage-by-stage numbers.

    python scripts/walkthrough.py            # run all steps
    python scripts/walkthrough.py --pause    # wait for Enter between steps
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline import dedupe, ingest, store, transform, validate  # noqa: E402
from pipeline.config import load_config  # noqa: E402
from pipeline.logging_utils import setup_logging  # noqa: E402
from pipeline.runner import customer_records, order_records, run_pipeline  # noqa: E402

OUT = ROOT / "walkthrough_output"
AS_OF = date(2026, 10, 21)          # pinned so the numbers match the docs
PAUSE = False

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 60)


# ---------------------------------------------------------------- helpers

def step(number: str, title: str, purpose: str) -> None:
    if PAUSE and number != "0":
        input("\n[press Enter for the next step] ")
    print("\n" + "=" * 78)
    print(f"STEP {number}: {title}")
    print(f"Purpose: {purpose}")
    print("=" * 78)


def note(text: str) -> None:
    print(f"\n  >> {text}")


def show(df: pd.DataFrame, columns: list[str] | None = None, n: int = 5) -> None:
    view = df[columns] if columns else df
    print(view.head(n).to_string(index=False))


def save(df: pd.DataFrame, name: str) -> None:
    """Write a stage's output as CSV; list columns are joined so Excel can read them."""
    out = df.copy()
    for col in out.columns:
        if out[col].map(lambda v: isinstance(v, list)).any():
            out[col] = out[col].map(lambda v: ", ".join(v) if isinstance(v, list) else v)
    path = OUT / name
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n  saved -> walkthrough_output/{name}  ({len(out)} rows)")


def before_after(raw_row: pd.Series, clean_row: pd.Series, pairs: list[tuple[str, str]]) -> None:
    """Print one record field by field: raw text vs cleaned value."""
    print(f"  {'field':<14}{'raw (as received)':<34}cleaned")
    print(f"  {'-' * 14}{'-' * 34}{'-' * 26}")
    for raw_col, clean_col in pairs:
        raw_v = raw_row[raw_col]
        clean_v = clean_row[clean_col]
        if isinstance(clean_v, pd.Timestamp):
            clean_v = clean_v.strftime("%Y-%m-%d")
        print(f"  {raw_col:<14}{repr(raw_v):<34}{'NULL' if pd.isna(clean_v) else clean_v}")
    print(f"  corrections tagged: {clean_row['_corrections'] or 'none'}")


# ---------------------------------------------------------------- the walk

def main() -> None:
    global PAUSE
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--pause", action="store_true", help="wait for Enter between steps")
    PAUSE = ap.parse_args().pause

    setup_logging(None, "WARNING")
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()
    cfg = replace(load_config(), as_of_date=AS_OF).with_overrides(
        database=OUT / "walkthrough.db", reports_dir=OUT / "reports", logs_dir=OUT / "logs")

    # ------------------------------------------------------------ 0 config
    step("0", "Load configuration (config/pipeline.yaml)",
         "Business rules live in a file, not in code, and are checked before any data is touched.")
    print(f"  customers file     : {cfg.paths.customers.relative_to(ROOT)}")
    print(f"  orders file        : {cfg.paths.orders.relative_to(ROOT)}")
    print(f"  date formats       : {list(cfg.date_formats)}")
    print(f"  valid currencies   : {sorted(cfg.orders.valid_currencies)}")
    print(f"  status spellings   : {len(cfg.orders.status_map)} mapped -> {sorted(set(cfg.orders.status_map.values()))}")
    print(f"  country spellings  : {len(cfg.customers.country_map)} mapped -> {sorted(set(cfg.customers.country_map.values()))}")
    print(f"  max reject rate    : {cfg.max_reject_rate}")
    print(f"  'today' for rules  : {cfg.effective_as_of()}  (pinned for reproducible numbers)")

    # ------------------------------------------------------------ 1 ingest
    step("1", "INGEST - read the files exactly as they are",
         "Get the data in WITHOUT changing it, so nothing is lost or silently converted.")
    cust_in = ingest.read_customers_jsonl(cfg.paths.customers)
    ord_in = ingest.read_orders_csv(cfg.paths.orders)
    for res in (cust_in, ord_in):
        print(f"  {res.source_file.name:<16} lines read={res.lines_read:<5} parsed={len(res.frame):<5} "
              f"unreadable={len(res.rejects)}   sha256={res.file_sha256[:12]}...")
    print("\n  First 3 orders, every value is still a raw STRING (note '$', spaces, mixed dates/case):")
    show(ord_in.frame, ingest.ORDER_COLUMNS[:8], 3)
    print("\n  Customers JSON is nested (address.city / address.country) -> flattened to columns:")
    show(cust_in.frame, ["customer_id", "email", "signup_date", "city", "country"], 3)
    print("\n  Lines that could not be read at all (kept, not thrown away):")
    for r in cust_in.rejects + ord_in.rejects:
        print(f"    line {r['_source_line']}: {r['reasons'][0]:<45} raw={r['_raw'][:45]!r}")
    note("Why raw strings? pandas would turn 'N/A' into NaN and '007' into 7 - we would never know what arrived.")
    save(ord_in.frame.drop(columns="_raw"), "01_ingested_orders.csv")
    save(cust_in.frame.drop(columns="_raw"), "01_ingested_customers.csv")

    # ------------------------------------------------------------ 2a exact duplicates
    step("2a", "REMOVE EXACT DUPLICATES",
         "Identical lines (e.g. an export that retried) carry no information - drop them early.")
    ord_raw, ord_exact = dedupe.drop_exact_duplicates(ord_in.frame)
    cust_raw, cust_exact = dedupe.drop_exact_duplicates(cust_in.frame)
    print(f"  customers: {len(cust_in.frame)} -> {len(cust_raw)}  ({cust_exact} exact duplicates removed)")
    print(f"  orders   : {len(ord_in.frame)} -> {len(ord_raw)}  ({ord_exact} exact duplicates removed)")
    dup_mask = ord_in.frame["_raw"].duplicated(keep=False)
    first_dup = ord_in.frame[dup_mask].iloc[0]["_raw"]
    example = ord_in.frame[ord_in.frame["_raw"] == first_dup]
    print("\n  Example - the same order appears on two lines, byte for byte; the first is kept:")
    show(example, ["_source_line", "order_id", "customer_id", "order_date", "unit_price", "status"])
    note("Done BEFORE validation, otherwise one bad duplicated row would be counted as two rejects.")

    # ------------------------------------------------------------ 2b transform
    step("2b", "CLEAN & TRANSFORM - make every value consistent and typed",
         "Same thing must be written the same way: 'USA'/'U.S.'->US, '$1,299.00'->129900 cents, "
         "'03/14/2025'->2025-03-14.")
    cust_clean = transform.transform_customers(cust_raw, cfg)
    ord_clean = transform.transform_orders(ord_raw, cfg)
    ord_pairs = [("order_id", "order_id"), ("customer_id", "customer_id"), ("order_date", "order_date"),
                 ("quantity", "quantity"), ("unit_price", "unit_price_cents"), ("currency", "currency"),
                 ("status", "status")]
    many = ord_clean[ord_clean["_corrections"].map(len) >= 3].index[0]
    dollar = ord_raw[ord_raw["unit_price"].str.contains(r"\$", na=False)].index[0]
    for idx, label in ((many, "an order needing several fixes"), (dollar, "a price written with '$' and ','")):
        print(f"\n  Example - {label} (file line {ord_raw.loc[idx, '_source_line']}):")
        before_after(ord_raw.loc[idx], ord_clean.loc[idx], ord_pairs)
        print(f"  {'total_cents':<14}{'(computed)':<34}{ord_clean.loc[idx, 'total_cents']}  = quantity x unit_price_cents")
    cidx = cust_clean[cust_clean["_corrections"].map(len) >= 3].index[0]
    print(f"\n  Example - a customer (file line {cust_raw.loc[cidx, '_source_line']}):")
    before_after(cust_raw.loc[cidx], cust_clean.loc[cidx],
                 [("customer_id", "customer_id"), ("email", "email"), ("signup_date", "signup_date"),
                  ("country", "country_code")])
    bad = ord_clean[ord_clean["unit_price_cents"].isna() & ord_raw["unit_price"].notna()
                    & (ord_raw["unit_price"].str.strip() != "")].index[0]
    print(f"\n  Example - a value that CANNOT be parsed: unit_price={ord_raw.loc[bad, 'unit_price']!r} "
          f"-> unit_price_cents={ord_clean.loc[bad, 'unit_price_cents']}")
    note("Cleaning never decides if a record is good; unparseable values just become empty (NA). "
         "Deciding is step 3's job.")
    print("\n  How many rows needed each kind of correction (orders):")
    counts = pd.Series([t for tags in ord_clean["_corrections"] for t in tags]).value_counts()
    print("   " + counts.to_string().replace("\n", "\n   "))
    save(ord_clean, "02_cleaned_orders.csv")
    save(cust_clean, "02_cleaned_customers.csv")

    # ------------------------------------------------------------ 3a validate
    step("3a", "VALIDATE - check every record against the rules",
         "Decide, record by record: load it, load it with a warning, or reject it (and say why).")
    c_rules = validate.customer_rules(cfg)
    c_val = validate.apply_rules(cust_clean, c_rules)
    valid_customers = set(c_val.accepted["customer_id"])
    signup = dict(zip(c_val.accepted["customer_id"], c_val.accepted["signup_date"]))
    o_rules = validate.order_rules(cfg, valid_customers, {k: v for k, v in signup.items() if pd.notna(v)})
    o_val = validate.apply_rules(ord_clean, o_rules)
    print(f"  customers: {len(cust_clean)} checked -> {len(c_val.accepted)} accepted, {len(c_val.rejected)} rejected")
    print(f"  orders   : {len(ord_clean)} checked -> {len(o_val.accepted)} accepted, {len(o_val.rejected)} rejected")
    note("Customers are validated FIRST because each order must point to a valid customer.")
    sev = {r.name: r.severity for r in o_rules}
    print("\n  Order rule failures (error = reject, warn = keep but flag):")
    for name, n in o_val.failures.most_common():
        print(f"    {sev[name]:<6}{name:<26}{n}")
    multi = o_val.rejected[o_val.rejected["reasons"].map(len) >= 2].iloc[0]
    print(f"\n  Example - rejected order {multi['order_id']} lists EVERY problem, not just the first:")
    print(f"    reasons: {multi['reasons']}")
    warn = o_val.accepted[o_val.accepted["dq_flags"].map(bool)].iloc[0]
    print(f"\n  Example - warned order {warn['order_id']}: raw status {warn['status__raw']!r} -> stored as "
          f"{warn['status']!r}, flags={warn['dq_flags']}")
    cw = c_val.accepted[c_val.accepted["dq_flags"].map(lambda f: "email_invalid" in f)].iloc[0]
    print(f"  Example - warned customer {cw['customer_id']}: raw email {cw['email__raw']!r} -> NULL, "
          f"flags={cw['dq_flags']}")
    note("Bad email -> keep the customer (rejecting would also orphan all their orders). "
         "Bad price/currency -> reject (guessing would corrupt revenue).")
    keep = ["_source_line", "order_id", "customer_id", "order_date__raw", "quantity__raw",
            "unit_price__raw", "currency__raw", "reasons"]
    save(o_val.rejected[keep], "03_rejected_orders.csv")
    save(o_val.accepted, "03_accepted_orders.csv")
    save(c_val.rejected[["_source_line", "customer_id__raw", "reasons"]], "03_rejected_customers.csv")

    # ------------------------------------------------------------ 3b versions
    step("3b", "RESOLVE VERSIONS - one row per order_id / customer_id",
         "The export re-sends a record when it changes; keep only the newest VALID version.")
    c_final, c_sup, _ = dedupe.resolve_versions(c_val.accepted, "customer_id")
    o_final, o_sup, o_ties = dedupe.resolve_versions(o_val.accepted, "order_id")
    print(f"  customers: {len(c_val.accepted)} -> {len(c_final)}  ({c_sup} older versions superseded)")
    print(f"  orders   : {len(o_val.accepted)} -> {len(o_final)}  ({o_sup} older versions superseded, "
          f"{o_ties} ties broken by file position)")
    acc = o_val.accepted
    multi_keys = acc.groupby("order_id")["status"].nunique()
    dup_key = multi_keys[multi_keys > 1].index[0]          # a key whose versions differ in status
    versions = o_val.accepted[o_val.accepted["order_id"] == dup_key].copy()
    versions["kept?"] = versions["_source_line"].isin(o_final["_source_line"]).map({True: "KEPT", False: "dropped"})
    print(f"\n  Example - order {dup_key} arrived twice with different content:")
    show(versions, ["_source_line", "order_id", "status", "source_updated_at", "kept?"])
    note("Newest updated_at wins, not 'last line wins' - the file order is not trusted.")
    save(o_final, "03b_final_orders.csv")
    save(c_final, "03b_final_customers.csv")

    # ------------------------------------------------------------ 3c reconcile + gate
    step("3c", "RECONCILE & QUALITY GATE",
         "Prove no record vanished, and refuse to load if too much data is bad.")
    rows = []
    for name, ing, n_exact, val, sup, final in (
            ("customers", cust_in, cust_exact, c_val, c_sup, c_final),
            ("orders", ord_in, ord_exact, o_val, o_sup, o_final)):
        rejected_total = len(ing.rejects) + len(val.rejected)
        rate = rejected_total / (ing.lines_read - n_exact)
        rows.append((name, ing.lines_read, len(ing.rejects), n_exact, len(val.rejected), sup, len(final), rate))
    for name, read, unread, exact, rej, sup, final, rate in rows:
        total = unread + exact + rej + sup + final
        print(f"  {name:<9} read {read} = unreadable {unread} + exact dup {exact} + rejected {rej} "
              f"+ superseded {sup} + loaded {final} = {total}  {'OK' if total == read else 'MISMATCH'}")
    print()
    for name, *_, rate in rows:
        limit = cfg.max_reject_rate[name]
        print(f"  {name:<9} reject rate {rate:6.1%}  limit {limit:5.0%}  -> {'PASS' if rate <= limit else 'STOP - nothing loaded'}")
    note("If the gate fails, the real pipeline exits with code 3 and writes NOTHING.")

    # ------------------------------------------------------------ 4 load
    step("4", "LOAD into SQLite - safely and repeatably",
         "Save the final records so that re-running never duplicates, never goes backwards, "
         "and a crash never leaves half the data.")
    conn = store.connect(cfg.paths.database)
    store.init_schema(conn)
    for attempt in ("first load", "same data again"):
        conn.execute("BEGIN IMMEDIATE")
        c_stats = store.upsert(conn, "customers", customer_records(c_final), "walkthrough")
        o_stats = store.upsert(conn, "orders", order_records(o_final), "walkthrough")
        checks = store.run_post_load_checks(conn)
        conn.execute("COMMIT")
        print(f"  {attempt:<16} customers {c_stats.as_dict()}")
        print(f"  {'':<16} orders    {o_stats.as_dict()}")
    print(f"\n  post-load integrity checks (all must be 0): {checks}")
    print(f"  rows in DB: {store.table_counts(conn)}")
    print("\n  First rows of the orders table as stored (money in integer cents):")
    print(pd.read_sql_query("SELECT order_id, customer_id, order_date, quantity, unit_price_cents, total_cents, "
                            "currency, status, dq_flags FROM orders LIMIT 4", conn).to_string(index=False))
    conn.close()
    note("Second load: inserted=0, updated=0 -> every row's hash matched, nothing was rewritten (idempotent).")

    # ------------------------------------------------------------ 5 real orchestrator
    step("5", "THE REAL PIPELINE does steps 1-4 in one call (python -m pipeline)",
         "Same functions, plus run record, quarantine, one transaction and the report.")
    for p in OUT.glob("walkthrough.db*"):
        p.unlink()
    stats = run_pipeline(cfg)
    ok = True
    for name, final in (("customers", c_final), ("orders", o_final)):
        e = stats["entities"][name]
        same = e["unique_valid"] == len(final)
        ok &= same
        print(f"  {name:<9} pipeline loaded {e['unique_valid']:<5} walkthrough loaded {len(final):<5} "
              f"{'MATCH' if same else 'DIFFERENT'}   load={e['load']}")
    print(f"\n  status={stats['status']}  run_id={stats['run_id']}")
    print("  report  -> walkthrough_output/reports/data_quality_report.md")
    print("  database-> walkthrough_output/walkthrough.db (tables: customers, orders, quarantine, pipeline_runs)")
    print("\n" + ("All steps match the real pipeline." if ok else "WARNING: walkthrough and pipeline disagree."))


if __name__ == "__main__":
    main()
