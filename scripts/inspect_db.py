"""Print a quick health summary of the warehouse database (no sqlite3 CLI needed).

    python scripts/inspect_db.py [path/to/warehouse.db]
"""

import sqlite3
import sys
from pathlib import Path

db = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data/processed/warehouse.db"
if not db.exists():
    sys.exit(f"{db} not found - run `python -m pipeline` first")

conn = sqlite3.connect(db)


def show(title, sql):
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    print(f"\n== {title}")
    print(" | ".join(cols))
    for r in rows:
        print(" | ".join("" if v is None else str(v) for v in r))
    if not rows:
        print("(no rows)")


show("Row counts", "SELECT 'customers' AS tbl, COUNT(*) AS n FROM customers UNION ALL "
                   "SELECT 'orders', COUNT(*) FROM orders UNION ALL "
                   "SELECT 'quarantine', COUNT(*) FROM quarantine UNION ALL "
                   "SELECT 'pipeline_runs', COUNT(*) FROM pipeline_runs")
show("Duplicate order ids (expect none)", "SELECT order_id, COUNT(*) AS n FROM orders GROUP BY 1 HAVING n > 1")
show("Orders without a customer (expect none)",
     "SELECT o.order_id FROM orders o LEFT JOIN customers c USING(customer_id) WHERE c.customer_id IS NULL")
show("Top quarantine reasons", "SELECT reasons, COUNT(*) AS n FROM quarantine GROUP BY 1 ORDER BY 2 DESC LIMIT 5")
show("Sample warning-flagged orders", "SELECT order_id, status, dq_flags FROM orders WHERE dq_flags IS NOT NULL LIMIT 3")
show("Revenue by currency", "SELECT currency, COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS revenue "
                            "FROM v_order_facts GROUP BY currency")
show("Run history", "SELECT run_id, status, finished_at FROM pipeline_runs ORDER BY started_at")
