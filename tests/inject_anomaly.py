"""
inject_anomaly.py
=================
Injects realistic data anomalies into your DuckDB warehouse so you can
demo the monitoring engine catching them.

Usage:
    python inject_anomaly.py --list                    # see all available scenarios
    python inject_anomaly.py --scenario schema_drift   # inject one scenario
    python inject_anomaly.py --scenario null_spike     # inject another
    python inject_anomaly.py --reset                   # restore original data

Each scenario simulates a real-world pipeline failure.
After injecting, run your monitoring engine to watch it catch the problem.
"""

import argparse
import shutil
import os

import duckdb
import pandas as pd
import numpy as np

DB_PATH   = "data/warehouse.duckdb"
DB_BACKUP = "data/warehouse_backup.duckdb"

# ─── Scenarios ────────────────────────────────────────────────────────────────

SCENARIOS = {}

def scenario(name: str, description: str, tables_affected: list[str]):
    """Decorator to register a scenario."""
    def decorator(fn):
        SCENARIOS[name] = {
            "fn":               fn,
            "description":      description,
            "tables_affected":  tables_affected,
        }
        return fn
    return decorator


@scenario(
    name="schema_drift",
    description='Renames orders.order_status → orders.status (classic upstream API change)',
    tables_affected=["orders"],
)
def inject_schema_drift(con: duckdb.DuckDBPyConnection):
    """
    The most common real-world failure: an upstream team renames a field.
    Your monitoring engine should catch:
      - Column 'order_status' disappeared
      - New unknown column 'status' appeared
    """
    print("  → Renaming column: order_status → status")
    # DuckDB doesn't have ALTER COLUMN RENAME directly, so we recreate
    con.execute("""
        CREATE OR REPLACE TABLE orders AS
        SELECT
            order_id,
            customer_id,
            order_status AS status,     -- renamed here
            channel,
            created_at,
            updated_at,
            shipping_country,
            promo_code,
            order_total
        FROM orders
    """)
    print("  ✓ Schema drift injected into: orders.order_status → orders.status")


@scenario(
    name="null_spike",
    description="Spikes null % in payments.amount from ~0% to ~45% (broken ETL job)",
    tables_affected=["payments"],
)
def inject_null_spike(con: duckdb.DuckDBPyConnection):
    """
    Simulates an ETL job that started failing to parse amounts — nulling out
    nearly half the payment amounts. Revenue dashboards would silently drop.
    """
    print("  → Nulling 45% of payments.amount …")
    con.execute("""
        CREATE OR REPLACE TABLE payments AS
        SELECT
            payment_id,
            order_id,
            payment_method,
            CASE
                WHEN random() < 0.45 THEN NULL   -- ← the bug
                ELSE amount
            END AS amount,
            currency,
            status,
            processed_at,
            gateway_ref
        FROM payments
    """)
    null_count = con.execute(
        "SELECT COUNT(*) FROM payments WHERE amount IS NULL"
    ).fetchone()[0]
    total = con.execute("SELECT COUNT(*) FROM payments").fetchone()[0]
    print(f"  ✓ Null spike injected: {null_count:,} / {total:,} rows nulled ({null_count/total:.0%})")


@scenario(
    name="row_drop",
    description="Drops 60% of today's orders (upstream source went down overnight)",
    tables_affected=["orders", "order_items"],
)
def inject_row_drop(con: duckdb.DuckDBPyConnection):
    """
    Simulates a source database going offline — today's orders are mostly missing.
    Row count anomaly detector should fire immediately.
    """
    from datetime import date
    today = date.today()
    print(f"  → Deleting 60% of orders with created_at = {today} …")

    # Add a synthetic "today" batch first so there's something to drop
    con.execute(f"""
        CREATE OR REPLACE TABLE orders AS
        SELECT * FROM orders
        WHERE NOT (
            CAST(created_at AS DATE) = CURRENT_DATE
            AND random() < 0.60
        )
    """)

    remaining = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    print(f"  ✓ Row drop injected: orders now has {remaining:,} rows")
    print("  ℹ  Note: 'today' rows are sparse — your monitor will flag the count drop")


@scenario(
    name="distribution_shift",
    description="Shifts order_total distribution — avg jumps from ~$120 to ~$340 (pricing bug)",
    tables_affected=["orders"],
)
def inject_distribution_shift(con: duckdb.DuckDBPyConnection):
    """
    Simulates a pricing engine bug that tripled order totals.
    Mean/stddev anomaly detection should fire on order_total.
    """
    print("  → Tripling order_total for 40% of recent orders …")
    con.execute("""
        CREATE OR REPLACE TABLE orders AS
        SELECT
            order_id,
            customer_id,
            order_status,
            channel,
            created_at,
            updated_at,
            shipping_country,
            promo_code,
            CASE
                WHEN random() < 0.40
                THEN ROUND(order_total * 3.2, 2)  -- ← pricing bug
                ELSE order_total
            END AS order_total
        FROM orders
    """)
    new_avg = con.execute("SELECT AVG(order_total) FROM orders").fetchone()[0]
    print(f"  ✓ Distribution shift injected: avg order_total now ${new_avg:,.2f}")


@scenario(
    name="new_bad_values",
    description="Injects unknown category values into products.category (lookup table out of sync)",
    tables_affected=["products"],
)
def inject_bad_values(con: duckdb.DuckDBPyConnection):
    """
    Simulates a lookup table update that wasn't propagated — products start
    arriving with category values the downstream system doesn't recognise.
    Cardinality / accepted_values monitoring should catch this.
    """
    bad_cats = ["Gadgets", "Wearables", "NFT Merch", "UNKNOWN_CAT"]
    print(f"  → Injecting unknown categories: {bad_cats}")
    con.execute(f"""
        CREATE OR REPLACE TABLE products AS
        SELECT
            product_id,
            product_name,
            CASE
                WHEN random() < 0.15
                THEN (ARRAY{bad_cats})[CAST(FLOOR(random() * {len(bad_cats)}) AS INTEGER) + 1]
                ELSE category
            END AS category,
            sub_category,
            unit_price,
            cost_price,
            stock_qty,
            is_active,
            created_at
        FROM products
    """)
    new_cats = con.execute(
        "SELECT DISTINCT category FROM products ORDER BY category"
    ).fetchdf()["category"].tolist()
    print(f"  ✓ Bad values injected. Unique categories now: {new_cats}")


@scenario(
    name="duplicate_rows",
    description="Duplicates 20% of order_items (double-insert bug in ETL)",
    tables_affected=["order_items"],
)
def inject_duplicates(con: duckdb.DuckDBPyConnection):
    """
    Simulates a double-insert ETL bug — some rows get written twice.
    Uniqueness tests and row count anomaly detection should both flag this.
    """
    print("  → Duplicating 20% of order_items …")
    con.execute("""
        CREATE OR REPLACE TABLE order_items AS
        SELECT * FROM order_items
        UNION ALL
        SELECT * FROM order_items WHERE random() < 0.20
    """)
    total = con.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
    dupes = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT item_id) FROM order_items"
    ).fetchone()[0]
    print(f"  ✓ Duplicates injected: {dupes:,} duplicate item_ids out of {total:,} rows")


# ─── Backup / Reset ───────────────────────────────────────────────────────────

def create_backup():
    if not os.path.exists(DB_BACKUP):
        print(f"  → Creating backup: {DB_BACKUP}")
        shutil.copy2(DB_PATH, DB_BACKUP)
        print("  ✓ Backup created")
    else:
        print("  ✓ Backup already exists (skip)")


def reset_db():
    if not os.path.exists(DB_BACKUP):
        print("  ✗ No backup found. Run generate_data.py first, then inject an anomaly.")
        return
    print(f"  → Restoring from backup …")
    shutil.copy2(DB_BACKUP, DB_PATH)
    print("  ✓ Database restored to clean state")


# ─── Print row counts ─────────────────────────────────────────────────────────

def print_table_stats(con: duckdb.DuckDBPyConnection, tables: list[str]):
    print("\n  Current row counts:")
    for t in tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"    {t:<20} {n:>8,} rows")
        except Exception:
            print(f"    {t:<20}  (not found)")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def list_scenarios():
    print("\n📋  Available anomaly scenarios:\n")
    for name, meta in SCENARIOS.items():
        tables = ", ".join(meta["tables_affected"])
        print(f"  --scenario {name}")
        print(f"      {meta['description']}")
        print(f"      Affects: {tables}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Inject anomalies into the Data Quality Copilot warehouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scenario", choices=list(SCENARIOS.keys()),
                        help="Anomaly scenario to inject")
    parser.add_argument("--list",   action="store_true", help="List all scenarios")
    parser.add_argument("--reset",  action="store_true", help="Restore clean data")
    args = parser.parse_args()

    if args.list:
        list_scenarios()
        return

    if not os.path.exists(DB_PATH):
        print(f"✗ Database not found at {DB_PATH}")
        print("  Run: python generate_data.py  first")
        return

    if args.reset:
        print("\n🔄  Resetting database …")
        reset_db()
        return

    if not args.scenario:
        parser.print_help()
        list_scenarios()
        return

    meta = SCENARIOS[args.scenario]
    print(f"\n💥  Injecting anomaly: {args.scenario}")
    print(f"    {meta['description']}\n")

    create_backup()

    con = duckdb.connect(DB_PATH)
    print_table_stats(con, meta["tables_affected"])
    print()

    meta["fn"](con)

    print()
    print_table_stats(con, meta["tables_affected"])
    con.close()

    print(f"""
✅  Anomaly injected!

Next steps:
  1. Run your monitoring engine against data/warehouse.duckdb
  2. It should detect and report the {args.scenario} anomaly
  3. When done:  python inject_anomaly.py --reset
""")


if __name__ == "__main__":
    main()
