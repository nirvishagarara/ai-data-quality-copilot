"""
generate_data.py
================
Synthetic e-commerce data generator for the Data Quality Copilot project.

Run this ONCE to set up your local DuckDB warehouse with 90 days of
realistic data across 5 related tables.

Requirements:
    pip install duckdb pandas numpy faker

Usage:
    python generate_data.py

Output:
    data/warehouse.duckdb   ← your local data warehouse
    data/snapshots/         ← daily metric snapshots (used by the monitor)
"""

import os
import random
import hashlib
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import duckdb
from faker import Faker

# ─── Config ───────────────────────────────────────────────────────────────────

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 3, 31)   # 90 days of history
N_CUSTOMERS = 5_000
N_PRODUCTS  = 500
N_ORDERS    = 30_000

DB_PATH = "data/warehouse.duckdb"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def random_dates(start: datetime, end: datetime, n: int) -> pd.Series:
    delta = (end - start).total_seconds()
    offsets = np.random.uniform(0, delta, n)
    return pd.to_datetime([start + timedelta(seconds=s) for s in offsets])


def print_section(title: str):
    print(f"\n{'─' * 55}")
    print(f"  {title}")
    print(f"{'─' * 55}")


# ─── Table generators ─────────────────────────────────────────────────────────

def make_customers() -> pd.DataFrame:
    print_section("Generating customers …")
    customer_ids = [f"CUST-{i:05d}" for i in range(1, N_CUSTOMERS + 1)]
    df = pd.DataFrame({
        "customer_id":   customer_ids,
        "email":         [fake.email() for _ in range(N_CUSTOMERS)],
        "first_name":    [fake.first_name() for _ in range(N_CUSTOMERS)],
        "last_name":     [fake.last_name() for _ in range(N_CUSTOMERS)],
        "country":       np.random.choice(
                             ["US", "CA", "GB", "AU", "DE"],
                             N_CUSTOMERS,
                             p=[0.60, 0.15, 0.12, 0.08, 0.05]
                         ),
        "signup_date":   random_dates(START_DATE, END_DATE, N_CUSTOMERS).date,
        "is_premium":    np.random.choice([True, False], N_CUSTOMERS, p=[0.20, 0.80]),
        "age_bucket":    np.random.choice(
                             ["18-24", "25-34", "35-44", "45-54", "55+"],
                             N_CUSTOMERS,
                             p=[0.15, 0.35, 0.25, 0.15, 0.10]
                         ),
    })
    print(f"  ✓ {len(df):,} customers")
    return df


def make_products() -> pd.DataFrame:
    print_section("Generating products …")
    categories = {
        "Electronics":  (120, 800),
        "Clothing":     (15,  150),
        "Books":        (8,   50),
        "Home & Garden":(20,  300),
        "Sports":       (25,  400),
        "Toys":         (10,  120),
    }
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        cat = random.choice(list(categories.keys()))
        lo, hi = categories[cat]
        price = round(random.uniform(lo, hi), 2)
        rows.append({
            "product_id":   f"PROD-{i:04d}",
            "product_name": fake.bs().title()[:60],
            "category":     cat,
            "sub_category": fake.word().capitalize(),
            "unit_price":   price,
            "cost_price":   round(price * random.uniform(0.40, 0.65), 2),
            "stock_qty":    random.randint(0, 2000),
            "is_active":    np.random.choice([True, False], p=[0.92, 0.08]),
            "created_at":   (START_DATE - timedelta(days=random.randint(30, 365))).date(),
        })
    df = pd.DataFrame(rows)
    print(f"  ✓ {len(df):,} products")
    return df


def make_orders(customers: pd.DataFrame, products: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (orders, order_items)."""
    print_section("Generating orders + order_items …")

    order_dates   = random_dates(START_DATE, END_DATE, N_ORDERS)
    customer_ids  = np.random.choice(customers["customer_id"].values, N_ORDERS)
    statuses      = np.random.choice(
        ["pending", "confirmed", "shipped", "delivered", "cancelled", "refunded"],
        N_ORDERS,
        p=[0.05, 0.10, 0.20, 0.55, 0.07, 0.03],
    )
    channels      = np.random.choice(
        ["web", "mobile_app", "email_campaign", "social"],
        N_ORDERS,
        p=[0.50, 0.30, 0.12, 0.08],
    )

    orders = pd.DataFrame({
        "order_id":        [f"ORD-{i:07d}" for i in range(1, N_ORDERS + 1)],
        "customer_id":     customer_ids,
        "order_status":    statuses,          # ← intentionally named order_status
        "channel":         channels,
        "created_at":      order_dates,
        "updated_at":      order_dates + pd.to_timedelta(
                               np.random.randint(0, 72, N_ORDERS), unit="h"
                           ),
        "shipping_country": np.random.choice(
                               ["US", "CA", "GB", "AU", "DE"], N_ORDERS,
                               p=[0.60, 0.15, 0.12, 0.08, 0.05]
                            ),
        "promo_code":      [
                               fake.bothify("SAVE##") if random.random() < 0.20 else None
                               for _ in range(N_ORDERS)
                           ],
    })

    # order_items: 1–5 items per order
    item_rows = []
    for _, order in orders.iterrows():
        n_items = np.random.choice([1, 2, 3, 4, 5], p=[0.45, 0.30, 0.14, 0.07, 0.04])
        chosen_products = products.sample(n_items)
        for _, prod in chosen_products.iterrows():
            qty = random.randint(1, 4)
            item_rows.append({
                "item_id":      hashlib.md5(
                                    f"{order['order_id']}{prod['product_id']}".encode()
                                ).hexdigest()[:12].upper(),
                "order_id":     order["order_id"],
                "product_id":   prod["product_id"],
                "quantity":     qty,
                "unit_price":   prod["unit_price"],
                "discount_pct": round(random.choice([0, 0, 0, 5, 10, 15, 20]), 0),
                "line_total":   round(prod["unit_price"] * qty * (1 - random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])), 2),
            })

    order_items = pd.DataFrame(item_rows)

    # Back-fill order totals
    totals = order_items.groupby("order_id")["line_total"].sum().reset_index()
    totals.columns = ["order_id", "order_total"]
    orders = orders.merge(totals, on="order_id", how="left")

    print(f"  ✓ {len(orders):,} orders")
    print(f"  ✓ {len(order_items):,} order items")
    return orders, order_items


def make_payments(orders: pd.DataFrame) -> pd.DataFrame:
    print_section("Generating payments …")

    delivered = orders[orders["order_status"].isin(["delivered", "shipped", "confirmed"])].copy()

    methods = np.random.choice(
        ["credit_card", "paypal", "debit_card", "apple_pay", "bank_transfer"],
        len(delivered),
        p=[0.45, 0.25, 0.15, 0.10, 0.05],
    )
    df = pd.DataFrame({
        "payment_id":     [f"PAY-{i:07d}" for i in range(1, len(delivered) + 1)],
        "order_id":       delivered["order_id"].values,
        "payment_method": methods,
        "amount":         delivered["order_total"].values,
        "currency":       "USD",
        "status":         np.random.choice(
                              ["success", "success", "success", "failed", "pending"],
                              len(delivered)
                          ),
        "processed_at":   pd.to_datetime(delivered["created_at"].values)
                          + pd.to_timedelta(
                              np.random.randint(1, 48, len(delivered)), unit="h"
                          ),
        "gateway_ref":    [fake.uuid4()[:16].upper() for _ in range(len(delivered))],
    })
    print(f"  ✓ {len(df):,} payments")
    return df


def make_events(orders: pd.DataFrame) -> pd.DataFrame:
    """Website clickstream events."""
    print_section("Generating clickstream events …")

    event_types = ["page_view", "add_to_cart", "checkout_start",
                   "checkout_complete", "search", "product_view"]
    rows = []
    for _ in range(60_000):
        order = orders.sample(1).iloc[0]
        rows.append({
            "event_id":    fake.uuid4(),
            "session_id":  fake.uuid4()[:16],
            "customer_id": order["customer_id"],
            "event_type":  random.choice(event_types),
            "page_url":    fake.uri(),
            "occurred_at": pd.Timestamp(order["created_at"])
                           - timedelta(hours=random.randint(0, 48)),
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "os":          random.choice(["Windows", "macOS", "iOS", "Android"]),
        })
    df = pd.DataFrame(rows)
    print(f"  ✓ {len(df):,} events")
    return df


# ─── DuckDB loader ────────────────────────────────────────────────────────────

def load_to_duckdb(tables: dict[str, pd.DataFrame], db_path: str):
    print_section("Loading into DuckDB …")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)
    for name, df in tables.items():
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM df")
        count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  ✓ {name:<20} {count:>8,} rows")
    con.close()


# ─── Daily metric snapshots ───────────────────────────────────────────────────

def save_daily_snapshots(tables: dict):
    print_section("Pre-computing daily metric snapshots ...")
    os.makedirs("data/snapshots", exist_ok=True)

    con = duckdb.connect(DB_PATH)

    for table_name, df in tables.items():
        schema = con.execute(f"DESCRIBE {table_name}").df()
        numeric_types = {"INTEGER","BIGINT","DOUBLE","FLOAT","DECIMAL"}

        rows = []
        for day_offset in range(90):
            snapshot = {"date": str((datetime(2024,1,1) + timedelta(days=day_offset)).date())}

            # Total row count — full table, not just that day
            snapshot["row_count"] = int(con.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0])

            # Per-column null % and mean/std
            for _, col_row in schema.iterrows():
                col   = col_row["column_name"]
                dtype = col_row["column_type"]

                null_pct = con.execute(
                    f'SELECT AVG(CASE WHEN "{col}" IS NULL THEN 1.0 ELSE 0.0 END) FROM {table_name}'
                ).fetchone()[0] or 0.0
                snapshot[f"{col}__null_pct"] = round(null_pct, 4)

                if any(dtype.startswith(t) for t in numeric_types):
                    result = con.execute(
                        f'SELECT AVG("{col}"), STDDEV("{col}") FROM {table_name}'
                    ).fetchone()
                    snapshot[f"{col}__mean"] = round(result[0] or 0.0, 4)
                    snapshot[f"{col}__std"]  = round(result[1] or 0.0, 4)

            rows.append(snapshot)

        snap_df = pd.DataFrame(rows)
        path = f"data/snapshots/{table_name}_daily.csv"
        snap_df.to_csv(path, index=False)
        print(f"  ✓ {path} ({len(snap_df)} days)")

    con.close()

# ─── Schema snapshot ──────────────────────────────────────────────────────────

def save_schema_snapshot(tables: dict[str, pd.DataFrame]):
    """Save schema snapshot using DuckDB types, not Pandas types."""
    print_section("Saving schema snapshots ...")
    os.makedirs("data/snapshots", exist_ok=True)

    con = duckdb.connect(DB_PATH)
    rows = []
    for table_name in tables.keys():
        result = con.execute(f"DESCRIBE {table_name}").df()
        for _, row in result.iterrows():
            rows.append({
                "table":          table_name,
                "column_name":    row["column_name"],
                "dtype":          row["column_type"],  # ← DuckDB types now
                "snapshotted_at": datetime.utcnow().isoformat(),
            })
    con.close()

    schema_df = pd.DataFrame(rows)
    schema_df.to_csv("data/snapshots/schema_baseline.csv", index=False)
    print(f"  ✓ data/snapshots/schema_baseline.csv ({len(rows)} columns total)")


# ─── Print summary ────────────────────────────────────────────────────────────

def print_summary(tables: dict[str, pd.DataFrame]):
    print_section("Dataset Summary")
    total_rows = 0
    for name, df in tables.items():
        print(f"  {name:<20} {len(df):>8,} rows   {len(df.columns):>3} columns")
        total_rows += len(df)
    print(f"\n  {'TOTAL':<20} {total_rows:>8,} rows")
    print(f"\n  DB:       {DB_PATH}")
    print(f"  Snapshots: data/snapshots/")
    print()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n🚀  Data Quality Copilot — Synthetic Data Generator")
    print("=" * 55)

    customers  = make_customers()
    products   = make_products()
    orders, order_items = make_orders(customers, products)
    payments   = make_payments(orders)
    events     = make_events(orders)

    tables = {
        "customers":   customers,
        "products":    products,
        "orders":      orders,
        "order_items": order_items,
        "payments":    payments,
        "events":      events,
    }

    load_to_duckdb(tables, DB_PATH)
    save_daily_snapshots(tables)
    save_schema_snapshot(tables)
    print_summary(tables)

    print("✅  All done! Your warehouse is ready.")
    print("    Next step: run the monitoring engine against data/warehouse.duckdb\n")


if __name__ == "__main__":
    main()
