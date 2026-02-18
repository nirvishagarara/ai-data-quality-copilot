"""
schema_monitor.py
=================
Detects schema drift across your DuckDB tables.

What it does:
- Takes a snapshot of every table's columns + types
- Compares today's snapshot to the saved baseline
- Reports: added columns, dropped columns, type changes
- Saves results to data/snapshots/schema_history.csv

Usage:
    python src/monitoring/schema_monitor.py
"""

import os
import json
from datetime import datetime
from dataclasses import dataclass, field

import duckdb
import pandas as pd

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH       = "data/warehouse.duckdb"
BASELINE_PATH = "data/snapshots/schema_baseline.csv"
HISTORY_PATH  = "data/snapshots/schema_history.csv"

TABLES_TO_MONITOR = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "events",
]

# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class SchemaDrift:
    table:       str
    drift_type:  str    # "column_added" | "column_dropped" | "type_changed"
    column_name: str
    old_value:   str    # old type, or "" if column is new
    new_value:   str    # new type, or "" if column was dropped
    severity:    str    # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    detected_at: str    = field(default_factory=lambda: datetime.utcnow().isoformat())

    def summary(self) -> str:
        if self.drift_type == "column_added":
            return f"[{self.severity}] '{self.table}' — new column added: '{self.column_name}' ({self.new_value})"
        elif self.drift_type == "column_dropped":
            return f"[{self.severity}] '{self.table}' — column DROPPED: '{self.column_name}' (was {self.old_value})"
        elif self.drift_type == "type_changed":
            return f"[{self.severity}] '{self.table}' — '{self.column_name}' type changed: {self.old_value} → {self.new_value}"
        return f"[{self.severity}] '{self.table}' — unknown drift on '{self.column_name}'"


# ─── Core functions ───────────────────────────────────────────────────────────

def get_current_schema(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    """
    Returns {column_name: dtype} for the given table.
    Example: {"order_id": "VARCHAR", "amount": "DOUBLE", ...}
    """
    df = con.execute(f"DESCRIBE {table}").df()
    return dict(zip(df["column_name"], df["column_type"]))


def load_baseline_schema(table: str) -> dict[str, str]:
    """
    Loads the saved baseline schema for a table from the CSV snapshot.
    Returns {} if no baseline exists yet.
    """
    if not os.path.exists(BASELINE_PATH):
        print(f"  ⚠  No baseline found at {BASELINE_PATH}")
        print("     Run generate_data.py first to create it.")
        return {}

    df = pd.read_csv(BASELINE_PATH)
    table_df = df[df["table"] == table]

    if table_df.empty:
        return {}

    return dict(zip(table_df["column_name"], table_df["dtype"]))


def compare_schemas(
    table: str,
    baseline: dict[str, str],
    current: dict[str, str],
) -> list[SchemaDrift]:
    """
    Compares baseline vs current schema and returns a list of drift events.

    Rules:
    - Dropped column → CRITICAL  (downstream queries will break)
    - Type changed   → HIGH      (silent data corruption risk)
    - Added column   → LOW       (usually safe but worth knowing)
    """
    drifts = []

    baseline_cols = set(baseline.keys())
    current_cols  = set(current.keys())

    # 1. Dropped columns (most dangerous)
    for col in baseline_cols - current_cols:
        drifts.append(SchemaDrift(
            table       = table,
            drift_type  = "column_dropped",
            column_name = col,
            old_value   = baseline[col],
            new_value   = "",
            severity    = "CRITICAL",
        ))

    # 2. Added columns
    for col in current_cols - baseline_cols:
        drifts.append(SchemaDrift(
            table       = table,
            drift_type  = "column_added",
            column_name = col,
            old_value   = "",
            new_value   = current[col],
            severity    = "LOW",
        ))

    # 3. Type changes on existing columns
    for col in baseline_cols & current_cols:
        if baseline[col] != current[col]:
            drifts.append(SchemaDrift(
                table       = table,
                drift_type  = "type_changed",
                column_name = col,
                old_value   = baseline[col],
                new_value   = current[col],
                severity    = "HIGH",
            ))

    return drifts


def save_to_history(all_drifts: list[SchemaDrift]):
    """Appends drift events to the schema history CSV."""
    if not all_drifts:
        return

    rows = [
        {
            "detected_at": d.detected_at,
            "table":       d.table,
            "drift_type":  d.drift_type,
            "column_name": d.column_name,
            "old_value":   d.old_value,
            "new_value":   d.new_value,
            "severity":    d.severity,
        }
        for d in all_drifts
    ]
    new_df = pd.DataFrame(rows)

    if os.path.exists(HISTORY_PATH):
        existing = pd.read_csv(HISTORY_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(HISTORY_PATH, index=False)
    print(f"\n  💾 Drift events saved → {HISTORY_PATH}")


def update_baseline(con: duckdb.DuckDBPyConnection):
    """
    Overwrites the baseline with today's schema.
    Call this AFTER a drift has been acknowledged and fixed.
    """
    rows = []
    for table in TABLES_TO_MONITOR:
        schema = get_current_schema(con, table)
        for col, dtype in schema.items():
            rows.append({
                "table":          table,
                "column_name":    col,
                "dtype":          dtype,
                "snapshotted_at": datetime.utcnow().isoformat(),
            })
    df = pd.DataFrame(rows)
    df.to_csv(BASELINE_PATH, index=False)
    print(f"  ✓ Baseline updated → {BASELINE_PATH}")


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_schema_monitor(verbose: bool = True) -> list[SchemaDrift]:
    """
    Runs the full schema monitor across all tables.
    Returns all detected drift events.
    """
    print("\n🔍  Schema Monitor")
    print("=" * 50)

    con = duckdb.connect(DB_PATH)
    all_drifts = []

    for table in TABLES_TO_MONITOR:
        baseline = load_baseline_schema(table)
        current  = get_current_schema(con, table)

        if not baseline:
            print(f"  ⚠  {table:<20} no baseline — skipping")
            continue

        drifts = compare_schemas(table, baseline, current)

        if drifts:
            print(f"\n  ❌ {table} — {len(drifts)} drift(s) detected:")
            for d in drifts:
                print(f"     {d.summary()}")
            all_drifts.extend(drifts)
        else:
            if verbose:
                cols = list(current.keys())
                print(f"  ✅ {table:<20} {len(cols)} columns — no drift")

    con.close()

    # Summary
    print("\n" + "─" * 50)
    if all_drifts:
        critical = [d for d in all_drifts if d.severity == "CRITICAL"]
        high     = [d for d in all_drifts if d.severity == "HIGH"]
        print(f"  ⚠️  Total drift events : {len(all_drifts)}")
        print(f"     CRITICAL           : {len(critical)}")
        print(f"     HIGH               : {len(high)}")
        save_to_history(all_drifts)
    else:
        print("  ✅ All schemas clean — no drift detected")

    return all_drifts


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Step 1: run clean — should show no drift
    print("\n--- Running on CLEAN data ---")
    drifts = run_schema_monitor()

    # Step 2: show what the output looks like as a dict (useful for LLM later)
    if drifts:
        print("\n📦  Drift data (will be passed to LLM later):")
        for d in drifts:
            print(f"   {d.__dict__}")
