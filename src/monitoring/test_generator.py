"""
test_generator.py
=================
Analyses 30-day historical data profiles and automatically generates
data quality tests for each table.

Generated test types:
  - not_null       : columns that have never had nulls
  - unique         : columns where every value is distinct (IDs)
  - accepted_values: low-cardinality columns (status, category etc)
  - row_count_between: expected row count range based on history
  - value_between  : expected numeric range based on history

Output:
  - Console report
  - data/generated_tests.yaml  (dbt-compatible format)
  - data/generated_tests.json  (for the API/frontend later)

Usage:
    python src/monitoring/test_generator.py
"""

import os
import json
from datetime import datetime
from dataclasses import dataclass, field

import duckdb
import pandas as pd
import numpy as np

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH       = "data/warehouse.duckdb"
SNAPSHOTS_DIR = "data/snapshots"
YAML_OUTPUT   = "data/generated_tests.yaml"
JSON_OUTPUT   = "data/generated_tests.json"

TABLES_TO_PROFILE = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "events",
]

# Thresholds for test generation decisions
NULL_TOLERANCE         = 0.01   # columns with <1% nulls get a not_null test
UNIQUE_TOLERANCE       = 0.99   # columns with >99% unique values get a unique test
MAX_CARDINALITY        = 20     # columns with <=20 distinct values get accepted_values
ROW_COUNT_BUFFER       = 0.20   # row_count_between uses ±20% of historical mean
VALUE_BUFFER           = 0.25   # value_between uses ±25% of historical min/max


# ─── Data class ───────────────────────────────────────────────────────────────

@dataclass
class DataTest:
    table:       str
    column:      str
    test_type:   str
    parameters:  dict
    reason:      str           # why this test was generated
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_plain_english(self) -> str:
        if self.test_type == "not_null":
            return f"{self.table}.{self.column} must never be null"
        elif self.test_type == "unique":
            return f"{self.table}.{self.column} must have no duplicate values"
        elif self.test_type == "accepted_values":
            vals = self.parameters.get("values", [])
            return f"{self.table}.{self.column} must only contain: {vals}"
        elif self.test_type == "row_count_between":
            return (f"{self.table} row count must be between "
                    f"{self.parameters['min_rows']:,} and {self.parameters['max_rows']:,}")
        elif self.test_type == "value_between":
            return (f"{self.table}.{self.column} values must be between "
                    f"{self.parameters['min_value']} and {self.parameters['max_value']}")
        return f"{self.table}.{self.column} — {self.test_type}"


# ─── Profiler ─────────────────────────────────────────────────────────────────

def profile_table(con: duckdb.DuckDBPyConnection, table: str) -> dict:
    """
    Builds a statistical profile of the table from the live database.
    Returns a dict with per-column stats needed for test generation.
    """
    profile = {"table": table, "columns": {}}

    total_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    profile["row_count"] = total_rows

    schema = con.execute(f"DESCRIBE {table}").df()
    numeric_types = {"INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL",
                     "HUGEINT", "SMALLINT", "TINYINT", "UBIGINT"}

    for _, row in schema.iterrows():
        col   = row["column_name"]
        dtype = row["column_type"]
        col_profile = {"dtype": dtype}

        # Null stats
        null_count = con.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL'
        ).fetchone()[0]
        col_profile["null_count"] = null_count
        col_profile["null_pct"]   = null_count / total_rows if total_rows > 0 else 0.0

        # Cardinality
        distinct = con.execute(
            f'SELECT COUNT(DISTINCT "{col}") FROM {table}'
        ).fetchone()[0]
        col_profile["distinct_count"] = distinct
        col_profile["distinct_ratio"] = distinct / total_rows if total_rows > 0 else 0.0

        # Accepted values for low-cardinality columns
        if distinct <= MAX_CARDINALITY:
            values = con.execute(
                f'SELECT DISTINCT "{col}" FROM {table} WHERE "{col}" IS NOT NULL ORDER BY "{col}"'
            ).df()[col].tolist()
            col_profile["distinct_values"] = [str(v) for v in values]

        # Numeric range
        if any(dtype.startswith(t) for t in numeric_types):
            result = con.execute(
                f'SELECT MIN("{col}"), MAX("{col}"), AVG("{col}") FROM {table}'
            ).fetchone()
            col_profile["min_value"]  = result[0]
            col_profile["max_value"]  = result[1]
            col_profile["mean_value"] = result[2]

        profile["columns"][col] = col_profile

    return profile


def load_row_count_history(table: str):
    """Loads historical row counts from the daily snapshot CSV."""
    path = os.path.join(SNAPSHOTS_DIR, f"{table}_daily.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    return df["row_count"].dropna().values.astype(float) if "row_count" in df.columns else None


# ─── Test generators ──────────────────────────────────────────────────────────

def generate_not_null_tests(table: str, profile: dict):
    tests = []
    for col, stats in profile["columns"].items():
        if stats["null_pct"] <= NULL_TOLERANCE:
            tests.append(DataTest(
                table      = table,
                column     = col,
                test_type  = "not_null",
                parameters = {},
                reason     = f"null_pct was {stats['null_pct']*100:.2f}% (under {NULL_TOLERANCE*100:.0f}% threshold)",
            ))
    return tests


def generate_unique_tests(table: str, profile: dict):
    tests = []
    for col, stats in profile["columns"].items():
        if stats["distinct_ratio"] >= UNIQUE_TOLERANCE and stats["null_pct"] == 0:
            tests.append(DataTest(
                table      = table,
                column     = col,
                test_type  = "unique",
                parameters = {},
                reason     = f"distinct_ratio was {stats['distinct_ratio']*100:.1f}% (over {UNIQUE_TOLERANCE*100:.0f}% threshold)",
            ))
    return tests


def generate_accepted_values_tests(table: str, profile: dict):
    tests = []
    for col, stats in profile["columns"].items():
        # Only for low-cardinality, non-unique columns (i.e. categoricals)
        if (stats["distinct_count"] <= MAX_CARDINALITY
                and stats["distinct_ratio"] < 0.50
                and "distinct_values" in stats
                and stats["distinct_count"] > 1):
            tests.append(DataTest(
                table      = table,
                column     = col,
                test_type  = "accepted_values",
                parameters = {"values": stats["distinct_values"]},
                reason     = f"only {stats['distinct_count']} distinct values found — treating as categorical",
            ))
    return tests


def generate_row_count_tests(table: str, profile: dict):
    history = load_row_count_history(table)
    if history is None or len(history) < 7:
        return []

    mean_count = float(np.mean(history))
    min_rows   = int(mean_count * (1 - ROW_COUNT_BUFFER))
    max_rows   = int(mean_count * (1 + ROW_COUNT_BUFFER))

    return [DataTest(
        table      = table,
        column     = "__row_count__",
        test_type  = "row_count_between",
        parameters = {"min_rows": min_rows, "max_rows": max_rows},
        reason     = f"historical mean row count is {mean_count:,.0f} (±{ROW_COUNT_BUFFER*100:.0f}% buffer)",
    )]


def generate_value_between_tests(table: str, profile: dict):
    tests = []
    for col, stats in profile["columns"].items():
        if "min_value" not in stats:
            continue
        if stats["min_value"] is None or stats["max_value"] is None:
            continue

        min_val = stats["min_value"]
        max_val = stats["max_value"]
        spread  = max_val - min_val

        if spread == 0:
            continue

        buffered_min = round(min_val - spread * VALUE_BUFFER, 4)
        buffered_max = round(max_val + spread * VALUE_BUFFER, 4)

        tests.append(DataTest(
            table      = table,
            column     = col,
            test_type  = "value_between",
            parameters = {"min_value": buffered_min, "max_value": buffered_max},
            reason     = f"historical range was [{min_val}, {max_val}] (±{VALUE_BUFFER*100:.0f}% buffer added)",
        ))
    return tests


# ─── Output writers ───────────────────────────────────────────────────────────

def write_yaml(all_tests: list):
    """
    Writes tests in dbt-compatible YAML format.
    You could drop this directly into a dbt project's schema.yml.
    """
    lines = [
        "# Auto-generated data quality tests",
        f"# Generated at: {datetime.utcnow().isoformat()}",
        "# Tool: Data Quality Copilot",
        "",
        "version: 2",
        "models:",
    ]

    # Group by table
    by_table = {}
    for t in all_tests:
        by_table.setdefault(t.table, []).append(t)

    for table, tests in by_table.items():
        lines.append(f"  - name: {table}")
        lines.append(f"    columns:")

        # Group by column
        by_col = {}
        for t in tests:
            by_col.setdefault(t.column, []).append(t)

        for col, col_tests in by_col.items():
            if col == "__row_count__":
                continue  # handled separately below
            lines.append(f"      - name: {col}")
            lines.append(f"        tests:")
            for t in col_tests:
                if t.test_type == "not_null":
                    lines.append(f"          - not_null")
                elif t.test_type == "unique":
                    lines.append(f"          - unique")
                elif t.test_type == "accepted_values":
                    vals = t.parameters["values"]
                    lines.append(f"          - accepted_values:")
                    lines.append(f"              values: {vals}")
                elif t.test_type == "value_between":
                    lines.append(f"          - dbt_utils.expression_is_true:")
                    lines.append(f"              expression: \">= {t.parameters['min_value']} and {col} <= {t.parameters['max_value']}\"")

        # Row count tests at table level
        row_tests = [t for t in tests if t.test_type == "row_count_between"]
        if row_tests:
            t = row_tests[0]
            lines.append(f"    tests:")
            lines.append(f"      - dbt_utils.expression_is_true:")
            lines.append(f"          expression: \"(select count(*) from {{{{ this }}}}) between {t.parameters['min_rows']} and {t.parameters['max_rows']}\"")

        lines.append("")

    with open(YAML_OUTPUT, "w") as f:
        f.write("\n".join(lines))


def write_json(all_tests: list):
    data = [
        {
            "table":        t.table,
            "column":       t.column,
            "test_type":    t.test_type,
            "parameters":   t.parameters,
            "reason":       t.reason,
            "plain_english": t.to_plain_english(),
            "generated_at": t.generated_at,
        }
        for t in all_tests
    ]
    with open(JSON_OUTPUT, "w") as f:
        json.dump(data, f, indent=2)


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_test_generator() -> list:
    print("\n🧪  Auto Test Generator")
    print("=" * 55)

    con = duckdb.connect(DB_PATH)
    all_tests = []

    for table in TABLES_TO_PROFILE:
        print(f"\n  📋 Profiling: {table}")

        profile = profile_table(con, table)

        tests = []
        tests += generate_not_null_tests(table, profile)
        tests += generate_unique_tests(table, profile)
        tests += generate_accepted_values_tests(table, profile)
        tests += generate_row_count_tests(table, profile)
        tests += generate_value_between_tests(table, profile)

        # Print summary per table
        by_type = {}
        for t in tests:
            by_type.setdefault(t.test_type, []).append(t)

        for test_type, type_tests in sorted(by_type.items()):
            print(f"     ✅ {test_type:<25} {len(type_tests)} test(s)")

        print(f"     → {len(tests)} total tests generated")
        all_tests.extend(tests)

    con.close()

    # Write outputs
    write_yaml(all_tests)
    write_json(all_tests)

    print(f"\n{'─'*55}")
    print(f"  Total tests generated : {len(all_tests)}")
    print(f"  YAML output           : {YAML_OUTPUT}")
    print(f"  JSON output           : {JSON_OUTPUT}")

    # Print all tests in plain English
    print(f"\n📝  All generated tests:\n")
    current_table = None
    for t in all_tests:
        if t.table != current_table:
            print(f"  [{t.table}]")
            current_table = t.table
        print(f"     • {t.to_plain_english()}")

    return all_tests


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_test_generator()
