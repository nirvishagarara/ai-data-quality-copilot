"""
anomaly_detector.py
===================
Detects statistical anomalies across your DuckDB tables.

Monitors per table, per day:
  - Row count        : did the table gain/lose rows unexpectedly?
  - Null percentage  : did nulls spike in any column?
  - Mean             : is a numeric column's average shifting?
  - Standard deviation: is a numeric column's spread changing?

Detection methods:
  - Z-score  : flags if today's value is >3 std deviations from 30-day rolling mean
  - Pct change: flags if row count changes >20% day-over-day

Usage:
    python src/monitoring/anomaly_detector.py

    # Or inject an anomaly first to see it caught:
    python tests/inject_anomaly.py --scenario null_spike
    python src/monitoring/anomaly_detector.py
    python tests/inject_anomaly.py --reset
"""

import os
from datetime import datetime, date
from dataclasses import dataclass, field

import duckdb
import pandas as pd
import numpy as np

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH        = "data/warehouse.duckdb"
SNAPSHOTS_DIR  = "data/snapshots"
HISTORY_PATH   = "data/snapshots/anomaly_history.csv"

TABLES_TO_MONITOR = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "events",
]

# How sensitive the detector is.
# Lower = more alerts. Higher = fewer alerts.
ZSCORE_THRESHOLD    = 3.0   # flag if metric is >3 std devs from rolling mean
PCT_CHANGE_THRESHOLD = 0.20  # flag if row count changes >20% day-over-day
MIN_HISTORY_DAYS    = 7     # need at least this many days of history to run Z-score


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class Anomaly:
    table:        str
    column:       str          # "__row_count__" for row-level anomalies
    metric:       str          # "row_count" | "null_pct" | "mean" | "std"
    current_value: float
    baseline_mean: float
    baseline_std:  float
    z_score:       float
    pct_change:    float
    severity:      str         # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    detected_at:   str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def summary(self) -> str:
        direction = "↑ spiked" if self.pct_change > 0 else "↓ dropped"
        return (
            f"[{self.severity}] {self.table}.{self.column} — "
            f"{self.metric} {direction} {abs(self.pct_change)*100:.1f}% "
            f"(value: {self.current_value:.4f}, z-score: {self.z_score:.2f})"
        )


# ─── Metric computation ───────────────────────────────────────────────────────

def compute_current_metrics(con: duckdb.DuckDBPyConnection, table: str) -> dict:
    """
    Computes today's metrics directly from the live DuckDB table.
    Returns a flat dict: { "row_count": N, "col__null_pct": X, "col__mean": Y, ... }
    """
    metrics = {}

    # Row count
    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    metrics["__row_count__||row_count"] = float(total)

    # Per-column metrics
    df_schema = con.execute(f"DESCRIBE {table}").df()

    for _, row in df_schema.iterrows():
        col   = row["column_name"]
        dtype = row["column_type"]

        # Null percentage — every column
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE \"{col}\" IS NULL"
        ).fetchone()[0]
        null_pct = null_count / total if total > 0 else 0.0
        metrics[f"{col}||null_pct"] = round(null_pct, 6)

        # Mean + std — numeric columns only
        numeric_types = {"INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL",
                         "HUGEINT", "SMALLINT", "TINYINT", "UBIGINT"}
        if any(dtype.startswith(t) for t in numeric_types):
            result = con.execute(
                f"SELECT AVG(\"{col}\"), STDDEV(\"{col}\") FROM {table}"
            ).fetchone()
            mean_val = result[0] if result[0] is not None else 0.0
            std_val  = result[1] if result[1] is not None else 0.0
            metrics[f"{col}||mean"] = round(mean_val, 6)
            metrics[f"{col}||std"]  = round(std_val, 6)

    return metrics


def load_historical_metrics(table: str):
    """
    Loads the pre-computed daily snapshots for this table.
    Returns a DataFrame with one row per day, or None if not found.
    """
    path = os.path.join(SNAPSHOTS_DIR, f"{table}_daily.csv")
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df


# ─── Z-score detection ────────────────────────────────────────────────────────

def assign_severity(z_score: float, metric: str) -> str:
    """
    Converts a Z-score into a human-readable severity level.
    Row count drops use stricter thresholds because they affect everything downstream.
    """
    abs_z = abs(z_score)

    if metric == "row_count":
        if abs_z > 6:   return "CRITICAL"
        if abs_z > 4:   return "HIGH"
        if abs_z > 3:   return "MEDIUM"
        return "LOW"
    elif metric == "null_pct":
        if abs_z > 5:   return "CRITICAL"
        if abs_z > 4:   return "HIGH"
        if abs_z > 3:   return "MEDIUM"
        return "LOW"
    else:
        if abs_z > 5:   return "HIGH"
        if abs_z > 3:   return "MEDIUM"
        return "LOW"


def detect_anomalies_for_table(
    table: str,
    current_metrics: dict,
    history_df: pd.DataFrame,
) -> list[Anomaly]:
    """
    Runs Z-score detection on each metric for a single table.
    Compares today's values against the rolling 30-day history.
    """
    anomalies = []

    for key, current_value in current_metrics.items():
        col, metric = key.split("||")

        # Map the metric key to the history column name
        if metric == "row_count":
            hist_col = "row_count"
        elif metric == "null_pct":
            hist_col = f"{col}__null_pct"
        elif metric == "mean":
            hist_col = f"{col}__mean"
        elif metric == "std":
            hist_col = f"{col}__std"
        else:
            continue

        if hist_col not in history_df.columns:
            continue

        # Get last 30 days of history for this metric
        history_values = (
            history_df[hist_col]
            .dropna()
            .tail(30)
            .values
            .astype(float)
        )

        if len(history_values) < MIN_HISTORY_DAYS:
            continue  # not enough history to be meaningful

        hist_mean = float(np.mean(history_values))
        hist_std  = float(np.std(history_values))

        # If std is 0, Z-score won't work — use absolute threshold instead
        if hist_std < 1e-10:
            # null_pct: fire if jumps above 5% from a zero baseline
            if metric == "null_pct" and hist_mean < 0.01 and current_value > 0.05:
                anomalies.append(Anomaly(
                    table         = table,
                    column        = col,
                    metric        = metric,
                    current_value = current_value,
                    baseline_mean = round(hist_mean, 6),
                    baseline_std  = round(hist_std, 6),
                    z_score       = 999.0,
                    pct_change    = round(current_value - hist_mean, 4),
                    severity      = "CRITICAL" if current_value > 0.20 else "HIGH",
                ))
            # mean: fire if it shifts more than 20% from baseline
            elif metric == "mean" and hist_mean > 0:
                pct_diff = abs(current_value - hist_mean) / hist_mean
                if pct_diff > 0.20:
                    anomalies.append(Anomaly(
                        table         = table,
                        column        = col,
                        metric        = metric,
                        current_value = current_value,
                        baseline_mean = round(hist_mean, 6),
                        baseline_std  = round(hist_std, 6),
                        z_score       = 999.0,
                        pct_change    = round(pct_diff, 4),
                        severity      = "CRITICAL" if pct_diff > 0.50 else "HIGH",
                    ))
            # row_count: fire if total drops more than 15%
            elif metric == "row_count" and hist_mean > 0:
                pct_diff = (current_value - hist_mean) / hist_mean
                if pct_diff < -0.15:
                    anomalies.append(Anomaly(
                        table         = table,
                        column        = col,
                        metric        = metric,
                        current_value = current_value,
                        baseline_mean = round(hist_mean, 6),
                        baseline_std  = round(hist_std, 6),
                        z_score       = 999.0,
                        pct_change    = round(pct_diff, 4),
                        severity      = "CRITICAL" if pct_diff < -0.30 else "HIGH",
                    ))
            continue

        z_score    = (current_value - hist_mean) / hist_std
        pct_change = (current_value - hist_mean) / hist_mean if hist_mean != 0 else 0.0

        # Flag if Z-score exceeds threshold
        if abs(z_score) >= ZSCORE_THRESHOLD:
            severity = assign_severity(z_score, metric)

            # Skip LOW severity null_pct unless it's a meaningful absolute change
            if metric == "null_pct" and severity == "LOW" and abs(pct_change) < 0.05:
                continue

            anomalies.append(Anomaly(
                table         = table,
                column        = col,
                metric        = metric,
                current_value = current_value,
                baseline_mean = round(hist_mean, 6),
                baseline_std  = round(hist_std, 6),
                z_score       = round(z_score, 3),
                pct_change    = round(pct_change, 4),
                severity      = severity,
            ))

    return anomalies


# ─── Save results ─────────────────────────────────────────────────────────────

def save_anomalies(anomalies: list[Anomaly]):
    """Appends detected anomalies to the history CSV."""
    if not anomalies:
        return

    rows = [
        {
            "detected_at":    a.detected_at,
            "table":          a.table,
            "column":         a.column,
            "metric":         a.metric,
            "current_value":  a.current_value,
            "baseline_mean":  a.baseline_mean,
            "baseline_std":   a.baseline_std,
            "z_score":        a.z_score,
            "pct_change":     a.pct_change,
            "severity":       a.severity,
        }
        for a in anomalies
    ]
    new_df = pd.DataFrame(rows)

    if os.path.exists(HISTORY_PATH):
        existing = pd.read_csv(HISTORY_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(HISTORY_PATH, index=False)
    print(f"\n  💾 Anomaly events saved → {HISTORY_PATH}")


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_anomaly_detector(verbose: bool = True) -> list[Anomaly]:
    """
    Runs the full anomaly detector across all tables.
    Returns all detected anomalies.
    """
    print("\n📊  Anomaly Detector")
    print("=" * 55)
    print(f"  Z-score threshold : >{ZSCORE_THRESHOLD}")
    print(f"  Min history days  : {MIN_HISTORY_DAYS}")
    print(f"  Tables monitored  : {len(TABLES_TO_MONITOR)}")
    print()

    con = duckdb.connect(DB_PATH)
    all_anomalies = []

    for table in TABLES_TO_MONITOR:

        # Load historical baseline
        history_df = load_historical_metrics(table)
        if history_df is None:
            print(f"  ⚠️  {table:<20} no snapshot file found — skipping")
            continue

        # Compute today's metrics from live data
        current_metrics = compute_current_metrics(con, table)

        # Run detection
        anomalies = detect_anomalies_for_table(table, current_metrics, history_df)

        if anomalies:
            print(f"  ❌ {table} — {len(anomalies)} anomaly(s) detected:")
            for a in anomalies:
                print(f"     {a.summary()}")
            all_anomalies.extend(anomalies)
        else:
            if verbose:
                n_metrics = len(current_metrics)
                print(f"  ✅ {table:<20} {n_metrics} metrics checked — all normal")

    con.close()

    # Summary
    print("\n" + "─" * 55)
    if all_anomalies:
        critical = [a for a in all_anomalies if a.severity == "CRITICAL"]
        high     = [a for a in all_anomalies if a.severity == "HIGH"]
        medium   = [a for a in all_anomalies if a.severity == "MEDIUM"]
        print(f"  ⚠️  Total anomalies : {len(all_anomalies)}")
        print(f"     CRITICAL       : {len(critical)}")
        print(f"     HIGH           : {len(high)}")
        print(f"     MEDIUM         : {len(medium)}")
        save_anomalies(all_anomalies)
    else:
        print("  ✅ All metrics normal — no anomalies detected")

    return all_anomalies


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n--- Step 1: Run on CLEAN data (expect no anomalies) ---")
    anomalies = run_anomaly_detector()

    print("\n\n--- How to test with injected anomalies ---")
    print("  python tests/inject_anomaly.py --scenario null_spike")
    print("  python src/monitoring/anomaly_detector.py")
    print("  python tests/inject_anomaly.py --reset")
    print()
    print("  Try each scenario to test a different detector:")
    print("  null_spike         → null_pct detector")
    print("  row_drop           → row_count detector")
    print("  distribution_shift → mean detector")
