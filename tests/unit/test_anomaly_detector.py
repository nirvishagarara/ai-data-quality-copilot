"""
Unit tests for anomaly_detector.assign_severity() and detect_anomalies_for_table()

Both functions are pure (no disk I/O or DB connections required):
  - assign_severity(z_score, metric) → str
  - detect_anomalies_for_table(table, current_metrics_dict, history_df) → list[Anomaly]
"""

import pytest
import pandas as pd
import numpy as np

from src.monitoring.anomaly_detector import assign_severity, detect_anomalies_for_table


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_history(hist_col: str, values: list) -> pd.DataFrame:
    """Build a history DataFrame with a date column and one metric column."""
    dates = pd.date_range("2024-01-01", periods=len(values))
    return pd.DataFrame({"date": dates, hist_col: values})


# ── assign_severity ───────────────────────────────────────────────────────────

class TestAssignSeverity:
    def test_row_count_low(self):
        assert assign_severity(2.9, "row_count") == "LOW"

    def test_row_count_medium(self):
        assert assign_severity(3.1, "row_count") == "MEDIUM"

    def test_row_count_high(self):
        assert assign_severity(4.1, "row_count") == "HIGH"

    def test_row_count_critical(self):
        assert assign_severity(6.1, "row_count") == "CRITICAL"

    def test_null_pct_low(self):
        assert assign_severity(2.9, "null_pct") == "LOW"

    def test_null_pct_medium(self):
        assert assign_severity(3.1, "null_pct") == "MEDIUM"

    def test_null_pct_high(self):
        assert assign_severity(4.1, "null_pct") == "HIGH"

    def test_null_pct_critical(self):
        assert assign_severity(5.1, "null_pct") == "CRITICAL"

    def test_other_metric_low(self):
        assert assign_severity(2.9, "mean") == "LOW"

    def test_other_metric_medium(self):
        assert assign_severity(3.5, "mean") == "MEDIUM"

    def test_other_metric_high(self):
        assert assign_severity(5.5, "mean") == "HIGH"

    def test_negative_z_score_uses_abs_value(self):
        # A large negative z-score (e.g. massive row drop) should still be CRITICAL
        assert assign_severity(-6.1, "row_count") == "CRITICAL"
        assert assign_severity(-3.1, "row_count") == "MEDIUM"


# ── detect_anomalies_for_table ────────────────────────────────────────────────

class TestDetectAnomaliesForTable:

    def test_normal_values_produce_no_anomalies(self):
        history = _make_history("row_count", [1000.0] * 30)
        current = {"__row_count__||row_count": 1001.0}
        result = detect_anomalies_for_table("orders", current, history)
        assert result == []

    def test_high_zscore_row_count_is_flagged(self):
        # History needs slight variation (std > 0) for the Z-score path to run.
        # Using values oscillating around 1000 gives mean≈1000, std≈7.9.
        # A spike to 9000 produces z_score >> 3 and fires the anomaly.
        varied = [990, 995, 1000, 1005, 1010] * 6   # 30 values, std ≈ 7.9
        history = _make_history("row_count", [float(v) for v in varied])
        current = {"__row_count__||row_count": 9000.0}
        result = detect_anomalies_for_table("orders", current, history)
        assert len(result) >= 1
        anomaly = result[0]
        assert anomaly.metric == "row_count"
        assert anomaly.table  == "orders"
        assert anomaly.z_score > 3.0

    def test_insufficient_history_skips_detection(self):
        # Only 5 days — below MIN_HISTORY_DAYS=7, so no anomaly should fire
        history = _make_history("row_count", [1000.0] * 5)
        current = {"__row_count__||row_count": 99999.0}
        result = detect_anomalies_for_table("orders", current, history)
        assert result == []

    def test_stable_null_pct_spike_triggers_absolute_threshold(self):
        # std=0 baseline (null_pct always 0.0) + current spike to 45% → CRITICAL
        history = _make_history("amount__null_pct", [0.0] * 30)
        current = {"amount||null_pct": 0.45}
        result = detect_anomalies_for_table("payments", current, history)
        assert len(result) == 1
        assert result[0].metric   == "null_pct"
        assert result[0].column   == "amount"
        assert result[0].severity == "CRITICAL"
        assert result[0].z_score  == 999.0   # absolute threshold marker

    def test_stable_null_pct_small_increase_does_not_fire(self):
        # A tiny increase (0.0 → 0.02) is below the 0.05 absolute threshold
        history = _make_history("amount__null_pct", [0.0] * 30)
        current = {"amount||null_pct": 0.02}
        result = detect_anomalies_for_table("payments", current, history)
        assert result == []

    def test_missing_hist_col_is_skipped_without_error(self):
        history = _make_history("some_other_metric", [1.0] * 30)
        current = {"amount||null_pct": 0.99}
        result = detect_anomalies_for_table("orders", current, history)
        assert result == []

    def test_anomaly_carries_correct_table_name(self):
        varied = [96, 98, 100, 102, 104] * 6   # mean≈100, std≈2.9
        history = _make_history("row_count", [float(v) for v in varied])
        current = {"__row_count__||row_count": 9999.0}
        result = detect_anomalies_for_table("payments", current, history)
        assert len(result) >= 1
        assert result[0].table == "payments"
