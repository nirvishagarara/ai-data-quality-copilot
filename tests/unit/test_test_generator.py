"""
Unit tests for test_generator.py

Tests two groups:
  1. profile_table(con, table) — needs in_memory_db fixture
  2. generate_*_tests(table, profile) — pure functions, no DB needed
     (generate_row_count_tests calls load_row_count_history which is monkeypatched)
"""

import numpy as np
import pytest

from src.monitoring.test_generator import (
    profile_table,
    generate_not_null_tests,
    generate_unique_tests,
    generate_accepted_values_tests,
    generate_row_count_tests,
    generate_value_between_tests,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _single_col_profile(**col_stats):
    """Build a minimal profile dict for a single column named 'col'."""
    return {
        "table":     "orders",
        "row_count": 100,
        "columns": {"col": col_stats},
    }


# ── profile_table ─────────────────────────────────────────────────────────────

def test_profile_returns_required_top_level_keys(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    assert "table"     in profile
    assert "row_count" in profile
    assert "columns"   in profile


def test_profile_table_name_matches(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    assert profile["table"] == "test_table"


def test_profile_row_count_is_correct(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    assert profile["row_count"] == 10


def test_profile_every_column_has_null_stats(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    for col_stats in profile["columns"].values():
        assert "null_count" in col_stats
        assert "null_pct"   in col_stats


def test_profile_every_column_has_distinct_stats(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    for col_stats in profile["columns"].values():
        assert "distinct_count" in col_stats
        assert "distinct_ratio" in col_stats


def test_profile_numeric_column_has_range_stats(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    amount = profile["columns"]["amount"]
    assert "min_value"  in amount
    assert "max_value"  in amount
    assert "mean_value" in amount
    assert amount["min_value"]  == 10.0
    assert amount["max_value"]  == 100.0


def test_profile_varchar_column_has_no_range_stats(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    id_col = profile["columns"]["id"]
    assert "min_value" not in id_col


def test_profile_low_cardinality_column_has_distinct_values(in_memory_db):
    profile = profile_table(in_memory_db, "test_table")
    status = profile["columns"]["status"]
    assert "distinct_values" in status
    assert set(status["distinct_values"]) == {"active", "inactive", "pending"}


# ── generate_not_null_tests ───────────────────────────────────────────────────

def test_not_null_test_generated_for_zero_null_column():
    profile = _single_col_profile(null_pct=0.0, distinct_count=5, distinct_ratio=0.05)
    tests = generate_not_null_tests("orders", profile)
    assert any(t.test_type == "not_null" for t in tests)


def test_not_null_test_generated_at_tolerance_boundary():
    # Exactly at the 1% tolerance threshold — should still get a not_null test
    profile = _single_col_profile(null_pct=0.01, distinct_count=5, distinct_ratio=0.05)
    tests = generate_not_null_tests("orders", profile)
    assert any(t.test_type == "not_null" for t in tests)


def test_not_null_test_skipped_for_high_null_column():
    profile = _single_col_profile(null_pct=0.5, distinct_count=5, distinct_ratio=0.05)
    tests = generate_not_null_tests("orders", profile)
    assert tests == []


# ── generate_unique_tests ─────────────────────────────────────────────────────

def test_unique_test_generated_for_fully_unique_null_free_column():
    profile = _single_col_profile(null_pct=0.0, distinct_ratio=1.0, distinct_count=100)
    tests = generate_unique_tests("orders", profile)
    assert any(t.test_type == "unique" for t in tests)


def test_unique_test_skipped_when_nulls_present():
    profile = _single_col_profile(null_pct=0.01, distinct_ratio=1.0, distinct_count=100)
    tests = generate_unique_tests("orders", profile)
    assert tests == []


def test_unique_test_skipped_for_low_cardinality_column():
    profile = _single_col_profile(null_pct=0.0, distinct_ratio=0.03, distinct_count=3)
    tests = generate_unique_tests("orders", profile)
    assert tests == []


# ── generate_accepted_values_tests ───────────────────────────────────────────

def test_accepted_values_generated_for_categorical_column():
    profile = _single_col_profile(
        null_pct=0.0,
        distinct_count=3,
        distinct_ratio=0.03,
        distinct_values=["active", "inactive", "pending"],
    )
    tests = generate_accepted_values_tests("orders", profile)
    assert len(tests) == 1
    assert tests[0].test_type == "accepted_values"
    assert set(tests[0].parameters["values"]) == {"active", "inactive", "pending"}


def test_no_accepted_values_for_high_cardinality_column():
    # 25 distinct values > MAX_CARDINALITY=20
    profile = _single_col_profile(null_pct=0.0, distinct_count=25, distinct_ratio=0.25)
    tests = generate_accepted_values_tests("orders", profile)
    assert tests == []


def test_no_accepted_values_when_distinct_values_missing():
    # distinct_count > MAX_CARDINALITY so distinct_values never gets populated
    profile = _single_col_profile(null_pct=0.0, distinct_count=50, distinct_ratio=0.5)
    tests = generate_accepted_values_tests("orders", profile)
    assert tests == []


# ── generate_row_count_tests ─────────────────────────────────────────────────

def test_row_count_test_generated_with_30_days_history(monkeypatch):
    history = np.array([1000.0] * 30)
    monkeypatch.setattr(
        "src.monitoring.test_generator.load_row_count_history",
        lambda table: history,
    )
    tests = generate_row_count_tests("orders", {})
    assert len(tests) == 1
    assert tests[0].test_type == "row_count_between"
    assert tests[0].parameters["min_rows"] == int(1000 * 0.80)
    assert tests[0].parameters["max_rows"] == int(1000 * 1.20)


def test_row_count_test_skipped_when_no_history(monkeypatch):
    monkeypatch.setattr(
        "src.monitoring.test_generator.load_row_count_history",
        lambda table: None,
    )
    tests = generate_row_count_tests("orders", {})
    assert tests == []


def test_row_count_test_skipped_with_insufficient_history(monkeypatch):
    # Only 5 days — needs at least 7
    monkeypatch.setattr(
        "src.monitoring.test_generator.load_row_count_history",
        lambda table: np.array([1000.0] * 5),
    )
    tests = generate_row_count_tests("orders", {})
    assert tests == []


# ── generate_value_between_tests ─────────────────────────────────────────────

def test_value_between_generated_for_numeric_column():
    profile = {
        "table": "orders", "row_count": 100,
        "columns": {
            "amount": {
                "null_pct": 0.0, "distinct_count": 100, "distinct_ratio": 1.0,
                "min_value": 0.0, "max_value": 100.0, "mean_value": 50.0,
            }
        },
    }
    tests = generate_value_between_tests("orders", profile)
    assert len(tests) == 1
    assert tests[0].test_type == "value_between"
    # Buffered min should be below 0.0 (−25% of range)
    assert tests[0].parameters["min_value"] < 0.0
    # Buffered max should be above 100.0 (+25% of range)
    assert tests[0].parameters["max_value"] > 100.0


def test_value_between_skipped_for_constant_column():
    profile = {
        "table": "orders", "row_count": 100,
        "columns": {
            "fixed": {
                "null_pct": 0.0, "distinct_count": 1, "distinct_ratio": 0.01,
                "min_value": 5.0, "max_value": 5.0, "mean_value": 5.0,
            }
        },
    }
    tests = generate_value_between_tests("orders", profile)
    assert tests == []


def test_value_between_skipped_for_varchar_column():
    profile = {
        "table": "orders", "row_count": 100,
        "columns": {
            "status": {"null_pct": 0.0, "distinct_count": 3, "distinct_ratio": 0.03}
            # no min_value/max_value — varchar column
        },
    }
    tests = generate_value_between_tests("orders", profile)
    assert tests == []
