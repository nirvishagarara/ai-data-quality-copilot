"""
Unit tests for schema_monitor.compare_schemas()

compare_schemas() is a pure function: (table, baseline_dict, current_dict) → list[SchemaDrift].
No database or disk I/O required.
"""

import pytest
from src.monitoring.schema_monitor import compare_schemas, SchemaDrift


def test_no_drift_returns_empty_list():
    baseline = {"order_id": "VARCHAR", "amount": "DOUBLE", "status": "VARCHAR"}
    current  = {"order_id": "VARCHAR", "amount": "DOUBLE", "status": "VARCHAR"}
    assert compare_schemas("orders", baseline, current) == []


def test_column_added_detected():
    baseline = {"order_id": "VARCHAR"}
    current  = {"order_id": "VARCHAR", "promo_code": "VARCHAR"}

    result = compare_schemas("orders", baseline, current)

    assert len(result) == 1
    assert result[0].drift_type  == "column_added"
    assert result[0].column_name == "promo_code"
    assert result[0].new_value   == "VARCHAR"
    assert result[0].old_value   == ""
    assert result[0].severity    == "LOW"


def test_column_dropped_detected():
    baseline = {"order_id": "VARCHAR", "amount": "DOUBLE"}
    current  = {"order_id": "VARCHAR"}

    result = compare_schemas("payments", baseline, current)

    assert len(result) == 1
    assert result[0].drift_type  == "column_dropped"
    assert result[0].column_name == "amount"
    assert result[0].old_value   == "DOUBLE"
    assert result[0].new_value   == ""
    assert result[0].severity    == "CRITICAL"


def test_type_changed_detected():
    baseline = {"amount": "INTEGER"}
    current  = {"amount": "DOUBLE"}

    result = compare_schemas("orders", baseline, current)

    assert len(result) == 1
    assert result[0].drift_type  == "type_changed"
    assert result[0].column_name == "amount"
    assert result[0].old_value   == "INTEGER"
    assert result[0].new_value   == "DOUBLE"
    assert result[0].severity    == "HIGH"


def test_multiple_changes_all_detected():
    baseline = {"id": "VARCHAR", "old_col": "INTEGER", "shared": "VARCHAR"}
    current  = {"id": "VARCHAR", "new_col": "DOUBLE",  "shared": "INTEGER"}

    result = compare_schemas("customers", baseline, current)

    drift_types = {d.drift_type for d in result}
    assert "column_added"   in drift_types  # new_col added
    assert "column_dropped" in drift_types  # old_col dropped
    assert "type_changed"   in drift_types  # shared changed type
    assert len(result) == 3


def test_schema_drift_carries_table_name():
    baseline = {"order_status": "VARCHAR"}
    current  = {"status": "VARCHAR"}   # rename = drop + add

    result = compare_schemas("orders", baseline, current)

    assert all(d.table == "orders" for d in result)


def test_empty_schemas_produce_no_drift():
    assert compare_schemas("empty_table", {}, {}) == []


def test_result_items_are_schema_drift_instances():
    baseline = {"col_a": "VARCHAR"}
    current  = {}
    result = compare_schemas("t", baseline, current)
    assert isinstance(result[0], SchemaDrift)
