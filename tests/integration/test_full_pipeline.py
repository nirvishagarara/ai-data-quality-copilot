"""
Integration tests for the full monitoring pipeline.

These tests use the real warehouse.duckdb but:
  - Create a clean backup before each test and restore after
  - Redirect all output CSV/JSON paths to tmp_path (avoids polluting real files)
  - Mock the Anthropic Claude API to avoid real API calls and cost

Prerequisites: data/warehouse.duckdb and data/snapshots/*_daily.csv must exist.
Run `python data/generate_data.py` once to create them.
"""

import json
import os
import shutil
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from src.monitoring.schema_monitor  import run_schema_monitor
from src.monitoring.anomaly_detector import run_anomaly_detector
from src.llm.root_cause_analyzer    import run_root_cause_analyzer


CANNED_LLM_RESPONSE = json.dumps({
    "explanation": "45% of payment amounts are NULL, indicating a data ingestion failure.",
    "root_cause":  "Payment gateway API stopped sending the amount field.",
    "fixes":       ["Check payment gateway logs", "Re-run ETL for affected date range"],
    "severity":    "CRITICAL",
})


@pytest.fixture
def clean_warehouse(tmp_path):
    """
    Backs up warehouse.duckdb before the test and restores it after.
    Also redirects all output file paths to tmp_path so no real data files
    are modified during the test run.
    """
    db_path     = "data/warehouse.duckdb"
    backup_path = str(tmp_path / "warehouse_backup.duckdb")

    # Create a fresh backup for this test run
    shutil.copy2(db_path, backup_path)

    yield {"db": db_path, "backup": backup_path, "tmp": tmp_path}

    # Restore the original database
    shutil.copy2(backup_path, db_path)


@pytest.fixture
def redirect_outputs(tmp_path, monkeypatch):
    """Redirect all write-side paths to tmp_path to avoid polluting real files."""
    monkeypatch.setattr(
        "src.monitoring.anomaly_detector.HISTORY_PATH",
        str(tmp_path / "anomaly_history.csv"),
    )
    monkeypatch.setattr(
        "src.monitoring.schema_monitor.HISTORY_PATH",
        str(tmp_path / "schema_history.csv"),
    )
    monkeypatch.setattr(
        "src.llm.root_cause_analyzer.REPORTS_OUTPUT",
        str(tmp_path / "root_cause_reports.json"),
    )
    return tmp_path


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_null_spike_detected_and_explained(clean_warehouse, redirect_outputs):
    """
    Full pipeline smoke test:
      inject null_spike → anomaly detector fires → LLM (mocked) explains → report saved
    """
    tmp_path = redirect_outputs

    # 1. Inject null spike into payments.amount
    con = duckdb.connect(clean_warehouse["db"])
    con.execute("UPDATE payments SET amount = NULL WHERE random() < 0.45")
    con.close()

    # 2. Anomaly detector should catch the null_pct spike on payments.amount
    anomalies = run_anomaly_detector(verbose=False)
    payment_anomalies = [a for a in anomalies if a.table == "payments"]
    assert len(payment_anomalies) >= 1, "Expected null_pct anomaly on payments table"
    assert any(a.metric == "null_pct" for a in payment_anomalies)

    # 3. Schema monitor should see no drift (null spike ≠ schema change)
    drifts = run_schema_monitor(verbose=False)
    assert drifts == [], "Null spike should not cause schema drift"

    # 4. Root-cause analyzer with mocked Claude API
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=CANNED_LLM_RESPONSE)]

    with patch("src.llm.root_cause_analyzer.anthropic.Anthropic") as MockAnthropic:
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client
        mock_client.messages.create.return_value = mock_msg

        reports = run_root_cause_analyzer()

    # 5. At least one report should have been generated
    assert len(reports) >= 1, "Expected at least one root-cause report"

    # 6. Each report must have the required fields
    required_fields = {"explanation", "root_cause", "fixes", "severity"}
    reports_path = str(tmp_path / "root_cause_reports.json")
    assert os.path.exists(reports_path), "reports JSON file was not written"

    with open(reports_path) as f:
        saved = json.load(f)
    assert len(saved) >= 1
    for report in saved:
        assert required_fields.issubset(report.keys()), \
            f"Report missing fields: {required_fields - report.keys()}"


def test_schema_drift_detected_after_column_rename(clean_warehouse, redirect_outputs):
    """
    Inject the schema_drift scenario (rename order_status → status) and verify
    the schema monitor detects both the dropped column and the added column.
    """
    # 1. Inject schema drift: rename orders.order_status → orders.status
    con = duckdb.connect(clean_warehouse["db"])
    # SELECT all columns, renaming order_status → status
    con.execute("""
        CREATE TABLE orders_new AS SELECT
            order_id,
            customer_id,
            order_status AS status,
            channel,
            order_total,
            created_at,
            updated_at,
            shipping_country,
            promo_code
        FROM orders
    """)
    con.execute("DROP TABLE orders")
    con.execute("ALTER TABLE orders_new RENAME TO orders")
    con.close()

    # 2. Schema monitor should detect the rename as drop + add
    drifts = run_schema_monitor(verbose=False)

    orders_drifts = [d for d in drifts if d.table == "orders"]
    assert len(orders_drifts) >= 1, "Expected drift events on the orders table"

    drift_types = {d.drift_type for d in orders_drifts}
    assert "column_dropped" in drift_types or "column_added" in drift_types, \
        f"Expected column_dropped or column_added, got: {drift_types}"


def test_clean_warehouse_has_no_anomalies(clean_warehouse, redirect_outputs):
    """
    With the clean warehouse (no injected anomalies) the anomaly detector
    should return an empty list.
    """
    anomalies = run_anomaly_detector(verbose=False)
    assert anomalies == [], \
        f"Expected no anomalies on clean data, got {len(anomalies)}: {[a.summary() for a in anomalies]}"
