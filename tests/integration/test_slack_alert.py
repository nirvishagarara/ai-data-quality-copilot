"""
Integration tests for slack_alerts.py

Tests three groups:
  1. format_report_alert()  — pure formatter, no I/O
  2. format_summary_alert() — pure formatter, no I/O
  3. send_slack_message()   — mocks requests.post
  4. run_slack_alerts()     — mocks requests.post + redirects REPORTS_PATH
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from src.alerts.slack_alerts import (
    format_report_alert,
    format_summary_alert,
    send_slack_message,
    run_slack_alerts,
)


# ── Sample data ───────────────────────────────────────────────────────────────

SAMPLE_REPORT = {
    "table":       "payments",
    "column":      "amount",
    "metric":      "null_pct",
    "explanation": "45% of payment amounts are NULL.",
    "root_cause":  "Payment gateway stopped sending the amount field.",
    "fixes":       ["Check gateway logs", "Re-run ETL"],
    "severity":    "CRITICAL",
    "generated_at": "2024-02-01T00:00:00",
}

SAMPLE_REPORTS = [
    SAMPLE_REPORT,
    {**SAMPLE_REPORT, "table": "orders", "column": "order_total",
     "severity": "HIGH", "metric": "mean"},
    {**SAMPLE_REPORT, "table": "events", "column": "event_type",
     "severity": "MEDIUM", "metric": "null_pct"},
]


# ── format_report_alert ───────────────────────────────────────────────────────

def test_format_report_alert_returns_attachments_key():
    payload = format_report_alert(SAMPLE_REPORT)
    assert "attachments" in payload


def test_format_report_alert_color_is_red_for_critical():
    payload = format_report_alert(SAMPLE_REPORT)
    color = payload["attachments"][0]["color"]
    assert color == "#FF0000"


def test_format_report_alert_color_varies_by_severity():
    high_payload   = format_report_alert({**SAMPLE_REPORT, "severity": "HIGH"})
    medium_payload = format_report_alert({**SAMPLE_REPORT, "severity": "MEDIUM"})
    critical_color = format_report_alert(SAMPLE_REPORT)["attachments"][0]["color"]

    assert high_payload["attachments"][0]["color"]   != critical_color
    assert medium_payload["attachments"][0]["color"] != critical_color


def test_format_report_alert_text_contains_table_and_column():
    payload = format_report_alert(SAMPLE_REPORT)
    text = payload["attachments"][0]["blocks"][0]["text"]["text"]
    assert "payments" in text
    assert "amount"   in text


def test_format_report_alert_text_contains_explanation():
    payload = format_report_alert(SAMPLE_REPORT)
    text = payload["attachments"][0]["blocks"][0]["text"]["text"]
    assert SAMPLE_REPORT["explanation"] in text


# ── format_summary_alert ──────────────────────────────────────────────────────

def test_format_summary_alert_returns_blocks_key():
    payload = format_summary_alert(SAMPLE_REPORTS)
    assert "blocks" in payload


def test_format_summary_alert_counts_severities_correctly():
    payload = format_summary_alert(SAMPLE_REPORTS)
    # Find the section with severity counts
    fields_block = next(
        b for b in payload["blocks"] if b.get("type") == "section" and "fields" in b
    )
    fields_text = " ".join(f["text"] for f in fields_block["fields"])
    assert "1" in fields_text   # 1 CRITICAL
    assert "1" in fields_text   # 1 HIGH
    assert "1" in fields_text   # 1 MEDIUM


def test_format_summary_alert_lists_all_issues():
    payload = format_summary_alert(SAMPLE_REPORTS)
    # The issues section contains table.column for each report
    issues_block = next(
        b for b in payload["blocks"]
        if b.get("type") == "section" and "Issues found" in b.get("text", {}).get("text", "")
    )
    text = issues_block["text"]["text"]
    assert "payments.amount"      in text
    assert "orders.order_total"   in text
    assert "events.event_type"    in text


# ── send_slack_message ────────────────────────────────────────────────────────

def test_send_slack_message_returns_true_on_success(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text        = "ok"

    with patch("src.alerts.slack_alerts.requests.post", return_value=mock_response):
        monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL", "https://hooks.slack.com/test")
        result = send_slack_message({"text": "test"})

    assert result is True


def test_send_slack_message_returns_false_on_http_error(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text        = "internal server error"

    with patch("src.alerts.slack_alerts.requests.post", return_value=mock_response):
        monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL", "https://hooks.slack.com/test")
        result = send_slack_message({"text": "test"})

    assert result is False


def test_send_slack_message_returns_false_when_no_webhook(monkeypatch):
    monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL", None)
    result = send_slack_message({"text": "test"})
    assert result is False


def test_send_slack_message_posts_to_correct_url(monkeypatch):
    mock_response = MagicMock(status_code=200, text="ok")
    webhook_url = "https://hooks.slack.com/services/TEST/URL"

    with patch("src.alerts.slack_alerts.requests.post", return_value=mock_response) as mock_post:
        monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL", webhook_url)
        send_slack_message({"text": "hello"})

    mock_post.assert_called_once()
    call_url = mock_post.call_args[0][0]
    assert call_url == webhook_url


# ── run_slack_alerts ──────────────────────────────────────────────────────────

def test_run_slack_alerts_sends_at_least_one_message(tmp_path, monkeypatch):
    # Write fake reports to tmp_path
    reports_file = str(tmp_path / "root_cause_reports.json")
    with open(reports_file, "w") as f:
        json.dump(SAMPLE_REPORTS, f)

    monkeypatch.setattr("src.alerts.slack_alerts.REPORTS_PATH",  reports_file)
    monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL",   "https://hooks.slack.com/test")

    mock_response = MagicMock(status_code=200, text="ok")

    with patch("src.alerts.slack_alerts.requests.post", return_value=mock_response) as mock_post:
        run_slack_alerts()

    assert mock_post.call_count >= 1, "Expected at least one POST to Slack webhook"


def test_run_slack_alerts_sends_summary_plus_individual_for_multiple_reports(tmp_path, monkeypatch):
    # 3 reports → 1 summary + 3 individual = 4 calls
    reports_file = str(tmp_path / "root_cause_reports.json")
    with open(reports_file, "w") as f:
        json.dump(SAMPLE_REPORTS, f)  # 3 reports

    monkeypatch.setattr("src.alerts.slack_alerts.REPORTS_PATH",  reports_file)
    monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL",   "https://hooks.slack.com/test")

    mock_response = MagicMock(status_code=200, text="ok")

    with patch("src.alerts.slack_alerts.requests.post", return_value=mock_response) as mock_post:
        run_slack_alerts()

    # 1 summary + 3 individual alerts = 4 total
    assert mock_post.call_count == 4


def test_run_slack_alerts_does_nothing_when_no_reports_file(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.alerts.slack_alerts.REPORTS_PATH",
        str(tmp_path / "nonexistent.json"),
    )
    monkeypatch.setattr("src.alerts.slack_alerts.WEBHOOK_URL", "https://hooks.slack.com/test")

    mock_response = MagicMock(status_code=200, text="ok")

    with patch("src.alerts.slack_alerts.requests.post", return_value=mock_response) as mock_post:
        run_slack_alerts()

    mock_post.assert_not_called()
