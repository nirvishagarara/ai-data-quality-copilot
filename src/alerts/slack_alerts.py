"""
slack_alerts.py
===============
Sends formatted Slack alerts when anomalies or schema drift are detected.

Reads from:
  - data/root_cause_reports.json  (LLM-generated reports)
  - data/snapshots/anomaly_history.csv
  - data/snapshots/schema_history.csv

Usage:
    # Send alerts for latest detections
    python src/alerts/slack_alerts.py

    # Full pipeline — detect, explain, then alert:
    python tests/inject_anomaly.py --scenario null_spike
    python src/llm/root_cause_analyzer.py
    python src/alerts/slack_alerts.py
    python tests/inject_anomaly.py --reset
"""

import os
import json
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

WEBHOOK_URL    = os.getenv("SLACK_WEBHOOK_URL")
REPORTS_PATH   = "data/root_cause_reports.json"
ANOMALY_PATH   = "data/snapshots/anomaly_history.csv"
SCHEMA_PATH    = "data/snapshots/schema_history.csv"

SEVERITY_COLORS = {
    "CRITICAL": "#FF0000",
    "HIGH":     "#FF8C00",
    "MEDIUM":   "#FFD700",
    "LOW":      "#36A64F",
}

SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH":     "🟠",
    "MEDIUM":   "🟡",
    "LOW":      "🟢",
}


# ─── Formatters ───────────────────────────────────────────────────────────────

def format_report_alert(report: dict) -> dict:
    severity = report.get("severity", "MEDIUM")
    color    = SEVERITY_COLORS.get(severity, "#808080")
    emoji    = SEVERITY_EMOJI.get(severity, "⚪")

    fixes = report.get("fixes", [])
    fixes_text = "\n".join(f"{i+1}. {fix}" for i, fix in enumerate(fixes))

    text = (
        f"{emoji} *[{severity}] `{report['table']}.{report['column']}` — {report['metric']}*\n\n"
        f"*What happened:*\n{report.get('explanation', 'N/A')}\n\n"
        f"*Root cause:*\n{report.get('root_cause', 'N/A')}\n\n"
        f"*Suggested fixes:*\n{fixes_text}\n\n"
        f"_🤖 Data Quality Copilot · Powered by Claude AI_"
    )

    return {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": text
                        }
                    }
                ]
            }
        ]
    }

def format_summary_alert(reports: list) -> dict:
    """
    Sends a single summary message when multiple anomalies are detected.
    Good for daily digest style alerting.
    """
    critical = sum(1 for r in reports if r.get("severity") == "CRITICAL")
    high     = sum(1 for r in reports if r.get("severity") == "HIGH")
    medium   = sum(1 for r in reports if r.get("severity") == "MEDIUM")

    lines = []
    for r in reports:
        emoji = SEVERITY_EMOJI.get(r.get("severity", "LOW"), "⚪")
        lines.append(f"{emoji} `{r['table']}.{r['column']}` — {r['metric']}")

    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 Data Quality Report — {len(reports)} Issue(s) Detected"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*🔴 Critical:* {critical}"},
                    {"type": "mrkdwn", "text": f"*🟠 High:* {high}"},
                    {"type": "mrkdwn", "text": f"*🟡 Medium:* {medium}"},
                    {"type": "mrkdwn", "text": f"*🕐 Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Issues found:*\n" + "\n".join(lines)
                }
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "🤖 *Data Quality Copilot* · Detailed reports follow below"
                    }
                ]
            }
        ]
    }


# ─── Sender ───────────────────────────────────────────────────────────────────

def send_slack_message(payload: dict) -> bool:
    """Sends a message to Slack via webhook. Returns True if successful."""
    if not WEBHOOK_URL:
        print("  ✗ SLACK_WEBHOOK_URL not found in .env")
        return False

    try:
        response = requests.post(
            WEBHOOK_URL,
            json=payload,
            timeout=10
        )
        if response.status_code == 200 and response.text == "ok":
            return True
        else:
            print(f"  ✗ Slack returned: {response.status_code} — {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Request failed: {e}")
        return False


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_slack_alerts():
    print("\n🔔  Slack Alerts")
    print("=" * 55)

    # Load latest reports
    if not os.path.exists(REPORTS_PATH):
        print("  ✗ No reports found at data/root_cause_reports.json")
        print("    Run src/llm/root_cause_analyzer.py first")
        return

    with open(REPORTS_PATH) as f:
        reports = json.load(f)

    if not reports:
        print("  ✅ No reports to send")
        return

    print(f"  Found {len(reports)} report(s) to send\n")

    # Send summary first if multiple issues
    if len(reports) > 1:
        print("  Sending summary alert ...")
        payload = format_summary_alert(reports)
        success = send_slack_message(payload)
        print(f"  {'✅ Summary sent' if success else '✗ Summary failed'}")
        print()

    # Send individual detailed alerts
    for i, report in enumerate(reports, 1):
        severity = report.get("severity", "?")
        table    = report.get("table", "?")
        col      = report.get("column", "?")
        print(f"  Sending [{severity}] {table}.{col} ...")
        payload  = format_report_alert(report)
        success  = send_slack_message(payload)
        print(f"  {'✅ Sent' if success else '✗ Failed'}")

    print(f"\n{'─'*55}")
    print(f"  ✅ Done — check your #data-alerts Slack channel")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not WEBHOOK_URL:
        print("\n✗ SLACK_WEBHOOK_URL missing from .env")
        print("  Add it like: SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...")
    else:
        run_slack_alerts()
