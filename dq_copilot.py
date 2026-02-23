#!/usr/bin/env python3
"""
dq_copilot.py — CLI entry point for the Data Quality Copilot.

Usage:
    python dq_copilot.py init          Create a starter dq_config.yaml
    python dq_copilot.py scan          Run schema monitor + anomaly detector
    python dq_copilot.py explain       Run LLM root-cause analyzer
    python dq_copilot.py test          Generate data quality tests
    python dq_copilot.py alert         Send Slack alerts
    python dq_copilot.py lineage       Build interactive lineage graph
    python dq_copilot.py dashboard     Launch Streamlit dashboard
    python dq_copilot.py full          Run entire pipeline end-to-end
"""

import argparse
import os
import sys
import shutil

def cmd_init(args):
    """Create a starter dq_config.yaml in the current directory."""
    target = "dq_config.yaml"
    template = os.path.join(os.path.dirname(__file__), "dq_config.yaml")

    if os.path.exists(target) and not args.force:
        print(f"  dq_config.yaml already exists. Use --force to overwrite.")
        return

    if os.path.exists(template):
        shutil.copy2(template, target)
    else:
        # Inline minimal config
        with open(target, "w") as f:
            f.write("""# dq_config.yaml — Data Quality Copilot Configuration
# Edit this file for your own DuckDB warehouse.

database:
  path: data/warehouse.duckdb

tables:
  - my_table_1
  - my_table_2

llm:
  model: claude-haiku-4-5
  max_tokens: 600
""")
    print(f"  Created {target}")
    print(f"  Edit it to point at your DuckDB database and tables.")


def cmd_scan(args):
    """Run schema monitor + anomaly detector."""
    from src.monitoring.schema_monitor import run_schema_monitor
    from src.monitoring.anomaly_detector import run_anomaly_detector

    run_schema_monitor()
    run_anomaly_detector()


def cmd_explain(args):
    """Run LLM root-cause analyzer."""
    from src.llm.root_cause_analyzer import run_root_cause_analyzer
    run_root_cause_analyzer()


def cmd_test(args):
    """Generate data quality tests."""
    from src.monitoring.test_generator import run_test_generator
    run_test_generator()


def cmd_alert(args):
    """Send Slack alerts."""
    from src.alerts.slack_alerts import run_slack_alerts
    run_slack_alerts()


def cmd_lineage(args):
    """Build interactive lineage graph."""
    from src.lineage.lineage_graph import run_lineage_graph
    run_lineage_graph()


def cmd_dashboard(args):
    """Launch Streamlit dashboard."""
    import subprocess
    frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "app.py")
    subprocess.run([sys.executable, "-m", "streamlit", "run", frontend_path])


def cmd_full(args):
    """Run entire pipeline: scan → explain → alert → lineage."""
    from src.monitoring.schema_monitor import run_schema_monitor
    from src.monitoring.anomaly_detector import run_anomaly_detector
    from src.llm.root_cause_analyzer import run_root_cause_analyzer
    from src.alerts.slack_alerts import run_slack_alerts
    from src.lineage.lineage_graph import run_lineage_graph
    from src.monitoring.test_generator import run_test_generator

    print("\n" + "=" * 60)
    print("  Data Quality Copilot — Full Pipeline")
    print("=" * 60)

    run_schema_monitor()
    run_anomaly_detector()
    run_root_cause_analyzer()
    run_slack_alerts()
    run_test_generator()
    run_lineage_graph()

    print("\n" + "=" * 60)
    print("  Pipeline complete!")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Data Quality Copilot — AI-powered data observability",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  init          Create a starter dq_config.yaml
  scan          Run schema monitor + anomaly detector
  explain       Run LLM root-cause analyzer (requires ANTHROPIC_API_KEY)
  test          Generate data quality tests from historical profiles
  alert         Send Slack alerts (requires SLACK_WEBHOOK_URL)
  lineage       Build interactive lineage graph
  dashboard     Launch Streamlit dashboard
  full          Run entire pipeline end-to-end
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # init
    init_parser = subparsers.add_parser("init", help="Create starter config")
    init_parser.add_argument("--force", action="store_true", help="Overwrite existing config")

    # scan
    subparsers.add_parser("scan", help="Run schema monitor + anomaly detector")

    # explain
    subparsers.add_parser("explain", help="Run LLM root-cause analyzer")

    # test
    subparsers.add_parser("test", help="Generate data quality tests")

    # alert
    subparsers.add_parser("alert", help="Send Slack alerts")

    # lineage
    subparsers.add_parser("lineage", help="Build lineage graph")

    # dashboard
    subparsers.add_parser("dashboard", help="Launch Streamlit dashboard")

    # full
    subparsers.add_parser("full", help="Run entire pipeline")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        "init":      cmd_init,
        "scan":      cmd_scan,
        "explain":   cmd_explain,
        "test":      cmd_test,
        "alert":     cmd_alert,
        "lineage":   cmd_lineage,
        "dashboard": cmd_dashboard,
        "full":      cmd_full,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
