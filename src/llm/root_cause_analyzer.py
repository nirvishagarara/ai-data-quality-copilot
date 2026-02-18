"""
root_cause_analyzer.py
======================
Uses Claude (Anthropic API) to generate plain-English root-cause
explanations for detected anomalies and schema drift events.

Takes the output from:
  - anomaly_detector.py  → list of Anomaly objects
  - schema_monitor.py    → list of SchemaDrift objects

Produces:
  - A human-readable explanation of what broke
  - The most likely root cause
  - 2-3 concrete fix suggestions
  - A severity rating

Output:
  - Console report
  - data/root_cause_reports.json  (used by API + frontend later)

Usage:
    # Run end-to-end (detects anomalies then explains them)
    python src/llm/root_cause_analyzer.py

    # Or inject first to see it explain a real problem:
    python tests/inject_anomaly.py --scenario null_spike
    python src/llm/root_cause_analyzer.py
    python tests/inject_anomaly.py --reset
"""

import os
import json
import sys
from datetime import datetime
from dataclasses import dataclass, field

import anthropic
from dotenv import load_dotenv

# Load .env so ANTHROPIC_API_KEY is available
load_dotenv()

# Add project root to path so we can import our own modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.monitoring.anomaly_detector import run_anomaly_detector, Anomaly
from src.monitoring.schema_monitor import run_schema_monitor, SchemaDrift

# ─── Config ───────────────────────────────────────────────────────────────────

REPORTS_OUTPUT = "data/root_cause_reports.json"
MODEL          = "claude-haiku-4-5"   # cheapest + fastest — perfect for this use case
MAX_TOKENS     = 600                  # enough for a full explanation without wasting money

# ─── Data class ───────────────────────────────────────────────────────────────

@dataclass
class RootCauseReport:
    table:          str
    column:         str
    metric:         str
    anomaly_summary: str
    explanation:    str    # plain-English "what happened"
    root_cause:     str    # most likely technical cause
    fixes:          list   # list of suggested fix strings
    severity:       str
    generated_at:   str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ─── Prompt builder ───────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior data engineer diagnosing data pipeline failures.
You will be given details about a data anomaly detected in a production data warehouse.

Your job is to respond with a JSON object containing exactly these fields:
{
  "explanation": "1-2 sentence plain English description of what went wrong",
  "root_cause": "the single most likely technical root cause, be specific",
  "fixes": ["fix 1", "fix 2", "fix 3"],
  "severity": "CRITICAL | HIGH | MEDIUM | LOW"
}

Rules:
- Be specific. Reference the exact column names, table names, and percentages given.
- The explanation should be understandable by a non-technical stakeholder.
- The root_cause should be understandable by an engineer.
- Fixes should be concrete actions, not vague advice.
- Respond with ONLY the JSON object. No preamble, no markdown, no backticks."""


def build_prompt(
    anomaly: Anomaly,
    related_drift: list,
) -> str:
    """
    Builds a rich context prompt by combining anomaly data
    with any related schema drift detected on the same table.
    """
    drift_summary = "None detected."
    if related_drift:
        drift_lines = [
            f"  - {d.drift_type}: column '{d.column_name}' "
            f"(old: '{d.old_value}', new: '{d.new_value}')"
            for d in related_drift
        ]
        drift_summary = "\n".join(drift_lines)

    direction = "increased" if anomaly.pct_change > 0 else "decreased"
    pct = abs(anomaly.pct_change) * 100

    return f"""A data quality anomaly has been detected. Please diagnose it.

TABLE:          {anomaly.table}
COLUMN:         {anomaly.column}
METRIC:         {anomaly.metric}
CURRENT VALUE:  {anomaly.current_value:.4f}
BASELINE MEAN:  {anomaly.baseline_mean:.4f}
CHANGE:         {direction} by {pct:.1f}% from baseline
Z-SCORE:        {anomaly.z_score} {"(absolute threshold — metric was historically stable)" if anomaly.z_score == 999.0 else ""}
DETECTED AT:    {anomaly.detected_at}

SCHEMA DRIFT ON THIS TABLE TODAY:
{drift_summary}

Based on the above, provide your root cause analysis as a JSON object."""


# ─── Claude API call ──────────────────────────────────────────────────────────

def analyze_with_llm(
    anomaly: Anomaly,
    related_drift: list,
) -> RootCauseReport:
    """
    Calls Claude API with the anomaly context and parses the response
    into a RootCauseReport.
    """
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from environment

    prompt = build_prompt(anomaly, related_drift)

    try:
        message = client.messages.create(
            model      = MODEL,
            max_tokens = MAX_TOKENS,
            system     = SYSTEM_PROMPT,
            messages   = [{"role": "user", "content": prompt}]
        )

        raw_text = message.content[0].text.strip()

        # Strip markdown code fences if the model added them despite instructions
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
        raw_text = raw_text.strip()

        parsed = json.loads(raw_text)

        return RootCauseReport(
            table            = anomaly.table,
            column           = anomaly.column,
            metric           = anomaly.metric,
            anomaly_summary  = anomaly.summary(),
            explanation      = parsed.get("explanation", "No explanation returned."),
            root_cause       = parsed.get("root_cause", "Unknown."),
            fixes            = parsed.get("fixes", []),
            severity         = parsed.get("severity", anomaly.severity),
        )

    except json.JSONDecodeError as e:
        # Fallback if LLM returns malformed JSON
        return RootCauseReport(
            table            = anomaly.table,
            column           = anomaly.column,
            metric           = anomaly.metric,
            anomaly_summary  = anomaly.summary(),
            explanation      = f"LLM response could not be parsed: {raw_text[:200]}",
            root_cause       = "Parse error — see raw response above.",
            fixes            = ["Check LLM response format", "Retry the analysis"],
            severity         = anomaly.severity,
        )
    except Exception as e:
        # Fallback for API errors
        return RootCauseReport(
            table            = anomaly.table,
            column           = anomaly.column,
            metric           = anomaly.metric,
            anomaly_summary  = anomaly.summary(),
            explanation      = f"API error: {str(e)}",
            root_cause       = "Could not reach LLM API.",
            fixes            = ["Check ANTHROPIC_API_KEY in .env", "Check internet connection"],
            severity         = anomaly.severity,
        )


# ─── Output ───────────────────────────────────────────────────────────────────

def save_reports(reports: list):
    data = [
        {
            "table":           r.table,
            "column":          r.column,
            "metric":          r.metric,
            "anomaly_summary": r.anomaly_summary,
            "explanation":     r.explanation,
            "root_cause":      r.root_cause,
            "fixes":           r.fixes,
            "severity":        r.severity,
            "generated_at":    r.generated_at,
        }
        for r in reports
    ]
    with open(REPORTS_OUTPUT, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n  💾 Reports saved → {REPORTS_OUTPUT}")


def print_report(report: RootCauseReport):
    severity_icons = {
        "CRITICAL": "🔴",
        "HIGH":     "🟠",
        "MEDIUM":   "🟡",
        "LOW":      "🟢",
    }
    icon = severity_icons.get(report.severity, "⚪")

    print(f"\n  {icon} [{report.severity}] {report.table}.{report.column} — {report.metric}")
    print(f"  {'─'*50}")
    print(f"  📢 What happened:")
    print(f"     {report.explanation}")
    print(f"  🔍 Root cause:")
    print(f"     {report.root_cause}")
    print(f"  🔧 Suggested fixes:")
    for i, fix in enumerate(report.fixes, 1):
        print(f"     {i}. {fix}")


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_root_cause_analyzer() -> list:
    print("\n🤖  LLM Root-Cause Analyzer")
    print("=" * 55)

    # Step 1: Run anomaly detector (silent mode)
    print("\n  Step 1: Running anomaly detector ...")
    anomalies = run_anomaly_detector(verbose=False)

    # Step 2: Run schema monitor to get any related drift context
    print("  Step 2: Running schema monitor for context ...")
    drift_events = run_schema_monitor(verbose=False)

    if not anomalies and not drift_events:
        print("\n  ✅ No anomalies or schema drift detected — nothing to explain.")
        return []

    print(f"  → {len(anomalies)} anomaly(s), {len(drift_events)} drift event(s) found, sending to Claude ...\n")

    # Step 3: For each anomaly, call Claude and get explanation
    print(f"\n  Step 3: Generating root-cause explanations ...")
    print(f"  {'─'*50}")

    reports = []

    # Explain anomalies
    for anomaly in anomalies:
        related_drift = [d for d in drift_events if d.table == anomaly.table]
        print(f"\n  🔄 Analyzing anomaly: {anomaly.table}.{anomaly.column} ({anomaly.metric}) ...")
        report = analyze_with_llm(anomaly, related_drift)
        print_report(report)
        reports.append(report)

    # Also explain schema drift events that had no matching anomaly
    explained_tables = {a.table for a in anomalies}
    for drift in drift_events:
        if drift.table not in explained_tables:
            # Convert drift into a fake anomaly so we can reuse analyze_with_llm
            from src.monitoring.anomaly_detector import Anomaly
            fake_anomaly = Anomaly(
                table         = drift.table,
                column        = drift.column_name,
                metric        = "schema_drift",
                current_value = 0,
                baseline_mean = 0,
                baseline_std  = 0,
                z_score       = 999.0,
                pct_change    = 0,
                severity      = drift.severity,
            )
            print(f"\n  🔄 Analyzing drift: {drift.table}.{drift.column_name} ({drift.drift_type}) ...")
            report = analyze_with_llm(fake_anomaly, [drift])
            print_report(report)
            reports.append(report)
            explained_tables.add(drift.table)
    save_reports(reports)

    print(f"\n{'='*55}")
    print(f"  ✅ {len(reports)} report(s) generated")
    print(f"  Model used : {MODEL}")
    print(f"  Output     : {REPORTS_OUTPUT}")

    return reports


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_root_cause_analyzer()
