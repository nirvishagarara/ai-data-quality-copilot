"""
main.py
=======
FastAPI backend for the Data Quality Copilot.

Exposes REST endpoints that the Streamlit dashboard (and any other
consumer) can call to get monitoring data, trigger runs, and fetch
LLM reports.

Endpoints:
    GET  /                          health check
    GET  /api/tables                list all monitored tables + row counts
    GET  /api/tables/{name}/schema  schema + drift history for a table
    GET  /api/anomalies             all detected anomalies
    GET  /api/reports               all LLM root-cause reports
    GET  /api/tests                 all auto-generated tests
    GET  /api/lineage               lineage graph data (nodes + edges)
    POST /api/run/schema-monitor    trigger schema monitor run
    POST /api/run/anomaly-detector  trigger anomaly detector run
    POST /api/run/full-pipeline     trigger full pipeline (detect + explain + alert)

Usage:
    uvicorn src.api.main:app --reload --port 8000

Then visit:
    http://localhost:8000/docs   (auto-generated Swagger UI)
"""

import os
import json
import sys
from datetime import datetime

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.monitoring.schema_monitor import run_schema_monitor
from src.monitoring.anomaly_detector import run_anomaly_detector
from src.monitoring.test_generator import run_test_generator

# ─── App setup ────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Data Quality Copilot API",
    description = "AI-powered data observability platform",
    version     = "1.0.0",
)

# Allow Streamlit (running on different port) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH        = "data/warehouse.duckdb"
REPORTS_PATH   = "data/root_cause_reports.json"
TESTS_JSON     = "data/generated_tests.json"
ANOMALY_PATH   = "data/snapshots/anomaly_history.csv"
SCHEMA_PATH    = "data/snapshots/schema_history.csv"
BASELINE_PATH  = "data/snapshots/schema_baseline.csv"

TABLES = ["customers", "products", "orders", "order_items", "payments", "events"]

PIPELINE_EDGES = [
    {"source": "customers",   "target": "orders",           "label": "customer_id"},
    {"source": "products",    "target": "order_items",      "label": "product_id"},
    {"source": "orders",      "target": "order_items",      "label": "order_id"},
    {"source": "orders",      "target": "payments",         "label": "order_id"},
    {"source": "order_items", "target": "orders",           "label": "aggregates to"},
    {"source": "customers",   "target": "events",           "label": "customer_id"},
    {"source": "orders",      "target": "revenue_report",   "label": "feeds"},
    {"source": "payments",    "target": "revenue_report",   "label": "feeds"},
    {"source": "order_items", "target": "revenue_report",   "label": "feeds"},
    {"source": "events",      "target": "behaviour_report", "label": "feeds"},
]


# ─── Response models ──────────────────────────────────────────────────────────

class RunResponse(BaseModel):
    status:     str
    message:    str
    count:      int
    ran_at:     str


# ─── Helper ───────────────────────────────────────────────────────────────────

def load_json(path: str):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def load_csv(path: str):
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    return json.loads(df.fillna("").to_json(orient="records"))


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def health_check():
    return {
        "status":  "healthy",
        "service": "Data Quality Copilot API",
        "version": "1.0.0",
        "time":    datetime.utcnow().isoformat(),
    }


@app.get("/api/tables")
def get_tables():
    """Returns all monitored tables with row counts and column counts."""
    try:
        con = duckdb.connect(DB_PATH)
        results = []
        for table in TABLES:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            col_count = len(con.execute(f"DESCRIBE {table}").df())
            results.append({
                "table":     table,
                "row_count": row_count,
                "col_count": col_count,
            })
        con.close()
        return {"tables": results, "total": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables/{table_name}/schema")
def get_table_schema(table_name: str):
    """Returns the current schema and drift history for a specific table."""
    if table_name not in TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    try:
        con = duckdb.connect(DB_PATH)
        schema_df = con.execute(f"DESCRIBE {table_name}").df()
        con.close()

        current_schema = json.loads(schema_df.fillna("").to_json(orient="records"))

        # Load drift history for this table
        drift_history = []
        if os.path.exists(SCHEMA_PATH):
            df = pd.read_csv(SCHEMA_PATH)
            table_drift = df[df["table"] == table_name]
            drift_history = json.loads(table_drift.fillna("").to_json(orient="records"))

        return {
            "table":         table_name,
            "current_schema": current_schema,
            "drift_history":  drift_history,
            "drift_count":    len(drift_history),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/anomalies")
def get_anomalies():
    """Returns all detected anomalies from history."""
    anomalies = load_csv(ANOMALY_PATH)
    return {
        "anomalies": anomalies,
        "total":     len(anomalies),
        "critical":  sum(1 for a in anomalies if a.get("severity") == "CRITICAL"),
        "high":      sum(1 for a in anomalies if a.get("severity") == "HIGH"),
        "medium":    sum(1 for a in anomalies if a.get("severity") == "MEDIUM"),
    }


@app.get("/api/reports")
def get_reports():
    """Returns all LLM-generated root-cause reports."""
    reports = load_json(REPORTS_PATH)
    return {
        "reports": reports,
        "total":   len(reports),
    }


@app.get("/api/tests")
def get_tests():
    """Returns all auto-generated data quality tests."""
    tests = load_json(TESTS_JSON)

    # Group by table for easier consumption
    by_table = {}
    for t in tests:
        table = t.get("table", "unknown")
        by_table.setdefault(table, []).append(t)

    return {
        "tests":    tests,
        "total":    len(tests),
        "by_table": {t: len(v) for t, v in by_table.items()},
    }


@app.get("/api/lineage")
def get_lineage():
    """Returns lineage graph nodes and edges with anomaly status."""
    reports = load_json(REPORTS_PATH)

    # Build anomaly status map
    anomalous = {}
    for r in reports:
        table    = r.get("table")
        severity = r.get("severity", "LOW")
        if table:
            priority = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
            existing = anomalous.get(table, "OK")
            if existing == "OK" or priority.index(severity) > priority.index(existing):
                anomalous[table] = severity

    # Build node list
    all_nodes = set()
    for edge in PIPELINE_EDGES:
        all_nodes.add(edge["source"])
        all_nodes.add(edge["target"])

    nodes = [
        {
            "id":       node,
            "label":    node,
            "severity": anomalous.get(node, "OK"),
            "healthy":  node not in anomalous,
        }
        for node in all_nodes
    ]

    return {
        "nodes": nodes,
        "edges": PIPELINE_EDGES,
        "anomalous_count": len(anomalous),
    }


@app.get("/api/summary")
def get_summary():
    """Returns a dashboard summary — KPIs for the top of the Streamlit page."""
    anomalies = load_csv(ANOMALY_PATH)
    reports   = load_json(REPORTS_PATH)
    tests     = load_json(TESTS_JSON)

    # Schema drift count
    drift_count = 0
    if os.path.exists(SCHEMA_PATH):
        df = pd.read_csv(SCHEMA_PATH)
        drift_count = len(df)

    return {
        "tables_monitored": len(TABLES),
        "anomalies_total":  len(anomalies),
        "anomalies_critical": sum(1 for a in anomalies if a.get("severity") == "CRITICAL"),
        "schema_drift_events": drift_count,
        "tests_generated":  len(tests),
        "reports_generated": len(reports),
        "last_updated":     datetime.utcnow().isoformat(),
    }


# ─── Trigger endpoints ────────────────────────────────────────────────────────

@app.post("/api/run/schema-monitor", response_model=RunResponse)
def trigger_schema_monitor():
    """Triggers a schema monitor run and returns the results."""
    try:
        drifts = run_schema_monitor(verbose=False)
        return RunResponse(
            status  = "success",
            message = f"Schema monitor complete — {len(drifts)} drift event(s) detected",
            count   = len(drifts),
            ran_at  = datetime.utcnow().isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run/anomaly-detector", response_model=RunResponse)
def trigger_anomaly_detector():
    """Triggers an anomaly detector run and returns the results."""
    try:
        anomalies = run_anomaly_detector(verbose=False)
        return RunResponse(
            status  = "success",
            message = f"Anomaly detector complete — {len(anomalies)} anomaly(s) detected",
            count   = len(anomalies),
            ran_at  = datetime.utcnow().isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run/full-pipeline", response_model=RunResponse)
def trigger_full_pipeline():
    """
    Triggers the full pipeline:
    schema monitor → anomaly detector → LLM explanation → Slack alert
    """
    try:
        from src.llm.root_cause_analyzer import run_root_cause_analyzer
        from src.alerts.slack_alerts import run_slack_alerts

        reports = run_root_cause_analyzer()
        if reports:
            run_slack_alerts()

        return RunResponse(
            status  = "success",
            message = f"Full pipeline complete — {len(reports)} report(s) generated",
            count   = len(reports),
            ran_at  = datetime.utcnow().isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
