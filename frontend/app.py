"""
app.py
======
Streamlit dashboard for the Data Quality Copilot.

Runs standalone — no FastAPI/uvicorn process needed.
All monitoring functions are called directly as Python imports.

Pages:
  1. Dashboard    — KPI summary cards + recent anomalies
  2. Anomalies    — full anomaly explorer with LLM explanations
  3. Schema       — schema drift history per table
  4. Tests        — auto-generated data quality tests
  5. Lineage      — embedded interactive lineage graph
  6. Run Pipeline — trigger monitoring runs from the UI

Usage:
    streamlit run frontend/app.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from datetime import datetime

import duckdb
import pandas as pd
import streamlit as st

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH      = "data/warehouse.duckdb"
REPORTS_PATH = "data/root_cause_reports.json"
TESTS_JSON   = "data/generated_tests.json"
ANOMALY_PATH = "data/snapshots/anomaly_history.csv"
SCHEMA_PATH  = "data/snapshots/schema_history.csv"
LINEAGE_PATH = "data/lineage_graph.html"

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

st.set_page_config(
    page_title = "Data Quality Copilot",
    page_icon  = "🔍",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

# ─── Design tokens ────────────────────────────────────────────────────────────

SEVERITY_COLORS = {
    "CRITICAL": "#dc2626",
    "HIGH":     "#ea580c",
    "MEDIUM":   "#d97706",
    "LOW":      "#16a34a",
    "OK":       "#16a34a",
}
SEVERITY_BG = {
    "CRITICAL": "#fef2f2",
    "HIGH":     "#fff7ed",
    "MEDIUM":   "#fffbeb",
    "LOW":      "#f0fdf4",
    "OK":       "#f0fdf4",
}
SEVERITY_BORDER = {
    "CRITICAL": "#fca5a5",
    "HIGH":     "#fdba74",
    "MEDIUM":   "#fcd34d",
    "LOW":      "#86efac",
    "OK":       "#86efac",
}

# ─── Global CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── Reset & base ── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

/* ── Page background ── */
.stApp {
    background-color: #f8fafc;
}

/* ── Sidebar — dark navy, Mercor-style ── */
[data-testid="stSidebar"] {
    background: #0f172a !important;
    border-right: 1px solid #1e293b;
}
[data-testid="stSidebar"] * {
    color: #94a3b8 !important;
}
[data-testid="stSidebar"] .stRadio > div {
    gap: 2px;
}
[data-testid="stSidebar"] .stRadio label {
    color: #94a3b8 !important;
    font-size: 0.875rem !important;
    font-weight: 500 !important;
    padding: 10px 14px !important;
    border-radius: 8px !important;
    transition: background 0.15s, color 0.15s;
    cursor: pointer;
}
[data-testid="stSidebar"] .stRadio label:hover {
    background: rgba(255,255,255,0.06) !important;
    color: #f1f5f9 !important;
}
[data-testid="stSidebar"] .stRadio [data-baseweb="radio"] input:checked ~ div {
    background: rgba(37,99,235,0.15) !important;
}

/* ── Main content padding ── */
.main .block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1280px;
}

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.03);
    transition: box-shadow 0.2s, transform 0.15s;
}
[data-testid="metric-container"]:hover {
    box-shadow: 0 4px 12px rgba(37,99,235,0.1);
    transform: translateY(-1px);
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-weight: 800 !important;
    color: #0f172a !important;
    letter-spacing: -0.02em;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    color: #64748b !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}

/* ── Dataframes ── */
[data-testid="stDataFrame"] {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    overflow: hidden;
    background: white;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    margin-bottom: 8px;
}
[data-testid="stExpander"] summary {
    font-weight: 600 !important;
    color: #0f172a !important;
}
[data-testid="stExpander"]:hover {
    border-color: #93c5fd !important;
}

/* ── Buttons ── */
.stButton > button {
    background: #2563eb !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.875rem !important;
    padding: 10px 20px !important;
    transition: background 0.15s, transform 0.1s, box-shadow 0.15s !important;
    box-shadow: 0 1px 3px rgba(37,99,235,0.3) !important;
}
.stButton > button:hover {
    background: #1d4ed8 !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 12px rgba(37,99,235,0.35) !important;
}
.stButton > button[kind="primary"] {
    background: #2563eb !important;
}

/* ── Selectbox ── */
[data-testid="stSelectbox"] > div > div {
    background: #ffffff !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 8px !important;
    color: #0f172a !important;
    font-size: 0.875rem !important;
}

/* ── Alert boxes ── */
[data-testid="stAlert"] {
    border-radius: 10px !important;
}

/* ── Divider ── */
hr {
    border-color: #e2e8f0 !important;
    margin: 1.5rem 0 !important;
}

/* ── Code blocks ── */
.stCodeBlock {
    border-radius: 10px !important;
    border: 1px solid #e2e8f0 !important;
}

/* ── Custom components ── */

.page-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    border-bottom: 1px solid #e2e8f0;
    padding-bottom: 20px;
    margin-bottom: 28px;
}
.page-header-left h1 {
    font-size: 1.6rem;
    font-weight: 800;
    color: #0f172a;
    margin: 0 0 4px 0;
    letter-spacing: -0.02em;
}
.page-header-left p {
    font-size: 0.875rem;
    color: #64748b;
    margin: 0;
}

/* KPI cards (custom HTML) */
.kpi-row {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 16px;
    margin-bottom: 28px;
}
.kpi-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 20px 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    transition: box-shadow 0.2s, transform 0.15s;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: #2563eb;
    border-radius: 12px 12px 0 0;
}
.kpi-card.critical::before { background: #dc2626; }
.kpi-card:hover {
    box-shadow: 0 6px 20px rgba(37,99,235,0.1);
    transform: translateY(-2px);
}
.kpi-label {
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: #64748b;
    margin-bottom: 8px;
}
.kpi-value {
    font-size: 2.2rem;
    font-weight: 800;
    color: #0f172a;
    letter-spacing: -0.03em;
    line-height: 1;
    margin-bottom: 4px;
}
.kpi-value.critical { color: #dc2626; }
.kpi-icon {
    position: absolute;
    top: 16px; right: 16px;
    font-size: 1.4rem;
    opacity: 0.4;
}

/* Table health */
.table-card {
    display: flex;
    align-items: center;
    gap: 12px;
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 13px 16px;
    margin-bottom: 8px;
    transition: border-color 0.15s, box-shadow 0.15s;
}
.table-card:hover {
    border-color: #93c5fd;
    box-shadow: 0 2px 8px rgba(37,99,235,0.08);
}
.status-dot {
    width: 9px; height: 9px;
    border-radius: 50%;
    flex-shrink: 0;
}
.table-name {
    font-weight: 600;
    font-size: 0.875rem;
    color: #0f172a;
    flex: 1;
}
.table-meta {
    font-size: 0.78rem;
    color: #94a3b8;
}

/* Severity badge */
.badge {
    display: inline-flex;
    align-items: center;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    border: 1px solid transparent;
    white-space: nowrap;
}

/* Section heading */
.section-title {
    font-size: 0.8rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: #64748b;
    margin: 0 0 14px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid #f1f5f9;
}

/* Report card */
.rpt-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-left-width: 4px;
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.03);
}
.rpt-title {
    font-size: 0.95rem;
    font-weight: 700;
    color: #0f172a;
    margin-bottom: 3px;
}
.rpt-meta {
    font-size: 0.78rem;
    color: #94a3b8;
    margin-bottom: 14px;
}
.rpt-label {
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: #94a3b8;
    margin-bottom: 5px;
}
.rpt-text {
    font-size: 0.875rem;
    color: #374151;
    line-height: 1.65;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 10px 13px;
    margin-bottom: 13px;
}
.fix-row {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 7px 0;
    border-bottom: 1px solid #f1f5f9;
    font-size: 0.875rem;
    color: #374151;
}
.fix-num {
    background: #2563eb;
    color: white;
    border-radius: 50%;
    width: 20px; height: 20px;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.68rem; font-weight: 700;
    flex-shrink: 0; margin-top: 1px;
}
.pill {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background: #f1f5f9;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 4px 10px;
    font-size: 0.78rem;
    color: #374151;
    margin: 3px 4px 3px 0;
}
.pill strong { color: #0f172a; }

/* Pipeline cards */
.pipe-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 24px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    text-align: center;
    height: 100%;
    transition: box-shadow 0.2s, transform 0.15s;
}
.pipe-card:hover {
    box-shadow: 0 8px 24px rgba(37,99,235,0.12);
    transform: translateY(-2px);
}
.pipe-icon { font-size: 2rem; margin-bottom: 10px; }
.pipe-title {
    font-size: 1rem;
    font-weight: 700;
    color: #0f172a;
    margin-bottom: 6px;
}
.pipe-desc {
    font-size: 0.82rem;
    color: #64748b;
    line-height: 1.55;
    margin-bottom: 18px;
}
.pipe-step {
    display: inline-block;
    background: #eff6ff;
    color: #2563eb;
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 3px 9px;
    border-radius: 20px;
    margin-bottom: 12px;
}

/* Test type badge */
.ttype {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 8px;
}

/* Sidebar brand */
.sb-brand {
    padding: 20px 16px 16px;
    border-bottom: 1px solid #1e293b;
    margin-bottom: 12px;
}
.sb-logo {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 4px;
}
.sb-logo-icon {
    background: #2563eb;
    border-radius: 8px;
    width: 32px; height: 32px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1rem;
}
.sb-logo-text {
    font-size: 0.9rem;
    font-weight: 700;
    color: #f8fafc !important;
    letter-spacing: -0.01em;
}
.sb-sub {
    font-size: 0.7rem;
    color: #475569 !important;
    margin-left: 42px;
    margin-top: -2px;
}
.sb-status {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 8px 14px;
    background: rgba(22,163,74,0.1);
    border: 1px solid rgba(22,163,74,0.2);
    border-radius: 8px;
    margin: 12px 0 0;
}
.sb-status-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: #22c55e;
    box-shadow: 0 0 6px #22c55e;
    animation: sb-pulse 2s ease-in-out infinite;
    flex-shrink: 0;
}
@keyframes sb-pulse { 0%,100% { opacity:1; } 50% { opacity:0.35; } }
.sb-status-text {
    font-size: 0.75rem;
    font-weight: 600;
    color: #22c55e !important;
}

.pipe-arrow {
    display: flex;
    align-items: center;
    justify-content: center;
    color: #cbd5e1;
    font-size: 1.3rem;
    padding-top: 55px;
}
</style>
""", unsafe_allow_html=True)


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _load_json(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def _load_csv(path):
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    return json.loads(df.fillna("").to_json(orient="records"))


def _get(path):
    if path == "/":
        return {"status": "healthy"}

    if path == "/api/summary":
        anomalies = _load_csv(ANOMALY_PATH)
        reports   = _load_json(REPORTS_PATH)
        tests     = _load_json(TESTS_JSON)
        drift_count = 0
        if os.path.exists(SCHEMA_PATH):
            drift_count = len(pd.read_csv(SCHEMA_PATH))
        return {
            "tables_monitored":    len(TABLES),
            "anomalies_total":     len(anomalies),
            "anomalies_critical":  sum(1 for a in anomalies if a.get("severity") == "CRITICAL"),
            "schema_drift_events": drift_count,
            "tests_generated":     len(tests),
            "reports_generated":   len(reports),
        }

    if path == "/api/tables":
        con = duckdb.connect(DB_PATH, read_only=True)
        results = []
        for table in TABLES:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            col_count = len(con.execute(f"DESCRIBE {table}").df())
            results.append({"table": table, "row_count": row_count, "col_count": col_count})
        con.close()
        return {"tables": results, "total": len(results)}

    if path.startswith("/api/tables/") and path.endswith("/schema"):
        table_name = path.split("/")[3]
        con = duckdb.connect(DB_PATH, read_only=True)
        schema_df = con.execute(f"DESCRIBE {table_name}").df()
        con.close()
        current_schema = json.loads(schema_df.fillna("").to_json(orient="records"))
        drift_history = []
        if os.path.exists(SCHEMA_PATH):
            df = pd.read_csv(SCHEMA_PATH)
            table_drift = df[df["table"] == table_name]
            drift_history = json.loads(table_drift.fillna("").to_json(orient="records"))
        return {"table": table_name, "current_schema": current_schema,
                "drift_history": drift_history, "drift_count": len(drift_history)}

    if path == "/api/anomalies":
        anomalies = _load_csv(ANOMALY_PATH)
        return {"anomalies": anomalies, "total": len(anomalies),
                "critical": sum(1 for a in anomalies if a.get("severity") == "CRITICAL"),
                "high":     sum(1 for a in anomalies if a.get("severity") == "HIGH"),
                "medium":   sum(1 for a in anomalies if a.get("severity") == "MEDIUM")}

    if path == "/api/reports":
        reports = _load_json(REPORTS_PATH)
        return {"reports": reports, "total": len(reports)}

    if path == "/api/tests":
        tests = _load_json(TESTS_JSON)
        by_table = {}
        for t in tests:
            by_table.setdefault(t.get("table", "unknown"), []).append(t)
        return {"tests": tests, "total": len(tests),
                "by_table": {t: len(v) for t, v in by_table.items()}}

    if path == "/api/lineage":
        reports = _load_json(REPORTS_PATH)
        anomalous = {}
        for r in reports:
            table    = r.get("table")
            severity = r.get("severity", "LOW")
            if table:
                priority = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
                existing = anomalous.get(table, "OK")
                if existing == "OK" or priority.index(severity) > priority.index(existing):
                    anomalous[table] = severity
        all_nodes = set()
        for edge in PIPELINE_EDGES:
            all_nodes.add(edge["source"])
            all_nodes.add(edge["target"])
        nodes = [{"id": n, "label": n, "severity": anomalous.get(n, "OK"),
                  "healthy": n not in anomalous} for n in all_nodes]
        return {"nodes": nodes, "edges": PIPELINE_EDGES, "anomalous_count": len(anomalous)}

    return None


def api_get(path):
    try:
        return _get(path)
    except Exception as e:
        st.error(f"Error: {e}")
        return None


def api_post(path):
    from src.monitoring.schema_monitor import run_schema_monitor
    from src.monitoring.anomaly_detector import run_anomaly_detector
    try:
        if path == "/api/run/schema-monitor":
            drifts = run_schema_monitor(verbose=False)
            return {"status": "success",
                    "message": f"Schema monitor complete — {len(drifts)} drift event(s) detected",
                    "count": len(drifts), "ran_at": datetime.utcnow().isoformat()}
        if path == "/api/run/anomaly-detector":
            anomalies = run_anomaly_detector(verbose=False)
            return {"status": "success",
                    "message": f"Anomaly detector complete — {len(anomalies)} anomaly(s) detected",
                    "count": len(anomalies), "ran_at": datetime.utcnow().isoformat()}
        if path == "/api/run/full-pipeline":
            from src.llm.root_cause_analyzer import run_root_cause_analyzer
            from src.alerts.slack_alerts import run_slack_alerts
            reports = run_root_cause_analyzer()
            if reports:
                run_slack_alerts()
            return {"status": "success",
                    "message": f"Full pipeline complete — {len(reports)} report(s) generated",
                    "count": len(reports), "ran_at": datetime.utcnow().isoformat()}
        return None
    except Exception as e:
        st.error(f"Error: {e}")
        return None


# ─── Helper renderers ─────────────────────────────────────────────────────────

def badge(severity):
    c  = SEVERITY_COLORS.get(severity, "#64748b")
    bg = SEVERITY_BG.get(severity, "#f8fafc")
    bd = SEVERITY_BORDER.get(severity, "#e2e8f0")
    return f'<span class="badge" style="color:{c};background:{bg};border-color:{bd};">{severity}</span>'


def ttype_badge(test_type):
    palette = {
        "not_null":          ("#0284c7", "#e0f2fe"),
        "unique":            ("#7c3aed", "#ede9fe"),
        "accepted_values":   ("#0891b2", "#ecfeff"),
        "row_count_between": ("#059669", "#ecfdf5"),
        "value_between":     ("#d97706", "#fffbeb"),
    }
    c, bg = palette.get(test_type, ("#64748b", "#f8fafc"))
    label = test_type.replace("_", " ")
    return f'<span class="ttype" style="color:{c};background:{bg};">{label}</span>'


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div class="sb-brand">
        <div class="sb-logo">
            <div class="sb-logo-icon">🔍</div>
            <div class="sb-logo-text">DQ Copilot</div>
        </div>
        <div class="sb-sub">AI-Powered Data Observability</div>
        <div class="sb-status">
            <div class="sb-status-dot"></div>
            <span class="sb-status-text">All systems operational</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigate",
        ["📊 Dashboard", "🚨 Anomalies", "📐 Schema Monitor",
         "🧪 Tests", "🗺️ Lineage Graph", "▶️ Run Pipeline"],
        label_visibility="collapsed"
    )

    st.markdown("""
    <div style="position:absolute;bottom:20px;left:16px;right:16px;">
        <div style="font-size:0.7rem;color:#334155;text-align:center;line-height:1.8;">
            Python · DuckDB · Claude AI
        </div>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════

if page == "📊 Dashboard":

    st.markdown("""
    <div class="page-header">
        <div class="page-header-left">
            <h1>Dashboard</h1>
            <p>Real-time overview of your data pipeline health and AI-detected anomalies.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    summary = api_get("/api/summary")
    if summary:
        c = summary.get("anomalies_critical", 0)
        crit_class = "critical" if c > 0 else ""
        st.markdown(f"""
        <div class="kpi-row">
            <div class="kpi-card">
                <div class="kpi-icon">🗄️</div>
                <div class="kpi-label">Tables Monitored</div>
                <div class="kpi-value">{summary['tables_monitored']}</div>
            </div>
            <div class="kpi-card {crit_class}">
                <div class="kpi-icon">🚨</div>
                <div class="kpi-label">Anomalies Detected</div>
                <div class="kpi-value {crit_class}">{summary['anomalies_total']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">📐</div>
                <div class="kpi-label">Schema Drift Events</div>
                <div class="kpi-value">{summary['schema_drift_events']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">🧪</div>
                <div class="kpi-label">Tests Generated</div>
                <div class="kpi-value">{summary['tests_generated']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">🤖</div>
                <div class="kpi-label">LLM Reports</div>
                <div class="kpi-value">{summary['reports_generated']}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.markdown('<div class="section-title">Table Health</div>', unsafe_allow_html=True)
        tables  = api_get("/api/tables")
        reports = api_get("/api/reports")

        if tables and reports:
            anomalous_map = {r["table"]: r["severity"] for r in reports.get("reports", [])}
            for t in tables.get("tables", []):
                name     = t["table"]
                severity = anomalous_map.get(name, "OK")
                color    = SEVERITY_COLORS.get(severity, "#16a34a")
                bdg      = "" if severity == "OK" else badge(severity)
                st.markdown(f"""
                <div class="table-card">
                    <div class="status-dot" style="background:{color};"></div>
                    <div class="table-name">{name}</div>
                    <div class="table-meta">{t['row_count']:,} rows · {t['col_count']} cols</div>
                    {bdg}
                </div>
                """, unsafe_allow_html=True)

    with col_right:
        anomalies = api_get("/api/anomalies")

        if anomalies and anomalies["total"] > 0:
            n_c = anomalies.get("critical", 0)
            n_h = anomalies.get("high", 0)
            n_m = anomalies.get("medium", 0)
            n_l = anomalies["total"] - n_c - n_h - n_m

            st.markdown('<div class="section-title">Severity Breakdown</div>', unsafe_allow_html=True)
            chart_df = pd.DataFrame({
                "Severity": ["Critical", "High", "Medium", "Low"],
                "Count":    [n_c, n_h, n_m, n_l],
            }).set_index("Severity")
            st.bar_chart(chart_df, color="#2563eb", height=170)

            st.markdown('<div class="section-title" style="margin-top:20px;">Recent Anomalies</div>', unsafe_allow_html=True)
            df     = pd.DataFrame(anomalies["anomalies"])
            recent = df.tail(6)[["table","column","metric","severity","pct_change"]].copy()
            recent["pct_change"] = recent["pct_change"].apply(lambda x: f"{float(x)*100:.1f}%")
            st.dataframe(recent, use_container_width=True, hide_index=True)
        else:
            st.markdown('<div class="section-title">Recent Anomalies</div>', unsafe_allow_html=True)
            st.success("✅ All clear — no anomalies detected")


# ══════════════════════════════════════════════════════════
# PAGE 2 — ANOMALIES
# ══════════════════════════════════════════════════════════

elif page == "🚨 Anomalies":

    st.markdown("""
    <div class="page-header">
        <div class="page-header-left">
            <h1>Anomaly Explorer</h1>
            <p>AI-generated root-cause analysis and fix recommendations for every detected anomaly.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    reports = api_get("/api/reports")

    if reports and reports["reports"]:
        st.markdown(
            f'<p style="color:#64748b;font-size:0.85rem;margin-bottom:18px;">'
            f'{reports["total"]} report{"s" if reports["total"] != 1 else ""} found</p>',
            unsafe_allow_html=True
        )

        for report in reports["reports"]:
            severity = report.get("severity", "LOW")
            lc       = SEVERITY_COLORS.get(severity, "#64748b")

            with st.expander(
                f"[{severity}]  {report['table']}.{report['column']} — {report['metric']}",
                expanded = severity in ["CRITICAL", "HIGH"]
            ):
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown(
                        f'<div class="rpt-label">What happened</div>'
                        f'<div class="rpt-text">{report.get("explanation", "N/A")}</div>',
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f'<div class="rpt-label">Root cause</div>'
                        f'<div class="rpt-text">{report.get("root_cause", "N/A")}</div>',
                        unsafe_allow_html=True
                    )
                    fixes = report.get("fixes", [])
                    if fixes:
                        items = "".join(
                            f'<div class="fix-row"><div class="fix-num">{i}</div><div>{f}</div></div>'
                            for i, f in enumerate(fixes, 1)
                        )
                        st.markdown(
                            f'<div class="rpt-label">Suggested fixes</div>{items}',
                            unsafe_allow_html=True
                        )

                with col2:
                    detected = report.get("generated_at", "")[:19].replace("T", " ")
                    st.markdown(f"""
                    <div class="rpt-label" style="margin-top:4px;">Details</div>
                    <div style="margin-top:8px;">
                        <div class="pill"><strong>Table</strong>&nbsp;{report['table']}</div>
                        <div class="pill"><strong>Column</strong>&nbsp;{report['column']}</div>
                        <div class="pill"><strong>Metric</strong>&nbsp;{report['metric']}</div>
                        <div style="margin-top:6px;">{badge(severity)}</div>
                        <div class="pill" style="margin-top:6px;"><strong>At</strong>&nbsp;{detected}</div>
                    </div>
                    """, unsafe_allow_html=True)
    else:
        st.info("No anomaly reports found. Go to **▶️ Run Pipeline** to trigger a monitoring run.")


# ══════════════════════════════════════════════════════════
# PAGE 3 — SCHEMA MONITOR
# ══════════════════════════════════════════════════════════

elif page == "📐 Schema Monitor":

    st.markdown("""
    <div class="page-header">
        <div class="page-header-left">
            <h1>Schema Monitor</h1>
            <p>Track column additions, removals, and type changes across all tables over time.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    tables = api_get("/api/tables")
    if not tables:
        st.stop()

    table_names = [t["table"] for t in tables["tables"]]
    selected    = st.selectbox("Select table", table_names)

    if selected:
        schema = api_get(f"/api/tables/{selected}/schema")
        if schema:
            dc = schema["drift_count"]
            drift_badge = (
                badge("CRITICAL").replace("CRITICAL", f"{dc} drift event{'s' if dc!=1 else ''}") if dc > 0
                else badge("OK").replace("OK", "No drift")
            )
            col1, col2 = st.columns([1, 1], gap="large")

            with col1:
                st.markdown('<div class="section-title">Current Schema</div>', unsafe_allow_html=True)
                df = pd.DataFrame(schema["current_schema"])
                if not df.empty:
                    display_cols = [c for c in ["column_name","column_type","null","key","default"] if c in df.columns]
                    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

            with col2:
                st.markdown(
                    f'<div class="section-title">Drift History &nbsp; {drift_badge}</div>',
                    unsafe_allow_html=True
                )
                if schema["drift_history"]:
                    st.dataframe(pd.DataFrame(schema["drift_history"]), use_container_width=True, hide_index=True)
                else:
                    st.success("✅ No drift events recorded for this table")


# ══════════════════════════════════════════════════════════
# PAGE 4 — TESTS
# ══════════════════════════════════════════════════════════

elif page == "🧪 Tests":

    st.markdown("""
    <div class="page-header">
        <div class="page-header-left">
            <h1>Auto-Generated Tests</h1>
            <p>Data quality tests generated automatically from 30-day historical column profiles.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    tests = api_get("/api/tests")
    if tests:
        by_table = tests.get("by_table", {})
        cols     = st.columns(len(by_table))
        for i, (table, count) in enumerate(by_table.items()):
            cols[i].metric(table, count)

        st.markdown(
            f'<p style="color:#64748b;font-size:0.85rem;margin:12px 0 20px;">'
            f'<strong style="color:#0f172a;">{tests["total"]}</strong> tests across {len(by_table)} tables</p>',
            unsafe_allow_html=True
        )
        st.markdown("---")

        fc1, fc2 = st.columns(2)
        with fc1:
            all_tests   = tests.get("tests", [])
            table_names = sorted(set(t["table"] for t in all_tests))
            sel_table   = st.selectbox("Filter by table", ["All"] + table_names)
        filtered = all_tests if sel_table == "All" else [t for t in all_tests if t["table"] == sel_table]

        with fc2:
            test_types = sorted(set(t["test_type"] for t in filtered))
            sel_type   = st.selectbox("Filter by test type", ["All"] + test_types)
        if sel_type != "All":
            filtered = [t for t in filtered if t["test_type"] == sel_type]

        st.markdown(
            f'<p style="color:#64748b;font-size:0.82rem;margin:10px 0 14px;">Showing <strong style="color:#0f172a;">{len(filtered)}</strong> tests</p>',
            unsafe_allow_html=True
        )

        icons = {"not_null":"🚫","unique":"🔑","accepted_values":"📋","row_count_between":"📊","value_between":"📏"}
        for test in filtered:
            with st.expander(f"{icons.get(test['test_type'],'🧪')}  {test['plain_english']}"):
                c1, c2 = st.columns([1, 1])
                with c1:
                    st.markdown(
                        f'{ttype_badge(test["test_type"])}'
                        f'<div class="pill"><strong>Table</strong>&nbsp;{test["table"]}</div>'
                        f'<div class="pill"><strong>Column</strong>&nbsp;{test["column"]}</div>',
                        unsafe_allow_html=True
                    )
                with c2:
                    st.markdown(
                        f'<div class="rpt-label">Why generated</div>'
                        f'<div style="font-size:0.85rem;color:#374151;">{test.get("reason","")}</div>',
                        unsafe_allow_html=True
                    )
                    if test.get("parameters"):
                        st.markdown(
                            f'<div class="rpt-label" style="margin-top:10px;">Parameters</div>'
                            f'<code style="font-size:0.8rem;color:#2563eb;">{test["parameters"]}</code>',
                            unsafe_allow_html=True
                        )


# ══════════════════════════════════════════════════════════
# PAGE 5 — LINEAGE GRAPH
# ══════════════════════════════════════════════════════════

elif page == "🗺️ Lineage Graph":

    st.markdown("""
    <div class="page-header">
        <div class="page-header-left">
            <h1>Pipeline Lineage Graph</h1>
            <p>Interactive graph showing data flow between tables. Anomalous nodes are highlighted.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    lineage = api_get("/api/lineage")
    if lineage:
        if lineage["anomalous_count"] > 0:
            for node in [n for n in lineage["nodes"] if not n["healthy"]]:
                st.markdown(
                    f'<div style="background:#fef2f2;border:1px solid #fca5a5;border-left:4px solid #dc2626;'
                    f'border-radius:8px;padding:10px 14px;margin-bottom:8px;font-size:0.875rem;color:#dc2626;font-weight:600;">'
                    f'⚠️ {node["id"]} — {badge(node["severity"])} anomaly active</div>',
                    unsafe_allow_html=True
                )
        else:
            st.markdown(
                '<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:8px;'
                'padding:10px 14px;font-size:0.875rem;color:#16a34a;font-weight:600;margin-bottom:16px;">'
                '✅ All pipeline nodes are healthy</div>',
                unsafe_allow_html=True
            )

    st.markdown("---")

    if os.path.exists(LINEAGE_PATH):
        with open(LINEAGE_PATH, "r") as f:
            html_content = f.read()
        st.components.v1.html(html_content, height=750, scrolling=False)
    else:
        st.warning("⚠️ Lineage graph not generated yet.")
        st.info("Run: `python src/lineage/lineage_graph.py` to generate it.")


# ══════════════════════════════════════════════════════════
# PAGE 6 — RUN PIPELINE
# ══════════════════════════════════════════════════════════

elif page == "▶️ Run Pipeline":

    st.markdown("""
    <div class="page-header">
        <div class="page-header-left">
            <h1>Run Pipeline</h1>
            <p>Trigger monitoring runs directly from the dashboard. Results update in real time.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, arr1, col2, arr2, col3 = st.columns([5, 1, 5, 1, 5])

    with col1:
        st.markdown("""
        <div class="pipe-card">
            <div class="pipe-step">Step 1</div>
            <div class="pipe-icon">📐</div>
            <div class="pipe-title">Schema Monitor</div>
            <div class="pipe-desc">Compares current table schemas against the saved baseline to detect column additions, drops, and type changes.</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Run Schema Monitor", use_container_width=True):
            with st.spinner("Running..."):
                result = api_post("/api/run/schema-monitor")
            if result:
                (st.warning if result["count"] > 0 else st.success)(f"{'⚠️' if result['count'] > 0 else '✅'} {result['message']}")

    with arr1:
        st.markdown('<div class="pipe-arrow">→</div>', unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="pipe-card">
            <div class="pipe-step">Step 2</div>
            <div class="pipe-icon">📊</div>
            <div class="pipe-title">Anomaly Detector</div>
            <div class="pipe-desc">Uses Z-score analysis on 30-day history to flag statistical anomalies in row counts, null rates, and distributions.</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Run Anomaly Detector", use_container_width=True):
            with st.spinner("Running..."):
                result = api_post("/api/run/anomaly-detector")
            if result:
                (st.warning if result["count"] > 0 else st.success)(f"{'⚠️' if result['count'] > 0 else '✅'} {result['message']}")

    with arr2:
        st.markdown('<div class="pipe-arrow">→</div>', unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="pipe-card">
            <div class="pipe-step">Step 3 · AI</div>
            <div class="pipe-icon">🤖</div>
            <div class="pipe-title">Full Pipeline</div>
            <div class="pipe-desc">Runs detection, calls Claude AI to generate root-cause analysis and fix recommendations, then sends Slack alerts.</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Run Full Pipeline", type="primary", use_container_width=True):
            with st.spinner("Calling Claude API..."):
                result = api_post("/api/run/full-pipeline")
            if result:
                if result["count"] > 0:
                    st.success(f"✅ {result['message']}")
                    st.balloons()
                else:
                    st.info(f"ℹ️ {result['message']}")

    st.markdown("---")
    st.markdown(
        '<div class="section-title">Demo: Inject & Detect</div>',
        unsafe_allow_html=True
    )
    st.markdown('<p style="color:#64748b;font-size:0.85rem;margin-bottom:10px;">Run these commands locally to simulate an anomaly, then click Run Full Pipeline above.</p>', unsafe_allow_html=True)
    st.code("""# Inject anomaly then run pipeline
python tests/inject_anomaly.py --scenario null_spike
python src/llm/root_cause_analyzer.py
python src/lineage/lineage_graph.py

# Then click Run Full Pipeline above to see results in the UI
# Reset when done:
python tests/inject_anomaly.py --reset
""", language="bash")
