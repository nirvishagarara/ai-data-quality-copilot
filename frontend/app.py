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

# Make project root importable (src.monitoring.*, src.llm.*, etc.)
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

# ─── Design System ────────────────────────────────────────────────────────────

SEVERITY_COLORS = {
    "CRITICAL": "#ef233c",
    "HIGH":     "#f77f00",
    "MEDIUM":   "#fcbf49",
    "LOW":      "#06d6a0",
    "OK":       "#06d6a0",
}

SEVERITY_BG = {
    "CRITICAL": "rgba(239,35,60,0.12)",
    "HIGH":     "rgba(247,127,0,0.12)",
    "MEDIUM":   "rgba(252,191,73,0.12)",
    "LOW":      "rgba(6,214,160,0.12)",
    "OK":       "rgba(6,214,160,0.12)",
}

# ─── Global CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}

/* ── Main background ── */
.stApp {
    background: linear-gradient(135deg, #0a0e1a 0%, #0f1629 50%, #0a0e1a 100%);
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1117 0%, #161b27 100%);
    border-right: 1px solid rgba(99,102,241,0.2);
}
[data-testid="stSidebar"] .stRadio label {
    color: #94a3b8 !important;
    font-size: 14px;
    padding: 6px 0;
    transition: color 0.2s;
}
[data-testid="stSidebar"] .stRadio label:hover {
    color: #e2e8f0 !important;
}

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: rgba(15,22,41,0.7);
    border: 1px solid rgba(99,102,241,0.3);
    border-radius: 12px;
    padding: 20px;
    backdrop-filter: blur(10px);
    transition: border-color 0.3s, transform 0.2s;
}
[data-testid="metric-container"]:hover {
    border-color: rgba(99,102,241,0.7);
    transform: translateY(-2px);
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-weight: 700 !important;
    color: #e2e8f0 !important;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    color: #94a3b8 !important;
    font-size: 0.8rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size: 0.78rem !important;
}

/* ── Dataframes ── */
[data-testid="stDataFrame"] {
    border: 1px solid rgba(99,102,241,0.2);
    border-radius: 10px;
    overflow: hidden;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: rgba(15,22,41,0.5);
    border: 1px solid rgba(99,102,241,0.2) !important;
    border-radius: 10px !important;
    margin-bottom: 10px;
}
[data-testid="stExpander"]:hover {
    border-color: rgba(99,102,241,0.5) !important;
}

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, #4361ee, #7209b7) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    letter-spacing: 0.02em !important;
    transition: opacity 0.2s, transform 0.15s !important;
}
.stButton > button:hover {
    opacity: 0.85 !important;
    transform: translateY(-1px) !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #ef233c, #7209b7) !important;
}

/* ── Selectbox ── */
[data-testid="stSelectbox"] > div > div {
    background: rgba(15,22,41,0.8) !important;
    border: 1px solid rgba(99,102,241,0.3) !important;
    border-radius: 8px !important;
    color: #e2e8f0 !important;
}

/* ── Info/warning/error/success boxes ── */
[data-testid="stAlert"] {
    border-radius: 10px !important;
}

/* ── Divider ── */
hr {
    border-color: rgba(99,102,241,0.2) !important;
}

/* ── Custom components ── */
.page-header {
    background: linear-gradient(135deg, rgba(67,97,238,0.15), rgba(114,9,183,0.15));
    border: 1px solid rgba(99,102,241,0.3);
    border-radius: 14px;
    padding: 24px 28px;
    margin-bottom: 28px;
}
.page-header h1 {
    margin: 0 0 6px 0;
    font-size: 1.8rem;
    font-weight: 700;
    color: #e2e8f0;
}
.page-header p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.95rem;
}

.kpi-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 16px;
    margin-bottom: 24px;
}
.kpi-card {
    background: rgba(15,22,41,0.7);
    border: 1px solid rgba(99,102,241,0.25);
    border-radius: 14px;
    padding: 20px;
    text-align: center;
    backdrop-filter: blur(10px);
    transition: border-color 0.3s, transform 0.2s;
}
.kpi-card:hover {
    border-color: rgba(99,102,241,0.6);
    transform: translateY(-3px);
}
.kpi-icon { font-size: 1.8rem; margin-bottom: 8px; }
.kpi-value {
    font-size: 2.2rem;
    font-weight: 800;
    color: #e2e8f0;
    line-height: 1;
    margin-bottom: 6px;
    background: linear-gradient(135deg, #a5b4fc, #818cf8);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.kpi-value.critical { background: linear-gradient(135deg, #ef233c, #f77f00); -webkit-background-clip: text; background-clip: text; }
.kpi-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #64748b;
    font-weight: 600;
}

.table-health-card {
    display: flex;
    align-items: center;
    gap: 14px;
    background: rgba(15,22,41,0.6);
    border: 1px solid rgba(99,102,241,0.2);
    border-radius: 10px;
    padding: 14px 18px;
    margin-bottom: 10px;
    transition: border-color 0.2s, transform 0.15s;
}
.table-health-card:hover {
    border-color: rgba(99,102,241,0.5);
    transform: translateX(3px);
}
.table-health-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
    box-shadow: 0 0 8px currentColor;
}
.table-health-name {
    font-weight: 600;
    color: #e2e8f0;
    font-size: 0.95rem;
    flex: 1;
}
.table-health-meta {
    font-size: 0.8rem;
    color: #64748b;
}
.sev-badge {
    font-size: 0.7rem;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 20px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.report-card {
    background: rgba(15,22,41,0.7);
    border: 1px solid rgba(99,102,241,0.2);
    border-left-width: 4px;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 14px;
    backdrop-filter: blur(10px);
}
.report-card-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: #e2e8f0;
    margin-bottom: 6px;
}
.report-card-meta {
    font-size: 0.82rem;
    color: #64748b;
    margin-bottom: 14px;
}
.report-section-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #64748b;
    font-weight: 700;
    margin-bottom: 6px;
}
.report-text {
    font-size: 0.92rem;
    color: #cbd5e1;
    line-height: 1.6;
    background: rgba(0,0,0,0.2);
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 14px;
}
.fix-item {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 8px 0;
    border-bottom: 1px solid rgba(99,102,241,0.1);
    font-size: 0.9rem;
    color: #94a3b8;
}
.fix-num {
    background: linear-gradient(135deg, #4361ee, #7209b7);
    color: white;
    border-radius: 50%;
    width: 22px;
    height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.72rem;
    font-weight: 700;
    flex-shrink: 0;
    margin-top: 1px;
}
.detail-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(99,102,241,0.1);
    border: 1px solid rgba(99,102,241,0.25);
    border-radius: 20px;
    padding: 4px 12px;
    font-size: 0.8rem;
    color: #94a3b8;
    margin: 3px 4px 3px 0;
}
.detail-pill strong { color: #e2e8f0; }

.pipeline-card {
    background: rgba(15,22,41,0.7);
    border: 1px solid rgba(99,102,241,0.25);
    border-radius: 14px;
    padding: 24px;
    height: 100%;
    backdrop-filter: blur(10px);
    text-align: center;
}
.pipeline-card-icon { font-size: 2.4rem; margin-bottom: 12px; }
.pipeline-card-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #e2e8f0;
    margin-bottom: 6px;
}
.pipeline-card-desc {
    font-size: 0.85rem;
    color: #64748b;
    margin-bottom: 20px;
    line-height: 1.5;
}
.pipeline-arrow {
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.4rem;
    color: rgba(99,102,241,0.5);
    padding-top: 60px;
}

.section-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 16px;
    padding-bottom: 10px;
    border-bottom: 1px solid rgba(99,102,241,0.2);
}
.section-header h3 {
    margin: 0;
    font-size: 1rem;
    font-weight: 700;
    color: #e2e8f0;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.section-tag {
    background: rgba(99,102,241,0.15);
    border: 1px solid rgba(99,102,241,0.3);
    border-radius: 6px;
    padding: 2px 8px;
    font-size: 0.72rem;
    color: #818cf8;
    font-weight: 600;
}

.test-type-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
.sidebar-brand {
    background: linear-gradient(135deg, rgba(67,97,238,0.2), rgba(114,9,183,0.2));
    border: 1px solid rgba(99,102,241,0.3);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
    margin-bottom: 8px;
}
.sidebar-brand-title {
    font-size: 1rem;
    font-weight: 800;
    background: linear-gradient(135deg, #818cf8, #c084fc);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.sidebar-brand-sub {
    font-size: 0.7rem;
    color: #64748b;
    margin-top: 2px;
}
.status-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #06d6a0;
    box-shadow: 0 0 8px #06d6a0;
    margin-right: 6px;
    animation: pulse 2s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
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
    """Dispatch GET-style requests to direct Python implementations."""
    if path == "/":
        return {"status": "healthy", "service": "Data Quality Copilot", "version": "1.0.0"}

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
        return {
            "table":          table_name,
            "current_schema": current_schema,
            "drift_history":  drift_history,
            "drift_count":    len(drift_history),
        }

    if path == "/api/anomalies":
        anomalies = _load_csv(ANOMALY_PATH)
        return {
            "anomalies": anomalies,
            "total":     len(anomalies),
            "critical":  sum(1 for a in anomalies if a.get("severity") == "CRITICAL"),
            "high":      sum(1 for a in anomalies if a.get("severity") == "HIGH"),
            "medium":    sum(1 for a in anomalies if a.get("severity") == "MEDIUM"),
        }

    if path == "/api/reports":
        reports = _load_json(REPORTS_PATH)
        return {"reports": reports, "total": len(reports)}

    if path == "/api/tests":
        tests = _load_json(TESTS_JSON)
        by_table = {}
        for t in tests:
            by_table.setdefault(t.get("table", "unknown"), []).append(t)
        return {
            "tests":    tests,
            "total":    len(tests),
            "by_table": {t: len(v) for t, v in by_table.items()},
        }

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
        nodes = [
            {"id": n, "label": n, "severity": anomalous.get(n, "OK"), "healthy": n not in anomalous}
            for n in all_nodes
        ]
        return {"nodes": nodes, "edges": PIPELINE_EDGES, "anomalous_count": len(anomalous)}

    return None


# ─── Public API helpers ────────────────────────────────────────────────────────

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
            return {
                "status":  "success",
                "message": f"Schema monitor complete — {len(drifts)} drift event(s) detected",
                "count":   len(drifts),
                "ran_at":  datetime.utcnow().isoformat(),
            }

        if path == "/api/run/anomaly-detector":
            anomalies = run_anomaly_detector(verbose=False)
            return {
                "status":  "success",
                "message": f"Anomaly detector complete — {len(anomalies)} anomaly(s) detected",
                "count":   len(anomalies),
                "ran_at":  datetime.utcnow().isoformat(),
            }

        if path == "/api/run/full-pipeline":
            from src.llm.root_cause_analyzer import run_root_cause_analyzer
            from src.alerts.slack_alerts import run_slack_alerts
            reports = run_root_cause_analyzer()
            if reports:
                run_slack_alerts()
            return {
                "status":  "success",
                "message": f"Full pipeline complete — {len(reports)} report(s) generated",
                "count":   len(reports),
                "ran_at":  datetime.utcnow().isoformat(),
            }

        return None
    except Exception as e:
        st.error(f"Error: {e}")
        return None


# ─── Helper renderers ─────────────────────────────────────────────────────────

def sev_badge(severity):
    color = SEVERITY_COLORS.get(severity, "#888")
    bg    = SEVERITY_BG.get(severity, "rgba(136,136,136,0.1)")
    return (f'<span class="sev-badge" style="color:{color};background:{bg};'
            f'border:1px solid {color}40;">{severity}</span>')


def test_type_badge(test_type):
    colors = {
        "not_null":          ("#22d3ee", "rgba(34,211,238,0.12)"),
        "unique":            ("#818cf8", "rgba(129,140,248,0.12)"),
        "accepted_values":   ("#a78bfa", "rgba(167,139,250,0.12)"),
        "row_count_between": ("#34d399", "rgba(52,211,153,0.12)"),
        "value_between":     ("#fb923c", "rgba(251,146,60,0.12)"),
    }
    c, bg = colors.get(test_type, ("#94a3b8", "rgba(148,163,184,0.12)"))
    return (f'<span class="test-type-badge" style="color:{c};background:{bg};'
            f'border:1px solid {c}40;">{test_type.replace("_"," ")}</span>')


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <div style="font-size:1.8rem;margin-bottom:4px;">🔍</div>
        <div class="sidebar-brand-title">Data Quality Copilot</div>
        <div class="sidebar-brand-sub">AI-Powered Observability</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    page = st.radio(
        "Navigate",
        ["📊 Dashboard", "🚨 Anomalies", "📐 Schema Monitor",
         "🧪 Tests", "🗺️ Lineage Graph", "▶️ Run Pipeline"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown(
        '<span class="status-dot"></span><span style="font-size:0.82rem;color:#94a3b8;">Running standalone</span>',
        unsafe_allow_html=True
    )
    st.markdown("---")
    st.markdown(
        '<div style="font-size:0.72rem;color:#475569;text-align:center;line-height:1.8;">'
        'Python &nbsp;·&nbsp; DuckDB &nbsp;·&nbsp; Claude AI<br>'
        'Streamlit Cloud</div>',
        unsafe_allow_html=True
    )


# ══════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════

if page == "📊 Dashboard":

    st.markdown("""
    <div class="page-header">
        <h1>📊 Dashboard</h1>
        <p>Real-time overview of your data pipeline health powered by AI-driven monitoring.</p>
    </div>
    """, unsafe_allow_html=True)

    summary = api_get("/api/summary")
    if summary:
        c = summary.get("anomalies_critical", 0)
        crit_class = "critical" if c > 0 else ""
        st.markdown(f"""
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-icon">🗄️</div>
                <div class="kpi-value">{summary["tables_monitored"]}</div>
                <div class="kpi-label">Tables Monitored</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">🚨</div>
                <div class="kpi-value {crit_class}">{summary["anomalies_total"]}</div>
                <div class="kpi-label">Anomalies Detected</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">📐</div>
                <div class="kpi-value">{summary["schema_drift_events"]}</div>
                <div class="kpi-label">Schema Drift Events</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">🧪</div>
                <div class="kpi-value">{summary["tests_generated"]}</div>
                <div class="kpi-label">Tests Generated</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon">🤖</div>
                <div class="kpi-value">{summary["reports_generated"]}</div>
                <div class="kpi-label">LLM Reports</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.markdown('<div class="section-header"><h3>🗄️ Table Health</h3></div>', unsafe_allow_html=True)
        tables  = api_get("/api/tables")
        reports = api_get("/api/reports")

        if tables and reports:
            anomalous_map = {r["table"]: r["severity"] for r in reports.get("reports", [])}

            for t in tables.get("tables", []):
                name     = t["table"]
                severity = anomalous_map.get(name, "OK")
                color    = SEVERITY_COLORS.get(severity, "#06d6a0")
                badge    = "" if severity == "OK" else sev_badge(severity)
                st.markdown(f"""
                <div class="table-health-card">
                    <div class="table-health-dot" style="background:{color};color:{color};"></div>
                    <div class="table-health-name">{name}</div>
                    <div class="table-health-meta">{t['row_count']:,} rows &nbsp;·&nbsp; {t['col_count']} cols</div>
                    {badge}
                </div>
                """, unsafe_allow_html=True)

    with col_right:
        anomalies = api_get("/api/anomalies")

        # Severity breakdown mini-chart
        if anomalies and anomalies["total"] > 0:
            n_crit = anomalies.get("critical", 0)
            n_high = anomalies.get("high", 0)
            n_med  = anomalies.get("medium", 0)
            n_low  = anomalies["total"] - n_crit - n_high - n_med

            st.markdown('<div class="section-header"><h3>📊 Severity Breakdown</h3></div>', unsafe_allow_html=True)
            chart_df = pd.DataFrame({
                "Severity": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                "Count":    [n_crit, n_high, n_med, n_low],
            }).set_index("Severity")
            st.bar_chart(chart_df, color="#4361ee", height=180)

            st.markdown('<div class="section-header" style="margin-top:20px;"><h3>⚡ Recent Anomalies</h3></div>', unsafe_allow_html=True)
            df = pd.DataFrame(anomalies["anomalies"])
            recent = df.tail(6)[["table", "column", "metric", "severity", "pct_change"]].copy()
            recent["pct_change"] = recent["pct_change"].apply(lambda x: f"{float(x)*100:.1f}%")
            st.dataframe(recent, use_container_width=True, hide_index=True)
        else:
            st.markdown('<div class="section-header"><h3>⚡ Recent Anomalies</h3></div>', unsafe_allow_html=True)
            st.success("✅ All clear — no anomalies detected")


# ══════════════════════════════════════════════════════════
# PAGE 2 — ANOMALIES
# ══════════════════════════════════════════════════════════

elif page == "🚨 Anomalies":

    st.markdown("""
    <div class="page-header">
        <h1>🚨 Anomaly Explorer</h1>
        <p>All detected anomalies with AI-generated root-cause analysis and fix recommendations.</p>
    </div>
    """, unsafe_allow_html=True)

    reports = api_get("/api/reports")

    if reports and reports["reports"]:
        total = reports["total"]
        st.markdown(
            f'<p style="color:#64748b;margin-bottom:20px;">{total} report{"s" if total != 1 else ""} found</p>',
            unsafe_allow_html=True
        )

        for report in reports["reports"]:
            severity = report.get("severity", "LOW")
            color    = SEVERITY_COLORS.get(severity, "#888")
            bg       = SEVERITY_BG.get(severity, "rgba(136,136,136,0.1)")

            with st.expander(
                f"[{severity}]  {report['table']}.{report['column']} — {report['metric']}",
                expanded = severity in ["CRITICAL", "HIGH"]
            ):
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown(
                        f'<div class="report-section-label">📢 What happened</div>'
                        f'<div class="report-text">{report.get("explanation","N/A")}</div>',
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f'<div class="report-section-label">🔍 Root cause</div>'
                        f'<div class="report-text">{report.get("root_cause","N/A")}</div>',
                        unsafe_allow_html=True
                    )
                    fixes = report.get("fixes", [])
                    if fixes:
                        items = "".join(
                            f'<div class="fix-item"><div class="fix-num">{i}</div><div>{f}</div></div>'
                            for i, f in enumerate(fixes, 1)
                        )
                        st.markdown(
                            f'<div class="report-section-label">🔧 Suggested fixes</div>{items}',
                            unsafe_allow_html=True
                        )

                with col2:
                    detected = report.get("generated_at", "")[:19].replace("T", " ")
                    st.markdown(f"""
                    <div style="margin-top:4px;">
                        <div class="report-section-label">Details</div>
                        <div style="margin-top:10px;">
                            <div class="detail-pill"><strong>Table</strong> {report['table']}</div>
                            <div class="detail-pill"><strong>Column</strong> {report['column']}</div>
                            <div class="detail-pill"><strong>Metric</strong> {report['metric']}</div>
                            <div class="detail-pill" style="background:{bg};border-color:{color}40;">
                                <strong style="color:{color};">⬤</strong>&nbsp;{severity}
                            </div>
                            <div class="detail-pill"><strong>At</strong> {detected}</div>
                        </div>
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
        <h1>📐 Schema Monitor</h1>
        <p>Track column additions, deletions, and type changes across all tables over time.</p>
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
            drift_label = (
                f'<span class="sev-badge" style="color:#ef233c;background:rgba(239,35,60,0.12);border:1px solid rgba(239,35,60,0.3);">'
                f'{dc} drift event{"s" if dc != 1 else ""}</span>'
                if dc > 0 else
                f'<span class="sev-badge" style="color:#06d6a0;background:rgba(6,214,160,0.12);border:1px solid rgba(6,214,160,0.3);">No drift</span>'
            )

            col1, col2 = st.columns([1, 1], gap="large")

            with col1:
                st.markdown('<div class="section-header"><h3>Current Schema</h3></div>', unsafe_allow_html=True)
                df = pd.DataFrame(schema["current_schema"])
                if not df.empty:
                    display_cols = [c for c in ["column_name","column_type","null","key","default"] if c in df.columns]
                    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

            with col2:
                st.markdown(
                    f'<div class="section-header"><h3>Drift History</h3>&nbsp;{drift_label}</div>',
                    unsafe_allow_html=True
                )
                if schema["drift_history"]:
                    drift_df = pd.DataFrame(schema["drift_history"])
                    st.dataframe(drift_df, use_container_width=True, hide_index=True)
                else:
                    st.success("✅ No drift events recorded for this table")


# ══════════════════════════════════════════════════════════
# PAGE 4 — TESTS
# ══════════════════════════════════════════════════════════

elif page == "🧪 Tests":

    st.markdown("""
    <div class="page-header">
        <h1>🧪 Auto-Generated Tests</h1>
        <p>Data quality tests generated automatically from 30-day historical column profiles.</p>
    </div>
    """, unsafe_allow_html=True)

    tests = api_get("/api/tests")
    if tests:
        by_table = tests.get("by_table", {})
        cols     = st.columns(len(by_table))
        for i, (table, count) in enumerate(by_table.items()):
            cols[i].metric(table, count)

        st.markdown(
            f'<p style="color:#64748b;margin:12px 0 20px;">'
            f'<strong style="color:#818cf8;">{tests["total"]}</strong> tests generated across {len(by_table)} tables</p>',
            unsafe_allow_html=True
        )

        st.markdown("---")

        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            all_tests   = tests.get("tests", [])
            table_names = sorted(set(t["table"] for t in all_tests))
            selected    = st.selectbox("Filter by table", ["All"] + table_names)
        filtered = all_tests if selected == "All" else [t for t in all_tests if t["table"] == selected]

        with filter_col2:
            test_types    = sorted(set(t["test_type"] for t in filtered))
            selected_type = st.selectbox("Filter by test type", ["All"] + test_types)
        if selected_type != "All":
            filtered = [t for t in filtered if t["test_type"] == selected_type]

        st.markdown(
            f'<p style="color:#64748b;margin:12px 0 16px;">Showing <strong style="color:#e2e8f0;">{len(filtered)}</strong> tests</p>',
            unsafe_allow_html=True
        )

        icons = {
            "not_null":          "🚫",
            "unique":            "🔑",
            "accepted_values":   "📋",
            "row_count_between": "📊",
            "value_between":     "📏",
        }

        for test in filtered:
            icon = icons.get(test["test_type"], "🧪")
            with st.expander(f"{icon}  {test['plain_english']}"):
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown(
                        f'<div style="margin-bottom:10px;">{test_type_badge(test["test_type"])}</div>'
                        f'<div class="detail-pill"><strong>Table</strong> {test["table"]}</div>'
                        f'<div class="detail-pill"><strong>Column</strong> {test["column"]}</div>',
                        unsafe_allow_html=True
                    )
                with col2:
                    st.markdown(
                        f'<div class="report-section-label">Why generated</div>'
                        f'<div style="font-size:0.85rem;color:#94a3b8;">{test.get("reason","")}</div>',
                        unsafe_allow_html=True
                    )
                    if test.get("parameters"):
                        st.markdown(
                            f'<div class="report-section-label" style="margin-top:10px;">Parameters</div>'
                            f'<code style="font-size:0.8rem;color:#818cf8;">{test["parameters"]}</code>',
                            unsafe_allow_html=True
                        )


# ══════════════════════════════════════════════════════════
# PAGE 5 — LINEAGE GRAPH
# ══════════════════════════════════════════════════════════

elif page == "🗺️ Lineage Graph":

    st.markdown("""
    <div class="page-header">
        <h1>🗺️ Pipeline Lineage Graph</h1>
        <p>Interactive graph showing data flow between tables. Anomalous nodes are highlighted in red.</p>
    </div>
    """, unsafe_allow_html=True)

    lineage = api_get("/api/lineage")
    if lineage:
        if lineage["anomalous_count"] > 0:
            anomalous_nodes = [n for n in lineage["nodes"] if not n["healthy"]]
            for node in anomalous_nodes:
                color = SEVERITY_COLORS.get(node["severity"], "#888")
                st.markdown(
                    f'<div style="background:rgba(239,35,60,0.1);border:1px solid rgba(239,35,60,0.3);'
                    f'border-radius:8px;padding:10px 16px;margin-bottom:8px;font-size:0.9rem;color:#ef233c;">'
                    f'⚠️ <strong>{node["id"]}</strong> — active {sev_badge(node["severity"])} anomaly</div>',
                    unsafe_allow_html=True
                )
        else:
            st.markdown(
                '<div style="background:rgba(6,214,160,0.1);border:1px solid rgba(6,214,160,0.3);'
                'border-radius:8px;padding:10px 16px;margin-bottom:16px;font-size:0.9rem;color:#06d6a0;">'
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
        <h1>▶️ Run Pipeline</h1>
        <p>Trigger monitoring runs directly from the dashboard. Results update in real time.</p>
    </div>
    """, unsafe_allow_html=True)

    col1, arrow1, col2, arrow2, col3 = st.columns([5, 1, 5, 1, 5])

    with col1:
        st.markdown("""
        <div class="pipeline-card">
            <div class="pipeline-card-icon">📐</div>
            <div class="pipeline-card-title">Schema Monitor</div>
            <div class="pipeline-card-desc">Compares current table schemas against the baseline snapshot to detect column additions, drops, and type changes.</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("▶ Run Schema Monitor", use_container_width=True):
            with st.spinner("Running schema monitor..."):
                result = api_post("/api/run/schema-monitor")
            if result:
                if result["count"] > 0:
                    st.warning(f"⚠️ {result['message']}")
                else:
                    st.success(f"✅ {result['message']}")

    with arrow1:
        st.markdown('<div class="pipeline-arrow">→</div>', unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="pipeline-card">
            <div class="pipeline-card-icon">📊</div>
            <div class="pipeline-card-title">Anomaly Detector</div>
            <div class="pipeline-card-desc">Uses Z-score analysis on 30-day history to detect statistical anomalies in row counts, null rates, and value distributions.</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("▶ Run Anomaly Detector", use_container_width=True):
            with st.spinner("Running anomaly detector..."):
                result = api_post("/api/run/anomaly-detector")
            if result:
                if result["count"] > 0:
                    st.warning(f"⚠️ {result['message']}")
                else:
                    st.success(f"✅ {result['message']}")

    with arrow2:
        st.markdown('<div class="pipeline-arrow">→</div>', unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="pipeline-card">
            <div class="pipeline-card-icon">🤖</div>
            <div class="pipeline-card-title">Full Pipeline</div>
            <div class="pipeline-card-desc">Runs detection, calls Claude AI to generate root-cause analysis and fix recommendations, then sends Slack alerts.</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("▶ Run Full Pipeline", type="primary", use_container_width=True):
            with st.spinner("Running full pipeline… (calls Claude API)"):
                result = api_post("/api/run/full-pipeline")
            if result:
                if result["count"] > 0:
                    st.success(f"✅ {result['message']}")
                    st.balloons()
                else:
                    st.info(f"ℹ️ {result['message']}")

    st.markdown("---")
    st.markdown(
        '<div class="section-header"><h3>Demo: Inject & Detect</h3>'
        '<span class="section-tag">Terminal</span></div>',
        unsafe_allow_html=True
    )
    st.markdown('<p style="color:#64748b;font-size:0.85rem;margin-bottom:12px;">Run these commands locally to demo the full anomaly detection pipeline:</p>', unsafe_allow_html=True)
    st.code("""# Inject anomaly then run pipeline
python tests/inject_anomaly.py --scenario null_spike
python src/llm/root_cause_analyzer.py
python src/lineage/lineage_graph.py

# Then click ▶ Run Full Pipeline above to see results in the UI
# Reset when done:
python tests/inject_anomaly.py --reset
""", language="bash")
