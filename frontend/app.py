"""
app.py
======
Streamlit dashboard for the Data Quality Copilot.

Pages:
  1. Dashboard    — KPI summary cards + recent anomalies
  2. Anomalies    — full anomaly explorer with LLM explanations
  3. Schema       — schema drift history per table
  4. Tests        — auto-generated data quality tests
  5. Lineage      — embedded interactive lineage graph
  6. Run Pipeline — trigger monitoring runs from the UI

Usage:
    # Make sure API server is running first:
    uvicorn src.api.main:app --reload --port 8000

    # Then in a new terminal:
    streamlit run frontend/app.py
"""

import json
import requests
import pandas as pd
import streamlit as st

# ─── Config ───────────────────────────────────────────────────────────────────

API_BASE    = "http://localhost:8000"
LINEAGE_PATH = "data/lineage_graph.html"

st.set_page_config(
    page_title = "Data Quality Copilot",
    page_icon  = "🔍",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Sidebar */
    .css-1d391kg { background-color: #0F1117; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background-color: #1a1a2e;
        border: 1px solid #2E75B6;
        border-radius: 8px;
        padding: 16px;
    }

    /* Severity badges */
    .badge-critical { background:#FF0000; color:white; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:bold; }
    .badge-high     { background:#FF8C00; color:white; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:bold; }
    .badge-medium   { background:#FFD700; color:black; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:bold; }
    .badge-low      { background:#107C10; color:white; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:bold; }
    .badge-ok       { background:#107C10; color:white; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:bold; }

    /* Report cards */
    .report-card {
        background: #1a1a2e;
        border-left: 4px solid #FF0000;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 16px;
    }
    .report-card-high   { border-left-color: #FF8C00; }
    .report-card-medium { border-left-color: #FFD700; }
    .report-card-low    { border-left-color: #107C10; }
</style>
""", unsafe_allow_html=True)


# ─── API helpers ──────────────────────────────────────────────────────────────

def api_get(path: str):
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to API server. Run: `uvicorn src.api.main:app --reload --port 8000`")
        return None
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_post(path: str):
    try:
        r = requests.post(f"{API_BASE}{path}", timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to API server.")
        return None
    except Exception as e:
        st.error(f"API error: {e}")
        return None


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔍 Data Quality Copilot")
    st.markdown("---")

    page = st.radio(
        "Navigate",
        ["📊 Dashboard", "🚨 Anomalies", "📐 Schema Monitor",
         "🧪 Tests", "🗺️ Lineage Graph", "▶️ Run Pipeline"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**API Status**")
    health = api_get("/")
    if health:
        st.success("✅ API connected")
    else:
        st.error("❌ API offline")

    st.markdown("---")
    st.caption("Built with Python · DuckDB · Claude AI")


# ══════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════

if page == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.markdown("Real-time overview of your data pipeline health.")
    st.markdown("---")

    summary = api_get("/api/summary")
    if summary:
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Tables Monitored",  summary["tables_monitored"])
        col2.metric("Anomalies Detected", summary["anomalies_total"],
                    delta=f"{summary['anomalies_critical']} critical",
                    delta_color="inverse")
        col3.metric("Schema Drift Events", summary["schema_drift_events"])
        col4.metric("Tests Generated",   summary["tests_generated"])
        col5.metric("LLM Reports",       summary["reports_generated"])

    st.markdown("---")

    col_left, col_right = st.columns([1, 1])

    # Table health
    with col_left:
        st.subheader("Table Health")
        tables = api_get("/api/tables")
        reports = api_get("/api/reports")

        if tables and reports:
            anomalous = {r["table"]: r["severity"] for r in reports.get("reports", [])}

            for t in tables.get("tables", []):
                name     = t["table"]
                severity = anomalous.get(name, "OK")
                icon     = {"CRITICAL":"🔴","HIGH":"🟠","MEDIUM":"🟡","LOW":"🟢","OK":"✅"}.get(severity,"✅")
                st.markdown(
                    f"{icon} **{name}** — {t['row_count']:,} rows · {t['col_count']} columns"
                    f"{'  `' + severity + '`' if severity != 'OK' else ''}"
                )

    # Recent anomalies
    with col_right:
        st.subheader("Recent Anomalies")
        anomalies = api_get("/api/anomalies")
        if anomalies and anomalies["anomalies"]:
            df = pd.DataFrame(anomalies["anomalies"])
            # Show last 8
            recent = df.tail(8)[["table","column","metric","severity","pct_change"]].copy()
            recent["pct_change"] = recent["pct_change"].apply(lambda x: f"{float(x)*100:.1f}%")
            st.dataframe(recent, use_container_width=True, hide_index=True)
        else:
            st.success("✅ No anomalies detected")


# ══════════════════════════════════════════════════════════
# PAGE 2 — ANOMALIES
# ══════════════════════════════════════════════════════════

elif page == "🚨 Anomalies":
    st.title("🚨 Anomaly Explorer")
    st.markdown("All detected anomalies with AI-generated root-cause explanations.")
    st.markdown("---")

    reports = api_get("/api/reports")

    if reports and reports["reports"]:
        for report in reports["reports"]:
            severity = report.get("severity", "LOW")
            icon     = {"CRITICAL":"🔴","HIGH":"🟠","MEDIUM":"🟡","LOW":"🟢"}.get(severity,"⚪")
            color    = {"CRITICAL":"#FF0000","HIGH":"#FF8C00","MEDIUM":"#FFD700","LOW":"#107C10"}.get(severity,"#888")

            with st.expander(
                f"{icon} [{severity}]  {report['table']}.{report['column']} — {report['metric']}",
                expanded = severity in ["CRITICAL", "HIGH"]
            ):
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.markdown("**📢 What happened**")
                    st.info(report.get("explanation", "N/A"))

                    st.markdown("**🔍 Root cause**")
                    st.warning(report.get("root_cause", "N/A"))

                    st.markdown("**🔧 Suggested fixes**")
                    for i, fix in enumerate(report.get("fixes", []), 1):
                        st.markdown(f"{i}. {fix}")

                with col2:
                    st.markdown("**Details**")
                    st.markdown(f"- **Table:** `{report['table']}`")
                    st.markdown(f"- **Column:** `{report['column']}`")
                    st.markdown(f"- **Metric:** `{report['metric']}`")
                    st.markdown(f"- **Severity:** `{severity}`")
                    detected = report.get("generated_at", "")[:19]
                    st.markdown(f"- **Detected:** `{detected}`")
    else:
        st.success("✅ No anomaly reports found. Run the pipeline to generate reports.")
        st.info("Go to **▶️ Run Pipeline** to trigger a monitoring run.")


# ══════════════════════════════════════════════════════════
# PAGE 3 — SCHEMA MONITOR
# ══════════════════════════════════════════════════════════

elif page == "📐 Schema Monitor":
    st.title("📐 Schema Monitor")
    st.markdown("Track schema changes across all tables.")
    st.markdown("---")

    tables = api_get("/api/tables")
    if not tables:
        st.stop()

    table_names = [t["table"] for t in tables["tables"]]
    selected    = st.selectbox("Select table", table_names)

    if selected:
        schema = api_get(f"/api/tables/{selected}/schema")
        if schema:
            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("Current Schema")
                df = pd.DataFrame(schema["current_schema"])
                if not df.empty:
                    display_cols = [c for c in ["column_name","column_type","null","key","default"] if c in df.columns]
                    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

            with col2:
                st.subheader(f"Drift History ({schema['drift_count']} events)")
                if schema["drift_history"]:
                    drift_df = pd.DataFrame(schema["drift_history"])
                    st.dataframe(drift_df, use_container_width=True, hide_index=True)
                else:
                    st.success("✅ No drift events recorded for this table")


# ══════════════════════════════════════════════════════════
# PAGE 4 — TESTS
# ══════════════════════════════════════════════════════════

elif page == "🧪 Tests":
    st.title("🧪 Auto-Generated Tests")
    st.markdown("Data quality tests generated automatically from 30-day historical profiles.")
    st.markdown("---")

    tests = api_get("/api/tests")
    if tests:
        # Summary by table
        st.subheader("Tests by Table")
        by_table = tests.get("by_table", {})
        cols     = st.columns(len(by_table))
        for i, (table, count) in enumerate(by_table.items()):
            cols[i].metric(table, count)

        st.markdown(f"**Total: {tests['total']} tests generated**")
        st.markdown("---")

        # Filter by table
        all_tests   = tests.get("tests", [])
        table_names = sorted(set(t["table"] for t in all_tests))
        selected    = st.selectbox("Filter by table", ["All"] + table_names)

        filtered = all_tests if selected == "All" else [t for t in all_tests if t["table"] == selected]

        # Filter by test type
        test_types = sorted(set(t["test_type"] for t in filtered))
        selected_type = st.selectbox("Filter by test type", ["All"] + test_types)
        if selected_type != "All":
            filtered = [t for t in filtered if t["test_type"] == selected_type]

        st.markdown(f"Showing **{len(filtered)}** tests")
        st.markdown("---")

        for test in filtered:
            icon = {
                "not_null":         "🚫",
                "unique":           "🔑",
                "accepted_values":  "📋",
                "row_count_between":"📊",
                "value_between":    "📏",
            }.get(test["test_type"], "🧪")

            with st.expander(f"{icon} {test['plain_english']}"):
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown(f"**Test type:** `{test['test_type']}`")
                    st.markdown(f"**Table:** `{test['table']}`")
                    st.markdown(f"**Column:** `{test['column']}`")
                with col2:
                    st.markdown(f"**Why generated:**")
                    st.caption(test.get("reason", ""))
                    if test.get("parameters"):
                        st.markdown(f"**Parameters:** `{test['parameters']}`")


# ══════════════════════════════════════════════════════════
# PAGE 5 — LINEAGE GRAPH
# ══════════════════════════════════════════════════════════

elif page == "🗺️ Lineage Graph":
    st.title("🗺️ Pipeline Lineage Graph")
    st.markdown("Interactive graph showing data flow between tables. Anomalous nodes are highlighted.")
    st.markdown("---")

    # Show anomaly status
    lineage = api_get("/api/lineage")
    if lineage:
        if lineage["anomalous_count"] > 0:
            anomalous = [n for n in lineage["nodes"] if not n["healthy"]]
            for node in anomalous:
                st.error(f"⚠️ **{node['id']}** has an active {node['severity']} anomaly")
        else:
            st.success("✅ All pipeline nodes are healthy")

    st.markdown("---")

    # Embed the lineage graph HTML
    import os
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
    st.title("▶️ Run Pipeline")
    st.markdown("Trigger monitoring runs directly from the dashboard.")
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Schema Monitor")
        st.caption("Detects column drift against baseline")
        if st.button("▶ Run Schema Monitor", use_container_width=True):
            with st.spinner("Running schema monitor..."):
                result = api_post("/api/run/schema-monitor")
            if result:
                if result["count"] > 0:
                    st.warning(f"⚠️ {result['message']}")
                else:
                    st.success(f"✅ {result['message']}")

    with col2:
        st.subheader("Anomaly Detector")
        st.caption("Detects statistical anomalies")
        if st.button("▶ Run Anomaly Detector", use_container_width=True):
            with st.spinner("Running anomaly detector..."):
                result = api_post("/api/run/anomaly-detector")
            if result:
                if result["count"] > 0:
                    st.warning(f"⚠️ {result['message']}")
                else:
                    st.success(f"✅ {result['message']}")

    with col3:
        st.subheader("Full Pipeline")
        st.caption("Detect → Explain with AI → Alert Slack")
        if st.button("▶ Run Full Pipeline", type="primary", use_container_width=True):
            with st.spinner("Running full pipeline... (this calls Claude API)"):
                result = api_post("/api/run/full-pipeline")
            if result:
                if result["count"] > 0:
                    st.success(f"✅ {result['message']}")
                    st.balloons()
                else:
                    st.info(f"ℹ️ {result['message']}")

    st.markdown("---")
    st.subheader("Demo: Inject & Detect")
    st.markdown("Run these commands in your terminal to demo the full pipeline:")

    st.code("""# Terminal 1 — keep API running
uvicorn src.api.main:app --reload --port 8000

# Terminal 2 — inject anomaly then run pipeline
python tests/inject_anomaly.py --scenario null_spike
python src/llm/root_cause_analyzer.py
python src/lineage/lineage_graph.py

# Then click ▶ Run Full Pipeline above to see results in the UI
# Reset when done:
python tests/inject_anomaly.py --reset
""", language="bash")
