"""
lineage_graph.py
================
Builds an interactive pipeline lineage graph showing how data flows
between tables, and highlights any tables with active anomalies or
schema drift in red.

Output:
  - data/lineage_graph.html   (interactive — open in browser)

Usage:
    # Clean view
    python src/lineage/lineage_graph.py

    # With anomalies highlighted
    python tests/inject_anomaly.py --scenario null_spike
    python src/llm/root_cause_analyzer.py
    python src/lineage/lineage_graph.py
    # Open data/lineage_graph.html in your browser
    python tests/inject_anomaly.py --reset
"""

import os
import sys
import json

import networkx as nx
from pyvis.network import Network

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config import REPORTS_PATH, SCHEMA_PATH, LINEAGE_PATH as OUTPUT_PATH, PIPELINE_EDGES as PIPELINE_EDGES_DICTS

# Convert dict-format edges from config to tuples for internal use
PIPELINE_EDGES = [
    (e["source"], e["target"], e["label"]) for e in PIPELINE_EDGES_DICTS
]


def _auto_discover_node_types(edges):
    """
    Auto-discovers node types from the edge list:
    - Source tables: only appear as sources, never as targets
    - Report/dashboard nodes: only appear as targets, never as sources
    - Fact tables: everything else
    """
    sources = set()
    targets = set()
    for src, dst, _ in edges:
        sources.add(src)
        targets.add(dst)

    source_only = sources - targets  # pure source tables
    target_only = targets - sources  # pure downstream (reports/dashboards)
    fact_tables = (sources & targets)  # appear on both sides

    return source_only, fact_tables, target_only


# ─── Load anomalous tables ────────────────────────────────────────────────────

def get_anomalous_tables() -> dict:
    """
    Reads the latest root cause reports and returns a dict of
    {table_name: severity} for any table with active issues.
    """
    anomalous = {}

    if not os.path.exists(REPORTS_PATH):
        return anomalous

    with open(REPORTS_PATH) as f:
        reports = json.load(f)

    for report in reports:
        table    = report.get("table")
        severity = report.get("severity", "LOW")
        # Keep the highest severity if a table has multiple issues
        if table:
            existing = anomalous.get(table, "LOW")
            priority = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
            if priority.index(severity) > priority.index(existing):
                anomalous[table] = severity
    return anomalous


# ─── Node styling ─────────────────────────────────────────────────────────────

def get_node_style(table: str, severity: str, source_tables: set, fact_tables: set, report_tables: set) -> dict:
    """Returns color and shape based on table type and anomaly status."""

    # Anomaly colors override everything
    if severity == "CRITICAL":
        return {"color": "#FF0000", "border": "#8B0000", "font_color": "white"}
    elif severity == "HIGH":
        return {"color": "#FF8C00", "border": "#CC5500", "font_color": "white"}
    elif severity == "MEDIUM":
        return {"color": "#FFD700", "border": "#B8860B", "font_color": "black"}

    # Default colors by table type
    if table in source_tables:
        return {"color": "#2E75B6", "border": "#1F4E79", "font_color": "white"}
    elif table in fact_tables:
        return {"color": "#107C10", "border": "#054005", "font_color": "white"}
    elif table in report_tables:
        return {"color": "#7030A0", "border": "#4B0082", "font_color": "white"}
    else:
        return {"color": "#595959", "border": "#333333", "font_color": "white"}


def get_node_shape(table: str, source_tables: set, report_tables: set) -> str:
    if table in source_tables:
        return "database"
    elif table in report_tables:
        return "diamond"
    else:
        return "box"


# ─── Graph builder ────────────────────────────────────────────────────────────

def build_lineage_graph(anomalous_tables: dict) -> Network:
    """
    Builds the NetworkX graph and converts it to a Pyvis interactive network.
    """
    G = nx.DiGraph()

    # Auto-discover node types from edges
    source_tables, fact_tables, report_tables = _auto_discover_node_types(PIPELINE_EDGES)

    # Add all nodes
    all_tables = set()
    for src, dst, _ in PIPELINE_EDGES:
        all_tables.add(src)
        all_tables.add(dst)

    for table in all_tables:
        G.add_node(table)

    # Add edges
    for src, dst, label in PIPELINE_EDGES:
        G.add_edge(src, dst, label=label)

    # Build Pyvis network
    net = Network(
        height="700px",
        width="100%",
        directed=True,
        bgcolor="#1a1a2e",      # dark background looks great in demos
        font_color="white",
    )

    # Add nodes with styling
    for table in G.nodes:
        severity  = anomalous_tables.get(table, "OK")
        style     = get_node_style(table, severity, source_tables, fact_tables, report_tables)
        shape     = get_node_shape(table, source_tables, report_tables)

        # Build tooltip
        status_line = f"⚠️ ANOMALY DETECTED — {severity}" if severity != "OK" else "✅ Healthy"
        tooltip = f"{table}\n\nStatus: {status_line}"

        net.add_node(
            table,
            label     = table,
            color     = {
                "background": style["color"],
                "border":     style["border"],
                "highlight":  {"background": "#FFD700", "border": "#B8860B"},
            },
            shape     = shape,
            title     = tooltip,
            font      = {"color": style["font_color"], "size": 14, "bold": True},
            size      = 30 if table in report_tables else 25,
            borderWidth = 3 if severity != "OK" else 1,
        )

    # Add edges with labels
    for src, dst, label in PIPELINE_EDGES:
        net.add_edge(
            src, dst,
            label     = label,
            color     = {"color": "#888888", "highlight": "#FFD700"},
            font      = {"size": 10, "color": "#AAAAAA"},
            arrows    = "to",
            smooth    = {"type": "curvedCW", "roundness": 0.2},
        )

    # Physics settings — makes the graph look clean and settled
    net.set_options("""
    {
      "physics": {
        "enabled": true,
        "stabilization": {"iterations": 200},
        "barnesHut": {
          "gravitationalConstant": -8000,
          "springLength": 180,
          "springConstant": 0.04
        }
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 100,
        "navigationButtons": true,
        "keyboard": true
      },
      "edges": {
        "smooth": {"type": "curvedCW", "roundness": 0.2}
      }
    }
    """)

    return net


# ─── Legend injector ─────────────────────────────────────────────────────────

def inject_legend(html_path: str, anomalous_tables: dict):
    """
    Injects a colour-coded legend into the generated HTML file.
    Pyvis doesn't support legends natively so we add it directly.
    """
    legend_html = """
    <div style="
        position: fixed; top: 20px; left: 20px; z-index: 9999;
        background: rgba(0,0,0,0.75); border-radius: 10px;
        padding: 14px 18px; color: white; font-family: Arial, sans-serif;
        font-size: 13px; border: 1px solid #444;
    ">
        <div style="font-weight:bold; margin-bottom:10px; font-size:14px;">
            🗺️ Pipeline Lineage
        </div>
        <div style="margin-bottom:6px;">
            <span style="background:#2E75B6;padding:2px 8px;border-radius:4px;">■</span>
            &nbsp;Source table
        </div>
        <div style="margin-bottom:6px;">
            <span style="background:#107C10;padding:2px 8px;border-radius:4px;">■</span>
            &nbsp;Fact table
        </div>
        <div style="margin-bottom:6px;">
            <span style="background:#7030A0;padding:2px 8px;border-radius:4px;">◆</span>
            &nbsp;Report / dashboard
        </div>
        <div style="border-top:1px solid #555;margin:8px 0;"></div>
        <div style="margin-bottom:6px;">
            <span style="background:#FF0000;padding:2px 8px;border-radius:4px;">■</span>
            &nbsp;CRITICAL anomaly
        </div>
        <div style="margin-bottom:6px;">
            <span style="background:#FF8C00;padding:2px 8px;border-radius:4px;">■</span>
            &nbsp;HIGH anomaly
        </div>
        <div style="margin-bottom:6px;">
            <span style="background:#FFD700;padding:2px 8px;border-radius:4px;">■</span>
            &nbsp;MEDIUM anomaly
        </div>
        <div style="border-top:1px solid #555;margin:8px 0;"></div>
        <div style="font-size:11px;color:#aaa;">Hover nodes for details</div>
    </div>
    """

    # Status bar at top
    if anomalous_tables:
        issues = ", ".join(
            f"{t} ({s})" for t, s in anomalous_tables.items()
        )
        status_bar = f"""
        <div style="
            position:fixed; top:0; left:0; right:0; z-index:9998;
            background:#8B0000; color:white; text-align:center;
            padding:8px; font-family:Arial,sans-serif; font-size:13px;
        ">
            ⚠️ Active issues detected: {issues}
        </div>
        """
    else:
        status_bar = """
        <div style="
            position:fixed; top:0; left:0; right:0; z-index:9998;
            background:#054005; color:white; text-align:center;
            padding:8px; font-family:Arial,sans-serif; font-size:13px;
        ">
            ✅ All pipelines healthy
        </div>
        """

    with open(html_path, "r") as f:
        html = f.read()

    html = html.replace("<body>", f"<body>{status_bar}{legend_html}", 1)

    with open(html_path, "w") as f:
        f.write(html)


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_lineage_graph():
    print("\n🗺️   Lineage Graph")
    print("=" * 55)

    anomalous = get_anomalous_tables()

    if anomalous:
        print(f"  ⚠️  Anomalous tables detected:")
        for table, severity in anomalous.items():
            print(f"     {table} → {severity}")
    else:
        print("  ✅ No active anomalies — all nodes will show healthy")

    print(f"\n  Building graph ...")
    net = build_lineage_graph(anomalous)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    net.save_graph(OUTPUT_PATH)

    inject_legend(OUTPUT_PATH, anomalous)

    print(f"  ✅ Graph saved → {OUTPUT_PATH}")
    print(f"\n  Open in browser:")
    print(f"     open {OUTPUT_PATH}        # Mac")
    print(f"     start {OUTPUT_PATH}       # Windows")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_lineage_graph()
