"""
config.py
=========
Central configuration loader for the Data Quality Copilot.

Reads from dq_config.yaml (or path set via DQ_CONFIG_PATH env var).
Falls back to sensible defaults matching the built-in demo warehouse,
so the tool works out of the box without any config file.

Usage:
    from src.config import DB_PATH, TABLES, SNAPSHOTS_DIR, ...
"""

import os
import copy

import yaml

# ─── Defaults ────────────────────────────────────────────────────────────────
# These match the built-in e-commerce demo so everything works without a config.

DEFAULT_CONFIG = {
    "database": {
        "path": "data/warehouse.duckdb",
    },
    "tables": [
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "events",
    ],
    "snapshots_dir": "data/snapshots",
    "outputs": {
        "reports": "data/root_cause_reports.json",
        "tests_json": "data/generated_tests.json",
        "tests_yaml": "data/generated_tests.yaml",
        "lineage_html": "data/lineage_graph.html",
    },
    "llm": {
        "model": "claude-haiku-4-5",
        "max_tokens": 600,
    },
    "anomaly_detection": {
        "zscore_threshold": 3.0,
        "pct_change_threshold": 0.20,
        "min_history_days": 7,
    },
    "test_generation": {
        "null_tolerance": 0.01,
        "unique_tolerance": 0.99,
        "max_cardinality": 20,
        "row_count_buffer": 0.20,
        "value_buffer": 0.25,
    },
    "lineage": {
        "edges": [
            {"source": "customers", "target": "orders", "label": "customer_id"},
            {"source": "products", "target": "order_items", "label": "product_id"},
            {"source": "orders", "target": "order_items", "label": "order_id"},
            {"source": "orders", "target": "payments", "label": "order_id"},
            {"source": "order_items", "target": "orders", "label": "aggregates to"},
            {"source": "customers", "target": "events", "label": "customer_id"},
            {"source": "orders", "target": "revenue_report", "label": "feeds"},
            {"source": "payments", "target": "revenue_report", "label": "feeds"},
            {"source": "order_items", "target": "revenue_report", "label": "feeds"},
            {"source": "events", "target": "behaviour_report", "label": "feeds"},
        ],
    },
}


# ─── Deep merge ──────────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Override values win."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


# ─── Loader ──────────────────────────────────────────────────────────────────

def load_config() -> dict:
    """Load config from YAML file, falling back to defaults."""
    config_path = os.environ.get("DQ_CONFIG_PATH", "dq_config.yaml")
    if os.path.exists(config_path):
        with open(config_path) as f:
            user_config = yaml.safe_load(f) or {}
        return _deep_merge(DEFAULT_CONFIG, user_config)
    return copy.deepcopy(DEFAULT_CONFIG)


# ─── Module-level exports ────────────────────────────────────────────────────
# Import these directly: from src.config import DB_PATH, TABLES, ...

_cfg = load_config()

# Database
DB_PATH = _cfg["database"]["path"]

# Tables to monitor
TABLES = _cfg["tables"]

# Snapshot / output paths
SNAPSHOTS_DIR = _cfg["snapshots_dir"]
REPORTS_PATH  = _cfg["outputs"]["reports"]
TESTS_JSON    = _cfg["outputs"]["tests_json"]
TESTS_YAML    = _cfg["outputs"]["tests_yaml"]
LINEAGE_PATH  = _cfg["outputs"]["lineage_html"]

# Derived paths
ANOMALY_PATH  = os.path.join(SNAPSHOTS_DIR, "anomaly_history.csv")
SCHEMA_PATH   = os.path.join(SNAPSHOTS_DIR, "schema_history.csv")
BASELINE_PATH = os.path.join(SNAPSHOTS_DIR, "schema_baseline.csv")

# LLM settings
LLM_MODEL      = _cfg["llm"]["model"]
LLM_MAX_TOKENS = _cfg["llm"]["max_tokens"]

# Anomaly detection thresholds
ZSCORE_THRESHOLD     = _cfg["anomaly_detection"]["zscore_threshold"]
PCT_CHANGE_THRESHOLD = _cfg["anomaly_detection"]["pct_change_threshold"]
MIN_HISTORY_DAYS     = _cfg["anomaly_detection"]["min_history_days"]

# Test generation thresholds
NULL_TOLERANCE    = _cfg["test_generation"]["null_tolerance"]
UNIQUE_TOLERANCE  = _cfg["test_generation"]["unique_tolerance"]
MAX_CARDINALITY   = _cfg["test_generation"]["max_cardinality"]
ROW_COUNT_BUFFER  = _cfg["test_generation"]["row_count_buffer"]
VALUE_BUFFER      = _cfg["test_generation"]["value_buffer"]

# Lineage
PIPELINE_EDGES = _cfg["lineage"]["edges"]
