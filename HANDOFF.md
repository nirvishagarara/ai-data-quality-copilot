# AI-Powered Data Quality Copilot — Context Handoff Document
## For continuing this project in VSCode with Claude

---

## 1. What This Project Is

An AI-powered data observability tool that monitors data pipelines, detects anomalies, and uses Claude API to explain root causes in plain English. Think "ChatGPT for broken data pipelines."

**One-line pitch for Claude:** "I'm building a data quality monitoring tool in Python. It uses DuckDB as a local warehouse, detects schema drift and statistical anomalies, then calls the Anthropic Claude API to generate plain-English root-cause explanations and fix suggestions. It also sends Slack alerts and renders an interactive lineage graph."

---

## 2. Current Build Status

```
✅ Phase 1  — Environment + synthetic data warehouse (COMPLETE)
✅ Phase 2.1 — Schema monitor (COMPLETE)
✅ Phase 2.2 — Anomaly detector (COMPLETE)
✅ Phase 2.3 — Auto test generator (COMPLETE)
✅ Phase 3  — LLM root-cause engine / Claude API (COMPLETE)
✅ Phase 4.1 — Slack alerts (COMPLETE)
✅ Phase 4.2 — Lineage graph (COMPLETE)
✅ Phase 5  — FastAPI backend + Streamlit dashboard (COMPLETE — bug fixed)
⬜ Phase 6  — Integration tests + deployment
```

---

## 3. Project Structure (complete)

```
data-quality-copilot/
├── data/
│   ├── generate_data.py              ← one-time warehouse setup script
│   ├── warehouse.duckdb              ← DuckDB database (gitignored)
│   ├── warehouse_backup.duckdb       ← backup created by inject_anomaly.py
│   ├── generated_tests.yaml          ← dbt-compatible tests
│   ├── generated_tests.json          ← tests in JSON for API/frontend
│   ├── root_cause_reports.json       ← LLM-generated reports
│   ├── lineage_graph.html            ← interactive lineage graph
│   └── snapshots/
│       ├── schema_baseline.csv       ← column names + DuckDB types per table
│       ├── schema_history.csv        ← all detected drift events (appended)
│       ├── anomaly_history.csv       ← all detected anomalies (appended)
│       ├── customers_daily.csv       ← daily metrics baseline
│       ├── products_daily.csv
│       ├── orders_daily.csv
│       ├── order_items_daily.csv
│       ├── payments_daily.csv
│       └── events_daily.csv
├── src/
│   ├── __init__.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── schema_monitor.py         ← COMPLETE
│   │   ├── anomaly_detector.py       ← COMPLETE
│   │   └── test_generator.py         ← COMPLETE
│   ├── llm/
│   │   ├── __init__.py
│   │   └── root_cause_analyzer.py    ← COMPLETE
│   ├── alerts/
│   │   ├── __init__.py
│   │   └── slack_alerts.py           ← COMPLETE
│   ├── lineage/
│   │   ├── __init__.py
│   │   └── lineage_graph.py          ← COMPLETE
│   └── api/
│       ├── __init__.py
│       └── main.py                   ← COMPLETE but has one open bug (see Section 7)
├── frontend/
│   └── app.py                        ← COMPLETE but depends on API bug fix
├── tests/
│   └── inject_anomaly.py             ← COMPLETE — demo + testing utility
├── architecture.svg                  ← project architecture diagram
├── README.md                         ← COMPLETE
├── .env                              ← API keys (never commit)
├── .gitignore
└── requirements.txt
```

---

## 4. Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.9.1 |
| Data warehouse | DuckDB | latest |
| Data processing | Pandas, NumPy | latest |
| Anomaly detection | Scipy (Z-score) | latest |
| LLM | Anthropic Claude API | claude-haiku-4-5 |
| Alerts | Slack Incoming Webhooks | free tier |
| Lineage graph | NetworkX + Pyvis | latest |
| API | FastAPI + Uvicorn | latest |
| Dashboard | Streamlit | latest |
| Testing | Pytest | latest |

---

## 5. Environment Setup

```bash
# Activate virtual environment (required every new terminal)
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt
```

**.env file (project root):**
```
ANTHROPIC_API_KEY=your_key_here
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
```

**requirements.txt (current clean version):**
```
duckdb
pandas
numpy
faker
scikit-learn
scipy
anthropic
python-dotenv
requests
networkx
pyvis
fastapi
uvicorn
streamlit
pytest
jupyter
```

---

## 6. How to Run Everything

### Start the full stack (requires 2 terminals):

**Terminal 1 — API server:**
```bash
source venv/bin/activate
uvicorn src.api.main:app --reload --port 8000
```

**Terminal 2 — Streamlit dashboard:**
```bash
source venv/bin/activate
streamlit run frontend/app.py
```

Dashboard: http://localhost:8501
API docs: http://localhost:8000/docs

### Run individual components:
```bash
# Generate warehouse (run once only)
python data/generate_data.py

# Schema monitor
python src/monitoring/schema_monitor.py

# Anomaly detector
python src/monitoring/anomaly_detector.py

# Test generator
python src/monitoring/test_generator.py

# LLM root-cause engine
python src/llm/root_cause_analyzer.py

# Slack alerts
python src/alerts/slack_alerts.py

# Lineage graph
python src/lineage/lineage_graph.py
open data/lineage_graph.html
```

### Demo workflow (inject → detect → explain → alert):
```bash
python tests/inject_anomaly.py --scenario null_spike
python src/llm/root_cause_analyzer.py
python src/alerts/slack_alerts.py
python src/lineage/lineage_graph.py
python tests/inject_anomaly.py --reset
```

### Available anomaly scenarios:
```bash
python tests/inject_anomaly.py --list

--scenario schema_drift        # renames orders.order_status → orders.status
--scenario null_spike          # 45% of payments.amount → NULL
--scenario row_drop            # deletes 35% of orders randomly
--scenario distribution_shift  # triples order_total for 40% of orders
--scenario new_bad_values      # injects unknown product categories
--scenario duplicate_rows      # duplicates 20% of order_items
--reset                        # restores clean warehouse from backup
```

---

## 7. Bug — NaN JSON Serialization (RESOLVED)

**File:** `src/api/main.py`
**Endpoint:** `GET /api/tables/{table_name}/schema`
**Error:** `ValueError: Out of range float values are not JSON compliant`
**Root cause:** DuckDB's `DESCRIBE` and CSV files can return `NaN` values that FastAPI's JSON encoder cannot serialize.

**Fix applied** to all three `.to_dict()` call sites in `main.py`:
- Line 110: `load_csv()` helper — used by `/api/anomalies` and `/api/summary`
- Line 156: `get_table_schema()` — the originally reported bug
- Line 163: drift history in `get_table_schema()`

All now use `json.loads(df.fillna("").to_json(orient="records"))` instead of `.to_dict(orient="records")`.

**Status: RESOLVED.**

---

## 8. Key Technical Decisions & Lessons Learned

### Schema Monitor
- Baseline must be saved using DuckDB type names (VARCHAR, DOUBLE, BIGINT), NOT Pandas dtype names (object, float64, int64). Mixing them causes 49 false positive drift events.
- Fix applied in `generate_data.py`: `save_schema_snapshot()` now runs `DESCRIBE {table}` via DuckDB connection to get native type names.

### Anomaly Detector
- Z-score detection fails when historical std is 0 (metric was always stable — same value every day). Added absolute threshold fallback:
  - `null_pct`: fire CRITICAL if jumps above 5% from zero baseline
  - `mean`: fire HIGH/CRITICAL if shifts more than 20%/50% from baseline
  - `row_count`: fire HIGH/CRITICAL if drops more than 15%/30% from baseline
- Daily snapshots must store TOTAL table metrics (not per-day slices) to give Z-score a stable baseline.
- Python 3.9 does not support `X | Y` union type hints — use no return type annotation instead.

### LLM Root-Cause Engine
- Model used: `claude-haiku-4-5` (cheapest, fastest — each call costs < $0.001)
- System prompt instructs Claude to return ONLY a JSON object with fields: `explanation`, `root_cause`, `fixes`, `severity`
- Multi-context prompt: passes anomaly data + related schema drift on the same table → gives Claude enough context to make specific root-cause guesses
- Handles schema-drift-only events (no statistical anomaly) by converting drift events into fake Anomaly objects
- Always strip markdown code fences from LLM response before JSON parsing

### Slack Alerts
- Use a single section block instead of multiple blocks to avoid Slack's "See more / See less" double-button UI bug
- Reads from `data/root_cause_reports.json` — run `root_cause_analyzer.py` first

### Lineage Graph
- Built with NetworkX + Pyvis, saved as standalone HTML
- Legend and status bar injected by string-replacing `<body>` tag in generated HTML
- Anomaly node colors: CRITICAL=red, HIGH=orange, MEDIUM=yellow, healthy=blue/green/purple by type

### FastAPI
- Always run from project root: `uvicorn src.api.main:app --reload --port 8000`
- CORS middleware enabled so Streamlit (port 8501) can call API (port 8000)
- All `pd.DataFrame.to_dict()` calls need `.fillna("")` first to handle None/NaN serialization

---

## 9. Data Architecture

### 6 Tables in warehouse.duckdb:
| Table | Rows | Key columns |
|---|---|---|
| customers | 5,000 | customer_id, email, country, is_premium, age_bucket |
| products | 500 | product_id, category, unit_price, cost_price, is_active |
| orders | ~30,000 | order_id, customer_id, order_status, channel, order_total |
| order_items | ~60,000 | item_id, order_id, product_id, quantity, line_total |
| payments | ~25,000 | payment_id, order_id, payment_method, amount, status |
| events | 60,000 | event_id, session_id, customer_id, event_type, device_type |

### Important design notes:
- `orders.order_status` is intentionally named this way — the `schema_drift` scenario renames it to `status` to simulate a real upstream API change
- Date range: 2024-01-01 to 2024-03-31 (90 days) — enough for Z-score baseline
- SEED = 42 — fully reproducible data
- `warehouse_backup.duckdb` is created automatically on first `inject_anomaly.py` run

---

## 10. What's Left to Build (Phase 6)

After fixing the open bug in Section 7, these are the remaining items:

### Phase 6 — Integration Tests
Suggested test structure:
```
tests/
├── inject_anomaly.py              ← already built
├── unit/
│   ├── test_schema_monitor.py     ← test compare_schemas() in isolation
│   ├── test_anomaly_detector.py   ← test Z-score logic in isolation
│   └── test_test_generator.py     ← test profile_table() logic
└── integration/
    ├── test_full_pipeline.py      ← inject → detect → report → verify → reset
    └── test_slack_alert.py        ← mock Slack webhook, verify payload format
```

### Phase 6 — Deployment (free options)
- **Streamlit Cloud** (recommended): push to GitHub → share.streamlit.io → connect repo → add secrets
- **Hugging Face Spaces**: supports Streamlit natively, free tier

### Nice-to-have extensions:
- dbt integration — parse dbt manifest.json to auto-build lineage graph
- BigQuery/Snowflake connector — swap DuckDB for real cloud warehouse
- Anomaly feedback loop — thumbs up/down on LLM explanations
- APScheduler — run monitoring on a cron schedule without Airflow
- API key auth on FastAPI endpoints

---

## 11. Resume Bullet (ready to use)

> Built an AI-powered data observability platform monitoring 6 tables daily across schema drift, statistical anomalies (Z-score + absolute thresholds), and data quality tests; integrated Claude API with multi-context prompt engineering to generate plain-English root-cause explanations and fix suggestions; deployed Slack alerting and an interactive lineage graph with real-time anomaly highlighting; built FastAPI backend with 11 REST endpoints and Streamlit dashboard with 6 pages.

---

## 12. How to Brief Claude in VSCode

Paste this at the start of your first message in VSCode:

---

*"I'm continuing a portfolio project called AI-Powered Data Quality Copilot. It's a Python data observability tool with: DuckDB warehouse (6 tables, ~120k rows), schema drift detection, Z-score anomaly detection, auto test generation (93 dbt-compatible tests), Claude API root-cause explanations, Slack alerts, NetworkX/Pyvis lineage graph, FastAPI backend (11 endpoints), and Streamlit dashboard (6 pages). Python 3.9.1, running locally on Mac.*

*I have a context handoff doc with full project structure, all technical decisions, open bugs, and what's left to build. The one open bug is: `GET /api/tables/{name}/schema` returns 500 due to NaN serialization — fix is to use `json.loads(schema_df.fillna("").to_json(orient="records"))` instead of `.to_dict()`. I'm ready to fix that and then move to Phase 6 integration tests."*

---

## 13. Git State

Commit before switching to VSCode:
```bash
git add .
git commit -m "feat: fastapi + streamlit dashboard complete (bug fix pending)"
git push
```
