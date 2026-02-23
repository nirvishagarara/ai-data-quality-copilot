# Data Quality Copilot

**AI-powered data observability for DuckDB.** Monitor any warehouse for schema drift, statistical anomalies, and data quality issues — then let Claude AI explain what broke and how to fix it.

---

## What It Does

- **Schema drift detection** — catches renamed, dropped, or type-changed columns
- **Anomaly detection** — Z-score based monitoring on null %, row count, mean, and std dev
- **Auto test generation** — generates dbt-compatible data quality tests from 30-day profiles
- **LLM root-cause analysis** — sends anomaly context to Claude AI for plain-English explanations
- **Slack alerts** — colour-coded severity alerts with fix suggestions
- **Lineage graph** — interactive HTML graph with anomalous nodes highlighted
- **Streamlit dashboard** — full UI for exploring all of the above

---

## Quick Start

### 1. Install

```bash
git clone https://github.com/nirvishagarara/ai-data-quality-copilot
cd ai-data-quality-copilot

python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env → add your ANTHROPIC_API_KEY
```

Edit `dq_config.yaml` to point at your DuckDB database and list your tables:

```yaml
database:
  path: path/to/your/warehouse.duckdb

tables:
  - users
  - orders
  - payments
```

### 3. Run

```bash
python dq_copilot.py scan          # detect schema drift + anomalies
python dq_copilot.py explain       # LLM root-cause analysis
python dq_copilot.py dashboard     # launch Streamlit UI
```

---

## Try With Demo Data

Don't have a DuckDB warehouse yet? Generate a synthetic e-commerce one:

```bash
python data/generate_data.py       # creates warehouse + 90 days of baselines
python dq_copilot.py full          # run the entire pipeline
python dq_copilot.py dashboard     # explore results in the UI
```

### Inject Anomalies (for testing)

```bash
python tests/inject_anomaly.py --list                    # see all scenarios
python tests/inject_anomaly.py --scenario null_spike     # inject a failure
python dq_copilot.py explain                             # watch it get caught
python tests/inject_anomaly.py --reset                   # restore clean data
```

Available scenarios: `schema_drift`, `null_spike`, `row_drop`, `distribution_shift`, `new_bad_values`, `duplicate_rows`

---

## Example LLM Output

```
🔴 [CRITICAL] payments.amount — null_pct

📢 What happened:
   The payments.amount column is unexpectedly null in 44.85% of records,
   a dramatic shift from the historical baseline of 0% nulls.

🔍 Root cause:
   Payment processing pipeline is inserting records with NULL amounts,
   likely due to upstream data source failure or a recent deployment
   that removed amount validation before insert.

🔧 Suggested fixes:
   1. Query payments table for records inserted in last 24 hours with
      NULL amount and cross-reference transaction logs
   2. Review recent git commits for logic changes affecting amount
      column population
   3. Add NOT NULL constraint to payments.amount and implement
      pre-insertion validation
```

---

## CLI Commands

| Command | Description |
|---|---|
| `python dq_copilot.py init` | Create a starter `dq_config.yaml` |
| `python dq_copilot.py scan` | Run schema monitor + anomaly detector |
| `python dq_copilot.py explain` | Run LLM root-cause analyzer |
| `python dq_copilot.py test` | Generate data quality tests |
| `python dq_copilot.py alert` | Send Slack alerts |
| `python dq_copilot.py lineage` | Build interactive lineage graph |
| `python dq_copilot.py dashboard` | Launch Streamlit dashboard |
| `python dq_copilot.py full` | Run entire pipeline end-to-end |

---

## Configuration

All settings live in `dq_config.yaml`:

| Section | Default | Description |
|---|---|---|
| `database.path` | `data/warehouse.duckdb` | Path to your DuckDB file |
| `tables` | (demo tables) | List of table names to monitor |
| `llm.model` | `claude-haiku-4-5` | Claude model to use |
| `llm.max_tokens` | `600` | Max tokens per LLM response |
| `anomaly_detection.zscore_threshold` | `3.0` | Flag metrics > N std devs |
| `anomaly_detection.pct_change_threshold` | `0.20` | Flag metrics changed > 20% |
| `anomaly_detection.min_history_days` | `7` | Min days of history needed |
| `lineage.edges` | (demo edges) | Pipeline dependency graph |

See [`dq_config.yaml`](dq_config.yaml) for the full list with comments.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | For `explain` command | Claude API key ([get one](https://console.anthropic.com)) |
| `SLACK_WEBHOOK_URL` | For `alert` command | Slack incoming webhook URL |

---

## How It Works

```
1. Schema monitor compares live schema to saved baseline → flags drift
2. Anomaly detector runs Z-score on daily metrics → flags statistical anomalies
3. Root-cause analyzer sends anomaly + drift context to Claude API
4. Claude returns structured JSON: explanation + root_cause + fixes + severity
5. Slack alerts sends formatted messages to your channel
6. Lineage graph renders interactive HTML with anomalous nodes in red
7. Test generator profiles tables and outputs dbt-compatible YAML tests
```

---

## Project Structure

```
data-quality-copilot/
├── dq_copilot.py               # CLI entry point
├── dq_config.yaml              # configuration (edit this)
├── data/
│   ├── generate_data.py        # synthetic warehouse generator (demo)
│   └── snapshots/              # daily metric baselines
├── src/
│   ├── config.py               # central config loader
│   ├── monitoring/
│   │   ├── schema_monitor.py   # schema drift detection
│   │   ├── anomaly_detector.py # statistical anomaly detection
│   │   └── test_generator.py   # auto test generation
│   ├── llm/
│   │   └── root_cause_analyzer.py  # Claude AI root-cause engine
│   ├── alerts/
│   │   └── slack_alerts.py     # Slack webhook alerts
│   └── lineage/
│       └── lineage_graph.py    # interactive pipeline graph
├── frontend/
│   └── app.py                  # Streamlit dashboard
├── tests/
│   └── inject_anomaly.py       # anomaly injection for testing
├── .env.example                # environment variable template
├── requirements.txt
└── LICENSE                     # MIT
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data warehouse | DuckDB |
| Data processing | Python, Pandas, NumPy |
| Anomaly detection | Z-score (SciPy), custom thresholds |
| LLM | Anthropic Claude API |
| Alerts | Slack Incoming Webhooks |
| Lineage graph | NetworkX, Pyvis |
| Dashboard | Streamlit |
| Testing | Pytest |

---

## License

MIT — see [LICENSE](LICENSE).
