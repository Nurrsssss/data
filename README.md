## Yahoo Finance Most Active Pipeline

This project automates collection of Yahoo Finance’s **Most Active** equities list, cleans the resulting dataset, persists it to SQLite, and orchestrates the workflow with Apache Airflow.

### Project Layout

```
projectdata/
├── airflow_dag.py          # Airflow DAG definition (also treated as module entry point)
├── data/output.db          # SQLite database created by the loader
├── src/
│   ├── scraper.py          # Selenium scraper
│   ├── cleaner.py          # Pandas-based cleaning utilities
│   └── loader.py           # SQLite loader
├── requirements.txt
└── README.md
```

### 1. Environment Setup

1. **Python**: use Python 3.10 .
2. Create and activate a virtual environment:
   ```bash
   cd /Users/nursultantolegen/projectdata
   python3.10 -m venv .venv
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

### 2. Standalone ETL Run (outside Airflow)

```bash
source .venv/bin/activate
python - <<'PY'
from src.scraper import scrape_yahoo_most_active
from src.cleaner import clean_records
from src.loader import SQLiteLoader

raw = scrape_yahoo_most_active()
cleaned = clean_records(raw)
SQLiteLoader().load(cleaned)
print(f"Inserted {len(cleaned)} rows into data/output.db")
PY
```

The loader overwrites the `most_active` table on each run. The schema:

| Column              | Type   | Notes                          |
|---------------------|--------|--------------------------------|
| symbol              | TEXT   | Stock ticker (unique)          |
| name                | TEXT   | Company name                   |
| price               | REAL   | Latest intraday price          |
| change              | REAL   | Absolute price change          |
| percent_change      | REAL   | Daily % change                 |
| volume              | INTEGER| Current session volume         |
| avg_volume          | INTEGER| 3‑month average volume         |
| market_cap          | REAL   | Market capitalization (USD)    |
| pe_ratio            | REAL   | Trailing P/E ratio             |
| fifty_two_wk_low    | REAL   | 52-week low                    |
| fifty_two_wk_high   | REAL   | 52-week high                   |
| scraped_at          | TEXT   | ISO timestamp (UTC)            |

### 3. Airflow Orchestration

1. **Set Airflow paths** (one-time):
   ```bash
   export AIRFLOW_HOME=/Users/nursultantolegen/projectdata/airflow_home
   export AIRFLOW__CORE__DAGS_FOLDER=/Users/nursultantolegen/projectdata/dags
   mkdir -p "$AIRFLOW_HOME"
   ```
2. **Initialize metadata DB**:
   ```bash
   source .venv/bin/activate
   airflow db init
   ```
3. **Trigger a DAG run (single execution for verification)**:
   ```bash
   airflow dags test yahoo_most_active_pipeline 2025-12-04
   ```
   Logs are stored under `airflow_home/logs/dag_id=yahoo_most_active_pipeline/`.
   A captured example run is available at `data/airflow_test_run.log`.
4. **(Optional) Scheduler/Webserver**:
   ```bash
   airflow webserver
   airflow scheduler
   ```

The DAG executes three Python tasks (scrape → clean → load), each with retries, logging, and local JSON staging between tasks to keep XCom payloads small.

### 4. Operational Notes

- Selenium runs Chrome in headless mode and interacts with Yahoo Finance’s dynamic table (auto-scroll + query parameters to request 100 rows per page).
- Cleaning enforces at least 100 deduplicated rows and normalizes all numeric fields.
- `data/output.db` always reflects the latest successful pipeline execution; archive or copy the DB before reruns if historical snapshots are required.
- `.airflowignore` excludes `airflow_home/` so that Airflow can treat the project root as the DAG folder without infinite recursion warnings.

### 5. Troubleshooting

- **Chromedriver mismatch**: delete `~/.wdm` to force webdriver-manager to download a fresh driver aligned with your Chrome version.
- **Airflow package errors**: ensure you are using the provided `requirements.txt` (includes `pendulum==2.1.2`, `Flask-Session`, and `connexion` which Airflow expects).
- **Headless failures or consent modals**: rerun the scraper; it automatically retries dismissing Yahoo’s consent dialog before scraping.


