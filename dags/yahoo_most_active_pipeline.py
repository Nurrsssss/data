from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

# Добавляем корень проекта в sys.path, чтобы импортировать src
PROJECT_ROOT = Path(__file__).resolve().parents[0]
SRC_PATH = PROJECT_ROOT.parent / "src"
DATA_DIR = PROJECT_ROOT.parent / "data"
RAW_PATH = DATA_DIR / "raw_most_active.json"
CLEAN_PATH = DATA_DIR / "clean_most_active.json"

if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="yahoo_most_active_pipeline",
    description="Scrape, clean, and load Yahoo Most Active stocks every day.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 12, 3),
    catchup=False,
    max_active_runs=1,
    tags=["selenium", "yahoo", "etl"],
) as dag:

    @task(task_id="scrape", retries=3, retry_delay=timedelta(minutes=5))
    def scrape_task() -> str:
        from scraper import scrape_yahoo_most_active

        DATA_DIR.mkdir(parents=True, exist_ok=True)

        raw_data = scrape_yahoo_most_active()
        logging.info("Scrape task captured %s rows.", len(raw_data))
        with RAW_PATH.open("w", encoding="utf-8") as fp:
            json.dump(raw_data, fp)
        return str(RAW_PATH)

    @task(task_id="clean")
    def clean_task(raw_path: str) -> str:
        from cleaner import clean_records

        with Path(raw_path).open("r", encoding="utf-8") as fp:
            raw_data = json.load(fp)

        cleaned = clean_records(raw_data)
        logging.info("Clean task produced %s rows.", len(cleaned))
        with CLEAN_PATH.open("w", encoding="utf-8") as fp:
            json.dump(cleaned, fp)
        return str(CLEAN_PATH)

    @task(task_id="load")
    def load_task(cleaned_path: str) -> str:
        from loader import SQLiteLoader

        with Path(cleaned_path).open("r", encoding="utf-8") as fp:
            cleaned_data = json.load(fp)

        loader = SQLiteLoader()
        loader.load(cleaned_data)
        message = f"Inserted {len(cleaned_data)} rows."
        logging.info(message)
        return message

    load_task(clean_task(scrape_task()))
