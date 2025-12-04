

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable, List, Mapping

DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "output.db"


class SQLiteLoader:
    """Persist cleaned Yahoo data into SQLite."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, records: Iterable[Mapping]) -> None:
        """Insert cleaned records into SQLite, replacing previous data."""
        rows: List[Mapping] = list(records)
        if not rows:
            raise ValueError("Loader received no records to insert.")

        with sqlite3.connect(self.db_path) as conn:
            self._ensure_table(conn)
            conn.execute("DELETE FROM most_active")
            insert_sql = """
                INSERT INTO most_active (
                    symbol,
                    name,
                    price,
                    change,
                    percent_change,
                    volume,
                    avg_volume,
                    market_cap,
                    pe_ratio,
                    fifty_two_wk_low,
                    fifty_two_wk_high,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            conn.executemany(
                insert_sql,
                [
                    (
                        row.get("symbol"),
                        row.get("name"),
                        row.get("price"),
                        row.get("change"),
                        row.get("percent_change"),
                        row.get("volume"),
                        row.get("avg_volume"),
                        row.get("market_cap"),
                        row.get("pe_ratio"),
                        row.get("fifty_two_wk_low"),
                        row.get("fifty_two_wk_high"),
                        row.get("scraped_at"),
                    )
                    for row in rows
                ],
            )
            conn.commit()
            logging.info("Inserted %s rows into %s.", len(rows), self.db_path)

    @staticmethod
    def _ensure_table(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS most_active (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE,
                name TEXT,
                price REAL,
                change REAL,
                percent_change REAL,
                volume INTEGER,
                avg_volume INTEGER,
                market_cap REAL,
                pe_ratio REAL,
                fifty_two_wk_low REAL,
                fifty_two_wk_high REAL,
                scraped_at TEXT
            )
            """
        )


def load_to_sqlite(records: Iterable[Mapping], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Convenience wrapper."""
    loader = SQLiteLoader(db_path=db_path)
    loader.load(records)


if __name__ == "__main__":
    raise SystemExit("Run this module via Airflow or a pipeline script.")







