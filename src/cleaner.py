

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import pandas as pd


def _parse_numeric(value: Optional[str]) -> Optional[float]:
    """Convert Yahoo-formatted numeric strings into floats."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"--", "N/A"}:
        return None
    text = text.replace(",", "")
    multiplier = 1.0
    suffix_map = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
    if text[-1] in suffix_map:
        multiplier = suffix_map[text[-1]]
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _parse_integer(value: Optional[str]) -> Optional[int]:
    """Convert a numeric string that may include suffixes into an integer."""
    num = _parse_numeric(value)
    return int(num) if num is not None else None


def _parse_percent(value: Optional[str]) -> Optional[float]:
    """Convert percent strings (e.g., '+1.23%') to floats."""
    if value is None:
        return None
    text = value.replace("%", "").strip()
    return _parse_numeric(text)


def _parse_range(value: Optional[str]) -> Dict[str, Optional[float]]:
    """Split the 52 week range column into low/high floats."""
    if not value:
        return {"fifty_two_wk_low": None, "fifty_two_wk_high": None}
    parts = value.replace("–", "-").replace("—", "-").split()
    if len(parts) >= 2:
        low = _parse_numeric(parts[0])
        high = _parse_numeric(parts[-1])
        return {"fifty_two_wk_low": low, "fifty_two_wk_high": high}
    if "-" in value:
        low_str, high_str = value.split("-", 1)
        return {
            "fifty_two_wk_low": _parse_numeric(low_str),
            "fifty_two_wk_high": _parse_numeric(high_str),
        }
    return {"fifty_two_wk_low": None, "fifty_two_wk_high": None}


class MostActiveCleaner:
    """Normalize raw Yahoo Finance rows into a structured dataset."""

    def clean(self, records: Iterable[Dict[str, Optional[str]]]) -> List[Dict]:
        """Clean raw records and ensure minimum dataset size."""
        df = pd.DataFrame(list(records))
        if df.empty:
            raise ValueError("Cleaner received no records.")

        df["symbol"] = df["symbol"].fillna("").str.upper().str.strip()
        df["name"] = df["name"].fillna("").str.strip()
        df = df[df["symbol"] != ""]
        df = df.drop_duplicates(subset=["symbol"])

        df["price"] = df["price"].apply(_parse_numeric)
        df["change"] = df["change"].apply(_parse_numeric)
        df["percent_change"] = df["percent_change"].apply(_parse_percent)
        df["volume"] = df["volume"].apply(_parse_integer)
        df["avg_volume"] = df["avg_volume"].apply(_parse_integer)
        df["market_cap"] = df["market_cap"].apply(_parse_numeric)
        df["pe_ratio"] = df["pe_ratio"].apply(_parse_numeric)

        ranges = df["fifty_two_wk_range"].apply(_parse_range).apply(pd.Series)
        df = pd.concat([df, ranges], axis=1)
        df = df.drop(columns=["fifty_two_wk_range"])

        df = df[df["price"].notna()]
        df = df.reset_index(drop=True)
        if len(df) < 100:
            raise RuntimeError(
                f"Cleaner produced only {len(df)} rows; expected at least 100."
            )

        timestamp = datetime.now(timezone.utc).isoformat()
        df["scraped_at"] = timestamp
        logging.info("Cleaned dataset rows: %s", len(df))

        return df.to_dict(orient="records")

    
def clean_records(records: Iterable[Dict[str, Optional[str]]]) -> List[Dict]:
    """Convenience wrapper around MostActiveCleaner."""
    cleaner = MostActiveCleaner()
    return cleaner.clean(records)


if __name__ == "__main__":
    raise SystemExit(
        "This module is not meant to be executed directly. Use src.scraper first."
    )






