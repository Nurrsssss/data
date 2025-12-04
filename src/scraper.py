

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

YAHOO_MOST_ACTIVE_URL = "https://finance.yahoo.com/most-active?offset=0&count=100"
HEADER_FIELD_MAPPING = {
    "symbol": "symbol",
    "name": "name",
    "price": "price",
    "price (intraday)": "price",
    "change": "change",
    "change %": "percent_change",
    "% change": "percent_change",
    "volume": "volume",
    "avg vol (3m)": "avg_volume",
    "avg vol (3 month)": "avg_volume",
    "market cap": "market_cap",
    "p/e ratio": "pe_ratio",
    "p/e ratio (ttm)": "pe_ratio",
    "pe ratio (ttm)": "pe_ratio",
    "52 wk range": "fifty_two_wk_range",
}


def _build_driver(headless: bool = True) -> webdriver.Chrome:
    """Initialize a Chrome WebDriver instance."""
    options = ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


@dataclass
class YahooMostActiveScraper:
    """Scraper that extracts raw rows from Yahoo Finance's Most Active table."""

    min_records: int = 100
    scroll_pause: float = 1.0
    max_scroll_attempts: int = 60
    headless: bool = True
    timeout: int = 30

    def run(self) -> List[Dict[str, Optional[str]]]:
        """Execute the scraping job and return raw row dictionaries."""
        driver = _build_driver(headless=self.headless)
        logging.info("Navigating to %s", YAHOO_MOST_ACTIVE_URL)
        try:
            driver.get(YAHOO_MOST_ACTIVE_URL)
            wait = WebDriverWait(driver, self.timeout)
            self._dismiss_consent(driver)
            table_body = self._get_table_body(driver, wait)
            logging.info("Most active table detected, starting scroll loop.")
            rows = self._collect_rows(driver, table_body)
            logging.info("Scraped %s rows from Yahoo Finance.", len(rows))
            if len(rows) < self.min_records:
                raise RuntimeError(
                    f"Expected at least {self.min_records} rows, got {len(rows)}."
                )
            return rows
        finally:
            driver.quit()

    def _collect_rows(
        self, driver: webdriver.Chrome, table_body
    ) -> List[Dict[str, Optional[str]]]:
        """Scroll through the table until enough rows are gathered."""
        headers = self._extract_headers(table_body)
        rows: List[Dict[str, Optional[str]]] = []
        last_height = 0
        attempts = 0

        while attempts < self.max_scroll_attempts:
            row_elements = table_body.find_elements(By.CSS_SELECTOR, "tr")
            rows = [
                self._parse_row(row, headers)
                for row in row_elements
                if row.is_displayed()
            ]
            logging.debug("Iteration %s captured %s rows.", attempts, len(rows))
            if len(rows) >= self.min_records:
                break

            try:
                driver.execute_script("arguments[0].scrollIntoView(false);", row_elements[-1])
            except WebDriverException:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
            time.sleep(self.scroll_pause)

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                attempts += 1
            else:
                attempts = 0
                last_height = new_height

        return [row for row in rows if row]

    def _get_table_body(self, driver: webdriver.Chrome, wait: WebDriverWait):
        """Locate the table body element using several fallback selectors."""
        selectors = [
            (By.CSS_SELECTOR, 'table[role="table"] tbody'),
            (By.CSS_SELECTOR, "table tbody"),
            (By.XPATH, "//table[contains(@class, 'W(100%)')]//tbody"),
        ]
        last_error: Optional[Exception] = None
        for by, selector in selectors:
            try:
                return wait.until(EC.presence_of_element_located((by, selector)))
            except TimeoutException as exc:
                last_error = exc
                logging.debug("Selector %s failed.", selector)
        raise TimeoutException("Unable to locate the most-active table.") from last_error

    def _parse_row(
        self, row, headers: List[str]
    ) -> Optional[Dict[str, Optional[str]]]:
        """Extract text values from a table row."""
        cells = row.find_elements(By.CSS_SELECTOR, "td")
        if not cells:
            return None

        record: Dict[str, Optional[str]] = {}
        for idx, cell in enumerate(cells):
            header = headers[idx] if idx < len(headers) else ""
            key = self._header_to_field(header)
            if not key:
                continue
            value = cell.text.strip()
            record[key] = value

        if not record.get("price"):
            logging.debug("Row missing price data, skipping.")
            return None
        return record

    def _extract_headers(self, table_body) -> List[str]:
        """Fetch table header labels."""
        try:
            table = table_body.find_element(By.XPATH, "..")
            header_cells = table.find_elements(By.CSS_SELECTOR, "thead th")
            headers = [cell.text.replace("\n", " ").strip() for cell in header_cells]
            logging.debug("Detected headers: %s", headers)
            return headers
        except WebDriverException as exc:
            logging.warning("Failed to extract headers: %s", exc)
            return []

    def _header_to_field(self, header: str) -> Optional[str]:
        normalized = header.lower().strip()
        return HEADER_FIELD_MAPPING.get(normalized)

    def _dismiss_consent(self, driver: webdriver.Chrome) -> None:
        """Attempt to dismiss Yahoo's consent modal if it appears."""
        try:
            time.sleep(1)
            buttons = driver.find_elements(
                By.XPATH,
                "//button[contains(translate(., 'ACEPTL', 'aceptl'), 'accept')]",
            )
            if buttons:
                buttons[0].click()
                logging.info("Consent dialog dismissed.")
                return

            # Attempt via iframe-based consent message
            WebDriverWait(driver, 5).until(
                EC.frame_to_be_available_and_switch_to_it(
                    (By.CSS_SELECTOR, "iframe[id^='sp_message_iframe']")
                )
            )
            iframe_buttons = driver.find_elements(By.CSS_SELECTOR, "button")
            for btn in iframe_buttons:
                if "accept" in btn.text.lower():
                    btn.click()
                    logging.info("Consent dialog dismissed via iframe.")
                    break
        except TimeoutException:
            logging.debug("Consent dialog not present.")
        finally:
            try:
                driver.switch_to.default_content()
            except WebDriverException:
                pass


def scrape_yahoo_most_active(min_records: int = 100) -> List[Dict[str, Optional[str]]]:
    """
    Convenience function used by scripts and tasks.

    Returns:
        List of dictionaries containing raw Yahoo table rows.
    """
    scraper = YahooMostActiveScraper(min_records=min_records)
    return scraper.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape_yahoo_most_active()
    logging.info("Scraped %s rows.", len(data))

