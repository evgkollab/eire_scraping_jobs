import logging
import os
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def setup_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--log-level=3")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--mute-audio")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-extensions")
    opts.binary_location = os.getenv("GOOGLE_CHROME_BIN", "/usr/bin/chromium")

    driver = webdriver.Chrome(
        service=Service(os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")),
        options=opts,
    )
    driver.set_page_load_timeout(60)
    return driver


def flush_to_bq(
    records: List[Dict],
    table_id: str,
    client: bigquery.Client,
    date_columns: Optional[List[str]] = None,
    type_casts: Optional[Dict[str, str]] = None,
):
    """
    Uploads records to a BigQuery table with optional date parsing and type casting.

    Args:
        records: List of dictionaries to insert.
        table_id: Full BigQuery table ID.
        client: BigQuery client instance.
        date_columns: List of column names to convert to datetime.
        type_casts: Dict of column names and their desired pandas dtypes (e.g. {"total_units": "Int64"}).
    """
    if not records:
        return

    df = pd.DataFrame(records)

    if date_columns:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    if type_casts:
        for col, dtype in type_casts.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logging.warning(f"Failed to cast column '{col}' to {dtype}: {e}")

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    logging.info(f"Wrote {len(df)} rows to BigQuery.")

    records.clear()
