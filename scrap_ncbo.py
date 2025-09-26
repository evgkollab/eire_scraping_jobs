from datetime import datetime
from time import sleep
from urllib.parse import urljoin
import logging

import pandas as pd
from google.cloud import bigquery
from thefuzz import fuzz

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from helpers.utils import flush_to_bq, setup_driver

# ---- Config ----
client = bigquery.Client()
TABLE_ID = "eire-1746041472369.eireestate_dataset_extending.large_developments_ncbo"
WRITE_BATCH = 40
SEARCH_BASE = "https://www.nbco.localgov.ie/en/bcms/search?search_api_views_fulltext="
SITE_BASE = "https://www.nbco.localgov.ie/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def _safe_text(driver, by, selector, default="Not Found", timeout=5):
    try:
        el = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )
        return el.text.strip()
    except Exception:
        return default


def _parse_commencement_date(raw_date: str) -> str | None:
    if not raw_date or raw_date == "Not Found":
        return None
    try:
        # Example: "Wednesday, 13 November, 2019"
        dt = datetime.strptime(raw_date, "%A, %d %B, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        # Fallback – keep raw or return None
        return None


def scrape_detail_page(driver, details_link: str, row: pd.Series) -> dict:
    driver.get(details_link)

    # Authority & fuzzy match
    extracted_authority = _safe_text(
        driver,
        By.XPATH,
        "//div[contains(@class, 'field-name-field-notice-local-authority')]//div[contains(@class, 'field-item')]",
        default="",
    )
    expected = (row.get("planning_authority") or "").strip()
    match_score = (
        fuzz.ratio(extracted_authority.lower(), expected.lower())
        if extracted_authority and expected
        else 0
    )
    is_special_limerick = (
        extracted_authority == "Limerick City and County Council"
        and expected == "Limerick County Council"
    )
    matched = (match_score >= 90) or is_special_limerick

    # Always capture what we can; if no match mark 'no match'
    notice_type = _safe_text(
        driver,
        By.CSS_SELECTOR,
        ".field-name-field-notice-type .field-item",
        default="Not Found",
    )
    raw_date = _safe_text(
        driver,
        By.CSS_SELECTOR,
        ".field-name-field-commencement-date .date-display-single",
        default="Not Found",
    )
    commencement_date = _parse_commencement_date(raw_date)

    owner_company = _safe_text(
        driver, By.CSS_SELECTOR, ".field-name-field-owner-company .field-item"
    )
    development_location = _safe_text(
        driver, By.CSS_SELECTOR, ".field-name-field-development-location .field-item"
    )
    builder_name = _safe_text(
        driver, By.CSS_SELECTOR, ".field-name-field-builder-name .field-item"
    )
    notice_description = _safe_text(
        driver, By.CSS_SELECTOR, ".field-name-field-notice-description .field-item"
    )
    notice_name = _safe_text(
        driver, By.CSS_SELECTOR, "header.notice-section h1.notice-title"
    )
    planning_permission_number = _safe_text(
        driver,
        By.CSS_SELECTOR,
        ".field-name-field-plannning-permission-num .field-item",
    )

    record = {
        "unique_application_number": row.get("unique_application_number"),
        "planning_authority": expected,
        "extracted_authority": extracted_authority,
        "notice_type": notice_type if matched else "no match",
        "CommencementDate": commencement_date,
        "owner_company": owner_company if matched else "",
        "development_location": development_location if matched else "",
        "builder_name": builder_name if matched else "",
        "notice_description": notice_description if matched else "",
        "notice_name": notice_name if matched else "",
        "details_link": details_link,
        "planning_permission_number": planning_permission_number if matched else "",
    }
    if matched:
        logging.info(
            f"✅ Match ({match_score}%) for {row.get('unique_application_number')}: {extracted_authority}"
        )
    else:
        logging.info(
            f"❌ No match ({match_score}%) for {row.get('unique_application_number')}: found '{extracted_authority}' expected '{expected}'"
        )
    driver.back()
    return record


def run():
    query = """

   WITH already_scrapped AS (
        SELECT unique_application_number, notice_type
        FROM `eire-1746041472369.eireestate_dataset_extending.large_developments_ncbo`
      )
   SELECT DISTINCT
        unique_application_number,
        planning_authority,
        '' AS real_link
      FROM (
        SELECT
        	part AS unique_application_number
        	, planning_authority
        FROM `eire-1746041472369.eireestate_dataset_extending.large_developments_extended`,
             UNNEST(included_unique_application_numbers) AS part
        UNION ALL
        SELECT
        	part,
        	planning_authority
        FROM `eire-1746041472369.eireestate_dataset_extending.large_developments_extended`,
          UNNEST(included_linked_abp_unique_application_numbers)  AS part
      ) AS z
      WHERE unique_application_number  NOT IN (SELECT unique_application_number FROM already_scrapped)
        AND unique_application_number != ''
       )
    UNION ALL
    SELECT
        unique_application_number,
        planning_authority,
        ncbo_link
    FROM `eire-1746041472369.eireestate_dataset_staging.developments_detective`
    WHERE ncbo_link IS NOT NULL
      AND unique_application_number NOT IN (
        SELECT unique_application_number FROM `eire-1746041472369.eireestate_dataset_extending.large_developments_ncbo`
      )

    """
    df_plan_apps = client.query(query).to_dataframe()

    # Build search URLs
    def make_search_url(u):
        u = str(u or "")
        # replicate your slicing logic; ensure length
        return f"{SEARCH_BASE}{u[3:]}" if len(u) > 3 else f"{SEARCH_BASE}{u}"

    df_plan_apps["URL"] = df_plan_apps["unique_application_number"].apply(
        make_search_url
    )

    driver = setup_driver()
    buffer: list[dict] = []

    try:
        for _, row in df_plan_apps.iterrows():
            initial_url = row["URL"]
            real_link = (row.get("real_link") or "").strip()

            if real_link:
                # Use the provided link directly
                rec = scrape_detail_page(driver, real_link, row)
                buffer.append(rec)
                if len(buffer) >= WRITE_BATCH:
                    flush_to_bq(
                        buffer, TABLE_ID, client, date_columns=["CommencementDate"]
                    )
                continue

            logging.info(f"Initial URL: {initial_url}")
            driver.get(initial_url)

            while True:
                # Results on this page
                results = driver.find_elements(
                    By.CSS_SELECTOR, ".item-list ul .accordion-item"
                )
                if not results:
                    # No results at all → write a 'no match' row
                    buffer.append(
                        {
                            "unique_application_number": row[
                                "unique_application_number"
                            ],
                            "planning_authority": row["planning_authority"],
                            "extracted_authority": "",
                            "notice_type": "no match",
                            "CommencementDate": None,
                            "owner_company": "",
                            "development_location": "",
                            "builder_name": "",
                            "notice_description": "",
                            "notice_name": "",
                            "details_link": "",
                            "planning_permission_number": "",
                        }
                    )
                    if len(buffer) >= WRITE_BATCH:
                        flush_to_bq(
                            buffer, TABLE_ID, client, date_columns=["CommencementDate"]
                        )
                    break

                for el in results:
                    details_link = el.find_element(
                        By.CSS_SELECTOR, "a.btn-small"
                    ).get_attribute("href")
                    logging.info(f"Detail URL: {details_link}")
                    sleep(1)

                    # Scrape the detail page
                    rec = scrape_detail_page(driver, details_link, row)
                    buffer.append(rec)
                    if len(buffer) >= WRITE_BATCH:
                        flush_to_bq(
                            buffer, TABLE_ID, client, date_columns=["CommencementDate"]
                        )

                # Pagination
                try:
                    next_a = driver.find_element(By.CSS_SELECTOR, "li.pager-next a")
                    next_href = next_a.get_attribute("href")
                    next_url = urljoin(SITE_BASE, next_href)
                    logging.info(f"Going to next page: {next_url}")
                    driver.get(next_url)
                    sleep(2)
                except NoSuchElementException:
                    logging.info("No more pages left. Exiting loop.")
                    break
            logging.info("Closing windows")
            # Get the first window handle (the main one)
            main_handle = driver.window_handles[0]

            # Loop through all window handles and close those that are not the main one
            for handle in driver.window_handles:
                if handle != main_handle:
                    driver.switch_to.window(handle)
                    driver.close()

                    # Optionally, switch back to the main window (first tab)
            driver.switch_to.window(main_handle)
        # Final flush
        if buffer:
            flush_to_bq(buffer, TABLE_ID, client, date_columns=["CommencementDate"])

    finally:
        driver.quit()
