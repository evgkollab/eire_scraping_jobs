import os, logging, pandas as pd
from time import sleep
from dotenv import load_dotenv
from google.cloud import bigquery
from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
)
from datetime import datetime
import socket
from requests.exceptions import ReadTimeout

from helpers.utils import flush_to_bq, setup_driver


# ──────────────────────────────  Helpers  ──────────────────────────────
def wait_for_element(driver, xpath, condition="presence", timeout=10):
    waits = {
        "presence": EC.presence_of_element_located,
        "presence_elements": EC.presence_of_all_elements_located,
        "clickable": EC.element_to_be_clickable,
        "visibility": EC.visibility_of_element_located,
    }
    if condition not in waits:
        raise ValueError("Invalid condition")
    return WebDriverWait(driver, timeout).until(waits[condition]((By.XPATH, xpath)))


def format_date(val):
    if pd.isna(val) or val == "":
        return ""
    try:
        return pd.to_datetime(val, dayfirst=True).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logging.warning(f"Failed to parse date '{val}': {e}")
        return ""


def get_property_value(driver, element, prop, timeout=5):
    try:
        el = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, f"//{element}[@name='{prop}']"))
        )
        return el.get_attribute("value")
    except Exception as e:
        logging.warning(
            f"get_property_value error for {prop}: {e}, current page {(driver.current_url,)}"
        )
        return None


# ──────────────────────────────  Cookie banner  ────────────────────────
def accept_cookies(driver):
    try:
        wait_for_element(
            driver, '//button[@ng-click="$consent.agree()"]', timeout=10
        ).click()
        logging.info("Cookies accepted.")
    except TimeoutException:
        try:
            agree = wait_for_element(driver, '//input[@id="chkAgree"]', timeout=10)
            if not agree.is_selected():
                agree.click()
            wait_for_element(driver, '//input[@id="btnViewFiles"]', timeout=10).click()
            logging.info("Agreed & viewed files.")
        except TimeoutException:
            logging.info("Cookie/agree banner not found.")


# ──────────────────────────────  Scraper core  ─────────────────────────
def retrieve_all_properties(driver, planning_authority):
    mapping = {
        "reference": ("input", "reference"),
        "decision_date": ("input", "decisionDate"),
        "appeal_decision_date": ("input", "appealDecisionDate"),
        "appeal_decision": ("input", "appealDecision"),
        "appeal_type": ("input", "appealType"),
        "final_grant_date": ("input", "finalGrantDate"),
        "application_type": ("input", "applicationType"),
        "full_proposal": ("textarea", "fullProposal"),
        # "status_owner": ("input", "statusOwner"),
        "applicant": ("input", "applicantSurname"),
    }

    mapping["received_date"] = ("input", "registrationDate")

    if planning_authority == "South Dublin County Council":
        mapping["status_non_owner"] = ("input", "statusOwner")
    else:
        mapping["status_non_owner"] = ("input", "statusNonOwner")

    data = {k: get_property_value(driver, *v) for k, v in mapping.items()}
    try:
        dec = (
            WebDriverWait(driver, 5)
            .until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//span[contains(@class,'stat-desc-span') and @ng-bind-html='propValue']",
                    )
                )
            )
            .text.strip()
        )
        data["decision"] = dec
    except Exception:
        data["decision"] = None
    return data


def retrieve_all_properties_wex(driver):
    def safe_get_text(xpath, timeout=5, extract_after_colon=False):
        try:
            el = WebDriverWait(driver, timeout).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            text = el.text.strip()
            return text.split(":", 1)[-1].strip() if extract_after_colon else text
        except Exception:
            return ""

    data = {
        "decision_date": format_date(
            safe_get_text(
                "//th[contains(text(), 'Decision Date:')]/following-sibling::td[1]"
            )
        ),
        "appeal_decision_date": "",  # not available on this layout
        "appeal_decision": "",  # not available on this layout
        "final_grant_date": "",  # not available on this layout
        "application_type": safe_get_text(
            "//th[contains(text(), 'Application Type:')]", extract_after_colon=True
        ),
        "appeal_type": "",  # not available
        "applicant": safe_get_text(
            "//th[contains(text(), 'Applicant Name:')]/following-sibling::td[1]"
        ),
        "status_non_owner": safe_get_text(
            "//th[contains(text(), 'Decision Stage:')]/following-sibling::td[1]"
        ),
        "status_owner": "",  # not present in this format
    }

    # Handle decision value
    data["decision"] = safe_get_text(
        "//th[contains(text(), 'Decision:')]", extract_after_colon=True
    )

    # Proposal extraction
    proposal = safe_get_text(
        "//th[contains(text(), 'Proposal:')]/following-sibling::td[1]"
    )
    if not proposal:
        try:
            iframe = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//td[iframe]/iframe"))
            )
            driver.switch_to.frame(iframe)
            try:
                proposal = (
                    WebDriverWait(driver, 5)
                    .until(EC.visibility_of_element_located((By.XPATH, "//p")))
                    .text
                )
            finally:
                driver.switch_to.default_content()
        except Exception:
            proposal = ""
    data["full_proposal"] = proposal

    return data


def retrieve_all_properties_others(driver):
    def extract_text(xpath, timeout=10):
        try:
            return (
                WebDriverWait(driver, timeout)
                .until(EC.visibility_of_element_located((By.XPATH, xpath)))
                .text.strip()
            )
        except Exception:
            return ""

    data = {}

    # ─── Application Tab (default loaded) ─────────────────────────────
    data["application_type"] = extract_text(
        "//th[contains(text(), 'Application Type')]/following-sibling::td[1]"
    )
    data["status_non_owner"] = extract_text(
        "//th[contains(text(), 'Planning Status')]/following-sibling::td[1]"
    )
    data["received_date"] = format_date(
        extract_text(
            "//th[contains(text(), 'Received Date:')]/following-sibling::td[1]"
        )
    )
    data["decision_date"] = format_date(
        extract_text(
            "//th[contains(text(), 'Decision Date:')]/following-sibling::td[1]"
        )
    )
    data["decision"] = extract_text(
        "//th[contains(text(), 'Decision Type:')]/following::td[1]"
    )

    # ─── Development Tab ────────────────────────────────────────────────
    try:
        development_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='#Development']"))
        )
        driver.execute_script("arguments[0].click();", development_tab)
        WebDriverWait(driver, 10).until(
            lambda d: d.find_element(
                By.XPATH, "//a[@href='#Development']"
            ).get_attribute("aria-selected")
            == "true"
        )
        data["development_description"] = extract_text(
            "//th[contains(text(), 'Development Description:')]/following-sibling::td[1]"
        )
    except Exception:
        data["development_description"] = ""
        logging.warning("Development tab activation or field extraction failed")

    # ─── Applicant Tab ────────────────────────────────────────────────
    try:
        applicant_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='#Applicant']"))
        )
        driver.execute_script("arguments[0].click();", applicant_tab)
        WebDriverWait(driver, 10).until(
            lambda d: d.find_element(By.XPATH, "//a[@href='#Applicant']").get_attribute(
                "aria-selected"
            )
            == "true"
        )
        data["applicant"] = extract_text(
            "//th[contains(text(), 'Applicant name:')]/following-sibling::td[1]"
        )
    except Exception:
        data["applicant"] = ""
        logging.warning("Applicant tab activation or field extraction failed")

    # ─── Decision Tab ─────────────────────────────────────────────────
    try:
        decision_tab = driver.find_element(By.XPATH, "//a[@href='#Decision']")
        driver.execute_script("arguments[0].click();", decision_tab)
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located(
                (By.XPATH, "//a[@href='#Decision' and @aria-selected='true']")
            )
        )
        data["final_grant_date"] = format_date(
            extract_text(
                "//th[normalize-space(text())='Grant Date:']/following-sibling::td[1]"
            )
        )
        data["full_proposal"] = extract_text(
            "//th[contains(text(), 'Decision Description:')]/following-sibling::td[1]"
        )
    except Exception:
        data["grant_date"] = ""
        data["full_proposal"] = ""
        logging.warning("Decision tab activation or field extraction failed")

    # ─── Appeal Tab ───────────────────────────────────────────────────
    try:
        appeal_tab = driver.find_element(By.XPATH, "//a[@href='#Appeal']")
        driver.execute_script("arguments[0].click();", appeal_tab)
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located(
                (By.XPATH, "//a[@href='#Appeal' and @aria-selected='true']")
            )
        )
        data["appeal_type"] = extract_text(
            "//th[contains(text(), 'Appeal Type: ')]/following-sibling::td[1]"
        )
        data["appeal_decision"] = extract_text(
            "//th[contains(text(), 'Appeal Decision: ')]/following-sibling::td[1]"
        )
        data["appeal_decision_date"] = format_date(
            extract_text(
                "//th[contains(text(), 'Decision Date:  ')]/following-sibling::td[1]"
            )
        )
    except Exception:
        data["appeal_type"] = ""
        data["appeal_decision"] = ""
        data["appeal_decision_date"] = ""
        logging.warning("Appeal tab activation or field extraction failed")

    return data


def parse_page(
    driver,
    row,
    batch,
    batch_ctr,
    batch_size,
    table_id,
    planning_authority,
    bq_client,
    replace_strings,
):
    try:
        # Check for "Server Error" page before proceeding
        if "Server Error in '/ePlan' Application." in driver.page_source:
            logging.warning(
                f"Server error page encountered for {row['unique_application_number']} — storing empty record."
            )
            batch.append(
                {
                    "unique_application_number": row["unique_application_number"],
                    "application_number": row["application_number"],
                    "application_status": "",
                    "development_description": "",
                    "application_type": "",
                    "decision": "",
                    "appeal_decision": "",
                    "appeal_type": "",
                    "decision_date": "",
                    "appeal_decision_date": "",
                    "grant_date": "",
                    "received_date": "",
                    "applicant": "",
                    "URL": driver.current_url,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            batch_ctr[0] += 1
            if batch_ctr[0] >= batch_size:
                flush_to_bq(
                    batch,
                    table_id,
                    bq_client,
                    date_columns=[
                        "decision_date",
                        "appeal_decision_date",
                        "grant_date",
                        "received_date",
                    ],
                )
                batch.clear()
                batch_ctr[0] = 0
            return
        # logging.info(f"▶ {planning_authority}")
        # Wait for expected element depending on authority
        if planning_authority == "Wexford County Council":
            wait_for_element(driver, "//a[text()='Open All']", "visibility", 20)
        elif planning_authority == "Cork County Council":
            wait_for_element(
                driver,
                "//ul[contains(@class, 'nav-tabs') and contains(@class, 'bg-info')]",
                "presence",
                60,
            )
        elif (
            planning_authority in SEARCH_PAGE
            and planning_authority != "Laois County Council"
        ):
            wait_for_element(
                driver, "//input[contains(@id,'reference')]", "presence", 20
            )
        else:
            wait_for_element(
                driver,
                "//ul[contains(@class, 'nav-tabs') and contains(@class, 'bg-info')]",
                "presence",
                10,
            )

    except TimeoutException:
        if (
            "Planning application details" in driver.page_source
            or "Application details" in driver.page_source
        ):
            logging.warning("Page appears to have loaded despite missing nav-tabs.")
        else:
            logging.warning(
                f"Skip UAN {row['unique_application_number']} – content not loaded."
            )
            return

    # Extract property fields based on authority
    if planning_authority == "Wexford County Council":
        props = retrieve_all_properties_wex(driver)
    elif planning_authority in SEARCH_PAGE and planning_authority in (
        "Dun Laoghaire Rathdown County Council",
        "Dublin City Council",
        "South Dublin County Council",
        "Fingal County Council",
    ):
        props = retrieve_all_properties(driver, planning_authority)
    else:
        props = retrieve_all_properties_others(driver)

    status = (
        (props.get("status_non_owner") or props.get("status_owner") or "")
        .lower()
        .strip()
    )
    for r in replace_strings:
        status = status.replace(r, "")

    batch.append(
        {
            "unique_application_number": row["unique_application_number"],
            "application_number": row["application_number"],
            "application_status": status,
            "development_description": (
                props.get("full_proposal") or props.get("development_description") or ""
            ).replace("'", "''"),
            "application_type": props.get("application_type"),
            "decision": props.get("decision"),
            "appeal_decision": props.get("appeal_decision"),
            "appeal_type": props.get("appeal_type"),
            "decision_date": props.get("decision_date"),
            "received_date": props.get("received_date"),
            "appeal_decision_date": props.get("appeal_decision_date"),
            "grant_date": props.get("final_grant_date"),
            "applicant": props.get("applicant"),
            "URL": driver.current_url,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    # print(batch)
    batch_ctr[0] += 1
    if batch_ctr[0] >= batch_size:
        flush_to_bq(
            batch,
            table_id,
            bq_client,
            date_columns=[
                "decision_date",
                "appeal_decision_date",
                "grant_date",
                "received_date",
            ],
        )
        batch.clear()
        batch_ctr[0] = 0


def process_application_rows(
    driver, row, batch, batch_ctr, batch_size, table_id, pa, bq_client, replace_strings
):
    uan = row["application_number"].strip()
    authority = row["planning_authority"]
    logging.info(f"🔍 Searching for UAN: {uan} in {authority}")

    modern_table_xpath = (
        "//div[@class='sas-table hidden-md hidden-lg']//tr[@ng-repeat='row in $data']"
    )
    legacy_table_xpath = (
        "//table[contains(@class, 'table-striped')]/tbody/tr[td]"  # Avoid header row
    )
    next_btn_xpath = "//a[@ng-switch-when='next']"

    while True:
        try:
            try:
                rows = wait_for_element(
                    driver, modern_table_xpath, "presence_elements", 10
                )
                table_type = "modern"
            except TimeoutException:
                rows = wait_for_element(
                    driver, legacy_table_xpath, "presence_elements", 10
                )
                table_type = "legacy"

            logging.info(f"➡️  Found {len(rows)} rows on current page.")
        except TimeoutException:
            logging.error("❌ Rows not found within timeout.")
            return

        for i, tr in enumerate(rows):
            if i >= 10:
                continue
            try:
                if table_type == "modern":
                    target_column_name = (
                        "Reference"
                        if authority
                        in (
                            "Dublin City Council",
                            "Dun Laoghaire Rathdown County Council",
                        )
                        else "Planning Reference"
                    )
                    try:
                        ref = tr.find_element(
                            By.XPATH,
                            f".//td[@data-title-override[contains(.,'{target_column_name}')]]//span",
                        ).text.strip()
                    except Exception:
                        ref = None
                else:  # legacy
                    try:
                        link = tr.find_element(By.XPATH, "./td[1]/a")
                        ref = link.text.strip()
                    except Exception:
                        ref = None
                logging.info(f"Reference: {ref}")
            except NoSuchElementException:
                logging.warning(f"⚠️ Row {i}: Could not extract reference.")
                continue

            if ref and ref.lower() == uan.strip().lower():
                logging.info(f"✅ Match found at row {i}: {ref}")
                if table_type == "modern":
                    driver.execute_script("arguments[0].click();", tr)
                    sleep(2)
                else:
                    driver.execute_script("arguments[0].click();", link)

                parse_page(
                    driver,
                    row,
                    batch,
                    batch_ctr,
                    batch_size,
                    table_id,
                    pa,
                    bq_client,
                    replace_strings,
                )
                return

        # paginate
        try:
            nxt = driver.find_element(By.XPATH, next_btn_xpath)
            if "disabled" in nxt.find_element(By.XPATH, "..").get_attribute("class"):
                logging.info("⛔ Reached last page, stopping pagination.")
                return
            logging.info("➡️ Clicking next button for pagination.")
            prev_url = driver.current_url
            driver.execute_script("arguments[0].click();", nxt)
            WebDriverWait(driver, 10).until(lambda d: d.current_url != prev_url)
        except Exception as e:
            logging.warning(f"⚠️ Pagination failed: {e}")
            return


def search_application(driver, row, retries, search_url):
    for attempt in range(1, retries + 1):
        logging.info(f"Attempt {attempt + 1}")
        try:
            if attempt > 1:
                driver.get(search_url)
                sleep(attempt)  # Progressive back-off
                logging.info(f"Retrying search attempt {attempt}/{retries}")

            # Wait for input and enter ID
            if row["planning_authority"].strip() in SEARCH_PAGE and row[
                "planning_authority"
            ].strip() in (
                "Dun Laoghaire Rathdown County Council",
                "Dublin City Council",
                "South Dublin County Council",
                "Fingal County Council",
                "Wexford County Council",
            ):
                # Try the first input field
                inp = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.ID, "searchInput"))
                )
            else:
                # Fall back to the alternative input field
                inp = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.ID, "TxtFileNumber"))
                )
            inp.clear()
            inp.send_keys(row["application_number"].strip())
            if row["planning_authority"].strip() in SEARCH_PAGE and row[
                "planning_authority"
            ].strip() in (
                "Dun Laoghaire Rathdown County Council",
                "Dublin City Council",
                "South Dublin County Council",
                "Fingal County Council",
            ):
                search_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.ID, "searchBtn"))
                )
            else:
                search_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.ID, "SearchButton"))
                )
            sleep(1)
            driver.execute_script("arguments[0].click();", search_button)
            logging.info("Search initiated successfully.")
            return True

        except TimeoutException:
            logging.warning(f"Search timeout on attempt {attempt}/{retries}")
        except Exception as e:
            logging.error(f"Unexpected error during search (attempt {attempt}): {e}")

    logging.error("All search attempts failed.")
    return False  # Give up but continue pipeline


# ──────────────────────────────  Bootstrapping  ────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s",
)

client = bigquery.Client()
TABLE_ID = "eire-1746041472369.eireestate_dataset_processing.applications_raw_scrapped"

SEARCH_PAGE = {
    "Dun Laoghaire Rathdown County Council": "https://planning.agileapplications.ie/dunlaoghaire/search-applications/",
    "Dublin City Council": "https://planning.agileapplications.ie/dublincity/search-applications/",
    "South Dublin County Council": "https://planning.agileapplications.ie/southdublin/search-applications/",
    "Fingal County Council": "https://planning.agileapplications.ie/fingal/search-applications/",
    "Laois County Council": "https://www.eplanning.ie/LaoisCC/searchexact/",
    "Louth County Council": "https://www.eplanning.ie/LouthCC/SearchExact/",
    "Mayo County Council": "https://www.eplanning.ie/mayocc/SearchExact/",
    "Wexford County Council": "https://planning.agileapplications.ie/wexford/search-applications/",
}


def safe_driver_get(driver, url, max_retries=3, wait_seconds=2):
    attempt = 0
    while attempt < max_retries:
        logging.info(
            f"[safe_driver_get] Attempt {attempt + 1} of {max_retries} for URL: {url}"
        )
        try:
            logging.debug(f"[safe_driver_get] Navigating to: {url}")
            driver.get(url)
            logging.debug(
                f"[safe_driver_get] Current URL after get: {driver.current_url}"
            )
            return True  # Success

        except (TimeoutException, WebDriverException, socket.timeout, ReadTimeout) as e:
            logging.warning(
                f"[safe_driver_get] Known exception on attempt {attempt + 1}: {e}"
            )
        except Exception as e:
            logging.error(
                f"[safe_driver_get] Unexpected error on attempt {attempt + 1}: {e}",
                exc_info=True,
            )

        attempt += 1
        sleep(wait_seconds)

    logging.error(
        f"[safe_driver_get] Failed to load {url} after {max_retries} attempts."
    )
    return False


# ──────────────────────────────  Main  ────────────────────────────────
def run():
    query = """
        SELECT DISTINCT unique_application_number,
                        CASE
                        WHEN planning_authority = 'Mayo County Council' THEN
                                              CONCAT(
                            'https://www.eplanning.ie/MayoCC/AppFileRefDetails/',
                            REGEXP_EXTRACT(
                              REPLACE(REPLACE(link_app_details, CHR(10), ''), CHR(13), ''),
                              r'filenum=(\d+)'
                            ),
                            '/0'
                          )
                        ELSE
                            REPLACE(REPLACE(link_app_details, CHR(10), ''), CHR(13), '')
                        END AS link_app_details,
               planning_authority,
               application_number
        FROM `eire-1746041472369.eireestate_dataset_processing.applications_raw_ready_to_scrap`
        WHERE
           unique_application_number NOT LIKE 'REF%%'
          AND unique_application_number NOT LIKE '%%ABP%%'
          AND unique_application_number NOT IN (
              SELECT unique_application_number
              FROM `eire-1746041472369.eireestate_dataset_processing.applications_raw_scrapped`)
          AND planning_authority not in ('Cork County Council','Cork City Council')
        ORDER BY planning_authority
    """
    df = client.query(query).to_dataframe()
    driver = setup_driver()
    replace_strings = [" by Fingal County Council", " - see appeal details", ""]
    batch, batch_ctr, batch_size = [], [0], 60
    MAX_ITERATIONS_PER_DRIVER = 31

    try:
        for i, row in df.iterrows():
            logging.info(f"iteration {i}")
            if i % MAX_ITERATIONS_PER_DRIVER == 0 and i != 0:
                driver.quit()
                driver = setup_driver()
                accept_cookies(driver)

            logging.info(f"▶ {row['unique_application_number']}")
            if i == 0:
                accept_cookies(driver)

            pa = row["planning_authority"]
            url = (row["link_app_details"] or "").strip()
            if url.startswith("http://"):
                url = "https://" + url[len("http://") :]
            if (
                url.endswith("E")
                and row["planning_authority"] == "Wexford County Council"
            ):
                url = url[:-1]

            if (
                not url
                or "fingal" in url
                or "pleanala" in url
                or "wexfordcoco.ie/application_maps" in url
            ):
                search_url = SEARCH_PAGE[pa]
                logging.info(f" search page▶ {search_url}")
                if not safe_driver_get(
                    driver, search_url, max_retries=3, wait_seconds=3
                ):
                    continue

                current_url = driver.current_url
                ok = search_application(driver, row, 3, search_url)
                if ok:
                    try:
                        WebDriverWait(driver, 10).until(
                            lambda d: d.current_url != current_url
                        )
                        logging.info(
                            f"▶ Proceeding to search result: {driver.current_url}"
                        )
                        process_application_rows(
                            driver,
                            row,
                            batch,
                            batch_ctr,
                            batch_size,
                            TABLE_ID,
                            pa,
                            client,
                            replace_strings,
                        )
                    except TimeoutException:
                        logging.warning(
                            "Timed out waiting for URL to change after search."
                        )
            else:
                if not safe_driver_get(driver, url, max_retries=3, wait_seconds=10):
                    continue
                sleep(2)
                logging.info(f"▶ parse_page {url}")
                parse_page(
                    driver,
                    row,
                    batch,
                    batch_ctr,
                    batch_size,
                    TABLE_ID,
                    pa,
                    client,
                    replace_strings,
                )

        flush_to_bq(
            batch,
            TABLE_ID,
            client,
            date_columns=[
                "decision_date",
                "appeal_decision_date",
                "grant_date",
                "received_date",
            ],
        )
    finally:
        driver.quit()
