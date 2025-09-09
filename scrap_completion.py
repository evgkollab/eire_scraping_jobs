
from datetime import datetime
from time import sleep
from urllib.parse import urljoin
import logging
import math
import random
import time

import pandas as pd
from google.cloud import bigquery
from thefuzz import fuzz

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from helpers.utils import flush_to_bq,setup_driver

# ---- Config ----
client = bigquery.Client()
TABLE_ID = 'eire-1746041472369.eireestate_dataset_extending.large_developments_ncbo_completion'
WRITE_BATCH = 40
SEARCH_BASE = "https://www.nbco.localgov.ie/en/bcms/search?search_api_views_fulltext="
SITE_BASE = "https://www.nbco.localgov.ie/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def run():
    query = """
    select
        distinct unique_application_number
        ,details_link
    from `eire-1746041472369.eireestate_dataset_extending.large_developments_ncbo`
    where  details_link <> ''
    and unique_application_number not in
    (select unique_application_number from `eire-1746041472369.eireestate_dataset_extending.large_developments_ncbo_completion`  )
    and notice_type <> 'no match'
    """
    df_planing_aplications = client.query(query).to_dataframe()

    USERNAME = "evgkol85@gmail.com"
    PASSWORD = "#yko827%UkRJ&*qf"

    data_completion = []
    driver = setup_driver()
    write_batch_counter = 0

    for index, row in df_planing_aplications.iterrows():
        print(row['details_link'])
        print(row['unique_application_number'])
        initial_url = row['details_link']

        print(f"Initial URL: {initial_url}")
        driver.get(initial_url)

        # Step 1: Check if user is already logged in by looking for 'Log Out'
        logged_in = False
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.LINK_TEXT, "Log Out"))
            )
            print("User is already logged in.")
            logged_in = True
        except TimeoutException:
            print("No 'Log Out' button found. Proceeding to handle cookies and log in.")

        # Step 2: Handle cookie banner ONLY IF not logged in
        if not logged_in:
            try:
                print("Waiting for cookie banner...")
                decline_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyButtonDecline"))
                )
                decline_button.click()
                print("Clicked 'Use necessary cookies only'.")
            except TimeoutException:
                print("Cookie banner did not appear within the timeout.")
            except Exception as e:
                print(f"Unexpected error while clicking cookie decline button: {e}")

            # Step 3: Perform login
            try:
                login_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Log In"))
                )
                login_button.click()

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "edit-name"))
                )
                driver.find_element(By.ID, "edit-name").send_keys(USERNAME)
                driver.find_element(By.ID, "edit-pass").send_keys(PASSWORD)
                driver.find_element(By.ID, "edit-submit").click()

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//p[contains(text(),"Use the tabs below to create and/or view your Notices")]')
                    )
                )
                print("Login successful.")
                # After login, go back to the original URL
                driver.get(initial_url)

            except Exception as e:
                print(f"Error during login process: {e}")

        certs = driver.find_elements(By.CSS_SELECTOR, "section.notice-info .field-name-field-certificate article.node-certificate")
        print(f"Found {len(certs)} certificates.")

        if len(certs)>0:
            for cert in certs:
                write_batch_counter+=1
                try:
                    compl_title = cert.find_element(By.CSS_SELECTOR, 'span[property="dc:title"]').get_attribute("content").strip()
                except:
                    compl_title = ""

                try:
                    compl_cert_no = cert.find_element(By.CSS_SELECTOR, ".field-name-field-cc-no .field-item").text.strip()
                except:
                    compl_cert_no = ""

                try:
                    units_text = cert.find_element(By.CSS_SELECTOR, ".field-name-field-cc-units .field-items").text.strip()

                    value = float(units_text)

                    # Only assign if it's a valid number and not NaN
                    if not math.isnan(value):
                        compl_total_units = int(value)
                    else:
                        compl_total_units = 0
                except Exception as e:
                    compl_total_units = 0

                try:
                    address_elements = cert.find_elements(By.CSS_SELECTOR, ".field-name-field-cc-address .certificate-address-part")
                    address_parts = [elem.text.strip() for elem in address_elements if elem.text.strip()]
                    compl_address = ", ".join(dict.fromkeys(address_parts))
                except:
                    compl_address = ""

                        # Collect the row into self.data
                data_completion.append({
                                    "unique_application_number": row['unique_application_number'],
                                    "title": compl_title,
                                    "certificate_number": compl_cert_no,
                                    "total_units": compl_total_units,
                                    "address": compl_address,
                                    "ncbo_link": initial_url})
        else:
            write_batch_counter+=1
            data_completion.append({
                                    "unique_application_number": row['unique_application_number'],
                                    "title": '',
                                    "certificate_number": '',
                                    "total_units": 0,
                                    "address": '',
                                    "ncbo_link": initial_url})

        if write_batch_counter >= WRITE_BATCH:

            flush_to_bq(data_completion, TABLE_ID, client,
                                type_casts={"total_units": "Int64"})

            write_batch_counter = 0
            # At the end of each iteration
        delay = random.uniform(2.5, 6.0)  # random delay between 2.5 and 6 seconds
        print(f"Delaying for {delay:.2f} seconds to reduce server load...")
        time.sleep(delay)
