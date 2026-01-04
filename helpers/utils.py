import logging
import os
import zipfile
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# from webdriver_manager.chrome import ChromeDriverManager


def get_proxy_auth_extension(proxy_host, proxy_port, proxy_user, proxy_pass):
    extension_dir = "/tmp/proxy_auth_plugin"
    zip_plugin_path = "/tmp/proxy_auth_plugin.zip"

    # 1. Manifest V3 (Required for Chrome 127+)
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 3,
        "name": "Chrome Proxy Auth",
        "permissions": [
            "proxy",
            "webRequest",
            "webRequestAuthProvider"
        ],
        "host_permissions": [
            "<all_urls>"
        ],
        "background": {
            "service_worker": "background.js"
        }
    }
    """

    # 2. Background Service Worker
    # Note: MV3 still allows blocking for onAuthRequired
    background_js = f"""
    var config = {{
        mode: "fixed_servers",
        rules: {{
            singleProxy: {{
                scheme: "http",
                host: "{proxy_host}",
                port: parseInt({proxy_port})
            }},
            bypassList: ["localhost"]
        }}
    }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{proxy_user}",
                password: "{proxy_pass}"
            }}
        }};
    }}

    chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {{urls: ["<all_urls>"]}},
            ['blocking']
    );
    """

    if not os.path.exists(extension_dir):
        os.makedirs(extension_dir)

    with open(os.path.join(extension_dir, "manifest.json"), "w") as f:
        f.write(manifest_json)

    with open(os.path.join(extension_dir, "background.js"), "w") as f:
        f.write(background_js)

    with zipfile.ZipFile(zip_plugin_path, "w") as zp:
        zp.write(os.path.join(extension_dir, "manifest.json"), "manifest.json")
        zp.write(os.path.join(extension_dir, "background.js"), "background.js")

    return zip_plugin_path


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
    # opts.add_argument("--disable-features=VizDisplayCompositor")
    # opts.add_argument("--disable-extensions")
    opts.add_argument("--remote-debugging-pipe")

    # --- 3. CRITICAL CRASH FIXES (Chrome 127+) ---
    # These prevent the browser from hanging on the "Search Engine Choice" popup
    opts.add_argument("--disable-search-engine-choice-screen")
    opts.add_argument(
        "--disable-features=VizDisplayCompositor,SearchEngineChoiceScreen"
    )
    opts.add_argument("--no-zygote")
    opts.add_argument("--disable-gpu-sandbox")

    PROXY_HOST = "proxy-eu.proxy-cheap.com"
    PROXY_PORT = "5959"
    PROXY_USER = "pcaNifMIJV-resfix-ie-nnid-0"
    PROXY_PASS = "PC_2t67sBsYxI0eNLqOh"
    proxy_plugin = get_proxy_auth_extension(
        PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS
    )

    # 2. Add the Extension to Chrome
    opts.add_extension(proxy_plugin)

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    )

    prefs = {
        "profile.managed_default_content_settings.images": 2,  # Block images
        "profile.managed_default_content_settings.stylesheets": 1,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.cookies": 1,
        "profile.managed_default_content_settings.javascript": 1,
        "profile.managed_default_content_settings.plugins": 1,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.media_stream": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    #
    opts.page_load_strategy = "eager"

    opts.binary_location = os.getenv("GOOGLE_CHROME_BIN", "/usr/bin/chromium")

    service = Service(
        executable_path=os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
    )
    driver = webdriver.Chrome(
        service=service,
        options=opts,
    )
    driver.set_page_load_timeout(60)
    driver.set_script_timeout(60)

    # ⬇️ THIS MUST BE HERE ⬇️ (before any driver.get())
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd(
        "Network.setBlockedURLs",
        {
            "urls": [
                "*googletagmanager.com*",
                "*google-analytics.com*",
                "*doubleclick.net*",
                "*googleadservices.com*",
                "*facebook.net*",
                "*hotjar.com*",
                "*clarity.ms*",
                "*analytics*",
                "*gtm*",
            ]
        },
    )

    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": """
            Object.defineProperty(window, 'ga', { get: () => undefined });
            Object.defineProperty(window, 'gtag', { get: () => undefined });
            Object.defineProperty(window, 'dataLayer', { value: [] });
        """
        },
    )

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
