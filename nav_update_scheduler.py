import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from bs4 import BeautifulSoup
import re
import shutil
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import backoff
import ratelimit
import aiosqlite
import json

# For Google Sheets Integration
import gspread
from google.oauth2.service_account import Credentials

# === Setup Logging ===
logging.basicConfig(
    filename='portfolio_updater.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# === Config Constants (Flat Style) ===
DATA_DIR = "src/data"
CACHE_DB = f"{DATA_DIR}/cache.db"
NAV_HISTORY_CSV = f"{DATA_DIR}/nav_history.csv"

# === Config Class with Fixed Structure ===
class Config:
    RETRY_ATTEMPTS: int = 3
    REQUEST_TIMEOUT: int = 10
    RATE_LIMIT: int = 2

    class Files:
        NAV_HISTORY_CSV: str = NAV_HISTORY_CSV
        FUND_TRACKER_EXCEL: str = "Fund-Tracker-original.xlsx"
        FUND_SHEET: str = "Fund Tracker"
        CACHE_DB: str = CACHE_DB

        # Replace with actual Google Sheet IDs or environment variables
        NIFTY_SHEET_ID: str = os.getenv("GOOGLE_SHEET_NIFTY_ID", "YOUR_NIFTY_SHEET_ID")
        GOLD_SHEET_ID: str = os.getenv("GOOGLE_SHEET_GOLD_ID", "YOUR_GOLD_SHEET_ID")
        CURRENCY_SHEET_ID: str = os.getenv("GOOGLE_SHEET_CURRENCY_ID", "YOUR_CURRENCY_SHEET_ID")

    class URLs:
        INVESTING_BASE: str = "https://www.investing.com"
        GOLD_URLS: List[str] = [
            "https://www.goodreturns.in/gold-rates/",
            "https://www.livemint.com/money/personal-finance/gold-rate-in-india",
            "https://www.mcxindia.com/market-data/spot-market-price"
        ]
        CURRENCY_ENDPOINTS: Dict[str, str] = {
            "USDINR": "/currencies/usd-inr",
            "EURINR": "/currencies/eur-inr",
            "BTCINR": "https://www.coindesk.com/price/bitcoin/"
        }
        AMFI_NAV: str = "https://www.amfiindia.com/spages/NAVAll.txt"


@dataclass
class MarketData:
    timestamp: datetime
    value: float
    source: str
    metadata: Dict[str, Any] = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "source": self.source,
            "metadata": self.metadata or {}
        }

class DataCache:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.setup_db()

    def setup_db(self):
        async def init_db():
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS market_data (
                        id INTEGER PRIMARY KEY,
                        data_type TEXT,
                        timestamp TEXT,
                        data TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                await db.commit()
        
        asyncio.run(init_db())

    async def set(self, data_type: str, data: MarketData, ttl_hours: int = 24):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO market_data (data_type, timestamp, data) VALUES (?, ?, ?)",
                (data_type, data.timestamp.isoformat(), json.dumps(data.to_dict()))
            )
            await db.commit()

    async def get(self, data_type: str) -> Optional[MarketData]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT data FROM market_data WHERE data_type = ? ORDER BY timestamp DESC LIMIT 1",
                (data_type,)
            )
            row = await cursor.fetchone()
            if row:
                data = json.loads(row[0])
                return MarketData(
                    timestamp=datetime.fromisoformat(data["timestamp"]),
                    value=data["value"],
                    source=data["source"],
                    metadata=data.get("metadata")
                )
        return None

class GoogleSheetsManager:
    def __init__(self, service_account_info: str):
        # service_account_info is expected to be a JSON string
        try:
            creds_json = json.loads(service_account_info)
            self.credentials = Credentials.from_service_account_info(
                creds_json,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            self.client = gspread.authorize(self.credentials)
            logging.info("Google Sheets API client authorized.")
        except Exception as e:
            logging.error(f"Failed to authorize Google Sheets API client: {e}")
            raise

    def append_data(self, sheet_id: str, sheet_name: str, data: List[List[Any]]) -> bool:
        """Appends a list of rows to the specified Google Sheet."""
        try:
            spreadsheet = self.client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.append_rows(data, value_input_option='USER_ENTERED')
            logging.info(f"Successfully appended {len(data)} rows to {sheet_name} in sheet {sheet_id}.")
            return True
        except gspread.exceptions.SpreadsheetNotFound:
            logging.error(f"Spreadsheet with ID '{sheet_id}' not found. Check ID and sharing permissions.")
            return False
        except gspread.exceptions.WorksheetNotFound:
            logging.error(f"Worksheet '{sheet_name}' not found in spreadsheet {sheet_id}.")
            return False
        except Exception as e:
            logging.error(f"Failed to append data to Google Sheet {sheet_id}/{sheet_name}: {e}")
            return False

    def get_all_records(self, sheet_id: str, sheet_name: str) -> List[Dict[str, Any]]:
        """Reads all records from a Google Sheet as a list of dictionaries."""
        try:
            spreadsheet = self.client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            return worksheet.get_all_records() # Returns list of dicts, first row as headers
        except gspread.exceptions.SpreadsheetNotFound:
            logging.error(f"Spreadsheet with ID '{sheet_id}' not found. Check ID and sharing permissions.")
            return []
        except gspread.exceptions.WorksheetNotFound:
            logging.error(f"Worksheet '{sheet_name}' not found in spreadsheet {sheet_id}.")
            return []
        except Exception as e:
            logging.error(f"Failed to read data from Google Sheet {sheet_id}/{sheet_name}: {e}")
            return []

class DataUpdater:
    def __init__(self, cache: DataCache, gs_manager: GoogleSheetsManager):
        self.cache = cache
        self.gs_manager = gs_manager
        self.ensure_directories()

    @staticmethod
    def ensure_directories():
        os.makedirs(Config.DATA_DIR, exist_ok=True)

    def _safe_merge_csv(self, filepath: str, new_df: pd.DataFrame, 
                        key_cols: List[str], date_fmt: Optional[str] = None) -> None:
        """
        Merges new DataFrame with existing CSV, drops duplicates, and sorts.
        This is still used for NAV history if kept locally.
        """
        try:
            if os.path.exists(filepath):
                # Optional: backup old file before merging
                # backup_path = filepath.replace('.csv', f'_backup_{datetime.now():%Y%m%d_%H%M%S}.csv')
                # shutil.copy2(filepath, backup_path)

                try:
                    existing_df = pd.read_csv(filepath)
                except FileNotFoundError: # Should not happen if os.path.exists is true
                    existing_df = pd.DataFrame() # Start with empty if file somehow disappeared

                combined = pd.concat([existing_df, new_df])
                combined = combined.drop_duplicates(subset=key_cols, keep='last')

                if date_fmt:
                    # Convert to datetime, handle errors, then sort
                    combined['_sort_date'] = pd.to_datetime(combined[key_cols[0]], format=date_fmt, errors='coerce')
                    combined = combined.dropna(subset=['_sort_date']) # Drop rows where date conversion failed
                    combined = combined.sort_values('_sort_date').drop(columns=['_sort_date'])

                combined.to_csv(filepath, index=False)
                logging.info(f"Successfully merged data into local CSV: {filepath}")
            else:
                new_df.to_csv(filepath, index=False)
                logging.info(f"Created new local CSV: {filepath}")

        except Exception as e:
            logging.error(f"Failed to merge CSV {filepath}: {str(e)}")
            raise

    async def update_nifty(self, fetcher: DataFetcher) -> bool:
        logging.info("Starting Nifty update for Google Sheet...")
        try:
            html = await fetcher.fetch_url(f"{Config.URLs.INVESTING_BASE}/indices/s-p-cnx-nifty")
            if not html:
                logging.warning("No HTML fetched for Nifty.")
                return False

            match = re.search(r'last\">(\d{4,5}\.\d+)', html)
            if not match:
                logging.warning("Could not parse Nifty price from HTML.")
                return False

            price = float(match.group(1))
            today_str = datetime.now().strftime("%Y-%m-%d") # Use YYYY-MM-DD for Sheets

            # Append to Google Sheet
            # Ensure your Google Sheet has columns like "Date", "Close"
            data_row = [today_str, price]
            success = self.gs_manager.append_data(Config.Files.NIFTY_SHEET_ID, "Sheet1", [data_row]) # Assuming "Sheet1"
            
            if success:
                logging.info(f"Updated Nifty price: {price} to Google Sheet.")
            else:
                logging.error(f"Failed to write Nifty price {price} to Google Sheet.")
            return success

        except Exception as e:
            logging.error(f"Nifty update failed: {str(e)}")
            return False

    async def update_gold(self, fetcher: DataFetcher) -> bool:
        logging.info("Starting Gold update for Google Sheet...")
        for url in Config.URLs.GOLD_URLS:
            try:
                html = await fetcher.fetch_url(url)
                if not html:
                    logging.warning(f"No HTML fetched for Gold from {url}.")
                    continue

                price = None
                if "goodreturns.in" in url:
                    soup = BeautifulSoup(html, "html.parser")
                    tag = soup.find("td", string=re.compile("22 carat", re.IGNORECASE))
                    if tag:
                        price_td = tag.find_next_sibling("td")
                        if price_td and price_td.text:
                            price = float(price_td.text.strip().replace("₹", "").replace(",", "")) * 10
                # Add more parsing logic for other gold URLs if needed
                # elif "livemint.com" in url:
                #     ...
                # elif "mcxindia.com" in url:
                #     ...

                if price:
                    today_str = datetime.now().strftime("%Y-%m-%d")

                    # Append to Google Sheet
                    # Ensure your Google Sheet has columns like "Date", "Price", "Source"
                    data_row = [today_str, price, url]
                    success = self.gs_manager.append_data(Config.Files.GOLD_SHEET_ID, "Sheet1", [data_row]) # Assuming "Sheet1"
                    
                    if success:
                        logging.info(f"Updated Gold price: ₹{price} from {url} to Google Sheet.")
                        return True # Return True as soon as one source succeeds
                    else:
                        logging.error(f"Failed to write Gold price {price} from {url} to Google Sheet.")
                        continue # Try next URL if writing fails

            except Exception as e:
                logging.error(f"Gold update failed for {str(e)}")
                continue

        logging.error("All Gold update sources failed.")
        return False

    async def update_currency(self, fetcher: DataFetcher) -> bool:
        logging.info("Starting currency update for Google Sheet...")
        success_count = 0
        all_currency_data_rows = []

        for currency_pair, endpoint in Config.URLs.CURRENCY_ENDPOINTS.items():
            url = f"{Config.URLs.INVESTING_BASE}{endpoint}" if not endpoint.startswith("http") else endpoint
            try:
                html = await fetcher.fetch_url(url)
                if not html:
                    logging.warning(f"No HTML fetched for {currency_pair} from {url}")
                    continue

                price = None
                if "investing.com" in url:
                    soup = BeautifulSoup(html, "html.parser")
                    price_tag = soup.find('div', class_='instrument-price-last') # This selector needs verification
                    if price_tag:
                        price = float(price_tag.text.strip().replace(',', ''))
                elif "coindesk.com" in url:
                    soup = BeautifulSoup(html, "html.parser")
                    price_tag = soup.find('span', class_='currency-price') # This selector needs verification
                    if price_tag:
                        price = float(price_tag.text.strip().replace('₹', '').replace(',', ''))

                if price:
                    today_str = datetime.now().strftime("%Y-%m-%d")
                    # Prepare row for Google Sheet
                    # Ensure your Google Sheet has columns like "Date", "CurrencyPair", "Rate", "Source"
                    all_currency_data_rows.append([today_str, currency_pair, price, url])
                    logging.info(f"Prepared {currency_pair} rate: {price} from {url} for Google Sheet.")
                    success_count += 1
                else:
                    logging.warning(f"Could not parse price for {currency_pair} from {url}")

            except Exception as e:
                logging.error(f"Currency update failed for {currency_pair} from {url}: {str(e)}")
                continue
        
        if all_currency_data_rows:
            # Append all collected currency data in one go
            success = self.gs_manager.append_data(Config.Files.CURRENCY_SHEET_ID, "Sheet1", all_currency_data_rows) # Assuming "Sheet1"
            if success:
                logging.info(f"Successfully appended {success_count} currency rates to Google Sheet.")
                return True
            else:
                logging.error(f"Failed to append currency data to Google Sheet.")
                return False
        else:
            logging.warning("No currency data collected to append to Google Sheet.")
            return False

    async def update_nav(self, fetcher: DataFetcher) -> bool:
        logging.info("Starting NAV update...")
        try:
            nav_data_raw = await fetcher.fetch_url(Config.URLs.AMFI_NAV)
            if not nav_data_raw:
                return False

            # Parse AMFI NAV data
            # This is a simplified parser, actual AMFI data needs robust parsing
            nav_lines = nav_data_raw.strip().split('\n')
            
            # Find the header line (usually starts with "Scheme Code")
            header_line_index = -1
            for i, line in enumerate(nav_lines):
                if "Scheme Code" in line and "Net Asset Value" in line:
                    header_line_index = i
                    break
            
            if header_line_index == -1:
                logging.error("Could not find header in AMFI NAV data.")
                return False

            # Extract header and data lines
            header = [h.strip() for h in nav_lines[header_line_index].split(';') if h.strip()]
            data_lines = nav_lines[header_line_index + 1:]

            records = []
            for line in data_lines:
                parts = [p.strip() for p in line.split(';') if p.strip()]
                if len(parts) == len(header): # Basic check for complete rows
                    record = dict(zip(header, parts))
                    records.append(record)
            
            if not records:
                logging.warning("No NAV records parsed from AMFI data.")
                return False

            df_nav = pd.DataFrame(records)
            
            # Rename columns for consistency and select relevant ones
            df_nav = df_nav.rename(columns={
                'Scheme Code': 'Fund Code',
                'Scheme Name': 'Fund Name',
                'Net Asset Value': 'NAV',
                'Date': 'Date' # AMFI date format is usually DD-Mon-YYYY
            })

            # Convert NAV to numeric, handle errors
            df_nav['NAV'] = pd.to_numeric(df_nav['NAV'], errors='coerce')
            df_nav = df_nav.dropna(subset=['NAV'])

            # Convert Date to datetime object for merging
            df_nav['Date'] = pd.to_datetime(df_nav['Date'], format='%d-%b-%Y', errors='coerce')
            df_nav = df_nav.dropna(subset=['Date'])
            df_nav['Date'] = df_nav['Date'].dt.strftime('%Y-%m-%d') # Standardize date format

            # Merge with existing NAV history (still to local CSV for now)
            self._safe_merge_csv(Config.Files.NAV_HISTORY_CSV, df_nav, ["Date", "Fund Code"], "%Y-%m-%d")
            
            logging.info(f"Successfully updated NAV history with {len(df_nav)} records.")
            return True

        except Exception as e:
            logging.error(f"NAV update failed: {str(e)}")
            return False
class DataFetcher:
    def __init__(self, cache: DataCache):
        self.cache = cache
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=Config.RETRY_ATTEMPTS)
    @ratelimit.limits(calls=Config.RATE_LIMIT, period=1)
    async def fetch_url(self, url: str) -> Optional[str]:
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()

            async with self.session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                if response.status == 200:
                    html = await response.text()
                    logging.info(f"Fetched data from {url}")
                    return html
                else:
                    logging.warning(f"Failed to fetch {url}, status {response.status}")
        except Exception as e:
            logging.error(f"Exception during fetch from {url}: {e}")
        return None

async def main():
    # Retrieve Google Service Account credentials from environment variable
    google_sa_key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
    if not google_sa_key_json:
        logging.error("GOOGLE_SERVICE_ACCOUNT_KEY environment variable not set. Cannot proceed with Google Sheets updates.")
        return False

    try:
        gs_manager = GoogleSheetsManager(google_sa_key_json)
    except Exception as e:
        logging.error(f"Failed to initialize GoogleSheetsManager: {e}")
        return False

    cache = DataCache(Config.Files.CACHE_DB)
    updater = DataUpdater(cache, gs_manager)
    
    # Ensure local data directory exists for NAV history and cache DB
    updater.ensure_directories()

    async with DataFetcher(cache) as fetcher:
        tasks = [
            updater.update_nifty(fetcher),
            updater.update_gold(fetcher),
            updater.update_currency(fetcher),
            updater.update_nav(fetcher), # NAV history still updates local CSV for now
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = all(isinstance(r, bool) and r for r in results)
        
        if success:
            logging.info("All data updates completed successfully")
        else:
            logging.error("Some data updates failed")
        
        # If NAV_HISTORY_CSV and CACHE_DB are still local, the workflow will handle pushing them.
        return success

if __name__ == "__main__":
    asyncio.run(main())
