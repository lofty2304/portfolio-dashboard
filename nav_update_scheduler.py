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
from typing import Dict, List, Optional, Any, Callable
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

# === Config Class with Fixed Structure ===
class Config:
    """
    Centralized configuration class for the portfolio updater.
    Uses nested classes for better organization of related constants.
    """
    pass

# Top-level config constants
Config.DATA_DIR: str = "src/data"
Config.RETRY_ATTEMPTS: int = 3
Config.REQUEST_TIMEOUT: int = 10
Config.RATE_LIMIT: int = 2 # Calls per period (e.g., 2 calls per 2 seconds)

# Nested Files class safely referencing Config
class Files:
    """File paths and Google Sheet IDs."""
    # NAV_HISTORY_CSV is no longer used for primary storage, but kept for reference if needed
    NAV_HISTORY_CSV: str = f"{Config.DATA_DIR}/nav_history.csv" 
    FUND_TRACKER_EXCEL: str = "Fund-Tracker-original.xlsx" # Not directly used in this script's logic
    FUND_SHEET: str = "Fund Tracker" # Not directly used in this script's logic
    CACHE_DB: str = f"{Config.DATA_DIR}/cache.db"

    # Sheet IDs pulled from environment variables
    NIFTY_SHEET_ID: str = os.getenv("GOOGLE_SHEET_NIFTY_ID", "YOUR_NIFTY_SHEET_ID")
    GOLD_SHEET_ID: str = os.getenv("GOOGLE_SHEET_GOLD_ID", "YOUR_GOLD_SHEET_ID")
    CURRENCY_SHEET_ID: str = os.getenv("GOOGLE_SHEET_CURRENCY_ID", "YOUR_CURRENCY_SHEET_ID")
    # NEW: Google Sheet ID for NAV history
    NAV_SHEET_ID: str = os.getenv("GOOGLE_SHEET_NAV_ID", "YOUR_NAV_SHEET_ID")


# Nested URLs class
class URLs:
    """URLs for data fetching."""
    INVESTING_BASE: str = "https://www.investing.com"
    GOLD_URLS: List[str] = [
        "https://www.goodreturns.in/gold-rates/",
        # "https://www.livemint.com/money/personal-finance/gold-rate-in-india", # Removed: Reported as not working
        "https://www.mcxindia.com/market-data/spot-market-price"
    ]
    CURRENCY_ENDPOINTS: Dict[str, str] = {
        "USDINR": "/currencies/usd-inr",
        "EURINR": "/currencies/eur-inr",
        "BTCINR": "https://www.coindesk.com/price/bitcoin/"
    }
    AMFI_NAV: str = "https://www.amfiindia.com/spages/NAVAll.txt"

# Attach nested classes to Config
Config.Files = Files
Config.URLs = URLs

@dataclass
class MarketData:
    """Dataclass to standardize market data."""
    timestamp: datetime
    value: float
    source: str
    metadata: Dict[str, Any] = None

    def to_dict(self) -> dict:
        """Converts the MarketData object to a dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "source": self.source,
            "metadata": self.metadata or {}
        }

class DataCache:
    """
    Manages a local SQLite cache for market data.
    Stores and retrieves MarketData objects.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Database setup is now an async method and must be called with await externally.
        # No synchronous setup in __init__ to avoid RuntimeError.

    async def initialize_db(self):
        """Initializes the SQLite database table if it doesn't exist."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(data_type, timestamp) ON CONFLICT REPLACE
                )
            """)
            await db.commit()
        logging.info(f"Database initialized at {self.db_path}")

    async def set(self, data_type: str, data: MarketData, ttl_hours: int = 24):
        """
        Stores market data in the cache.
        Uses UPSERT (REPLACE) to avoid duplicate entries for the same data_type and timestamp.
        TTL is currently not enforced for cleanup but can be used for future expiration logic.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO market_data (data_type, timestamp, data) VALUES (?, ?, ?)",
                (data_type, data.timestamp.isoformat(), json.dumps(data.to_dict()))
            )
            await db.commit()
        logging.info(f"Cached data for {data_type} with timestamp {data.timestamp.isoformat()}")
        # Trigger cleanup after setting new data
        # await self.cleanup_old_data(data_type, ttl_hours) # This was commented out by user request


    async def get(self, data_type: str) -> Optional[MarketData]:
        """
        Retrieves the most recent market data for a given type from the cache.
        Does not enforce TTL during retrieval, only fetches the latest.
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT data FROM market_data WHERE data_type = ? ORDER BY timestamp DESC LIMIT 1",
                (data_type,)
            )
            row = await cursor.fetchone()
            if row:
                data = json.loads(row[0])
                logging.info(f"Retrieved cached data for {data_type} from {data['timestamp']}")
                return MarketData(
                    timestamp=datetime.fromisoformat(data["timestamp"]),
                    value=data["value"],
                    source=data["source"],
                    metadata=data.get("metadata")
                )
        logging.debug(f"No cached data found for {data_type}")
        return None

    # This method was added for cache cleanup, but the user requested to disregard that step for now.
    # async def cleanup_old_data(self, data_type: str, ttl_hours: int = 24):
    #     """
    #     Removes old data entries from the cache for a specific data_type.
    #     Data older than ttl_hours will be deleted.
    #     """
    #     cutoff_time = datetime.now() - timedelta(hours=ttl_hours)
    #     async with aiosqlite.connect(self.db_path) as db:
    #         cursor = await db.execute(
    #             "DELETE FROM market_data WHERE data_type = ? AND timestamp < ?",
    #             (data_type, cutoff_time.isoformat())
    #         )
    #         await db.commit()
    #         logging.info(f"Cleaned up {cursor.rowcount} old entries for {data_type} (older than {cutoff_time.isoformat()})")


class GoogleSheetsManager:
    """
    Manages interactions with Google Sheets using gspread.
    Handles authorization and common sheet operations.
    """
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
            raise # Re-raise to prevent script from continuing without auth

    def append_data(self, sheet_id: str, sheet_name: str, data: List[List[Any]]) -> bool:
        """Appends a list of rows to the specified Google Sheet."""
        if not data:
            logging.warning(f"No data provided to append to {sheet_name} in sheet {sheet_id}.")
            return True # Consider it a success if nothing to append
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

# === DataFetcher Class ===
class DataFetcher:
    """
    Handles asynchronous HTTP requests with retry and rate limiting.
    Uses aiohttp for efficient network operations.
    """
    def __init__(self, cache: DataCache):
        self.cache = cache
        self.session = None # Will be initialized in async context manager

    async def __aenter__(self):
        """Initializes the aiohttp ClientSession."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Closes the aiohttp ClientSession."""
        if self.session:
            await self.session.close()
            logging.info("aiohttp ClientSession closed.")

    @backoff.on_exception(backoff.expo,
                          aiohttp.ClientError,
                          max_tries=Config.RETRY_ATTEMPTS,
                          factor=Config.RATE_LIMIT)
    @ratelimit.limits(calls=1, period=Config.RATE_LIMIT)
    async def fetch_url(self, url: str) -> Optional[str]:
        """
        Fetches content from a given URL with retries and rate limiting.
        """
        logging.info(f"Attempting to fetch URL: {url}")
        try:
            async with self.session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
                text = await response.text()
                logging.info(f"Successfully fetched URL: {url}")
                return text
        except aiohttp.ClientError as e:
            logging.error(f"HTTP error fetching {url}: {e}")
            return None
        except asyncio.TimeoutError:
            logging.error(f"Timeout fetching {url}.")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred while fetching {url}: {e}")
            return None

class DataUpdater:
    """
    Orchestrates the fetching, parsing, and updating of market data
    to Google Sheets and local files.
    """
    def __init__(self, cache: DataCache, gs_manager: GoogleSheetsManager):
        self.cache = cache
        self.gs_manager = gs_manager
        self.ensure_directories()

    @staticmethod
    def ensure_directories():
        """Ensures that the necessary data directories exist."""
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        logging.info(f"Ensured data directory exists: {Config.DATA_DIR}")

    def _safe_merge_csv(self, filepath: str, new_df: pd.DataFrame, 
                         key_cols: List[str], date_fmt: Optional[str] = None) -> None:
        """
        Merges new DataFrame with existing CSV, drops duplicates, and sorts.
        This is still used for NAV history if kept locally.
        """
        try:
            if os.path.exists(filepath):
                try:
                    existing_df = pd.read_csv(filepath)
                except pd.errors.EmptyDataError:
                    logging.warning(f"CSV file {filepath} is empty. Starting with an empty DataFrame.")
                    existing_df = pd.DataFrame()
                except Exception as e:
                    logging.error(f"Error reading existing CSV {filepath}: {e}. Starting with empty DataFrame.")
                    existing_df = pd.DataFrame() # Start with empty if file somehow corrupted/unreadable

                combined = pd.concat([existing_df, new_df])
                combined = combined.drop_duplicates(subset=key_cols, keep='last')

                if date_fmt:
                    # Convert to datetime, handle errors, then sort
                    # Use a temporary column for sorting to avoid modifying the original date column type
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
        """Fetches Nifty data and appends it to the Google Sheet."""
        logging.info("Starting Nifty update for Google Sheet...")
        try:
            html = await fetcher.fetch_url(f"{Config.URLs.INVESTING_BASE}/indices/s-p-cnx-nifty")
            if not html:
                logging.warning("No HTML fetched for Nifty.")
                return False

            soup = BeautifulSoup(html, "html.parser")
            # Using the same robust selector approach as currencies on Investing.com
            price = self._extract_price_from_soup(soup, [
                {'tag': 'div', 'data-test': 'instrument-price-last'},
                {'tag': 'div', 'class_': 'instrument-price-last'},
                {'tag': 'span', 'class_': 'last-price'} # Fallback
            ])

            if price is None:
                logging.warning("Could not parse Nifty price from Investing.com. Selector might have changed.")
                return False

            today_str = datetime.now().strftime("%Y-%m-%d") # Use YYYY-MM-DD for Sheets

            # Append to Google Sheet
            # Ensure your Google Sheet has columns like "Date", "Close"
            data_row = [today_str, price]
            # Assuming "Sheet1" is the target sheet name. Consider making this configurable.
            success = self.gs_manager.append_data(Config.Files.NIFTY_SHEET_ID, "Sheet1", [data_row])
            
            if success:
                logging.info(f"Updated Nifty price: {price} to Google Sheet.")
                # Cache the Nifty data
                await self.cache.set("Nifty", MarketData(datetime.now(), price, "Investing.com"))
            else:
                logging.error(f"Failed to write Nifty price {price} to Google Sheet.")
            return success

        except Exception as e:
            logging.error(f"Nifty update failed: {str(e)}")
            return False

    def _extract_price_from_soup(self, soup: BeautifulSoup, selectors: List[Dict[str, str]], 
                                  cleaner: Callable[[str], str] = lambda x: x.strip().replace("₹", "").replace("$", "").replace(",", "")) -> Optional[float]:
        """
        Attempts to extract a price from BeautifulSoup object using a list of selectors.
        Each selector is a dict like {'tag': 'span', 'class_': 'price-value'} or {'id': 'someId'}.
        Applies a cleaning function before conversion to float.
        """
        for selector in selectors:
            try:
                tag = soup.find(**selector)
                if tag and tag.text:
                    price_text = cleaner(tag.text)
                    price = float(price_text)
                    logging.debug(f"Successfully extracted price '{price_text}' using selector {selector}")
                    return price
            except (ValueError, TypeError, AttributeError) as e:
                logging.debug(f"Failed to extract price with selector {selector}: {e}")
            except Exception as e:
                logging.debug(f"Unexpected error with selector {selector}: {e}")
        return None

    async def update_gold(self, fetcher: DataFetcher) -> bool:
        """Fetches Gold data from multiple sources and appends to Google Sheet."""
        logging.info("Starting Gold update for Google Sheet...")
        gold_selectors = {
            "goodreturns.in": [
                {'tag': 'td', 'string': re.compile(r"22\s*carat", re.IGNORECASE)} # Needs next_sibling logic
            ],
            # Livemint.com removed as per user's report of it not working.
            # mcxindia.com now has dedicated parsing logic below, not using _extract_price_from_soup
        }

        for url in Config.URLs.GOLD_URLS:
            try:
                html = await fetcher.fetch_url(url)
                if not html:
                    logging.warning(f"No HTML fetched for Gold from {url}. Trying next URL.")
                    continue

                price = None
                source_name = url.split("//")[1].split("/")[0] # Extract domain for source
                soup = BeautifulSoup(html, "html.parser")

                if "goodreturns.in" in url:
                    # Special handling for goodreturns.in as it requires next_sibling
                    tag = soup.find("td", string=re.compile(r"22\s*carat", re.IGNORECASE))
                    if tag:
                        price_td = tag.find_next_sibling("td")
                        if price_td and price_td.text:
                            try:
                                # goodreturns often shows price per 1 gram, multiply by 10 for 10 grams
                                price = float(price_td.text.strip().replace("₹", "").replace(",", "")) * 10
                                logging.info(f"Parsed Gold price from goodreturns.in: {price}")
                            except ValueError as ve:
                                logging.warning(f"Could not convert goodreturns.in gold price '{price_td.text.strip()}' to float: {ve}")
                                price = None
                    if price is None:
                        logging.warning(f"Could not find 22 carat gold price on goodreturns.in from {url}.")
                
                elif "mcxindia.com" in url:
                    # Specific parsing logic for mcxindia.com based on provided screenshot
                    gold_symbol_td = soup.find("td", class_="symbol", string="GOLD")
                    if gold_symbol_td:
                        # The price is in the 4th td (index 3) of the same row
                        # Assuming the structure is <td>GOLD</td> <td>Unit</td> <td>Location</td> <td>Spot Price</td>
                        parent_tr = gold_symbol_td.find_parent("tr")
                        if parent_tr:
                            all_tds_in_row = parent_tr.find_all("td")
                            # Ensure there are enough columns and the 4th td exists
                            if len(all_tds_in_row) > 3 and all_tds_in_row[3]: 
                                price_td = all_tds_in_row[3] # 4th td is at index 3
                                if price_td and price_td.text:
                                    try:
                                        price = float(price_td.text.strip().replace(",", ""))
                                        logging.info(f"Parsed Gold price from mcxindia.com: {price}")
                                    except ValueError as ve:
                                        logging.warning(f"Could not convert mcxindia.com gold price '{price_td.text.strip()}' to float: {ve}")
                                        price = None
                    if price is None:
                        logging.warning(f"Could not parse gold price from mcxindia.com: {url}. Table structure or selectors might have changed.")

                if price:
                    today_str = datetime.now().strftime("%Y-%m-%d")
                    data_row = [today_str, price, source_name]
                    success = self.gs_manager.append_data(Config.Files.GOLD_SHEET_ID, "Sheet1", [data_row])
                    
                    if success:
                        logging.info(f"Updated Gold price: ₹{price} from {url} to Google Sheet.")
                        await self.cache.set("Gold", MarketData(datetime.now(), price, source_name))
                        return True # Return True as soon as one source succeeds
                    else:
                        logging.error(f"Failed to write Gold price {price} from {url} to Google Sheet.")
                        continue # Try next URL if writing fails

            except Exception as e:
                logging.error(f"Gold update failed for {url}: {str(e)}")
                continue # Try next URL

        logging.error("All Gold update sources failed or parsing logic not implemented for all.")
        return False

    async def update_currency(self, fetcher: DataFetcher) -> bool:
        """Fetches currency data for multiple pairs and appends to Google Sheet."""
        logging.info("Starting currency update for Google Sheet...")
        success_count = 0
        all_currency_data_rows = []

        currency_selectors = {
            "investing.com": [
                {'tag': 'div', 'data-test': 'instrument-price-last'}, # Derived from screenshot
                {'tag': 'div', 'class_': 'instrument-price-last'}, # Fallback
                {'tag': 'span', 'class_': 'last-price'} # Another common class fallback
            ],
            "coindesk.com": [
                {'tag': 'span', 'class_': 'currency-price'}, # Derived from screenshot
                {'tag': 'div', 'class_': 'CoinDeskcoinPrice'} # Placeholder fallback
            ]
        }

        for currency_pair, endpoint in Config.URLs.CURRENCY_ENDPOINTS.items():
            url = f"{Config.URLs.INVESTING_BASE}{endpoint}" if not endpoint.startswith("http") else endpoint
            try:
                html = await fetcher.fetch_url(url)
                if not html:
                    logging.warning(f"No HTML fetched for {currency_pair} from {url}. Skipping.")
                    continue

                price = None
                source_name = url.split("//")[1].split("/")[0] if url.startswith("http") else "Investing.com"
                soup = BeautifulSoup(html, "html.parser")

                if "investing.com" in url:
                    price = self._extract_price_from_soup(soup, currency_selectors["investing.com"])
                    if price is None:
                        logging.warning(f"Could not parse price for {currency_pair} from investing.com: {url}. Selectors might have changed.")
                
                elif "coindesk.com" in url:
                    price = self._extract_price_from_soup(soup, currency_selectors["coindesk.com"])
                    if price is None:
                        logging.warning(f"Could not parse price for {currency_pair} from coindesk.com: {url}. Selectors might have changed.")

                if price:
                    today_str = datetime.now().strftime("%Y-%m-%d")
                    all_currency_data_rows.append([today_str, currency_pair, price, source_name])
                    logging.info(f"Prepared {currency_pair} rate: {price} from {url} for Google Sheet.")
                    success_count += 1
                    await self.cache.set(f"Currency_{currency_pair}", MarketData(datetime.now(), price, source_name))
                else:
                    logging.warning(f"Could not parse price for {currency_pair} from {url}. Skipping this currency pair.")
                    continue # Continue to next currency pair even if one fails
            except Exception as e:
                logging.error(f"Currency update failed for {currency_pair} from {url}: {str(e)}")
                continue # Continue to next currency pair even if one fails
        
        if all_currency_data_rows:
            success = self.gs_manager.append_data(Config.Files.CURRENCY_SHEET_ID, "Sheet1", all_currency_data_rows)
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
        """
        Fetches Mutual Fund NAV data from AMFI and appends it to a Google Sheet.
        This replaces the local CSV storage for NAV history.
        """
        logging.info("Starting NAV update for Google Sheet...")
        try:
            nav_data_raw = await fetcher.fetch_url(Config.URLs.AMFI_NAV)
            if not nav_data_raw:
                logging.error("Failed to fetch raw NAV data from AMFI.")
                return False

            # Parse AMFI NAV data
            # AMFI provides a semi-colon separated text file.
            nav_lines = nav_data_raw.strip().split('\n')
            
            # Find the header line (usually starts with "Scheme Code")
            header_line_index = -1
            for i, line in enumerate(nav_lines):
                if "Scheme Code" in line and "Net Asset Value" in line:
                    header_line_index = i
                    break
            
            if header_line_index == -1:
                logging.error("Could not find header in AMFI NAV data. AMFI file format might have changed.")
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
                else:
                    logging.debug(f"Skipping malformed NAV line: {line}")
            
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
            df_nav = df_nav.dropna(subset=['NAV']) # Drop rows where NAV could not be converted

            # Convert Date to datetime object for Google Sheets (YYYY-MM-DD)
            # AMFI date format is typically 'DD-Mon-YYYY' (e.g., '25-Jul-2025')
            df_nav['Date'] = pd.to_datetime(df_nav['Date'], format='%d-%b-%Y', errors='coerce')
            df_nav = df_nav.dropna(subset=['Date']) # Drop rows where date conversion failed
            df_nav['Date'] = df_nav['Date'].dt.strftime('%Y-%m-%d') # Standardize date format for Sheets

            # Prepare data for Google Sheets (list of lists)
            # Ensure your Google Sheet has columns like "Date", "Fund Code", "Fund Name", "NAV"
            nav_data_for_sheet = df_nav[['Date', 'Fund Code', 'Fund Name', 'NAV']].values.tolist()

            # Append to Google Sheet
            # Assuming "Sheet1" is the target sheet name for NAV. Consider making this configurable.
            success = self.gs_manager.append_data(Config.Files.NAV_SHEET_ID, "Sheet1", nav_data_for_sheet)
            
            if success:
                logging.info(f"Successfully appended {len(nav_data_for_sheet)} NAV records to Google Sheet.")
                # Cache the NAV update status
                await self.cache.set("NAV_Update_Status", MarketData(datetime.now(), len(nav_data_for_sheet), "AMFI", {"records_count": len(nav_data_for_sheet)}))
            else:
                logging.error(f"Failed to write NAV data to Google Sheet.")
            return success

        except Exception as e:
            logging.error(f"NAV update failed: {str(e)}")
            return False

async def main():
    """Main function to orchestrate the data fetching and updating process."""
    # Retrieve Google Service Account credentials from environment variable
    google_sa_key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
    if not google_sa_key_json:
        logging.error("GOOGLE_SERVICE_ACCOUNT_KEY environment variable not set. Cannot proceed with Google Sheets updates.")
        return False

    try:
        gs_manager = GoogleSheetsManager(google_sa_key_json)
    except Exception as e:
        logging.error(f"Failed to initialize GoogleSheetsManager: {e}")
        return False # Exit if Google Sheets manager cannot be initialized

    cache = DataCache(Config.Files.CACHE_DB)
    # IMPORTANT: Await the database initialization now that it's an async method
    await cache.initialize_db() 
    
    updater = DataUpdater(cache, gs_manager)
    
    # Ensure local data directory exists for NAV history and cache DB
    updater.ensure_directories()

    async with DataFetcher(cache) as fetcher:
        tasks = [
            updater.update_nifty(fetcher),
            updater.update_gold(fetcher),
            updater.update_currency(fetcher),
            updater.update_nav(fetcher), # NAV history now updates Google Sheet
        ]
        # Run all update tasks concurrently. return_exceptions=True allows all tasks to complete
        # even if some fail, and their exceptions are returned as results.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if all tasks completed successfully (returned True, not an exception)
        success = all(isinstance(r, bool) and r for r in results)
        
        if success:
            logging.info("All data updates completed successfully")
        else:
            logging.error("Some data updates failed. Check logs for details.")
            # Log specific failures
            for i, r in enumerate(results):
                if not (isinstance(r, bool) and r):
                    logging.error(f"Task {i} failed: {r}")
        
        return success

if __name__ == "__main__":
    # Run the main asynchronous function
    asyncio.run(main())
