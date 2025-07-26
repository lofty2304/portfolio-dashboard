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

# NEW: API Keys for new services
Config.API_KEY_EXCHANGE_RATE: str = os.getenv("EXCHANGE_RATE_API_KEY", "YOUR_EXCHANGE_RATE_API_KEY")
Config.API_KEY_FMP: str = os.getenv("FMP_API_KEY", "YOUR_FMP_API_KEY")
Config.API_KEY_GOLDAPI: str = os.getenv("GOLDAPI_API_KEY", "YOUR_GOLDAPI_API_KEY") # NEW GoldAPI Key
Config.API_KEY_FRED: str = os.getenv("FRED_API_KEY", "YOUR_FRED_API_KEY") # NEW FRED API Key
Config.API_KEY_TWELVE_DATA: str = os.getenv("TWELVE_DATA_API_KEY", "YOUR_TWELVE_DATA_API_KEY") # NEW Twelve Data API Key
Config.API_KEY_POLYGON: str = os.getenv("POLYGON_API_KEY", "YOUR_POLYGON_API_KEY") # NEW Polygon.io API Key


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
    FRED_SHEET_ID: str = os.getenv("GOOGLE_SHEET_FRED_ID", "1WuiOm26IiU9UoJDFdQXHhDtQERATr6WAfnNr78zNP_w") # UPDATED FRED Sheet ID


# Nested URLs class
class URLs:
    """URLs for data fetching."""
    INVESTING_BASE: str = "https://www.investing.com" # Still used for Nifty if FMP/Twelve Data/Polygon fails or for other data
    
    # NEW API Base URLs
    EXCHANGE_RATE_BASE: str = "https://v6.exchangerate-api.com/v6"
    FMP_BASE: str = "https://financialmodelingprep.com/api/v3"
    GOLDAPI_BASE: str = "https://www.goldapi.io/api" # NEW GoldAPI Base URL
    FRED_BASE: str = "https://api.stlouisfed.org/fred" # NEW FRED API Base URL
    TWELVE_DATA_BASE: str = "https://api.twelvedata.com" # NEW Twelve Data API Base URL
    POLYGON_BASE: str = "https://api.polygon.io" # NEW Polygon.io API Base URL

    # Gold URLs now only for API, web scraping URLs removed
    GOLD_URLS: List[str] = [
        # "https://www.goodreturns.in/gold-rates/", # Removed: Using GoldAPI.io
        # "https://www.mcxindia.com/market-data/spot-market-price" # Removed: Using GoldAPI.io
    ]
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
    async def fetch_url(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Fetches content from a given URL with retries and rate limiting.
        Supports optional parameters and headers for API calls.
        """
        logging.info(f"Attempting to fetch URL: {url} with params: {params} and headers: {headers}")
        try:
            async with self.session.get(url, params=params, headers=headers, timeout=Config.REQUEST_TIMEOUT) as response:
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
        """
        Fetches Nifty data, first from FMP API, then from Twelve Data API as a fallback,
        then from Polygon.io API as a third fallback, and appends it to the Google Sheet.
        """
        logging.info("Starting Nifty update for Google Sheet...")
        price = None
        source = None

        # --- Attempt 1: Financial Modeling Prep (FMP) API ---
        if Config.API_KEY_FMP and Config.API_KEY_FMP != "YOUR_FMP_API_KEY":
            logging.info("Attempting to fetch Nifty from Financial Modeling Prep API...")
            symbol = '^NSEI' # Common symbol for Nifty 50. Verify FMP documentation for free tier support.
            params = {"apikey": Config.API_KEY_FMP}
            url = f"{Config.URLs.FMP_BASE}/quote/{symbol}"

            try:
                json_data_raw = await fetcher.fetch_url(url, params=params)
                if json_data_raw:
                    data = json.loads(json_data_raw)
                    if data and len(data) > 0 and data[0].get('price') is not None:
                        price = float(data[0]['price'])
                        source = "Financial Modeling Prep"
                        logging.info(f"Successfully fetched Nifty from FMP: {price}")
            except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
                logging.warning(f"FMP API failed for Nifty ({symbol}): {e}. Trying next source.")
            except Exception as e:
                logging.warning(f"Unexpected error with FMP API for Nifty ({symbol}): {e}. Trying next source.")

        # --- Attempt 2: Twelve Data API (Fallback) ---
        if price is None and Config.API_KEY_TWELVE_DATA and Config.API_KEY_TWELVE_DATA != "YOUR_TWELVE_DATA_API_KEY":
            logging.info("Attempting to fetch Nifty from Twelve Data API (fallback)...")
            # Twelve Data symbol for Nifty 50 might be 'NIFTY_50' or '^NSEI' depending on exchange.
            # 'NIFTY_50' is often more reliable for indices on Twelve Data.
            symbol_td = 'NIFTY_50' 
            params_td = {
                "symbol": symbol_td,
                "interval": "1min", # Using 1min for latest price, adjust as needed (e.g., '1day' for daily close)
                "apikey": Config.API_KEY_TWELVE_DATA
            }
            url_td = f"{Config.URLs.TWELVE_DATA_BASE}/time_series"

            try:
                json_data_raw_td = await fetcher.fetch_url(url_td, params=params_td)
                if json_data_raw_td:
                    data_td = json.loads(json_data_raw_td)
                    if data_td and data_td.get('status') == 'ok' and data_td.get('values') and len(data_td['values']) > 0:
                        # Get the latest close price
                        price = float(data_td['values'][0]['close'])
                        source = "Twelve Data"
                        logging.info(f"Successfully fetched Nifty from Twelve Data: {price}")
                    elif data_td.get('status') == 'error':
                        logging.warning(f"Twelve Data API error for Nifty ({symbol_td}): {data_td.get('message')}. Trying next source.")
            except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
                logging.warning(f"Twelve Data API failed for Nifty ({symbol_td}): {e}. Trying next source.")
            except Exception as e:
                logging.warning(f"Unexpected error with Twelve Data API for Nifty ({symbol_td}): {e}. Trying next source.")

        # --- Attempt 3: Polygon.io API (Third Fallback) ---
        if price is None and Config.API_KEY_POLYGON and Config.API_KEY_POLYGON != "YOUR_POLYGON_API_KEY":
            logging.info("Attempting to fetch Nifty from Polygon.io API (third fallback)...")
            # Polygon.io ticker for Nifty 50 is typically I:NSE50
            symbol_poly = 'I:NSE50' 
            # Using /v2/aggs/ticker/{ticker}/prev for previous day's close
            url_poly = f"{Config.URLs.POLYGON_BASE}/v2/aggs/ticker/{symbol_poly}/prev"
            params_poly = {
                "apiKey": Config.API_KEY_POLYGON
            }

            try:
                json_data_raw_poly = await fetcher.fetch_url(url_poly, params=params_poly)
                if json_data_raw_poly:
                    data_poly = json.loads(json_data_raw_poly)
                    if data_poly and data_poly.get('status') == 'OK' and data_poly.get('results') and len(data_poly['results']) > 0:
                        # Get the close price from the results array
                        price = float(data_poly['results'][0]['c']) # 'c' stands for close price
                        source = "Polygon.io"
                        logging.info(f"Successfully fetched Nifty from Polygon.io: {price}")
                    elif data_poly.get('status') == 'NOT_FOUND':
                         logging.warning(f"Polygon.io API error for Nifty ({symbol_poly}): Symbol not found. Trying next source.")
                    elif data_poly.get('status') == 'ERROR':
                         logging.warning(f"Polygon.io API error for Nifty ({symbol_poly}): {data_poly.get('error')}. Trying next source.")
            except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
                logging.warning(f"Polygon.io API failed for Nifty ({symbol_poly}): {e}. No more sources.")
            except Exception as e:
                logging.warning(f"Unexpected error with Polygon.io API for Nifty ({symbol_poly}): {e}. No more sources.")


        if price is None:
            logging.error("Failed to fetch Nifty price from all available API sources.")
            return False

        today_str = datetime.now().strftime("%Y-%m-%d")
        data_row = [today_str, price]
        success = self.gs_manager.append_data(Config.Files.NIFTY_SHEET_ID, "Sheet1", [data_row])
        
        if success:
            logging.info(f"Updated Nifty price: {price} to Google Sheet via {source}.")
            await self.cache.set("Nifty", MarketData(datetime.now(), price, source))
        else:
            logging.error(f"Failed to write Nifty price {price} to Google Sheet.")
        return success

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
        """Fetches Gold data from GoldAPI.io and appends to Google Sheet."""
        logging.info("Starting Gold update for Google Sheet via GoldAPI.io...")
        if not Config.API_KEY_GOLDAPI or Config.API_KEY_GOLDAPI == "YOUR_GOLDAPI_API_KEY":
            logging.error("GoldAPI.io key not set. Skipping Gold update.")
            return False

        # GoldAPI.io endpoint for XAU (Gold) in INR
        url = f"{Config.URLs.GOLDAPI_BASE}/XAU/INR"
        headers = {
            "x-access-token": Config.API_KEY_GOLDAPI,
            "Content-Type": "application/json"
        }

        try:
            json_data_raw = await fetcher.fetch_url(url, headers=headers)
            if not json_data_raw:
                logging.warning(f"No data fetched for Gold from GoldAPI.io.")
                return False
            
            data = json.loads(json_data_raw)

            if data.get("error"):
                logging.error(f"GoldAPI.io error: {data['error']}. Response: {data}")
                return False
            
            # GoldAPI.io provides 'price'
            price = float(data.get('price'))
            if price is None:
                logging.warning(f"Could not find 'price' in GoldAPI.io response. Response: {data}")
                return False

            today_str = datetime.now().strftime("%Y-%m-%d")
            data_row = [today_str, price, "GoldAPI.io"]
            success = self.gs_manager.append_data(Config.Files.GOLD_SHEET_ID, "Sheet1", [data_row])
            
            if success:
                logging.info(f"Updated Gold price: ₹{price} from GoldAPI.io to Google Sheet.")
                await self.cache.set("Gold", MarketData(datetime.now(), price, "GoldAPI.io"))
                return True
            else:
                logging.error(f"Failed to write Gold price {price} from GoldAPI.io to Google Sheet.")
                return False

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON from GoldAPI.io: {e}")
            return False
        except KeyError as e:
            logging.error(f"Missing key in GoldAPI.io response: {e}. Response: {data}")
            return False
        except Exception as e:
            logging.error(f"Gold update failed via GoldAPI.io: {str(e)}")
            return False

    async def update_currency(self, fetcher: DataFetcher) -> bool:
        """Fetches currency data from ExchangeRate-API and appends to Google Sheet."""
        logging.info("Starting currency update for Google Sheet via ExchangeRate-API...")
        if not Config.API_KEY_EXCHANGE_RATE or Config.API_KEY_EXCHANGE_RATE == "YOUR_EXCHANGE_RATE_API_KEY":
            logging.error("ExchangeRate-API key not set. Skipping currency update.")
            return False

        # Define base currencies for ExchangeRate-API
        # ExchangeRate-API works by fetching 'latest' rates for a BASE currency
        # and then you extract the target currency from the 'rates' object.
        base_currencies_to_fetch = {
            "USDINR": "USD",
            "EURINR": "EUR",
            "BTCINR": "BTC" # ExchangeRate-API primarily fiat, BTC might not work here.
        }
        target_currency = "INR"
        
        success_count = 0
        all_currency_data_rows = []

        for currency_pair, base_currency in base_currencies_to_fetch.items():
            url = f"{Config.URLs.EXCHANGE_RATE_BASE}/{Config.API_KEY_EXCHANGE_RATE}/latest/{base_currency}"
            
            try:
                json_data_raw = await fetcher.fetch_url(url)
                if not json_data_raw:
                    logging.warning(f"No data fetched for {currency_pair} from ExchangeRate-API (base: {base_currency}).")
                    continue
                
                data = json.loads(json_data_raw)

                if data.get("result") != "success":
                    logging.error(f"ExchangeRate-API error for {currency_pair} (base: {base_currency}): {data.get('error-type', 'Unknown error')}")
                    continue
                if target_currency not in data.get("rates", {}):
                    logging.warning(f"Target currency '{target_currency}' not found in rates for {currency_pair} (base: {base_currency}). This may happen for crypto pairs like BTCINR. Response: {data}")
                    continue

                price = float(data["rates"][target_currency])
                today_str = datetime.now().strftime("%Y-%m-%d")

                all_currency_data_rows.append([today_str, currency_pair, price, "ExchangeRate-API"])
                logging.info(f"Prepared {currency_pair} rate: {price} from ExchangeRate-API for Google Sheet.")
                success_count += 1
                await self.cache.set(f"Currency_{currency_pair}", MarketData(datetime.now(), price, "ExchangeRate-API"))
            
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON from ExchangeRate-API for {currency_pair} (base: {base_currency}): {e}")
                continue
            except KeyError as e:
                logging.error(f"Missing key in ExchangeRate-API response for {currency_pair} (base: {base_currency}): {e}. Response: {data}")
                continue
            except Exception as e:
                logging.error(f"Currency update failed for {currency_pair} via ExchangeRate-API: {str(e)}")
                continue
        
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

    async def update_fred_data(self, fetcher: DataFetcher) -> bool:
        """Fetches FRED data (e.g., India CPI) and appends to Google Sheet."""
        logging.info("Starting FRED data update for Google Sheet...")
        if not Config.API_KEY_FRED or Config.API_KEY_FRED == "YOUR_FRED_API_KEY":
            logging.error("FRED API key not set. Skipping FRED data update.")
            return False

        series_id = 'CPALTT01INM657N' # Consumer Price Index: All Items for India
        url = f"{Config.URLs.FRED_BASE}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": Config.API_KEY_FRED,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 1 # Get only the latest observation
        }

        try:
            json_data_raw = await fetcher.fetch_url(url, params=params)
            if not json_data_raw:
                logging.warning(f"No data fetched for FRED series {series_id}.")
                return False
            
            data = json.loads(json_data_raw)

            if not data.get("observations") or len(data["observations"]) == 0:
                logging.warning(f"FRED API returned no observations for series {series_id}. Response: {data}")
                return False

            latest_observation = data["observations"][0]
            value = float(latest_observation['value'])
            date = latest_observation['date'] # FRED date is YYYY-MM-DD

            data_row = [date, series_id, value, "FRED"]
            success = self.gs_manager.append_data(Config.Files.FRED_SHEET_ID, "Sheet1", [data_row])
            
            if success:
                logging.info(f"Updated FRED series {series_id} value: {value} to Google Sheet.")
                await self.cache.set(f"FRED_{series_id}", MarketData(datetime.now(), value, "FRED", {"series_id": series_id}))
            else:
                logging.error(f"Failed to write FRED series {series_id} to Google Sheet.")
            return success

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON from FRED API for series {series_id}: {e}")
            return False
        except (KeyError, IndexError, TypeError, ValueError) as e:
            logging.error(f"Missing key/invalid structure in FRED API response for series {series_id}: {e}. Response: {data}")
            return False
        except Exception as e:
            logging.error(f"FRED data update failed for series {series_id}: {str(e)}")
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
            updater.update_fred_data(fetcher), # FRED data update
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
