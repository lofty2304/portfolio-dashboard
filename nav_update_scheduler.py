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
# from apscheduler.schedulers.blocking import BlockingScheduler # Removed for GitHub Actions
import backoff
import ratelimit
import aiosqlite
import json

# === Setup Logging ===
logging.basicConfig(
    filename='portfolio_updater.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Config Class with Fixed Structure ===
class Config:
    # Base paths and settings (define these first)
    DATA_DIR: str = "src/data"
    # SCHEDULE_HOUR: int = 18  # 6 PM IST - No longer needed for single-run job
    # SCHEDULE_MINUTE: int = 30 # No longer needed for single-run job
    RETRY_ATTEMPTS: int = 3
    REQUEST_TIMEOUT: int = 10
    RATE_LIMIT: int = 2

    class Files:
        # Now we can reference Config.DATA_DIR since it's defined above
        NIFTY_CSV: str = f"{Config.DATA_DIR}/nifty.csv"
        GOLD_CSV: str = f"{Config.DATA_DIR}/gold.csv"
        CURRENCY_CSV: str = f"{Config.DATA_DIR}/currency.csv"
        NAV_HISTORY_CSV: str = f"{Config.DATA_DIR}/nav_history.csv"
        FUND_TRACKER_EXCEL: str = "Fund-Tracker-original.xlsx"
        FUND_SHEET: str = "Fund Tracker"
        CACHE_DB: str = f"{Config.DATA_DIR}/cache.db" # Added cache DB path

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

class DataFetcher:
    def __init__(self, cache: DataCache):
        self.session = None
        self.cache = cache
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=Config.RETRY_ATTEMPTS)
    @ratelimit.limits(calls=Config.RATE_LIMIT, period=1)
    async def fetch_url(self, url: str) -> Optional[str]:
        try:
            async with self.session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            logging.error(f"Failed to fetch {url}: {str(e)}")
            return None

class DataUpdater:
    def __init__(self, cache: DataCache):
        self.cache = cache
        self.ensure_directories()

    @staticmethod
    def ensure_directories():
        os.makedirs(Config.DATA_DIR, exist_ok=True)

    def _safe_merge_csv(self, filepath: str, new_df: pd.DataFrame, 
                        key_cols: List[str], date_fmt: Optional[str] = None) -> None:
        try:
            if os.path.exists(filepath):
                backup_path = filepath.replace('.csv', f'_backup_{datetime.now():%Y%m%d_%H%M%S}.csv')
                shutil.copy2(filepath, backup_path)

            try:
                existing_df = pd.read_csv(filepath)
            except FileNotFoundError:
                new_df.to_csv(filepath, index=False)
                return

            combined = pd.concat([existing_df, new_df])
            combined = combined.drop_duplicates(subset=key_cols, keep='last')

            if date_fmt:
                combined['_sort_date'] = pd.to_datetime(combined[key_cols[0]], format=date_fmt)
                combined = combined.sort_values('_sort_date').drop(columns=['_sort_date'])

            combined.to_csv(filepath, index=False)

        except Exception as e:
            logging.error(f"Failed to merge CSV {filepath}: {str(e)}")
            raise

    async def update_nifty(self, fetcher: DataFetcher) -> bool:
        try:
            html = await fetcher.fetch_url(f"{Config.URLs.INVESTING_BASE}/indices/s-p-cnx-nifty")
            if not html:
                return False

            match = re.search(r'last\">(\d{4,5}\.\d+)', html)
            if not match:
                return False

            price = float(match.group(1))
            today = datetime.now().strftime("%d-%b-%y")
            
            data = MarketData(
                timestamp=datetime.now(),
                value=price,
                source="investing.com"
            )
            await self.cache.set("nifty", data)
            
            df = pd.DataFrame([{"Date": today, "Close": price}])
            self._safe_merge_csv(Config.Files.NIFTY_CSV, df, ["Date"], "%d-%b-%y")
            
            logging.info(f"Updated Nifty price: {price}")
            return True

        except Exception as e:
            logging.error(f"Nifty update failed: {str(e)}")
            return False

    async def update_gold(self, fetcher: DataFetcher) -> bool:
        for url in Config.URLs.GOLD_URLS:
            try:
                html = await fetcher.fetch_url(url)
                if not html:
                    continue

                price = None
                if "goodreturns.in" in url:
                    soup = BeautifulSoup(html, "html.parser")
                    tag = soup.find("td", string=re.compile("22 carat", re.IGNORECASE))
                    if tag:
                        price_td = tag.find_next_sibling("td")
                        if price_td and price_td.text:
                            price = float(price_td.text.strip().replace("₹", "").replace(",", "")) * 10

                if price:
                    today = datetime.now().strftime("%d-%m-%Y")
                    data = MarketData(
                        timestamp=datetime.now(),
                        value=price,
                        source=url
                    )
                    await self.cache.set("gold", data)
                    
                    df = pd.DataFrame([{"Date": today, "Price": price}])
                    self._safe_merge_csv(Config.Files.GOLD_CSV, df, ["Date"], "%d-%m-%Y")
                    
                    logging.info(f"Updated Gold price: ₹{price} from {url}")
                    return True

            except Exception as e:
                logging.error(f"Gold update failed for {url}: {str(e)}")
                continue

        return False

    # Placeholder for other update functions (update_currency, update_nav)
    # You will need to implement these similarly, handling their specific scraping logic.
    async def update_currency(self, fetcher: DataFetcher) -> bool:
        logging.info("Starting currency update...")
        success_count = 0
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
                    # Example selector for Investing.com prices, needs verification
                    price_tag = soup.find('div', class_='instrument-price-last')
                    if price_tag:
                        price = float(price_tag.text.strip().replace(',', ''))
                elif "coindesk.com" in url:
                    soup = BeautifulSoup(html, "html.parser")
                    price_tag = soup.find('span', class_='currency-price')
                    if price_tag:
                        price = float(price_tag.text.strip().replace('₹', '').replace(',', ''))

                if price:
                    today = datetime.now().strftime("%Y-%m-%d")
                    data = MarketData(
                        timestamp=datetime.now(),
                        value=price,
                        source=url,
                        metadata={"currency_pair": currency_pair}
                    )
                    await self.cache.set(f"currency_{currency_pair}", data)
                    
                    # Assuming a simple CSV structure for currency data
                    df = pd.DataFrame([{"Date": today, "CurrencyPair": currency_pair, "Rate": price}])
                    self._safe_merge_csv(Config.Files.CURRENCY_CSV, df, ["Date", "CurrencyPair"], "%Y-%m-%d")
                    
                    logging.info(f"Updated {currency_pair} rate: {price} from {url}")
                    success_count += 1
                else:
                    logging.warning(f"Could not parse price for {currency_pair} from {url}")

            except Exception as e:
                logging.error(f"Currency update failed for {currency_pair} from {url}: {str(e)}")
                continue
        return success_count > 0

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

            # Merge with existing NAV history
            self._safe_merge_csv(Config.Files.NAV_HISTORY_CSV, df_nav, ["Date", "Fund Code"], "%Y-%m-%d")
            
            logging.info(f"Successfully updated NAV history with {len(df_nav)} records.")
            return True

        except Exception as e:
            logging.error(f"NAV update failed: {str(e)}")
            return False

async def main():
    cache = DataCache(Config.Files.CACHE_DB)
    updater = DataUpdater(cache)
    
    async with DataFetcher(cache) as fetcher:
        tasks = [
            updater.update_nifty(fetcher),
            updater.update_gold(fetcher),
            updater.update_currency(fetcher), # Added currency update
            updater.update_nav(fetcher),     # Added NAV update
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = all(isinstance(r, bool) and r for r in results)
        
        if success:
            logging.info("All data updates completed successfully")
        else:
            logging.error("Some data updates failed")
        
        # After all updates, the CSV files in src/data will be updated locally within the container.
        # The GitHub Actions workflow will then handle pushing these updated files to the repo.
        return success

if __name__ == "__main__":
    # This part is removed for GitHub Actions single-run execution
    # scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    
    # def scheduled_update():
    #     asyncio.run(main())
    
    # # Run immediate update
    # scheduled_update()
    
    # # Schedule daily updates
    # scheduler.add_job(
    #     scheduled_update, 
    #     'cron', 
    #     hour=Config.SCHEDULE_HOUR, 
    #     minute=Config.SCHEDULE_MINUTE
    # )
    
    # print(f"⏰ Scheduler running - Updates will run daily at {Config.SCHEDULE_HOUR:02d}:{Config.SCHEDULE_MINUTE:02d} IST")
    # print("Press Ctrl+C to exit")
    
    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     print("\n👋 Scheduler stopped")
    
    # For GitHub Actions, simply run the main function and exit
    asyncio.run(main())
