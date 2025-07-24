# src/get_fred_data.py

import os
from fredapi import Fred
from dotenv import load_dotenv
import pandas as pd

# ✅ Load API Key from .env
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")
print("⛳ DEBUG: YOUR FRED_API_KEY IS:", FRED_API_KEY)

# ✅ Initialize FRED client
fred = Fred(api_key=FRED_API_KEY)

# ✅ Choose series to fetch (GDP, CPI, etc.)
series_id = "CPIAUCSL"

# ✅ Fetch time series data
data = fred.get_series(series_id)

# ✅ Convert to DataFrame and save
df = pd.DataFrame(data, columns=[series_id])
df.index.name = 'Date'

print(df.tail())

# Save as CSV (optional)
os.makedirs("data", exist_ok=True)
df.to_csv(f"data/{series_id}_fred.csv")
