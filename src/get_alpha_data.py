import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
ALPHA_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# Replace this with NSE/BSE stock symbol or fund proxy (like INF, AMFI codes may be unavailable on free APIs)
symbol = "AAPL"  # You can test using Apple stock

# Use Alpha Vantage TIME_SERIES_DAILY
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=compact&apikey={ALPHA_KEY}"

response = requests.get(url)
data = response.json()

if "Time Series (Daily)" in data:
    ts = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(ts, orient="index")
    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. adjusted close": "adj_close",
        "6. volume": "volume"
    })
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Convert all cols to numeric
    df = df.apply(pd.to_numeric)

    print(df.tail())

    # Save to CSV
    os.makedirs("data", exist_ok=True)
    df.to_csv(f"data/{symbol}_alpha.csv")
else:
    print("Error from Alpha Vantage:", data.get("Note") or data.get("Error Message") or data)
