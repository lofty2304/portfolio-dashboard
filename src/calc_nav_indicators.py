import pathlib
import pandas as pd
from ta.trend import SMAIndicator, MACD
from ta.momentum import RSIIndicator

# Directory containing daily NAV files
NAV_DIR = pathlib.Path("data/funds/daily")
# Output directory for indicator results
OUT_DIR = pathlib.Path("data/funds/indicators")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Process the most recent NAV file
latest_file = max(NAV_DIR.glob("mf_nav_amfi_*.csv"))
df = pd.read_csv(latest_file, parse_dates=["date"])

# Ensure numeric types
df["nav"] = pd.to_numeric(df["nav"], errors="coerce")

# Group by mutual fund scheme code
results = []
for scheme_code, group in df.groupby("scheme_code"):
    group = group.sort_values("date").reset_index(drop=True)
    # 30-day Simple Moving Average of NAV
    group["sma_30"] = SMAIndicator(close=group["nav"], window=30, fillna=False).sma_indicator()
    # 14-period RSI of NAV
    group["rsi_14"] = RSIIndicator(close=group["nav"], window=14, fillna=False).rsi()
    # MACD and signal line (12- and 26-period EMAs, 9-period signal)
    macd = MACD(close=group["nav"], window_slow=26, window_fast=12, window_sign=9, fillna=False)
    group["macd"] = macd.macd()
    group["macd_signal"] = macd.macd_signal()
    results.append(group)

# Combine all schemes and save
ind_df = pd.concat(results, ignore_index=True)
ind_df.to_parquet(OUT_DIR / "nav_indicators_latest.parquet", index=False)

print("✅ Indicator calculation complete. Results saved to:", OUT_DIR / "nav_indicators_latest.parquet")
