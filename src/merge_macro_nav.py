# src/merge_macro_nav.py

import pandas as pd
from pathlib import Path

# 1. Load fund indicators
ind = pd.read_parquet("data/funds/indicators/nav_indicators_latest.parquet")
ind["date"] = pd.to_datetime(ind["date"])

# 2. Load and forward‐fill GDP (quarterly → daily)
gdp = pd.read_csv("data/GDP_fred.csv", parse_dates=["Date"], index_col="Date")
gdp_daily = gdp.resample("D").ffill().rename(columns={gdp.columns[0]: "GDP"})

# 3. Load and forward‐fill CPI (monthly → daily)
cpi = pd.read_csv("data/CPIAUCSL_fred.csv", parse_dates=["Date"], index_col="Date")
cpi_daily = cpi.resample("D").ffill().rename(columns={cpi.columns[0]: "CPI"})

# 4. Merge all series
merged = (
    ind
    .merge(gdp_daily,   left_on="date", right_index=True, how="left")
    .merge(cpi_daily,   left_on="date", right_index=True, how="left")
)

# 5. Save merged data
out_path = Path("data/merged/nav_macro_merged.parquet")
out_path.parent.mkdir(parents=True, exist_ok=True)
merged.to_parquet(out_path, index=False)

print(f"✅ Merged NAV+macro saved to {out_path}")
