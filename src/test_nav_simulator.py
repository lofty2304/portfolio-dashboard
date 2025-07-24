import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator

# Simulate for 3 schemes over 120 days
dates = pd.date_range(end=pd.Timestamp.today(), periods=120)
schemes = [100001, 100002, 100003]

frames = []

for code in schemes:
    navs = 100 + np.cumsum(np.random.randn(len(dates)))  # random walk
    df = pd.DataFrame({
        "date": dates,
        "scheme_code": code,
        "nav": navs
    })
    df["sma_30"] = SMAIndicator(close=df["nav"], window=30).sma_indicator()
    df["rsi_14"] = RSIIndicator(close=df["nav"], window=14).rsi()
    macd = MACD(close=df["nav"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    frames.append(df)

df_full = pd.concat(frames)

df_full.to_parquet("data/funds/indicators/nav_simulated.parquet", index=False)
print("✅ Simulated indicator dataset saved.")
