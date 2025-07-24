import pandas as pd

# Load merged data
df = pd.read_parquet("data/merged/nav_macro_merged.parquet")

# Ensure data is sorted and dated properly if needed
df = df.sort_values(["scheme_code", "date"]).reset_index(drop=True)

# Calculate quarter-over-quarter GDP growth for each row
# Assuming GDP is daily forward-filled, compare to 3 months before
df["GDP_lag_q"] = df.groupby("scheme_code")["GDP"].shift(63)  # approx. 63 trading days per quarter
df["GDP_growth_q"] = (df["GDP"] - df["GDP_lag_q"]) / df["GDP_lag_q"]

# Initialize signal column
df["signal"] = "HOLD"

# BUY condition
buy_cond = (df["rsi_14"] < 30) & (df["GDP_growth_q"] > 0)
df.loc[buy_cond, "signal"] = "BUY"

# SELL condition
sell_cond = (df["rsi_14"] > 70) | (df["macd"] < df["macd_signal"])
df.loc[sell_cond, "signal"] = "SELL"

# Save signals file
df.to_parquet("data/merged/nav_macro_signals.parquet", index=False)

# Summary counts for quick sanity check
print("Signal counts:\n", df["signal"].value_counts())
print("Sample signals:\n", df[["date", "scheme_code", "rsi_14", "GDP_growth_q", "signal"]].head(10))
