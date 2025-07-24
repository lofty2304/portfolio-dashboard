import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import scipy.optimize as opt
import requests

# === Currency Tracker ===
@st.cache_data(show_spinner=True)
def load_currency_rates():
    API_KEY = "fa1b936cd85be54a6587c4f0"  # Your ExchangeRate-API key
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"

    try:
        response = requests.get(url)
        data = response.json()

        if data.get("result") != "success":
            st.error(f"❌ API failed: {data.get('error-type', 'Unknown error')}")
            return None

        rates = data.get("conversion_rates", {})
        usd_inr = rates.get("INR")
        usd_eur = rates.get("EUR")

        # Gold fallback: static value in case not supported
        xau_usd = 2365.0

        # Calculate INR/EUR = (INR/USD) ÷ (EUR/USD)
        inr_eur = usd_inr / usd_eur if usd_inr and usd_eur else None

        return {
            "USD/INR": round(usd_inr, 3) if usd_inr else None,
            "EUR/USD": round(1 / usd_eur, 4) if usd_eur else None,
            "INR/EUR": round(inr_eur, 3) if inr_eur else None,
            "XAU/USD": round(xau_usd, 2)
        }

    except Exception as e:
        st.error(f"⚠️ Currency API error: {e}")
        return None

# === XIRR calculation function ===
def xirr(cash_flows: dict, guess=0.1):
    def _xnpv(rate, cash_flows):
        t0 = min(cash_flows.keys())
        return sum([cf / (1 + rate) ** ((date - t0).days / 365) for date, cf in cash_flows.items()])
    try:
        return opt.newton(lambda r: _xnpv(r, cash_flows), guess)
    except (RuntimeError, OverflowError):
        return None

def compute_portfolio_xirr(sip_df, holdings_df):
    cash_flows = {}
    for _, row in sip_df.iterrows():
        date = pd.to_datetime(row["SIP Date"], errors="coerce")
        amt = -abs(row["SIP Amount"])
        if pd.notna(date):
            cash_flows[date] = cash_flows.get(date, 0) + amt
    # Add current value as final inflow
    cash_flows[pd.Timestamp.today()] = holdings_df["Current Value"].sum()
    return xirr(cash_flows)

# === File paths ===
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(PROJECT_ROOT, "Fund-Tracker-original.xlsx")

# === Load Functions with Safe Fallbacks ===
@st.cache_data(show_spinner=True)
def load_holdings():
    df = pd.read_excel(DATA_FILE, sheet_name="Fund Tracker")
    numeric_cols = ["Current Value", "Invested Amount", "SIP Amount", "Allocation %", "Return %"]
    for col in numeric_cols:
        df[col] = df.get(col, 0)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["Gain/Loss"] = df["Current Value"] - df["Invested Amount"]
    for col in ["Status", "Category", "Platform"]:
        df[col] = df.get(col, "Unknown")
        df[col] = df[col].fillna("Unknown").astype(str)
    return df

@st.cache_data(show_spinner=True)
def load_sip_history():
    df = pd.read_excel(DATA_FILE, sheet_name="SIP History")
    df = df.dropna(subset=["Fund Name", "SIP Amount", "SIP Date"])
    df["SIP Date"] = pd.to_datetime(df["SIP Date"], errors="coerce")
    df["SIP Amount"] = pd.to_numeric(df["SIP Amount"], errors="coerce").fillna(0)
    df["Platform"] = df.get("Platform", "Unknown")
    df["Platform"] = df["Platform"].fillna("Unknown").astype(str)
    return df

@st.cache_data(show_spinner=True)
def load_sip_calendar():
    try:
        df = pd.read_excel(DATA_FILE, sheet_name="SIP Calendar")
        df["Next Debit Date"] = pd.to_datetime(df.get("Next Debit Date"), errors="coerce")
        today = pd.Timestamp.today()
        df = df[df["Next Debit Date"] >= today]
        return df
    except Exception as e:
        st.warning(f"⚠️ SIP Calendar sheet error: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=True)
def load_normalized_nifty():
    df = pd.read_csv("src/data/nifty.csv")
    df.columns = df.columns.str.strip()  # Remove extra spaces
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%y", errors="coerce")
    df = df.dropna(subset=["Date", "Close"])
    df = df.sort_values("Date").reset_index(drop=True)
    # Normalize the close to starting value = 100
    df["Nifty (Indexed)"] = (df["Close"] / df["Close"].iloc[0]) * 100
    return df[["Date", "Nifty (Indexed)"]]

@st.cache_data(show_spinner=True)
def build_normalized_portfolio(sip_df, holdings_df):
    df = sip_df.copy()
    df["SIP Date"] = pd.to_datetime(df["SIP Date"])
    monthly = df.groupby(pd.Grouper(key="SIP Date", freq="M"))["SIP Amount"].sum().cumsum().reset_index()
    monthly.columns = ["Date", "Portfolio Value Estimate"]
    monthly = monthly[monthly["Portfolio Value Estimate"] > 0]
    monthly["Portfolio (Indexed)"] = (
        monthly["Portfolio Value Estimate"] / monthly["Portfolio Value Estimate"].iloc[0]
    ) * 100
    return monthly

@st.cache_data(show_spinner=True)
def load_normalized_gold():
    df = pd.read_csv("src/data/gold.csv")
    df.columns = df.columns.str.strip()  # Clean extra spaces
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df = df.dropna(subset=["Date", "Price"])
    df = df.sort_values("Date").reset_index(drop=True)
    df["Gold (Indexed)"] = (df["Price"] / df["Price"].iloc[0]) * 100
    return df[["Date", "Gold (Indexed)"]]

@st.cache_data(show_spinner=True)
def compute_rolling_cagr(df, date_col="Date", value_col="Value", window_months=12):
    df = df[[date_col, value_col]].dropna().sort_values(date_col).reset_index(drop=True)
    results = []
    for i in range(window_months, len(df)):
        start = df.iloc[i - window_months]
        end = df.iloc[i]
        start_val = start[value_col]
        end_val = end[value_col]
        start_date = start[date_col]
        end_date = end[date_col]
        if start_val > 0:
            cagr = (end_val / start_val) ** (1 / 1) - 1
            results.append({"Date": end_date, "CAGR": cagr * 100})
    return pd.DataFrame(results)

# === Load Data ===
holdings = load_holdings()
sip_history = load_sip_history()
sip_calendar = load_sip_calendar()

# === Tabs ===
tabs = st.tabs(["📈 Portfolio Holdings", "💰 SIP Tracker & Schedule", "📊 Performance Analytics"])

# === TAB 1: Portfolio Holdings ===
with tabs[0]:
    st.title("📈 Portfolio Holdings Summary")
    st.sidebar.header("🔍 Filters")
    platforms = ["All"] + sorted(holdings["Platform"].unique())
    categories = ["All"] + sorted(holdings["Category"].unique())
    selected_platform = st.sidebar.selectbox("Platform", platforms)
    selected_category = st.sidebar.selectbox("Category", categories)
    search_text = st.sidebar.text_input("Search by Fund Name 🔍")

    filtered = holdings.copy()
    if selected_platform != "All":
        filtered = filtered[filtered["Platform"] == selected_platform]
    if selected_category != "All":
        filtered = filtered[filtered["Category"] == selected_category]
    if search_text:
        filtered = filtered[filtered["Fund Name"].str.contains(search_text, case=False, na=False)]

    st.metric("💼 Portfolio Value", f"₹{filtered['Current Value'].sum():,.2f}")
    st.metric("📥 Invested", f"₹{filtered['Invested Amount'].sum():,.2f}")
    gain = filtered["Current Value"].sum() - filtered["Invested Amount"].sum()
    roi = (gain / filtered["Invested Amount"].sum()) * 100 if filtered["Invested Amount"].sum() > 0 else 0
    st.metric("📊 Gain/Loss", f"₹{gain:,.2f} ({roi:.2f}%)")

    st.subheader("🧭 Allocation by Category")
    cat = filtered.groupby("Category")["Current Value"].sum().reset_index()
    st.plotly_chart(px.pie(cat, names="Category", values="Current Value", hole=0.3), use_container_width=True)

    st.subheader("🏛 Allocation by Platform")
    plat = filtered.groupby("Platform")["Current Value"].sum().reset_index()
    st.plotly_chart(px.bar(plat, x="Platform", y="Current Value", text_auto=".2s"), use_container_width=True)

    st.subheader("📄 Fund Holdings Detail")
    columns = ["Fund Name", "Category", "Platform", "Current Value", "Invested Amount",
               "Return %", "Allocation %", "Gain/Loss", "SIP Amount", "Status"]
    st.dataframe(filtered[columns].sort_values("Current Value", ascending=False), use_container_width=True)

# === TAB 2: SIP Tracker ===
with tabs[1]:
    st.title("💰 SIP Tracker")
    # === SIP Alerts & Health ===
    st.subheader("🔔 SIP Activity & Health")

    today = pd.Timestamp.today()

    # Detect missed SIPs
    missed_sips = sip_history[
        (sip_history["SIP Date"] < today) &
        (~sip_history["SIP Date"].dt.date.isin(
            sip_calendar["Next Debit Date"].dt.date if not sip_calendar.empty else []
        ))
    ]

    # Detect paused funds
    paused_funds = sip_history.groupby("Fund Name")["SIP Date"].max().reset_index()
    paused_funds = paused_funds[paused_funds["SIP Date"] < (today - pd.Timedelta(days=60))]

    # Upcoming SIPs
    upcoming_sips = sip_calendar[
        (sip_calendar["Next Debit Date"] >= today) &
        (sip_calendar["Next Debit Date"] <= today + pd.Timedelta(days=7))
    ]

    # SIP Health Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("🚩 Missed SIPs", len(missed_sips))
    col2.metric("⏸️ Paused Funds", len(paused_funds))
    col3.metric("🗓️ Upcoming SIPs (7d)", len(upcoming_sips))

    # Details
    if not missed_sips.empty:
        st.warning("🚩 Missed SIP entries found:")
        st.dataframe(missed_sips[["Fund Name", "SIP Date", "SIP Amount", "Platform"]])
    else:
        st.success("✅ No missed SIPs found.")

    if not paused_funds.empty:
        st.warning("⏸️ Paused funds (no SIP in over 60 days):")
        st.dataframe(
            paused_funds.rename(columns={"SIP Date": "Last SIP"})[["Fund Name", "Last SIP"]]
        )
    else:
        st.success("✅ All funds recently active.")

    if not upcoming_sips.empty:
        st.info("🗓️ Upcoming SIPs within 7 days:")
        st.dataframe(upcoming_sips[["Fund Name", "Next Debit Date", "SIP Amount", "Platform"]])
    else:
        st.info("ℹ️ No SIPs scheduled in the next 7 days.")

    sip_platforms = ["All"] + sorted(sip_history["Platform"].dropna().unique())
    sel_platform = st.selectbox("Filter by Platform", sip_platforms)
    query = st.text_input("Search Fund")

    df = sip_history.copy()
    if sel_platform != "All":
        df = df[df["Platform"] == sel_platform]
    if query:
        df = df[df["Fund Name"].str.contains(query, case=False, na=False)]

    st.subheader("📋 SIP Summary")
    summary = df.groupby(["Fund Name", "Platform"]).agg(
        SIPs=("SIP Amount", "count"),
        Invested=("SIP Amount", "sum"),
        First_SIP=("SIP Date", "min"),
        Last_SIP=("SIP Date", "max")
    ).reset_index()
    summary = summary.merge(holdings[["Fund Name", "Current Value"]], on="Fund Name", how="left")
    st.dataframe(summary, use_container_width=True)

    st.subheader("📊 SIPs Over Time")
    if not df.empty:
        st.plotly_chart(px.scatter(df, x="SIP Date", y="SIP Amount", color="Fund Name"), use_container_width=True)
    else:
        st.info("No SIPs found for selection.")

    st.subheader("📅 Upcoming SIPs")
    if not sip_calendar.empty:
        st.dataframe(sip_calendar[["Fund Name", "Next Debit Date", "SIP Amount", "Platform"]])
    else:
        st.info("No upcoming SIPs found.")

# === TAB 3: Performance Analytics ===
with tabs[2]:
    st.title("📊 Performance Analytics")

    def xirr_per_fund(sip_df, holdings_df):
        results = []
        today = pd.Timestamp.today()
        for fund in sip_df["Fund Name"].unique():
            df_fund = sip_df[sip_df["Fund Name"] == fund]
            cf = []
            for _, row in df_fund.iterrows():
                if pd.notna(row["SIP Date"]) and row["SIP Amount"] > 0:
                    cf.append((row["SIP Date"], -row["SIP Amount"]))
            latest_value = holdings_df.loc[holdings_df["Fund Name"] == fund, "Current Value"].values
            if len(latest_value) > 0:
                cf.append((today, latest_value[0]))
            try:
                dates, amounts = zip(*cf)
                irr = xirr(dict(zip(dates, amounts)))
                results.append({"Fund Name": fund, "XIRR %": round(irr * 100, 2)})
            except:
                results.append({"Fund Name": fund, "XIRR %": None})
        return pd.DataFrame(results)

    # ℹ Currency Section
    st.subheader("🪙 Currency Exchange Monitor")

    currency_data = load_currency_rates()
    if currency_data:
        col1, col2, col3 = st.columns([1, 1, 1])
        col1.metric("🇺🇸 USD/INR", f"₹{currency_data['USD/INR']}")
        col2.metric("🇪🇺 EUR/USD", f"${currency_data['EUR/USD']}")
        col3.metric("🇮🇳/🇪🇺 INR/EUR", f"{currency_data['INR/EUR']}")
        st.metric("🥇 XAU/USD", f"${currency_data['XAU/USD']}")
    else:
        st.warning("🌐 Currency data not available.")

    perf_df = holdings.copy()
    xirr_df = xirr_per_fund(sip_history, holdings)
    perf_df = perf_df.merge(xirr_df, on="Fund Name", how="left")

    st.subheader("📌 Fund Performance (XIRR vs Return %)")
    st.dataframe(
        perf_df[["Fund Name", "Category", "Return %", "XIRR %"]]
        .sort_values("XIRR %", ascending=False)
        .style.format({"Return %": "{:.2f}%", "XIRR %": "{:.2f}%"})
    )

    st.subheader("📊 XIRR Comparison Bar Chart")
    st.plotly_chart(px.bar(perf_df, x="Fund Name", y="XIRR %", color="Category"), use_container_width=True)

    st.subheader("📉 Timeline: Cumulative SIP vs Current Value")
    selected = st.selectbox("Select Fund", sip_history["Fund Name"].unique())
    df_selected = sip_history[sip_history["Fund Name"] == selected].copy().sort_values("SIP Date")
    df_selected["Cumulative SIP"] = df_selected["SIP Amount"].cumsum()
    val = holdings.loc[holdings["Fund Name"] == selected, "Current Value"].values
    latest_date = df_selected["SIP Date"].max()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_selected["SIP Date"], y=df_selected["Cumulative SIP"],
        mode='lines+markers', name="Cumulative SIP", line=dict(color="blue")
    ))
    if len(val) > 0:
        fig.add_trace(go.Scatter(
            x=[latest_date], y=[val[0]],
            mode='markers+text', name="Current Value",
            text=["Current Value"], textposition="bottom center",
            marker=dict(color="orange", size=12)
        ))
    fig.update_layout(title=f"SIP vs NAV — {selected}", xaxis_title="Date", yaxis_title="Amount (₹)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📌 Portfolio Summary Metrics")
    total_invested = sip_history["SIP Amount"].sum()
    total_current = holdings["Current Value"].sum()
    gain = total_current - total_invested
    roi = (gain / total_invested) * 100 if total_invested > 0 else 0
    pxirr = compute_portfolio_xirr(sip_history, holdings)

    st.metric("💰 Total Invested", f"₹{total_invested:,.2f}")
    st.metric("📈 Current Value", f"₹{total_current:,.2f}")
    st.metric("📊 Gain/Loss", f"₹{gain:,.2f} ({roi:.2f}%)")
    st.metric("⚙️ Portfolio XIRR", f"{pxirr:.2f}%" if pxirr is not None else "N/A")

    st.subheader("📈 Nifty 50 vs Portfolio vs Gold Performance (Indexed)")

    nifty_df = load_normalized_nifty()
    port_df = build_normalized_portfolio(sip_history, holdings)
    gold_df = load_normalized_gold()

    # Rolling 1-Year CAGR for Nifty, Gold, Portfolio
    cagr_nifty = compute_rolling_cagr(nifty_df, value_col="Nifty (Indexed)")
    cagr_gold = compute_rolling_cagr(gold_df, value_col="Gold (Indexed)")
    cagr_port = compute_rolling_cagr(port_df, value_col="Portfolio (Indexed)")

    # Label for each
    cagr_nifty["Source"] = "Nifty 50"
    cagr_gold["Source"] = "Gold (INR)"
    cagr_port["Source"] = "Portfolio"

    # Combine into one DataFrame
    cagr_df = pd.concat([cagr_nifty, cagr_gold, cagr_port])

    st.subheader("🔁 Rolling 1-Year CAGR (Annualized Returns)")
    fig = px.line(
        cagr_df,
        x="Date",
        y="CAGR",
        color="Source",
        title="Rolling 12-Month CAGR: Nifty vs Gold vs Portfolio",
        markers=True
    )
    fig.update_layout(
        yaxis_title="CAGR (%)",
        xaxis_title="Date",
        legend=dict(orientation="h", y=-0.2),
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=nifty_df["Date"], y=nifty_df["Nifty (Indexed)"],
        name="Nifty 50", line=dict(color="blue")
    ))
    fig.add_trace(go.Scatter(
        x=port_df["Date"], y=port_df["Portfolio (Indexed)"],
        name="Your Portfolio (Cumulative SIPs)", line=dict(color="green")
    ))
    fig.add_trace(go.Scatter(
        x=gold_df["Date"], y=gold_df["Gold (Indexed)"],
        name="Gold (INR)", line=dict(color="goldenrod")
    ))
    fig.update_layout(
        title="Relative Performance: Nifty vs Portfolio vs Gold",
        xaxis_title="Date",
        yaxis_title="Indexed to 100",
        legend=dict(orientation="h", y=-0.2)
    )
    st.plotly_chart(fig, use_container_width=True)
