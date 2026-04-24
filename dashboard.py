"""
Streamlit dashboard — Cryptocurrency Market Analytics

Run:  streamlit run dashboard.py
"""

import datetime
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import db

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Crypto Analytics",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Cryptocurrency Market Analytics")

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=120)
def load_coins():
    return db.get_all_coins()


def moving_average(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean()


MARKET_CAP_BINS = {
    "All": (0, float("inf")),
    "Large-cap  (> $10B)":  (10_000_000_000, float("inf")),
    "Mid-cap  ($1B – $10B)": (1_000_000_000, 10_000_000_000),
    "Small-cap  (< $1B)":   (0, 1_000_000_000),
}

# ---------------------------------------------------------------------------
# Sidebar — global controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Global settings")
    coins_df = load_coins()
    if coins_df.empty:
        st.warning("No coins in database yet. Run `python ingest.py --once` first.")
        st.stop()

    all_coin_options = dict(zip(coins_df["name"], coins_df["id"]))

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_ts, tab_quant, tab_about = st.tabs([
    "🕐 Time Series", "📊 Quantitative Analysis", "ℹ️ About"
])

# ===========================================================================
# TAB 1 — TIME SERIES
# ===========================================================================
with tab_ts:
    st.subheader("Time Series Analysis")

    # --- Filters (6) --------------------------------------------------------
    col1, col2 = st.columns([2, 2])
    with col1:
        # Filter 1 — Coin selection
        ts_coin_names = st.multiselect(
            "1️⃣  Coins",
            options=list(all_coin_options.keys()),
            default=list(all_coin_options.keys())[:3],
            key="ts_coins",
        )

        # Filter 2 — Date range
        ts_date_range = st.date_input(
            "2️⃣  Date range",
            value=(
                datetime.date.today() - datetime.timedelta(days=30),
                datetime.date.today(),
            ),
            key="ts_dates",
        )

        # Filter 3 — Currency
        ts_currency = st.selectbox(
            "3️⃣  Currency",
            options=["usd", "eur"],
            format_func=lambda x: x.upper(),
            key="ts_currency",
        )

    with col2:
        # Filter 4 — Metric
        ts_metric = st.selectbox(
            "4️⃣  Metric",
            options=["price", "market_cap_usd", "volume_24h_usd"],
            format_func=lambda x: {
                "price": "Price",
                "market_cap_usd": "Market Cap (USD)",
                "volume_24h_usd": "Volume 24h (USD)",
            }[x],
            key="ts_metric",
        )

        # Filter 5 — Aggregation granularity
        ts_granularity = st.selectbox(
            "5️⃣  Aggregation",
            options=["raw", "1H", "4H", "1D", "1W"],
            format_func=lambda x: {
                "raw": "Raw (every snapshot)",
                "1H": "Hourly",
                "4H": "4-Hourly",
                "1D": "Daily",
                "1W": "Weekly",
            }[x],
            key="ts_granularity",
        )

        # Filter 6 — Moving average
        ts_ma_window = st.select_slider(
            "6️⃣  Moving average window",
            options=[0, 3, 7, 14, 30],
            value=0,
            format_func=lambda x: "None" if x == 0 else f"{x}-period MA",
            key="ts_ma",
        )

    # --- Data loading -------------------------------------------------------
    if len(ts_date_range) != 2:
        st.info("Select a start and end date.")
        st.stop()

    ts_date_from, ts_date_to = ts_date_range
    ts_coin_ids = [all_coin_options[n] for n in ts_coin_names if n in all_coin_options]

    if not ts_coin_ids:
        st.info("Select at least one coin.")
    else:
        df = db.get_snapshots(
            ts_coin_ids,
            str(ts_date_from),
            str(ts_date_to) + " 23:59:59",
            currency=ts_currency,
        )

        if df.empty:
            st.warning("No data for the selected filters.")
        else:
            df["captured_at"] = pd.to_datetime(df["captured_at"])
            df = df.sort_values("captured_at")

            # Aggregation
            if ts_granularity != "raw":
                df = (
                    df.groupby(["coin_name", "symbol", pd.Grouper(key="captured_at", freq=ts_granularity)])
                    .agg({
                        "price":         "mean",
                        "market_cap_usd":"mean",
                        "volume_24h_usd":"sum",
                    })
                    .reset_index()
                )

            y_col = ts_metric if ts_metric != "price" else "price"
            y_label = {
                "price": f"Price ({ts_currency.upper()})",
                "market_cap_usd": "Market Cap (USD)",
                "volume_24h_usd": "Volume 24h (USD)",
            }[ts_metric]

            fig = go.Figure()
            for coin in df["coin_name"].unique():
                subset = df[df["coin_name"] == coin].copy()
                fig.add_trace(go.Scatter(
                    x=subset["captured_at"],
                    y=subset[y_col],
                    mode="lines",
                    name=coin,
                ))
                if ts_ma_window > 0:
                    ma = moving_average(subset[y_col], ts_ma_window)
                    fig.add_trace(go.Scatter(
                        x=subset["captured_at"],
                        y=ma,
                        mode="lines",
                        name=f"{coin} ({ts_ma_window}p MA)",
                        line=dict(dash="dot"),
                    ))

            fig.update_layout(
                title=f"{y_label} — {ts_date_from} to {ts_date_to}",
                xaxis_title="Date",
                yaxis_title=y_label,
                hovermode="x unified",
                height=480,
            )
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Raw data"):
                st.dataframe(df, use_container_width=True)

# ===========================================================================
# TAB 2 — QUANTITATIVE ANALYSIS
# ===========================================================================
with tab_quant:
    st.subheader("Quantitative Analysis")

    col1, col2 = st.columns([2, 2])

    with col1:
        # Filter 1 — Coin selection
        qa_coin_names = st.multiselect(
            "1️⃣  Coins",
            options=list(all_coin_options.keys()),
            default=list(all_coin_options.keys()),
            key="qa_coins",
        )

        # Filter 2 — Primary metric
        qa_metric = st.selectbox(
            "2️⃣  Metric",
            options=[
                "price_usd", "market_cap_usd", "volume_24h_usd",
                "price_change_1h", "price_change_24h",
                "price_change_7d", "price_change_30d",
                "circulating_supply",
            ],
            format_func=lambda x: {
                "price_usd":          "Price (USD)",
                "market_cap_usd":     "Market Cap (USD)",
                "volume_24h_usd":     "Volume 24h (USD)",
                "price_change_1h":    "Price Change 1h (%)",
                "price_change_24h":   "Price Change 24h (%)",
                "price_change_7d":    "Price Change 7d (%)",
                "price_change_30d":   "Price Change 30d (%)",
                "circulating_supply": "Circulating Supply",
            }[x],
            key="qa_metric",
        )

        # Filter 3 — Market cap category
        qa_cap_cat = st.selectbox(
            "3️⃣  Market cap category",
            options=list(MARKET_CAP_BINS.keys()),
            key="qa_cap_cat",
        )

    with col2:
        # Filter 4 — Minimum market cap (USD)
        qa_min_cap = st.number_input(
            "4️⃣  Min market cap (USD)",
            min_value=0,
            value=0,
            step=1_000_000,
            format="%d",
            key="qa_min_cap",
        )

        # Filter 5 — Sort order
        qa_sort_asc = st.radio(
            "5️⃣  Sort order",
            options=["Descending", "Ascending"],
            index=0,
            horizontal=True,
            key="qa_sort",
        )

        # Filter 6 — Top N
        qa_top_n = st.slider(
            "6️⃣  Show top N coins",
            min_value=1,
            max_value=min(50, len(all_coin_options)),
            value=min(10, len(all_coin_options)),
            key="qa_top_n",
        )

    qa_coin_ids = [all_coin_options[n] for n in qa_coin_names if n in all_coin_options]

    if not qa_coin_ids:
        st.info("Select at least one coin.")
    else:
        df_latest = db.get_latest_snapshots(qa_coin_ids)

        if df_latest.empty:
            st.warning("No data for the selected coins.")
        else:
            # Apply market cap category filter
            cap_lo, cap_hi = MARKET_CAP_BINS[qa_cap_cat]
            df_latest = df_latest[
                (df_latest["market_cap_usd"] >= cap_lo) &
                (df_latest["market_cap_usd"] < cap_hi)
            ]

            # Apply minimum market cap filter
            df_latest = df_latest[df_latest["market_cap_usd"] >= qa_min_cap]

            if df_latest.empty:
                st.warning("No coins match the current filters.")
            else:
                ascending = qa_sort_asc == "Ascending"
                df_latest = (
                    df_latest
                    .sort_values(qa_metric, ascending=ascending)
                    .head(qa_top_n)
                )

                metric_label = {
                    "price_usd":          "Price (USD)",
                    "market_cap_usd":     "Market Cap (USD)",
                    "volume_24h_usd":     "Volume 24h (USD)",
                    "price_change_1h":    "Price Change 1h (%)",
                    "price_change_24h":   "Price Change 24h (%)",
                    "price_change_7d":    "Price Change 7d (%)",
                    "price_change_30d":   "Price Change 30d (%)",
                    "circulating_supply": "Circulating Supply",
                }.get(qa_metric, qa_metric)

                # Bar chart
                color_col = "price_change_24h" if "price_change" not in qa_metric else qa_metric
                fig_bar = px.bar(
                    df_latest,
                    x="coin_name",
                    y=qa_metric,
                    color=color_col,
                    color_continuous_scale="RdYlGn",
                    text_auto=".3s",
                    title=f"{metric_label} — Top {qa_top_n} coins",
                    labels={"coin_name": "Coin", qa_metric: metric_label},
                )
                fig_bar.update_layout(height=420, coloraxis_colorbar_title="24h %")
                st.plotly_chart(fig_bar, use_container_width=True)

                # Scatter: Market Cap vs Volume coloured by 24h change
                st.markdown("#### Market Cap vs. 24h Volume")
                fig_scatter = px.scatter(
                    df_latest,
                    x="market_cap_usd",
                    y="volume_24h_usd",
                    color="price_change_24h",
                    size=df_latest["market_cap_usd"].clip(lower=1),
                    hover_name="coin_name",
                    color_continuous_scale="RdYlGn",
                    labels={
                        "market_cap_usd":  "Market Cap (USD)",
                        "volume_24h_usd":  "Volume 24h (USD)",
                        "price_change_24h": "24h Change (%)",
                    },
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)

                # Summary table
                st.markdown("#### Summary table")
                display_cols = [
                    "coin_name", "symbol", "price_usd",
                    "market_cap_usd", "volume_24h_usd",
                    "price_change_1h", "price_change_24h",
                    "price_change_7d", "price_change_30d",
                    "market_cap_rank",
                ]
                present_cols = [c for c in display_cols if c in df_latest.columns]
                st.dataframe(
                    df_latest[present_cols].reset_index(drop=True),
                    use_container_width=True,
                )

# ===========================================================================
# TAB 3 — ABOUT
# ===========================================================================
with tab_about:
    st.markdown("""
## About this dashboard

**Data source:** [CoinGecko API](https://www.coingecko.com/en/api)

**Database:** PostgreSQL

### How to run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy and fill in credentials
copy .env.example .env

# 3. Create DB schema & run first ingestion
python ingest.py --once

# 4. Start scheduled ingestion (runs every INGEST_INTERVAL_SECONDS)
python ingest.py

# 5. Launch dashboard (in a separate terminal)
streamlit run dashboard.py
```

### Filters per view

| View | Filters |
|------|---------|
| Time Series | Coin selection, Date range, Currency, Metric, Aggregation granularity, Moving average window |
| Quantitative | Coin selection, Metric, Market cap category, Min market cap, Sort order, Top N |
    """)
