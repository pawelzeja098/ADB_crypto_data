"""
Time Series Analysis tab for the cryptocurrency dashboard.
"""

import datetime
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import db
from helpers import moving_average


def render_time_series_tab(all_coin_options):
    """Render the Time Series Analysis tab."""
    st.subheader("Time Series Analysis")

    # --- Filters --------------------------------------------------------
    col1, col2 = st.columns([2, 2])
    with col1:
        # Filter 1 — Coin selection
        ts_coin_names = st.multiselect(
            "Coins",
            options=list(all_coin_options.keys()),
            default=list(all_coin_options.keys())[:1],
            key="ts_coins",
        )

        # Filter 2 — Date range
        ts_date_range = st.date_input(
            "Date range",
            value=(
                datetime.date.today() - datetime.timedelta(days=1),
                datetime.date.today(),
            ),
            key="ts_dates",
        )

        # Filter 3 — Currency
        ts_currency = st.selectbox(
            "Currency",
            options=["usd", "eur"],
            format_func=lambda x: x.upper(),
            key="ts_currency",
        )

    with col2:
        # Filter 4 — Metric
        ts_metric = st.selectbox(
            "Metric",
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
            "Aggregation",
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
            "Moving average window",
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
