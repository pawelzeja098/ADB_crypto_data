"""
Quantitative Analysis tab for the cryptocurrency dashboard.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

import db
from helpers import MARKET_CAP_BINS


def render_quantitative_tab(all_coin_options):
    """Render the Quantitative Analysis tab."""
    st.subheader("Quantitative Analysis")

    col1, col2 = st.columns([2, 2])

    with col1:
        # Filter 1 — Coin selection
        qa_coin_names = st.multiselect(
            "Coins",
            options=list(all_coin_options.keys()),
            default=list(all_coin_options.keys()),
            key="qa_coins",
        )

        # Filter 2 — Primary metric
        qa_metric = st.selectbox(
            "Metric",
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
            "Market cap category",
            options=list(MARKET_CAP_BINS.keys()),
            key="qa_cap_cat",
        )

    with col2:
        # Filter 4 — Minimum market cap (USD)
        qa_min_cap = st.number_input(
            "Min market cap (USD)",
            min_value=0,
            value=0,
            step=1_000_000,
            format="%d",
            key="qa_min_cap",
        )

        # Filter 5 — Sort order
        qa_sort_asc = st.radio(
            "Sort order",
            options=["Descending", "Ascending"],
            index=0,
            horizontal=True,
            key="qa_sort",
        )

        # Filter 6 — Top N
        qa_top_n = st.slider(
            "Show top N coins",
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
                    size=pd.to_numeric(df_latest["market_cap_usd"], errors="coerce").fillna(0).clip(lower=1),
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
                
                # Format column names for display
                col_rename = {
                    "coin_name": "Coin",
                    "symbol": "Symbol",
                    "price_usd": "Price (USD)",
                    "market_cap_usd": "Market Cap (USD)",
                    "volume_24h_usd": "Volume 24h (USD)",
                    "price_change_1h": "Price Change 1h (%)",
                    "price_change_24h": "Price Change 24h (%)",
                    "price_change_7d": "Price Change 7d (%)",
                    "price_change_30d": "Price Change 30d (%)",
                    "market_cap_rank": "Market Cap Rank",
                }
                
                table_df = df_latest[present_cols].reset_index(drop=True)
                table_df = table_df.rename(columns=col_rename)
                st.dataframe(table_df, use_container_width=True)
