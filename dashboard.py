"""
Streamlit dashboard — Cryptocurrency Market Analytics

Run:  streamlit run dashboard.py
"""

import streamlit as st

import db
from time_series import render_time_series_tab
from quantitative import render_quantitative_tab

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Crypto Analytics",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

st.title("Cryptocurrency Market Analytics")

# ---------------------------------------------------------------------------
# Helper functions (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=120)
def load_coins():
    return db.get_all_coins()

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
    "Time Series", "Quantitative Analysis", "About"
])

# ===========================================================================
# TAB 1 — TIME SERIES
# ===========================================================================
with tab_ts:
    render_time_series_tab(all_coin_options)

# ===========================================================================
# TAB 2 — QUANTITATIVE ANALYSIS
# ===========================================================================
with tab_quant:
    render_quantitative_tab(all_coin_options)

# ===========================================================================
# TAB 3 — ABOUT
# ===========================================================================
with tab_about:
    st.markdown("""
## About this dashboard

**Data source:** [CoinGecko API](https://www.coingecko.com/en/api)

**Database:** MySQL

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
