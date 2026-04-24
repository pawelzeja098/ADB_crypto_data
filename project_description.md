# Project Description — Advanced Databases

## 1. Team Members

| No. | Name |
|-----|------|
| 1   | Mateusz |
| 2   | Maciej |
| 3   | *(add name)* |
| 4   | *(add name)* |

---

## 2. Project Concept

The goal of the project is to build a **Cryptocurrency Market Analytics System** that continuously collects real-time and historical market data from the CoinGecko public API and stores it in a relational PostgreSQL database. On top of the collected data, an interactive data visualization dashboard will be developed to allow statistical exploration of cryptocurrency trends and metrics.

### System Architecture

```
CoinGecko API
      │
      ▼
 Data Ingestion Layer  (Python scheduler — periodic API polling)
      │
      ▼
 PostgreSQL Database   (structured storage of coins, prices, market data)
      │
      ▼
 Visualization Layer   (charts and filters exposed via a web dashboard)
```

### Collected Data

The system will log the following data points for a set of tracked cryptocurrencies:

- Current and historical **price** (USD, EUR)
- **Market capitalization**
- **24 h trading volume**
- **Price change** (1 h, 24 h, 7 d, 30 d)
- **Circulating supply** and **total supply**
- **All-time high / all-time low** prices and dates
- **Market dominance** percentage

### Visualization Categories

#### Time Series Analysis
Price and volume trends over configurable time windows for one or more coins.

**Filters (≥ 5):**
1. Coin / token selection (e.g. Bitcoin, Ethereum, …)
2. Date range (start date – end date)
3. Price currency (USD / EUR)
4. Metric type (price / volume / market cap)
5. Aggregation granularity (hourly / daily / weekly)
6. Moving average window (none / 7-day / 30-day)

#### Quantitative Analysis
Comparative statistics across coins at a given point in time or over a period.

**Filters (≥ 5):**
1. Coin selection (multi-select)
2. Metric (market cap / volume / price change %)
3. Time period for change metrics (1 h / 24 h / 7 d / 30 d)
4. Market cap category (large-cap / mid-cap / small-cap)
5. Minimum / maximum price threshold
6. Sort order (ascending / descending by selected metric)

---

## 3. Data Sources

### Primary Source — CoinGecko API

| Property | Details |
|----------|---------|
| URL | `https://api.coingecko.com/api/v3` |
| Authentication | API key (already obtained) |
| Plan | Demo / Pro (as available) |
| Format | JSON over HTTPS |
| Rate limit | Varies by plan; handled via exponential back-off in the ingestion layer |

**Key endpoints used:**

| Endpoint | Purpose |
|----------|---------|
| `/coins/list` | Retrieve full list of supported coins |
| `/coins/markets` | Snapshot of price, volume, market cap for multiple coins |
| `/coins/{id}/market_chart/range` | Historical OHLC and volume data for a given time range |
| `/coins/{id}` | Detailed metadata for a single coin |
| `/global` | Global market dominance and total market cap |

### Database — PostgreSQL

The ingested data will be persisted in a **PostgreSQL** database with the following core tables:

```sql
coins           -- static coin metadata (id, symbol, name, …)
market_snapshots -- periodic snapshots (coin_id, timestamp, price_usd, market_cap, volume_24h, …)
price_history   -- fine-grained historical prices (coin_id, timestamp, price_usd, price_eur)
global_stats    -- global market stats per polling cycle (timestamp, total_market_cap, btc_dominance, …)
```

Data ingestion will run as a scheduled Python job (e.g., every 15 minutes for live snapshots, daily for full historical backfill).

---

*Last updated: 2026-04-24*
