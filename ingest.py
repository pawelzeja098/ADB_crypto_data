"""
Data ingestion job.

Run once:          python ingest.py --once
Run on schedule:   python ingest.py          (uses INGEST_INTERVAL_SECONDS from .env)
"""

import os
import argparse
import datetime
import time

from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler

from connect_api import CoinGeckoClient
import db

load_dotenv()

TRACKED_COINS: list[str] = [
    c.strip()
    for c in os.getenv(
        "TRACKED_COINS",
        "bitcoin,ethereum,binancecoin,solana,ripple,cardano,dogecoin,polkadot,avalanche-2,chainlink",
    ).split(",")
    if c.strip()
]

INTERVAL = int(os.getenv("INGEST_INTERVAL_SECONDS", 60))

client = CoinGeckoClient()


def ingest_markets():
    now = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
    print(f"[Ingest] Fetching market data @ {now.isoformat()} …")

    # Fetch USD and EUR snapshots (2 calls; add delay to respect 30 calls/min demo limit)
    usd_data = {row["id"]: row for row in client.get_markets(TRACKED_COINS, "usd")}
    time.sleep(2)
    eur_data = {row["id"]: row for row in client.get_markets(TRACKED_COINS, "eur")}
    time.sleep(2)

    for coin_id, row in usd_data.items():
        # Ensure coin exists in reference table
        db.upsert_coin(coin_id, row.get("symbol", ""), row.get("name", ""))

        db.insert_snapshot({
            "coin_id":            coin_id,
            "captured_at":        now,
            "price_usd":          row.get("current_price"),
            "price_eur":          eur_data.get(coin_id, {}).get("current_price"),
            "market_cap_usd":     row.get("market_cap"),
            "volume_24h_usd":     row.get("total_volume"),
            "price_change_1h":    row.get("price_change_percentage_1h_in_currency"),
            "price_change_24h":   row.get("price_change_percentage_24h_in_currency"),
            "price_change_7d":    row.get("price_change_percentage_7d_in_currency"),
            "price_change_30d":   row.get("price_change_percentage_30d_in_currency"),
            "circulating_supply": row.get("circulating_supply"),
            "total_supply":       row.get("total_supply"),
            "market_cap_rank":    row.get("market_cap_rank"),
        })

    print(f"[Ingest] Saved {len(usd_data)} coin snapshots.")


def ingest_global():
    now = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
    raw = client.get_global_stats().get("data", {})
    db.insert_global_stats({
        "captured_at":          now,
        "total_market_cap_usd": raw.get("total_market_cap", {}).get("usd"),
        "total_volume_24h_usd": raw.get("total_volume", {}).get("usd"),
        "btc_dominance":        raw.get("market_cap_percentage", {}).get("btc"),
        "eth_dominance":        raw.get("market_cap_percentage", {}).get("eth"),
        "active_coins":         raw.get("active_cryptocurrencies"),
    })
    print("[Ingest] Saved global stats.")


def run_once():
    db.create_schema()
    ingest_markets()
    ingest_global()
    print("[Ingest] Done.")


def run_scheduled():
    db.create_schema()
    print(f"[Ingest] Scheduler started. Interval: {INTERVAL}s. Press Ctrl+C to stop.")
    # Run immediately on start
    ingest_markets()
    ingest_global()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(ingest_markets, "interval", seconds=INTERVAL, id="markets")
    scheduler.add_job(ingest_global,  "interval", seconds=INTERVAL, id="global")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[Ingest] Scheduler stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CoinGecko → PostgreSQL ingestion")
    parser.add_argument("--once", action="store_true",
                        help="Run a single ingestion cycle and exit")
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        run_scheduled()
