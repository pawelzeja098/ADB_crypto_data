"""
CoinGecko API client with automatic rate-limit handling.
Supports both Demo (free) and Pro API plans.
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()


class CoinGeckoClient:
    DEMO_BASE = "https://api.coingecko.com/api/v3"
    PRO_BASE = "https://pro-api.coingecko.com/api/v3"

    def __init__(self):
        self.api_key = os.getenv("COINGECKO_API_KEY", "")
        plan = os.getenv("COINGECKO_PLAN", "demo").lower()
        self.base_url = self.PRO_BASE if plan == "pro" else self.DEMO_BASE
        self.session = requests.Session()
        header_name = "x-cg-pro-api-key" if plan == "pro" else "x-cg-demo-api-key"
        self.session.headers.update({
            header_name: self.api_key,
            "Accept": "application/json",
        })

    def _get(self, endpoint: str, params: dict = None, retries: int = 3) -> dict | list:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                # Demo plan: 30 calls/min — wait at least 2s, back off on retries
                wait = 2 * (2 ** attempt)  # 2s, 4s, 8s
                print(f"[API] Rate limited (429). Waiting {wait}s …")
                time.sleep(wait)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Failed to fetch {endpoint} after {retries} retries")

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------

    def get_coins_list(self) -> list[dict]:
        """All coins supported by CoinGecko (id, symbol, name)."""
        return self._get("/coins/list")

    def get_markets(self, coin_ids: list[str], vs_currency: str = "usd",
                    per_page: int = 250) -> list[dict]:
        """
        Market data snapshot for a list of coins.
        Returns price, market cap, volume, price changes, supply, rank.
        """
        ids_str = ",".join(coin_ids)
        params = {
            "vs_currency": vs_currency,
            "ids": ids_str,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "1h,24h,7d,30d",
        }
        return self._get("/coins/markets", params=params)

    def get_market_chart_range(self, coin_id: str, vs_currency: str,
                               from_ts: int, to_ts: int) -> dict:
        """
        Historical prices + volumes + market caps for a coin in a time range.
        from_ts / to_ts are Unix timestamps (seconds).
        """
        params = {
            "vs_currency": vs_currency,
            "from": from_ts,
            "to": to_ts,
        }
        return self._get(f"/coins/{coin_id}/market_chart/range", params=params)

    def get_coin_detail(self, coin_id: str) -> dict:
        """Full metadata for a single coin (ATH, ATL, description, links, …)."""
        params = {
            "localization": False,
            "tickers": False,
            "market_data": True,
            "community_data": False,
            "developer_data": False,
        }
        return self._get(f"/coins/{coin_id}", params=params)

    def get_global_stats(self) -> dict:
        """Global crypto market data (total market cap, BTC dominance, etc.)."""
        return self._get("/global")
