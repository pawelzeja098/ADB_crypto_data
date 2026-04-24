"""
Database layer: connection pool, schema creation, and all read/write helpers.
"""

import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

_pool: pool.SimpleConnectionPool | None = None


def get_pool() -> pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "crypto_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
        )
    return _pool


@contextmanager
def get_conn():
    conn = get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        get_pool().putconn(conn)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS coins (
    id          VARCHAR(100) PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    name        VARCHAR(200) NOT NULL,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id                  SERIAL PRIMARY KEY,
    coin_id             VARCHAR(100) REFERENCES coins(id) ON DELETE CASCADE,
    captured_at         TIMESTAMPTZ  NOT NULL,
    price_usd           NUMERIC(30, 8),
    price_eur           NUMERIC(30, 8),
    market_cap_usd      NUMERIC(30, 2),
    volume_24h_usd      NUMERIC(30, 2),
    price_change_1h     NUMERIC(10, 4),
    price_change_24h    NUMERIC(10, 4),
    price_change_7d     NUMERIC(10, 4),
    price_change_30d    NUMERIC(10, 4),
    circulating_supply  NUMERIC(30, 2),
    total_supply        NUMERIC(30, 2),
    market_cap_rank     INTEGER,
    UNIQUE (coin_id, captured_at)
);

CREATE TABLE IF NOT EXISTS global_stats (
    id                   SERIAL PRIMARY KEY,
    captured_at          TIMESTAMPTZ UNIQUE NOT NULL,
    total_market_cap_usd NUMERIC(30, 2),
    total_volume_24h_usd NUMERIC(30, 2),
    btc_dominance        NUMERIC(10, 4),
    eth_dominance        NUMERIC(10, 4),
    active_coins         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_snapshots_coin_time
    ON market_snapshots (coin_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_global_time
    ON global_stats (captured_at DESC);
"""


def create_schema():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    print("[DB] Schema ready.")


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def upsert_coin(coin_id: str, symbol: str, name: str):
    sql = """
        INSERT INTO coins (id, symbol, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (coin_id, symbol, name))


def insert_snapshot(data: dict):
    sql = """
        INSERT INTO market_snapshots (
            coin_id, captured_at,
            price_usd, price_eur,
            market_cap_usd, volume_24h_usd,
            price_change_1h, price_change_24h, price_change_7d, price_change_30d,
            circulating_supply, total_supply, market_cap_rank
        ) VALUES (
            %(coin_id)s, %(captured_at)s,
            %(price_usd)s, %(price_eur)s,
            %(market_cap_usd)s, %(volume_24h_usd)s,
            %(price_change_1h)s, %(price_change_24h)s,
            %(price_change_7d)s, %(price_change_30d)s,
            %(circulating_supply)s, %(total_supply)s, %(market_cap_rank)s
        )
        ON CONFLICT (coin_id, captured_at) DO NOTHING
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, data)


def insert_global_stats(data: dict):
    sql = """
        INSERT INTO global_stats (
            captured_at,
            total_market_cap_usd, total_volume_24h_usd,
            btc_dominance, eth_dominance, active_coins
        ) VALUES (
            %(captured_at)s,
            %(total_market_cap_usd)s, %(total_volume_24h_usd)s,
            %(btc_dominance)s, %(eth_dominance)s, %(active_coins)s
        )
        ON CONFLICT (captured_at) DO NOTHING
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, data)


# ---------------------------------------------------------------------------
# Read helpers (used by dashboard)
# ---------------------------------------------------------------------------

import pandas as pd


def query_df(sql: str, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_all_coins() -> pd.DataFrame:
    return query_df("SELECT id, symbol, name FROM coins ORDER BY name")


def get_snapshots(coin_ids: list[str], date_from: str, date_to: str,
                  currency: str = "usd") -> pd.DataFrame:
    price_col = "price_usd" if currency == "usd" else "price_eur"
    sql = f"""
        SELECT
            s.captured_at,
            c.name         AS coin_name,
            c.symbol,
            s.{price_col}  AS price,
            s.market_cap_usd,
            s.volume_24h_usd,
            s.price_change_1h,
            s.price_change_24h,
            s.price_change_7d,
            s.price_change_30d,
            s.circulating_supply,
            s.total_supply,
            s.market_cap_rank
        FROM market_snapshots s
        JOIN coins c ON c.id = s.coin_id
        WHERE s.coin_id = ANY(%s)
          AND s.captured_at BETWEEN %s AND %s
        ORDER BY s.captured_at
    """
    return query_df(sql, params=(coin_ids, date_from, date_to))


def get_latest_snapshots(coin_ids: list[str] | None = None) -> pd.DataFrame:
    base = """
        SELECT DISTINCT ON (s.coin_id)
            s.captured_at,
            c.name          AS coin_name,
            c.symbol,
            s.price_usd,
            s.price_eur,
            s.market_cap_usd,
            s.volume_24h_usd,
            s.price_change_1h,
            s.price_change_24h,
            s.price_change_7d,
            s.price_change_30d,
            s.circulating_supply,
            s.total_supply,
            s.market_cap_rank
        FROM market_snapshots s
        JOIN coins c ON c.id = s.coin_id
    """
    if coin_ids:
        sql = base + " WHERE s.coin_id = ANY(%s) ORDER BY s.coin_id, s.captured_at DESC"
        return query_df(sql, params=(coin_ids,))
    return query_df(base + " ORDER BY s.coin_id, s.captured_at DESC")


def get_global_stats(date_from: str, date_to: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM global_stats
        WHERE captured_at BETWEEN %s AND %s
        ORDER BY captured_at
    """
    return query_df(sql, params=(date_from, date_to))
