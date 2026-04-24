"""
Database layer: connection pool, schema creation, and all read/write helpers.
Backend: MySQL 8.0+ via mysql-connector-python.
"""

import os
from contextlib import contextmanager
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

load_dotenv()

_pool: pooling.MySQLConnectionPool | None = None


def get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="crypto_pool",
            pool_size=10,
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 3306)),
            database=os.getenv("DB_NAME", "crypto_db"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            autocommit=False,
            time_zone="+00:00",
        )
    return _pool


@contextmanager
def get_conn():
    conn = get_pool().get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()  # returns connection to pool


# ---------------------------------------------------------------------------
# Schema — individual statements (MySQL connector executes one at a time)
# ---------------------------------------------------------------------------

_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS coins (
        id         VARCHAR(100) PRIMARY KEY,
        symbol     VARCHAR(20)  NOT NULL,
        name       VARCHAR(200) NOT NULL,
        created_at DATETIME     DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_snapshots (
        id                 INT AUTO_INCREMENT PRIMARY KEY,
        coin_id            VARCHAR(100)  NOT NULL,
        captured_at        DATETIME      NOT NULL,
        price_usd          DECIMAL(30,8),
        price_eur          DECIMAL(30,8),
        market_cap_usd     DECIMAL(30,2),
        volume_24h_usd     DECIMAL(30,2),
        price_change_1h    DECIMAL(10,4),
        price_change_24h   DECIMAL(10,4),
        price_change_7d    DECIMAL(10,4),
        price_change_30d   DECIMAL(10,4),
        circulating_supply DECIMAL(30,2),
        total_supply       DECIMAL(30,2),
        market_cap_rank    INT,
        UNIQUE KEY uq_coin_time (coin_id, captured_at),
        FOREIGN KEY (coin_id) REFERENCES coins(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS global_stats (
        id                   INT AUTO_INCREMENT PRIMARY KEY,
        captured_at          DATETIME     UNIQUE NOT NULL,
        total_market_cap_usd DECIMAL(30,2),
        total_volume_24h_usd DECIMAL(30,2),
        btc_dominance        DECIMAL(10,4),
        eth_dominance        DECIMAL(10,4),
        active_coins         INT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "CREATE INDEX IF NOT EXISTS idx_snapshots_coin_time ON market_snapshots (coin_id, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_global_time ON global_stats (captured_at DESC)",
]


def create_schema():
    with get_conn() as conn:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
    print("[DB] Schema ready.")


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def upsert_coin(coin_id: str, symbol: str, name: str):
    sql = """
        INSERT INTO coins (id, symbol, name)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE symbol = VALUES(symbol), name = VALUES(name)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (coin_id, symbol, name))
        cur.close()


def insert_snapshot(data: dict):
    sql = """
        INSERT IGNORE INTO market_snapshots (
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
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, data)
        cur.close()


def insert_global_stats(data: dict):
    sql = """
        INSERT IGNORE INTO global_stats (
            captured_at,
            total_market_cap_usd, total_volume_24h_usd,
            btc_dominance, eth_dominance, active_coins
        ) VALUES (
            %(captured_at)s,
            %(total_market_cap_usd)s, %(total_volume_24h_usd)s,
            %(btc_dominance)s, %(eth_dominance)s, %(active_coins)s
        )
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, data)
        cur.close()


# ---------------------------------------------------------------------------
# Read helpers (used by dashboard)
# ---------------------------------------------------------------------------

import pandas as pd


def _placeholders(lst: list) -> str:
    """Return comma-separated %s placeholders for a list, e.g. '%s, %s, %s'."""
    return ", ".join(["%s"] * len(lst))


def query_df(sql: str, params=None) -> pd.DataFrame:
    """Execute a SELECT and return results as a DataFrame via cursor (MySQL-safe)."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows)


def get_all_coins() -> pd.DataFrame:
    return query_df("SELECT id, symbol, name FROM coins ORDER BY name")


def get_snapshots(coin_ids: list[str], date_from: str, date_to: str,
                  currency: str = "usd") -> pd.DataFrame:
    price_col = "price_usd" if currency == "usd" else "price_eur"
    ph = _placeholders(coin_ids)
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
        WHERE s.coin_id IN ({ph})
          AND s.captured_at BETWEEN %s AND %s
        ORDER BY s.captured_at
    """
    return query_df(sql, params=list(coin_ids) + [date_from, date_to])


def get_latest_snapshots(coin_ids: list[str] | None = None) -> pd.DataFrame:
    # MySQL has no DISTINCT ON; use subquery to get latest captured_at per coin
    where = ""
    params: list = []
    if coin_ids:
        ph = _placeholders(coin_ids)
        where = f"WHERE s.coin_id IN ({ph})"
        params = list(coin_ids) * 2  # used in both subquery and outer query

    sql = f"""
        SELECT
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
        JOIN (
            SELECT coin_id, MAX(captured_at) AS max_at
            FROM market_snapshots
            {"WHERE coin_id IN (" + _placeholders(coin_ids) + ")" if coin_ids else ""}
            GROUP BY coin_id
        ) latest ON s.coin_id = latest.coin_id AND s.captured_at = latest.max_at
        {where}
        ORDER BY s.market_cap_rank
    """
    return query_df(sql, params=params if params else None)


def get_global_stats(date_from: str, date_to: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM global_stats
        WHERE captured_at BETWEEN %s AND %s
        ORDER BY captured_at
    """
    return query_df(sql, params=(date_from, date_to))

