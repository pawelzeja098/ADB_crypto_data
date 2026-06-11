"""
Database schema reset script.
Use this to recreate all tables from scratch.

Run: python reset_db_schema.py
"""

from db import get_conn

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


def reset_schema():
    """Create or recreate all schema tables."""
    with get_conn() as conn:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
    print("[DB] Schema ready.")


if __name__ == "__main__":
    reset_schema()
