from __future__ import annotations

import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from common.dates import coerce_to_date

DEFAULT_DB_PATH = ROOT_DIR / "sqlite" / "market_data.db"
STOCK_DAILY_TABLE = "stock_kline_daily"
STOCK_DAILY_HFQ_TABLE = "stock_kline_daily_hfq"

load_dotenv(ROOT_DIR / ".env")


CREATE_STOCK_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS stock_kline_daily (
    code TEXT NOT NULL,
    name TEXT,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    amount REAL,
    adjust_factor REAL,
    PRIMARY KEY (code, trade_date)
)
"""

CREATE_STOCK_DAILY_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_kline_daily_trade_date
ON stock_kline_daily (trade_date)
"""

CREATE_STOCK_DAILY_HFQ_SQL = """
CREATE TABLE IF NOT EXISTS stock_kline_daily_hfq (
    code TEXT NOT NULL,
    name TEXT,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    PRIMARY KEY (code, trade_date)
)
"""

CREATE_STOCK_DAILY_HFQ_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_kline_daily_hfq_trade_date
ON stock_kline_daily_hfq (trade_date)
"""


def get_db_path() -> Path:
    raw_path = os.getenv("SQLITE_DB_PATH")
    if raw_path:
        path = Path(raw_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        return path
    return DEFAULT_DB_PATH


def connect_sqlite(*, enable_wal: bool = True) -> sqlite3.Connection:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    if enable_wal:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
    return connection


def ensure_stock_daily_table(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_STOCK_DAILY_SQL)
    conn.execute(CREATE_STOCK_DAILY_INDEX_SQL)


def ensure_stock_daily_hfq_table(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_STOCK_DAILY_HFQ_SQL)
    conn.execute(CREATE_STOCK_DAILY_HFQ_INDEX_SQL)


def ensure_table_exists(conn: sqlite3.Connection, table_name: str) -> None:
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    )
    if cursor.fetchone() is None:
        raise RuntimeError(f"SQLite table not found: {table_name}")


def get_table_min_date(conn: sqlite3.Connection, table_name: str) -> date | None:
    ensure_table_exists(conn, table_name)
    row = conn.execute(f'SELECT MIN(trade_date) FROM "{table_name}"').fetchone()
    if not row:
        return None
    return coerce_to_date(row[0])


def get_table_max_date(conn: sqlite3.Connection, table_name: str) -> date | None:
    ensure_table_exists(conn, table_name)
    row = conn.execute(f'SELECT MAX(trade_date) FROM "{table_name}"').fetchone()
    if not row:
        return None
    return coerce_to_date(row[0])


def get_latest_trade_date(conn: sqlite3.Connection, table_name: str = STOCK_DAILY_TABLE) -> date | None:
    return get_table_max_date(conn, table_name)


def clear_date_range(
    conn: sqlite3.Connection,
    table_name: str,
    start_date: date,
    end_date: date,
) -> int:
    ensure_table_exists(conn, table_name)
    cursor = conn.execute(
        f'DELETE FROM "{table_name}" WHERE trade_date >= ? AND trade_date <= ?',
        (start_date.isoformat(), end_date.isoformat()),
    )
    return int(cursor.rowcount or 0)


def _stock_daily_rows(chunk: "pd.DataFrame") -> list[tuple]:
    import pandas as pd

    rows: list[tuple] = []
    for row in chunk.itertuples(index=False):
        trade_date = row.trade_date
        if hasattr(trade_date, "isoformat"):
            trade_date_text = trade_date.isoformat()
        else:
            trade_date_text = str(trade_date)[:10]

        rows.append(
            (
                str(row.code),
                str(row.name) if pd.notna(getattr(row, "name", None)) else "",
                trade_date_text,
                float(row.open) if pd.notna(row.open) else None,
                float(row.high) if pd.notna(row.high) else None,
                float(row.low) if pd.notna(row.low) else None,
                float(row.close) if pd.notna(row.close) else None,
                int(row.volume) if pd.notna(row.volume) else 0,
                float(row.amount) if pd.notna(getattr(row, "amount", None)) else None,
                float(row.adjust_factor) if pd.notna(getattr(row, "adjust_factor", None)) else 1.0,
            )
        )
    return rows


def insert_stock_daily_chunk(conn: sqlite3.Connection, chunk: "pd.DataFrame") -> int:
    if chunk.empty:
        return 0

    rows = _stock_daily_rows(chunk)
    conn.executemany(
        """
        INSERT OR REPLACE INTO stock_kline_daily
        (code, name, trade_date, open, high, low, close, volume, amount, adjust_factor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def insert_stock_daily_chunks(
    conn: sqlite3.Connection,
    df: "pd.DataFrame",
    chunksize: int = 50000,
) -> int:
    if df.empty:
        return 0
    if chunksize <= 0:
        raise ValueError("chunksize must be positive")

    total = 0
    for begin in range(0, len(df), chunksize):
        chunk = df.iloc[begin : begin + chunksize]
        total += insert_stock_daily_chunk(conn, chunk)
        conn.commit()
    return total


def insert_stock_daily_hfq_chunk(conn: sqlite3.Connection, chunk: "pd.DataFrame") -> int:
    import pandas as pd

    if chunk.empty:
        return 0

    rows: list[tuple] = []
    for row in chunk.itertuples(index=False):
        trade_date = row.trade_date
        if hasattr(trade_date, "isoformat"):
            trade_date_text = trade_date.isoformat()
        else:
            trade_date_text = str(trade_date)[:10]

        rows.append(
            (
                str(row.code),
                str(row.name) if pd.notna(getattr(row, "name", None)) else "",
                trade_date_text,
                float(row.open) if pd.notna(row.open) else None,
                float(row.high) if pd.notna(row.high) else None,
                float(row.low) if pd.notna(row.low) else None,
                float(row.close) if pd.notna(row.close) else None,
                int(row.volume) if pd.notna(row.volume) else 0,
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO stock_kline_daily_hfq
        (code, name, trade_date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)
