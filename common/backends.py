from __future__ import annotations

"""
Lightweight storage backend adapters so download/update scripts share one call shape:

    get_latest_trade_date()
    clear_date_range(start, end)
    insert_stock_daily_chunks(df, chunksize)
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import pandas as pd


class MarketDataBackend(ABC):
    @abstractmethod
    def get_latest_trade_date(self, table_name: str) -> date | None:
        raise NotImplementedError

    @abstractmethod
    def clear_date_range(self, table_name: str, start_date: date, end_date: date) -> int:
        raise NotImplementedError

    @abstractmethod
    def insert_stock_daily_chunks(self, table_name: str, df: pd.DataFrame, chunksize: int) -> int:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class SqliteMarketDataBackend(MarketDataBackend):
    def __init__(self, conn: Any | None = None):
        from sqlite._common import connect_sqlite, ensure_stock_daily_table

        self._owns_conn = conn is None
        self.conn = conn or connect_sqlite()
        ensure_stock_daily_table(self.conn)
        self.conn.commit()

    def get_latest_trade_date(self, table_name: str) -> date | None:
        from sqlite._common import get_latest_trade_date

        return get_latest_trade_date(self.conn, table_name)

    def clear_date_range(self, table_name: str, start_date: date, end_date: date) -> int:
        from sqlite._common import clear_date_range

        return clear_date_range(self.conn, table_name, start_date, end_date)

    def insert_stock_daily_chunks(self, table_name: str, df: pd.DataFrame, chunksize: int) -> int:
        from sqlite._common import STOCK_DAILY_TABLE, insert_stock_daily_chunk

        if table_name != STOCK_DAILY_TABLE:
            raise ValueError(f"Unsupported SQLite table for daily insert: {table_name}")

        total = 0
        for begin in range(0, len(df), chunksize):
            chunk = df.iloc[begin : begin + chunksize]
            total += insert_stock_daily_chunk(self.conn, chunk)
            self.conn.commit()
        return total

    def close(self) -> None:
        if self._owns_conn:
            self.conn.close()


class DolphinDBMarketDataBackend(MarketDataBackend):
    def __init__(self, session: Any | None = None, db_path: str = "dfs://market_data"):
        self.db_path = db_path
        self._owns_session = session is None
        if session is None:
            import os

            import dolphindb as ddb

            host = os.getenv("DOLPHINDB_HOST", "localhost")
            port = int(os.getenv("DOLPHINDB_PORT", 8848))
            user = os.getenv("DOLPHINDB_USER", "admin")
            password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))
            session = ddb.Session()
            session.connect(host, port, user, password)
        self.session = session

    def get_latest_trade_date(self, table_name: str) -> date | None:
        from common.dates import coerce_to_date

        self.session.upload({"dbPath": self.db_path, "tableName": table_name})
        latest = self.session.run("exec max(trade_date) from loadTable(dbPath, tableName)")
        return coerce_to_date(latest)

    def clear_date_range(self, table_name: str, start_date: date, end_date: date) -> int:
        self.session.upload(
            {
                "dbPath": self.db_path,
                "tableName": table_name,
                "startDate": start_date,
                "endDate": end_date,
            }
        )
        deleted = self.session.run(
            """
            t = loadTable(dbPath, tableName)
            oldCnt = exec count(*) from t where trade_date >= startDate and trade_date <= endDate
            delete from t where trade_date >= startDate and trade_date <= endDate
            oldCnt
            """
        )
        return int(deleted or 0)

    def insert_stock_daily_chunks(self, table_name: str, df: pd.DataFrame, chunksize: int) -> int:
        total = 0
        for begin in range(0, len(df), chunksize):
            chunk = df.iloc[begin : begin + chunksize].copy()
            if chunk.empty:
                continue
            self.session.upload(
                {
                    "dbPath": self.db_path,
                    "tableName": table_name,
                    "chunkData": chunk,
                }
            )
            inserted = self.session.run("append!(loadTable(dbPath, tableName), chunkData)")
            total += int(inserted) if inserted is not None else len(chunk)
        return total

    def close(self) -> None:
        if self._owns_session:
            try:
                self.session.close()
            except Exception:
                pass


def create_backend(engine: str, **kwargs: Any) -> MarketDataBackend:
    normalized = (engine or "dolphinDB").strip().lower().replace("_", "")
    if normalized == "sqlite":
        return SqliteMarketDataBackend(**kwargs)
    if "dolphin" in normalized:
        return DolphinDBMarketDataBackend(**kwargs)
    raise ValueError(f"Unsupported QUANT_DATA_ENGINE: {engine}")
