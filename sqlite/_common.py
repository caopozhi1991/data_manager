from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "sqlite" / "market_data.db"
DEFAULT_VNPY_DB_PATH = ROOT_DIR / "sqlite" / "vnpy_bar_db.db"

load_dotenv(ROOT_DIR / ".env")


def get_db_path(database_name: str | None = None) -> Path:
    if database_name:
        normalized = database_name.strip().lower().replace("-", "_").replace(".", "_")
        env_name = {
            "market_data": "SQLITE_DB_PATH",
            "vnpy_bar_db": "SQLITE_VNPY_BAR_DB_PATH",
            "vnpy_db": "SQLITE_VNPY_BAR_DB_PATH",
            "vnpy": "SQLITE_VNPY_BAR_DB_PATH",
        }.get(normalized, f"SQLITE_{normalized.upper()}_DB_PATH")
        raw_path = os.getenv(env_name)
        if raw_path:
            path = Path(raw_path)
            if not path.is_absolute():
                path = ROOT_DIR / path
            return path

        if normalized == "market_data":
            return DEFAULT_DB_PATH
        if normalized in {"vnpy_bar_db", "vnpy_db", "vnpy"}:
            return DEFAULT_VNPY_DB_PATH
        return ROOT_DIR / "sqlite" / f"{normalized}.db"

    raw_path = os.getenv("SQLITE_DB_PATH")
    if raw_path:
        path = Path(raw_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        return path
    return DEFAULT_DB_PATH


def connect_sqlite(database_name: str | None = None) -> sqlite3.Connection:
    db_path = get_db_path(database_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection