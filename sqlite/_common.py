from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "sqlite" / "market_data.db"

load_dotenv(ROOT_DIR / ".env")


def get_db_path() -> Path:
    raw_path = os.getenv("SQLITE_DB_PATH")
    if raw_path:
        path = Path(raw_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        return path
    return DEFAULT_DB_PATH


def connect_sqlite() -> sqlite3.Connection:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection