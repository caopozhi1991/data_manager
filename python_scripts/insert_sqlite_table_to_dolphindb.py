from __future__ import annotations

import argparse
import os
import re
import sqlite3
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_DB_PATH = ROOT_DIR / "sqlite" / "market_data.db"


def validate_dolphindb_identifier(name: str, field_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid {field_name}: {name}")
    return name


def quote_sqlite_identifier(name: str) -> str:
    return f'"{name.replace("\"", "\"\"")}"'


def resolve_sqlite_db_path(raw_path: str | None) -> Path:
    if not raw_path:
        return DEFAULT_SQLITE_DB_PATH
    path = Path(raw_path)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path


def connect_dolphindb_session():
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    session = ddb.Session()
    session.connect(host, port, user, password)
    return session


def ensure_sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> None:
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    )
    if cursor.fetchone() is None:
        raise ValueError(f"SQLite table not found: {table_name}")


def ensure_dolphindb_table_exists(session, db_path: str, table_name: str) -> None:
    session.upload({"dbPath": db_path, "tableName": table_name})
    exists = bool(session.run("existsTable(dbPath, tableName)"))
    if not exists:
        raise ValueError(f"DolphinDB table not found: db={db_path}, table={table_name}")


def build_sql_query(table_name: str, where: str | None) -> str:
    quoted_table = quote_sqlite_identifier(table_name)
    query = f"SELECT * FROM {quoted_table}"
    if where and where.strip():
        query = f"{query} WHERE {where.strip()}"
    return query


def insert_chunk_to_dolphindb(session, db_path: str, table_name: str, chunk: pd.DataFrame) -> int:
    if chunk.empty:
        return 0

    session.upload(
        {
            "dbPath": db_path,
            "tableName": table_name,
            "chunkData": chunk,
        }
    )
    inserted = session.run("append!(loadTable(dbPath, tableName), chunkData)")
    if inserted is None:
        return len(chunk)
    try:
        return int(inserted)
    except (TypeError, ValueError):
        return len(chunk)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Insert a SQLite table into a DolphinDB table in batches."
    )
    parser.add_argument(
        "sqlite_db_file",
        help="SQLite db file path (absolute or relative to project root).",
    )
    parser.add_argument("sqlite_table", help="SQLite source table name.")
    parser.add_argument("dolphindb_db", help="DolphinDB db path, e.g. dfs://market_data.")
    parser.add_argument("dolphindb_table", help="DolphinDB target table name.")
    parser.add_argument(
        "--where",
        default=None,
        help="Optional SQLite WHERE clause, e.g. trade_date >= '2020-01-01'.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=50000,
        help="Rows per batch uploaded to DolphinDB.",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    if args.chunksize <= 0:
        raise ValueError("--chunksize must be a positive integer")

    sqlite_path = resolve_sqlite_db_path(args.sqlite_db_file)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite db file not found: {sqlite_path}")

    sqlite_table = args.sqlite_table
    ddb_db = args.dolphindb_db
    ddb_table = validate_dolphindb_identifier(args.dolphindb_table, "DolphinDB table name")

    conn = sqlite3.connect(sqlite_path)
    session = connect_dolphindb_session()

    try:
        ensure_sqlite_table_exists(conn, sqlite_table)
        ensure_dolphindb_table_exists(session, ddb_db, ddb_table)

        query = build_sql_query(sqlite_table, args.where)
        total_inserted = 0

        chunk_iter = pd.read_sql_query(query, conn, chunksize=args.chunksize)
        for index, chunk in enumerate(chunk_iter, start=1):
            inserted = insert_chunk_to_dolphindb(session, ddb_db, ddb_table, chunk)
            total_inserted += inserted
            print(f"Batch {index}: inserted={inserted}, total={total_inserted}")

        print(
            "Done. "
            f"sqlite_db={sqlite_path}, sqlite_table={sqlite_table}, "
            f"dolphindb_db={ddb_db}, dolphindb_table={ddb_table}, total_inserted={total_inserted}"
        )
    finally:
        try:
            conn.close()
        finally:
            try:
                session.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
