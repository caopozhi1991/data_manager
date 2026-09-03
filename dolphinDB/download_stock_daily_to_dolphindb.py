from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.dates import date_to_ts_ms, parse_date_arg, resolve_incremental_date_range
from common.stock_daily_fetch import fetch_stock_daily, get_all_stock_symbols


def connect_dolphindb_session():
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    session = ddb.Session()
    session.connect(host, port, user, password)
    return session


def ensure_dolphindb_table_exists(session, db_path: str, table_name: str) -> None:
    session.upload({"dbPath": db_path, "tableName": table_name})
    exists = bool(session.run("existsTable(dbPath, tableName)"))
    if not exists:
        raise RuntimeError(f"DolphinDB table not found: {db_path}/{table_name}")


def get_latest_trade_date(session, db_path: str, table_name: str):
    from datetime import date, datetime

    session.upload({"dbPath": db_path, "tableName": table_name})
    latest = session.run("exec max(trade_date) from loadTable(dbPath, tableName)")
    if latest is None or (isinstance(latest, float) and pd.isna(latest)):
        return None

    if isinstance(latest, pd.Timestamp):
        return latest.date()
    if isinstance(latest, datetime):
        return latest.date()
    if isinstance(latest, date):
        return latest

    raw = str(latest)
    if not raw or raw.lower() == "nan":
        return None
    return parse_date_arg(raw[:10])


def clear_target_date_range(session, db_path: str, table_name: str, start_date, end_date) -> int:
    session.upload(
        {
            "dbPath": db_path,
            "tableName": table_name,
            "startDate": start_date,
            "endDate": end_date,
        }
    )
    deleted = session.run(
        """
        t = loadTable(dbPath, tableName)
        oldCnt = exec count(*) from t where trade_date >= startDate and trade_date <= endDate
        delete from t where trade_date >= startDate and trade_date <= endDate
        oldCnt
        """
    )
    if deleted is None:
        return 0
    return int(deleted)


def insert_chunk_to_dolphindb(session, db_path: str, table_name: str, chunk: pd.DataFrame) -> int:
    if chunk.empty:
        return 0

    session.upload({"dbPath": db_path, "tableName": table_name, "chunkData": chunk})
    inserted = session.run("append!(loadTable(dbPath, tableName), chunkData)")

    if inserted is None:
        return len(chunk)
    try:
        return int(inserted)
    except (TypeError, ValueError):
        return len(chunk)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download stock daily K-line data and insert into DolphinDB table."
    )
    parser.add_argument("--db-path", default="dfs://market_data", help="DolphinDB database path")
    parser.add_argument("--table", default="stock_kline_daily", help="DolphinDB table name")
    parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--init-start-date",
        default="2005-01-01",
        help="Used only when table has no data and start/end are omitted",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols, empty means full market",
    )
    parser.add_argument("--chunksize", type=int, default=50000, help="Rows per append batch")
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    if args.chunksize <= 0:
        raise ValueError("--chunksize must be positive")

    try:
        from tickflow import TickFlow
    except ImportError as exc:
        raise RuntimeError("tickflow is not installed. Please install it in current environment.") from exc

    api_key = os.getenv("TICKFLOW_APIKEY")
    if not api_key:
        raise RuntimeError("TICKFLOW_APIKEY is missing in .env")

    tf = TickFlow(api_key=api_key)
    session = connect_dolphindb_session()

    try:
        ensure_dolphindb_table_exists(session, args.db_path, args.table)
        latest_trade_date = get_latest_trade_date(session, args.db_path, args.table)
        start_date, end_date = resolve_incremental_date_range(
            start_date_raw=args.start_date,
            end_date_raw=args.end_date,
            latest_trade_date=latest_trade_date,
            init_start_date=args.init_start_date,
        )

        if latest_trade_date is not None:
            print(f"Current latest trade_date in {args.db_path}/{args.table}: {latest_trade_date}")
        else:
            print(f"Table {args.db_path}/{args.table} has no data yet")

        print(f"Downloading range: {start_date} -> {end_date}")

        if start_date > end_date:
            print("No date to update")
            return

        symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
        if not symbols:
            symbols = get_all_stock_symbols(tf)

        print(f"Symbols count: {len(symbols)}")

        start_ts = date_to_ts_ms(start_date)
        end_ts = date_to_ts_ms(end_date)
        df = fetch_stock_daily(tf=tf, symbols=symbols, start_ts=start_ts, end_ts=end_ts)

        if df.empty:
            print("No data fetched")
            return

        deleted = clear_target_date_range(session, args.db_path, args.table, start_date, end_date)
        print(f"Deleted existing rows in range: {deleted}")

        total_inserted = 0
        for index, begin in enumerate(range(0, len(df), args.chunksize), start=1):
            chunk = df.iloc[begin : begin + args.chunksize].copy()
            inserted = insert_chunk_to_dolphindb(session, args.db_path, args.table, chunk)
            total_inserted += inserted
            print(f"Batch {index}: inserted={inserted}, total={total_inserted}")

        print(
            "Done. "
            f"db={args.db_path}, table={args.table}, range={start_date}->{end_date}, "
            f"downloaded={len(df)}, inserted={total_inserted}"
        )
    finally:
        try:
            session.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
