from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    # Append so this script directory stays first and `from _common` still works.
    sys.path.append(str(ROOT_DIR))

from common.dates import date_to_ts_ms, resolve_incremental_date_range
from common.stock_daily_fetch import fetch_stock_daily, get_all_stock_symbols
from _common import (
    STOCK_DAILY_TABLE,
    clear_date_range,
    connect_sqlite,
    ensure_stock_daily_table,
    get_db_path,
    get_latest_trade_date,
    insert_stock_daily_chunk,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download stock daily K-line data and insert into SQLite (DolphinDB-style incremental flow)."
    )
    parser.add_argument("--table", default=STOCK_DAILY_TABLE, help="SQLite table name")
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
    parser.add_argument("--chunksize", type=int, default=50000, help="Rows per insert batch")
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    if args.chunksize <= 0:
        raise ValueError("--chunksize must be positive")
    if args.table != STOCK_DAILY_TABLE:
        raise ValueError(f"Only table '{STOCK_DAILY_TABLE}' is supported currently")

    try:
        from tickflow import TickFlow
    except ImportError as exc:
        raise RuntimeError("tickflow is not installed. Please install it in current environment.") from exc

    api_key = os.getenv("TICKFLOW_APIKEY")
    if not api_key:
        raise RuntimeError("TICKFLOW_APIKEY is missing in .env")

    tf = TickFlow(api_key=api_key)
    conn = connect_sqlite()

    try:
        ensure_stock_daily_table(conn)
        conn.commit()

        latest_trade_date = get_latest_trade_date(conn, args.table)
        start_date, end_date = resolve_incremental_date_range(
            start_date_raw=args.start_date,
            end_date_raw=args.end_date,
            latest_trade_date=latest_trade_date,
            init_start_date=args.init_start_date,
        )

        db_path = get_db_path()
        if latest_trade_date is not None:
            print(f"Current latest trade_date in {db_path}::{args.table}: {latest_trade_date}")
        else:
            print(f"Table {db_path}::{args.table} has no data yet")

        print(f"Downloading range: {start_date} -> {end_date}")

        if start_date > end_date:
            print("No date to update")
            return

        symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
        if not symbols:
            symbols = get_all_stock_symbols(tf)

        print(f"Symbols count: {len(symbols)}")

        df = fetch_stock_daily(
            tf=tf,
            symbols=symbols,
            start_ts=date_to_ts_ms(start_date),
            end_ts=date_to_ts_ms(end_date),
        )
        if df.empty:
            print("No data fetched")
            return

        deleted = clear_date_range(conn, args.table, start_date, end_date)
        print(f"Deleted existing rows in range: {deleted}")

        total_inserted = 0
        for index, begin in enumerate(range(0, len(df), args.chunksize), start=1):
            chunk = df.iloc[begin : begin + args.chunksize].copy()
            inserted = insert_stock_daily_chunk(conn, chunk)
            conn.commit()
            total_inserted += inserted
            print(f"Batch {index}: inserted={inserted}, total={total_inserted}")

        print(
            "Done. "
            f"db={db_path}, table={args.table}, range={start_date}->{end_date}, "
            f"downloaded={len(df)}, inserted={total_inserted}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
