from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    # Append so this script directory stays first and `from _common` still works.
    sys.path.append(str(ROOT_DIR))

from common.dates import resolve_hfq_update_range
from _common import (
    STOCK_DAILY_HFQ_TABLE,
    STOCK_DAILY_TABLE,
    clear_date_range,
    connect_sqlite,
    ensure_stock_daily_hfq_table,
    ensure_stock_daily_table,
    get_db_path,
    get_table_max_date,
    get_table_min_date,
    insert_stock_daily_hfq_chunk,
)


def rebuild_hfq_range(
    conn,
    *,
    src_table: str,
    dst_table: str,
    start_date: date,
    end_date: date,
    batch_days: int,
) -> tuple[int, int]:
    deleted = clear_date_range(conn, dst_table, start_date, end_date)
    conn.commit()

    inserted_total = 0
    cursor = start_date
    batch_days = max(1, int(batch_days))

    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=batch_days - 1))
        query = f"""
            SELECT
                code,
                name,
                trade_date,
                open * COALESCE(adjust_factor, 1.0) AS open,
                high * COALESCE(adjust_factor, 1.0) AS high,
                low * COALESCE(adjust_factor, 1.0) AS low,
                close * COALESCE(adjust_factor, 1.0) AS close,
                volume
            FROM "{src_table}"
            WHERE trade_date >= ? AND trade_date <= ?
        """
        chunk = pd.read_sql_query(
            query,
            conn,
            params=(cursor.isoformat(), window_end.isoformat()),
        )
        if not chunk.empty:
            inserted = insert_stock_daily_hfq_chunk(conn, chunk)
            conn.commit()
            inserted_total += inserted
        else:
            inserted = 0

        print(f"HFQ batch {cursor} -> {window_end}: inserted={inserted}, total={inserted_total}")
        cursor = window_end + timedelta(days=1)

    return deleted, inserted_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally update stock_kline_daily_hfq in SQLite")
    parser.add_argument("--src-table", default=STOCK_DAILY_TABLE, help="Source table")
    parser.add_argument("--dst-table", default=STOCK_DAILY_HFQ_TABLE, help="Target table")
    parser.add_argument("--start-date", default=None, help="Start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date, format YYYY-MM-DD")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help=(
            "In default incremental mode, re-compute this many days before the HFQ table's "
            "latest date to catch retroactive adjust_factor corrections in the source data. "
            "Set to 0 to only append strictly new dates. "
            "Pass --start-date/--end-date explicitly to override the range entirely."
        ),
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=20,
        help="Rebuild HFQ data in date windows of N days",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    conn = connect_sqlite()
    try:
        ensure_stock_daily_table(conn)
        ensure_stock_daily_hfq_table(conn)
        conn.commit()

        src_min_date = get_table_min_date(conn, args.src_table)
        src_max_date = get_table_max_date(conn, args.src_table)
        dst_max_date = get_table_max_date(conn, args.dst_table)

        start_date, end_date = resolve_hfq_update_range(
            start_date_raw=args.start_date,
            end_date_raw=args.end_date,
            src_min_date=src_min_date,
            src_max_date=src_max_date,
            dst_max_date=dst_max_date,
            lookback_days=args.lookback_days,
        )

        if start_date > end_date:
            print(
                "No range to update. "
                f"resolved start={start_date}, end={end_date}, src_max={src_max_date}, today={date.today()}"
            )
            return

        db_path = get_db_path()
        print(
            "Updating HFQ range: "
            f"{start_date} -> {end_date} | src={db_path}::{args.src_table} "
            f"dst={db_path}::{args.dst_table}"
        )

        deleted_count, inserted_count = rebuild_hfq_range(
            conn,
            src_table=args.src_table,
            dst_table=args.dst_table,
            start_date=start_date,
            end_date=end_date,
            batch_days=args.batch_days,
        )

        print(
            "Done. "
            f"deleted={deleted_count}, inserted={inserted_count}, range={start_date}->{end_date}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
