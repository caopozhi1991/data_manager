from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from _common import get_db_path


DEFAULT_SRC_TABLE = "stock_kline_daily"
DEFAULT_DST_TABLE = "stock_kline_daily_hfq"


def parse_date_arg(raw: str | None) -> date | None:
    if raw is None or raw == "":
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def open_connection(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def get_table_min_date(connection: sqlite3.Connection, table_name: str, column_name: str = "trade_date") -> date | None:
    row = connection.execute(f"SELECT MIN({column_name}) FROM {table_name}").fetchone()
    value = row[0] if row is not None else None
    if value in (None, "", "nan"):
        return None
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def get_table_max_date(connection: sqlite3.Connection, table_name: str, column_name: str = "trade_date") -> date | None:
    row = connection.execute(f"SELECT MAX({column_name}) FROM {table_name}").fetchone()
    value = row[0] if row is not None else None
    if value in (None, "", "nan"):
        return None
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def resolve_update_range(
    start_date_raw: str | None,
    end_date_raw: str | None,
    src_min_date: date | None,
    src_max_date: date | None,
    dst_max_date: date | None,
    lookback_days: int = 30,
) -> tuple[date, date]:
    if src_min_date is None or src_max_date is None:
        raise RuntimeError("Source table has no data")

    today = date.today()

    if start_date_raw and end_date_raw:
        start_date = parse_date_arg(start_date_raw)
        end_date = parse_date_arg(end_date_raw)
    elif start_date_raw or end_date_raw:
        raise ValueError("Please provide both --start-date and --end-date, or provide neither")
    else:
        if dst_max_date is None:
            start_date = src_min_date
        else:
            start_date = max(src_min_date, dst_max_date - timedelta(days=lookback_days))
        end_date = today

    if start_date is None or end_date is None:
        raise ValueError("Start date or end date is missing")
    if end_date > today:
        end_date = today
    if end_date > src_max_date:
        end_date = src_max_date
    if start_date < src_min_date:
        start_date = src_min_date

    return start_date, end_date


def rebuild_hfq_range(
    connection: sqlite3.Connection,
    src_table: str,
    dst_table: str,
    start_date: date,
    end_date: date,
    batch_days: int,
) -> tuple[int, int]:
    rows = connection.execute(
        f"SELECT code, name, trade_date, open, high, low, close, volume, adjust_factor FROM {src_table} WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date, code",
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    if not rows:
        return 0, 0

    deleted = connection.execute(
        f"DELETE FROM {dst_table} WHERE trade_date >= ? AND trade_date <= ?",
        (start_date.isoformat(), end_date.isoformat()),
    ).rowcount

    inserted_total = 0
    cursor = start_date
    batch_days = max(1, int(batch_days))

    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=batch_days - 1))
        batch_rows = [
            row for row in rows if cursor <= datetime.strptime(str(row[2])[:10], "%Y-%m-%d").date() <= window_end
        ]
        if batch_rows:
            values = []
            for code, name, trade_date, open_price, high_price, low_price, close_price, volume, adjust_factor in batch_rows:
                factor = float(adjust_factor) if adjust_factor not in (None, "", "nan") else 1.0
                values.append(
                    (
                        str(code),
                        str(name) if name is not None else "",
                        str(trade_date)[:10],
                        float(open_price) * factor if open_price is not None else None,
                        float(high_price) * factor if high_price is not None else None,
                        float(low_price) * factor if low_price is not None else None,
                        float(close_price) * factor if close_price is not None else None,
                        int(volume) if volume is not None else 0,
                    )
                )
            connection.executemany(
                f"INSERT INTO {dst_table} (code, name, trade_date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                values,
            )
            inserted_total += len(values)
            print(f"HFQ batch {cursor} -> {window_end}: inserted={len(values)}, total={inserted_total}")
        cursor = window_end + timedelta(days=1)

    connection.commit()
    return deleted, inserted_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally update stock_kline_daily_hfq in SQLite")
    parser.add_argument(
        "--src-db-path",
        default=os.getenv("SQLITE_DB_PATH", str(get_db_path())),
        help="Source SQLite DB path; defaults to the main market_data DB",
    )
    parser.add_argument("--src-table", default=DEFAULT_SRC_TABLE, help="Source table")
    parser.add_argument(
        "--dst-db-path",
        default=os.getenv("SQLITE_DB_PATH", str(get_db_path())),
        help="Target SQLite DB path; defaults to the main market_data DB",
    )
    parser.add_argument("--dst-table", default=DEFAULT_DST_TABLE, help="Target table")
    parser.add_argument("--start-date", default=None, help="Start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date, format YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=30, help="Backfill recent source corrections before the target's latest date")
    parser.add_argument("--batch-days", type=int, default=20, help="Append HFQ data in date windows of N days")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    src_db_path = Path(args.src_db_path) if args.src_db_path else get_db_path()
    dst_db_path = Path(args.dst_db_path) if args.dst_db_path else get_db_path()

    src_conn = open_connection(src_db_path)
    dst_conn = open_connection(dst_db_path)
    try:
        src_min_date = get_table_min_date(src_conn, args.src_table)
        src_max_date = get_table_max_date(src_conn, args.src_table)
        dst_max_date = get_table_max_date(dst_conn, args.dst_table)

        start_date, end_date = resolve_update_range(
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

        print(
            "Updating HFQ range: "
            f"{start_date} -> {end_date} | src={src_db_path}/{args.src_table} "
            f"dst={dst_db_path}/{args.dst_table}"
        )

        deleted_count, inserted_count = rebuild_hfq_range(
            connection=dst_conn,
            src_table=args.src_table,
            dst_table=args.dst_table,
            start_date=start_date,
            end_date=end_date,
            batch_days=args.batch_days,
        )

        print(f"Done. deleted={deleted_count}, inserted={inserted_count}, range={start_date}->{end_date}")
    finally:
        src_conn.close()
        dst_conn.close()


if __name__ == "__main__":
    main()
