from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from _common import get_db_path


DEFAULT_SRC_DB = "market_data"
DEFAULT_DST_DB = "vnpy_bar_db"
DEFAULT_SRC_TABLE = "stock_kline_daily_hfq"
DEFAULT_DST_TABLE = "vnpy_stock_daily_hfq"


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


def normalize_symbol(code: str) -> str:
    value = str(code).strip()
    if len(value) >= 6:
        return value[:6]
    return value


def normalize_exchange(code: str) -> str:
    value = str(code).upper()
    if ".SH" in value or value.startswith("6"):
        return "SSE"
    if ".SZ" in value or value.startswith("0") or value.startswith("3"):
        return "SZSE"
    if ".BJ" in value:
        return "BSE"
    return "UNKNOWN"


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


def rebuild_vnpy_hfq_range(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    src_table: str,
    dst_table: str,
    start_date: date,
    end_date: date,
    batch_days: int,
) -> tuple[int, int]:
    rows = src_conn.execute(
        f"SELECT code, trade_date, volume, open, high, low, close FROM {src_table} WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date, code",
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    if not rows:
        return 0, 0

    deleted = dst_conn.execute(
        f"DELETE FROM {dst_table} WHERE datetime >= ? AND datetime <= ?",
        (start_date.isoformat(), end_date.isoformat()),
    ).rowcount

    inserted_total = 0
    cursor = start_date
    batch_days = max(1, int(batch_days))

    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=batch_days - 1))
        batch_rows = [
            row for row in rows if cursor <= datetime.strptime(str(row[1])[:10], "%Y-%m-%d").date() <= window_end
        ]
        if batch_rows:
            values = []
            for code, trade_date, volume, open_price, high_price, low_price, close_price in batch_rows:
                values.append(
                    (
                        normalize_symbol(code),
                        normalize_exchange(code),
                        str(trade_date)[:10],
                        "d",
                        float(volume) if volume is not None else 0.0,
                        0.0,
                        0.0,
                        float(open_price) if open_price is not None else 0.0,
                        float(high_price) if high_price is not None else 0.0,
                        float(low_price) if low_price is not None else 0.0,
                        float(close_price) if close_price is not None else 0.0,
                    )
                )
            dst_conn.executemany(
                f"INSERT INTO {dst_table} (symbol, exchange, datetime, interval, volume, turnover, open_interest, open_price, high_price, low_price, close_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                values,
            )
            inserted_total += len(values)
            print(f"VNpy HFQ batch {cursor} -> {window_end}: inserted={len(values)}, total={inserted_total}")
        cursor = window_end + timedelta(days=1)

    dst_conn.commit()
    return deleted, inserted_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally sync stock_kline_daily_hfq to vnpy_stock_daily_hfq in SQLite")
    parser.add_argument(
        "--src-db-path",
        default=os.getenv("SQLITE_DB_PATH", str(get_db_path(DEFAULT_SRC_DB))),
        help="Source SQLite DB path; defaults to the main market_data DB",
    )
    parser.add_argument("--src-table", default=DEFAULT_SRC_TABLE, help="Source table")
    parser.add_argument(
        "--dst-db-path",
        default=os.getenv("SQLITE_VNPY_BAR_DB_PATH", str(get_db_path(DEFAULT_DST_DB))),
        help="Target SQLite DB path; defaults to the vnpy_bar_db DB",
    )
    parser.add_argument("--dst-table", default=DEFAULT_DST_TABLE, help="Target table")
    parser.add_argument("--start-date", default=None, help="Start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date, format YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=30, help="Backfill recent source corrections before the target's latest date")
    parser.add_argument("--batch-days", type=int, default=20, help="Append VN.py data in date windows of N days")
    return parser.parse_args()


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    args = parse_args()

    src_db_path = Path(args.src_db_path) if args.src_db_path else get_db_path(DEFAULT_SRC_DB)
    dst_db_path = Path(args.dst_db_path) if args.dst_db_path else get_db_path(DEFAULT_DST_DB)

    src_conn = open_connection(src_db_path)
    dst_conn = open_connection(dst_db_path)
    try:
        src_min_date = get_table_min_date(src_conn, args.src_table)
        src_max_date = get_table_max_date(src_conn, args.src_table)
        dst_max_date = get_table_max_date(dst_conn, args.dst_table, "datetime")

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
            "Syncing VNpy HFQ range: "
            f"{start_date} -> {end_date} | src={src_db_path}/{args.src_table} "
            f"dst={dst_db_path}/{args.dst_table}"
        )

        deleted_count, inserted_count = rebuild_vnpy_hfq_range(
            src_conn=src_conn,
            dst_conn=dst_conn,
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
