from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from tickflow import TickFlow


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "sqlite" / "market_data.db"
TABLE_NAME = "stock_kline_daily"
DAY_MS = 86_400_000

BATCH_SIZE = int(os.getenv("STOCK_DAILY_DOWNLOAD_BATCH_SIZE", 80))
MAX_COUNT_PER_REQ = int(os.getenv("KLINE_MAX_COUNT_PER_REQ", 5000))


def parse_date_arg(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def date_to_ts_ms(d: date) -> int:
    return int(datetime.combine(d, datetime.min.time()).timestamp() * 1000)


def open_connection(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def ensure_stock_table_exists(connection: sqlite3.Connection, table_name: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date
        ON {table_name} (trade_date)
        """
    )


def get_latest_trade_date(connection: sqlite3.Connection, table_name: str) -> date | None:
    row = connection.execute(f"SELECT MAX(trade_date) FROM {table_name}").fetchone()
    value = row[0] if row is not None else None
    if value in (None, "", "nan"):
        return None
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def get_all_stock_symbols(tf: TickFlow) -> List[str]:
    all_frames: List[pd.DataFrame] = []
    for exchange in ["SH", "SZ", "BJ"]:
        raw = tf.exchanges.get_instruments(exchange=exchange, instrument_type="stock")
        if raw is None:
            continue
        df = pd.DataFrame(raw)
        if df.empty or "symbol" not in df.columns:
            continue
        all_frames.append(df.loc[:, ["symbol"]].copy())

    if not all_frames:
        raise RuntimeError("No symbols returned from TickFlow exchanges.get_instruments")

    merged = pd.concat(all_frames, ignore_index=True)
    merged = merged.dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"])
    return merged["symbol"].astype(str).tolist()


def get_ex_factors_batch(tf: TickFlow, symbols: List[str]) -> Dict[str, pd.DataFrame]:
    try:
        factors_raw = tf.klines.ex_factors(symbols, as_dataframe=True)
    except Exception:
        return {}

    factors_df = pd.DataFrame(factors_raw)
    if factors_df.empty:
        return {}

    if "symbol" not in factors_df.columns and "code" in factors_df.columns:
        factors_df = factors_df.rename(columns={"code": "symbol"})

    required_cols = {"symbol", "trade_date", "ex_factor"}
    if not required_cols.issubset(set(factors_df.columns)):
        return {}

    factors_df = factors_df.loc[:, ["symbol", "trade_date", "ex_factor"]].copy()
    factors_df["trade_date"] = pd.to_datetime(factors_df["trade_date"]).dt.date
    factors_df["ex_factor"] = pd.to_numeric(factors_df["ex_factor"], errors="coerce")
    factors_df = factors_df.dropna(subset=["symbol", "trade_date", "ex_factor"])

    factor_map: Dict[str, pd.DataFrame] = {}
    for sym, group in factors_df.groupby("symbol"):
        factor_map[str(sym)] = group.drop_duplicates(subset=["trade_date"], keep="last")
    return factor_map


def fetch_stock_daily(
    tf: TickFlow,
    symbols: List[str],
    start_ts: int,
    end_ts: int,
) -> pd.DataFrame:
    if start_ts > end_ts:
        return pd.DataFrame()

    max_days_per_req = max(1, MAX_COUNT_PER_REQ)
    chunk_ranges: List[Tuple[int, int]] = []
    cursor = start_ts
    while cursor <= end_ts:
        chunk_end = min(end_ts, cursor + max_days_per_req * DAY_MS - 1)
        chunk_ranges.append((cursor, chunk_end))
        cursor = chunk_end + 1

    symbol_chunks = [symbols[i : i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    ex_factor_map = get_ex_factors_batch(tf, symbols)

    all_rows: List[pd.DataFrame] = []
    for chunk_start, chunk_end in chunk_ranges:
        span_days = max(1, int((chunk_end - chunk_start) / DAY_MS) + 1)
        for symbol_chunk in symbol_chunks:
            result = tf.klines.batch(
                symbols=symbol_chunk,
                period="1d",
                start_time=chunk_start,
                end_time=chunk_end,
                count=min(span_days, MAX_COUNT_PER_REQ),
                adjust="none",
                as_dataframe=True,
                show_progress=False,
                batch_size=min(100, max(1, len(symbol_chunk))),
                max_workers=5,
            )

            for symbol in symbol_chunk:
                sym_df = result.get(symbol)
                if sym_df is None:
                    continue
                df = pd.DataFrame(sym_df)
                if df.empty:
                    continue

                df = df.rename(columns={"symbol": "code"})
                if "trade_date" not in df.columns:
                    continue

                df["trade_date"] = pd.to_datetime(df["trade_date"])
                df["name"] = ""

                factor_df = ex_factor_map.get(str(symbol))
                if factor_df is not None and not factor_df.empty:
                    tmp = factor_df.copy()
                    tmp["trade_date"] = pd.to_datetime(tmp["trade_date"])
                    tmp = tmp.rename(columns={"ex_factor": "adjust_factor"})
                    df = pd.merge_asof(
                        df.sort_values("trade_date"),
                        tmp.sort_values("trade_date"),
                        on="trade_date",
                        direction="backward",
                    )
                    df["adjust_factor"] = df["adjust_factor"].fillna(1.0)
                else:
                    df["adjust_factor"] = 1.0

                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
                df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")

                for col in ["open", "high", "low", "close", "amount", "adjust_factor"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                if "volume" in df.columns:
                    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

                keep_cols = [
                    "code",
                    "name",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "adjust_factor",
                ]
                keep_cols = [c for c in keep_cols if c in df.columns]
                df = df.loc[:, keep_cols].copy()
                all_rows.append(df)

    if not all_rows:
        return pd.DataFrame()

    merged = pd.concat(all_rows, ignore_index=True)
    merged = merged.drop_duplicates(subset=["code", "trade_date"], keep="last")
    merged = merged.sort_values(["trade_date", "code"]).reset_index(drop=True)
    return merged


def clear_target_date_range(connection: sqlite3.Connection, table_name: str, start_date: date, end_date: date) -> int:
    cursor = connection.execute(
        f"DELETE FROM {table_name} WHERE trade_date >= ? AND trade_date <= ?",
        (start_date.isoformat(), end_date.isoformat()),
    )
    return cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0


def insert_chunk_to_sqlite(connection: sqlite3.Connection, table_name: str, chunk: pd.DataFrame) -> int:
    if chunk.empty:
        return 0

    values = []
    for row in chunk.itertuples(index=False):
        values.append(
            (
                str(row.code),
                str(row.name) if pd.notna(row.name) else "",
                row.trade_date.isoformat() if hasattr(row.trade_date, "isoformat") else str(row.trade_date)[:10],
                float(row.open) if pd.notna(row.open) else None,
                float(row.high) if pd.notna(row.high) else None,
                float(row.low) if pd.notna(row.low) else None,
                float(row.close) if pd.notna(row.close) else None,
                int(row.volume) if pd.notna(row.volume) else 0,
                float(row.amount) if pd.notna(row.amount) else None,
                float(row.adjust_factor) if pd.notna(row.adjust_factor) else 1.0,
            )
        )

    connection.executemany(
        f"""
        INSERT OR REPLACE INTO {table_name}
        (code, name, trade_date, open, high, low, close, volume, amount, adjust_factor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    return len(values)


def resolve_date_range(args: argparse.Namespace, latest_trade_date: date | None) -> tuple[date, date]:
    today = date.today()

    if args.start_date and args.end_date:
        start_date = parse_date_arg(args.start_date)
        end_date = parse_date_arg(args.end_date)
    elif args.start_date or args.end_date:
        raise ValueError("Please provide both --start-date and --end-date, or provide neither")
    else:
        if latest_trade_date is None:
            start_date = parse_date_arg(args.init_start_date)
        else:
            start_date = latest_trade_date + timedelta(days=1)
        end_date = today

    if start_date > end_date:
        if latest_trade_date is not None and not args.start_date and not args.end_date:
            return start_date, end_date
        raise ValueError(f"Invalid date range: {start_date} > {end_date}")

    if end_date > today:
        end_date = today

    return start_date, end_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download stock daily data into SQLite stock_kline_daily")
    parser.add_argument(
        "--db-path",
        default=os.getenv("SQLITE_DB_PATH", str(DEFAULT_DB_PATH)),
        help="SQLite database path for the main market_data database",
    )
    parser.add_argument("--table", default=TABLE_NAME, help="SQLite table name")
    parser.add_argument("--start-date", default="", help="Optional YYYY-MM-DD. Omit to auto-increment from local data.")
    parser.add_argument("--end-date", default="", help="Optional YYYY-MM-DD. Omit to auto-increment to today.")
    parser.add_argument(
        "--init-start-date",
        default="2005-01-01",
        help="Used when the table has no data yet and start/end are omitted",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols, e.g. 600000.SH,000001.SZ. Empty means full market.",
    )
    parser.add_argument("--chunksize", type=int, default=50000, help="Rows per insert batch")
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    if args.chunksize <= 0:
        raise ValueError("--chunksize must be positive")

    api_key = os.getenv("TICKFLOW_APIKEY")
    if not api_key:
        raise RuntimeError("TICKFLOW_APIKEY is missing in .env")

    db_path = Path(args.db_path)
    connection = open_connection(db_path)

    try:
        ensure_stock_table_exists(connection, args.table)
        latest_trade_date = get_latest_trade_date(connection, args.table)
        start_date, end_date = resolve_date_range(args, latest_trade_date)

        if latest_trade_date is not None:
            print(f"Current latest trade_date in {db_path}/{args.table}: {latest_trade_date}")
        else:
            print(f"Table {db_path}/{args.table} has no data yet")

        print(f"Downloading range: {start_date} -> {end_date}")

        if start_date > end_date:
            print("No date to update")
            return

        tf = TickFlow(api_key=api_key)
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

        deleted = clear_target_date_range(connection, args.table, start_date, end_date)
        print(f"Deleted existing rows in range: {deleted}")

        total_inserted = 0
        for index, begin in enumerate(range(0, len(df), args.chunksize), start=1):
            chunk = df.iloc[begin : begin + args.chunksize].copy()
            inserted = insert_chunk_to_sqlite(connection, args.table, chunk)
            total_inserted += inserted
            print(f"Batch {index}: inserted={inserted}, total={total_inserted}")

        connection.commit()
        print(
            "Done. "
            f"db={db_path}, table={args.table}, range={start_date}->{end_date}, "
            f"downloaded={len(df)}, inserted={total_inserted}"
        )
    finally:
        try:
            connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()