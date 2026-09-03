from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tickflow import TickFlow

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common.dates import date_to_ts_ms, parse_date_arg
from common.stock_daily_fetch import fetch_stock_daily, get_all_stock_symbols

DATA_DIR = ROOT_DIR / "data"
TABLE_NAME = "stock_kline_daily"


def save_partitioned_by_month(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    table_root = DATA_DIR / TABLE_NAME
    table_root.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["trade_month"] = df["trade_date"].dt.strftime("%Y-%m")

    file_count = 0
    for month, group in df.groupby("trade_month"):
        out_dir = table_root / f"trade_month={month}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.csv"
        group.drop(columns=["trade_month"]).assign(
            trade_date=lambda x: pd.to_datetime(x["trade_date"]).dt.strftime("%Y-%m-%d")
        ).to_csv(out_file, index=False, encoding="utf-8")
        file_count += 1

    return file_count


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(description="Download stock daily data into data/stock_kline_daily")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols, e.g. 600000.SH,000001.SZ. Empty means full market.",
    )
    args = parser.parse_args()

    start_date = parse_date_arg(args.start_date)
    end_date = parse_date_arg(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date cannot be later than end-date")

    api_key = os.getenv("TICKFLOW_APIKEY")
    if not api_key:
        raise RuntimeError("TICKFLOW_APIKEY is missing in .env")

    tf = TickFlow(api_key=api_key)

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        symbols = get_all_stock_symbols(tf)

    print(f"Downloading stock daily for {len(symbols)} symbols: {start_date} -> {end_date}")
    start_ts = date_to_ts_ms(start_date)
    end_ts = date_to_ts_ms(end_date)

    df = fetch_stock_daily(tf=tf, symbols=symbols, start_ts=start_ts, end_ts=end_ts)
    if df.empty:
        print("No data fetched")
        return

    file_count = save_partitioned_by_month(df)
    print(f"Saved {len(df)} rows into {file_count} monthly files under {DATA_DIR / TABLE_NAME}")


if __name__ == "__main__":
    main()
