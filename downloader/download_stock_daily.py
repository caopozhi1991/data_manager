from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from tickflow import TickFlow


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
TABLE_NAME = "stock_kline_daily"
DAY_MS = 86_400_000

BATCH_SIZE = int(os.getenv("STOCK_DAILY_DOWNLOAD_BATCH_SIZE", 80))
MAX_COUNT_PER_REQ = int(os.getenv("KLINE_MAX_COUNT_PER_REQ", 5000))


def parse_date_arg(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def date_to_ts_ms(d: date) -> int:
    return int(datetime.combine(d, datetime.min.time()).timestamp() * 1000)


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
