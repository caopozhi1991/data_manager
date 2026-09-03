from __future__ import annotations

import os
from typing import TYPE_CHECKING, Dict, List, Tuple

import pandas as pd

if TYPE_CHECKING:
    from tickflow import TickFlow


DAY_MS = 86_400_000
BATCH_SIZE = int(os.getenv("STOCK_DAILY_DOWNLOAD_BATCH_SIZE", 80))
MAX_COUNT_PER_REQ = int(os.getenv("KLINE_MAX_COUNT_PER_REQ", 5000))


def get_all_stock_symbols(tf: "TickFlow") -> List[str]:
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


def get_ex_factors_batch(tf: "TickFlow", symbols: List[str]) -> Dict[str, pd.DataFrame]:
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
    for symbol, group in factors_df.groupby("symbol"):
        factor_map[str(symbol)] = group.drop_duplicates(subset=["trade_date"], keep="last")
    return factor_map


def fetch_stock_daily(
    tf: "TickFlow",
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
                symbol_df = result.get(symbol)
                if symbol_df is None:
                    continue
                df = pd.DataFrame(symbol_df)
                if df.empty or "trade_date" not in df.columns:
                    continue

                df = df.rename(columns={"symbol": "code"})
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
                keep_cols = [col for col in keep_cols if col in df.columns]
                if keep_cols:
                    all_rows.append(df.loc[:, keep_cols].copy())

    if not all_rows:
        return pd.DataFrame()

    merged = pd.concat(all_rows, ignore_index=True)
    merged = merged.drop_duplicates(subset=["code", "trade_date"], keep="last")
    merged = merged.sort_values(["trade_date", "code"]).reset_index(drop=True)
    return merged
