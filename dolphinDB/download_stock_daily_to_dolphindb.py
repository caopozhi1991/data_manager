from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv

if TYPE_CHECKING:
    from tickflow import TickFlow


ROOT_DIR = Path(__file__).resolve().parent.parent
DAY_MS = 86_400_000
BATCH_SIZE = int(os.getenv("STOCK_DAILY_DOWNLOAD_BATCH_SIZE", 80))
MAX_COUNT_PER_REQ = int(os.getenv("KLINE_MAX_COUNT_PER_REQ", 5000))


def parse_date_arg(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def date_to_ts_ms(value: date) -> int:
    return int(datetime.combine(value, datetime.min.time()).timestamp() * 1000)


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


def get_latest_trade_date(session, db_path: str, table_name: str) -> date | None:
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


def fetch_stock_daily(tf: "TickFlow", symbols: List[str], start_ts: int, end_ts: int) -> pd.DataFrame:
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
                    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int32")

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


def clear_target_date_range(session, db_path: str, table_name: str, start_date: date, end_date: date) -> int:
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


def resolve_date_range(args: argparse.Namespace, latest_trade_date: date | None) -> Tuple[date, date]:
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
        raise ValueError(f"Invalid date range: {start_date} > {end_date}")

    if end_date > today:
        end_date = today

    return start_date, end_date


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
        start_date, end_date = resolve_date_range(args, latest_trade_date)

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
