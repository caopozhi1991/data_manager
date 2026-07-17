"""
增量更新 DolphinDB 中的股票日线前复权数据到指定日期。

逻辑说明
1) 从 DolphinDB 读取每只股票最后一条记录(last_trade_date, last_close)。
2) 使用 TickFlow 拉取 [last_trade_date, target_date] 区间的前复权日线。
3) 用 last_trade_date 这一天的旧收盘价作为锚点，对新拉取区间做比例衔接，
   保证新老数据口径连续（从旧数据往后复权）。
4) 仅写入 trade_date > last_trade_date 的新增行。
"""

import argparse
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import dolphindb as ddb
import pandas as pd
from dotenv import load_dotenv
from tickflow import TickFlow


load_dotenv()

DEFAULT_DB_PATH = "dfs://market_data"
DEFAULT_TABLE_NAME = "stock_kline_daily_qfq"
DEFAULT_TARGET_DATE = "2026-07-16"

BATCH_SIZE = int(os.getenv("QFQ_UPDATE_BATCH_SIZE", 80))
BATCH_RPM = int(os.getenv("QFQ_UPDATE_BATCH_RPM", 30))
BATCH_INTERVAL_SEC = 60.0 / max(1, BATCH_RPM)
MAX_COUNT_PER_REQ = int(os.getenv("KLINE_MAX_COUNT_PER_REQ", 10000))
DAY_MS = 86400000

DDB_HOST = os.getenv("DOLPHINDB_HOST", "localhost")
DDB_PORT = int(os.getenv("DOLPHINDB_PORT", 8848))
DDB_USER = os.getenv("DOLPHINDB_USER", "admin")
DDB_PWD = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))


def connect_dolphindb_session() -> ddb.Session:
    s = ddb.Session()
    s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PWD)
    return s


def to_tickflow_symbol(code: str) -> str:
    if not isinstance(code, str) or "." not in code:
        return str(code)
    left, right = code.rsplit(".", 1)
    suffix = right.upper()
    if suffix == "SSE":
        return f"{left}.SH"
    if suffix == "SZSE":
        return f"{left}.SZ"
    if suffix == "BJSE":
        return f"{left}.BJ"
    return code


def parse_date_yyyy_mm_dd(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def get_table_columns(session: ddb.Session, db_path: str, table_name: str) -> List[str]:
    session.upload({"dbPath": db_path, "tableName": table_name})
    cols = session.run(
        """
        if(!existsDatabase(dbPath)) throw "Database not found: " + dbPath
        if(!existsTable(dbPath, tableName)) throw "Table not found: " + dbPath + "." + tableName
        schema(loadTable(dbPath, tableName)).colDefs.name
        """
    )
    return [str(c) for c in list(cols)]


def get_latest_rows(session: ddb.Session, db_path: str, table_name: str) -> pd.DataFrame:
    session.upload({"dbPath": db_path, "tableName": table_name})
    latest = session.run(
        """
        t = loadTable(dbPath, tableName)
        // 每个 code 取最新一条
        select code, trade_date as last_trade_date, close as last_close
        from t
        context by code
        csort trade_date desc
        """
    )
    df = pd.DataFrame(latest)
    if df.empty:
        return df

    expected = {"code", "last_trade_date", "last_close"}
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest rows query missing columns: {missing}")

    df = df.loc[:, ["code", "last_trade_date", "last_close"]].copy()
    df["code"] = df["code"].astype(str)
    df["last_trade_date"] = pd.to_datetime(df["last_trade_date"]).dt.date
    df["last_close"] = pd.to_numeric(df["last_close"], errors="coerce")
    df = df.dropna(subset=["last_trade_date", "last_close"])
    return df.reset_index(drop=True)


def safe_batch_fetch_qfq(
    tf: TickFlow,
    symbols: List[str],
    start_ts: int,
    end_ts: int,
    count: int,
) -> Dict[str, pd.DataFrame]:
    adjust_candidates = ["forward", "qfq"]
    last_err: Optional[Exception] = None

    for adjust_mode in adjust_candidates:
        try:
            res = tf.klines.batch(
                symbols=symbols,
                period="1d",
                start_time=start_ts,
                end_time=end_ts,
                count=count,
                adjust=adjust_mode,
                as_dataframe=True,
                show_progress=False,
                batch_size=min(100, max(1, len(symbols))),
                max_workers=5,
            )
            return {k: pd.DataFrame(v) for k, v in res.items()}
        except Exception as exc:
            last_err = exc

    if last_err is not None:
        raise last_err
    raise RuntimeError("failed to fetch qfq data")


def fetch_qfq_daily_batch(
    tf: TickFlow,
    symbols: List[str],
    start_ts: int,
    end_ts: int,
) -> Dict[str, pd.DataFrame]:
    if start_ts > end_ts:
        return {sym: pd.DataFrame() for sym in symbols}

    max_days_per_req = max(1, MAX_COUNT_PER_REQ)
    chunk_ranges: List[Tuple[int, int]] = []
    cursor = start_ts
    while cursor <= end_ts:
        chunk_end = min(end_ts, cursor + max_days_per_req * DAY_MS - 1)
        chunk_ranges.append((cursor, chunk_end))
        cursor = chunk_end + 1

    out: Dict[str, List[pd.DataFrame]] = {sym: [] for sym in symbols}
    for chunk_start, chunk_end in chunk_ranges:
        day_span = max(1, int((chunk_end - chunk_start) / DAY_MS) + 1)
        fetched = safe_batch_fetch_qfq(
            tf=tf,
            symbols=symbols,
            start_ts=chunk_start,
            end_ts=chunk_end,
            count=min(day_span, MAX_COUNT_PER_REQ),
        )
        for sym in symbols:
            df = fetched.get(sym)
            if df is None or df.empty:
                continue
            out[sym].append(df)

    merged: Dict[str, pd.DataFrame] = {}
    for sym in symbols:
        chunks = out.get(sym, [])
        if not chunks:
            merged[sym] = pd.DataFrame()
            continue
        df = pd.concat(chunks, ignore_index=True)
        if "trade_date" not in df.columns:
            merged[sym] = pd.DataFrame()
            continue
        df = df.rename(columns={"symbol": "code"})
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")
        merged[sym] = df.reset_index(drop=True)

    return merged


def normalize_output_columns(df: pd.DataFrame, table_columns: List[str]) -> pd.DataFrame:
    base_defaults: Dict[str, Any] = {
        "name": "",
        "adjust_factor": 1.0,
    }

    out = df.copy()
    for col, default_value in base_defaults.items():
        if col in table_columns and col not in out.columns:
            out[col] = default_value

    # 常见数值列类型清洗
    for num_col in ["open", "high", "low", "close", "amount", "adjust_factor"]:
        if num_col in out.columns:
            out[num_col] = pd.to_numeric(out[num_col], errors="coerce")
    if "volume" in out.columns:
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")

    if "trade_date" in out.columns:
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date

    use_cols = [c for c in table_columns if c in out.columns]
    return out.loc[:, use_cols].copy()


def table_insert(session: ddb.Session, db_path: str, table_name: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    session.upload({"dbPath": db_path, "tableName": table_name, "dfTable": df})
    inserted = session.run(
        """
        t = loadTable(dbPath, tableName)
        t.tableInsert(dfTable)
        """
    )
    if isinstance(inserted, (int, float)):
        return int(inserted)
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="增量更新股票日线前复权数据到目标日期")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="DolphinDB 数据库路径")
    parser.add_argument("--table", default=DEFAULT_TABLE_NAME, help="DolphinDB 表名")
    parser.add_argument("--target-date", default=DEFAULT_TARGET_DATE, help="目标日期，格式 YYYY-MM-DD")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="每批股票数")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不写入")
    args = parser.parse_args()

    target_date = parse_date_yyyy_mm_dd(args.target_date)
    target_end_ts = int(datetime.combine(target_date, datetime.min.time()).timestamp() * 1000)

    tf = TickFlow(api_key=os.getenv("TICKFLOW_APIKEY"))
    s = connect_dolphindb_session()

    try:
        table_columns = get_table_columns(s, args.db_path, args.table)
        if "code" not in table_columns or "trade_date" not in table_columns or "close" not in table_columns:
            raise RuntimeError("目标表至少需要字段: code, trade_date, close")

        latest_df = get_latest_rows(s, args.db_path, args.table)
        if latest_df.empty:
            raise RuntimeError("目标表为空，当前脚本仅支持增量更新（非首次全量）")

        latest_df["fetch_code"] = latest_df["code"].map(to_tickflow_symbol)
        latest_df = latest_df.dropna(subset=["fetch_code"]).reset_index(drop=True)

        total_symbols = len(latest_df)
        total_written = 0
        total_new_rows = 0

        print(f"Loaded latest state for {total_symbols} symbols from {args.db_path}.{args.table}")
        print(f"Target date: {target_date}")

        for i in range(0, total_symbols, args.batch_size):
            batch_meta = latest_df.iloc[i : i + args.batch_size].copy()
            batch_idx = i // args.batch_size + 1
            total_batches = (total_symbols + args.batch_size - 1) // args.batch_size

            fetch_codes = batch_meta["fetch_code"].tolist()
            min_last_date = min(batch_meta["last_trade_date"])
            start_ts = int(datetime.combine(min_last_date, datetime.min.time()).timestamp() * 1000)

            print(
                f"[{batch_idx}/{total_batches}] fetching {len(fetch_codes)} symbols, "
                f"window {min_last_date} -> {target_date}"
            )

            fetched_map = fetch_qfq_daily_batch(
                tf=tf,
                symbols=fetch_codes,
                start_ts=start_ts,
                end_ts=target_end_ts,
            )

            rows_to_write: List[pd.DataFrame] = []
            batch_new_rows = 0

            for _, row in batch_meta.iterrows():
                db_code = str(row["code"])
                fetch_code = str(row["fetch_code"])
                last_date = row["last_trade_date"]
                last_close = float(row["last_close"])

                new_df = fetched_map.get(fetch_code, pd.DataFrame())
                if new_df.empty:
                    continue

                new_df = new_df.copy()
                if "trade_date" not in new_df.columns or "close" not in new_df.columns:
                    continue

                new_df["trade_date"] = pd.to_datetime(new_df["trade_date"]).dt.date
                new_df = new_df[new_df["trade_date"] >= last_date].copy()
                if new_df.empty:
                    continue

                anchor_df = new_df[new_df["trade_date"] == last_date]
                if anchor_df.empty:
                    # 没有重叠日，退化为首行锚点
                    anchor_close = float(pd.to_numeric(new_df.iloc[0]["close"], errors="coerce"))
                else:
                    anchor_close = float(pd.to_numeric(anchor_df.iloc[-1]["close"], errors="coerce"))

                if anchor_close <= 0 or last_close <= 0:
                    continue

                scale = last_close / anchor_close
                for px_col in ["open", "high", "low", "close"]:
                    if px_col in new_df.columns:
                        new_df[px_col] = pd.to_numeric(new_df[px_col], errors="coerce") * scale

                new_df["code"] = db_code
                insert_df = new_df[new_df["trade_date"] > last_date].copy()
                if insert_df.empty:
                    continue

                insert_df = normalize_output_columns(insert_df, table_columns)
                insert_df = insert_df.dropna(subset=[c for c in ["code", "trade_date", "close"] if c in insert_df.columns])
                if insert_df.empty:
                    continue

                batch_new_rows += len(insert_df)
                rows_to_write.append(insert_df)

            if rows_to_write:
                batch_all = pd.concat(rows_to_write, ignore_index=True)
                batch_all = batch_all.sort_values([c for c in ["code", "trade_date"] if c in batch_all.columns])
                total_new_rows += len(batch_all)

                if args.dry_run:
                    print(f"[{batch_idx}/{total_batches}] dry-run new rows: {len(batch_all)}")
                else:
                    written = table_insert(s, args.db_path, args.table, batch_all)
                    total_written += written
                    print(f"[{batch_idx}/{total_batches}] inserted rows: {written}")
            else:
                print(f"[{batch_idx}/{total_batches}] no new rows")

            if batch_idx < total_batches:
                time.sleep(BATCH_INTERVAL_SEC)

        if args.dry_run:
            print(f"Dry-run done. would_insert={total_new_rows}")
        else:
            print(f"Done. inserted={total_written}, planned_new_rows={total_new_rows}")

    finally:
        try:
            s.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
