"""
从 tickflow 批量拉取分钟级 K 线（1m/5m/15m/30m/60m）并写入 DolphinDB。
复用 insert_stock_daily.py 中的通用函数和限流参数。
"""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from insert_stock_daily import (
    BATCH_INTERVAL_SEC,
    BATCH_SIZE,
    DDB_DB_PATH,
    connect_dolphindb_session,
    get_ex_factors_batch,
    get_all_stock_symbols,
    get_intraday_klines_multi_period_batch,
    is_closed_connection_error,
)

# 默认拉取最近 365 天分钟线，可通过环境变量覆盖
INTRADAY_START = os.getenv("INTRADAY_START", (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
INTRADAY_END = os.getenv("INTRADAY_END", datetime.now().strftime("%Y-%m-%d"))
INTRADAY_PERIODS = os.getenv("INTRADAY_PERIODS", "1m,5m,15m,30m,60m")
INTRADAY_SYMBOL_LIMIT = int(os.getenv("INTRADAY_SYMBOL_LIMIT", "50"))
INTRADAY_DDB_TABLE_PREFIX = os.getenv("INTRADAY_DDB_TABLE_PREFIX", "stock_kline")
LOCAL_TIMEZONE = "Asia/Shanghai"


def _intraday_table_name(period: str) -> str:
    return f"{INTRADAY_DDB_TABLE_PREFIX}_{period}"


def _parse_periods(periods_str: str) -> List[str]:
    periods = [p.strip() for p in periods_str.split(",") if p.strip()]
    if not periods:
        return ["1m", "5m", "15m", "30m", "60m"]
    return periods


def _to_timestamp_ms(date_str: str, end_of_day: bool = False) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    return int(dt.timestamp() * 1000)


def _flatten_period_result(period_result: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    for symbol, df in period_result.items():
        if df is None:
            continue
        df = pd.DataFrame(df)
        if df.empty:
            continue
        if "symbol" not in df.columns:
            df = df.copy()
            df["symbol"] = symbol
        parts.append(df)

    if not parts:
        return pd.DataFrame()

    merged = pd.concat(parts, ignore_index=True)
    if "timestamp" in merged.columns:
        merged = merged.sort_values(["symbol", "timestamp"]).drop_duplicates(subset=["symbol", "timestamp"], keep="last")
    elif "trade_time" in merged.columns:
        merged = merged.sort_values(["symbol", "trade_time"]).drop_duplicates(subset=["symbol", "trade_time"], keep="last")
    return merged.reset_index(drop=True)


def _apply_ex_factors_to_intraday(period_df: pd.DataFrame, factor_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """将日级除权因子映射到分钟线（按交易日向前匹配最近因子）。"""
    if period_df is None or period_df.empty:
        return pd.DataFrame(period_df)

    df = pd.DataFrame(period_df).copy()
    if "symbol" not in df.columns and "code" in df.columns:
        df["symbol"] = df["code"]
    if "symbol" not in df.columns:
        df["adjust_factor"] = 1.0
        return df

    if "timestamp" in df.columns:
        trade_time = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
    elif "trade_time" in df.columns:
        trade_time = pd.to_datetime(df["trade_time"], errors="coerce")
    elif "trade_date" in df.columns:
        trade_time = pd.to_datetime(df["trade_date"], errors="coerce")
    else:
        df["adjust_factor"] = 1.0
        return df

    df["_trade_day"] = pd.to_datetime(trade_time).dt.normalize()
    df["adjust_factor"] = 1.0

    for sym, idx in df.groupby("symbol").groups.items():
        factor_df = factor_map.get(str(sym))
        if factor_df is None or factor_df.empty:
            continue

        factor_tmp = factor_df.copy()
        factor_tmp["trade_date"] = pd.to_datetime(factor_tmp["trade_date"]).dt.normalize()
        factor_tmp = factor_tmp.sort_values("trade_date")

        sub = df.loc[idx].copy().sort_values("_trade_day")
        merged = pd.merge_asof(
            sub,
            factor_tmp.rename(columns={"ex_factor": "adj_from_factor"}),
            left_on="_trade_day",
            right_on="trade_date",
            direction="backward",
        )
        df.loc[merged.index, "adjust_factor"] = merged["adj_from_factor"].fillna(1.0).to_numpy()

    df = df.drop(columns=["_trade_day"], errors="ignore")
    df["adjust_factor"] = pd.to_numeric(df["adjust_factor"], errors="coerce").fillna(1.0)
    return df


def _normalize_intraday_for_dolphindb(period_df: pd.DataFrame) -> pd.DataFrame:
    """将分钟线数据标准化为 DolphinDB 入库结构。"""
    if period_df is None or period_df.empty:
        return pd.DataFrame(columns=["code", "trade_date", "trade_time", "open", "high", "low", "close", "volume", "amount", "adjust_factor"])

    df = pd.DataFrame(period_df).copy()
    if "symbol" in df.columns and "code" not in df.columns:
        df = df.rename(columns={"symbol": "code"})

    if "code" not in df.columns:
        raise RuntimeError("分钟线数据缺少 code/symbol 列")

    # 优先使用 trade_time（交易所本地时间），避免由 UTC 毫秒戳造成 8 小时偏移。
    if "trade_time" in df.columns:
        trade_time = pd.to_datetime(df["trade_time"], errors="coerce")
    elif "timestamp" in df.columns:
        trade_time = (
            pd.to_datetime(df["timestamp"], unit="ms", errors="coerce", utc=True)
            .dt.tz_convert(LOCAL_TIMEZONE)
            .dt.tz_localize(None)
        )
    elif "trade_date" in df.columns:
        trade_time = pd.to_datetime(df["trade_date"], errors="coerce")
    else:
        raise RuntimeError("分钟线数据缺少 timestamp/trade_time/trade_date 时间列")

    df["trade_time"] = trade_time
    df = df.dropna(subset=["trade_time"])  # 无法解析时间的记录跳过
    if df.empty:
        return pd.DataFrame(columns=["code", "trade_date", "trade_time", "open", "high", "low", "close", "volume", "amount", "adjust_factor"])

    df["trade_date"] = df["trade_time"].dt.date
    for col in ["open", "high", "low", "close", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = 0.0
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    else:
        df["volume"] = 0

    if "adjust_factor" in df.columns:
        df["adjust_factor"] = pd.to_numeric(df["adjust_factor"], errors="coerce").fillna(1.0)
    else:
        df["adjust_factor"] = 1.0

    out = df.loc[:, ["code", "trade_date", "trade_time", "open", "high", "low", "close", "volume", "amount", "adjust_factor"]].copy()
    out = out.sort_values(["code", "trade_time"]).drop_duplicates(subset=["code", "trade_time"], keep="last")
    return out.reset_index(drop=True)


def ensure_intraday_table(ddb_session, period: str) -> None:
    """确保分钟线表存在（每个周期一张表）。"""
    table_name = _intraday_table_name(period)
    create_script = f"""
        dbPath = "{DDB_DB_PATH}"
        tableName = "{table_name}"
        db = database(dbPath)
        if(not existsTable(dbPath, tableName)){{
            sch = table(1:0, `code`trade_date`trade_time`open`high`low`close`volume`amount`adjust_factor,
                [`SYMBOL, `DATE, `TIMESTAMP, `DOUBLE, `DOUBLE, `DOUBLE, `DOUBLE, `LONG, `DOUBLE, `DOUBLE])
            db.createPartitionedTable(sch, tableName, `trade_date)
        }}
    """
    ddb_session.run(create_script)


def write_intraday_to_dolphindb(period_df: pd.DataFrame, period: str, ddb_session) -> int:
    """写入单个周期分钟线数据到 DolphinDB。"""
    write_df = _normalize_intraday_for_dolphindb(period_df)
    if write_df.empty:
        return 0

    table_name = _intraday_table_name(period)
    ddb_session.upload({"dfTable": write_df, "dbPath": DDB_DB_PATH, "tableName": table_name})
    res = ddb_session.run("""
        pt = loadTable(dbPath, tableName)
        pt.tableInsert(dfTable)
    """)
    if isinstance(res, (int, float)):
        return int(res)
    return len(write_df)


def get_intraday_table_row_count(ddb_session, period: str) -> int:
    """返回分钟线表当前总行数（数据库侧 count(*)）。"""
    table_name = _intraday_table_name(period)
    ddb_session.upload({"dbPath": DDB_DB_PATH, "tableName": table_name})
    exists = ddb_session.run("existsTable(dbPath, tableName)")
    if not exists:
        return 0
    cnt = ddb_session.run("exec count(*) from loadTable(dbPath, tableName)")
    if isinstance(cnt, (int, float)):
        return int(cnt)
    return int(cnt) if cnt is not None else 0


def main() -> None:
    ddb_session = connect_dolphindb_session()

    periods = _parse_periods(INTRADAY_PERIODS)
    start_ts = _to_timestamp_ms(INTRADAY_START, end_of_day=False)
    end_ts = _to_timestamp_ms(INTRADAY_END, end_of_day=True)

    symbols = get_all_stock_symbols()
    if INTRADAY_SYMBOL_LIMIT > 0:
        symbols = symbols[:INTRADAY_SYMBOL_LIMIT]

    if not symbols:
        raise RuntimeError("未获取到可用股票代码")

    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    period_total_rows = {p: 0 for p in periods}
    period_total_written = {p: 0 for p in periods}

    for period in periods:
        ensure_intraday_table(ddb_session, period)

    print(f"分钟线入库开始: symbols={len(symbols)}, periods={periods}, range={INTRADAY_START}~{INTRADAY_END}")

    for i in range(0, len(symbols), BATCH_SIZE):
        batch_symbols = symbols[i:i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        print(f"处理第 {batch_idx}/{total_batches} 批，标的数={len(batch_symbols)}")

        try:
            period_map = get_intraday_klines_multi_period_batch(
                symbols=batch_symbols,
                start_ts=start_ts,
                end_ts=end_ts,
                periods=periods,
            )
            factor_map = get_ex_factors_batch(batch_symbols)
        except Exception as e:
            print(f"第 {batch_idx} 批拉取失败: {e}")
            time.sleep(BATCH_INTERVAL_SEC)
            continue

        for period in periods:
            period_df = _flatten_period_result(period_map.get(period, {}))
            period_df = _apply_ex_factors_to_intraday(period_df, factor_map)
            rows = len(period_df)
            period_total_rows[period] += rows
            written = 0
            try:
                written = write_intraday_to_dolphindb(period_df, period, ddb_session)
            except Exception as e:
                if is_closed_connection_error(e):
                    print(f"  ! {period} 写入时连接已断开，重连后重试")
                    try:
                        try:
                            ddb_session.close()
                        except Exception:
                            pass
                        ddb_session = connect_dolphindb_session()
                        ensure_intraday_table(ddb_session, period)
                        written = write_intraday_to_dolphindb(period_df, period, ddb_session)
                    except Exception as retry_e:
                        print(f"  ✗ {period} 重连后写入失败: {retry_e}")
                else:
                    print(f"  ✗ {period} 写入失败: {e}")
            period_total_written[period] += written
            print(f"  {period}: 拉取 {rows} 行, 写入 {written} 行")

        if batch_idx < total_batches:
            time.sleep(BATCH_INTERVAL_SEC)

    print("入库完成")
    for period in periods:
        db_count = -1
        try:
            db_count = get_intraday_table_row_count(ddb_session, period)
        except Exception as e:
            print(f"{period}: 数据库计数失败: {e}")
        print(
            f"{period}: 拉取总行数={period_total_rows[period]}, "
            f"写入总行数={period_total_written[period]}, "
            f"数据库总行数={db_count}"
        )

    try:
        ddb_session.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
