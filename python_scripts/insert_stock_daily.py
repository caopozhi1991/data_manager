"""
从 tickflow 全量拉取 A 股历史日 K 线并写入 DolphinDB
遵守 tickflow 查询限制：批量查询 60 次/分钟，最多 100 只/次
"""

import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, cast

import pandas as pd
from dotenv import load_dotenv

from tickflow import TickFlow
import dolphindb as ddb

# 加载 .env（存放 DOLPHINDB_HOST, DOLPHINDB_PORT 等）
load_dotenv()


# ==================== 常量与配置 ====================
# tickflow 批量查询限制
BATCH_SIZE = 100          # 每批最多 100 个标的
RPM = 60                  # 60 次/分钟
BATCH_INTERVAL_SEC = 60.0 / RPM   # 1 秒（RPM=60 时）
DAY_MS = 86400000
KLINE_MAX_COUNT_PER_REQ = int(os.getenv("KLINE_MAX_COUNT_PER_REQ", 10000))
INTRADAY_MAX_COUNT_PER_REQ = int(os.getenv("INTRADAY_MAX_COUNT_PER_REQ", 5000))
INTRADAY_BATCH_RPM = int(os.getenv("INTRADAY_BATCH_RPM", 30))
INTRADAY_BATCH_INTERVAL_SEC = 60.0 / max(1, INTRADAY_BATCH_RPM)

# 时间范围（可手动调整）
START_DATE = datetime(2020, 1, 1)    # 从 2020 年开始
END_DATE = datetime.now()          # 到今天为止

# DolphinDB 连接信息
DDB_HOST = os.getenv("DOLPHINDB_HOST", "localhost")
DDB_PORT = int(os.getenv("DOLPHINDB_PORT", 8848))
DDB_USER = os.getenv("DOLPHINDB_USER", "admin")
DDB_PWD = os.getenv("DOLPHINDB_PWD", "123456")
DDB_DB_PATH = "dfs://ohlcv_daily"
DDB_TABLE_NAME = "stock_kline_daily"

# 交易日K线字段类型（保持与 DolphinDB 表定义一致）
KLINE_COLUMNS = ["code", "name", "trade_date", "open", "high", "low", "close", "volume", "amount", "adjust_factor"]
tf = TickFlow(api_key=os.getenv("TICKFLOW_APIKEY"))

# ==================== 辅助函数 ====================
def get_ex_factors_batch(symbols: List[str]) -> Dict[str, pd.DataFrame]:
    """批量获取除权因子，返回按 symbol 分组的因子表"""
    try:
        factors_raw = tf.klines.ex_factors(symbols, as_dataframe=True)
    except Exception as e:
        print(f"获取除权因子失败: {e}")
        return {}

    factors_df = pd.DataFrame(factors_raw)
    if factors_df.empty:
        return {}

    if "symbol" not in factors_df.columns and "code" in factors_df.columns:
        factors_df = factors_df.rename(columns={"code": "symbol"})

    required_cols = {"symbol", "trade_date", "ex_factor"}
    if not required_cols.issubset(set(factors_df.columns)):
        print("除权因子返回字段不完整，已跳过因子合并")
        return {}

    selected_cols = ["symbol", "trade_date", "ex_factor"]
    if "timestamp" in factors_df.columns:
        selected_cols.append("timestamp")

    factors_df = factors_df.loc[:, selected_cols].copy()
    factors_df["trade_date"] = pd.to_datetime(factors_df["trade_date"]).dt.date
    factors_df["ex_factor"] = pd.to_numeric(factors_df["ex_factor"], errors="coerce")

    if "timestamp" in factors_df.columns:
        factors_df["timestamp"] = pd.to_numeric(factors_df["timestamp"], errors="coerce")

    factors_df = factors_df.dropna(subset=["symbol", "trade_date", "ex_factor"])
    if factors_df.empty:
        return {}

    sort_cols = ["symbol", "trade_date"]
    if "timestamp" in factors_df.columns:
        sort_cols.append("timestamp")
    factors_df = factors_df.sort_values(sort_cols)

    factor_map: Dict[str, pd.DataFrame] = {}
    for sym, grp in factors_df.groupby("symbol"):
        sym_key = str(sym)
        factor_map[sym_key] = grp.drop_duplicates(subset=["trade_date"], keep="last").loc[:, ["trade_date", "ex_factor"]]
    return factor_map


def get_history_klines_batch(symbols: List[str], start_ts: int, end_ts: int) -> Dict[str, pd.DataFrame]:
    """
    批量获取一批股票的历史日 K 线（含复权因子 adjust_factor）
    返回: { "000001.SZ": pd.DataFrame, ... }
    """
    if start_ts > end_ts:
        raise ValueError("start_ts 不能大于 end_ts")

    # 超过单次上限时按时间区间拆分，保证每次请求 count 不超过上限。
    max_days_per_req = max(1, KLINE_MAX_COUNT_PER_REQ)
    chunk_ranges = []
    cursor = start_ts
    while cursor <= end_ts:
        chunk_end = min(end_ts, cursor + max_days_per_req * DAY_MS - 1)
        chunk_ranges.append((cursor, chunk_end))
        cursor = chunk_end + 1

    raw_by_symbol: Dict[str, List[pd.DataFrame]] = {sym: [] for sym in symbols}
    for chunk_idx, (chunk_start, chunk_end) in enumerate(chunk_ranges, start=1):
        chunk_day_span = max(1, int((chunk_end - chunk_start) / DAY_MS) + 1)
        result = tf.klines.batch(
            symbols=symbols,
            period="1d",
            start_time=chunk_start,
            end_time=chunk_end,
            count=min(chunk_day_span, KLINE_MAX_COUNT_PER_REQ),
            adjust="none",        # 不复权 K 线价格
            as_dataframe=True,
            show_progress=(chunk_idx == 1),
            batch_size=BATCH_SIZE,
            max_workers=5,           # 并发请求，内部自动控制频率
        )
        for sym in symbols:
            sym_df = result.get(sym)
            if sym_df is None:
                continue
            sym_df = pd.DataFrame(sym_df)
            if sym_df.empty:
                continue
            raw_by_symbol[sym].append(sym_df)

    ex_factor_map = get_ex_factors_batch(symbols)

    # 标准化列名和日期类型
    clean_dict = {}
    for sym in symbols:
        raw_chunks = raw_by_symbol.get(sym, [])
        if not raw_chunks:
            clean_dict[sym] = pd.DataFrame(columns=KLINE_COLUMNS)
            continue
        df = pd.concat(raw_chunks, ignore_index=True)
        # 重命名常见列名
        rename_map = {}
        rename_map['symbol'] = 'code'  # tickflow 返回的 symbol 列重命名为 code
        df = df.rename(columns=rename_map)

        # 确保 trade_date 为日期类型
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        else:
            # 如果没有日期列，跳过
            continue

        df["name"] = ""

        factor_df = ex_factor_map.get(str(sym))
        if factor_df is not None and not factor_df.empty:
            factor_tmp = factor_df.copy()
            factor_tmp["trade_date"] = pd.to_datetime(factor_tmp["trade_date"])

            df = pd.merge_asof(
                df.sort_values("trade_date"),
                factor_tmp.rename(columns={"ex_factor": "adjust_factor"}).sort_values("trade_date"),
                on="trade_date",
                direction="backward",
            )
            df["adjust_factor"] = df["adjust_factor"].fillna(1.0)
        else:
            df["adjust_factor"] = 1.0

        # 输出前转回 date，保证与 DolphinDB DATE 列一致
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")

        # 按标准列顺序输出
        out_df = df[KLINE_COLUMNS].copy()
        clean_dict[sym] = out_df
    return clean_dict


def get_intraday_klines_multi_period_batch(
    symbols: List[str],
    start_ts: int,
    end_ts: int,
    periods: Optional[List[str]] = None,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    使用 tf.klines.batch 按多个分钟周期批量获取 K 线。

    参数
    ----------
    symbols : List[str]
        标的列表，如 ["600000.SH", "000001.SZ"]。
    start_ts : int
        起始毫秒时间戳。
    end_ts : int
        结束毫秒时间戳。
    periods : Optional[List[str]]
        周期列表，默认 ["1m", "5m", "15m", "30m", "60m"]。

    返回
    ----------
    Dict[str, Dict[str, pd.DataFrame]]
        结构为 {period: {symbol: DataFrame}}。
    """
    if start_ts > end_ts:
        raise ValueError("start_ts 不能大于 end_ts")

    if not symbols:
        return {}

    period_to_ms = {
        "1m": 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "30m": 30 * 60_000,
        "60m": 60 * 60_000,
    }
    if periods is None:
        periods = ["1m", "5m", "15m", "30m", "60m"]

    for p in periods:
        if p not in period_to_ms:
            raise ValueError(f"不支持的 period: {p}")

    max_count_per_req = max(1, min(KLINE_MAX_COUNT_PER_REQ, INTRADAY_MAX_COUNT_PER_REQ))
    symbol_chunks = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    last_batch_call_time: Optional[float] = None
    output: Dict[str, Dict[str, pd.DataFrame]] = {}

    for period in periods:
        bar_ms = period_to_ms[period]
        max_window_ms = max_count_per_req * bar_ms

        chunk_ranges = []
        cursor = start_ts
        while cursor <= end_ts:
            chunk_end = min(end_ts, cursor + max_window_ms - 1)
            chunk_ranges.append((cursor, chunk_end))
            cursor = chunk_end + 1

        raw_by_symbol: Dict[str, List[pd.DataFrame]] = {sym: [] for sym in symbols}
        for chunk_idx, (chunk_start, chunk_end) in enumerate(chunk_ranges, start=1):
            chunk_count = max(1, int((chunk_end - chunk_start) / bar_ms) + 1)
            for symbol_chunk_idx, symbol_chunk in enumerate(symbol_chunks, start=1):
                if last_batch_call_time is not None:
                    elapsed = time.monotonic() - last_batch_call_time
                    wait_sec = INTRADAY_BATCH_INTERVAL_SEC - elapsed
                    if wait_sec > 0:
                        time.sleep(wait_sec)

                chunk_result = tf.klines.batch(
                    symbols=symbol_chunk,
                    period=cast(Any, period),
                    start_time=chunk_start,
                    end_time=chunk_end,
                    count=min(chunk_count, max_count_per_req),
                    adjust="none",
                    as_dataframe=True,
                    show_progress=(chunk_idx == 1 and symbol_chunk_idx == 1),
                    batch_size=BATCH_SIZE,
                    max_workers=5,
                )
                last_batch_call_time = time.monotonic()

                for sym in symbol_chunk:
                    sym_df = chunk_result.get(sym)
                    if sym_df is None:
                        continue
                    sym_df = pd.DataFrame(sym_df)
                    if sym_df.empty:
                        continue
                    raw_by_symbol[sym].append(sym_df)

        period_dict: Dict[str, pd.DataFrame] = {}
        for sym in symbols:
            raw_chunks = raw_by_symbol.get(sym, [])
            if not raw_chunks:
                period_dict[sym] = pd.DataFrame()
                continue

            merged_df = pd.concat(raw_chunks, ignore_index=True)
            if "timestamp" in merged_df.columns:
                merged_df = merged_df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
            elif "trade_time" in merged_df.columns:
                merged_df = merged_df.sort_values("trade_time").drop_duplicates(subset=["trade_time"], keep="last")
            elif "trade_date" in merged_df.columns:
                merged_df = merged_df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")

            period_dict[sym] = merged_df.reset_index(drop=True)

        output[period] = period_dict

    return output


def get_all_stock_symbols() -> List[str]:
    """获取全市场 A 股（沪深北）股票代码列表"""
    all_instruments = []
    for exchange in ["SH", "SZ", "BJ"]:
        instruments_raw = tf.exchanges.get_instruments(
            exchange=exchange,
            instrument_type="stock",
        )
        if instruments_raw is None:
            continue
        instruments_df: pd.DataFrame = pd.DataFrame(instruments_raw)
        if instruments_df.empty:
            continue
        if "symbol" not in instruments_df.columns:
            raise RuntimeError(f"{exchange} 市场返回结果中缺少 symbol 列")
        all_instruments.append(instruments_df.loc[:, ["symbol"]].copy())

    if not all_instruments:
        raise RuntimeError("无法获取股票列表，请检查 tickflow 连接")

    symbols_df = pd.concat(all_instruments, ignore_index=True)
    symbols_df = symbols_df.dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"])
    return symbols_df["symbol"].tolist()


def write_to_dolphindb(df: pd.DataFrame, ddb_session: ddb.Session) -> int:
    """将单个股票的 K 线数据追加写入 DolphinDB 分区表"""
    if df.empty:
        return 0
    # 先上传参数，再执行无参脚本，避免 run(..., *args) 走 call 路径导致脚本体被当作函数名。
    ddb_session.upload(
        {
            "dfTable": df,
            "dbPath": DDB_DB_PATH,
            "tableName": DDB_TABLE_NAME,
        }
    )
    res = ddb_session.run("""
        pt = loadTable(dbPath, tableName)
        pt.tableInsert(dfTable)
    """)
    # 返回写入行数（最后一个表达式的值）
    if isinstance(res, (int, float)):
        return int(res)
    return len(df)


def connect_dolphindb_session() -> ddb.Session:
    """创建并连接 DolphinDB 会话"""
    session = ddb.Session()
    session.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PWD)
    return session


def is_closed_connection_error(exc: Exception) -> bool:
    """判断是否为连接已关闭错误"""
    err = str(exc).lower()
    return "connection has been closed" in err or "connection closed" in err


def ensure_dolphindb_table(ddb_session: ddb.Session):
    """检查 DolphinDB 表是否存在，若不存在则创建（按 trade_date 范围分区）"""
    create_table_script = f"""
        dbPath = "{DDB_DB_PATH}"
        tableName = "{DDB_TABLE_NAME}"
        db = database(dbPath)
        if(not existsTable(dbPath, tableName)){{
            sch = table(1:0, `code`name`trade_date`open`high`low`close`volume`amount`adjust_factor,
                [`SYMBOL, `STRING, `DATE, `DOUBLE, `DOUBLE, `DOUBLE, `DOUBLE, `INT, `DOUBLE, `DOUBLE])
            db.createPartitionedTable(sch, tableName, `trade_date)
        }}
    """
    ddb_session.run(create_table_script)
    print(f"DolphinDB 表 {DDB_DB_PATH}.{DDB_TABLE_NAME} 已准备就绪")


# ==================== 主流程 ====================
def main():
    # 1. 连接 DolphinDB
    s = connect_dolphindb_session()
    print("已连接 DolphinDB")
    ensure_dolphindb_table(s)

    # 2. 获取全市场股票列表
    symbols = get_all_stock_symbols()
    print(f"获取到 {len(symbols)} 只股票")

    # 3. 定义时间范围对应的毫秒时间戳
    start_ts = int(START_DATE.timestamp() * 1000)
    end_ts = int(END_DATE.timestamp() * 1000)

    # 4. 分批获取并写入
    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    total_written = 0

    for i in range(0, len(symbols), BATCH_SIZE):
        batch_symbols = symbols[i:i+BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        print(f"正在处理第 {batch_idx}/{total_batches} 批，共 {len(batch_symbols)} 支股票...")

        # 4.1 获取该批次的 K 线数据
        try:
            kline_dict = get_history_klines_batch(batch_symbols, start_ts, end_ts)
        except Exception as e:
            print(f"第 {batch_idx} 批失败: {e}")
            # 等待 RATE_LIMIT 间隔后继续下一批
            time.sleep(BATCH_INTERVAL_SEC)
            continue

        # 4.2 逐只股票写入 DolphinDB（也可合并后批量写入，此处为简单可靠按股票写入）
        for sym, df in kline_dict.items():
            if df.empty:
                continue
            try:
                rows = write_to_dolphindb(df, s)
                total_written += rows
                print(f"✓ {sym} 写入 {rows} 行")
            except Exception as e:
                if is_closed_connection_error(e):
                    print(f"! {sym} 写入时连接已断开，尝试重连并重试...")
                    try:
                        try:
                            s.close()
                        except Exception:
                            pass
                        s = connect_dolphindb_session()
                        rows = write_to_dolphindb(df, s)
                        total_written += rows
                        print(f"✓ {sym} 重试写入成功 {rows} 行")
                    except Exception as retry_e:
                        print(f"✗ {sym} 重连后写入失败: {retry_e}")
                else:
                    print(f"✗ {sym} 写入失败: {e}")

        # 4.3 控制请求频率（遵守 60 次/分钟限制）
        if batch_idx < total_batches:
            time.sleep(BATCH_INTERVAL_SEC)

    # 5. 清理
    s.close()
    print(f"写入完成，共写入 {total_written} 条（含 adjust_factor 列）")


if __name__ == "__main__":
    main()