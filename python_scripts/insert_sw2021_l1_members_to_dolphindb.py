import os
from typing import Dict

import dolphindb as ddb
import pandas as pd


DDB_HOST = os.getenv("DOLPHINDB_HOST", "localhost")
DDB_PORT = int(os.getenv("DOLPHINDB_PORT", 8848))
DDB_USER = os.getenv("DOLPHINDB_USER", "admin")
DDB_PWD = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

DB_PATH = "dfs://market_data"
TABLE_NAME = "sw2021_l1_members"
CSV_PATH = "tushare_data/sw2021/members_l1/all_l1_members_merged.csv"


def connect_dolphindb() -> ddb.Session:
    s = ddb.Session()
    s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PWD)
    return s


def convert_ts_code_suffix(ts_code: str) -> str:
    if not isinstance(ts_code, str) or "." not in ts_code:
        return str(ts_code)

    code, suffix = ts_code.rsplit(".", 1)
    suffix_map: Dict[str, str] = {
        "SH": "SSE",
        "SZ": "SZSE",
        "BJ": "BJSE",
    }
    target_suffix = suffix_map.get(suffix.upper(), suffix)
    return f"{code}.{target_suffix}"


def load_and_normalize_members(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str)

    expected_cols = [
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "ts_code",
        "name",
        "in_date",
        "out_date",
        "is_new",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV 缺少字段: {missing}")

    df = df[expected_cols].copy()

    # ts_code: 000001.SZ -> 000001.SZSE, 600000.SH -> 600000.SSE
    df["stock_code"] = df["ts_code"].astype(str).map(convert_ts_code_suffix)

    # 与 stock_kline_daily_qfq.trade_date 保持一致：DolphinDB DATE
    # 原始格式示例：19990429
    df["in_date"] = pd.to_datetime(df["in_date"], format="%Y%m%d", errors="coerce").dt.date
    df["out_date"] = pd.to_datetime(df["out_date"], format="%Y%m%d", errors="coerce").dt.date

    df = df[
        [
            "l1_code",
            "l1_name",
            "l2_code",
            "l2_name",
            "l3_code",
            "l3_name",
            "stock_code",
            "ts_code",
            "name",
            "in_date",
            "out_date",
            "is_new",
        ]
    ].copy()

    # 轻量去重，避免重复写入
    df = df.drop_duplicates(subset=["l1_code", "stock_code", "in_date"], keep="last")
    return df.reset_index(drop=True)


def recreate_table(s: ddb.Session) -> None:
    script = f"""
        dbPath = \"{DB_PATH}\"
        tableName = \"{TABLE_NAME}\"
        if(!existsDatabase(dbPath)){{
            throw \"Database not found: \" + dbPath
        }}
        db = database(dbPath)

        if(existsTable(dbPath, tableName)){{
            dropTable(db, tableName)
        }}

        sch = table(
            1:0,
            `l1_code`l1_name`l2_code`l2_name`l3_code`l3_name`stock_code`ts_code`name`in_date`out_date`is_new,
            [`SYMBOL, `STRING, `SYMBOL, `STRING, `SYMBOL, `STRING, `SYMBOL, `SYMBOL, `STRING, `DATE, `DATE, `STRING]
        )

        db.createTable(sch, tableName)
    """
    s.run(script)


def insert_members(s: ddb.Session, df: pd.DataFrame) -> int:
    s.upload({"dbPath": DB_PATH, "tableName": TABLE_NAME, "dfTable": df})
    inserted = s.run(
        """
            t = loadTable(dbPath, tableName)
            t.tableInsert(dfTable)
        """
    )
    if isinstance(inserted, (int, float)):
        return int(inserted)
    return len(df)


def main() -> None:
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"找不到文件: {CSV_PATH}")

    df = load_and_normalize_members(CSV_PATH)
    if df.empty:
        raise RuntimeError("处理后数据为空，已停止写入")

    s = connect_dolphindb()
    try:
        recreate_table(s)
        rows = insert_members(s, df)
        print(f"Inserted rows: {rows} -> {DB_PATH}.{TABLE_NAME}")

        s.upload({"dbPath": DB_PATH, "tableName": TABLE_NAME})
        cnt = s.run("exec count(*) from loadTable(dbPath, tableName)")
        print(f"Table count check: {int(cnt)}")
    finally:
        try:
            s.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
