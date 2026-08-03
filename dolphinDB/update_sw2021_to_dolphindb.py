from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import tushare as ts
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = "dfs://market_data"

# SW2021 配置
SRC = "SW2021"
LEVELS = ["L1", "L2", "L3"]
CLASSIFY_TABLE_MAP: Dict[str, str] = {
    "L1": "sw2021_index_classify_l1",
    "L2": "sw2021_index_classify_l2",
    "L3": "sw2021_index_classify_l3",
}
CLASSIFY_TABLES = tuple(CLASSIFY_TABLE_MAP.values())
L1_MEMBERS_TABLE = "sw2021_l1_members"


def parse_date_arg(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def save_table_csv(df: pd.DataFrame, table_name: str) -> Path:
    """将 DataFrame 保存为 CSV 文件。"""
    out_dir = DATA_DIR / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.csv"
    df.to_csv(out_file, index=False, encoding="utf-8")
    return out_file


def filter_members_by_range(df: pd.DataFrame, start_date: date | None, end_date: date | None) -> pd.DataFrame:
    """按日期范围筛选成分股数据。"""
    if df.empty:
        return df

    out = df.copy()
    out["in_date_dt"] = pd.to_datetime(out["in_date"], format="%Y%m%d", errors="coerce").dt.date
    out["out_date_dt"] = pd.to_datetime(out["out_date"], format="%Y%m%d", errors="coerce").dt.date

    mask = pd.Series(True, index=out.index)
    if end_date is not None:
        mask = mask & ((out["in_date_dt"].isna()) | (out["in_date_dt"] <= end_date))
    if start_date is not None:
        mask = mask & ((out["out_date_dt"].isna()) | (out["out_date_dt"] >= start_date))

    out = out[mask].copy()
    out = out.drop(columns=["in_date_dt", "out_date_dt"])
    return out.reset_index(drop=True)


def download_classify_and_l1_members(start_date: date | None, end_date: date | None) -> None:
    """从 Tushare 下载 SW2021 分类和成分股数据。"""
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is missing in .env")

    ts.set_token(token)
    pro = ts.pro_api()

    # 下载三级行业分类
    l1_df = pd.DataFrame()
    for level in LEVELS:
        df = pro.index_classify(level=level, src=SRC)
        table_name = CLASSIFY_TABLE_MAP[level]
        out_file = save_table_csv(df, table_name)
        print(f"[{table_name}] rows={len(df)} -> {out_file}")
        if level == "L1":
            l1_df = df.copy()

    if l1_df.empty or "index_code" not in l1_df.columns:
        raise RuntimeError("L1 classify data is empty or missing index_code")

    # 下载一级行业成分股
    all_members: List[pd.DataFrame] = []
    total_l1 = len(l1_df)

    for i, (_, row) in enumerate(l1_df.iterrows(), start=1):
        l1_code = str(row["index_code"])
        l1_name = str(row.get("industry_name", ""))

        df_default = pro.index_member_all(l1_code=l1_code)
        df_is_new_n = pro.index_member_all(l1_code=l1_code, is_new="N")

        merged = pd.concat([df_default, df_is_new_n], ignore_index=True)
        if not merged.empty:
            merged = merged.drop_duplicates(keep="last").reset_index(drop=True)
            merged["l1_code"] = l1_code
            merged["l1_name"] = l1_name
            front_cols = ["l1_code", "l1_name"]
            rest_cols = [c for c in merged.columns if c not in front_cols]
            merged = merged[front_cols + rest_cols]
            all_members.append(merged)

        print(f"[L1 {i}/{total_l1}] {l1_code} {l1_name}: rows={len(merged)}")
        time.sleep(0.2)

    all_l1 = pd.concat(all_members, ignore_index=True) if all_members else pd.DataFrame()
    all_l1 = filter_members_by_range(all_l1, start_date=start_date, end_date=end_date)
    out_file = save_table_csv(all_l1, L1_MEMBERS_TABLE)
    print(f"[{L1_MEMBERS_TABLE}] rows={len(all_l1)} -> {out_file}")


def load_classify_data() -> Dict[str, pd.DataFrame]:
    """从 CSV 加载三张分类维表。"""
    out: Dict[str, pd.DataFrame] = {}
    for table in CLASSIFY_TABLES:
        path = DATA_DIR / table / "data.csv"
        if path.exists():
            out[table] = pd.read_csv(path, dtype=str)
        else:
            out[table] = pd.DataFrame()
    return out


def convert_ts_code_suffix(ts_code: str) -> str:
    """将 Tushare 代码后缀转换为标准代码（如 SH->SSE）。"""
    if not isinstance(ts_code, str) or "." not in ts_code:
        return str(ts_code)
    code, suffix = ts_code.rsplit(".", 1)
    suffix_map = {"SH": "SSE", "SZ": "SZSE", "BJ": "BJSE"}
    return f"{code}.{suffix_map.get(suffix.upper(), suffix)}"


def load_l1_members_data(start_date: date | None, end_date: date | None) -> pd.DataFrame:
    """从 CSV 加载一级行业成分股数据。"""
    path = DATA_DIR / L1_MEMBERS_TABLE / "data.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str)
    if df.empty:
        return df

    required_cols = [
        "l1_code", "l1_name", "l2_code", "l2_name",
        "l3_code", "l3_name", "ts_code", "name",
        "in_date", "out_date", "is_new",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    df = df[required_cols].copy()
    df["stock_code"] = df["ts_code"].astype(str).map(convert_ts_code_suffix)

    df["in_date"] = pd.to_datetime(df["in_date"], format="%Y%m%d", errors="coerce").dt.date
    df["out_date"] = pd.to_datetime(df["out_date"], format="%Y%m%d", errors="coerce").dt.date

    mask = pd.Series(True, index=df.index)
    if end_date is not None:
        mask = mask & ((df["in_date"].isna()) | (df["in_date"] <= end_date))
    if start_date is not None:
        mask = mask & ((df["out_date"].isna()) | (df["out_date"] >= start_date))

    df = df[mask].copy()
    cols = [
        "l1_code", "l1_name", "l2_code", "l2_name",
        "l3_code", "l3_name", "stock_code", "ts_code",
        "name", "in_date", "out_date", "is_new",
    ]
    return df[cols].drop_duplicates(subset=["l1_code", "stock_code", "in_date"], keep="last").reset_index(drop=True)


def prepare_classify_for_ddb(df: pd.DataFrame) -> pd.DataFrame:
    """为 DolphinDB 准备分类维表数据。"""
    out = df.copy()
    for col in ["industry_code", "is_pub", "parent_code"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int32")
    for col in ["index_code", "industry_name", "level", "src"]:
        out[col] = out[col].astype(str)
    cols = ["index_code", "industry_name", "level", "industry_code", "is_pub", "parent_code", "src"]
    return out[cols].copy()


def prepare_members_for_ddb(df: pd.DataFrame) -> pd.DataFrame:
    """为 DolphinDB 准备成分股数据，保持日期为 ISO 格式字符串。"""
    out = df.copy()
    for col in ("in_date", "out_date"):
        if col in out.columns:
            # 保持为 YYYY-MM-DD 格式字符串，兼容 STRING 或 DATE 列
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d")
    cols = [
        "l1_code", "l1_name", "l2_code", "l2_name",
        "l3_code", "l3_name", "stock_code", "ts_code",
        "name", "in_date", "out_date", "is_new",
    ]
    return out[[c for c in cols if c in out.columns]].copy()


def connect_dolphindb_session():
    """连接到 DolphinDB 服务器。"""
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    s = ddb.Session()
    s.connect(host, port, user, password)
    return s


def ensure_sw2021_tables(session) -> None:
    """若目标表不存在则按标准 schema 建表。"""
    session.run(
        f"""
        dbPath = "{DB_PATH}"
        if(!existsDatabase(dbPath)) {{
            db = database(dbPath, VALUE, 1..1, , "OLAP")
        }} else {{
            db = database(dbPath)
        }}

        schClassify = table(
            1:0,
            `index_code`industry_name`level`industry_code`is_pub`parent_code`src,
            [`SYMBOL, `STRING, `STRING, `INT, `INT, `INT, `STRING]
        )
        tableNames = ["sw2021_index_classify_l1", "sw2021_index_classify_l2", "sw2021_index_classify_l3"]
        for(tn in tableNames) {{
            if(!existsTable(dbPath, tn)) {{
                db.createTable(schClassify, tn)
                print("created " + tn)
            }}
        }}

        schMembers = table(
            1:0,
            `l1_code`l1_name`l2_code`l2_name`l3_code`l3_name`stock_code`ts_code`name`in_date`out_date`is_new,
            [`SYMBOL, `STRING, `SYMBOL, `STRING, `SYMBOL, `STRING, `SYMBOL, `SYMBOL, `STRING, `STRING, `STRING, `STRING]
        )
        if(!existsTable(dbPath, "sw2021_l1_members")) {{
            db.createTable(schMembers, "sw2021_l1_members")
            print("created sw2021_l1_members")
        }}
        """
    )


def ddb_table_insert(session, table_name: str, df: pd.DataFrame) -> int:
    """将 DataFrame 追加写入 DolphinDB 表，返回实际写入行数。"""
    if df.empty:
        return 0
    session.upload({"dbPath": DB_PATH, "tableName": table_name, "dfTable": df})
    inserted = session.run(
        """
        t = loadTable(dbPath, tableName)
        t.tableInsert(dfTable)
        """
    )
    if isinstance(inserted, (int, float)):
        return int(inserted)
    return len(df)


def upsert_classify(session, dry_run: bool) -> None:
    """用 CSV 中的最新数据全量替换三张分类维表。"""
    classify_map = load_classify_data()
    for table_name, df in classify_map.items():
        if df.empty:
            print(f"[dolphindb][{table_name}] no data to insert")
            continue
        payload = prepare_classify_for_ddb(df)
        if dry_run:
            print(f"[dolphindb][{table_name}] dry-run rows={len(payload)}")
            continue
        # 静态维表直接清空重写
        session.upload({"dbPath": DB_PATH, "tableName": table_name})
        session.run("delete from loadTable(dbPath, tableName)")
        inserted = ddb_table_insert(session, table_name, payload)
        print(f"[dolphindb][{table_name}] inserted rows={inserted}")


def upsert_l1_members(
    session,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
) -> None:
    """加载 CSV 数据并按日期范围增量更新 sw2021_l1_members。"""
    members_df = load_l1_members_data(start_date=start_date, end_date=end_date)
    if members_df.empty:
        print("[dolphindb][sw2021_l1_members] no data to insert")
        return

    payload = prepare_members_for_ddb(members_df)

    if dry_run:
        print(f"[dolphindb][sw2021_l1_members] dry-run rows={len(payload)}")
        return

    if start_date is not None and end_date is not None:
        # 删除与本次范围有交叉的旧记录后重新写入
        session.upload({"sd": start_date, "ed": end_date})
        session.run(
            """
            t = loadTable("dfs://market_data", "sw2021_l1_members")
            delete from t
            where (in_date <= ed or isNull(in_date))
              and (out_date >= sd or isNull(out_date))
            """
        )
    else:
        # 无日期范围时全量替换
        session.run('delete from loadTable("dfs://market_data", "sw2021_l1_members")')

    inserted = ddb_table_insert(session, L1_MEMBERS_TABLE, payload)
    print(f"[dolphindb][sw2021_l1_members] inserted rows={inserted}")


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="下载 SW2021 系列数据并更新到 DolphinDB"
    )
    parser.add_argument(
        "--target",
        default="all",
        choices=["all", "sw2021_classify", "sw2021_l1_members"],
        help="要处理的目标数据集（默认 all）",
    )
    parser.add_argument("--start-date", default="", help="可选，YYYY-MM-DD，用于筛选成分股日期范围")
    parser.add_argument("--end-date", default="", help="可选，YYYY-MM-DD，用于筛选成分股日期范围")
    parser.add_argument("--skip-download", action="store_true", help="跳过下载步骤，直接从已有 CSV 插入")
    parser.add_argument("--dry-run", action="store_true", help="预览行数但不写入数据库")
    args = parser.parse_args()

    start_date = parse_date_arg(args.start_date)
    end_date = parse_date_arg(args.end_date)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("--start-date 不能晚于 --end-date")

    # 1. 下载 ---------------------------------------------------------------
    if not args.skip_download:
        print("=== 步骤 1/2: 下载数据 ===")
        download_classify_and_l1_members(start_date=start_date, end_date=end_date)
    else:
        print("=== 跳过下载步骤 ===")

    # 2. 写入 DolphinDB ------------------------------------------------------
    print("=== 步骤 2/2: 写入 DolphinDB ===")
    session = connect_dolphindb_session()
    try:
        ensure_sw2021_tables(session)

        if args.target in ("sw2021_classify", "all"):
            upsert_classify(session, dry_run=args.dry_run)

        if args.target in ("sw2021_l1_members", "all"):
            upsert_l1_members(session, start_date=start_date, end_date=end_date, dry_run=args.dry_run)

    finally:
        try:
            session.close()
        except Exception:
            pass

    print("SW2021 更新完成。")


if __name__ == "__main__":
    main()
