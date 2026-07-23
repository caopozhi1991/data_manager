from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DEFAULT_SQLITE_DB_PATH = ROOT_DIR / "sqlite" / "market_data.db"

CLASSIFY_TABLES: Tuple[str, ...] = (
    "sw2021_index_classify_l1",
    "sw2021_index_classify_l2",
    "sw2021_index_classify_l3",
)
STOCK_TABLE = "stock_kline_daily"
L1_MEMBERS_TABLE = "sw2021_l1_members"


def parse_date_arg(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def normalize_engine(raw_engine: str | None) -> str:
    engine = (raw_engine or "dolphinDB").strip().lower()
    if engine == "dolphindb":
        return "dolphindb"
    if engine == "sqlite":
        return "sqlite"
    raise ValueError(f"Unsupported QUANT_DATA_ENGINE: {raw_engine}")


def get_sqlite_db_path() -> Path:
    raw_path = os.getenv("SQLITE_DB_PATH")
    if raw_path:
        p = Path(raw_path)
        if not p.is_absolute():
            p = ROOT_DIR / p
        return p
    return DEFAULT_SQLITE_DB_PATH


def load_stock_daily_data(start_date: date | None, end_date: date | None) -> pd.DataFrame:
    base = DATA_DIR / STOCK_TABLE
    files = sorted(base.glob("trade_month=*/data.csv"))
    if not files:
        return pd.DataFrame()

    frames = [pd.read_csv(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    if df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    mask = pd.Series(True, index=df.index)
    if start_date is not None:
        mask = mask & (df["trade_date"] >= start_date)
    if end_date is not None:
        mask = mask & (df["trade_date"] <= end_date)

    df = df[mask].copy()
    if df.empty:
        return df

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
    return df.loc[:, keep_cols].drop_duplicates(subset=["code", "trade_date"], keep="last").reset_index(drop=True)


def load_classify_data() -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    for table in CLASSIFY_TABLES:
        path = DATA_DIR / table / "data.csv"
        if path.exists():
            out[table] = pd.read_csv(path, dtype=str)
        else:
            out[table] = pd.DataFrame()
    return out


def convert_ts_code_suffix(ts_code: str) -> str:
    if not isinstance(ts_code, str) or "." not in ts_code:
        return str(ts_code)
    code, suffix = ts_code.rsplit(".", 1)
    suffix_map = {"SH": "SSE", "SZ": "SZSE", "BJ": "BJSE"}
    return f"{code}.{suffix_map.get(suffix.upper(), suffix)}"


def load_l1_members_data(start_date: date | None, end_date: date | None) -> pd.DataFrame:
    path = DATA_DIR / L1_MEMBERS_TABLE / "data.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str)
    if df.empty:
        return df

    required_cols = [
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
    return df[cols].drop_duplicates(subset=["l1_code", "stock_code", "in_date"], keep="last").reset_index(drop=True)


def ensure_sqlite_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_kline_daily (
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sw2021_index_classify_l1 (
            index_code TEXT,
            industry_name TEXT,
            level TEXT,
            industry_code INTEGER,
            is_pub INTEGER,
            parent_code INTEGER,
            src TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sw2021_index_classify_l2 (
            index_code TEXT,
            industry_name TEXT,
            level TEXT,
            industry_code INTEGER,
            is_pub INTEGER,
            parent_code INTEGER,
            src TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sw2021_index_classify_l3 (
            index_code TEXT,
            industry_name TEXT,
            level TEXT,
            industry_code INTEGER,
            is_pub INTEGER,
            parent_code INTEGER,
            src TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sw2021_l1_members (
            l1_code TEXT,
            l1_name TEXT,
            l2_code TEXT,
            l2_name TEXT,
            l3_code TEXT,
            l3_name TEXT,
            stock_code TEXT,
            ts_code TEXT,
            name TEXT,
            in_date TEXT,
            out_date TEXT,
            is_new TEXT,
            PRIMARY KEY (l1_code, stock_code, in_date)
        )
        """
    )


def insert_sqlite(
    target: str,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
) -> None:
    db_path = get_sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        ensure_sqlite_tables(conn)

        if target in ("stock", "all"):
            stock_df = load_stock_daily_data(start_date=start_date, end_date=end_date)
            if stock_df.empty:
                print("[sqlite][stock_kline_daily] no data to insert")
            elif dry_run:
                print(f"[sqlite][stock_kline_daily] dry-run rows={len(stock_df)}")
            else:
                if start_date is not None and end_date is not None:
                    conn.execute(
                        "DELETE FROM stock_kline_daily WHERE trade_date BETWEEN ? AND ?",
                        (start_date.isoformat(), end_date.isoformat()),
                    )
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO stock_kline_daily
                    (code, name, trade_date, open, high, low, close, volume, amount, adjust_factor)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            str(row.code),
                            str(row.name) if pd.notna(row.name) else "",
                            row.trade_date.isoformat(),
                            float(row.open) if pd.notna(row.open) else None,
                            float(row.high) if pd.notna(row.high) else None,
                            float(row.low) if pd.notna(row.low) else None,
                            float(row.close) if pd.notna(row.close) else None,
                            int(row.volume) if pd.notna(row.volume) else 0,
                            float(row.amount) if pd.notna(row.amount) else None,
                            float(row.adjust_factor) if pd.notna(row.adjust_factor) else 1.0,
                        )
                        for row in stock_df.itertuples(index=False)
                    ],
                )
                print(f"[sqlite][stock_kline_daily] inserted rows={len(stock_df)}")

        if target in ("sw2021_classify", "all"):
            classify_map = load_classify_data()
            for table_name, df in classify_map.items():
                if df.empty:
                    print(f"[sqlite][{table_name}] no data to insert")
                    continue
                if dry_run:
                    print(f"[sqlite][{table_name}] dry-run rows={len(df)}")
                    continue

                conn.execute(f"DELETE FROM {table_name}")
                rows = [
                    (
                        str(row.get("index_code", "")),
                        str(row.get("industry_name", "")),
                        str(row.get("level", "")),
                        int(pd.to_numeric(row.get("industry_code", 0), errors="coerce") or 0),
                        int(pd.to_numeric(row.get("is_pub", 0), errors="coerce") or 0),
                        int(pd.to_numeric(row.get("parent_code", 0), errors="coerce") or 0),
                        str(row.get("src", "")),
                    )
                    for _, row in df.iterrows()
                ]
                conn.executemany(
                    f"""
                    INSERT INTO {table_name}
                    (index_code, industry_name, level, industry_code, is_pub, parent_code, src)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                print(f"[sqlite][{table_name}] inserted rows={len(rows)}")

        if target in ("sw2021_l1_members", "all"):
            members_df = load_l1_members_data(start_date=start_date, end_date=end_date)
            if members_df.empty:
                print("[sqlite][sw2021_l1_members] no data to insert")
            elif dry_run:
                print(f"[sqlite][sw2021_l1_members] dry-run rows={len(members_df)}")
            else:
                if start_date is not None and end_date is not None:
                    conn.execute(
                        """
                        DELETE FROM sw2021_l1_members
                        WHERE (in_date <= ? OR in_date IS NULL OR in_date = '')
                          AND (out_date >= ? OR out_date IS NULL OR out_date = '')
                        """,
                        (end_date.isoformat(), start_date.isoformat()),
                    )
                rows = [
                    (
                        str(row.l1_code),
                        str(row.l1_name),
                        str(row.l2_code),
                        str(row.l2_name),
                        str(row.l3_code),
                        str(row.l3_name),
                        str(row.stock_code),
                        str(row.ts_code),
                        str(row.name),
                        row.in_date.isoformat() if pd.notna(row.in_date) else None,
                        row.out_date.isoformat() if pd.notna(row.out_date) else None,
                        str(row.is_new),
                    )
                    for row in members_df.itertuples(index=False)
                ]
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO sw2021_l1_members
                    (l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, stock_code, ts_code, name, in_date, out_date, is_new)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                print(f"[sqlite][sw2021_l1_members] inserted rows={len(rows)}")

        if not dry_run:
            conn.commit()
    finally:
        conn.close()


def connect_dolphindb_session():
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    s = ddb.Session()
    s.connect(host, port, user, password)
    return s


def ensure_dolphindb_tables(session) -> None:
    session.run(
        """
        dbPath = "dfs://market_data"
        if(!existsDatabase(dbPath)) {
            partitionBounds = [2020.01.01, 2021.01.01, 2022.01.01, 2023.01.01, 2024.01.01, 2025.01.01, 2026.01.01, 2027.01.01, 2028.01.01, 2029.01.01, 2030.01.01, 2031.01.01]
            db = database(dbPath, RANGE, partitionBounds, , "OLAP")
        } else {
            db = database(dbPath)
        }

        if(!existsTable(dbPath, "stock_kline_daily")) {
            schDaily = table(
                1:0,
                `code`name`trade_date`open`high`low`close`volume`amount`adjust_factor,
                [`SYMBOL, `STRING, `DATE, `DOUBLE, `DOUBLE, `DOUBLE, `DOUBLE, `INT, `DOUBLE, `DOUBLE]
            )
            db.createPartitionedTable(schDaily, "stock_kline_daily", `trade_date)
        }

        schClassify = table(
            1:0,
            `index_code`industry_name`level`industry_code`is_pub`parent_code`src,
            [`SYMBOL, `STRING, `STRING, `INT, `INT, `INT, `STRING]
        )
        tableNames = ["sw2021_index_classify_l1", "sw2021_index_classify_l2", "sw2021_index_classify_l3"]
        for(tn in tableNames) {
            if(!existsTable(dbPath, tn)) {
                db.createTable(schClassify, tn)
            }
        }

        schMembers = table(
            1:0,
            `l1_code`l1_name`l2_code`l2_name`l3_code`l3_name`stock_code`ts_code`name`in_date`out_date`is_new,
            [`SYMBOL, `STRING, `SYMBOL, `STRING, `SYMBOL, `STRING, `SYMBOL, `SYMBOL, `STRING, `DATE, `DATE, `STRING]
        )
        if(!existsTable(dbPath, "sw2021_l1_members")) {
            db.createTable(schMembers, "sw2021_l1_members")
        }
        """
    )


def ddb_table_insert(session, table_name: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    session.upload({"dbPath": "dfs://market_data", "tableName": table_name, "dfTable": df})
    inserted = session.run(
        """
        t = loadTable(dbPath, tableName)
        t.tableInsert(dfTable)
        """
    )
    if isinstance(inserted, (int, float)):
        return int(inserted)
    return len(df)


def prepare_classify_for_ddb(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["industry_code", "is_pub", "parent_code"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int32")
    for col in ["index_code", "industry_name", "level", "src"]:
        out[col] = out[col].astype(str)
    cols = ["index_code", "industry_name", "level", "industry_code", "is_pub", "parent_code", "src"]
    return out[cols].copy()


def insert_dolphindb(
    target: str,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
) -> None:
    s = connect_dolphindb_session()
    try:
        ensure_dolphindb_tables(s)

        if target in ("stock", "all"):
            stock_df = load_stock_daily_data(start_date=start_date, end_date=end_date)
            if stock_df.empty:
                print("[dolphindb][stock_kline_daily] no data to insert")
            elif dry_run:
                print(f"[dolphindb][stock_kline_daily] dry-run rows={len(stock_df)}")
            else:
                if start_date is not None and end_date is not None:
                    s.upload({"sd": start_date, "ed": end_date})
                    s.run(
                        """
                        t = loadTable("dfs://market_data", "stock_kline_daily")
                        delete from t where trade_date between sd:ed
                        """
                    )
                inserted = ddb_table_insert(s, STOCK_TABLE, stock_df)
                print(f"[dolphindb][stock_kline_daily] inserted rows={inserted}")

        if target in ("sw2021_classify", "all"):
            classify_map = load_classify_data()
            for table_name, df in classify_map.items():
                if df.empty:
                    print(f"[dolphindb][{table_name}] no data to insert")
                    continue
                payload = prepare_classify_for_ddb(df)
                if dry_run:
                    print(f"[dolphindb][{table_name}] dry-run rows={len(payload)}")
                    continue
                s.upload({"dbPath": "dfs://market_data", "tableName": table_name})
                s.run("delete from loadTable(dbPath, tableName)")
                inserted = ddb_table_insert(s, table_name, payload)
                print(f"[dolphindb][{table_name}] inserted rows={inserted}")

        if target in ("sw2021_l1_members", "all"):
            members_df = load_l1_members_data(start_date=start_date, end_date=end_date)
            if members_df.empty:
                print("[dolphindb][sw2021_l1_members] no data to insert")
            elif dry_run:
                print(f"[dolphindb][sw2021_l1_members] dry-run rows={len(members_df)}")
            else:
                if start_date is not None and end_date is not None:
                    s.upload({"sd": start_date, "ed": end_date})
                    s.run(
                        """
                        t = loadTable("dfs://market_data", "sw2021_l1_members")
                        delete from t where (in_date <= ed or isNull(in_date)) and (out_date >= sd or isNull(out_date))
                        """
                    )
                inserted = ddb_table_insert(s, L1_MEMBERS_TABLE, members_df)
                print(f"[dolphindb][sw2021_l1_members] inserted rows={inserted}")
    finally:
        try:
            s.close()
        except Exception:
            pass


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(description="Insert downloaded CSV data into sqlite or dolphindb")
    parser.add_argument(
        "--target",
        default="all",
        choices=["all", "stock", "sw2021_classify", "sw2021_l1_members"],
        help="Target dataset to insert",
    )
    parser.add_argument("--start-date", default="", help="Optional YYYY-MM-DD")
    parser.add_argument("--end-date", default="", help="Optional YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Preview rows without writing")
    args = parser.parse_args()

    start_date = parse_date_arg(args.start_date)
    end_date = parse_date_arg(args.end_date)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start-date cannot be later than end-date")

    engine = normalize_engine(os.getenv("QUANT_DATA_ENGINE"))
    print(f"QUANT_DATA_ENGINE={engine}, target={args.target}, start={start_date}, end={end_date}, dry_run={args.dry_run}")

    if engine == "sqlite":
        insert_sqlite(target=args.target, start_date=start_date, end_date=end_date, dry_run=args.dry_run)
    else:
        insert_dolphindb(target=args.target, start_date=start_date, end_date=end_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
