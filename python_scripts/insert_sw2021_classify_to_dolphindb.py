import os
from typing import Dict, Tuple

import dolphindb as ddb
import pandas as pd


DDB_HOST = os.getenv("DOLPHINDB_HOST", "localhost")
DDB_PORT = int(os.getenv("DOLPHINDB_PORT", 8848))
DDB_USER = os.getenv("DOLPHINDB_USER", "admin")
DDB_PWD = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

DB_PATH = "dfs://market_data"
CSV_ROOT = "tushare_data/sw2021/classify"

LEVEL_TO_FILE_TABLE: Dict[str, Tuple[str, str]] = {
    "L1": ("index_classify_SW2021_L1.csv", "sw2021_index_classify_l1"),
    "L2": ("index_classify_SW2021_L2.csv", "sw2021_index_classify_l2"),
    "L3": ("index_classify_SW2021_L3.csv", "sw2021_index_classify_l3"),
}


def connect_dolphindb() -> ddb.Session:
    s = ddb.Session()
    s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PWD)
    return s


def ensure_db_and_tables(s: ddb.Session) -> None:
    create_script = """
        dbPath = "dfs://market_data"
        if(!existsDatabase(dbPath)){
            db = database(dbPath, VALUE, 1..1, , "OLAP")
        } else {
            db = database(dbPath)
        }

        schema = table(
            1:0,
            `index_code`industry_name`level`industry_code`is_pub`parent_code`src,
            [`SYMBOL, `STRING, `STRING, `INT, `INT, `INT, `STRING]
        )

        tableNames = ["sw2021_index_classify_l1", "sw2021_index_classify_l2", "sw2021_index_classify_l3"]

        for(tn in tableNames) {
            if(existsTable(dbPath, tn)) {
                dropTable(dbPath, tn)
            }
            db.createTable(schema, tn)
        }
    """
    s.run(create_script)


def load_and_normalize(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str)
    expected_cols = [
        "index_code",
        "industry_name",
        "level",
        "industry_code",
        "is_pub",
        "parent_code",
        "src",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"文件 {csv_path} 缺少字段: {missing}")

    df = df[expected_cols].copy()
    for c in ["industry_code", "is_pub", "parent_code"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int32")

    df["index_code"] = df["index_code"].astype(str)
    df["industry_name"] = df["industry_name"].astype(str)
    df["level"] = df["level"].astype(str)
    df["src"] = df["src"].astype(str)
    return df


def insert_one_table(s: ddb.Session, table_name: str, df: pd.DataFrame) -> int:
    s.upload({"dbPath": DB_PATH, "tableName": table_name, "dfTable": df})
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
    s = connect_dolphindb()
    try:
        print(f"Connected DolphinDB: {DDB_HOST}:{DDB_PORT}, db={DB_PATH}")
        ensure_db_and_tables(s)
        print("Tables recreated: sw2021_index_classify_l1/l2/l3")

        total = 0
        for level, (file_name, table_name) in LEVEL_TO_FILE_TABLE.items():
            csv_path = os.path.join(CSV_ROOT, file_name)
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV not found: {csv_path}")

            df = load_and_normalize(csv_path)
            rows = insert_one_table(s, table_name, df)
            total += rows
            print(f"[{level}] {rows} rows inserted -> {DB_PATH}.{table_name}")

        print(f"Done. Total inserted rows: {total}")
    finally:
        try:
            s.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
