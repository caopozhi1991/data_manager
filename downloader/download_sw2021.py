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
SRC = "SW2021"
LEVELS = ["L1", "L2", "L3"]

CLASSIFY_TABLE_MAP: Dict[str, str] = {
    "L1": "sw2021_index_classify_l1",
    "L2": "sw2021_index_classify_l2",
    "L3": "sw2021_index_classify_l3",
}
L1_MEMBERS_TABLE = "sw2021_l1_members"


def parse_date_arg(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def save_table_csv(df: pd.DataFrame, table_name: str) -> Path:
    out_dir = DATA_DIR / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.csv"
    df.to_csv(out_file, index=False, encoding="utf-8")
    return out_file


def filter_members_by_range(df: pd.DataFrame, start_date: date | None, end_date: date | None) -> pd.DataFrame:
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
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is missing in .env")

    ts.set_token(token)
    pro = ts.pro_api()

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


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(description="Download SW2021 classify and L1 members into data folder")
    parser.add_argument("--start-date", default="", help="Optional YYYY-MM-DD. Used to filter l1 members by overlap")
    parser.add_argument("--end-date", default="", help="Optional YYYY-MM-DD. Used to filter l1 members by overlap")
    args = parser.parse_args()

    start_date = parse_date_arg(args.start_date)
    end_date = parse_date_arg(args.end_date)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start-date cannot be later than end-date")

    download_classify_and_l1_members(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()
