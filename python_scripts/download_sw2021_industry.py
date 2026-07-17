import os
import time
from typing import Dict, List

import pandas as pd
import tushare as ts


# 建议改为读取环境变量: os.getenv("TUSHARE_TOKEN")
TUSHARE_TOKEN = "8d433391c4fddb4c08789e005f278794c23e76ea557a0c5096d39423"
SRC = "SW2021"
LEVELS: List[str] = ["L1", "L2", "L3"]
OUTPUT_ROOT = "tushare_data/sw2021"


def ensure_dirs() -> Dict[str, str]:
    classify_dir = os.path.join(OUTPUT_ROOT, "classify")
    member_dir = os.path.join(OUTPUT_ROOT, "members_l1")
    os.makedirs(classify_dir, exist_ok=True)
    os.makedirs(member_dir, exist_ok=True)
    return {"classify": classify_dir, "member": member_dir}


def download_classify(pro, classify_dir: str) -> pd.DataFrame:
    l1_df = pd.DataFrame()
    for level in LEVELS:
        df = pro.index_classify(level=level, src=SRC)
        save_path = os.path.join(classify_dir, f"index_classify_{SRC}_{level}.csv")
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"[{level}] {len(df)} rows -> {save_path}")
        if level == "L1":
            l1_df = df
    return l1_df


def download_l1_members(pro, l1_df: pd.DataFrame, member_dir: str) -> None:
    all_members = []

    if l1_df.empty or "index_code" not in l1_df.columns:
        raise ValueError("L1 数据为空或缺少 index_code 列，无法下载成分股。")

    for i, (_, row) in enumerate(l1_df.iterrows()):
        l1_code = row["index_code"]
        l1_name = row.get("industry_name", "unknown")

        # 第一次：默认参数拉取
        df_member_default = pro.index_member_all(l1_code=l1_code)
        # 第二次：显式拉取 is_new='N'
        df_member_n = pro.index_member_all(l1_code=l1_code, is_new="N")

        # 合并并去重
        df_member = pd.concat([df_member_default, df_member_n], ignore_index=True)
        if not df_member.empty:
            df_member = df_member.drop_duplicates(keep="last").reset_index(drop=True)

        safe_name = str(l1_name).replace("/", "_").replace(" ", "_")
        per_path = os.path.join(member_dir, f"l1_members_{l1_code}_{safe_name}.csv")
        df_member.to_csv(per_path, index=False, encoding="utf-8-sig")
        print(
            f"[{i + 1}/{len(l1_df)}] {l1_code} {l1_name}: "
            f"default={len(df_member_default)}, is_new='N'={len(df_member_n)}, merged={len(df_member)} -> {per_path}"
        )

        if not df_member.empty:
            # 某些返回结果已包含 l1_code/l1_name，使用赋值避免 insert 重复列报错
            df_member["l1_code"] = l1_code
            df_member["l1_name"] = l1_name

            # 将 l1_code/l1_name 放到前两列，其他列保持原顺序
            front_cols = ["l1_code", "l1_name"]
            other_cols = [c for c in df_member.columns if c not in front_cols]
            df_member = df_member[front_cols + other_cols]
            all_members.append(df_member)

        # 避免请求过快触发限频
        time.sleep(0.2)

    merged = pd.concat(all_members, ignore_index=True) if all_members else pd.DataFrame()
    merged_path = os.path.join(member_dir, "all_l1_members_merged.csv")
    merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
    print(f"[ALL L1] {len(merged)} rows -> {merged_path}")


def main() -> None:
    paths = ensure_dirs()

    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()

    l1_df = download_classify(pro, paths["classify"])
    download_l1_members(pro, l1_df, paths["member"])


if __name__ == "__main__":
    main()