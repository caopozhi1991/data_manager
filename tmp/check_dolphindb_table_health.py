#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import math
from dataclasses import dataclass
from typing import Dict, List, Sequence, TypedDict


DEFAULT_DB_PATH = "dfs://market_data"


@dataclass(frozen=True)
class TableConfig:
    table_name: str
    date_col: str
    dup_keys: Sequence[str]


class TableCheckResult(TypedDict):
    table: str
    date_col: str
    dup_keys: List[str]
    total_rows: int
    latest_date: str
    dup_groups: int
    dup_rows: int


DEFAULT_TABLE_CONFIGS: Dict[str, TableConfig] = {
    "stock_kline_daily": TableConfig(
        table_name="stock_kline_daily",
        date_col="trade_date",
        dup_keys=("code", "trade_date"),
    ),
    "sw2021_l1_members": TableConfig(
        table_name="sw2021_l1_members",
        date_col="in_date",
        dup_keys=("l1_code", "stock_code", "in_date"),
    ),
}


def _validate_identifier(name: str, kind: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid {kind}: {name}")
    return name


def _parse_csv_items(raw_values: Sequence[str]) -> List[str]:
    items: List[str] = []
    for raw in raw_values:
        for part in raw.split(","):
            item = part.strip()
            if not item:
                continue
            if item not in items:
                items.append(item)
    return items


def _parse_keys_override(raw_values: Sequence[str]) -> Dict[str, List[str]]:
    """
    Parse key overrides in format:
    table:key1,key2 table2:keyA,keyB
    """
    out: Dict[str, List[str]] = {}
    for raw in raw_values:
        val = raw.strip()
        if not val:
            continue
        if ":" not in val:
            raise ValueError(f"--keys item must be table:key1,key2, got: {val}")
        table_name, cols_raw = val.split(":", 1)
        table_name = table_name.strip()
        _validate_identifier(table_name, "table name")
        cols = _parse_csv_items([cols_raw])
        if not cols:
            raise ValueError(f"--keys has empty key list for table: {table_name}")
        out[table_name] = [_validate_identifier(c, "key column") for c in cols]
    return out


def _parse_date_override(raw_values: Sequence[str]) -> Dict[str, str]:
    """
    Parse date overrides in format:
    table:date_col table2:trade_date
    """
    out: Dict[str, str] = {}
    for raw in raw_values:
        val = raw.strip()
        if not val:
            continue
        if ":" not in val:
            raise ValueError(f"--date-col item must be table:date_col, got: {val}")
        table_name, col = val.split(":", 1)
        table_name = _validate_identifier(table_name.strip(), "table name")
        col = _validate_identifier(col.strip(), "date column")
        out[table_name] = col
    return out


def connect_dolphindb_session():
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    session = ddb.Session()
    session.connect(host, port, user, password)
    return session


def _table_exists(session, db_path: str, table_name: str) -> bool:
    session.upload({"dbPath": db_path, "tableName": table_name})
    return bool(session.run("existsTable(dbPath, tableName)"))


def _table_columns(session, db_path: str, table_name: str) -> List[str]:
    session.upload({"dbPath": db_path, "tableName": table_name})
    col_defs = session.run("schema(loadTable(dbPath, tableName)).colDefs")
    if col_defs is None or len(col_defs) == 0:
        return []
    return [str(x) for x in col_defs["name"].tolist()]


def _run_table_check(
    session,
    db_path: str,
    table_name: str,
    date_col: str,
    dup_keys: Sequence[str],
) -> TableCheckResult:
    _validate_identifier(table_name, "table name")
    _validate_identifier(date_col, "date column")
    dup_keys = [_validate_identifier(c, "key column") for c in dup_keys]

    key_expr = ",".join(dup_keys)
    session.upload({"dbPath": db_path, "tableName": table_name})

    total_rows = int(session.run("exec count(*) from loadTable(dbPath, tableName)"))

    max_date = session.run(f"exec max({date_col}) from loadTable(dbPath, tableName)")

    dup_df = session.run(
        f"""
        t = loadTable(dbPath, tableName)
        d = select count(*) as cnt from t group by {key_expr} having count(*) > 1
        select count(*) as dup_groups, sum(cnt) as dup_rows from d
        """
    )

    dup_groups = 0
    dup_rows = 0
    if dup_df is not None and len(dup_df) > 0:
        row = dup_df.iloc[0]
        dup_groups_raw = row.get("dup_groups", 0)
        dup_rows_raw = row.get("dup_rows", 0)
        dup_groups = _safe_int(dup_groups_raw)
        dup_rows = _safe_int(dup_rows_raw)

    return {
        "table": table_name,
        "date_col": date_col,
        "dup_keys": list(dup_keys),
        "total_rows": total_rows,
        "latest_date": "" if max_date is None else str(max_date),
        "dup_groups": dup_groups,
        "dup_rows": dup_rows,
    }


def _safe_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return 0 if math.isnan(value) else int(value)
    try:
        text = str(value).strip()
        if not text:
            return 0
        if text.lower() in {"nan", "none", "null"}:
            return 0
        return int(float(text))
    except Exception:
        return 0


def _build_table_configs(
    tables: Sequence[str],
    date_override: Dict[str, str],
    keys_override: Dict[str, List[str]],
) -> List[TableConfig]:
    out: List[TableConfig] = []
    for table_name in tables:
        _validate_identifier(table_name, "table name")
        base = DEFAULT_TABLE_CONFIGS.get(table_name)
        date_col = date_override.get(table_name, base.date_col if base else "trade_date")
        dup_keys = keys_override.get(table_name, list(base.dup_keys) if base else ["code", "trade_date"])
        out.append(TableConfig(table_name=table_name, date_col=date_col, dup_keys=tuple(dup_keys)))
    return out


def _print_report(db_path: str, rows: Sequence[TableCheckResult]) -> None:
    print(f"DolphinDB table health check | dbPath={db_path}")
    print("-" * 120)
    print(
        f"{'table':<28} {'date_col':<14} {'latest_date':<14} {'total_rows':>12} {'dup_groups':>12} {'dup_rows':>10} {'dup_keys'}"
    )
    print("-" * 120)
    for item in rows:
        keys_str = ",".join(item["dup_keys"])
        print(
            f"{item['table']:<28} {item['date_col']:<14} {item['latest_date']:<14} "
            f"{item['total_rows']:>12} {item['dup_groups']:>12} {item['dup_rows']:>10} {keys_str}"
        )
    print("-" * 120)


def run_check(
    db_path: str,
    table_names: Sequence[str],
    date_override: Dict[str, str],
    keys_override: Dict[str, List[str]],
) -> int:
    configs = _build_table_configs(table_names, date_override, keys_override)
    session = connect_dolphindb_session()

    report_rows: List[TableCheckResult] = []
    failed = False
    try:
        for cfg in configs:
            if not _table_exists(session, db_path, cfg.table_name):
                failed = True
                print(f"[ERROR] table not found: {cfg.table_name}")
                continue

            cols = _table_columns(session, db_path, cfg.table_name)
            missing = [c for c in [cfg.date_col, *cfg.dup_keys] if c not in cols]
            if missing:
                failed = True
                print(
                    f"[ERROR] column missing in {cfg.table_name}: {', '.join(missing)} | available={', '.join(cols)}"
                )
                continue

            result = _run_table_check(
                session=session,
                db_path=db_path,
                table_name=cfg.table_name,
                date_col=cfg.date_col,
                dup_keys=cfg.dup_keys,
            )
            report_rows.append(result)
    finally:
        try:
            session.close()
        except Exception:
            pass

    if report_rows:
        _print_report(db_path, report_rows)

    if failed:
        return 2

    has_dup = any(x["dup_groups"] > 0 for x in report_rows)
    return 1 if has_dup else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="检查 DolphinDB 多张表的最新日期和重复数据"
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help="数据库路径，默认 dfs://market_data",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["stock_kline_daily", "sw2021_l1_members"],
        help="需要检查的表名，支持空格分隔或逗号分隔",
    )
    parser.add_argument(
        "--date-col",
        nargs="*",
        default=[],
        help="覆盖日期列，格式 table:date_col，可传多个",
    )
    parser.add_argument(
        "--keys",
        nargs="*",
        default=[],
        help="覆盖重复主键，格式 table:key1,key2，可传多个",
    )

    args = parser.parse_args()

    table_names = _parse_csv_items(args.tables)
    if not table_names:
        raise ValueError("--tables 不能为空")

    date_override = _parse_date_override(args.date_col)
    keys_override = _parse_keys_override(args.keys)

    code = run_check(
        db_path=args.db_path,
        table_names=table_names,
        date_override=date_override,
        keys_override=keys_override,
    )

    if code == 0:
        print("[OK] 无重复数据")
    elif code == 1:
        print("[WARN] 检测到重复数据，请处理后重跑")
    else:
        print("[ERROR] 检查过程中存在缺失表或缺失字段")

    raise SystemExit(code)


if __name__ == "__main__":
    main()
