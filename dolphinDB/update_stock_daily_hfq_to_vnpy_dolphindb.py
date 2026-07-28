from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONDA_ENV = os.getenv("DEFAULT_RUN_CONDA_ENV", "agent")
REEXEC_COUNT_ENV = "_AUTO_CONDA_REEXEC_COUNT"


def ensure_default_conda_env(expected_env: str) -> None:
    if not expected_env:
        return

    current_env = os.getenv("CONDA_DEFAULT_ENV", "")
    if current_env == expected_env:
        return

    reexec_count_raw = os.getenv(REEXEC_COUNT_ENV, "0")
    try:
        reexec_count = int(reexec_count_raw)
    except ValueError:
        reexec_count = 0

    if reexec_count >= 1:
        print(
            "Warning: already tried auto switch once but still not in target env. "
            f"current={current_env or 'unknown'}, expected={expected_env}."
        )
        return

    conda_exe = shutil.which("conda")
    if not conda_exe:
        print(
            "Warning: conda command not found, running in current environment "
            f"({current_env or 'unknown'})."
        )
        return

    cmd = [conda_exe, "run", "-n", expected_env, "python", str(Path(__file__).resolve()), *sys.argv[1:]]
    env = os.environ.copy()
    env[REEXEC_COUNT_ENV] = str(reexec_count + 1)

    print(f"Re-running in conda env: {expected_env}")
    result = subprocess.run(cmd, env=env)
    raise SystemExit(result.returncode)


def parse_date_arg(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def validate_identifier(name: str, field_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid {field_name}: {name}")
    return name


def connect_dolphindb_session():
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    session = ddb.Session()
    session.connect(host, port, user, password)
    return session


def ensure_database_exists(session, db_path: str) -> None:
    session.upload({"dbPath": db_path})
    exists = bool(session.run("existsDatabase(dbPath)"))
    if not exists:
        raise RuntimeError(f"DolphinDB database not found: {db_path}")


def ensure_table_exists(session, db_path: str, table_name: str) -> None:
    ensure_database_exists(session, db_path)
    session.upload({"dbPath": db_path, "tableName": table_name})
    exists = bool(session.run("existsTable(dbPath, tableName)"))
    if not exists:
        raise RuntimeError(f"DolphinDB table not found: {db_path}/{table_name}")


def get_table_max_date(session, db_path: str, table_name: str, column_name: str = "trade_date") -> date | None:
    safe_column = validate_identifier(column_name, "column name")
    session.upload({"dbPath": db_path, "tableName": table_name})
    value = session.run(f"exec max({safe_column}) from loadTable(dbPath, tableName)")
    if value is None:
        return None

    text = str(value)
    if not text or text.lower() == "nan":
        return None
    return parse_date_arg(text[:10])


def get_table_min_date(session, db_path: str, table_name: str, column_name: str = "trade_date") -> date | None:
    safe_column = validate_identifier(column_name, "column name")
    session.upload({"dbPath": db_path, "tableName": table_name})
    value = session.run(f"exec min({safe_column}) from loadTable(dbPath, tableName)")
    if value is None:
        return None

    text = str(value)
    if not text or text.lower() == "nan":
        return None
    return parse_date_arg(text[:10])


def resolve_update_range(
    start_date_raw: str | None,
    end_date_raw: str | None,
    src_min_date: date | None,
    src_max_date: date | None,
    dst_max_date: date | None,
    lookback_days: int = 30,
) -> tuple[date, date]:
    """
    默认增量模式下，回溯 lookback_days 天，确保源表中近期因子/除权修正仍能覆盖到目标表。
    如果需要全量重算，请显式传入 --start-date/--end-date。
    """
    if src_min_date is None or src_max_date is None:
        raise RuntimeError("Source table has no data")

    today = date.today()

    if start_date_raw and end_date_raw:
        start_date = parse_date_arg(start_date_raw)
        end_date = parse_date_arg(end_date_raw)
    elif start_date_raw or end_date_raw:
        raise ValueError("Please provide both --start-date and --end-date, or provide neither")
    else:
        if dst_max_date is None:
            start_date = src_min_date
        else:
            start_date = max(src_min_date, dst_max_date - timedelta(days=lookback_days))
        end_date = today

    if end_date > today:
        end_date = today
    if end_date > src_max_date:
        end_date = src_max_date
    if start_date < src_min_date:
        start_date = src_min_date

    return start_date, end_date


def rebuild_vnpy_hfq_range(
    session,
    src_db_path: str,
    src_table: str,
    dst_db_path: str,
    dst_table: str,
    start_date: date,
    end_date: date,
    batch_days: int,
) -> tuple[int, int]:
    session.upload(
        {
            "srcDbPath": src_db_path,
            "srcTable": src_table,
            "dstDbPath": dst_db_path,
            "dstTable": dst_table,
            "startDate": start_date,
            "endDate": end_date,
        }
    )

    deleted = session.run(
        """
        dst = loadTable(dstDbPath, dstTable)
        oldCnt = exec count(*) from dst where datetime >= startDate and datetime <= endDate
        delete from dst where datetime >= startDate and datetime <= endDate
        oldCnt
        """
    )

    inserted_total = 0
    cursor = start_date
    batch_days = max(1, int(batch_days))

    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=batch_days - 1))
        session.upload({"windowStart": cursor, "windowEnd": window_end})
        inserted = session.run(
            """
            src = loadTable(srcDbPath, srcTable)
            rows = select
                iif(strlen(code) >= 6, substr(code, 0, 6), code) as symbol,
                iif(upper(code) like "%.SH", "SSE",
                    iif(upper(code) like "%.SZ", "SZSE",
                        iif(upper(code) like "%.BJ", "BSE",
                            iif(code like "6%", "SSE", iif(code like "0%" or code like "3%", "SZSE", "UNKNOWN"))
                        )
                    )
                ) as exchange,
                trade_date as datetime,
                "d" as interval,
                volume,
                0.0 as turnover,
                0.0 as open_interest,
                open as open_price,
                high as high_price,
                low as low_price,
                close as close_price
            from src
            where trade_date >= windowStart and trade_date <= windowEnd

            rowCnt = exec count(*) from rows
            if(rowCnt > 0){
                append!(loadTable(dstDbPath, dstTable), rows)
            }
            rowCnt
            """
        )
        inserted_count = int(inserted) if inserted is not None else 0
        inserted_total += inserted_count
        print(f"VNpy HFQ batch {cursor} -> {window_end}: inserted={inserted_count}, total={inserted_total}")
        cursor = window_end + timedelta(days=1)

    deleted_count = int(deleted) if deleted is not None else 0
    return deleted_count, inserted_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally sync stock_kline_daily_hfq to vnpy_stock_daily_hfq")
    parser.add_argument("--src-db-path", default="dfs://market_data", help="Source database path")
    parser.add_argument("--src-table", default="stock_kline_daily_hfq", help="Source table")
    parser.add_argument("--dst-db-path", default="dfs://vnpy_bar_db", help="Target database path")
    parser.add_argument("--dst-table", default="vnpy_stock_daily_hfq", help="Target table")
    parser.add_argument("--start-date", default=None, help="Start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date, format YYYY-MM-DD")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help=(
            "In default incremental mode, re-compute this many days before the target table's latest date "
            "to catch recent retroactive corrections from the source data. "
            "Set to 0 to only append strictly new dates. "
            "Pass --start-date/--end-date explicitly to override the range entirely."
        ),
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=20,
        help="Append data in date windows of N days to avoid DFS append memory limits",
    )
    return parser.parse_args()


def main() -> None:
    ensure_default_conda_env(DEFAULT_CONDA_ENV)
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    session = connect_dolphindb_session()
    try:
        ensure_table_exists(session, args.src_db_path, args.src_table)
        ensure_table_exists(session, args.dst_db_path, args.dst_table)

        src_min_date = get_table_min_date(session, args.src_db_path, args.src_table, "trade_date")
        src_max_date = get_table_max_date(session, args.src_db_path, args.src_table, "trade_date")
        dst_max_date = get_table_max_date(session, args.dst_db_path, args.dst_table, "datetime")

        start_date, end_date = resolve_update_range(
            start_date_raw=args.start_date,
            end_date_raw=args.end_date,
            src_min_date=src_min_date,
            src_max_date=src_max_date,
            dst_max_date=dst_max_date,
            lookback_days=args.lookback_days,
        )

        if start_date > end_date:
            print(
                "No range to update. "
                f"resolved start={start_date}, end={end_date}, src_max={src_max_date}, today={date.today()}"
            )
            return

        print(
            "Syncing VNpy HFQ range: "
            f"{start_date} -> {end_date} | src={args.src_db_path}/{args.src_table} "
            f"dst={args.dst_db_path}/{args.dst_table}"
        )

        deleted_count, inserted_count = rebuild_vnpy_hfq_range(
            session=session,
            src_db_path=args.src_db_path,
            src_table=args.src_table,
            dst_db_path=args.dst_db_path,
            dst_table=args.dst_table,
            start_date=start_date,
            end_date=end_date,
            batch_days=args.batch_days,
        )

        print(
            "Done. "
            f"deleted={deleted_count}, inserted={inserted_count}, range={start_date}->{end_date}"
        )
    finally:
        try:
            session.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
