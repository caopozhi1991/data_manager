from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SQLITE_DIR = Path(__file__).resolve().parent

INIT_SCRIPTS = [
    "create_db.py",
    "create_stock_daily.py",
    "create_stock_daily_hfq.py",
    "create_sw2021_classify_tables.py",
    "create_sw2021_l1_members.py",
    "create_vnpy_bar_db.py",
    "create_vnpy_stock_daily_hfq.py",
]


def resolve_db_path(raw_path: str | None) -> str | None:
    if raw_path is None or raw_path == "":
        return None

    path = Path(raw_path)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return str(path)


def run_script(script_name: str, db_path: str | None, vnpy_db_path: str | None) -> None:
    script_path = SQLITE_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"SQLite script not found: {script_path}")

    env = os.environ.copy()
    env["QUANT_DATA_ENGINE"] = "sqlite"
    if db_path is not None:
        env["SQLITE_DB_PATH"] = db_path
    if vnpy_db_path is not None:
        env["SQLITE_VNPY_BAR_DB_PATH"] = vnpy_db_path

    print(
        f"[{script_name}] running with db={db_path or os.getenv('SQLITE_DB_PATH', 'default')} "
        f"vnpy_db={vnpy_db_path or os.getenv('SQLITE_VNPY_BAR_DB_PATH', 'default')}"
    )
    subprocess.run([sys.executable, str(script_path)], cwd=str(ROOT_DIR), env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize SQLite database schema for the project.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("SQLITE_DB_PATH", str(ROOT_DIR / "sqlite" / "market_data.db")),
        help="SQLite database file for the main market_data database. Different DB paths represent different logical databases.",
    )
    parser.add_argument(
        "--vnpy-db-path",
        default=os.getenv("SQLITE_VNPY_BAR_DB_PATH", str(ROOT_DIR / "sqlite" / "vnpy_bar_db.db")),
        help="SQLite database file for vnpy_bar_db. This keeps the VN.py DB separate from the main market_data DB.",
    )
    args = parser.parse_args()

    db_path = resolve_db_path(args.db_path)
    vnpy_db_path = resolve_db_path(args.vnpy_db_path)
    total = len(INIT_SCRIPTS)
    for index, script_name in enumerate(INIT_SCRIPTS, start=1):
        print(f"[{index}/{total}] Running: {script_name}")
        run_script(script_name, db_path, vnpy_db_path)

    print("SQLite initialization completed.")


if __name__ == "__main__":
    main()
