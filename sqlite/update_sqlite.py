from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DOWNLOADER_DIR = ROOT_DIR / "downloader"
SQLITE_DIR = Path(__file__).resolve().parent
DEFAULT_CONDA_ENV = os.getenv("DEFAULT_RUN_CONDA_ENV", "agent")
REEXEC_COUNT_ENV = "_AUTO_CONDA_REEXEC_COUNT"


def ensure_default_conda_env(expected_env: str) -> None:
    if not expected_env:
        return

    try:
        import tickflow  # noqa: F401
        return
    except ImportError:
        pass

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


def resolve_db_path(raw_path: str | None) -> str | None:
    if raw_path is None or raw_path == "":
        return None

    path = Path(raw_path)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return str(path)


def run_script(script_path: Path, extra_args: list[str], db_path: str | None, vnpy_db_path: str | None = None) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    env = os.environ.copy()
    env["QUANT_DATA_ENGINE"] = "sqlite"
    if db_path is not None:
        env["SQLITE_DB_PATH"] = db_path
    if vnpy_db_path is not None:
        env["SQLITE_VNPY_BAR_DB_PATH"] = vnpy_db_path

    cmd = [sys.executable, str(script_path), *extra_args]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(ROOT_DIR), env=env, check=True)


def add_date_args(parser: argparse.ArgumentParser) -> None:
    default_start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    default_end_date = date.today().strftime("%Y-%m-%d")
    parser.add_argument("--start-date", default=default_start_date, help="Optional YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end_date, help="Optional YYYY-MM-DD")


def main() -> None:
    ensure_default_conda_env(DEFAULT_CONDA_ENV)

    parser = argparse.ArgumentParser(description="Download data and load it into SQLite.")
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
    parser.add_argument("--symbols", default="", help="Comma-separated stock symbols for stock daily download")
    parser.add_argument(
        "--target",
        default="all",
        choices=["all", "stock", "sw2021_classify", "sw2021_l1_members"],
        help="Target dataset to insert into SQLite",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview rows without writing to SQLite")
    add_date_args(parser)
    args = parser.parse_args()

    db_path = resolve_db_path(args.db_path)
    vnpy_db_path = resolve_db_path(args.vnpy_db_path)
    stock_script = DOWNLOADER_DIR / "download_stock_daily.py"
    sw_script = DOWNLOADER_DIR / "download_sw2021.py"
    insert_script = DOWNLOADER_DIR / "insert_downloaded_data.py"
    hfq_script = SQLITE_DIR / "update_stock_daily_hfq.py"
    vnpy_script = SQLITE_DIR / "update_vnpy_stock_daily_hfq.py"

    stock_args: list[str] = []
    if args.start_date:
        stock_args.extend(["--start-date", args.start_date])
    if args.end_date:
        stock_args.extend(["--end-date", args.end_date])
    if args.symbols:
        stock_args.extend(["--symbols", args.symbols])

    sw_args: list[str] = []
    if args.start_date:
        sw_args.extend(["--start-date", args.start_date])
    if args.end_date:
        sw_args.extend(["--end-date", args.end_date])

    insert_args: list[str] = ["--target", args.target]
    if args.start_date:
        insert_args.extend(["--start-date", args.start_date])
    if args.end_date:
        insert_args.extend(["--end-date", args.end_date])
    if args.dry_run:
        insert_args.append("--dry-run")

    hfq_args: list[str] = []
    if args.start_date:
        hfq_args.extend(["--start-date", args.start_date])
    if args.end_date:
        hfq_args.extend(["--end-date", args.end_date])

    vnpy_args: list[str] = []
    if args.start_date:
        vnpy_args.extend(["--start-date", args.start_date])
    if args.end_date:
        vnpy_args.extend(["--end-date", args.end_date])

    total_steps = 5
    for index, (script_path, extra_args) in enumerate(
        [
            (stock_script, stock_args),
            (sw_script, sw_args),
            (insert_script, insert_args),
            (hfq_script, hfq_args),
            (vnpy_script, vnpy_args),
        ],
        start=1,
    ):
        print(f"[{index}/{total_steps}] Running: {script_path.name}")
        run_script(script_path, extra_args, db_path, vnpy_db_path)

    print("SQLite update pipeline completed.")


if __name__ == "__main__":
    main()
