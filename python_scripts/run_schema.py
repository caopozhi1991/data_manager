from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DOLPHINDB_DIR = ROOT_DIR / "dolphinDB"
SQLITE_DIR = ROOT_DIR / "sqlite"
SUPPORTED_SCRIPTS = (
    "create_db",
    "create_stock_daily",
    "create_sw2021_classify_tables",
    "create_sw2021_l1_members",
)


def normalize_engine(raw_engine: str | None) -> str:
    engine = (raw_engine or "dolphinDB").strip().lower()
    if engine == "dolphindb":
        return engine
    if engine == "sqlite":
        return engine
    raise ValueError(f"Unsupported QUANT_DATA_ENGINE: {raw_engine}")


def normalize_script_name(name: str) -> str:
    candidate = Path(name).stem
    if candidate not in SUPPORTED_SCRIPTS:
        raise ValueError(
            f"Unsupported script: {name}. Supported scripts: {', '.join(SUPPORTED_SCRIPTS)}"
        )
    return candidate


def run_dolphindb_script(script_name: str) -> None:
    import dolphindb as ddb

    script_path = DOLPHINDB_DIR / f"{script_name}.dos"
    if not script_path.exists():
        raise FileNotFoundError(f"DolphinDB script not found: {script_path}")

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    session = ddb.Session()
    session.connect(host, port, user, password)
    try:
        source = script_path.read_text(encoding="utf-8")
        filtered_source = "\n".join(
            line for line in source.splitlines() if not line.strip().startswith("login(")
        )
        session.run(filtered_source)
        print(f"Executed DolphinDB script: {script_path}")
    finally:
        try:
            session.close()
        except Exception:
            pass


def run_sqlite_script(script_name: str) -> None:
    script_path = SQLITE_DIR / f"{script_name}.py"
    if not script_path.exists():
        raise FileNotFoundError(f"SQLite script not found: {script_path}")

    subprocess.run([sys.executable, str(script_path)], cwd=str(ROOT_DIR), check=True)


def iter_target_scripts(script_names: Iterable[str], run_all: bool) -> list[str]:
    if run_all or not script_names:
        return list(SUPPORTED_SCRIPTS)
    return [normalize_script_name(name) for name in script_names]


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="Run schema creation scripts for the configured data engine."
    )
    parser.add_argument("scripts", nargs="*", help="Script basenames, such as create_db")
    parser.add_argument("--all", action="store_true", help="Run all supported scripts")
    args = parser.parse_args()

    engine = normalize_engine(os.getenv("QUANT_DATA_ENGINE"))
    targets = iter_target_scripts(args.scripts, args.all)

    for script_name in targets:
        print(f"[{engine}] running {script_name}")
        if engine == "dolphindb":
            run_dolphindb_script(script_name)
        else:
            run_sqlite_script(script_name)


if __name__ == "__main__":
    main()