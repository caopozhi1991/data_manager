from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
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


def run_script(script_path: Path, db_path: str | None, vnpy_db_path: str | None = None) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    env = os.environ.copy()
    env["QUANT_DATA_ENGINE"] = "sqlite"
    if db_path is not None:
        env["SQLITE_DB_PATH"] = db_path
    if vnpy_db_path is not None:
        env["SQLITE_VNPY_BAR_DB_PATH"] = vnpy_db_path

    cmd = [sys.executable, str(script_path)]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(ROOT_DIR), env=env, check=True)


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    ensure_default_conda_env(DEFAULT_CONDA_ENV)
    db_path = resolve_db_path(os.getenv("SQLITE_DB_PATH", str(ROOT_DIR / "sqlite" / "market_data.db")))
    vnpy_db_path = resolve_db_path(os.getenv("SQLITE_VNPY_BAR_DB_PATH", str(ROOT_DIR / "sqlite" / "vnpy_bar_db.db")))
    stock_script = SQLITE_DIR / "download_stock_daily_to_sqlite.py"
    hfq_script = SQLITE_DIR / "update_stock_daily_hfq_to_sqlite.py"
    vnpy_script = SQLITE_DIR / "update_stock_daily_hfq_to_vnpy_sqlite.py"

    steps: list[Path] = [
        stock_script,
        hfq_script,
        vnpy_script,
    ]

    total_steps = len(steps)
    for index, script_path in enumerate(steps, start=1):
        print(f"[{index}/{total_steps}] Running: {script_path.name}")
        run_script(script_path, db_path, vnpy_db_path)

    print("SQLite update pipeline completed.")


if __name__ == "__main__":
    main()
