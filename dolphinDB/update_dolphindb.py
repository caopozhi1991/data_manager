from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent

UPDATE_SCRIPTS = [
    "download_stock_daily_to_dolphindb.py",
    "update_stock_daily_hfq_to_dolphindb.py",
    "update_stock_daily_hfq_to_vnpy_dolphindb.py",
    "update_sw2021_to_dolphindb.py",
]


def run_script(script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    cmd = [sys.executable, str(script_path)]
    subprocess.run(cmd, check=True)


def main() -> None:
    total = len(UPDATE_SCRIPTS)
    for index, script_name in enumerate(UPDATE_SCRIPTS, start=1):
        script_path = SCRIPT_DIR / script_name
        print(f"[{index}/{total}] Running: {script_path.name}")
        run_script(script_path)

    print("DolphinDB update pipeline completed.")


if __name__ == "__main__":
    main()
