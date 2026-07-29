from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

INIT_DOS_FILES = [
    "create_db.dos",
    "create_stock_daily.dos",
    "create_stock_daily_hfq.dos",
    "create_sw2021_classify_tables.dos",
    "create_vnpy_bar_db.dos",
    "create_vnpy_stock_daily_hfq.dos",
]


def connect_dolphindb_session():
    import dolphindb as ddb

    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", 8848))
    user = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", os.getenv("DOLPHINDB_PWD", "123456"))

    session = ddb.Session()
    session.connect(host, port, user, password)
    return session


def run_dos_script(session, script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"DOS script not found: {script_path}")

    script_content = script_path.read_text(encoding="utf-8")
    session.run(script_content)


def main() -> None:
    load_dotenv(ROOT_DIR / ".env")

    session = connect_dolphindb_session()
    try:
        total = len(INIT_DOS_FILES)
        for index, file_name in enumerate(INIT_DOS_FILES, start=1):
            script_path = SCRIPT_DIR / file_name
            print(f"[{index}/{total}] Running: {script_path.name}")
            run_dos_script(session, script_path)
        print("DolphinDB initialization completed.")
    finally:
        try:
            session.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
