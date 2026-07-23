from __future__ import annotations

import sqlite3

from _common import get_db_path


def main() -> None:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(db_path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.commit()
    finally:
        connection.close()

    print(f"SQLite database recreated: {db_path}")


if __name__ == "__main__":
    main()