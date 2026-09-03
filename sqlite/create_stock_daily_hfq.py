from __future__ import annotations

from _common import (
    connect_sqlite,
    ensure_stock_daily_hfq_table,
    get_db_path,
    STOCK_DAILY_HFQ_TABLE,
)


def main() -> None:
    connection = connect_sqlite()
    try:
        ensure_stock_daily_hfq_table(connection)
        connection.commit()
    finally:
        connection.close()

    print(f"SQLite table ready: {get_db_path()}::{STOCK_DAILY_HFQ_TABLE}")


if __name__ == "__main__":
    main()
