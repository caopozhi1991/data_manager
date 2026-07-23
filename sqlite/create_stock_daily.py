from __future__ import annotations

from _common import connect_sqlite, get_db_path


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_kline_daily (
    code TEXT NOT NULL,
    name TEXT,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    amount REAL,
    adjust_factor REAL,
    PRIMARY KEY (code, trade_date)
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_kline_daily_trade_date
ON stock_kline_daily (trade_date)
"""


def main() -> None:
    connection = connect_sqlite()
    try:
        connection.execute(CREATE_TABLE_SQL)
        connection.execute(CREATE_INDEX_SQL)
        connection.commit()
    finally:
        connection.close()

    print(f"SQLite table ready: {get_db_path()}::stock_kline_daily")


if __name__ == "__main__":
    main()