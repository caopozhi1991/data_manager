from __future__ import annotations

from _common import connect_sqlite, get_db_path


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vnpy_stock_daily_hfq (
    symbol TEXT NOT NULL,
    exchange TEXT,
    datetime TEXT NOT NULL,
    interval TEXT,
    volume REAL,
    turnover REAL,
    open_interest REAL,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    close_price REAL,
    PRIMARY KEY (symbol, datetime)
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_vnpy_stock_daily_hfq_datetime
ON vnpy_stock_daily_hfq (datetime)
"""


def main() -> None:
    connection = connect_sqlite("vnpy_bar_db")
    try:
        connection.execute(CREATE_TABLE_SQL)
        connection.execute(CREATE_INDEX_SQL)
        connection.commit()
    finally:
        connection.close()

    print(f"SQLite table ready: {get_db_path('vnpy_bar_db')}::vnpy_stock_daily_hfq")


if __name__ == "__main__":
    main()
