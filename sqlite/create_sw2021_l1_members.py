from __future__ import annotations

from _common import connect_sqlite, get_db_path


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sw2021_l1_members (
    l1_code TEXT,
    l1_name TEXT,
    l2_code TEXT,
    l2_name TEXT,
    l3_code TEXT,
    l3_name TEXT,
    stock_code TEXT,
    ts_code TEXT,
    name TEXT,
    in_date TEXT,
    out_date TEXT,
    is_new TEXT,
    PRIMARY KEY (l1_code, stock_code, in_date)
)
"""


def main() -> None:
    connection = connect_sqlite()
    try:
        connection.execute(CREATE_TABLE_SQL)
        connection.commit()
    finally:
        connection.close()

    print(f"SQLite table ready: {get_db_path()}::sw2021_l1_members")


if __name__ == "__main__":
    main()
