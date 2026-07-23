from __future__ import annotations

from _common import connect_sqlite, get_db_path


TABLE_NAMES = (
    "sw2021_index_classify_l1",
    "sw2021_index_classify_l2",
    "sw2021_index_classify_l3",
)

CREATE_TABLE_TEMPLATE = """
CREATE TABLE {table_name} (
    index_code TEXT,
    industry_name TEXT,
    level TEXT,
    industry_code INTEGER,
    is_pub INTEGER,
    parent_code INTEGER,
    src TEXT
)
"""


def main() -> None:
    connection = connect_sqlite()
    try:
        for table_name in TABLE_NAMES:
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(CREATE_TABLE_TEMPLATE.format(table_name=table_name))
        connection.commit()
    finally:
        connection.close()

    print(
        "SQLite tables recreated: "
        f"{get_db_path()}::{', '.join(TABLE_NAMES)}"
    )


if __name__ == "__main__":
    main()