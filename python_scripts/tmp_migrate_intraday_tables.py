"""
临时迁移脚本：
1. 将 dfs://ohlcv_daily 下的分钟表迁移到 dfs://ohlcv_minute 同名表
2. 校验迁移行数后删除 dfs://ohlcv_daily 下的分钟表

默认 dry-run（仅检查与预演）。
执行实际迁移请加参数: --execute
"""

import argparse
from typing import Dict, List, Tuple

from insert_stock_daily import connect_dolphindb_session

SRC_DB_PATH = "dfs://ohlcv_daily"
DST_DB_PATH = "dfs://ohlcv_minute"
TABLES = [
    "stock_kline_1m",
    "stock_kline_5m",
    "stock_kline_15m",
    "stock_kline_30m",
    "stock_kline_60m",
]


def _parse_tables_arg(table_args: List[str]) -> List[str]:
    if not table_args:
        return TABLES

    selected: List[str] = []
    for raw in table_args:
        for item in raw.split(","):
            table_name = item.strip()
            if not table_name:
                continue
            if table_name not in TABLES:
                raise ValueError(f"不支持的表名: {table_name}，可选值: {', '.join(TABLES)}")
            if table_name not in selected:
                selected.append(table_name)

    if not selected:
        raise ValueError("--tables 不能为空")

    return selected


def _ensure_target_db(session, src_db_path: str, dst_db_path: str) -> None:
    """确保目标库存在，分区方案与源库一致。"""
    session.upload({"srcDbPath": src_db_path, "dstDbPath": dst_db_path})
    session.run(
        """
        if(not existsDatabase(srcDbPath)){
            throw("源数据库不存在: " + srcDbPath)
        }
        if(not existsDatabase(dstDbPath)){
            partSchema = schema(database(srcDbPath)).partitionSchema
            database(dstDbPath, VALUE, partSchema)
            print("已创建目标数据库: " + dstDbPath)
        }
        """
    )


def _ensure_target_table(session, src_db_path: str, dst_db_path: str, table_name: str) -> None:
    """确保目标分钟表存在，不存在则按源表结构创建。"""
    session.upload({"srcDbPath": src_db_path, "dstDbPath": dst_db_path, "tableName": table_name})
    session.run(
        """
        if(not existsTable(srcDbPath, tableName)){
            throw("源表不存在: " + tableName)
        }
        if(existsTable(dstDbPath, tableName)){
            srcDefs = schema(loadTable(srcDbPath, tableName)).colDefs
            dstDefs = schema(loadTable(dstDbPath, tableName)).colDefs
            sameSchema = true
            if(size(srcDefs.name) != size(dstDefs.name)){
                sameSchema = false
            }else{
                for(i in 0:(size(srcDefs.name)-1)){
                    if(srcDefs.name[i] != dstDefs.name[i] || srcDefs.typeInt[i] != dstDefs.typeInt[i]){
                        sameSchema = false
                        break
                    }
                }
            }

            if(!sameSchema){
                dst = loadTable(dstDbPath, tableName)
                dstRows = exec count(*) from dst
                if(dstRows > 0){
                    throw("目标表结构不一致且非空，拒绝覆盖: " + tableName)
                }
                dstDb = database(dstDbPath)
                dropTable(dstDb, tableName)
                print("目标表结构不一致且为空，已删除重建: " + tableName)
            }
        }

        if(not existsTable(dstDbPath, tableName)){
            src = loadTable(srcDbPath, tableName)
            sch = select top 0 * from src
            db = database(dstDbPath)
            db.createPartitionedTable(sch, tableName, `trade_date)
            print("已创建目标表: " + tableName)
        }
        """
    )


def _count_rows(session, db_path: str, table_name: str) -> int:
    session.upload({"dbPath": db_path, "tableName": table_name})
    exists = session.run("existsTable(dbPath, tableName)")
    if not exists:
        return 0
    cnt = session.run("exec count(*) from loadTable(dbPath, tableName)")
    return int(cnt)


def _get_trade_date_counts(session, db_path: str, table_name: str) -> List[Tuple[object, int]]:
    session.upload({"dbPath": db_path, "tableName": table_name})
    result = session.run(
        "select trade_date, count(*) as cnt from loadTable(dbPath, tableName) group by trade_date order by trade_date"
    )
    if result is None or len(result) == 0:
        return []
    return [(row.trade_date, int(row.cnt)) for row in result.itertuples(index=False)]


def _insert_trade_date_rows(session, src_db_path: str, dst_db_path: str, table_name: str, trade_date: object) -> int:
    session.upload(
        {
            "srcDbPath": src_db_path,
            "dstDbPath": dst_db_path,
            "tableName": table_name,
            "tradeDate": trade_date,
        }
    )
    inserted = session.run(
        """
        src = loadTable(srcDbPath, tableName)
        dst = loadTable(dstDbPath, tableName)
        batchRows = select * from src where trade_date = tradeDate
        dst.tableInsert(batchRows)
        """
    )
    return int(inserted)


def _sync_rows_by_trade_date(session, src_db_path: str, dst_db_path: str, table_name: str) -> int:
    src_counts = _get_trade_date_counts(session, src_db_path, table_name)
    dst_counts: Dict[object, int] = dict(_get_trade_date_counts(session, dst_db_path, table_name))

    total_inserted = 0
    for trade_date, src_count in src_counts:
        dst_count = dst_counts.get(trade_date, 0)
        if dst_count == src_count:
            continue
        if dst_count != 0:
            raise RuntimeError(
                f"目标表已存在部分数据，请先清理后重试: {trade_date}, src={src_count}, dst={dst_count}"
            )

        inserted = _insert_trade_date_rows(session, src_db_path, dst_db_path, table_name, trade_date)
        if inserted != src_count:
            raise RuntimeError(
                f"按交易日迁移后行数不一致: {trade_date}, expected={src_count}, actual={inserted}"
            )

        total_inserted += inserted

    return total_inserted


def _find_trade_date_mismatches(session, src_db_path: str, dst_db_path: str, table_name: str) -> List[str]:
    src_counts = dict(_get_trade_date_counts(session, src_db_path, table_name))
    dst_counts = dict(_get_trade_date_counts(session, dst_db_path, table_name))

    mismatches: List[str] = []
    all_dates = sorted(set(src_counts) | set(dst_counts), key=str)
    for trade_date in all_dates:
        src_count = src_counts.get(trade_date, 0)
        dst_count = dst_counts.get(trade_date, 0)
        if src_count != dst_count:
            mismatches.append(f"{trade_date}: src={src_count}, dst={dst_count}")
    return mismatches


def _drop_source_table(session, src_db_path: str, table_name: str) -> None:
    session.upload({"srcDbPath": src_db_path, "tableName": table_name})
    session.run("srcDb = database(srcDbPath); dropTable(srcDb, tableName)")


def migrate_tables(execute: bool, tables: List[str]) -> None:
    session = connect_dolphindb_session()
    try:
        _ensure_target_db(session, SRC_DB_PATH, DST_DB_PATH)

        print(f"模式: {'EXECUTE' if execute else 'DRY-RUN'}")
        print(f"源库: {SRC_DB_PATH}")
        print(f"目标库: {DST_DB_PATH}")

        for table_name in tables:
            print("=" * 80)
            print(f"处理表: {table_name}")

            src_exists = _count_rows(session, SRC_DB_PATH, table_name) > 0 or session.run(
                f"existsTable('{SRC_DB_PATH}', '{table_name}')"
            )
            if not src_exists:
                print("源表不存在，跳过")
                continue

            _ensure_target_table(session, SRC_DB_PATH, DST_DB_PATH, table_name)

            src_before = _count_rows(session, SRC_DB_PATH, table_name)
            dst_before = _count_rows(session, DST_DB_PATH, table_name)
            print(f"迁移前: 源={src_before}, 目标={dst_before}")

            if not execute:
                print("DRY-RUN: 不执行写入/删除")
                continue

            inserted = _sync_rows_by_trade_date(session, SRC_DB_PATH, DST_DB_PATH, table_name)
            src_after_insert = _count_rows(session, SRC_DB_PATH, table_name)
            dst_after_insert = _count_rows(session, DST_DB_PATH, table_name)
            print(
                f"写入结果: tableInsert返回={inserted}, "
                f"写入后 源={src_after_insert}, 目标={dst_after_insert}"
            )

            expected_dst_after = src_after_insert
            if dst_after_insert != expected_dst_after:
                raise RuntimeError(
                    f"校验失败: {table_name} 目标行数不符合预期, "
                    f"期望={expected_dst_after}, 实际={dst_after_insert}"
                )

            mismatches = _find_trade_date_mismatches(session, SRC_DB_PATH, DST_DB_PATH, table_name)
            if mismatches:
                raise RuntimeError(
                    f"校验失败: {table_name} 存在交易日级别差异: {'; '.join(mismatches[:5])}"
                )

            _drop_source_table(session, SRC_DB_PATH, table_name)
            source_exists_after_drop = session.run(
                f"existsTable('{SRC_DB_PATH}', '{table_name}')"
            )
            if source_exists_after_drop:
                raise RuntimeError(f"删除源表失败: {table_name}")

            print(f"迁移并删除源表完成: {table_name}")

        print("=" * 80)
        print("完成")
    finally:
        try:
            session.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="迁移分钟表 ohlcv_daily -> ohlcv_minute")
    parser.add_argument("--execute", action="store_true", help="执行真实迁移与删除（默认仅 dry-run）")
    parser.add_argument(
        "--tables",
        nargs="+",
        help="指定要迁移的表名，支持空格分隔或逗号分隔；默认迁移全部分钟表",
    )
    args = parser.parse_args()

    migrate_tables(execute=args.execute, tables=_parse_tables_arg(args.tables or []))


if __name__ == "__main__":
    main()
