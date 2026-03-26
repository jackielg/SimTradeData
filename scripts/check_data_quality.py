# -*- coding: utf-8 -*-
"""检查已下载的 5 分钟数据质量"""

import argparse
from pathlib import Path
import sys

import duckdb
import pandas as pd

from simtradedata.writers.duckdb_writer_5min import DuckDBWriter5Min


def check_data_quality(db_path: str, month: str = None):
    """Check data quality for downloaded 5-minute data

    Args:
        db_path: Path to DuckDB database
        month: Month to check (YYYY-MM format), default is latest month
    """
    writer = DuckDBWriter5Min(db_path)

    try:
        # 获取统计信息
        stats = writer.get_stats()
        print("=" * 60)
        print("数据统计")
        print("=" * 60)
        print(f'总记录数：{stats["total_records"]:,}')
        print(f'股票数量：{stats["unique_symbols"]:,}')
        print(f'时间范围：{stats["min_datetime"]} ~ {stats["max_datetime"]}')

        # 获取连接查询详细数据
        conn = duckdb.connect(db_path)

        # 如果没有指定月份，使用最新月份
        if month is None:
            query = """
            SELECT strftime(datetime, '%Y-%m') as month
            FROM stocks_5min
            ORDER BY month DESC
            LIMIT 1
            """
            result = conn.execute(query).fetchone()
            if result:
                month = result[0]
                print(f"\n自动选择最新月份：{month}")

        # 查询指定月份的数据
        print("\n" + "=" * 60)
        print(f"{month} 数据分布")
        print("=" * 60)

        query = f"""
        SELECT symbol, COUNT(*) as cnt 
        FROM stocks_5min 
        WHERE strftime(datetime, '%Y-%m') = '{month}' 
        GROUP BY symbol 
        ORDER BY cnt DESC
        """

        result = conn.execute(query).fetchdf()
        print(f"\n有数据的股票数量：{len(result)}")
        print(f"\n前 10 只股票:")
        print(result.head(10).to_string())

        if len(result) > 0:
            print(f"\n记录数统计:")
            print(result["cnt"].describe())

            # 按记录数分组统计
            print(f"\n数据完整性分布:")
            bins = [0, 100, 500, 800, 864, float("inf")]
            labels = [
                "很少 (<100)",
                "较少 (100-500)",
                "中等 (500-800)",
                "完整 (864)",
                "异常 (>864)",
            ]
            result["category"] = pd.cut(result["cnt"], bins=bins, labels=labels)
            print(result["category"].value_counts().sort_index())

        # 查询每个交易日的记录数分布
        print("\n" + "=" * 60)
        print(f"{month} 每日数据分布")
        print("=" * 60)

        query = f"""
        SELECT strftime(datetime, '%Y-%m-%d') as trade_date, 
               COUNT(*) as cnt,
               COUNT(DISTINCT symbol) as stock_count
        FROM stocks_5min 
        WHERE strftime(datetime, '%Y-%m') = '{month}' 
        GROUP BY trade_date
        ORDER BY trade_date
        """

        daily_result = conn.execute(query).fetchdf()
        print(daily_result.to_string())

        # 计算理论值
        print("\n" + "=" * 60)
        print("数据完整性分析")
        print("=" * 60)
        print("理论上，每只股票每天应该有 48 条 5 分钟记录 (4 小时交易时间)")
        trading_days = len(daily_result)
        print(f"{month} 有 {trading_days} 个交易日")
        expected_records = 48 * trading_days
        print(f"所以每只股票应该有约 {expected_records} 条记录 (48 * {trading_days})")

        # 检查数据质量
        if len(result) > 0:
            complete_stocks = len(result[result["cnt"] == expected_records])
            partial_stocks = len(
                result[(result["cnt"] < expected_records) & (result["cnt"] >= 100)]
            )
            sparse_stocks = len(result[result["cnt"] < 100])

            print(f"\n数据质量分类:")
            print(
                f"  完整数据 ({expected_records}条): {complete_stocks} 只股票 ({complete_stocks/len(result)*100:.1f}%)"
            )
            print(
                f"  部分数据 (100-{expected_records-1}条): {partial_stocks} 只股票 ({partial_stocks/len(result)*100:.1f}%)"
            )
            print(
                f"  稀疏数据 (<100 条): {sparse_stocks} 只股票 ({sparse_stocks/len(result)*100:.1f}%)"
            )

        conn.close()

    except Exception as e:
        print(f"检查数据质量时出错：{e}")
        sys.exit(1)
    finally:
        writer.close()


def main():
    parser = argparse.ArgumentParser(description="Check 5-minute data quality")
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/simtradedata_5min.duckdb",
        help="DuckDB database path",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Month to check (YYYY-MM format), default is latest month",
    )

    args = parser.parse_args()

    # Ensure database exists
    if not Path(args.db_path).exists():
        print(f"错误：数据库文件不存在：{args.db_path}")
        sys.exit(1)

    check_data_quality(args.db_path, args.month)


if __name__ == "__main__":
    main()
