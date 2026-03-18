# -*- coding: utf-8 -*-
"""
Data integrity checker for SimTradeData DuckDB database

Reports coverage gaps across all tables and identifies stocks
that need re-downloading due to interrupted downloads.

Usage:
    poetry run python scripts/check_integrity.py
    poetry run python scripts/check_integrity.py --fix   # re-download missing data
"""

import argparse
import sys
from pathlib import Path

import duckdb

from simtradedata.utils.paths import DUCKDB_PATH

DB_PATH = str(DUCKDB_PATH)


def get_a_share_filter(col: str = "symbol") -> str:
    """SQL filter for A-share stocks only"""
    return (
        f"(({col} LIKE '6%' AND {col} LIKE '%.SS') OR "
        f"({col} LIKE '0%' AND {col} LIKE '%.SZ') OR "
        f"({col} LIKE '3%' AND {col} LIKE '%.SZ'))"
    )


def check_integrity(db_path: str = DB_PATH) -> dict:
    """
    Check data integrity and return a summary report.

    Returns:
        dict with keys: tables, pool_size, coverage, missing_stocks
    """
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = duckdb.connect(db_path, read_only=True)
    report = {}

    # --- Table summary ---
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()

    print("=" * 70)
    print("SimTradeData Integrity Report")
    print("=" * 70)

    print("\n--- Table Row Counts ---")
    table_info = {}
    for (name,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        table_info[name] = count
        print(f"  {name:30s} {count:>12,} rows")
    report["tables"] = table_info

    # --- Stock pool ---
    pool_stocks = set(
        r[0] for r in conn.execute(
            f"SELECT DISTINCT symbol FROM stock_pool WHERE {get_a_share_filter()}"
        ).fetchall()
    )
    print(f"\n--- A-Share Stock Pool: {len(pool_stocks):,} stocks ---")
    report["pool_size"] = len(pool_stocks)

    # --- Per-table coverage ---
    print("\n--- Coverage vs Stock Pool ---")
    coverage = {}
    data_tables = ["stocks", "valuation", "adjust_factors", "exrights"]
    for table in data_tables:
        if table not in table_info:
            continue
        existing = set(
            r[0] for r in conn.execute(
                f"SELECT DISTINCT symbol FROM {table} WHERE {get_a_share_filter()}"
            ).fetchall()
        )
        missing = pool_stocks - existing
        pct = len(existing) / len(pool_stocks) * 100 if pool_stocks else 0
        coverage[table] = {
            "existing": len(existing),
            "missing": len(missing),
            "pct": pct,
        }
        status = "OK" if pct > 95 else "LOW" if pct > 50 else "CRITICAL"
        print(
            f"  {table:20s} {len(existing):>5,} / {len(pool_stocks):>5,} "
            f"({pct:5.1f}%)  [{status}]"
        )
    report["coverage"] = coverage

    # --- Date range per table ---
    print("\n--- Date Ranges ---")
    for table in data_tables:
        if table not in table_info or table_info[table] == 0:
            continue
        min_d = conn.execute(f"SELECT MIN(date) FROM {table}").fetchone()[0]
        max_d = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()[0]
        print(f"  {table:20s} {min_d} ~ {max_d}")

    # --- Missing stocks detail ---
    print("\n--- Missing Stocks (valuation table) ---")
    val_existing = set(
        r[0] for r in conn.execute(
            f"SELECT DISTINCT symbol FROM valuation WHERE {get_a_share_filter()}"
        ).fetchall()
    )
    val_missing = sorted(pool_stocks - val_existing)
    report["missing_stocks"] = {
        "valuation": val_missing,
    }

    if val_missing:
        print(f"  {len(val_missing):,} A-share stocks missing valuation data")
        # Show first/last few
        show = val_missing[:5]
        if len(val_missing) > 10:
            show += ["..."]
            show += val_missing[-5:]
        print(f"  Examples: {', '.join(show)}")
    else:
        print("  All stocks have valuation data")

    # --- Stale data detection ---
    print("\n--- Stale Data (valuation not up to latest trading day) ---")
    latest_val_date = conn.execute("SELECT MAX(date) FROM valuation").fetchone()[0]
    if latest_val_date:
        stale = conn.execute(
            f"""
            SELECT COUNT(DISTINCT symbol) FROM valuation
            WHERE {get_a_share_filter()}
            GROUP BY symbol
            HAVING MAX(date) < '{latest_val_date}'
            """
        ).fetchall()
        stale_count = len(stale)
        print(f"  Latest valuation date: {latest_val_date}")
        print(f"  Stocks not at latest date: {stale_count:,}")
    else:
        print("  No valuation data")

    # --- Fundamentals progress ---
    if "fundamentals_progress" in table_info:
        print("\n--- Fundamentals Progress ---")
        completed = conn.execute(
            "SELECT year, quarter, stock_count FROM fundamentals_progress "
            "ORDER BY year, quarter"
        ).fetchall()
        if completed:
            print(f"  Completed quarters: {len(completed)}")
            latest_q = completed[-1]
            print(f"  Latest: {latest_q[0]}Q{latest_q[1]} ({latest_q[2]} stocks)")
        else:
            print("  No quarters completed")

    conn.close()

    # --- Overall status ---
    print("\n" + "=" * 70)
    val_cov = coverage.get("valuation", {}).get("pct", 0)
    if val_cov > 95:
        print("Status: HEALTHY")
    elif val_cov > 50:
        print(f"Status: INCOMPLETE (valuation coverage {val_cov:.0f}%)")
    else:
        print(f"Status: INTERRUPTED (valuation coverage {val_cov:.0f}%)")
        print("  Run the download again to resume:")
        print("  poetry run python scripts/download_efficient.py")
    print("=" * 70)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check SimTradeData integrity")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Re-run download to fill missing data",
    )
    args = parser.parse_args()

    report = check_integrity()

    if args.fix:
        val_missing = report.get("missing_stocks", {}).get("valuation", [])
        if val_missing:
            print(f"\nRe-downloading {len(val_missing):,} missing stocks...")
            from scripts.download_efficient import download_all_data
            download_all_data()
        else:
            print("\nNo missing stocks to fix.")
