#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export DuckDB data to Parquet format

Usage:
    poetry run python scripts/export_parquet.py                  # export CN
    poetry run python scripts/export_parquet.py --market us      # export US
    poetry run python scripts/export_parquet.py --market us --output /custom/path
"""

import argparse
import logging
from pathlib import Path

from simtradedata.utils.paths import DUCKDB_PATH, US_DUCKDB_PATH
from simtradedata.writers.duckdb_writer import DuckDBWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# market → DuckDB path
DB_PATHS = {
    "cn": str(DUCKDB_PATH),
    "us": str(US_DUCKDB_PATH),
}

# Legacy DB names → canonical names (for auto-migration)
_LEGACY_DB = {
    "data/simtradedata.duckdb": str(DUCKDB_PATH),
    "data/us_stocks.duckdb": str(US_DUCKDB_PATH),
}


def _resolve_db(market: str) -> str:
    """Resolve DB path, preferring the one with actual data."""
    canonical = DB_PATHS[market]
    if Path(canonical).exists():
        return canonical
    # Check legacy names as fallback
    for legacy, target in _LEGACY_DB.items():
        if target == canonical and Path(legacy).exists():
            return legacy
    return canonical


def export_to_parquet(db_path: str, output_dir: str, market: str = "cn") -> None:
    print("=" * 70)
    print(f"SimTradeData Export  [{market.upper()}]")
    print("=" * 70)
    print(f"  DB:     {db_path}")
    print(f"  Output: {output_dir}")
    print("=" * 70)

    if not Path(db_path).exists():
        print(f"\nError: Database not found: {db_path}")
        return

    writer = DuckDBWriter(db_path=db_path)
    try:
        writer.export_to_parquet(output_dir, market=market)

        output_path = Path(output_dir)

        def count_files(subdir: str) -> int:
            path = output_path / subdir
            return len(list(path.glob("*.parquet"))) if path.exists() else 0

        def get_dir_size(subdir: str) -> float:
            path = output_path / subdir
            if not path.exists():
                return 0
            return sum(f.stat().st_size for f in path.glob("*.parquet")) / (1024 * 1024)

        print("\nExport Statistics:")
        print(f"  stocks/:       {count_files('stocks'):>5} files, {get_dir_size('stocks'):>8.1f} MB")
        print(f"  exrights/:     {count_files('exrights'):>5} files, {get_dir_size('exrights'):>8.1f} MB")
        print(f"  fundamentals/: {count_files('fundamentals'):>5} files, {get_dir_size('fundamentals'):>8.1f} MB")
        print(f"  valuation/:    {count_files('valuation'):>5} files, {get_dir_size('valuation'):>8.1f} MB")
        print(f"  metadata/:     {count_files('metadata'):>5} files, {get_dir_size('metadata'):>8.1f} MB")

        print(f"\nDone! → {output_dir}")

    finally:
        writer.close()


def main():
    parser = argparse.ArgumentParser(description="Export DuckDB to Parquet")
    parser.add_argument(
        "--market", choices=["cn", "us"], default="cn",
        help="Market to export (default: cn)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output directory (default: data/export/{market})",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Override DuckDB path (default: auto by market)",
    )
    args = parser.parse_args()

    db_path = args.db or _resolve_db(args.market)
    output_dir = args.output or f"data/export/{args.market}"

    export_to_parquet(db_path, output_dir, market=args.market)


if __name__ == "__main__":
    main()
