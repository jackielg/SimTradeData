# -*- coding: utf-8 -*-
"""
DuckDB writer for SimTradeData

This module provides incremental data storage using DuckDB,
with automatic upsert (INSERT OR REPLACE) for deduplication.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd

from simtradedata.utils.paths import DUCKDB_PATH

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = str(DUCKDB_PATH)


class DuckDBWriter:
    """
    Writer for DuckDB incremental storage

    Features:
    - Automatic upsert via INSERT OR REPLACE (uses PRIMARY KEY)
    - Incremental updates: query MAX(date) to determine start_date
    - Export to PTrade Parquet format
    """

    # A-share stock code filter (excludes indices, B-shares, BJ exchange, etc.)
    # Pattern: {prefix}.{market} where prefix is 6-digit numeric
    _CN_STOCK_FILTER = (
        "(symbol LIKE '000___.SZ' OR symbol LIKE '001___.SZ' "
        "OR symbol LIKE '002___.SZ' OR symbol LIKE '003___.SZ' "
        "OR symbol LIKE '300___.SZ' OR symbol LIKE '301___.SZ' "
        "OR symbol LIKE '302___.SZ' "
        "OR symbol LIKE '600___.SS' OR symbol LIKE '601___.SS' "
        "OR symbol LIKE '603___.SS' OR symbol LIKE '605___.SS' "
        "OR symbol LIKE '688___.SS' OR symbol LIKE '689___.SS')"
    )

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

        logger.info(f"DuckDBWriter initialized: {self.db_path}")

    def _init_schema(self) -> None:
        """Initialize database schema"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                close DOUBLE,
                high DOUBLE,
                low DOUBLE,
                high_limit DOUBLE,
                low_limit DOUBLE,
                preclose DOUBLE,
                volume BIGINT,
                money DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)

        # Create index for faster MAX(date) queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stocks_symbol_date
            ON stocks (symbol, date DESC)
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS exrights (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                allotted_ps DOUBLE DEFAULT 0,
                rationed_ps DOUBLE DEFAULT 0,
                rationed_px DOUBLE DEFAULT 0,
                bonus_ps DOUBLE DEFAULT 0,
                dividend DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS valuation (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                pe_ttm DOUBLE,
                pb DOUBLE,
                ps_ttm DOUBLE,
                pcf DOUBLE,
                roe DOUBLE,
                roe_ttm DOUBLE,
                roa DOUBLE,
                roa_ttm DOUBLE,
                naps DOUBLE,
                total_shares DOUBLE,
                a_floats DOUBLE,
                turnover_rate DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                publ_date VARCHAR,
                operating_revenue_grow_rate DOUBLE,
                net_profit_grow_rate DOUBLE,
                basic_eps_yoy DOUBLE,
                np_parent_company_yoy DOUBLE,
                net_profit_ratio DOUBLE,
                net_profit_ratio_ttm DOUBLE,
                gross_income_ratio DOUBLE,
                gross_income_ratio_ttm DOUBLE,
                roa DOUBLE,
                roa_ttm DOUBLE,
                roe DOUBLE,
                roe_ttm DOUBLE,
                total_asset_grow_rate DOUBLE,
                total_asset_turnover_rate DOUBLE,
                current_assets_turnover_rate DOUBLE,
                inventory_turnover_rate DOUBLE,
                accounts_receivables_turnover_rate DOUBLE,
                current_ratio DOUBLE,
                quick_ratio DOUBLE,
                debt_equity_ratio DOUBLE,
                interest_cover DOUBLE,
                roic DOUBLE,
                roa_ebit_ttm DOUBLE,
                total_shares DOUBLE,
                a_floats DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)


        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_metadata (
                symbol VARCHAR PRIMARY KEY,
                stock_name VARCHAR,
                listed_date VARCHAR,
                de_listed_date VARCHAR,
                blocks VARCHAR
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS benchmark (
                date DATE PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                money DOUBLE
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_days (
                date DATE PRIMARY KEY
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_constituents (
                date VARCHAR NOT NULL,
                index_code VARCHAR NOT NULL,
                symbols VARCHAR NOT NULL,
                PRIMARY KEY (date, index_code)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_status (
                date VARCHAR NOT NULL,
                status_type VARCHAR NOT NULL,
                symbols VARCHAR NOT NULL,
                PRIMARY KEY (date, status_type)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_pool (
                symbol VARCHAR PRIMARY KEY,
                first_seen_date DATE NOT NULL,
                last_seen_date DATE NOT NULL
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sampling_progress (
                sample_date DATE PRIMARY KEY
            )
        """)

        # Fundamentals download progress tracking (by quarter)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals_progress (
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                stock_count INTEGER DEFAULT 0,
                filename VARCHAR,
                file_hash VARCHAR,
                PRIMARY KEY (year, quarter)
            )
        """)

        # Migrate existing table: add filename and file_hash columns if missing
        self._migrate_fundamentals_progress()

        # Money flow data (from EastMoney)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS money_flow (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                net_main DOUBLE,
                net_super DOUBLE,
                net_large DOUBLE,
                net_medium DOUBLE,
                net_small DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)

        # LHB (Dragon Tiger Board) data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lhb (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                reason VARCHAR DEFAULT '',
                net_buy DOUBLE,
                buy_amount DOUBLE,
                sell_amount DOUBLE,
                PRIMARY KEY (symbol, date, reason)
            )
        """)

        # Margin trading data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS margin_trading (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                rzye DOUBLE,
                rqyl DOUBLE,
                rzrqye DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS version_info (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)

        # Initialize format info
        self.conn.execute("""
            INSERT OR IGNORE INTO version_info VALUES ('format', 'duckdb')
        """)

    def _migrate_fundamentals_progress(self) -> None:
        """Migrate fundamentals_progress table to add filename and file_hash columns."""
        columns = self.conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fundamentals_progress'
        """).fetchall()
        column_names = {row[0] for row in columns}

        if "filename" not in column_names:
            self.conn.execute("""
                ALTER TABLE fundamentals_progress ADD COLUMN filename VARCHAR
            """)
            logger.info("Added filename column to fundamentals_progress")

        if "file_hash" not in column_names:
            self.conn.execute("""
                ALTER TABLE fundamentals_progress ADD COLUMN file_hash VARCHAR
            """)
            logger.info("Added file_hash column to fundamentals_progress")

    def get_sampled_dates(self) -> list:
        """Get list of dates that have already been sampled"""
        result = self.conn.execute(
            "SELECT sample_date FROM sampling_progress ORDER BY sample_date"
        ).fetchall()
        return [row[0] for row in result]

    def add_sampled_date(self, sample_date) -> None:
        """Mark a date as sampled"""
        self.conn.execute(
            "INSERT OR IGNORE INTO sampling_progress VALUES (?)", [sample_date]
        )

    def get_stock_pool(self) -> list:
        """Get all symbols in stock pool"""
        result = self.conn.execute(
            "SELECT symbol FROM stock_pool ORDER BY symbol"
        ).fetchall()
        return [row[0] for row in result]

    def update_stock_pool(self, symbols: list, sample_date) -> None:
        """Update stock pool with new symbols from a sample date"""
        for symbol in symbols:
            self.conn.execute(
                """
                INSERT INTO stock_pool (symbol, first_seen_date, last_seen_date)
                VALUES (?, ?, ?)
                ON CONFLICT (symbol) DO UPDATE SET
                    last_seen_date = CASE
                        WHEN excluded.last_seen_date > stock_pool.last_seen_date
                        THEN excluded.last_seen_date
                        ELSE stock_pool.last_seen_date
                    END,
                    first_seen_date = CASE
                        WHEN excluded.first_seen_date < stock_pool.first_seen_date
                        THEN excluded.first_seen_date
                        ELSE stock_pool.first_seen_date
                    END
            """,
                [symbol, sample_date, sample_date],
            )

    # ========================================
    # Fundamentals progress tracking
    # ========================================

    def get_existing_fundamental_dates(self, symbol: str) -> set:
        """Get set of existing quarter end dates for a symbol in fundamentals table.

        Returns:
            Set of date strings like {'2024-03-31', '2024-06-30', ...}
        """
        result = self.conn.execute(
            """
            SELECT DISTINCT date FROM fundamentals WHERE symbol = ?
        """,
            [symbol],
        ).fetchall()
        return {str(row[0]) for row in result}

    def has_fundamental(self, symbol: str, date_str: str) -> bool:
        """Check if a specific symbol+date exists in fundamentals table."""
        result = self.conn.execute(
            """
            SELECT 1 FROM fundamentals WHERE symbol = ? AND date = ?
        """,
            [symbol, date_str],
        ).fetchone()
        return result is not None

    def get_completed_fundamental_quarters(self) -> set:
        """Get set of (year, quarter) tuples that are fully downloaded."""
        result = self.conn.execute(
            "SELECT year, quarter FROM fundamentals_progress ORDER BY year, quarter"
        ).fetchall()
        return {(row[0], row[1]) for row in result}

    def get_fundamental_quarter_hash(self, year: int, quarter: int) -> Optional[str]:
        """Get stored hash value for a quarter's financial data.

        Args:
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)

        Returns:
            Hash string if exists, None otherwise
        """
        result = self.conn.execute(
            """
            SELECT file_hash FROM fundamentals_progress
            WHERE year = ? AND quarter = ?
        """,
            [year, quarter],
        ).fetchone()
        return result[0] if result else None

    def delete_fundamental_quarter_data(self, year: int, quarter: int) -> int:
        """Delete all fundamentals data for a specific quarter.

        Args:
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)

        Returns:
            Number of rows deleted
        """
        quarter_end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
        date_str = f"{year}-{quarter_end[quarter]}"

        # Get count before delete (DuckDB doesn't have changes() function)
        count_result = self.conn.execute(
            """
            SELECT COUNT(*) FROM fundamentals WHERE date = ?
        """,
            [date_str],
        ).fetchone()
        count = count_result[0] if count_result else 0

        self.conn.execute(
            """
            DELETE FROM fundamentals WHERE date = ?
        """,
            [date_str],
        )

        # Also delete the progress record
        self.conn.execute(
            """
            DELETE FROM fundamentals_progress WHERE year = ? AND quarter = ?
        """,
            [year, quarter],
        )

        logger.info(f"Deleted {count} fundamentals rows for {year}Q{quarter}")
        return count

    def mark_fundamental_quarter_completed(
        self,
        year: int,
        quarter: int,
        stock_count: int,
        filename: str | None = None,
        file_hash: str | None = None,
    ) -> None:
        """Mark a quarter's fundamentals as fully downloaded.

        Args:
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)
            stock_count: Number of stocks with data
            filename: Source filename (e.g., 'gpcw20231231.zip')
            file_hash: Hash value from TDX server for change detection
        """
        self.conn.execute(
            """
            INSERT OR REPLACE INTO fundamentals_progress
                (year, quarter, stock_count, filename, file_hash)
            VALUES (?, ?, ?, ?, ?)
        """,
            [year, quarter, stock_count, filename, file_hash],
        )

    def close(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def begin(self) -> None:
        """Begin a transaction for batch writes"""
        self.conn.execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        """Commit current transaction"""
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        """Rollback current transaction"""
        self.conn.execute("ROLLBACK")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ========================================
    # Core write methods (with upsert)
    # ========================================

    def write_market_data(self, symbol: str, df: pd.DataFrame) -> int:
        """Write market data with automatic upsert"""
        if df.empty:
            return 0

        df = df.copy()
        df["symbol"] = symbol

        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date

        columns = [
            "symbol",
            "date",
            "open",
            "close",
            "high",
            "low",
            "high_limit",
            "low_limit",
            "preclose",
            "volume",
            "money",
        ]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO stocks ({cols_str})
            SELECT {cols_str} FROM df
        """)

        logger.debug(f"Wrote {len(df)} market rows for {symbol}")
        return len(df)

    def write_valuation(self, symbol: str, df: pd.DataFrame) -> int:
        """Write valuation data with upsert"""
        if df.empty:
            return 0

        df = df.copy()
        df["symbol"] = symbol

        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date

        columns = [
            "symbol",
            "date",
            "pe_ttm",
            "pb",
            "ps_ttm",
            "pcf",
            "roe",
            "roe_ttm",
            "roa",
            "roa_ttm",
            "naps",
            "total_shares",
            "a_floats",
            "turnover_rate",
        ]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO valuation ({cols_str})
            SELECT {cols_str} FROM df
        """)

        logger.debug(f"Wrote {len(df)} valuation rows for {symbol}")
        return len(df)

    def write_fundamentals(self, symbol: str, df: pd.DataFrame) -> int:
        """Write quarterly fundamentals with upsert"""
        if df.empty:
            return 0

        df = df.copy()
        df["symbol"] = symbol

        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "date"})

        if "end_date" in df.columns and "date" not in df.columns:
            df = df.rename(columns={"end_date": "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date

        if "publ_date" in df.columns:
            df["publ_date"] = pd.to_datetime(
                df["publ_date"], errors="coerce"
            ).dt.strftime("%Y%m%d")

        columns = [
            "symbol",
            "date",
            "publ_date",
            "operating_revenue_grow_rate",
            "net_profit_grow_rate",
            "basic_eps_yoy",
            "np_parent_company_yoy",
            "net_profit_ratio",
            "net_profit_ratio_ttm",
            "gross_income_ratio",
            "gross_income_ratio_ttm",
            "roa",
            "roa_ttm",
            "roe",
            "roe_ttm",
            "total_asset_grow_rate",
            "total_asset_turnover_rate",
            "current_assets_turnover_rate",
            "inventory_turnover_rate",
            "accounts_receivables_turnover_rate",
            "current_ratio",
            "quick_ratio",
            "debt_equity_ratio",
            "interest_cover",
            "roic",
            "roa_ebit_ttm",
            "total_shares",
            "a_floats",
        ]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        # Use ON CONFLICT to only update columns present in the DataFrame,
        # preserving existing values for columns not in this write batch.
        update_cols = [c for c in available if c not in ("symbol", "date")]
        if update_cols:
            set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            conflict_clause = f"ON CONFLICT (symbol, date) DO UPDATE SET {set_clause}"
        else:
            conflict_clause = "ON CONFLICT (symbol, date) DO NOTHING"
        self.conn.execute(f"""
            INSERT INTO fundamentals ({cols_str})
            SELECT {cols_str} FROM df
            {conflict_clause}
        """)

        logger.debug(f"Wrote {len(df)} fundamental rows for {symbol}")
        return len(df)

    def write_exrights(self, symbol: str, df: pd.DataFrame) -> int:
        """Write exrights data with upsert"""
        if df.empty:
            return 0

        df = df.copy()
        df["symbol"] = symbol

        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date

        columns = [
            "symbol",
            "date",
            "allotted_ps",
            "rationed_ps",
            "rationed_px",
            "bonus_ps",
            "dividend",
        ]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO exrights ({cols_str})
            SELECT {cols_str} FROM df
        """)

        logger.debug(f"Wrote {len(df)} exrights rows for {symbol}")
        return len(df)

    def write_adjust_factor(self, symbol: str, data) -> int:
        """Deprecated: adjust factors removed. SimTradeLab computes from exrights."""
        return 0

    def write_benchmark(self, df: pd.DataFrame) -> int:
        """Write benchmark index data"""
        if df.empty:
            return 0

        df = df.copy()

        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date

        columns = ["date", "open", "high", "low", "close", "volume", "money"]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO benchmark ({cols_str})
            SELECT {cols_str} FROM df
        """)

        logger.info(f"Wrote {len(df)} benchmark rows")
        return len(df)

    def write_trade_days(self, df: pd.DataFrame) -> int:
        """Write trading calendar"""
        if df.empty:
            return 0

        df = df.copy()

        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "date"})

        if "trade_date" in df.columns:
            df = df.rename(columns={"trade_date": "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[["date"]]

        self.conn.execute("""
            INSERT OR IGNORE INTO trade_days
            SELECT * FROM df
        """)

        logger.info(f"Wrote {len(df)} trade days")
        return len(df)

    def write_stock_metadata(self, df: pd.DataFrame) -> int:
        """Write stock metadata"""
        if df.empty:
            return 0

        df = df.copy()

        if df.index.name == "stock_code" or "stock_code" in df.columns:
            df = df.reset_index()
            if "stock_code" in df.columns:
                df = df.rename(columns={"stock_code": "symbol"})

        if "index" in df.columns and "symbol" not in df.columns:
            df = df.rename(columns={"index": "symbol"})

        columns = ["symbol", "stock_name", "listed_date", "de_listed_date", "blocks"]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO stock_metadata ({cols_str})
            SELECT {cols_str} FROM df
        """)

        logger.info(f"Wrote {len(df)} stock metadata records")
        return len(df)

    def write_index_constituents(
        self, date: str, index_code: str, symbols: List[str]
    ) -> None:
        """Write index constituents for a specific date"""
        symbols_json = json.dumps(symbols, ensure_ascii=False)

        self.conn.execute(
            """
            INSERT OR REPLACE INTO index_constituents (date, index_code, symbols)
            VALUES (?, ?, ?)
        """,
            [date, index_code, symbols_json],
        )

    def write_stock_status(
        self, date: str, status_type: str, symbols: List[str]
    ) -> None:
        """Write stock status for a specific date"""
        symbols_json = json.dumps(symbols, ensure_ascii=False)

        self.conn.execute(
            """
            INSERT OR REPLACE INTO stock_status (date, status_type, symbols)
            VALUES (?, ?, ?)
        """,
            [date, status_type, symbols_json],
        )

    def write_money_flow(self, symbol: str, df: pd.DataFrame) -> int:
        """Write money flow data with upsert."""
        if df.empty:
            return 0
        df = df.copy()
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date
        columns = [
            "symbol",
            "date",
            "net_main",
            "net_super",
            "net_large",
            "net_medium",
            "net_small",
        ]
        available = [c for c in columns if c in df.columns]
        df = df[available]
        cols_str = ", ".join(available)
        self.conn.execute(
            f"INSERT OR REPLACE INTO money_flow ({cols_str}) SELECT {cols_str} FROM df"
        )
        return len(df)

    def write_lhb(self, df: pd.DataFrame) -> int:
        """Write LHB data with upsert. DataFrame must include symbol column."""
        if df.empty:
            return 0
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        if "reason" in df.columns:
            df["reason"] = df["reason"].fillna("")
        columns = ["symbol", "date", "reason", "net_buy", "buy_amount", "sell_amount"]
        available = [c for c in columns if c in df.columns]
        df = df[available]
        cols_str = ", ".join(available)
        self.conn.execute(
            f"INSERT OR REPLACE INTO lhb ({cols_str}) SELECT {cols_str} FROM df"
        )
        return len(df)

    def write_margin_trading(self, symbol: str, df: pd.DataFrame) -> int:
        """Write margin trading data with upsert."""
        if df.empty:
            return 0
        df = df.copy()
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date
        columns = ["symbol", "date", "rzye", "rqyl", "rzrqye"]
        available = [c for c in columns if c in df.columns]
        df = df[available]
        cols_str = ", ".join(available)
        self.conn.execute(
            f"INSERT OR REPLACE INTO margin_trading ({cols_str}) SELECT {cols_str} FROM df"
        )
        return len(df)

    def write_global_metadata(self, meta: pd.Series) -> None:
        """Write global metadata to version_info table"""
        for key, value in meta.items():
            self.conn.execute(
                """
                INSERT OR REPLACE INTO version_info (key, value)
                VALUES (?, ?)
            """,
                [str(key), str(value)],
            )

    # ========================================
    # Incremental update helpers
    # ========================================

    def get_max_date(self, table: str, symbol: str = None) -> Optional[str]:
        """Get maximum date for incremental update"""
        if symbol:
            result = self.conn.execute(
                f"""
                SELECT MAX(date) FROM {table} WHERE symbol = ?
            """,
                [symbol],
            ).fetchone()
        else:
            result = self.conn.execute(f"""
                SELECT MAX(date) FROM {table}
            """).fetchone()

        if result and result[0]:
            return str(result[0])
        return None

    def get_min_date(self, table: str, symbol: str = None) -> Optional[str]:
        """Get minimum date for backfill detection"""
        if symbol:
            result = self.conn.execute(
                f"""
                SELECT MIN(date) FROM {table} WHERE symbol = ?
            """,
                [symbol],
            ).fetchone()
        else:
            result = self.conn.execute(f"""
                SELECT MIN(date) FROM {table}
            """).fetchone()

        if result and result[0]:
            return str(result[0])
        return None

    def get_existing_stocks(self, table: str = "stocks") -> List[str]:
        """Get list of symbols in database"""
        result = self.conn.execute(f"""
            SELECT DISTINCT symbol FROM {table}
        """).fetchall()
        return [r[0] for r in result]

    def get_stock_count(self) -> int:
        """Get total number of unique stocks"""
        result = self.conn.execute("""
            SELECT COUNT(DISTINCT symbol) FROM stocks
        """).fetchone()
        return result[0] if result else 0

    def get_data_status(self) -> dict:
        """Get a summary of data completeness across all tables.

        Returns:
            Dict with table names as keys and summary dicts as values.
        """
        status = {}
        for table in [
            "stocks",
            "valuation",
            "fundamentals",
            "exrights",
        ]:
            status[table] = self._get_table_summary(table)

        # Add fundamentals quarter progress
        status["fundamentals_quarters"] = len(self.get_completed_fundamental_quarters())

        # Add metadata counts
        for table in ["benchmark", "trade_days", "index_constituents", "stock_status"]:
            status[table] = self._get_table_summary_simple(table)

        return status

    def _get_table_summary(self, table: str) -> dict:
        """Get row count, stock count, and date range for a symbol-based table."""
        try:
            result = self.conn.execute(f"""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT symbol) as stock_count,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM {table}
            """).fetchone()
            return {
                "rows": result[0],
                "stocks": result[1],
                "min_date": str(result[2]) if result[2] else None,
                "max_date": str(result[3]) if result[3] else None,
            }
        except Exception:
            return {"rows": 0, "stocks": 0, "min_date": None, "max_date": None}

    def _get_table_summary_simple(self, table: str) -> dict:
        """Get row count for a non-symbol table."""
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table}
            """).fetchone()
            return {"rows": result[0]}
        except Exception:
            return {"rows": 0}

    # ========================================
    # Derived fields
    # ========================================

    def compute_derived_fundamentals(self) -> None:
        """
        Fill roa, roe_ttm, roa_ttm from existing fundamentals data.

        - roa = roe / (1 + debt_equity_ratio)
        - roe_ttm = rolling 4-quarter average of roe
        - roa_ttm = rolling 4-quarter average of roa
        """
        # roa = roe / (1 + debt_equity_ratio)
        self.conn.execute("""
            UPDATE fundamentals
            SET roa = roe / (1 + debt_equity_ratio)
            WHERE roe IS NOT NULL
              AND debt_equity_ratio IS NOT NULL
              AND debt_equity_ratio != -1
              AND roa IS NULL
        """)

        # roe_ttm / roa_ttm = rolling 4-quarter average
        self.conn.execute("""
            UPDATE fundamentals f
            SET
                roe_ttm = sub.roe_ttm,
                roa_ttm = sub.roa_ttm
            FROM (
                SELECT
                    symbol, date,
                    AVG(roe) OVER w AS roe_ttm,
                    AVG(roa) OVER w AS roa_ttm
                FROM fundamentals
                WHERE roe IS NOT NULL OR roa IS NOT NULL
                WINDOW w AS (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                )
            ) sub
            WHERE f.symbol = sub.symbol
              AND f.date = sub.date
              AND f.roe_ttm IS NULL
        """)

        updated = self.conn.execute("""
            SELECT
                SUM(CASE WHEN roa IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN roe_ttm IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN roa_ttm IS NOT NULL THEN 1 ELSE 0 END)
            FROM fundamentals
        """).fetchone()
        logger.info(
            f"Derived fundamentals: roa={updated[0]:,} roe_ttm={updated[1]:,} roa_ttm={updated[2]:,}"
        )

    # ========================================
    # Export to Parquet
    # ========================================

    def export_to_parquet(self, output_dir: str, market: str = "cn") -> None:
        """Export all data to PTrade Parquet format"""
        output_path = Path(output_dir)

        # Compute derived fields before export
        self.compute_derived_fundamentals()

        # Clean output directory to avoid mixing data from different markets
        if output_path.exists():
            import shutil

            shutil.rmtree(output_path)

        for subdir in ["stocks", "exrights", "fundamentals", "valuation", "metadata"]:
            (output_path / subdir).mkdir(parents=True, exist_ok=True)

        logger.info("Exporting stocks...")
        self._export_stocks_batch(output_path / "stocks", market=market)

        logger.info("Exporting exrights...")
        self._export_exrights_batch(output_path / "exrights")

        logger.info("Exporting fundamentals...")
        self._export_fundamentals_batch(output_path / "fundamentals")

        logger.info("Exporting valuation...")
        self._export_valuation_batch(output_path / "valuation")

        logger.info("Exporting metadata...")
        self._export_metadata(output_path / "metadata", market=market)

        self._write_manifest(output_path, market=market)

        logger.info(f"Export complete: {output_path}")

    def _export_per_symbol_table(
        self, table: str, output_dir: Path, market: str = "cn"
    ) -> None:
        """Export table to per-symbol Parquet files using DuckDB COPY"""
        symbols = self.get_existing_stocks(table)

        if not symbols:
            logger.info(f"No data in {table} to export")
            return

        for symbol in symbols:
            output_file = output_dir / f"{symbol}.parquet"
            # Escape single quotes in symbol for SQL safety
            symbol_escaped = symbol.replace("'", "''")

            if table == "stocks":
                # Calculate high_limit and low_limit during export
                self._export_stocks_with_limits(
                    symbol_escaped, output_file, market=market
                )
            elif table == "fundamentals":
                # Calculate TTM indicators during export
                self._export_fundamentals_with_ttm(symbol_escaped, output_file)
            elif table == "valuation":
                # Enrich with total_shares/a_floats from fundamentals
                self._export_valuation_enriched(symbol_escaped, output_file)
            elif table == "exrights":
                self._export_exrights_with_factors(symbol_escaped, output_file)
            else:
                self.conn.execute(f"""
                    COPY (
                        SELECT * EXCLUDE (symbol) REPLACE (date::TIMESTAMP_NS AS date) FROM {table}
                        WHERE symbol = '{symbol_escaped}'
                        ORDER BY date
                    ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
                """)

        logger.info(f"Exported {len(symbols)} {table} files")

    def _export_exrights_with_factors(self, symbol_escaped: str, output_file: Path) -> None:
        """Export exrights with computed exer_forward_a/b factors"""
        import numpy as np

        df = self.conn.execute(f"""
            SELECT * EXCLUDE (symbol) REPLACE (date::TIMESTAMP_NS AS date)
            FROM exrights WHERE symbol = '{symbol_escaped}' ORDER BY date
        """).fetchdf()

        if df.empty:
            df.to_parquet(str(output_file), index=False)
            return

        n = len(df)
        allotted = df["allotted_ps"].values
        bonus = df["bonus_ps"].values
        rationed = df["rationed_ps"].values
        rationed_px = df["rationed_px"].values

        # Backward accumulation of forward-adjustment factors.
        # Continuity at each ex-date requires:
        #   fa[i] * P_raw + fb[i] = fa[i+1] * P_ex + fb[i+1]
        # where P_ex = (P_raw - bonus + rat*rat_px) / m, giving:
        #   fa[i] = fa[i+1] / m
        #   fb[i] = fa[i+1] * (-bonus + rat*rat_px) / m + fb[i+1]
        fa = np.ones(n + 1, dtype="float64")
        fb = np.zeros(n + 1, dtype="float64")
        for i in range(n - 1, -1, -1):
            m = 1.0 + allotted[i] + rationed[i]
            fa[i] = fa[i + 1] / m
            fb[i] = fa[i + 1] * (-bonus[i] + rationed[i] * rationed_px[i]) / m + fb[i + 1]

        df["exer_forward_a"] = fa[:n]
        df["exer_forward_b"] = fb[:n]

        df.to_parquet(str(output_file), index=False, compression="zstd")

    def _export_exrights_batch(self, output_dir: Path) -> None:
        """Export all exrights with pre-computed forward adj factors (batch)."""
        import time
        import numpy as np
        t0 = time.time()

        df_all = self.conn.execute(f"""
            SELECT symbol, date::TIMESTAMP_NS AS date,
                   allotted_ps, rationed_ps, rationed_px, bonus_ps, dividend
            FROM exrights
            WHERE {self._CN_STOCK_FILTER}
            ORDER BY symbol, date
        """).fetchdf()

        if df_all.empty:
            logger.info("No exrights data to export")
            return

        count = 0
        for symbol, group in df_all.groupby("symbol"):
            df = group.drop(columns=["symbol"]).reset_index(drop=True)
            n = len(df)
            allotted = df["allotted_ps"].values
            bonus = df["bonus_ps"].values
            rationed = df["rationed_ps"].values
            rationed_px = df["rationed_px"].values

            fa = np.ones(n + 1, dtype="float64")
            fb = np.zeros(n + 1, dtype="float64")
            for i in range(n - 1, -1, -1):
                m = 1.0 + allotted[i] + rationed[i]
                fa[i] = fa[i + 1] / m
                fb[i] = fa[i + 1] * (-bonus[i] + rationed[i] * rationed_px[i]) / m + fb[i + 1]

            df["exer_forward_a"] = fa[:n]
            df["exer_forward_b"] = fb[:n]
            df.to_parquet(str(output_dir / f"{symbol}.parquet"), index=False, compression="zstd")
            count += 1

        logger.info(f"Exported {count} exrights files in {time.time() - t0:.1f}s")

    def _export_fundamentals_batch(self, output_dir: Path) -> None:
        """Export all fundamentals with TTM ratios (batch via temp table)."""
        import time
        t0 = time.time()

        self.conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _fundamentals_export AS
            SELECT
                symbol,
                date::TIMESTAMP_NS AS date, publ_date,
                operating_revenue_grow_rate, net_profit_grow_rate,
                basic_eps_yoy, np_parent_company_yoy,
                net_profit_ratio,
                AVG(net_profit_ratio) OVER (
                    PARTITION BY symbol ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) AS net_profit_ratio_ttm,
                gross_income_ratio,
                AVG(gross_income_ratio) OVER (
                    PARTITION BY symbol ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) AS gross_income_ratio_ttm,
                roa, roa_ttm,
                roe, roe_ttm,
                total_asset_grow_rate, total_asset_turnover_rate,
                current_assets_turnover_rate, inventory_turnover_rate,
                accounts_receivables_turnover_rate,
                current_ratio, quick_ratio, debt_equity_ratio,
                interest_cover, roic, roa_ebit_ttm,
                total_shares, a_floats
            FROM fundamentals
            WHERE {self._CN_STOCK_FILTER}
        """)

        symbols = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT symbol FROM _fundamentals_export ORDER BY symbol"
        ).fetchall()]

        for symbol in symbols:
            se = symbol.replace("'", "''")
            self.conn.execute(f"""
                COPY (
                    SELECT * EXCLUDE (symbol) FROM _fundamentals_export
                    WHERE symbol = '{se}' ORDER BY date
                ) TO '{output_dir / f"{symbol}.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        self.conn.execute("DROP TABLE IF EXISTS _fundamentals_export")
        logger.info(f"Exported {len(symbols)} fundamentals files in {time.time() - t0:.1f}s")

    def _export_valuation_batch(self, output_dir: Path) -> None:
        """Export all valuation data enriched with fundamentals (batch via temp table).

        Uses ASOF JOIN to efficiently pick the most recent fundamentals record
        for each valuation row, avoiding expensive window-function gap-filling.
        """
        import time
        t0 = time.time()

        self.conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _valuation_export AS
            SELECT
                v.symbol,
                v.date::TIMESTAMP_NS AS date,
                v.pe_ttm, v.pb, v.ps_ttm, v.pcf,
                f.roe, f.roe_ttm, f.roa, f.roa_ttm,
                CASE WHEN v.pb > 0 THEN ROUND(s.close / v.pb, 4) ELSE NULL END AS naps,
                f.total_shares, f.a_floats,
                CASE WHEN f.total_shares > 0 AND s.close IS NOT NULL
                     THEN ROUND(f.total_shares * s.close, 2) END AS total_value,
                CASE WHEN f.a_floats > 0 AND s.close IS NOT NULL
                     THEN ROUND(f.a_floats * s.close, 2) END AS float_value,
                v.turnover_rate
            FROM valuation v
            ASOF JOIN (SELECT symbol, date, close FROM stocks) s
                ON v.symbol = s.symbol AND v.date >= s.date
            LEFT JOIN LATERAL (
                SELECT total_shares, a_floats, roe, roe_ttm, roa, roa_ttm
                FROM fundamentals f2
                WHERE f2.symbol = v.symbol AND f2.date <= v.date
                ORDER BY f2.date DESC LIMIT 1
            ) f ON TRUE
            WHERE {self._CN_STOCK_FILTER.replace('symbol', 'v.symbol')}
        """)

        symbols = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT symbol FROM _valuation_export ORDER BY symbol"
        ).fetchall()]

        for symbol in symbols:
            se = symbol.replace("'", "''")
            self.conn.execute(f"""
                COPY (
                    SELECT * EXCLUDE (symbol) FROM _valuation_export
                    WHERE symbol = '{se}' ORDER BY date
                ) TO '{output_dir / f"{symbol}.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        self.conn.execute("DROP TABLE IF EXISTS _valuation_export")
        logger.info(f"Exported {len(symbols)} valuation files in {time.time() - t0:.1f}s")

    def _export_stocks_batch(self, output_dir: Path, market: str = "cn") -> None:
        """Export all stocks with gap-filling and price limits (batch optimized).

        Pre-computes gap-filled data for ALL stocks in one query,
        then writes per-symbol parquet files with a simple filter.
        """
        import time
        t0 = time.time()

        # Step 1: Build gap-filled table for all stocks at once
        cn_filter = f"WHERE {self._CN_STOCK_FILTER}" if market == "cn" else ""
        logger.info("  Pre-computing gap-filled data for all stocks...")
        self.conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _stocks_filled AS
            WITH trade_cal AS (
                SELECT DISTINCT date FROM stocks
                UNION
                SELECT date FROM trade_days
            ),
            lifespans AS (
                SELECT symbol, MIN(date) AS first_date, MAX(date) AS last_date
                FROM stocks {cn_filter} GROUP BY symbol
            ),
            joined AS (
                SELECT
                    ls.symbol,
                    tc.date,
                    -- NULL out OHLC when volume=0 (suspended).
                    -- This prevents mootdx's ex-rights adjusted prices
                    -- from replacing the last traded price during suspension.
                    CASE WHEN s.volume > 0 THEN s.open END AS open,
                    CASE WHEN s.volume > 0 THEN s.close END AS close,
                    CASE WHEN s.volume > 0 THEN s.high END AS high,
                    CASE WHEN s.volume > 0 THEN s.low END AS low,
                    -- Keep preclose only on trading days; during suspension
                    -- it will be recomputed as LAG(close) in the final step.
                    CASE WHEN s.volume > 0 THEN s.preclose END AS preclose,
                    COALESCE(s.volume, 0) AS volume,
                    COALESCE(s.money, 0.0) AS money
                FROM lifespans ls
                CROSS JOIN trade_cal tc
                LEFT JOIN stocks s ON s.symbol = ls.symbol AND s.date = tc.date
                WHERE tc.date >= ls.first_date AND tc.date <= ls.last_date
            ),
            gap_filled AS (
                SELECT
                    symbol, date,
                    COALESCE(open, last_value(close IGNORE NULLS) OVER w) AS open,
                    COALESCE(close, last_value(close IGNORE NULLS) OVER w) AS close,
                    COALESCE(high, last_value(close IGNORE NULLS) OVER w) AS high,
                    COALESCE(low, last_value(close IGNORE NULLS) OVER w) AS low,
                    preclose, volume, money
                FROM joined
                WINDOW w AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            ),
            with_lag AS (
                SELECT
                    symbol, date,
                    open, close, high, low,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS lag_close,
                    -- Use last ACTIVE trading date (not gap-filled date) for exrights range check.
                    -- During suspension, lag_date would be yesterday (gap-filled),
                    -- but we need the last real trading day to catch exrights events
                    -- that occurred during the suspension period.
                    last_value(CASE WHEN volume > 0 THEN date END IGNORE NULLS) OVER (
                        PARTITION BY symbol ORDER BY date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ) AS last_active_date,
                    preclose AS stored_preclose,
                    volume, money
                FROM gap_filled
            ),
            adj AS (
                SELECT
                    wl.symbol, wl.date,
                    SUM(COALESCE(ex.bonus_ps, 0)
                        - COALESCE(ex.rationed_px, 0) * COALESCE(ex.rationed_ps, 0)
                    ) AS total_deduction,
                    EXP(SUM(LN(
                        1 + COALESCE(ex.allotted_ps, 0) + COALESCE(ex.rationed_ps, 0)
                    ))) AS total_divisor,
                    COUNT(ex.date) AS event_count
                FROM with_lag wl
                INNER JOIN exrights ex ON ex.symbol = wl.symbol
                    AND ex.date > wl.last_active_date AND ex.date <= wl.date
                GROUP BY wl.symbol, wl.date
            )
            SELECT
                wl.symbol,
                wl.date::TIMESTAMP_NS AS date,
                wl.open, wl.close, wl.high, wl.low,
                CASE
                    WHEN adj.event_count > 0 AND wl.lag_close IS NOT NULL
                         AND wl.volume > 0 THEN
                        ROUND(
                            (wl.lag_close - adj.total_deduction)
                            / adj.total_divisor,
                            2)
                    ELSE COALESCE(wl.lag_close, wl.stored_preclose)
                END AS preclose,
                wl.volume, wl.money
            FROM with_lag wl
            LEFT JOIN adj ON adj.symbol = wl.symbol AND adj.date = wl.date
        """)
        t1 = time.time()
        row_count = self.conn.execute("SELECT COUNT(*) FROM _stocks_filled").fetchone()[0]
        logger.info(f"  Gap-fill complete: {row_count} rows in {t1 - t0:.1f}s")

        # Step 2: Write per-symbol parquet files with price limits
        symbols = [r[0] for r in self.conn.execute(
            "SELECT DISTINCT symbol FROM _stocks_filled ORDER BY symbol"
        ).fetchall()]

        if market == "us":
            for symbol in symbols:
                se = symbol.replace("'", "''")
                self.conn.execute(f"""
                    COPY (
                        SELECT date, open, close, high, low,
                            NULL AS high_limit, NULL AS low_limit,
                            preclose, volume, money
                        FROM _stocks_filled WHERE symbol = '{se}' ORDER BY date
                    ) TO '{output_dir / f"{symbol}.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
                """)
        else:
            for symbol in symbols:
                se = symbol.replace("'", "''")
                output_file = output_dir / f"{symbol}.parquet"
                code_prefix = symbol[:3]
                is_chinext_star = code_prefix in ("300", "301", "688", "689")

                if is_chinext_star:
                    limit_sql = """
                        CASE WHEN date >= '2020-08-24' THEN ROUND(preclose * 1.20, 2)
                             ELSE ROUND(preclose * 1.10, 2) END AS high_limit,
                        CASE WHEN date >= '2020-08-24' THEN ROUND(preclose * 0.80, 2)
                             ELSE ROUND(preclose * 0.90, 2) END AS low_limit,
                    """
                else:
                    limit_sql = """
                        ROUND(preclose * 1.10, 2) AS high_limit,
                        ROUND(preclose * 0.90, 2) AS low_limit,
                    """

                self.conn.execute(f"""
                    COPY (
                        SELECT date, open, close, high, low,
                            {limit_sql}
                            preclose, volume, money
                        FROM _stocks_filled WHERE symbol = '{se}' ORDER BY date
                    ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
                """)

        self.conn.execute("DROP TABLE IF EXISTS _stocks_filled")
        logger.info(f"Exported {len(symbols)} stocks files in {time.time() - t0:.1f}s")

    def _export_stocks_with_limits(
        self, symbol_escaped: str, output_file: Path, market: str = "cn"
    ) -> None:
        """
        Export stocks data with calculated price limits

        Price limit rules:
        - US stocks: no price limits (NULL)
        - Normal stocks: ±10%
        - ST stocks: ±5%
        - ChiNext (300xxx, 301xxx) / STAR (688xxx, 689xxx): ±20% after 2020-08-24
        """
        # CTE fills suspension days (volume=0, OHLC = last close)
        # and missing preclose with previous day's close.
        # trade_cal: all trading days derived from stocks table
        # raw: actual data rows for this symbol
        # joined: left join ensures every trading day within stock's lifespan has a row
        # gap_filled: forward-fill close into suspension gaps
        # filled: compute preclose from gap_filled close
        base_cte = f"""
            WITH raw AS (
                SELECT date, open, close, high, low, preclose, volume, money
                FROM stocks WHERE symbol = '{symbol_escaped}'
            ),
            lifespan AS (
                SELECT MIN(date) AS first_date, MAX(date) AS last_date FROM raw
            ),
            joined AS (
                SELECT
                    tc.date,
                    -- NULL out OHLC when volume=0 (suspended).
                    -- Prevents ex-rights adjusted prices from replacing
                    -- the last traded price during suspension.
                    CASE WHEN r.volume > 0 THEN r.open END AS open,
                    CASE WHEN r.volume > 0 THEN r.close END AS close,
                    CASE WHEN r.volume > 0 THEN r.high END AS high,
                    CASE WHEN r.volume > 0 THEN r.low END AS low,
                    CASE WHEN r.volume > 0 THEN r.preclose END AS preclose,
                    COALESCE(r.volume, 0) AS volume,
                    COALESCE(r.money, 0.0) AS money
                FROM _trade_cal tc
                CROSS JOIN lifespan ls
                LEFT JOIN raw r ON tc.date = r.date
                WHERE tc.date >= ls.first_date AND tc.date <= ls.last_date
            ),
            gap_filled AS (
                SELECT
                    date,
                    COALESCE(open, last_value(close IGNORE NULLS) OVER w) AS open,
                    COALESCE(close, last_value(close IGNORE NULLS) OVER w) AS close,
                    COALESCE(high, last_value(close IGNORE NULLS) OVER w) AS high,
                    COALESCE(low, last_value(close IGNORE NULLS) OVER w) AS low,
                    preclose,
                    volume,
                    money
                FROM joined
                WINDOW w AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            ),
            with_lag AS (
                SELECT
                    date, open, close, high, low,
                    LAG(close) OVER (ORDER BY date) AS lag_close,
                    last_value(CASE WHEN volume > 0 THEN date END IGNORE NULLS) OVER (
                        ORDER BY date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ) AS last_active_date,
                    preclose AS stored_preclose,
                    volume, money
                FROM gap_filled
            ),
            adj AS (
                SELECT
                    wl.date,
                    SUM(COALESCE(ex.bonus_ps, 0)
                        - COALESCE(ex.rationed_px, 0) * COALESCE(ex.rationed_ps, 0)
                    ) AS total_deduction,
                    EXP(SUM(LN(
                        1 + COALESCE(ex.allotted_ps, 0) + COALESCE(ex.rationed_ps, 0)
                    ))) AS total_divisor,
                    COUNT(ex.date) AS event_count
                FROM with_lag wl
                INNER JOIN exrights ex ON ex.symbol = '{symbol_escaped}'
                    AND ex.date > wl.last_active_date AND ex.date <= wl.date
                GROUP BY wl.date
            ),
            filled AS (
                SELECT
                    wl.date::TIMESTAMP_NS AS date, wl.open, wl.close, wl.high, wl.low,
                    CASE
                        WHEN adj.event_count > 0 AND wl.lag_close IS NOT NULL
                             AND wl.volume > 0 THEN
                            ROUND(
                                (wl.lag_close - adj.total_deduction)
                                / adj.total_divisor,
                                2)
                        ELSE COALESCE(wl.lag_close, wl.stored_preclose)
                    END AS preclose,
                    wl.volume, wl.money
                FROM with_lag wl
                LEFT JOIN adj ON adj.date = wl.date
            )
        """

        if market == "us":
            self.conn.execute(f"""
                COPY (
                    {base_cte}
                    SELECT date, open, close, high, low,
                        NULL AS high_limit, NULL AS low_limit,
                        preclose, volume, money
                    FROM filled ORDER BY date
                ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)
            return
        # Extract numeric code prefix to determine board type
        code_prefix = symbol_escaped[:3]

        # Check if ChiNext or STAR market
        is_chinext_star = code_prefix in ("300", "301", "688", "689")

        if is_chinext_star:
            # ChiNext/STAR: 20% after 2020-08-24, 10% before
            self.conn.execute(f"""
                COPY (
                    {base_cte}
                    SELECT date, open, close, high, low,
                        CASE
                            WHEN date >= '2020-08-24' THEN ROUND(preclose * 1.20, 2)
                            ELSE ROUND(preclose * 1.10, 2)
                        END AS high_limit,
                        CASE
                            WHEN date >= '2020-08-24' THEN ROUND(preclose * 0.80, 2)
                            ELSE ROUND(preclose * 0.90, 2)
                        END AS low_limit,
                        preclose, volume, money
                    FROM filled ORDER BY date
                ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)
        else:
            # Normal stocks: 10% limit (ST handling needs isST from status)
            # For now, use 10% as default; ST detection could be added later
            self.conn.execute(f"""
                COPY (
                    {base_cte}
                    SELECT date, open, close, high, low,
                        ROUND(preclose * 1.10, 2) AS high_limit,
                        ROUND(preclose * 0.90, 2) AS low_limit,
                        preclose, volume, money
                    FROM filled ORDER BY date
                ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)

    def _export_fundamentals_with_ttm(
        self, symbol_escaped: str, output_file: Path
    ) -> None:
        """
        Export fundamentals data with TTM indicators.

        TTM fields (roe_ttm, roa_ttm, net_profit_ratio_ttm, gross_income_ratio_ttm)
        are pre-computed in DB by compute_derived_fundamentals() or download.
        Only net_profit_ratio_ttm and gross_income_ratio_ttm are calculated here
        since they are not stored in the DB.
        """
        self.conn.execute(f"""
            COPY (
                SELECT
                    date::TIMESTAMP_NS AS date, publ_date,
                    operating_revenue_grow_rate, net_profit_grow_rate,
                    basic_eps_yoy, np_parent_company_yoy,
                    net_profit_ratio,
                    AVG(net_profit_ratio) OVER (
                        ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS net_profit_ratio_ttm,
                    gross_income_ratio,
                    AVG(gross_income_ratio) OVER (
                        ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS gross_income_ratio_ttm,
                    roa, roa_ttm,
                    roe, roe_ttm,
                    total_asset_grow_rate, total_asset_turnover_rate,
                    current_assets_turnover_rate, inventory_turnover_rate,
                    accounts_receivables_turnover_rate,
                    current_ratio, quick_ratio, debt_equity_ratio,
                    interest_cover, roic, roa_ebit_ttm,
                    total_shares, a_floats
                FROM fundamentals
                WHERE symbol = '{symbol_escaped}'
                ORDER BY date
            ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
        """)

    def _export_valuation_enriched(
        self, symbol_escaped: str, output_file: Path
    ) -> None:
        """
        Export valuation data with enriched fields:
        - total_shares, a_floats: forward filled from fundamentals
        - total_value, float_value: market cap computed as shares * close
        - roe, roa, roe_ttm, roa_ttm: forward filled from fundamentals
        - naps: calculated as close / pb (derived from pbMRQ definition)

        Uses LAST_VALUE with IGNORE NULLS for forward fill.
        """
        self.conn.execute(f"""
            COPY (
                SELECT
                    v.date::TIMESTAMP_NS AS date,
                    v.pe_ttm, v.pb, v.ps_ttm, v.pcf,
                    f.roe, f.roe_ttm, f.roa, f.roa_ttm,
                    CASE WHEN v.pb > 0 THEN ROUND(s.close / v.pb, 4)
                         ELSE NULL END AS naps,
                    f.total_shares, f.a_floats,
                    CASE WHEN f.total_shares > 0 AND s.close IS NOT NULL
                         THEN ROUND(f.total_shares * s.close, 2) END AS total_value,
                    CASE WHEN f.a_floats > 0 AND s.close IS NOT NULL
                         THEN ROUND(f.a_floats * s.close, 2) END AS float_value,
                    v.turnover_rate
                FROM valuation v
                ASOF JOIN stocks s
                    ON v.symbol = s.symbol AND v.date >= s.date
                LEFT JOIN LATERAL (
                    SELECT total_shares, a_floats, roe, roe_ttm, roa, roa_ttm
                    FROM fundamentals f2
                    WHERE f2.symbol = v.symbol AND f2.date <= v.date
                    ORDER BY f2.date DESC LIMIT 1
                ) f ON TRUE
                WHERE v.symbol = '{symbol_escaped}'
                ORDER BY v.date
            ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
        """)

    def _ensure_stock_metadata_from_pool(self) -> None:
        """Populate stock_metadata with accurate listed/de_listed dates.

        Uses MIN(date) from stocks table as listed_date (99.8% match with
        actual IPO dates). Sets de_listed_date to '2900-01-01' for active
        stocks, or MAX(date) for stocks no longer in the mootdx stock list.
        """
        # Check current stock_metadata quality
        current_count = self.conn.execute(
            "SELECT COUNT(*) FROM stock_metadata"
        ).fetchone()[0]
        valid_delisted_count = self.conn.execute(
            "SELECT COUNT(*) FROM stock_metadata "
            "WHERE de_listed_date IS NOT NULL AND de_listed_date != ''"
        ).fetchone()[0]
        pool_count = self.conn.execute(
            "SELECT COUNT(*) FROM stock_pool"
        ).fetchone()[0]

        needs_population = (
            current_count < pool_count or valid_delisted_count < 100
        )

        if not needs_population:
            return

        logger.info(
            f"Populating stock_metadata: {current_count} records, "
            f"{valid_delisted_count} with de_listed_date, pool has {pool_count}"
        )

        # Step 1: Get stock names from mootdx (current active stocks)
        active_stock_names = {}
        try:
            from mootdx.quotes import Quotes

            client = Quotes.factory(market="std", quiet=True)
            sz_prefixes = ("000", "001", "002", "003", "300", "301", "302")
            sh_prefixes = ("600", "601", "603", "605", "688", "689")

            for market in [0, 1]:  # 0=SZ, 1=SH
                try:
                    df = client.stocks(market=market)
                    if df is None or df.empty:
                        continue
                    for _, row in df.iterrows():
                        code = str(row.get("code", "")).strip()
                        name = str(row.get("name", "")).strip()
                        if not code or len(code) != 6:
                            continue
                        if market == 1 and code.startswith(("000", "399", "999")):
                            continue
                        if market == 0 and (
                            code.startswith(
                                ("15", "16", "50", "51", "52", "56", "58", "59")
                            )
                            or code.startswith("39")
                        ):
                            continue
                        if market == 0 and code.startswith(sz_prefixes):
                            active_stock_names[f"{code}.SZ"] = name
                        elif market == 1 and code.startswith(sh_prefixes):
                            active_stock_names[f"{code}.SS"] = name
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch stocks for market {market}: {e}"
                    )
            logger.info(f"Fetched {len(active_stock_names)} stock names from mootdx")
        except Exception as e:
            logger.warning(f"Failed to fetch stock names: {e}")

        # Step 2: Get listed_date from MIN(date) in stocks table
        # This matches actual IPO dates with 99.8% accuracy
        # Filter to A-share codes only (exclude indices, B-shares, etc.)
        stock_dates = {}
        try:
            dates_df = self.conn.execute(
                "SELECT symbol, MIN(date) as listed_date, MAX(date) as last_date "
                "FROM stocks "
                "WHERE (symbol LIKE '000___.SZ' OR symbol LIKE '001___.SZ' "
                "    OR symbol LIKE '002___.SZ' OR symbol LIKE '003___.SZ' "
                "    OR symbol LIKE '300___.SZ' OR symbol LIKE '301___.SZ' "
                "    OR symbol LIKE '302___.SZ' "
                "    OR symbol LIKE '600___.SS' OR symbol LIKE '601___.SS' "
                "    OR symbol LIKE '603___.SS' OR symbol LIKE '605___.SS' "
                "    OR symbol LIKE '688___.SS' OR symbol LIKE '689___.SS') "
                "GROUP BY symbol"
            ).fetchdf()
            for _, row in dates_df.iterrows():
                stock_dates[row["symbol"]] = {
                    "listed_date": str(row["listed_date"]),
                    "last_date": str(row["last_date"]),
                }
            logger.info(
                f"Got listed dates from stocks table for {len(stock_dates)} symbols"
            )
        except Exception as e:
            logger.warning(f"Failed to get dates from stocks table: {e}")

        # Step 3: Determine de_listed_date and build batch
        # Active in mootdx → '2900-01-01'
        # Not active and last_date < latest → last_date (likely delisted)
        active_set = set(active_stock_names.keys())
        latest_date_str = str(
            self.conn.execute("SELECT MAX(date) FROM stocks").fetchone()[0]
        )

        # Pre-load existing blocks to preserve them
        existing_blocks = {}
        try:
            blocks_df = self.conn.execute(
                "SELECT symbol, blocks FROM stock_metadata WHERE blocks IS NOT NULL"
            ).fetchdf()
            for _, row in blocks_df.iterrows():
                existing_blocks[row["symbol"]] = row["blocks"]
        except Exception:
            pass

        # Build batch rows
        all_symbols = set(stock_dates.keys()) | active_set
        rows = []
        for symbol in all_symbols:
            dates = stock_dates.get(symbol, {})
            listed_date = dates.get("listed_date")
            last_date = dates.get("last_date")

            if symbol in active_set:
                de_listed_date = "2900-01-01"
            elif last_date and last_date < latest_date_str:
                de_listed_date = last_date
            else:
                de_listed_date = "2900-01-01"

            rows.append((
                symbol,
                active_stock_names.get(symbol),
                listed_date,
                de_listed_date,
                existing_blocks.get(symbol),
            ))

        # Batch insert via temp table
        batch_df = pd.DataFrame(
            rows,
            columns=["symbol", "stock_name", "listed_date", "de_listed_date", "blocks"],
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO stock_metadata "
            "SELECT * FROM batch_df"
        )

        logger.info(
            f"stock_metadata population complete: {len(rows)} records, "
            f"{len(active_set)} active stocks"
        )

    def _enrich_halt_status_from_volume(self) -> None:
        """Enrich stock_status HALT entries using volume=0 from stocks table.

        For each trading date, marks A-share stocks with volume=0 (within
        their lifespan) as HALT. Merges with any existing BaoStock-sourced
        HALT data already in the stock_status table.
        """
        import time
        t0 = time.time()

        # Use a temp table to avoid complex INSERT OR REPLACE with aggregation
        self.conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _halt_enriched AS
            WITH lifespans AS (
                SELECT symbol, MIN(date) AS first_date, MAX(date) AS last_date
                FROM stocks
                WHERE {self._CN_STOCK_FILTER}
                GROUP BY symbol
            ),
            trade_dates AS (
                SELECT DISTINCT date FROM stocks
            ),
            vol_halted AS (
                SELECT td.date, ls.symbol
                FROM trade_dates td
                CROSS JOIN lifespans ls
                LEFT JOIN stocks s ON s.symbol = ls.symbol AND s.date = td.date
                WHERE td.date >= ls.first_date AND td.date <= ls.last_date
                  AND (s.volume IS NULL OR s.volume = 0)
            ),
            existing_halt AS (
                SELECT STRPTIME(date, '%Y%m%d')::DATE AS date,
                       unnest(symbols::JSON::VARCHAR[]) AS symbol
                FROM stock_status
                WHERE status_type = 'HALT'
            ),
            combined AS (
                SELECT date, symbol FROM vol_halted
                UNION
                SELECT date, symbol FROM existing_halt
            )
            SELECT
                STRFTIME(date, '%Y%m%d') AS date,
                'HALT' AS status_type,
                to_json(list(symbol ORDER BY symbol)) AS symbols
            FROM combined
            GROUP BY date
        """)

        # Replace existing HALT entries
        self.conn.execute(
            "DELETE FROM stock_status WHERE status_type = 'HALT'"
        )
        self.conn.execute("""
            INSERT INTO stock_status (date, status_type, symbols)
            SELECT date, status_type, symbols FROM _halt_enriched
        """)
        self.conn.execute("DROP TABLE IF EXISTS _halt_enriched")

        halt_count = self.conn.execute(
            "SELECT COUNT(*) FROM stock_status WHERE status_type = 'HALT'"
        ).fetchone()[0]
        logger.info(
            f"Enriched HALT status from volume data: "
            f"{halt_count} date entries in {time.time() - t0:.1f}s"
        )

    def _export_metadata(self, output_dir: Path, market: str = "cn") -> None:
        """Export metadata tables using DuckDB COPY"""

        # Before exporting stock_metadata, ensure it's populated from stock_pool
        # This ensures all A-shares are included, not just those with downloaded data
        self._ensure_stock_metadata_from_pool()

        # stock_metadata.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM stock_metadata").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY stock_metadata TO '{output_dir / "stock_metadata.parquet"}'
                (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # benchmark.parquet — prefer stocks table for full history,
        # fall back to benchmark table for recent data only
        benchmark_symbol = '000300.SS'
        has_stocks_benchmark = self.conn.execute(
            f"SELECT COUNT(*) FROM stocks WHERE symbol = '{benchmark_symbol}'"
        ).fetchone()[0]

        if has_stocks_benchmark > 0:
            self.conn.execute(f"""
                COPY (
                    SELECT date, open, high, low, close, volume,
                           COALESCE(money, 0.0) AS money
                    FROM stocks
                    WHERE symbol = '{benchmark_symbol}'
                    ORDER BY date
                ) TO '{output_dir / "benchmark.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)
        else:
            count = self.conn.execute(
                "SELECT COUNT(*) FROM benchmark"
            ).fetchone()[0]
            if count > 0:
                self.conn.execute(f"""
                    COPY (SELECT * FROM benchmark ORDER BY date)
                    TO '{output_dir / "benchmark.parquet"}'
                    (FORMAT PARQUET, CODEC 'ZSTD')
                """)

        # trade_days.parquet — merge DB trade_days with dates from stocks table
        # The trade_days table may only have recent dates (from mootdx),
        # but stocks table has full history back to 1991.
        self.conn.execute(f"""
            COPY (
                SELECT DISTINCT date FROM (
                    SELECT date FROM trade_days
                    UNION
                    SELECT DISTINCT date FROM stocks
                ) ORDER BY date
            ) TO '{output_dir / "trade_days.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
        """)

        # index_constituents.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM index_constituents").fetchone()[
            0
        ]
        if count > 0:
            self.conn.execute(f"""
                COPY (
                    SELECT date, index_code, symbols::JSON::VARCHAR[] AS symbols
                    FROM index_constituents
                ) TO '{output_dir / "index_constituents.parquet"}'
                (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # stock_status.parquet — enrich HALT data from volume before export
        self._enrich_halt_status_from_volume()
        count = self.conn.execute("SELECT COUNT(*) FROM stock_status").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY (
                    SELECT date, status_type, symbols::JSON::VARCHAR[] AS symbols
                    FROM stock_status
                ) TO '{output_dir / "stock_status.parquet"}'
                (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # version.parquet
        cn_filter = f"WHERE {self._CN_STOCK_FILTER}" if market == "cn" else ""
        result = self.conn.execute(f"""
            SELECT
                (SELECT MAX(date)::VARCHAR FROM stocks) as version,
                (SELECT COUNT(DISTINCT symbol) FROM stocks {cn_filter}) as num_stocks,
                CURRENT_DATE as export_date,
                (SELECT MIN(date)::VARCHAR FROM stocks) as start_date
        """).fetchone()

        version_data = pd.DataFrame(
            [
                {
                    "version": result[0] or "",
                    "num_stocks": result[1] or 0,
                    "export_date": str(result[2]),
                    "start_date": result[3] or "",
                }
            ]
        )
        version_data.to_parquet(output_dir / "version.parquet", index=False)


    def _write_manifest(self, output_dir: Path, market: str = "cn") -> None:
        """Write manifest.json"""
        cn_filter = f"WHERE {self._CN_STOCK_FILTER}" if market == "cn" else ""
        result = self.conn.execute(f"""
            SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol)
            FROM stocks {cn_filter}
        """).fetchone()

        start_date = str(result[0]) if result[0] else ""
        end_date = str(result[1]) if result[1] else ""
        stock_count = result[2] or 0

        manifest = {
            "version": end_date,
            "date_range": {
                "start": start_date,
                "end": end_date,
            },
            "description": f"SimTradeData export ({stock_count} stocks)",
            "export_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        with open(output_dir / "manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
