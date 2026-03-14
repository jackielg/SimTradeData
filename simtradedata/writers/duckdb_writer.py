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

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = "data/simtradedata.duckdb"


class DuckDBWriter:
    """
    Writer for DuckDB incremental storage

    Features:
    - Automatic upsert via INSERT OR REPLACE (uses PRIMARY KEY)
    - Incremental updates: query MAX(date) to determine start_date
    - Export to PTrade Parquet format
    """

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
            CREATE TABLE IF NOT EXISTS adjust_factors (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL,
                adj_a DOUBLE NOT NULL,
                adj_b DOUBLE DEFAULT 0,
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
            "INSERT OR IGNORE INTO sampling_progress VALUES (?)",
            [sample_date]
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
            self.conn.execute("""
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
            """, [symbol, sample_date, sample_date])

    # ========================================
    # Fundamentals progress tracking
    # ========================================

    def get_existing_fundamental_dates(self, symbol: str) -> set:
        """Get set of existing quarter end dates for a symbol in fundamentals table.

        Returns:
            Set of date strings like {'2024-03-31', '2024-06-30', ...}
        """
        result = self.conn.execute("""
            SELECT DISTINCT date FROM fundamentals WHERE symbol = ?
        """, [symbol]).fetchall()
        return {str(row[0]) for row in result}

    def has_fundamental(self, symbol: str, date_str: str) -> bool:
        """Check if a specific symbol+date exists in fundamentals table."""
        result = self.conn.execute("""
            SELECT 1 FROM fundamentals WHERE symbol = ? AND date = ?
        """, [symbol, date_str]).fetchone()
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
        result = self.conn.execute("""
            SELECT file_hash FROM fundamentals_progress
            WHERE year = ? AND quarter = ?
        """, [year, quarter]).fetchone()
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
        count_result = self.conn.execute("""
            SELECT COUNT(*) FROM fundamentals WHERE date = ?
        """, [date_str]).fetchone()
        count = count_result[0] if count_result else 0

        self.conn.execute("""
            DELETE FROM fundamentals WHERE date = ?
        """, [date_str])

        # Also delete the progress record
        self.conn.execute("""
            DELETE FROM fundamentals_progress WHERE year = ? AND quarter = ?
        """, [year, quarter])

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
        self.conn.execute("""
            INSERT OR REPLACE INTO fundamentals_progress
                (year, quarter, stock_count, filename, file_hash)
            VALUES (?, ?, ?, ?, ?)
        """, [year, quarter, stock_count, filename, file_hash])

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
            "symbol", "date", "open", "close", "high", "low",
            "high_limit", "low_limit", "preclose", "volume", "money",
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
            "symbol", "date", "pe_ttm", "pb", "ps_ttm", "pcf",
            "roe", "roe_ttm", "roa", "roa_ttm", "naps",
            "total_shares", "a_floats", "turnover_rate",
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
            "symbol", "date", "publ_date",
            "operating_revenue_grow_rate", "net_profit_grow_rate",
            "basic_eps_yoy", "np_parent_company_yoy",
            "net_profit_ratio", "net_profit_ratio_ttm",
            "gross_income_ratio", "gross_income_ratio_ttm",
            "roa", "roa_ttm", "roe", "roe_ttm",
            "total_asset_grow_rate", "total_asset_turnover_rate",
            "current_assets_turnover_rate", "inventory_turnover_rate",
            "accounts_receivables_turnover_rate",
            "current_ratio", "quick_ratio", "debt_equity_ratio",
            "interest_cover", "roic", "roa_ebit_ttm",
            "total_shares", "a_floats",
        ]
        available = [c for c in columns if c in df.columns]
        df = df[available]

        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO fundamentals ({cols_str})
            SELECT {cols_str} FROM df
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
            "symbol", "date", "allotted_ps", "rationed_ps",
            "rationed_px", "bonus_ps", "dividend",
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
        """Write adjust factors with upsert"""
        if isinstance(data, pd.Series):
            df = data.reset_index()
            df.columns = ["date", "adj_a"]
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
                if "index" in df.columns:
                    df = df.rename(columns={"index": "date"})
        else:
            return 0

        if df.empty:
            return 0

        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date

        if "backAdjustFactor" in df.columns:
            df["adj_a"] = df["backAdjustFactor"]

        if "adj_b" not in df.columns:
            df["adj_b"] = 0.0

        df = df[["symbol", "date", "adj_a", "adj_b"]]

        self.conn.execute("""
            INSERT OR REPLACE INTO adjust_factors
            SELECT * FROM df
        """)

        logger.debug(f"Wrote {len(df)} adjust factor rows for {symbol}")
        return len(df)

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

        self.conn.execute("""
            INSERT OR REPLACE INTO index_constituents (date, index_code, symbols)
            VALUES (?, ?, ?)
        """, [date, index_code, symbols_json])

    def write_stock_status(
        self, date: str, status_type: str, symbols: List[str]
    ) -> None:
        """Write stock status for a specific date"""
        symbols_json = json.dumps(symbols, ensure_ascii=False)

        self.conn.execute("""
            INSERT OR REPLACE INTO stock_status (date, status_type, symbols)
            VALUES (?, ?, ?)
        """, [date, status_type, symbols_json])

    def write_money_flow(self, symbol: str, df: pd.DataFrame) -> int:
        """Write money flow data with upsert."""
        if df.empty:
            return 0
        df = df.copy()
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date
        columns = ["symbol", "date", "net_main", "net_super", "net_large", "net_medium", "net_small"]
        available = [c for c in columns if c in df.columns]
        df = df[available]
        cols_str = ", ".join(available)
        self.conn.execute(f"INSERT OR REPLACE INTO money_flow ({cols_str}) SELECT {cols_str} FROM df")
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
        self.conn.execute(f"INSERT OR REPLACE INTO lhb ({cols_str}) SELECT {cols_str} FROM df")
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
        self.conn.execute(f"INSERT OR REPLACE INTO margin_trading ({cols_str}) SELECT {cols_str} FROM df")
        return len(df)

    def write_global_metadata(self, meta: pd.Series) -> None:
        """Write global metadata to version_info table"""
        for key, value in meta.items():
            self.conn.execute("""
                INSERT OR REPLACE INTO version_info (key, value)
                VALUES (?, ?)
            """, [str(key), str(value)])

    # ========================================
    # Incremental update helpers
    # ========================================

    def get_max_date(self, table: str, symbol: str = None) -> Optional[str]:
        """Get maximum date for incremental update"""
        if symbol:
            result = self.conn.execute(f"""
                SELECT MAX(date) FROM {table} WHERE symbol = ?
            """, [symbol]).fetchone()
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
            result = self.conn.execute(f"""
                SELECT MIN(date) FROM {table} WHERE symbol = ?
            """, [symbol]).fetchone()
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
        for table in ["stocks", "valuation", "fundamentals", "exrights", "adjust_factors"]:
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
        - roe_ttm = rolling sum of roe over 4 quarters
        - roa_ttm = rolling sum of roa over 4 quarters
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

        # roe_ttm / roa_ttm = rolling 4-quarter sum
        self.conn.execute("""
            UPDATE fundamentals f
            SET
                roe_ttm = sub.roe_ttm,
                roa_ttm = sub.roa_ttm
            FROM (
                SELECT
                    symbol, date,
                    SUM(roe) OVER w AS roe_ttm,
                    SUM(roa) OVER w AS roa_ttm
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
        self._export_per_symbol_table("stocks", output_path / "stocks", market=market)

        logger.info("Exporting exrights...")
        self._export_per_symbol_table("exrights", output_path / "exrights", market=market)

        logger.info("Exporting fundamentals...")
        self._export_per_symbol_table("fundamentals", output_path / "fundamentals", market=market)

        logger.info("Exporting valuation...")
        self._export_per_symbol_table("valuation", output_path / "valuation", market=market)

        logger.info("Exporting metadata...")
        self._export_metadata(output_path / "metadata")

        logger.info("Exporting adjust factors...")
        self._export_adjust_factors(output_path)

        self._write_manifest(output_path)

        logger.info(f"Export complete: {output_path}")

    def _export_per_symbol_table(self, table: str, output_dir: Path, market: str = "cn") -> None:
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
                self._export_stocks_with_limits(symbol_escaped, output_file, market=market)
            elif table == "fundamentals":
                # Calculate TTM indicators during export
                self._export_fundamentals_with_ttm(symbol_escaped, output_file)
            elif table == "valuation":
                # Enrich with total_shares/a_floats from fundamentals
                self._export_valuation_enriched(symbol_escaped, output_file)
            else:
                self.conn.execute(f"""
                    COPY (
                        SELECT * EXCLUDE (symbol) FROM {table}
                        WHERE symbol = '{symbol_escaped}'
                        ORDER BY date
                    ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
                """)

        logger.info(f"Exported {len(symbols)} {table} files")

    def _export_stocks_with_limits(self, symbol_escaped: str, output_file: Path, market: str = "cn") -> None:
        """
        Export stocks data with calculated price limits

        Price limit rules:
        - US stocks: no price limits (NULL)
        - Normal stocks: ±10%
        - ST stocks: ±5%
        - ChiNext (300xxx, 301xxx) / STAR (688xxx, 689xxx): ±20% after 2020-08-24
        """
        if market == "us":
            self.conn.execute(f"""
                COPY (
                    SELECT date, open, close, high, low,
                        NULL AS high_limit, NULL AS low_limit,
                        preclose, volume, money
                    FROM stocks
                    WHERE symbol = '{symbol_escaped}'
                    ORDER BY date
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
                    SELECT
                        date, open, close, high, low,
                        CASE
                            WHEN date >= DATE '2020-08-24' THEN ROUND(preclose * 1.20, 2)
                            ELSE ROUND(preclose * 1.10, 2)
                        END AS high_limit,
                        CASE
                            WHEN date >= DATE '2020-08-24' THEN ROUND(preclose * 0.80, 2)
                            ELSE ROUND(preclose * 0.90, 2)
                        END AS low_limit,
                        preclose, volume, money
                    FROM stocks
                    WHERE symbol = '{symbol_escaped}'
                    ORDER BY date
                ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)
        else:
            # Normal stocks: 10% limit (ST handling needs isST from status)
            # For now, use 10% as default; ST detection could be added later
            self.conn.execute(f"""
                COPY (
                    SELECT
                        date, open, close, high, low,
                        ROUND(preclose * 1.10, 2) AS high_limit,
                        ROUND(preclose * 0.90, 2) AS low_limit,
                        preclose, volume, money
                    FROM stocks
                    WHERE symbol = '{symbol_escaped}'
                    ORDER BY date
                ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)

    def _export_fundamentals_with_ttm(
        self, symbol_escaped: str, output_file: Path
    ) -> None:
        """
        Export fundamentals data with TTM indicators calculated

        TTM (Trailing Twelve Months) is calculated as 4-quarter rolling average
        for ratio fields: roe, roa, net_profit_ratio, gross_income_ratio
        """
        self.conn.execute(f"""
            COPY (
                SELECT
                    date, publ_date,
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
                    roa,
                    AVG(roa) OVER (
                        ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS roa_ttm,
                    roe,
                    AVG(roe) OVER (
                        ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS roe_ttm,
                    total_asset_grow_rate, total_asset_turnover_rate,
                    current_assets_turnover_rate, inventory_turnover_rate,
                    accounts_receivables_turnover_rate,
                    current_ratio, quick_ratio, debt_equity_ratio,
                    interest_cover, roic, roa_ebit_ttm
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
        - roe, roa, roe_ttm, roa_ttm: forward filled from fundamentals
        - naps: calculated as close / pb (derived from pbMRQ definition)

        Uses LAST_VALUE with IGNORE NULLS for forward fill.
        """
        self.conn.execute(f"""
            COPY (
                WITH daily_data AS (
                    SELECT
                        v.date,
                        v.pe_ttm,
                        v.pb,
                        v.ps_ttm,
                        v.pcf,
                        v.turnover_rate,
                        s.close
                    FROM valuation v
                    LEFT JOIN stocks s ON v.symbol = s.symbol AND v.date = s.date
                    WHERE v.symbol = '{symbol_escaped}'
                ),
                quarterly_data AS (
                    SELECT
                        date,
                        roe, roa, roe_ttm, roa_ttm,
                        total_shares, a_floats
                    FROM fundamentals
                    WHERE symbol = '{symbol_escaped}'
                ),
                combined AS (
                    SELECT
                        d.date,
                        d.pe_ttm, d.pb, d.ps_ttm, d.pcf,
                        d.turnover_rate,
                        d.close,
                        q.roe AS q_roe,
                        q.roa AS q_roa,
                        q.roe_ttm AS q_roe_ttm,
                        q.roa_ttm AS q_roa_ttm,
                        q.total_shares AS q_total_shares,
                        q.a_floats AS q_a_floats
                    FROM daily_data d
                    LEFT JOIN quarterly_data q ON d.date = q.date
                )
                SELECT
                    date,
                    pe_ttm, pb, ps_ttm, pcf,
                    LAST_VALUE(q_roe IGNORE NULLS) OVER w AS roe,
                    LAST_VALUE(q_roe_ttm IGNORE NULLS) OVER w AS roe_ttm,
                    LAST_VALUE(q_roa IGNORE NULLS) OVER w AS roa,
                    LAST_VALUE(q_roa_ttm IGNORE NULLS) OVER w AS roa_ttm,
                    CASE WHEN pb > 0 THEN ROUND(close / pb, 4) ELSE NULL END AS naps,
                    LAST_VALUE(q_total_shares IGNORE NULLS) OVER w AS total_shares,
                    LAST_VALUE(q_a_floats IGNORE NULLS) OVER w AS a_floats,
                    turnover_rate
                FROM combined
                WINDOW w AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                ORDER BY date
            ) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')
        """)

    def _export_metadata(self, output_dir: Path) -> None:
        """Export metadata tables using DuckDB COPY"""
        # stock_metadata.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM stock_metadata").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY stock_metadata TO '{output_dir / "stock_metadata.parquet"}'
                (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # benchmark.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM benchmark").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY (SELECT * FROM benchmark ORDER BY date)
                TO '{output_dir / "benchmark.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # trade_days.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM trade_days").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY (SELECT * FROM trade_days ORDER BY date)
                TO '{output_dir / "trade_days.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # index_constituents.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM index_constituents").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY index_constituents TO '{output_dir / "index_constituents.parquet"}'
                (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # stock_status.parquet
        count = self.conn.execute("SELECT COUNT(*) FROM stock_status").fetchone()[0]
        if count > 0:
            self.conn.execute(f"""
                COPY stock_status TO '{output_dir / "stock_status.parquet"}'
                (FORMAT PARQUET, CODEC 'ZSTD')
            """)

        # version.parquet
        result = self.conn.execute("""
            SELECT
                (SELECT MAX(date)::VARCHAR FROM stocks) as version,
                (SELECT COUNT(DISTINCT symbol) FROM stocks) as num_stocks,
                CURRENT_DATE as export_date,
                (SELECT MIN(date)::VARCHAR FROM stocks) as start_date
        """).fetchone()

        version_data = pd.DataFrame([{
            "version": result[0] or "",
            "num_stocks": result[1] or 0,
            "export_date": str(result[2]),
            "start_date": result[3] or "",
        }])
        version_data.to_parquet(output_dir / "version.parquet", index=False)

    def _export_adjust_factors(self, output_dir: Path) -> None:
        """Export adjust factors to pre/post files using DuckDB COPY"""
        count = self.conn.execute("SELECT COUNT(*) FROM adjust_factors").fetchone()[0]
        if count == 0:
            logger.info("No adjust factors to export")
            return

        # ptrade_adj_pre.parquet (backward adjust = pre-adjust)
        self.conn.execute(f"""
            COPY (
                SELECT date, symbol, adj_a, adj_b
                FROM adjust_factors
                ORDER BY date, symbol
            ) TO '{output_dir / "ptrade_adj_pre.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
        """)

        # ptrade_adj_post.parquet (same data for now)
        self.conn.execute(f"""
            COPY (
                SELECT date, symbol, adj_a, adj_b
                FROM adjust_factors
                ORDER BY date, symbol
            ) TO '{output_dir / "ptrade_adj_post.parquet"}' (FORMAT PARQUET, CODEC 'ZSTD')
        """)

    def _write_manifest(self, output_dir: Path) -> None:
        """Write manifest.json"""
        result = self.conn.execute("""
            SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol)
            FROM stocks
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
