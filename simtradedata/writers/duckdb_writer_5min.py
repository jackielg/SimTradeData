# -*- coding: utf-8 -*-
"""
DuckDB writer extension for 5-minute data

This module extends DuckDBWriter to support 5-minute K-line data storage.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from simtradedata.writers.duckdb_writer import DuckDBWriter, DEFAULT_DB_PATH

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH_5MIN = "data/simtradedata_5min.duckdb"


class DuckDBWriter5Min(DuckDBWriter):
    """
    Writer for 5-minute data DuckDB storage
    
    Extends DuckDBWriter with 5-minute data table support.
    Uses a separate database file for 5-minute data to avoid
    mixing with daily data.
    
    Features:
    - Automatic upsert via INSERT OR REPLACE
    - Incremental updates
    - Export to Parquet format
    """
    
    def __init__(self, db_path: str = DEFAULT_DB_PATH_5MIN):
        """
        Initialize 5-minute data writer
        
        Args:
            db_path: Path to DuckDB database file
        """
        # Don't call parent __init__, we need custom schema
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema_5min()
        
        logger.info(f"DuckDBWriter5Min initialized: {self.db_path}")
    
    def _init_schema_5min(self) -> None:
        """Initialize 5-minute data schema"""
        # 5-minute OHLCV data table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks_5min (
                symbol VARCHAR NOT NULL,
                datetime TIMESTAMP NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                money DOUBLE,
                PRIMARY KEY (symbol, datetime)
            )
        """)
        
        # Create index for faster queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stocks_5min_symbol_datetime
            ON stocks_5min (symbol, datetime DESC)
        """)
        
        # Progress tracking table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS download_progress (
                symbol VARCHAR PRIMARY KEY,
                last_datetime TIMESTAMP,
                record_count INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Version info
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS version_info (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        
        # Initialize version
        self.conn.execute("""
            INSERT OR REPLACE INTO version_info VALUES ('version', '1.0.0')
        """)
        self.conn.execute("""
            INSERT OR REPLACE INTO version_info VALUES ('data_type', '5min')
        """)
    
    def write_5min_data(self, symbol: str, df: pd.DataFrame) -> int:
        """
        Write 5-minute data with automatic upsert
        
        Args:
            symbol: Stock code in PTrade format (e.g., '000001.SZ')
            df: DataFrame with columns [open, high, low, close, volume, money]
                Index should be datetime
        
        Returns:
            Number of rows written
        """
        if df.empty:
            return 0
        
        df = df.copy()
        
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'datetime' in df.columns:
                df.set_index('datetime', inplace=True)
            else:
                logger.error(f"DataFrame must have datetime index or 'datetime' column")
                return 0
        
        # Reset index to get datetime as column
        df = df.reset_index()
        df['symbol'] = symbol
        
        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume', 'money']
        for col in required_cols:
            if col not in df.columns:
                logger.warning(f"Missing column '{col}' for {symbol}, filling with NaN")
                df[col] = float('nan')
        
        # Select only Needed columns
        columns = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'money']
        available = [c for c in columns if c in df.columns]
        df = df[available]
        
        # Write data
        cols_str = ", ".join(available)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO stocks_5min ({cols_str})
            SELECT {cols_str} FROM df
        """)
        
        # Update progress
        last_datetime = df['datetime'].max()
        record_count = len(df)
        self.conn.execute("""
            INSERT OR REPLACE INTO download_progress (symbol, last_datetime, record_count, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, [symbol, last_datetime, record_count])
        
        logger.debug(f"Wrote {len(df)} 5min rows for {symbol}")
        return len(df)
    
    def get_last_datetime(self, symbol: str) -> Optional[datetime]:
        """
        Get the last datetime for a symbol
        
        Args:
            symbol: Stock code
        
        Returns:
            Last datetime or None if no data
        """
        result = self.conn.execute("""
            SELECT MAX(datetime) FROM stocks_5min WHERE symbol = ?
        """, [symbol]).fetchone()
        
        return result[0] if result and result[0] else None
    
    def get_downloaded_symbols(self) -> list:
        """
        Get list of symbols that have been downloaded
        
        Returns:
            List of stock codes
        """
        result = self.conn.execute("""
            SELECT DISTINCT symbol FROM stocks_5min ORDER BY symbol
        """).fetchall()
        
        return [row[0] for row in result]
    
    def read_5min_data(
        self,
        symbol: str,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Read 5-minute data for a symbol
        
        Args:
            symbol: Stock code
            start_datetime: Start datetime (inclusive)
            end_datetime: End datetime (inclusive)
        
        Returns:
            DataFrame with datetime index
        """
        query = "SELECT * FROM stocks_5min WHERE symbol = ?"
        params = [symbol]
        
        if start_datetime:
            query += " AND datetime >= ?"
            params.append(start_datetime)
        
        if end_datetime:
            query += " AND datetime <= ?"
            params.append(end_datetime)
        
        query += " ORDER BY datetime"
        
        df = self.conn.execute(query, params).fetchdf()
        
        if df.empty:
            return pd.DataFrame()
        
        # Set datetime as index
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        
        return df
    
    def export_to_parquet(
        self,
        output_dir: str,
        symbol: Optional[str] = None
    ) -> int:
        """
        Export 5-minute data to Parquet files
        
        Args:
            output_dir: Output directory path
            symbol: If specified, export only this symbol; otherwise export all
        
        Returns:
            Number of files exported
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        if symbol:
            symbols = [symbol]
        else:
            symbols = self.get_downloaded_symbols()
        
        exported_count = 0
        
        for sym in symbols:
            df = self.read_5min_data(sym)
            
            if df.empty:
                continue
            
            # Remove symbol column (it's in the filename)
            if 'symbol' in df.columns:
                df = df.drop('symbol', axis=1)
            
            # Save to parquet
            output_file = output_path / f"{sym}.parquet"
            df.to_parquet(output_file)
            
            logger.info(f"Exported {sym}: {len(df)} rows to {output_file}")
            exported_count += 1
        
        logger.info(f"Exported {exported_count} files to {output_dir}")
        return exported_count
    
    def get_stats(self) -> dict:
        """
        Get database statistics
        
        Returns:
            Dict with statistics
        """
        # Total records
        total_result = self.conn.execute(
            "SELECT COUNT(*) FROM stocks_5min"
        ).fetchone()
        total_records = total_result[0] if total_result else 0
        
        # Unique symbols
        symbols_result = self.conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM stocks_5min"
        ).fetchone()
        unique_symbols = symbols_result[0] if symbols_result else 0
        
        # Date range
        date_range_result = self.conn.execute(
            "SELECT MIN(datetime), MAX(datetime) FROM stocks_5min"
        ).fetchone()
        
        return {
            'total_records': total_records,
            'unique_symbols': unique_symbols,
            'min_datetime': date_range_result[0] if date_range_result else None,
            'max_datetime': date_range_result[1] if date_range_result else None,
            'db_path': str(self.db_path),
        }
