# -*- coding: utf-8 -*-
"""
BaoStock 5-minute data fetcher

专门用于获取 5 分钟 K 线数据，支持按日期范围查询
"""

import logging
import threading
from datetime import datetime
from typing import List, Optional

import baostock as bs
import pandas as pd

from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.utils.code_utils import convert_from_ptrade_code, retry_on_failure

logger = logging.getLogger(__name__)

# 5 分钟数据字段
MINUTE_FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"

# Timeout configuration
DEFAULT_TIMEOUT = 30  # seconds


class BaoStock5MinFetcher(BaoStockFetcher):
    """
    Fetch 5-minute K-line data from BaoStock API

    Features:
    - Query by date range (start_date to end_date)
    - Unadjusted data (adjustflag="3")
    - Convert to PTrade format
    """

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        """Initialize 5-minute fetcher

        Args:
            timeout: Timeout in seconds for each query (default: 30)
        """
        super().__init__()
        self.timeout = timeout
        logger.info(f"BaoStock5MinFetcher initialized with timeout={timeout}s")

    def _execute_with_timeout(self, func, *args, **kwargs):
        """
        Execute a function with timeout protection

        Args:
            func: Function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result from func

        Raises:
            TimeoutError: If function execution exceeds timeout
        """
        result = [None]
        exception = [None]

        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(self.timeout)

        if thread.is_alive():
            logger.warning(f"Function execution timeout after {self.timeout} seconds")
            raise TimeoutError(f"Operation timed out after {self.timeout} seconds")

        if exception[0]:
            raise exception[0]

        return result[0]

    @retry_on_failure()
    def fetch_5min_bars(
        self, symbol: str, start_date: str, end_date: str, adjustflag: str = "3"
    ) -> pd.DataFrame:
        """
        Fetch 5-minute K-line data for a stock

        Args:
            symbol: Stock code in PTrade format (e.g., '000001.SZ')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            adjustflag: Adjustment flag (1=backward, 2=forward, 3=unadjusted)

        Returns:
            DataFrame with columns:
            - datetime: Timestamp (index)
            - open, high, low, close: OHLC prices
            - volume: Trading volume
            - money: Trading amount (renamed from 'amount')
            - symbol: PTrade format stock code
        """
        self._ensure_login()

        # Convert to BaoStock format
        bs_code = convert_from_ptrade_code(symbol, "baostock")

        logger.info(
            f"Fetching 5min data for {symbol} ({bs_code}): {start_date} ~ {end_date}"
        )

        try:
            # Execute query with timeout protection
            rs = self._execute_with_timeout(
                bs.query_history_k_data_plus,
                code=bs_code,
                fields=MINUTE_FIELDS,
                start_date=start_date,
                end_date=end_date,
                frequency="5",
                adjustflag=adjustflag,
            )

            if rs.error_code != "0":
                logger.error(f"Query failed for {symbol}: {rs.error_msg}")
                return pd.DataFrame()

            # Fetch all data with timeout
            data_list = []

            def fetch_all_rows():
                while (rs.error_code == "0") & rs.next():
                    data_list.append(rs.get_row_data())

            self._execute_with_timeout(fetch_all_rows)

            if not data_list:
                logger.warning(
                    f"No 5min data for {symbol} in {start_date} ~ {end_date}"
                )
                return pd.DataFrame()

            # Create DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)

            # Convert data types
            numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Build datetime index from time field
            # time format: YYYYMMDDHHMMSSsss (17 digits with milliseconds)
            # Truncate to 14 digits (YYYYMMDDHHMMSS) for parsing
            df["datetime"] = pd.to_datetime(df["time"].str[:14], format="%Y%m%d%H%M%S")

            # Select and rename columns
            result = pd.DataFrame(
                {
                    "datetime": df["datetime"],
                    "open": df["open"],
                    "high": df["high"],
                    "low": df["low"],
                    "close": df["close"],
                    "volume": df["volume"],
                    "money": df["amount"],  # Rename 'amount' to 'money'
                    "symbol": symbol,
                }
            )

            # Set datetime as index
            result.set_index("datetime", inplace=True)

            logger.info(f"Fetched {len(result)} 5min bars for {symbol}")
            return result

        except Exception as e:
            logger.error(f"Failed to fetch 5min data for {symbol}: {e}")
            raise

    def fetch_multiple_stocks(
        self, symbols: List[str], start_date: str, end_date: str, adjustflag: str = "3"
    ) -> dict:
        """
        Fetch 5-minute data for multiple stocks

        Args:
            symbols: List of stock codes in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            adjustflag: Adjustment flag

        Returns:
            Dict mapping symbol to DataFrame
        """
        results = {}

        for i, symbol in enumerate(symbols):
            logger.info(f"[{i+1}/{len(symbols)}] Downloading {symbol}...")
            try:
                df = self.fetch_5min_bars(symbol, start_date, end_date, adjustflag)
                if not df.empty:
                    results[symbol] = df
            except Exception as e:
                logger.error(f"Failed to download {symbol}: {e}")
                continue

        logger.info(f"Downloaded 5min data for {len(results)}/{len(symbols)} stocks")
        return results

    def get_stock_list(self, date: str = None) -> List[str]:
        """
        Get all A-share stock list

        Args:
            date: Query date (YYYY-MM-DD), default is today

        Returns:
            List of stock codes in BaoStock format (e.g., ['sh.600000', 'sz.000001'])
        """
        self._ensure_login()

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        rs = bs.query_all_stock(day=date)

        if rs.error_code != "0":
            logger.error(f"Failed to get stock list: {rs.error_msg}")
            return []

        stocks = []
        while (rs.error_code == "0") & rs.next():
            row = rs.get_row_data()
            stocks.append(row[0])  # code is first column

        logger.info(f"Got {len(stocks)} stocks")
        return stocks
