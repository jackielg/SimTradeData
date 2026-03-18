# -*- coding: utf-8 -*-
"""
Mootdx data download program with DuckDB storage

Complements download_efficient.py (BaoStock) by fetching data from mootdx:
- Daily K-line data (OHLCV)
- XDXR (ex-rights/ex-dividend) data
- Adjust factors (derived from hfq prices)
- Batch financial data (from Affair ZIP files)
- Trading calendar (derived from index data)
- Benchmark index data

Output: DuckDB database (data/cn.duckdb)
"""

import argparse
import fcntl
import logging
import os
import socket
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path

# Suppress FutureWarning from mootdx using deprecated pandas fillna(method=...)
warnings.filterwarnings("ignore", category=FutureWarning, module="mootdx")

import pandas as pd
from tqdm import tqdm

from simtradedata.config.field_mappings import BENCHMARK_CONFIG
from simtradedata.fetchers.mootdx_unified_fetcher import MootdxUnifiedFetcher
from simtradedata.writers.duckdb_writer import DEFAULT_DB_PATH, DuckDBWriter

# Configuration
OUTPUT_DIR = "data"
LOG_FILE = "data/download_mootdx.log"
LOCK_FILE = "data/.download_mootdx.lock"

# Date range
START_DATE = "2015-01-01"
END_DATE = None  # None means current date

# Batch size for stock processing
BATCH_SIZE = 20

# Ensure log directory exists
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)

# Prevent infinite hangs on TDX server socket recv()
socket.setdefaulttimeout(30)


class ProcessLock:
    """Process lock to prevent multiple instances from running simultaneously"""

    def __init__(self, lock_file: str):
        self.lock_file = Path(lock_file)
        self.lock_fd = None

    def __enter__(self):
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.lock_fd = open(self.lock_file, "w")

        try:
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
        except IOError:
            print("\nError: Another mootdx download process is running")
            print(f"Lock file: {self.lock_file}")
            print(f"\nIf no other process is running, delete the lock file:")
            print(f"  rm {self.lock_file}")
            sys.exit(1)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
            except Exception:
                pass

            try:
                self.lock_file.unlink(missing_ok=True)
            except Exception:
                pass


class MootdxDownloader:
    """Mootdx data downloader with DuckDB storage"""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        skip_fundamentals: bool = False,
        download_dir: str = None,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.unified_fetcher = MootdxUnifiedFetcher(download_dir=download_dir)
        self.writer = DuckDBWriter(db_path=str(self.db_path))

        self.skip_fundamentals = skip_fundamentals
        self.download_dir = download_dir
        self.failed_stocks = []

    def _reconnect(self):
        """Reconnect to TDX server after connection error."""
        logger.warning("Reconnecting to TDX server...")
        try:
            self.unified_fetcher.logout()
        except Exception:
            pass
        try:
            self.unified_fetcher.login()
            logger.info("TDX server reconnected")
        except Exception as e:
            logger.error(f"Failed to reconnect: {e}")

    def get_incremental_start_date(self, symbol: str) -> str:
        """Get next date after MAX(date) for incremental updates."""
        max_date = self.writer.get_max_date("stocks", symbol)
        if max_date:
            next_day = datetime.strptime(max_date, "%Y-%m-%d") + timedelta(days=1)
            return next_day.strftime("%Y-%m-%d")
        return START_DATE

    def download_stock_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> bool:
        """
        Download daily OHLCV + adjust factor + XDXR for a single stock.

        OHLCV uses incremental logic (skip if up to date).
        Adjust factor and XDXR are downloaded if missing for the symbol,
        even when OHLCV is already current (e.g. after TDX bulk import).

        Returns:
            True if any data was downloaded, False if all skipped/failed
        """
        try:
            downloaded = False

            # --- OHLCV: incremental ---
            actual_start = self.get_incremental_start_date(symbol)
            ohlcv_start = max(actual_start, start_date)

            if ohlcv_start <= end_date:
                df = self.unified_fetcher.fetch_daily_data(
                    symbol, ohlcv_start, end_date
                )

                if not df.empty:
                    # Filter out empty rows (halted stocks return rows with all NaN)
                    price_cols = ["open", "high", "low", "close"]
                    available_cols = [c for c in price_cols if c in df.columns]
                    if available_cols:
                        df = df.dropna(subset=available_cols, how="all")

                    if not df.empty:
                        if "date" in df.columns:
                            market_df = df.set_index("date")
                        else:
                            market_df = df

                        if "amount" in market_df.columns:
                            market_df = market_df.rename(
                                columns={"amount": "money"}
                            )

                        self.writer.write_market_data(symbol, market_df)
                        downloaded = True

            # --- Adjust factor: download if missing for this symbol ---
            if not self.writer.get_max_date("adjust_factors", symbol):
                try:
                    adj_df = self.unified_fetcher.fetch_adjust_factor(
                        symbol, start_date, end_date
                    )
                    if not adj_df.empty:
                        adj_series = adj_df.set_index("date")["backAdjustFactor"]
                        self.writer.write_adjust_factor(symbol, adj_series)
                        downloaded = True
                except (socket.timeout, ConnectionError):
                    logger.warning(f"Connection error fetching adjust factor for {symbol}, reconnecting")
                    self._reconnect()
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch adjust factor for {symbol}: {e}"
                    )

            # --- XDXR: download if missing for this symbol ---
            if not self.writer.get_max_date("exrights", symbol):
                try:
                    xdxr_df = self.unified_fetcher.fetch_xdxr(symbol)
                    if not xdxr_df.empty:
                        exrights = self._convert_xdxr_to_exrights(xdxr_df)
                        if not exrights.empty:
                            self.writer.write_exrights(symbol, exrights)
                            downloaded = True
                except (socket.timeout, ConnectionError):
                    logger.warning(f"Connection error fetching XDXR for {symbol}, reconnecting")
                    self._reconnect()
                except Exception as e:
                    logger.warning(f"Failed to fetch XDXR for {symbol}: {e}")

            return downloaded

        except (socket.timeout, ConnectionError):
            logger.warning(f"Connection error downloading {symbol}, reconnecting")
            self.failed_stocks.append(symbol)
            self._reconnect()
            return False
        except Exception as e:
            logger.error(f"Failed to download {symbol}: {e}")
            self.failed_stocks.append(symbol)
            return False

    def _convert_xdxr_to_exrights(self, xdxr_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert mootdx XDXR data to PTrade exrights format.

        Args:
            xdxr_df: Raw XDXR DataFrame from mootdx

        Returns:
            DataFrame with columns: date, allotted_ps, rationed_ps,
                                   rationed_px, bonus_ps, dividend
        """
        if xdxr_df.empty:
            return pd.DataFrame()

        # XDXR category: 1 = ex-rights/ex-dividend
        if "category" in xdxr_df.columns:
            xdxr_df = xdxr_df[xdxr_df["category"] == 1]

        if xdxr_df.empty:
            return pd.DataFrame()

        result = pd.DataFrame()

        # Map XDXR fields to PTrade exrights format
        if "datetime" in xdxr_df.columns:
            result["date"] = pd.to_datetime(xdxr_df["datetime"])
        elif "date" in xdxr_df.columns:
            result["date"] = pd.to_datetime(xdxr_df["date"])
        else:
            return pd.DataFrame()

        # Song gu (bonus shares per share)
        result["bonus_ps"] = pd.to_numeric(
            xdxr_df.get("songzhuangu", 0), errors="coerce"
        ).fillna(0.0)

        # Pei gu (rationed shares per share)
        result["rationed_ps"] = pd.to_numeric(
            xdxr_df.get("peigu", 0), errors="coerce"
        ).fillna(0.0)

        # Pei gu price
        result["rationed_px"] = pd.to_numeric(
            xdxr_df.get("peigujia", 0), errors="coerce"
        ).fillna(0.0)

        # Cash dividend (per share, before tax)
        result["dividend"] = pd.to_numeric(
            xdxr_df.get("fenhong", 0), errors="coerce"
        ).fillna(0.0)

        # Allotted shares (not directly available from mootdx)
        result["allotted_ps"] = 0.0

        return result

    def download_batch(
        self, stock_batch: list, start_date: str, end_date: str, pbar=None
    ) -> int:
        """Download data for a batch of stocks in a single transaction."""
        success_count = 0

        self.writer.begin()
        try:
            for stock in stock_batch:
                try:
                    if self.download_stock_data(stock, start_date, end_date):
                        success_count += 1
                except Exception as e:
                    logger.error(f"Exception downloading {stock}: {e}")
                finally:
                    if pbar:
                        pbar.update(1)

            self.writer.commit()
        except Exception:
            self.writer.rollback()
            raise

        return success_count

    # Number of recent quarters to always re-download (reports may still be
    # updated, and TDX server hashes are unreliable across distributed nodes)
    RECENT_QUARTERS_TO_REFRESH = 4

    def download_fundamentals_batch(
        self, start_date: str, end_date: str,
        refresh: bool = False,
    ) -> None:
        """
        Download batch financial data by quarter with incremental updates.

        Uses Affair API which downloads one ZIP per quarter containing
        all stocks' data - much more efficient than per-stock queries.

        Incremental strategy:
        - Recent quarters (last RECENT_QUARTERS_TO_REFRESH): always re-download
          because financial reports may be updated after initial publication.
        - Historical quarters: skip if already completed.
        - refresh=True: force re-download all quarters.

        TDX server hashes are NOT used for change detection because distributed
        TDX servers return inconsistent hashes for the same file.
        """
        from simtradedata.fetchers.mootdx_affair_fetcher import MootdxAffairFetcher
        from simtradedata.utils.ttm_calculator import get_quarters_in_range

        quarters = get_quarters_in_range(start_date, end_date)
        if not quarters:
            print("  No quarters in date range")
            return

        # Create affair fetcher (same download dir as unified_fetcher)
        affair_fetcher = MootdxAffairFetcher(download_dir=self.download_dir)

        print(f"  Total quarters: {len(quarters)}")
        if refresh:
            print("  Mode: refresh (re-download all quarters)")
        else:
            print(f"  Mode: incremental (refresh last {self.RECENT_QUARTERS_TO_REFRESH} quarters)")

        # Get already completed quarters
        completed_quarters = self.writer.get_completed_fundamental_quarters()

        # Batch fetch remote file info (for availability check)
        try:
            remote_files = affair_fetcher.list_available_reports()
            remote_hash_map = {f.get("filename"): f.get("hash") for f in remote_files}
        except Exception as e:
            logger.warning(f"Failed to fetch remote file list: {e}")
            remote_hash_map = {}

        # Determine which recent quarters should always be refreshed
        recent_quarters = set(quarters[-self.RECENT_QUARTERS_TO_REFRESH:])

        pending = []
        skipped = 0

        for year, quarter in quarters:
            filename = affair_fetcher.get_quarter_filename(year, quarter)
            remote_hash = remote_hash_map.get(filename)

            if remote_hash is None:
                # File not available on server
                logger.info(f"File {filename} not available on TDX server")
                continue

            if refresh:
                # Refresh mode: re-download everything
                pending.append((year, quarter, filename, remote_hash))
                continue

            is_recent = (year, quarter) in recent_quarters
            is_completed = (year, quarter) in completed_quarters

            if is_completed and not is_recent:
                # Historical completed quarter: skip
                skipped += 1
                continue

            # New quarter or recent quarter: download
            pending.append((year, quarter, filename, remote_hash))

        if not pending:
            print(f"  All {len(quarters)} quarters up-to-date ({skipped} completed)")
            return

        print(
            f"  Pending: {len(pending)} (recent: {len(recent_quarters)}), skipped: {skipped}"
        )

        for qi, (year, quarter, filename, remote_hash) in enumerate(pending, 1):
            print(f"\n  Quarter {qi}/{len(pending)}: {year}Q{quarter}")

            try:
                fund_df = self.unified_fetcher.fetch_fundamentals_for_quarter(
                    year, quarter
                )

                if fund_df.empty:
                    logger.warning(f"No fundamentals for {year}Q{quarter}")
                    # Still mark as completed (empty is valid state)
                    self.writer.mark_fundamental_quarter_completed(
                        year, quarter, 0,
                        filename=filename, file_hash=remote_hash or ""
                    )
                    continue

                # Write per-stock fundamentals (INSERT OR REPLACE overwrites
                # existing records safely, no need to delete old data first)
                success_count = 0
                if "code" in fund_df.columns:
                    self.writer.begin()
                    try:
                        for code, group in fund_df.groupby("code"):
                            try:
                                # Convert code to PTrade format
                                from simtradedata.utils.code_utils import (
                                    convert_to_ptrade_code,
                                )
                                ptrade_code = convert_to_ptrade_code(
                                    str(code), "qstock"
                                )

                                write_df = group.drop(columns=["code"])
                                if "end_date" in write_df.columns:
                                    write_df = write_df.sort_values("end_date")
                                    write_df = write_df.set_index("end_date")

                                self.writer.write_fundamentals(ptrade_code, write_df)
                                success_count += 1
                            except Exception as e:
                                logger.warning(
                                    f"Failed to write fundamentals for {code}: {e}"
                                )

                        # Record progress in same transaction
                        self.writer.mark_fundamental_quarter_completed(
                            year, quarter, success_count,
                            filename=filename, file_hash=remote_hash or ""
                        )
                        self.writer.commit()
                        print(f"    Completed: {success_count} stocks")
                    except Exception:
                        self.writer.rollback()
                        raise

            except Exception as e:
                logger.error(f"Failed to download fundamentals {year}Q{quarter}: {e}")


def download_all_data(
    skip_fundamentals: bool = False,
    start_date: str = None,
    download_dir: str = None,
    refresh_fundamentals: bool = False,
):
    """
    Main mootdx download function.

    Auto-incremental: each symbol starts from MAX(date)+1.
    """
    with ProcessLock(LOCK_FILE):
        print("=" * 70)
        print("SimTradeData Download (mootdx Source)")
        print("=" * 70)
        print("Mode: Auto-incremental (queries MAX(date) per symbol)")
        if skip_fundamentals:
            print("Fundamentals: Skipped")
        print("=" * 70)

        # Date range
        end_date = (
            datetime.now().date()
            if END_DATE is None
            else datetime.strptime(END_DATE, "%Y-%m-%d").date()
        )

        use_start_date = start_date or START_DATE
        start_date_str = use_start_date
        end_date_str = end_date.strftime("%Y-%m-%d")

        print(f"\nDate range: {start_date_str} ~ {end_date_str}")

        # Initialize downloader
        db_path = Path(DEFAULT_DB_PATH)
        downloader = MootdxDownloader(
            db_path=str(db_path),
            skip_fundamentals=skip_fundamentals,
            download_dir=download_dir,
        )
        downloader.unified_fetcher.login()

        try:
            # Check if stocks data is already up to date
            global_max_date = downloader.writer.get_max_date("stocks")
            skip_stock_download = False
            extras_stock_pool = None
            if global_max_date:
                # Check if there's any new trading day since global_max_date
                # by fetching a single stock's data for the date range
                try:
                    test_df = downloader.unified_fetcher.fetch_daily_data(
                        "000001.SZ",
                        (datetime.strptime(global_max_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"),
                        end_date_str,
                    )
                except Exception as e:
                    logger.warning(f"Test fetch for new trading days failed: {e}")
                    test_df = pd.DataFrame()

                if test_df.empty:
                    # OHLCV is current (or test fetch failed), check extras coverage
                    stock_count = downloader.writer.conn.execute(
                        "SELECT COUNT(DISTINCT symbol) FROM stocks"
                    ).fetchone()[0]
                    adj_count = downloader.writer.conn.execute(
                        "SELECT COUNT(DISTINCT symbol) FROM adjust_factors"
                    ).fetchone()[0]

                    if adj_count >= stock_count * 0.9:
                        print(f"\nStocks data already up to date (max_date: {global_max_date})")
                        print(f"Adjust factors coverage: {adj_count}/{stock_count} stocks")
                        print("No new trading days since last update, skipping stock download.")
                        skip_stock_download = True
                    else:
                        # Only process stocks missing adjust factors or exrights
                        missing = downloader.writer.conn.execute("""
                            SELECT DISTINCT s.symbol FROM stocks s
                            LEFT JOIN (SELECT DISTINCT symbol FROM adjust_factors) a
                                ON s.symbol = a.symbol
                            LEFT JOIN (SELECT DISTINCT symbol FROM exrights) x
                                ON s.symbol = x.symbol
                            WHERE a.symbol IS NULL OR x.symbol IS NULL
                        """).fetchall()
                        extras_stock_pool = [row[0] for row in missing]
                        from simtradedata.utils.code_utils import is_etf_code
                        etf_count = sum(1 for s in extras_stock_pool if is_etf_code(s))
                        print(f"\nOHLCV up to date (max_date: {global_max_date})")
                        print(
                            f"But adjust factors incomplete ({adj_count}/{stock_count}), "
                            f"downloading extras..."
                        )
                        print(f"  Missing: {len(extras_stock_pool)} "
                              f"({etf_count} ETFs, {len(extras_stock_pool) - etf_count} stocks)")
                        skip_stock_download = True

            if not skip_stock_download:
                # Get stock list from mootdx
                print("\nFetching stock list from mootdx...")
                stock_pool = downloader.unified_fetcher.fetch_stock_list()
                print(f"Total stocks: {len(stock_pool)}")

                if not stock_pool:
                    print("Error: No stocks found")
                    return

                # Resume: prioritize stocks needing backfill
                if global_max_date:
                    result = downloader.writer.conn.execute(
                        "SELECT DISTINCT symbol FROM stocks WHERE date = ?",
                        [global_max_date],
                    ).fetchall()
                    current_symbols = {r[0] for r in result}
                    needs_work = [s for s in stock_pool if s not in current_symbols]
                    already_current = [s for s in stock_pool if s in current_symbols]
                    if already_current:
                        stock_pool = needs_work + already_current
                        print(f"  Resume: {len(needs_work)} need download, "
                              f"{len(already_current)} already have latest data")

                # Download in batches
                batches = [
                    stock_pool[i : i + BATCH_SIZE]
                    for i in range(0, len(stock_pool), BATCH_SIZE)
                ]

                print(f"\nProcessing {len(stock_pool)} stocks in {len(batches)} batches...")
                print(f"Batch size: {BATCH_SIZE}")
                print("=" * 60)

                total_success = 0

                with tqdm(
                    total=len(stock_pool),
                    desc="Downloading stocks",
                    unit="stock",
                    ncols=100,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                ) as pbar:
                    for batch in batches:
                        try:
                            success = downloader.download_batch(
                                batch, start_date_str, end_date_str, pbar
                            )
                            total_success += success
                        except Exception as e:
                            logger.error(f"Batch failed: {e}")
                            pbar.update(len(batch))

                print("=" * 60)
                print(
                    f"Download complete: {total_success} updated, "
                    f"{len(stock_pool) - total_success} skipped/failed"
                )

            # Download extras (adjust factors / exrights) for stocks missing them
            if extras_stock_pool:
                batches = [
                    extras_stock_pool[i : i + BATCH_SIZE]
                    for i in range(0, len(extras_stock_pool), BATCH_SIZE)
                ]
                print(f"\nDownloading extras for {len(extras_stock_pool)} stocks in {len(batches)} batches...")
                print("=" * 60)
                total_success = 0

                with tqdm(
                    total=len(extras_stock_pool),
                    desc="Downloading extras",
                    unit="stock",
                    ncols=100,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                ) as pbar:
                    for batch in batches:
                        try:
                            success = downloader.download_batch(
                                batch, start_date_str, end_date_str, pbar
                            )
                            total_success += success
                        except Exception as e:
                            logger.error(f"Extras batch failed: {e}")
                            pbar.update(len(batch))

                print("=" * 60)
                print(f"Extras complete: {total_success} updated, "
                      f"{len(extras_stock_pool) - total_success} skipped/failed")

            # Download batch fundamentals
            if not skip_fundamentals:
                print("\nDownloading batch financial data (mootdx Affair)...")
                try:
                    downloader.download_fundamentals_batch(
                        start_date_str, end_date_str,
                        refresh=refresh_fundamentals,
                    )
                except Exception as e:
                    logger.error(f"Failed to download fundamentals: {e}")

            # Download global data
            print("\nDownloading global data...")

            # Trading calendar
            print("  Trading calendar...")
            try:
                trade_cal = downloader.unified_fetcher.fetch_trade_calendar(
                    start_date_str, end_date_str
                )
                if not trade_cal.empty:
                    trade_days = trade_cal[trade_cal["is_trading_day"] == "1"]
                    trade_days = trade_days.rename(
                        columns={"calendar_date": "trade_date"}
                    )
                    downloader.writer.write_trade_days(trade_days)
                    print(f"    {len(trade_days)} days")
            except Exception as e:
                logger.error(f"Failed to download trading calendar: {e}")

            # Benchmark index
            benchmark = BENCHMARK_CONFIG["default_index"]
            print(f"  Benchmark index ({benchmark})...")
            try:
                benchmark_df = downloader.unified_fetcher.fetch_index_data(
                    benchmark, start_date_str, end_date_str
                )
                if not benchmark_df.empty:
                    downloader.writer.write_benchmark(benchmark_df)
                    print(f"    {len(benchmark_df)} days")
            except Exception as e:
                logger.error(f"Failed to download benchmark: {e}")

        finally:
            downloader.writer.close()
            downloader.unified_fetcher.logout()

        # Summary
        print("\n" + "=" * 70)
        print("Download Complete!")
        print("=" * 70)

        db_file = Path(DEFAULT_DB_PATH)
        if db_file.exists():
            db_size = db_file.stat().st_size / (1024 * 1024)
            print(f"\nDatabase: {db_file}")
            print(f"Size: {db_size:.1f} MB")

        if downloader.failed_stocks:
            print(f"\nFailed stocks: {len(downloader.failed_stocks)}")
            logger.info(f"Failed stocks: {downloader.failed_stocks}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download mootdx data to DuckDB (auto-incremental)"
    )
    parser.add_argument(
        "--skip-fundamentals",
        action="store_true",
        help="Skip batch financial data download",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Override default start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default=None,
        help="Directory for downloading financial data ZIP files",
    )
    parser.add_argument(
        "--refresh-fundamentals",
        action="store_true",
        help="Force re-download all financial data quarters",
    )

    args = parser.parse_args()

    download_all_data(
        skip_fundamentals=args.skip_fundamentals,
        start_date=args.start_date,
        download_dir=args.download_dir,
        refresh_fundamentals=args.refresh_fundamentals,
    )
