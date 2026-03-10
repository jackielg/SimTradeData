English | [中文](README_zh.md)

# SimTradeData - Quantitative Trading Data Downloader

> **BaoStock + Mootdx + yfinance Multi-Source** | **China A-Shares + US Stocks** | **PTrade Compatible** | **DuckDB + Parquet Storage**

**SimTradeData** is an efficient data download tool designed for [SimTradeLab](https://github.com/kay-ou/SimTradeLab). It supports China A-shares (BaoStock, Mootdx) and US stocks (yfinance) from multiple data sources, automatically orchestrating each source's strengths. Data is stored in DuckDB as intermediate storage and exported to Parquet format, with efficient incremental updates and querying.

---

<div align="center">

### Recommended Combo: SimTradeData + SimTradeLab

**Fully PTrade Compatible | A-Shares + US Stocks | 20x+ Backtesting Speedup**

[![SimTradeLab](https://img.shields.io/badge/SimTradeLab-Quant_Backtesting-blue?style=for-the-badge)](https://github.com/kay-ou/SimTradeLab)

**No PTrade Strategy Code Changes Needed** | **Ultra-Fast Local Backtesting** | **Zero-Cost Solution**

</div>

---

## Key Features

### Efficient Storage Architecture
- **DuckDB Intermediate Storage**: High-performance columnar database with SQL queries and incremental updates
- **Parquet Export Format**: Highly compressed, cross-platform compatible, ideal for large-scale data analysis
- **Automatic Incremental Updates**: Intelligently detects existing data, only downloads new records

### Comprehensive Data Coverage
- **Market Data**: OHLCV daily bars with limit-up/down prices and previous close
- **Valuation Metrics**: PE/PB/PS/PCF/Turnover Rate/Total Shares/Float Shares
- **Financial Data**: 23 quarterly financial indicators + automatic TTM calculation
- **Corporate Actions**: Dividends, bonus shares, rights offerings
- **Adjustment Factors**: Forward and backward adjustment factors
- **Metadata**: Stock info, trading calendar, index constituents, ST/suspension status
- **US Stock Support**: 6,000+ US common stocks, S&P 500 / NASDAQ-100 index constituents

### Data Quality Assurance
- **Auto-Validation**: Data integrity validation before writes
- **Export-Time Calculation**: Limit prices, TTM metrics computed at export for consistency
- **Detailed Logging**: Comprehensive error logs and warnings

## Generated Data Structure

```
data/
├── simtradedata.duckdb          # DuckDB database - A-shares (used during download)
├── us_stocks.duckdb             # DuckDB database - US stocks (used during download)
└── parquet/                     # Exported Parquet files
    ├── stocks/                  # Daily stock bars (one file per stock)
    │   ├── 000001.SZ.parquet
    │   └── 600000.SS.parquet
    ├── exrights/                # Corporate action events
    ├── fundamentals/            # Quarterly financials (with TTM)
    ├── valuation/               # Valuation metrics (daily frequency)
    ├── metadata/                # Metadata
    │   ├── stock_metadata.parquet
    │   ├── benchmark.parquet
    │   ├── trade_days.parquet
    │   ├── index_constituents.parquet
    │   ├── stock_status.parquet
    │   └── version.parquet
    ├── ptrade_adj_pre.parquet   # Forward adjustment factors
    ├── ptrade_adj_post.parquet  # Backward adjustment factors
    └── manifest.json            # Data package manifest
```

## Quick Start

### Option 1: Download Pre-Built Data (Recommended)

Download the latest `simtradelab-data-*.tar.gz` from [Releases](https://github.com/kay-ou/SimTradeData/releases):

```bash
# Extract to SimTradeLab data directory
tar -xzf simtradelab-data-*.tar.gz -C /path/to/SimTradeLab/data/
```

### Option 2: Download Data Yourself

#### 1. Install Dependencies

```bash
# Clone the project
git clone https://github.com/kay-ou/SimTradeData.git
cd SimTradeData

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

#### 2. Download Data

**Recommended: Unified Download Command**

A single command downloads all data, automatically orchestrating Mootdx and BaoStock for their respective strengths:

```bash
# Full download (recommended)
# Mootdx: market data, adjustment factors, corporate actions, bulk financials, trading calendar, benchmark index
# BaoStock: valuation metrics, ST/suspension status, index constituents
poetry run python scripts/download.py

# Fast first-time download: import TDX daily package first, then supplement with adjustment factors etc.
# (6,000+ stocks OHLCV reduced from hours to minutes)
poetry run python scripts/download.py --tdx-download --source mootdx --skip-fundamentals

# Use an already-downloaded TDX ZIP file
poetry run python scripts/download.py --tdx-source data/downloads/hsjday.zip --source mootdx

# Check data status
poetry run python scripts/download.py --status

# Skip financial data (faster)
poetry run python scripts/download.py --skip-fundamentals

# Run Mootdx phase only
poetry run python scripts/download.py --source mootdx

# Run BaoStock phase only
poetry run python scripts/download.py --source baostock
```

**Data Source Division of Labor**

| Data Type | Source | Reason |
|-----------|--------|--------|
| OHLCV Market Data (first time) | TDX Daily Package | Fastest, ~500MB bulk import of full history |
| OHLCV Market Data (incremental) | Mootdx | Fast, local network |
| Adjustment Factors | Mootdx | Downloaded with market data |
| Corporate Actions (XDXR) | Mootdx | More complete data |
| Bulk Financial Data | Mootdx | One ZIP = all stocks, far better than per-stock queries |
| Valuation PE/PB/PS/Turnover | BaoStock | Exclusive data |
| ST/Suspension Status | BaoStock | Exclusive data |
| Index Constituents | BaoStock | Exclusive data |
| Trading Calendar | Mootdx | Comes with market data |
| Benchmark Index | Mootdx | Comes with market data |

**Using Individual Data Sources**

```bash
# BaoStock (includes valuation data, but slower)
poetry run python scripts/download_efficient.py
poetry run python scripts/download_efficient.py --skip-fundamentals
poetry run python scripts/download_efficient.py --valuation-only  # Valuation + status only

# Mootdx (faster, but no valuation data)
poetry run python scripts/download_mootdx.py
poetry run python scripts/download_mootdx.py --skip-fundamentals
```

**US Stock Data (yfinance)**

Free US stock data via yfinance, no API key required:

```bash
# Full download (6,000+ US stocks with OHLCV + financials + valuation + metadata)
poetry run python scripts/download_us.py

# Specific symbols (small-scale testing)
poetry run python scripts/download_us.py --symbols AAPL,MSFT,GOOGL

# Market data only (skip time-consuming per-stock financials and metadata)
poetry run python scripts/download_us.py --skip-fundamentals --skip-metadata

# Specify start date
poetry run python scripts/download_us.py --start-date 2020-01-01
```

US stock ticker format: `AAPL.US` (consistent with A-shares `600000.SS` using `{code}.{market}`), stored in a separate database `data/us_stocks.duckdb`.

**TDX Official Data Package (Fastest Way to Get Full Historical Data)**

```bash
# Auto-download official TDX Shanghai/Shenzhen/Beijing daily data package (~500MB)
poetry run python scripts/download_tdx_day.py

# Force re-download
poetry run python scripts/download_tdx_day.py --force-download

# Use an already-downloaded file
poetry run python scripts/download_tdx_day.py --file hsjday.zip
```

#### 3. Export to Parquet

```bash
# Export to PTrade-compatible Parquet format
poetry run python scripts/export_parquet.py

# Specify output directory
poetry run python scripts/export_parquet.py --output data/parquet
```

#### 4. Use in SimTradeLab

```bash
# Copy Parquet files to SimTradeLab data directory
cp -r data/parquet/* /path/to/SimTradeLab/data/
```

## Project Architecture

```
SimTradeData/
├── scripts/
│   ├── download.py                # Unified download entry (recommended for A-shares)
│   ├── download_efficient.py      # BaoStock download script
│   ├── download_mootdx.py         # Mootdx (TDX API) download script
│   ├── download_tdx_day.py        # TDX official daily data package download/import
│   ├── download_us.py             # US stock download script (yfinance)
│   ├── import_tdx_day.py          # TDX .day file import script
│   └── export_parquet.py          # Parquet export script
├── simtradedata/
│   ├── fetchers/
│   │   ├── base_fetcher.py      # Base Fetcher class
│   │   ├── baostock_fetcher.py  # BaoStock data fetching
│   │   ├── unified_fetcher.py   # BaoStock unified fetching (optimized)
│   │   ├── mootdx_fetcher.py    # Mootdx basic data fetching
│   │   ├── mootdx_unified_fetcher.py  # Mootdx unified data fetching
│   │   ├── mootdx_affair_fetcher.py   # Mootdx financial data fetching
│   │   └── yfinance_fetcher.py  # yfinance US stock data fetching
│   ├── processors/
│   │   └── data_splitter.py     # Data stream splitting
│   ├── writers/
│   │   └── duckdb_writer.py     # DuckDB write and export
│   ├── validators/
│   │   └── data_validator.py    # Data quality validation
│   ├── config/
│   │   ├── field_mappings.py    # A-share field mapping config
│   │   ├── us_field_mappings.py # US stock field mapping config
│   │   └── mootdx_finvalue_map.py  # Mootdx financial field mapping
│   └── utils/
│       ├── code_utils.py        # Stock code conversion
│       └── ttm_calculator.py    # Quarterly range calculation
├── data/                        # Data directory
└── docs/                        # Documentation
    ├── PTRADE_PARQUET_FORMAT.md # Parquet format specification
    └── PTrade_API_mini_Reference.md
```

### Core Modules

**1. UnifiedDataFetcher** - Unified Data Fetching
- Single API call fetches market, valuation, and status data
- Reduces API calls by 33%

**2. DuckDBWriter** - Data Storage and Export
- Efficient incremental writes (upsert)
- Computes limit prices and TTM metrics at export time
- Forward-fills quarterly data to daily frequency

**3. DataSplitter** - Data Stream Splitting
- Routes unified data to appropriate tables by type

## Data Field Reference

### stocks/ - Daily Stock Bars
| Field | Description |
|-------|-------------|
| date | Trading date |
| open/high/low/close | OHLC prices |
| high_limit/low_limit | Limit-up/down prices (computed at export) |
| preclose | Previous close price |
| volume | Trading volume (shares) |
| money | Trading amount (CNY) |

### valuation/ - Valuation Metrics (Daily)
| Field | Description |
|-------|-------------|
| pe_ttm/pb/ps_ttm/pcf | Valuation ratios |
| roe/roe_ttm/roa/roa_ttm | Profitability metrics (forward-filled from quarterly reports) |
| naps | Net asset per share (computed at export) |
| total_shares/a_floats | Total shares / float shares |
| turnover_rate | Turnover rate |

### fundamentals/ - Financial Data (Quarterly)
Contains 23 financial indicators and their TTM versions. See [PTRADE_PARQUET_FORMAT.md](docs/PTRADE_PARQUET_FORMAT.md) for details.

## Configuration

Edit `scripts/download_efficient.py`:

```python
# Date range
START_DATE = "2017-01-01"
END_DATE = None  # None = current date

# Output directory
OUTPUT_DIR = "data"

# Batch size
BATCH_SIZE = 20
```

## Documentation

| Document | Description |
|----------|-------------|
| [PTRADE_PARQUET_FORMAT.md](docs/PTRADE_PARQUET_FORMAT.md) | Parquet data format specification |
| [PTrade_API_mini_Reference.md](docs/PTrade_API_mini_Reference.md) | PTrade API reference |

## Notes

### Data Source Comparison

| Feature | BaoStock | Mootdx API | TDX Official Package | yfinance (US) |
|---------|----------|------------|---------------------|---------------|
| Market | A-shares | A-shares | A-shares | US stocks |
| Speed | Slower | Fast | Fastest (bulk download) | Medium |
| Valuation Data | Yes (PE/PB/PS etc.) | No | No | Yes (computed) |
| Financial Data | Yes (per-stock query) | Yes (bulk ZIP, faster) | No | Yes (per-stock query) |
| History Start | 2015 | 2015 | Full history | Full history |
| API Key | Not required | Not required | N/A | Not required |

> **Recommended**: Use `scripts/download.py` unified command to automatically assign Mootdx for market data and financials, BaoStock for valuation and status, leveraging each source's strengths.

### Incremental Update Mechanism

- **Market Data**: Checks for new trading days; skips in seconds when no new data
- **Financial Data**: Incremental checks based on remote file hash; only downloads changed quarters
- **Index Constituents**: Tracks downloaded months; only downloads new months
- **Interrupt Recovery**: Financial data progress and data are committed in the same transaction; resumes after interruption

#### Incremental Update Workflow

```bash
# 1. Incremental download (fetches only new data, automatically skips existing)
poetry run python scripts/download.py

# 2. Export to Parquet (overwrites previous export)
poetry run python scripts/export_parquet.py
```

Step 1 automatically detects the latest date of existing data in DuckDB and only downloads the delta. When there are no new trading days, all stocks are skipped in seconds.

### Data Quality
- Data sourced from BaoStock free data service
- For research and educational purposes only

## Version History

### v1.1.0 (2026-03-10) - TDX Fast Import Integration
- Added `--tdx-download` flag: auto-download TDX official daily data package and import
- Added `--tdx-source` flag: import TDX daily data from local ZIP file or directory
- First-time download of 6,000+ stocks OHLCV reduced from hours to minutes
- TDX import runs as Phase 0 before Mootdx phase
- Fixed adjust factors and XDXR not being downloaded after TDX bulk import
- Adjust factors and XDXR now check per-symbol existence independently of OHLCV incremental logic

### v0.6.0 (2026-02-08) - US Stock Support
- Added yfinance data source supporting 6,000+ US common stocks
- US stock ticker format `AAPL.US`, consistent with A-shares `{code}.{market}`
- Separate database `data/us_stocks.duckdb`, isolated from A-share data
- 5-phase download: stock list -> bulk OHLCV -> financials+valuation -> metadata+corporate actions -> global data
- `yf.download()` batch market data (50 per batch) for efficiency
- S&P 500 / NASDAQ-100 index constituents (scraped from Wikipedia)
- Incremental updates: reuses `get_max_date()` logic, only downloads new data

### v0.5.0 (2026-02-01) - Unified Download Command
- Added `scripts/download.py` unified download entry point
- Automatic orchestration of Mootdx and BaoStock sources, leveraging strengths
- Optimized incremental detection: skips all stocks in seconds when no new trading days
- Financial data incremental: detects changes via remote file hash
- Index constituents incremental: tracks downloaded months to avoid duplicates
- Fixed Mootdx Affair API return value handling
- Fixed DuckDB `changes()` function compatibility
- Auto-filters empty rows for suspended stocks

### v0.4.0 (2026-01-30) - DuckDB + Parquet Architecture
- Migrated storage from HDF5 to DuckDB + Parquet
- Added limit-up/down price calculation (computed at export from preclose)
- Added TTM metric calculation (SQL window functions at export)
- Added corporate action data download
- Added share capital data (total_shares/a_floats)
- Optimized incremental update logic
- Cleaned up deprecated code and docs

### v0.3.0 (2025-11-24) - Quality & Architecture Optimization
- Implemented market cap field calculation
- Fixed TTM metric calculation
- Added data validator
- Extracted BaseFetcher base class

### v0.2.0 (2025-11-22) - Performance Optimization
- Implemented unified data fetching, reducing API calls by 33%
- Optimized HDF5 write logic

### v0.1.0 (2024-11-14) - Initial Release
- Basic data download functionality
- BaoStock data source integration

## Related Links

- **SimTradeLab**: https://github.com/kay-ou/SimTradeLab
- **BaoStock**: http://baostock.com/
- **Mootdx**: https://github.com/mootdx/mootdx
- **yfinance**: https://github.com/ranaroussi/yfinance

## License

This project is licensed under AGPL-3.0. See the [LICENSE](LICENSE) file for details.

---

**Status**: Production Ready | **Version**: v1.1.0 | **Last Updated**: 2026-03-10
