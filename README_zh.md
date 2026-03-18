[English](README.md) | 中文

# SimTradeData - 高效量化交易数据下载工具

> **BaoStock + Mootdx + EastMoney + yfinance 多数据源** | **A股 + 美股** | **PTrade格式兼容** | **DuckDB + Parquet存储**

**SimTradeData** 是为 [SimTradeLab](https://github.com/kay-ou/SimTradeLab) 设计的高效数据下载工具。支持 A 股（BaoStock、Mootdx、EastMoney）和美股（yfinance）多数据源，各取所长自动编排，采用 DuckDB 作为中间存储，导出为 Parquet 格式，支持高效的增量更新和数据查询。

---

<div align="center">

### 推荐组合：SimTradeData + SimTradeLab

**完全兼容PTrade | A股+美股 | 回测速度提升20倍以上**

[![SimTradeLab](https://img.shields.io/badge/SimTradeLab-量化回测框架-blue?style=for-the-badge)](https://github.com/kay-ou/SimTradeLab)

**无需修改PTrade策略代码** | **极速本地回测** | **零成本解决方案**

</div>

---

## 核心特性

### 高效存储架构
- **DuckDB 中间存储**: 高性能列式数据库，支持 SQL 查询和增量更新
- **Parquet 导出格式**: 压缩高效，跨平台兼容，适合大规模数据分析
- **自动增量更新**: 智能识别已下载数据，仅更新增量部分

### 数据完整性
- **市场数据**: OHLCV 日线数据，含涨跌停价、前收盘价
- **估值指标**: PE/PB/PS/PCF/换手率/总股本/流通股
- **财务数据**: 23个季度财务指标 + TTM指标自动计算
- **除权除息**: 分红、送股、配股数据
- **复权因子**: 前复权/后复权因子
- **元数据**: 股票信息、交易日历、指数成分股、ST/停牌状态
- **美股支持**: 6000+ 美股普通股，S&P 500 / NASDAQ-100 指数成分股

### 数据质量保障
- **自动验证**: 写入前自动验证数据完整性
- **导出时计算**: 涨跌停价、TTM指标等在导出时计算，确保数据一致性
- **详细日志**: 完整的错误日志和警告信息

## 生成的数据结构

```
data/
├── cn.duckdb          # DuckDB 数据库 - A股（下载源）
├── us.duckdb             # DuckDB 数据库 - 美股（下载源）
└── export/                      # 导出的 Parquet 文件（按市场分目录）
    ├── cn/                      # A股导出
    │   ├── stocks/              # 日线行情（每只股票一个文件）
    │   │   ├── 000001.SZ.parquet
    │   │   └── 600519.SS.parquet
    │   ├── exrights/            # 除权除息事件
    │   ├── fundamentals/        # 季度财务数据（含TTM）
    │   ├── valuation/           # 估值指标（日频）
    │   ├── metadata/            # 元数据
    │   ├── ptrade_adj_pre.parquet
    │   ├── ptrade_adj_post.parquet
    │   └── manifest.json
    └── us/                      # 美股导出
        ├── stocks/
        │   ├── AAPL.US.parquet
        │   └── MSFT.US.parquet
        ├── exrights/
        ├── fundamentals/
        ├── valuation/
        ├── metadata/
        ├── ptrade_adj_pre.parquet
        ├── ptrade_adj_post.parquet
        └── manifest.json
```

## 快速开始

### 方式一：直接下载现成数据（推荐）

从 [Releases](https://github.com/kay-ou/SimTradeData/releases) 下载最新数据：

- **A股**: `data-cn-v*` release → 解压到 `data/cn/`
- **美股**: `data-us-v*` release → 解压到 `data/us/`

```bash
# A股
mkdir -p /path/to/SimTradeLab/data/cn
tar -xzf simtradelab-data-cn-*.tar.gz -C /path/to/SimTradeLab/data/cn/

# 美股
mkdir -p /path/to/SimTradeLab/data/us
tar -xzf simtradelab-data-us-*.tar.gz -C /path/to/SimTradeLab/data/us/
```

### 方式二：自行下载数据

#### 1. 安装依赖

```bash
# 克隆项目
git clone https://github.com/kay-ou/SimTradeData.git
cd SimTradeData

# 安装依赖
poetry install

# 激活虚拟环境
poetry shell
```

#### 2. 下载数据

**推荐方式：统一下载命令**

一条命令完成所有数据下载，自动编排 Mootdx 和 BaoStock 各自擅长的数据：

```bash
# 完整下载（推荐）
# Mootdx: 行情、复权因子、除权除息、批量财务、交易日历、基准指数
# BaoStock: 估值指标、ST/停牌状态、指数成分股
poetry run python scripts/download.py

# 首次下载加速：先导入 TDX 日线包，再补充复权因子等
# （6000+ 只股票的 OHLCV 从数小时缩短到几分钟）
poetry run python scripts/download.py --tdx-download --source mootdx --skip-fundamentals

# 使用已下载的 TDX ZIP 文件
poetry run python scripts/download.py --tdx-source data/downloads/hsjday.zip --source mootdx

# 查看数据状态
poetry run python scripts/download.py --status

# 跳过财务数据（更快）
poetry run python scripts/download.py --skip-fundamentals

# 仅运行 Mootdx 阶段
poetry run python scripts/download.py --source mootdx

# 仅运行 BaoStock 阶段
poetry run python scripts/download.py --source baostock
```

**数据源分工说明**

| 数据类型 | 负责数据源 | 原因 |
|---------|-----------|------|
| 行情 OHLCV（首次） | TDX 日线包 | 最快，~500MB 一次性导入全部历史 |
| 行情 OHLCV（增量） | Mootdx | 速度快，本地网络 |
| 复权因子 | Mootdx | 随行情一起下载 |
| 除权除息 (XDXR) | Mootdx | 数据更完整 |
| 批量财务数据 | Mootdx | 一个ZIP=所有股票，远优于逐股查询 |
| 估值 PE/PB/PS/换手率 | BaoStock | 独有数据 |
| ST/停牌状态 | BaoStock | 独有数据 |
| 指数成分股 | BaoStock | 独有数据 |
| 交易日历 | Mootdx | 随行情一起 |
| 基准指数 | Mootdx | 随行情一起 |

**单独使用某个数据源**

```bash
# BaoStock（包含估值数据，但速度较慢）
poetry run python scripts/download_efficient.py
poetry run python scripts/download_efficient.py --skip-fundamentals
poetry run python scripts/download_efficient.py --valuation-only  # 仅估值+状态

# Mootdx（速度快，但无估值数据）
poetry run python scripts/download_mootdx.py
poetry run python scripts/download_mootdx.py --skip-fundamentals
```

**EastMoney 补充数据（资金流向、龙虎榜、融资融券）**

```bash
# 下载最近30天的补充数据（需先有行情数据）
poetry run python scripts/download_daily_extras.py

# 指定天数（龙虎榜 API 仅保留~30天数据，建议定期运行）
poetry run python scripts/download_daily_extras.py --days 7
```

**美股数据下载（yfinance）**

使用 yfinance 免费获取美股数据，无需 API Key：

```bash
# 完整下载（6000+ 只美股，含 OHLCV + 财务 + 估值 + 元数据）
poetry run python scripts/download_us.py

# 指定股票（小规模测试）
poetry run python scripts/download_us.py --symbols AAPL,MSFT,GOOGL

# 仅下载行情数据（跳过耗时的逐股财务和元数据）
poetry run python scripts/download_us.py --skip-fundamentals --skip-metadata

# 指定起始日期
poetry run python scripts/download_us.py --start-date 2020-01-01
```

美股代码格式：`AAPL.US`（与 A 股 `600000.SS` 保持 `{code}.{market}` 一致），数据存入独立数据库 `data/us.duckdb`。

**TDX 官方数据包（最快获取完整历史行情）**

```bash
# 自动下载通达信官方沪深京日线完整包（~500MB）
poetry run python scripts/download_tdx_day.py

# 强制重新下载
poetry run python scripts/download_tdx_day.py --force-download

# 使用已下载的文件
poetry run python scripts/download_tdx_day.py --file hsjday.zip
```

#### 3. 导出为 Parquet

```bash
# 导出 A股 → data/export/cn/
poetry run python scripts/export_parquet.py

# 导出美股 → data/export/us/
poetry run python scripts/export_parquet.py --market us

# 自定义输出目录
poetry run python scripts/export_parquet.py --market cn --output /custom/path
```

#### 4. 发布到 GitHub（维护者）

```bash
# 发布 A股数据
bash scripts/release_data.sh --market cn

# 发布美股数据
bash scripts/release_data.sh --market us

# 指定版本号
bash scripts/release_data.sh --market cn 1.3.0
```

#### 5. 在 SimTradeLab 中使用

```bash
# 复制导出数据到 SimTradeLab 数据目录
rsync -a data/export/cn/ /path/to/SimTradeLab/data/cn/
rsync -a data/export/us/ /path/to/SimTradeLab/data/us/
```

## 项目架构

```
SimTradeData/
├── scripts/
│   ├── download.py                # 统一下载入口（A股推荐）
│   ├── download_efficient.py      # BaoStock 下载脚本
│   ├── download_mootdx.py         # Mootdx（通达信API）下载脚本
│   ├── download_daily_extras.py   # EastMoney 补充数据下载脚本
│   ├── download_tdx_day.py        # TDX 官方日线数据包下载导入脚本
│   ├── download_us.py             # 美股下载脚本（yfinance）
│   ├── import_tdx_day.py          # TDX .day 文件导入脚本
│   ├── export_parquet.py          # Parquet 导出脚本
│   └── release_data.sh            # GitHub Release 发布脚本
├── simtradedata/
│   ├── router/
│   │   ├── smart_router.py      # SmartRouter 智能数据源路由
│   │   ├── route_config.py      # 路由表配置
│   │   └── exceptions.py        # 路由异常
│   ├── fetchers/
│   │   ├── base_fetcher.py      # 基础 Fetcher 类
│   │   ├── baostock_fetcher.py  # BaoStock 数据获取
│   │   ├── unified_fetcher.py   # BaoStock 统一数据获取（优化版）
│   │   ├── mootdx_fetcher.py    # Mootdx 基础数据获取
│   │   ├── mootdx_unified_fetcher.py  # Mootdx 统一数据获取
│   │   ├── mootdx_affair_fetcher.py   # Mootdx 财务数据获取
│   │   ├── eastmoney_fetcher.py # EastMoney 补充数据获取
│   │   └── yfinance_fetcher.py  # yfinance 美股数据获取
│   ├── processors/
│   │   └── data_splitter.py     # 数据分流处理
│   ├── writers/
│   │   └── duckdb_writer.py     # DuckDB 写入和导出
│   ├── validators/
│   │   └── data_validator.py    # 数据质量验证
│   ├── config/
│   │   ├── field_mappings.py    # A股字段映射配置
│   │   ├── us_field_mappings.py # 美股字段映射配置
│   │   └── mootdx_finvalue_map.py  # Mootdx 财务字段映射
│   └── utils/
│       ├── code_utils.py        # 股票代码转换
│       └── ttm_calculator.py    # 季度范围计算
├── data/                        # 数据目录
└── docs/                        # 文档
    ├── PTRADE_PARQUET_FORMAT.md # Parquet 格式规范
    └── PTrade_API_mini_Reference.md
```

### 核心模块

**1. SmartRouter** - 智能数据源路由
- 统一数据访问接口，自动根据数据类型和市场选择最佳数据源
- 静态优先级 + 健康感知：主源失败时自动 fallback 到备用源
- 集成 Phase 1 熔断器，跳过不健康的数据源

```python
from simtradedata.router import SmartRouter

with SmartRouter() as router:
    # 自动选择最佳源：mootdx → eastmoney → baostock
    df = router.get_daily_bars("600000.SS", "2024-01-01", "2024-12-31")

    # 单源数据也走 router，接口统一
    mf = router.get_money_flow("600000.SS", "2024-01-01", "2024-12-31")

    # 美股自动路由到 yfinance
    us = router.get_daily_bars("AAPL.US", "2024-01-01", "2024-12-31")
```

**2. UnifiedDataFetcher** - 统一数据获取
- 一次 API 调用获取行情、估值、状态数据
- 减少 API 调用次数 33%

**2. DuckDBWriter** - 数据存储和导出
- 高效的增量写入（upsert）
- 导出时计算涨跌停价、TTM指标
- Forward fill 季度数据到日频

**3. DataSplitter** - 数据分流
- 将统一数据按类型分流到不同表

## 数据字段说明

### stocks/ - 股票日线
| 字段 | 说明 |
|------|------|
| date | 交易日期 |
| open/high/low/close | OHLC价格 |
| high_limit/low_limit | 涨跌停价（导出时计算） |
| preclose | 前收盘价 |
| volume | 成交量（股） |
| money | 成交金额（元） |

### valuation/ - 估值指标（日频）
| 字段 | 说明 |
|------|------|
| pe_ttm/pb/ps_ttm/pcf | 估值比率 |
| roe/roe_ttm/roa/roa_ttm | 盈利指标（季报forward fill） |
| naps | 每股净资产（导出时计算） |
| total_shares/a_floats | 总股本/流通股 |
| turnover_rate | 换手率 |

### fundamentals/ - 财务数据（季频）
包含23个财务指标及其TTM版本，详见 [PTRADE_PARQUET_FORMAT.md](docs/PTRADE_PARQUET_FORMAT.md)

## 配置说明

编辑 `scripts/download_efficient.py`:

```python
# 日期范围
START_DATE = "2017-01-01"
END_DATE = None  # None = 当前日期

# 输出目录
OUTPUT_DIR = "data"

# 批次大小
BATCH_SIZE = 20
```

## 文档

| 文档 | 说明 |
|------|------|
| [PTRADE_PARQUET_FORMAT.md](docs/PTRADE_PARQUET_FORMAT.md) | Parquet 数据格式规范 |
| [PTrade_API_mini_Reference.md](docs/PTrade_API_mini_Reference.md) | PTrade API 参考 |

## 注意事项

### 数据源对比

| 特性 | BaoStock | Mootdx API | EastMoney | TDX 官方数据包 | yfinance (美股) |
|------|----------|------------|-----------|---------------|----------------|
| 市场 | A股 | A股 | A股 | A股 | 美股 |
| 速度 | 较慢 | 快 | 快 | 最快（一次性下载） | 中等 |
| 估值数据 | 有 (PE/PB/PS等) | 无 | 无 | 无 | 有（计算得出） |
| 财务数据 | 有（逐股查询） | 有（批量ZIP，更快） | 无 | 无 | 有（逐股查询） |
| 资金流向 | 无 | 无 | 有（独有） | 无 | 无 |
| 龙虎榜 | 无 | 无 | 有（独有） | 无 | 无 |
| 融资融券 | 无 | 无 | 有（独有） | 无 | 无 |
| 历史起始 | 2015年 | 2015年 | 2015年 | 完整历史 | 完整历史 |
| API Key | 不需要 | 不需要 | 不需要 | N/A | 不需要 |

> **推荐**：使用 `scripts/download.py` 统一命令，自动让 Mootdx 负责行情和财务，BaoStock 负责估值和状态，各取所长。

### 增量更新机制

- **行情数据**：检查是否有新交易日，无新数据时秒级跳过
- **财务数据**：基于远程文件 hash 增量检查，仅下载有变更的季度
- **指数成分股**：记录已下载月份，仅下载新月份
- **中断恢复**：财务数据进度与数据在同一事务中提交，中断后可续传

#### 增量更新流程

```bash
# 1. 增量下载（仅获取新数据，已有数据自动跳过）
poetry run python scripts/download.py

# 2. 导出为 Parquet（覆盖旧的导出）
poetry run python scripts/export_parquet.py
```

第 1 步会自动检测 DuckDB 中已有数据的最新日期，只下载增量部分。
无新交易日时全部股票秒级跳过。

### 数据质量
- 数据来自 BaoStock 免费数据源
- 仅供学习研究使用

## 测试

```bash
# 单元测试（无需网络）
poetry run pytest tests/ -v

# SmartRouter 路由和 fallback 测试
poetry run pytest tests/router/ -v

# SmartRouter 真实数据源集成测试（需要网络）
poetry run python scripts/test_smart_router_live.py
```

## 版本历史

### v1.2.0 (2026-03-13) - 智能数据源路由
- 新增 SmartRouter 统一数据访问层
- 自动根据数据类型和市场选择最佳数据源
- 静态优先级 + 熔断器健康感知，主源失败自动 fallback
- 支持 13 种数据类型：日线、复权因子、XDXR、财务、估值、资金流向、龙虎榜、融资融券等
- 新增 EastMoney 数据源作为 A 股日线 fallback
- 输出列标准化：无论使用哪个数据源，返回一致的列结构

### v1.1.0 (2026-03-10) - TDX 快速导入集成
- 新增 `--tdx-download` 参数：自动下载 TDX 官方沪深日线包并导入
- 新增 `--tdx-source` 参数：从本地 ZIP 文件或目录导入 TDX 日线数据
- 首次下载 6000+ 只股票 OHLCV 从数小时缩短到几分钟
- TDX 导入作为 Phase 0 在 Mootdx 阶段之前自动执行
- 修复 TDX 导入后复权因子和除权除息无法补充下载的问题
- 复权因子和除权除息改为按个股检查是否缺失，独立于 OHLCV 增量逻辑

### v0.6.0 (2026-02-08) - 美股数据支持
- 新增 yfinance 数据源，支持 6000+ 只美股普通股
- 美股代码格式 `AAPL.US`，与 A 股 `{code}.{market}` 一致
- 独立数据库 `data/us.duckdb`，与 A 股数据隔离
- 5 阶段下载：股票列表 → 批量 OHLCV → 财务+估值 → 元数据+除权 → 全局数据
- `yf.download()` 批量获取行情（每批 50 只），效率高
- 支持 S&P 500 / NASDAQ-100 指数成分股（Wikipedia 爬取）
- 增量更新：复用 `get_max_date()` 逻辑，仅下载新数据

### v0.5.0 (2026-02-01) - 统一下载命令
- 新增 `scripts/download.py` 统一下载入口
- 自动编排 Mootdx 和 BaoStock 数据源，各取所长
- 优化增量检测：无新交易日时秒级跳过全部股票
- 财务数据增量：基于远程文件 hash 检测变更
- 指数成分股增量：记录已下载月份避免重复
- 修复 Mootdx Affair API 返回值处理
- 修复 DuckDB `changes()` 函数兼容性
- 自动过滤停牌股票的空行数据

### v0.4.0 (2026-01-30) - DuckDB + Parquet 架构
- 存储格式从 HDF5 迁移到 DuckDB + Parquet
- 添加涨跌停价计算（导出时基于 preclose）
- 添加 TTM 指标计算（导出时用 SQL window function）
- 添加除权除息数据下载
- 添加股本数据（total_shares/a_floats）
- 优化增量更新逻辑
- 清理废弃代码和文档

### v0.3.0 (2025-11-24) - 质量与架构优化版
- 实现市值字段计算
- 修复 TTM 指标计算
- 添加数据验证器
- 提取 BaseFetcher 基类

### v0.2.0 (2025-11-22) - 性能优化版
- 实现统一数据获取，API 调用减少 33%
- 优化 HDF5 写入逻辑

### v0.1.0 (2024-11-14) - 初始版本
- 基础数据下载功能
- BaoStock 数据源集成

## 相关链接

- **SimTradeLab**: https://github.com/kay-ou/SimTradeLab
- **BaoStock**: http://baostock.com/
- **Mootdx**: https://github.com/mootdx/mootdx
- **EastMoney**: https://www.eastmoney.com/
- **yfinance**: https://github.com/ranaroussi/yfinance

## 💖 赞助支持

如果这个项目对您有帮助，欢迎赞助支持开发！

| 微信赞助 | 支付宝赞助 |
|:---:|:---:|
| <img src="docs/sponsor/WechatPay.png?raw=true" alt="微信赞助" width="200"> | <img src="docs/sponsor/AliPay.png?raw=true" alt="支付宝赞助" width="200"> |

**您的支持是我们持续改进的动力！**

## 许可证

本项目采用 AGPL-3.0 许可证。详见 [LICENSE](LICENSE) 文件。

---

**项目状态**: 生产就绪 | **当前版本**: v1.2.0 | **最后更新**: 2026-03-13
