# CLAUDE.md — SimTradeData

## Project
数据管线 v1.2.0，下载 A股 (BaoStock/Mootdx/EastMoney) 和美股 (yfinance) 行情数据到 DuckDB，导出 Parquet 供 SimTradeLab 消费。

## Stack
Python >=3.10, Poetry, Black+isort, DuckDB, flat layout (PEP 621 + poetry-core)

## Commands
- Download CN: `poetry run python scripts/download_efficient.py`
- Download unified: `poetry run python scripts/download.py`
- Download US: `poetry run python scripts/download_us.py`
- Download extras: `poetry run python scripts/download_daily_extras.py`
- Export Parquet: `poetry run python scripts/export_parquet.py`
- Quality check: `poetry run python scripts/check_data_quality.py`
- Integrity check: `poetry run python scripts/check_integrity.py`
- Test all: `poetry run pytest`
- Test single: `poetry run pytest tests/router/test_smart_router.py`
- Test by marker: `poetry run pytest -m unit`
- Format: `black . && isort .`

## Architecture
- `simtradedata/fetchers/` → BaseFetcher ABC (circuit breaker, cooldown, retry), 各数据源实现
- `simtradedata/router/` → SmartRouter 自动选择最优数据源
- `simtradedata/writers/` → DuckDBWriter (schema, upsert, Parquet export, 派生计算)
- `simtradedata/config/field_mappings.py` → 数据源→PTrade 列名映射
- `simtradedata/resilience/` → CircuitBreaker, SmartCooldown, RequestMonitor
- `data/` → DuckDB + Parquet 导出数据 (cn/ 子目录)

## Rules
- field_mappings.py 是数据源→PTrade 列名的唯一映射来源，新增字段必须在此注册
- DuckDBWriter 负责 schema 管理、增量更新和派生计算（价格限制、TTM、除权因子）
- 数据路径: data/cn/ 为导出给 SimTradeLab 的标准 Parquet 目录
- BaoStock fetcher 必须在 login 前设置 `socket.setdefaulttimeout(30)`，防止 TCP 长连接 hang
- 大批量下载（500+ 只股票）应使用 `nohup` 后台运行，不要使用 Claude Code 的 `run_in_background`

## Pitfalls（已踩坑记录）

- BaoStock 底层使用 TCP 长连接（非 HTTP），`send_msg()` 的 `recv()` 无超时保护，长时间运行后 TCP 半开连接会导致无限 hang。**必须在 login 前设置 `socket.setdefaulttimeout(30)`**，确保所有 socket 操作有超时
- BaoStock 使用全局 session（`baostock.common.context.default_socket`），进程被 kill 后 TCP 连接残留，新进程可能复用坏连接。**务必先 `bs.logout()` 清理再重启**
- Claude Code 的 `run_in_background` 与 BaoStock 长连接存在 I/O 冲突，大批量下载应使用 `nohup` 后台运行
- exrights 数据缺失会导致前复权因子全部为默认值（adj_a=1, adj_b=0），回测使用未复权价格。**数据完整性检查应包含 exrights 文件数验证**

## Out of Scope

- 不自动推送到远程仓库
