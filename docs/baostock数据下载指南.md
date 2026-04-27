# Baostock 全量数据下载 — 任务执行手册

> **本文档面向全新环境首次下载。** 核心原则：不追求下载速度，追求下载稳定性。
> 预计全量下载耗时 **5~8 小时**（约 5,500 只股票，含所有 phases）。

---

## 1. 准备工作

### 1.1 清理旧数据（首次全新下载必须）

```bash
# 删除旧数据目录（全新下载前必须执行）
rm -rf "C:/Users/Admin/SynologyDrive/PtradeProjects/SimTradeData/data/cn"
mkdir -p "C:/Users/Admin/SynologyDrive/PtradeProjects/SimTradeData/data/cn"
```

> **重要**：清理后所有 phases 必须重新下载，但断点续传机制可以避免重复下载已完成的部分。

### 1.2 确认 baostock 版本

BaoStock 0.8.9 已停止服务，必须使用 0.9.1+：

```bash
cd "C:/Users/Admin/SynologyDrive/PtradeProjects/SimTradeData"
poetry run pip show baostock  # 确认版本 >= 0.9.1
poetry run pip install baostock>=0.9.1  # 如需要则升级
```

### 1.3 网络要求

- **Tailscale 用户**：exit node 必须设为 `None`（国内网络直连），BaoStock 服务器不接受海外出口 IP
- BaoStock 使用 TCP 协议（端口 10030），不是 HTTP
- 验证连接：
```bash
poetry run python -c "
import baostock as bs
lg = bs.login()
print(lg.error_code, lg.error_msg)
if lg.error_code == '0':
    bs.logout()
    print('BaoStock 连接正常')
else:
    print('连接失败: ', lg.error_msg)
"
```

### 1.4 验证环境

```bash
poetry run python -c "import baostock; print('baostock OK')"
poetry run python -c "import simtradedata; print('simtradedata OK')"
```

---

## 2. 策略数据需求（来自 trends_up_new）

`trends_up_new` 策略对数据的要求：

| 数据类型 | 用途 | 下载参数 | 存储方式 |
|----------|------|----------|----------|
| **日线** | 趋势判断、选股 | `adjustflag="3"`（不复权）| `stocks/{symbol}.parquet` |
| **除权因子** | 动态前复权（`fq="dypre"`）计算 | — | `exrights/{symbol}.parquet` |
| **5分钟线** | 盘中风控 | `adjustflag="3"`（不复权）| `stocks_5m/{symbol}.parquet` |

> **说明**：存储的是**原始不复权数据**，前复权（`fq="dypre"`）由回测引擎在运行时动态计算，依赖 `exrights/` 目录下的 `exer_forward_a` 和 `exer_forward_b` 因子。

---

## 3. 推荐下载方案（稳定性优先）

### 方案 A：分阶段顺序执行（最稳定）

```bash
cd "C:/Users/Admin/SynologyDrive/PtradeProjects/SimTradeData"

# ── 第 1 天 ──────────────────────────────────────
# Phase 0: 元数据（~700 次请求，约 2 分钟）
poetry run python scripts/download_baostock_full.py --phase metadata

# Phase 1: 日线 + 估值（~5,500 次请求，预计 1~2 小时）
poetry run python scripts/download_baostock_full.py --phase daily

# ── 第 2 天 ──────────────────────────────────────
# Phase 2: 除权除息 + 前复权因子（预计 1~2 小时）
poetry run python scripts/download_baostock_full.py --phase exrights

# ── 第 3~4 天 ───────────────────────────────────
# Phase 3: 季度基本面（预计 2~3 小时，可中断续传）
poetry run python scripts/download_baostock_full.py --phase fundamentals

# ── 第 5~6 天 ───────────────────────────────────
# Phase 4: 5分钟K线（预计 1~2 小时）
poetry run python scripts/download_baostock_full.py --phase 5min
```

每个 phase 独立断点续传，中断后可立即重新运行相同命令继续。

### 方案 B：全自动一键下载（需要值守）

```bash
# 一次性执行全部 phases（约 5~8 小时）
poetry run python scripts/download_baostock_full.py --phase all --force
```

> **警告**：方案 B 需要确保机器长时间稳定运行，建议先跑方案 A 的 Phase 0 和 Phase 1，确认稳定后再继续。

---

## 4. 参数说明

```bash
# 查看当前下载进度
poetry run python scripts/download_baostock_full.py --status

# 强制重新下载（跳过断点续传）
poetry run python scripts/download_baostock_full.py --phase daily --force

# 仅测试 3 只股票（验证网络和程序功能）
poetry run python scripts/download_baostock_full.py --phase daily --stocks "000001.SZ,000002.SZ,600000.SH" --force

# 5 分钟线可分批并行（使用 --stock-range）
# 假设 ~5500 只股票，分 4 批：
poetry run python scripts/download_baostock_full.py --phase 5min --stock-range "0-1375"
poetry run python scripts/download_baostock_full.py --phase 5min --stock-range "1375-2750"
poetry run python scripts/download_baostock_full.py --phase 5min --stock-range "2750-4125"
poetry run python scripts/download_baostock_full.py --phase 5min --stock-range "4125-5500"
```

---

## 5. 内置稳定性保护

脚本已内置以下保护机制，**无需手动干预**：

| 保护机制 | 说明 |
|----------|------|
| **日配额追踪** | 持久化到 `data/cn/.baostock_daily_counter.json`，80% 警告，95% 自动停止 |
| **黑名单检测** | 每次 API 返回检查错误码 `10001011`，立即终止 |
| **自动重试** | 每只股票最多 3 次重试（指数退避最长 30 秒）|
| **连续失败保护** | 连续 10 次失败则暂停 60 秒 |
| **进程锁** | 防止多进程同时运行 |
| **断点续传** | 每个 phase 独立续传，不重复下载已完成股票 |

### 配额估算

| Phase | 阶段 | 请求量 | 耗时 |
|--------|------|--------|------|
| Phase 0 | 元数据 | ~700 | ~2 分钟 |
| Phase 1 | 日线+估值 | ~5,500 | ~1~2 小时 |
| Phase 2 | 除权因子 | ~20,000~55,000 | ~1~2 小时 |
| Phase 3 | 季度基本面 | ~60,000 | ~2~3 小时 |
| Phase 4 | 5分钟线 | ~32,500 | ~1~2 小时 |

> **日配额 50,000 次**：Phase 1 约用 11%，Phase 2+3 可能需要多天。脚本在 95% 时自动停止，次日继续。

---

## 6. 故障处理

| 问题 | 错误码/现象 | 解决方案 |
|------|------------|----------|
| BaoStock 服务器不可用 | `10002007` | 确认 baostock>=0.9.1；旧版 0.8.9 已停服 |
| Tailscale exit node 导致失败 | `10002007` | 将 exit node 设为 `None`，使用国内网络直连 |
| 日配额超限 | 脚本自动停止 | 正常行为，次日运行同一命令自动续传 |
| IP 黑名单 | `10001011` | 联系 QQ 群管理员解封，提供公网 IP |
| 登录上限 | `10001005` | 等 1~2 分钟后重试 |
| 进程锁冲突 | `Another download process is running` | 删除 `data/cn/.download.lock` 后重试 |

---

## 7. 验证下载结果

### 7.1 检查文件数量

```bash
cd "C:/Users/Admin/SynologyDrive/PtradeProjects/SimTradeData/data/cn"
echo "=== 各目录文件数量 ==="
for dir in stocks stocks_5m exrights fundamentals valuation metadata; do
    count=$(ls $dir/*.parquet 2>/dev/null | wc -l)
    printf "  %-15s %s 文件\n" "$dir/" "$count"
done
```

**预期结果（全量完成后）**：

| 目录 | 预期文件数 | 说明 |
|------|-----------|------|
| `stocks/` | ~5,500 | 每只 A 股一个 parquet |
| `stocks_5m/` | ~5,500 | 每只 A 股一个 parquet |
| `exrights/` | ~3,000~5,500 | 有除权事件的股票（含前复权因子）|
| `fundamentals/` | ~5,500 | 每只 A 股一个 parquet |
| `valuation/` | ~5,500 | 每只 A 股一个 parquet |
| `metadata/` | 4 | trade_days, benchmark, index_constituents, stock_metadata |

### 7.2 检查数据字段（针对 trends_up_new）

```bash
poetry run python -c "
import pandas as pd
from pathlib import Path

cn = Path('C:/Users/Admin/SynologyDrive/PtradeProjects/SimTradeData/data/cn')

# 检查日K线字段
f = next((cn / 'stocks').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['date','open','high','low','close','volume','money']
    missing = [c for c in required if c not in df.columns]
    print(f'日K线字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')
    print(f'  行数: {len(df)}, 日期范围: {df[\"date\"].min()} ~ {df[\"date\"].max()}')

# 检查除权因子（含前复权因子）
f = next((cn / 'exrights').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['date','exer_forward_a','exer_forward_b']
    missing = [c for c in required if c not in df.columns]
    print(f'除权因子字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')

# 检查5分钟线字段
f = next((cn / 'stocks_5m').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['open','high','low','close','volume','money']
    missing = [c for c in required if c not in df.columns]
    print(f'5分钟线字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')
    print(f'  行数: {len(df)}')
"
```

---

## 8. 下载数据规格

| 参数 | 值 |
|------|---|
| 日期范围 | **2023-01-01 ~ 2026-03-31** |
| 复权方式 | **不复权**（`adjustflag="3"`），存储原始价格 |
| 输出目录 | **`SimTradeData/data/cn/`** |
| 文件格式 | Parquet（每只股票一个文件） |
| 股票范围 | 全 A 股（约 5,500 只，沪深主板 + 创业板 + 科创板） |
| 不下载 | 60 分钟 K 线（可由 5 分钟数据聚合） |

### 数据字段详情

**日线** `stocks/{symbol}.parquet`：

| 列名 | 说明 |
|------|------|
| `date` | 交易日期 |
| `open/high/low/close` | 原始 OHLC（不复权） |
| `volume/money` | 成交量/成交额 |
| `preclose` | 前收盘价 |
| `high_limit/low_limit` | 涨跌停价 |

**除权因子** `exrights/{symbol}.parquet`（Phase 2）：

| 列名 | 说明 |
|------|------|
| `date` | 除权除息日 |
| `exer_forward_a` | **前复权因子 A**（乘数） |
| `exer_forward_b` | **前复权因子 B**（偏移量） |
| `bonus_ps/dividend` | 每股分红 |
| `allotted_ps/rationed_ps` | 送股/转增比例 |

> **前复权公式**：`前复权价 = exer_forward_a × 未复权价 + exer_forward_b`
> 回测引擎使用 `fq="dypre"` 时依赖此因子动态计算前复权价格。

**5分钟线** `stocks_5m/{symbol}.parquet`：

| 列名 | 说明 |
|------|------|
| (DatetimeIndex) | 时间戳索引 |
| `open/high/low/close` | 原始 OHLC（不复权） |
| `volume/money` | 成交量/成交额 |

---

## 9. BaoStock API 关键限制

| 限制项 | 值 |
|---|---|
| 每日请求上限 | **50,000 次/IP/天** |
| 单次查询记录上限 | **10,000 条** |
| 历史数据起始 | **2015-01-01** |
| 错误码 `10001011` | IP 黑名单 |
| 错误码 `10002007` | 网络接收错误（服务器不可用）|
