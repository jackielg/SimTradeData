# Baostock 全量数据下载 — 任务执行手册

> **本文档是面向自动化执行的完整任务手册。** 另一台机器只需按顺序执行第 1~4 步即可完成全部数据下载。

---

## 1. 环境准备

```bash
# 1.1 进入项目目录
cd /path/to/SimTradeData

# 1.2 安装依赖（Poetry 自动创建虚拟环境）
poetry install

# 1.3 验证环境
poetry run python -c "import baostock; print('OK')"
poetry run python -c "import simtradedata; print('OK')"
```

如果 baostock 登录测试失败（网络错误 `10002007`），说明服务器暂时不可用，等几分钟后重试：

```bash
poetry run python -c "
import baostock as bs
lg = bs.login()
print(lg.error_code, lg.error_msg)
if lg.error_code == '0':
    bs.logout()
    print('BaoStock 连接正常')
else:
    print('BaoStock 暂时不可用，稍后重试')
"
```

---

## 2. 执行下载

### 2.1 一键全量下载（推荐）

```bash
cd /path/to/SimTradeData
poetry run python scripts/download_baostock_full.py
```

脚本会自动按顺序执行 5 个阶段（Phase 0~4），每个阶段支持断点续传。

### 2.2 分阶段下载（配额受限时）

全量下载约需 **415,000+ 次 API 请求**，日配额 **50,000 次**，需分多天完成。
推荐按以下顺序逐阶段执行：

```bash
# ── 第1天 ──────────────────────────────────────
# Phase 0: 元数据 (~700 次请求)
poetry run python scripts/download_baostock_full.py --phase metadata

# Phase 1: 日K线 + 估值 (~5,000 次请求)
poetry run python scripts/download_baostock_full.py --phase daily

# ── 第2天 ──────────────────────────────────────
# Phase 2: 除权除息 + 前复权因子 (~20,000~55,000 次请求)
poetry run python scripts/download_baostock_full.py --phase exrights

# ── 第3~7天 ────────────────────────────────────
# Phase 3: 季度基本面 (~60,000次/天，自动按季度续传)
poetry run python scripts/download_baostock_full.py --phase fundamentals

# ── 第8~9天 ────────────────────────────────────
# Phase 4: 5分钟K线 (~32,500次/天，需2天)
poetry run python scripts/download_baostock_full.py --phase 5min
```

### 2.3 其他选项

```bash
# 查看当前下载进度
poetry run python scripts/download_baostock_full.py --status

# 仅下载指定股票（测试用）
poetry run python scripts/download_baostock_full.py --phase daily --stocks 000001.SZ,600519.SS

# 忽略续传，强制重新下载
poetry run python scripts/download_baostock_full.py --phase daily --force
```

---

## 3. 验证下载结果

### 3.1 检查文件数量

```bash
cd /path/to/SimTradeData/data/cn
echo "=== 各目录文件数量 ==="
for dir in stocks stocks_5m exrights fundamentals valuation metadata; do
    count=$(ls $dir/*.parquet 2>/dev/null | wc -l)
    printf "  %-15s %s 文件\n" "$dir/" "$count"
done
```

**预期结果（全量完成后）：**

| 目录 | 预期文件数 | 说明 |
|------|-----------|------|
| `stocks/` | ~5,000 | 每只 A 股一个 parquet |
| `stocks_5m/` | ~5,000 | 每只 A 股一个 parquet |
| `exrights/` | ~3,000~5,000 | 有除权事件的股票 |
| `fundamentals/` | ~5,000 | 每只 A 股一个 parquet |
| `valuation/` | ~5,000 | 每只 A 股一个 parquet |
| `metadata/` | 4 | trade_days, benchmark, index_constituents, stock_metadata |

### 3.2 检查数据字段完整性

```bash
poetry run python -c "
import pandas as pd
from pathlib import Path

cn = Path('data/cn')

# 检查日K线字段
f = next((cn / 'stocks').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['date','open','high','low','close','volume','money','preclose','high_limit','low_limit']
    missing = [c for c in required if c not in df.columns]
    print(f'日K线字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')

# 检查除权因子字段（关键！）
f = next((cn / 'exrights').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['date','allotted_ps','rationed_ps','rationed_px','bonus_ps','dividend','exer_forward_a','exer_forward_b']
    missing = [c for c in required if c not in df.columns]
    print(f'除权因子字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')
else:
    print('除权因子: 无文件（尚未下载 Phase 2）')

# 检查5分钟线字段
f = next((cn / 'stocks_5m').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['open','high','low','close','volume','money']
    missing = [c for c in required if c not in df.columns]
    print(f'5分钟线字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')

# 检查估值字段
f = next((cn / 'valuation').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['date','pe_ttm','pb','ps_ttm','pcf','turnover_rate']
    missing = [c for c in required if c not in df.columns]
    print(f'估值字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')

# 检查基本面字段
f = next((cn / 'fundamentals').glob('*.parquet'), None)
if f:
    df = pd.read_parquet(f)
    required = ['end_date','total_shares','a_floats']
    missing = [c for c in required if c not in df.columns]
    print(f'基本面字段: {\"OK\" if not missing else \"缺失: \" + str(missing)}')
"
```

### 3.3 抽样检查数据质量

```bash
poetry run python -c "
import pandas as pd

# 检查日K线数据
df = pd.read_parquet('data/cn/stocks/000001.SZ.parquet')
print('=== 日K线 000001.SZ ===')
print(f'行数: {len(df)}, 日期范围: {df[\"date\"].min()} ~ {df[\"date\"].max()}')
print(f'字段: {df.columns.tolist()}')
print(f'空值: {df.isnull().sum().to_dict()}')

# 检查除权因子
import glob
files = glob.glob('data/cn/exrights/*.parquet')
if files:
    df = pd.read_parquet(files[0])
    print(f'\n=== 除权因子 {files[0].split(\"/\")[-1]} ===')
    print(f'行数: {len(df)}, 字段: {df.columns.tolist()}')
    if 'exer_forward_a' in df.columns:
        print(f'exer_forward_a 范围: {df[\"exer_forward_a\"].min():.6f} ~ {df[\"exer_forward_a\"].max():.6f}')
"
```

### 3.4 检查日配额使用

```bash
cat data/.baostock_daily_counter.json 2>/dev/null || echo "无配额文件"
```

---

## 4. 故障处理

### 4.1 常见问题

| 问题 | 错误码/现象 | 解决方案 |
|------|------------|----------|
| 网络接收错误 | `10002007` | BaoStock 服务器暂不可用，等几分钟后重试 |
| 日配额超限 | 脚本自动停止 | 正常行为，次日运行同一命令自动续传 |
| IP 黑名单 | `10001011` | 到 QQ 群联系管理员解封，提供公网 IP |
| 登录上限 | `10001005` | 等 1~2 分钟后重试，减少并发 |
| 进程锁冲突 | `Another download process is running` | 删除 `data/cn/.download.lock` 后重试 |

### 4.2 重新开始某个阶段

```bash
# 删除该阶段的进度记录（不删除已下载数据）
# 编辑 data/cn/download_progress.json，删除对应 phase 的条目
# 或使用 --force 强制重新下载
poetry run python scripts/download_baostock_full.py --phase daily --force
```

---

## 5. 下载数据规格

### 5.1 总体参数

| 参数 | 值 |
|------|---|
| 日期范围 | **2023-01-01 ~ 2026-03-31** |
| 复权方式 | **不复权**（`adjustflag="3"`） |
| 输出目录 | **`SimTradeData/data/cn/`** |
| 文件格式 | Parquet（每只股票一个文件） |
| 股票范围 | 全 A 股（沪深主板 + 创业板 + 科创板，排除指数/ETF/北交所） |
| 不下载 | **60 分钟 K 线**（可由 5 分钟数据聚合） |

### 5.2 各阶段数据详情

#### Phase 0: 元数据 → `metadata/`

| 文件 | 内容 |
|------|------|
| `trade_days.parquet` | 交易日历，列：`trade_date` |
| `benchmark.parquet` | 沪深300指数日线，列：`date, open, high, low, close, volume, money` |
| `index_constituents.parquet` | 指数成分股（月末采样），列：`date, index_code, symbols` |
| `stock_metadata.parquet` | 股票基本信息（前500只），列：`symbol, code_name, ipoDate, outDate, status` |

#### Phase 1: 日K线 + 估值 → `stocks/` + `valuation/`

**日K线** (`stocks/{symbol}.parquet`)：

| 列名 | 类型 | 说明 |
|------|------|------|
| `date` | datetime | 交易日期 |
| `open` | float | 开盘价（不复权） |
| `high` | float | 最高价（不复权） |
| `low` | float | 最低价（不复权） |
| `close` | float | 收盘价（不复权） |
| `volume` | float | 成交量（股） |
| `money` | float | 成交额（元） |
| `preclose` | float | 前收盘价 |
| `high_limit` | float | 涨停价（由 preclose × 1.10/1.20 计算） |
| `low_limit` | float | 跌停价（由 preclose × 0.90/0.80 计算） |

**估值** (`valuation/{symbol}.parquet`)：

| 列名 | 类型 | 说明 |
|------|------|------|
| `date` | datetime | 交易日期 |
| `pe_ttm` | float | 市盈率 TTM |
| `pb` | float | 市净率 MRQ |
| `ps_ttm` | float | 市销率 TTM |
| `pcf` | float | 现金流市现率 TTM |
| `turnover_rate` | float | 换手率 |

#### Phase 2: 除权除息 + 前复权因子 → `exrights/`

`exrights/{symbol}.parquet`：

| 列名 | 类型 | 说明 |
|------|------|------|
| `date` | datetime | 除权除息日 |
| `allotted_ps` | float | 每股送股（送股比例） |
| `rationed_ps` | float | 每股转增（资本公积转增比例） |
| `rationed_px` | float | 配股价格 |
| `bonus_ps` | float | 每股现金分红（税前） |
| `dividend` | float | 现金红利（同 bonus_ps） |
| `exer_forward_a` | float | **前复权因子 A**（乘数） |
| `exer_forward_b` | float | **前复权因子 B**（偏移量） |

> **前复权公式**：`前复权价 = exer_forward_a × 未复权价 + exer_forward_b`
>
> 这两个字段由脚本从原始分红数据通过反向累积算法自动计算，回测引擎 `get_history(fq="dypre")` 依赖它们做动态前复权。

#### Phase 3: 季度基本面 → `fundamentals/`

`fundamentals/{symbol}.parquet`：合并 5 个 BaoStock API 的数据

| 列名 | 来源 API | 说明 |
|------|----------|------|
| `publ_date` | 全部 | 公告日期 |
| `end_date` | 全部 | 季度结束日 |
| `total_shares` | profit | 总股本 |
| `a_floats` | profit | 流通 A 股 |
| `roe` | profit | 净资产收益率 |
| `roa` | profit | 总资产收益率 |
| `net_profit_ratio` | profit | 净利率 |
| `gross_income_ratio` | profit | 毛利率 |
| `operating_revenue_grow_rate` | growth | 营收同比增长率 |
| `net_profit_grow_rate` | growth | 净利润同比增长率 |
| `total_asset_grow_rate` | growth | 总资产增长率 |
| `current_ratio` | balance | 流动比率 |
| `quick_ratio` | balance | 速动比率 |
| `debt_equity_ratio` | balance | 资产负债率 |
| `accounts_receivables_turnover_rate` | operation | 应收账款周转率 |
| `inventory_turnover_rate` | operation | 存货周转率 |
| `interest_cover` | cash_flow | 利息保障倍数 |

#### Phase 4: 5分钟K线 → `stocks_5m/`

`stocks_5m/{symbol}.parquet`：

| 列名 | 类型 | 说明 |
|------|------|------|
| (DatetimeIndex) | datetime | 时间戳索引 |
| `open` | float | 开盘价（不复权） |
| `high` | float | 最高价（不复权） |
| `low` | float | 最低价（不复权） |
| `close` | float | 收盘价（不复权） |
| `volume` | float | 成交量（股） |
| `money` | float | 成交额（元） |

> 每只股票约 13 个 3 月分段查询，3 个月 ≈ 4000 行，远低于 BaoStock 单次 10000 行上限。

---

## 6. 内置合规保护

项目在 fetcher 层级内置了以下保护，**无需手动处理**：

| 保护机制 | 说明 |
|----------|------|
| **日配额追踪** | 持久化到 `data/.baostock_daily_counter.json`，80% 警告，95% 自动停止 |
| **黑名单检测** | 每次 API 返回检查错误码 `10001011`，立即终止 |
| **请求间隔** | 内置 50ms 间隔，符合官方 10~50ms 推荐 |
| **日期保护** | 早于 2015-01-01 自动修正 |
| **断点续传** | 每阶段每只股票进度保存到 `download_progress.json` |

### 配额估算

| 阶段 | 请求量 | 占日配额 |
|------|--------|----------|
| Phase 0: 元数据 | ~700 | 1.4% |
| Phase 1: 日线+估值 | ~5,000 | 10% |
| Phase 2: 除权因子 | ~20,000~55,000 | 40~110% |
| Phase 3: 季度基本面 | ~60,000/天 | 120%/天（需多天） |
| Phase 4: 5分钟线 | ~32,500/天 | 65%/天 |

> **建议**：Phase 2 和 Phase 3 可能需要 1 天以上，脚本会自动在 95% 配额时停止，次日同一命令继续即可。

---

## 7. BaoStock API 参考

### 7.1 基本使用流程

```python
import baostock as bs

lg = bs.login()
# 登录 → 查询 → 遍历结果 → 登出
rs = bs.query_history_k_data_plus("sh.600000", fields="...", start_date="2023-01-01", end_date="2026-03-31", frequency="d", adjustflag="3")
while rs.error_code == "0" and rs.next():
    row = rs.get_row_data()
bs.logout()
```

### 7.2 主要接口

| 接口 | 用途 |
|---|---|
| `query_history_k_data_plus(code, fields, start_date, end_date, frequency, adjustflag)` | K 线数据（日/周/月/5/15/30/60分钟） |
| `query_all_stock(date)` | 查询指定日期所有证券列表 |
| `query_stock_basic(code)` | 查询证券基本信息 |
| `query_stock_industry(code)` | 查询证券所属行业 |
| `query_profit/growth/balance/operation/cash_flow_data(code, year, quarter)` | 季度财务数据（5 个 API） |
| `query_dividend_data(code, year, yearType)` | 除权除息数据 |
| `query_adjust_factor(code, start_date, end_date)` | 复权因子 |
| `query_trade_dates(start_date, end_date)` | 交易日历 |
| `query_sz50/hs300/zz500_stocks(date)` | 指数成分股 |

### 7.3 证券代码规则

| 市场 | BaoStock 格式 | 示例 |
|---|---|---|
| 上海 | `sh.XXXXXX` | `sh.600000`（主板）、`sh.688xxx`（科创板） |
| 深圳 | `sz.XXXXXX` | `sz.000001`（主板）、`sz.300xxx`（创业板） |

### 7.4 关键限制

| 限制项 | 值 |
|---|---|
| 每日请求上限 | **50,000 次**（按 IP，超限入黑名单） |
| 单次查询记录上限 | **10,000 条**（自动分页） |
| 历史数据起始 | **2015-01-01** |
| 黑名单错误码 | `10001011` |
| 网络错误码 | `10002001`~`10002008` |

---

## 8. 错误码速查

| 错误码 | 含义 | 处理 |
|--------|------|------|
| `0` | 成功 | — |
| `10001005` | 登录数达到上限 | 等 1~2 分钟重试 |
| `10001011` | **IP 黑名单** | 联系 QQ 群管理员解封 |
| `10002007` | 网络接收错误 | 服务器暂不可用，等几分钟后重试 |
| `10004013` | 超出日期范围 | 起始日期不能早于 2015-01-01 |
