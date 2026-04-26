# -*- coding: utf-8 -*-
"""
BaoStock 全套 A 股数据下载 (2023-01 ~ 2026-03)

统一从 BaoStock 下载所有数据类型，消除多数据源不一致问题。
支持断点续传、限流、分阶段执行。

内置合规保护:
- 日配额追踪 (50,000 次/IP/天, 95% 自动停止)
- 黑名单检测 (错误码 10001011, 立即终止)
- 请求间隔 (50ms, 符合官方推荐 10-50ms)

Usage:
    python scripts/download_baostock_full.py                          # 全量下载(自动续传)
    python scripts/download_baostock_full.py --phase daily            # 仅日线
    python scripts/download_baostock_full.py --phase 5min             # 仅5分钟线
    python scripts/download_baostock_full.py --phase exrights         # 仅除权因子
    python scripts/download_baostock_full.py --phase fundamentals     # 仅基本面
    python scripts/download_baostock_full.py --status                 # 查看进度
    python scripts/download_baostock_full.py --stocks 000001.SZ,600519.SS  # 指定股票
    python scripts/download_baostock_full.py --force                  # 忽略续传
"""

import argparse
import platform

if platform.system() != "Windows":
    import fcntl
else:
    fcntl = None
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from tqdm import tqdm

from simtradedata.fetchers.baostock_5min_fetcher import BaoStock5MinFetcher
from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.fetchers.unified_fetcher import UnifiedDataFetcher
from simtradedata.processors.data_splitter import DataSplitter
from simtradedata.resilience.baostock_rate_limiter import (
    BaoStockBlacklistError,
    BaoStockDailyLimitExceeded,
    get_baostock_rate_limiter,
)
from simtradedata.utils.code_utils import (
    convert_from_ptrade_code,
    convert_to_ptrade_code,
)

# ── 路径 ───────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "SimTradeData" / "data" / "cn"
START_DATE = "2023-01-01"
END_DATE = "2026-03-31"

# ── A 股过滤 ──────────────────────────────────────────────────────────
A_SHARE_PREFIXES = {
    "600", "601", "603", "605", "688", "689",  # 上海
    "000", "001", "002", "003", "300", "301",  # 深圳
}

# 指数代码 (从股票列表排除)
INDEX_CODES = {
    "000300.SS", "000016.SS", "000905.SS", "000001.SS",
    "399001.SZ", "399006.SZ", "000688.SS",
}

# 指数成分股采样代码
INDEX_SAMPLE_CODES = ["000016.SS", "000300.SS", "000905.SS"]

# 创业板/科创板前缀 (20%涨跌幅)
CHINEXT_STAR_PREFIXES = {"300", "301", "688", "689"}

# ── 限流 (fetcher 层已内置 50ms 间隔 + 日配额追踪) ──────────────────────
BASE_DELAY = 0        # fetcher 层 _baostock_api_call 已处理 50ms 间隔
MINUTE_DELAY = 0      # 同上
BATCH_SIZE = 500
BATCH_PAUSE = 3.0
MAX_BACKOFF = 30.0
MAX_RETRIES = 3
CONSECUTIVE_FAIL_THRESHOLD = 10
LONG_PAUSE = 60.0

# ── 日志 ───────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent.parent / "data"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "download_baostock_full.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
#  工具类
# ══════════════════════════════════════════════════════════════════════

class ProcessLock:
    """进程锁，防止并发运行"""

    def __init__(self, lock_file: str):
        self.lock_file = Path(lock_file)
        self.lock_fd = None

    def __enter__(self):
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        if fcntl is None:
            # Windows: msvcrt-based file lock
            import msvcrt
            try:
                self.lock_fd = open(self.lock_file, "w")
                msvcrt.locking(self.lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
                self.lock_fd.write(str(os.getpid()))
                self.lock_fd.flush()
            except (IOError, OSError):
                print(f"\n错误: 另一个下载进程正在运行。如确认无其他进程，删除锁文件:\n  {self.lock_file}")
                sys.exit(1)
        else:
            # Unix: fcntl flock
            self.lock_fd = open(self.lock_file, "w")
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.lock_fd.write(str(os.getpid()))
                self.lock_fd.flush()
            except IOError:
                print(f"\n错误: 另一个下载进程正在运行。如确认无其他进程，删除锁文件:\n  {self.lock_file}")
                sys.exit(1)
        return self

    def __exit__(self, *args):
        if self.lock_fd:
            try:
                if fcntl is None:
                    import msvcrt
                    self.lock_fd.seek(0)
                    msvcrt.locking(self.lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
            except Exception:
                pass
            try:
                self.lock_file.unlink(missing_ok=True)
            except Exception:
                pass


class ProgressTracker:
    """断点续传进度追踪"""

    def __init__(self, progress_file: Path):
        self.file = progress_file
        self.data = self._load()

    def _load(self) -> dict:
        if self.file.exists():
            try:
                with open(self.file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"phases": {}, "meta": {}}

    def save(self):
        self.data["updated_at"] = datetime.now().isoformat()
        with open(self.file, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def is_phase_done(self, phase: str) -> bool:
        return self.data.get("phases", {}).get(phase, {}).get("status") == "completed"

    def mark_phase_done(self, phase: str):
        self.data.setdefault("phases", {})[phase] = {
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
        }
        self.save()

    def get_done_stocks(self, phase: str) -> set:
        phase_data = self.data.get("phases", {}).get(phase, {})
        return set(phase_data.get("done_stocks", []))

    def mark_stock_done(self, phase: str, symbol: str):
        phase_data = self.data.setdefault("phases", {}).setdefault(phase, {"status": "in_progress", "done_stocks": []})
        phase_data["status"] = "in_progress"
        done = set(phase_data.get("done_stocks", []))
        done.add(symbol)
        phase_data["done_stocks"] = sorted(done)
        self.save()

    def get_done_quarters(self, phase: str) -> set:
        phase_data = self.data.get("phases", {}).get(phase, {})
        return set(phase_data.get("done_quarters", []))

    def mark_quarter_done(self, phase: str, quarter_key: str):
        phase_data = self.data.setdefault("phases", {}).setdefault(phase, {"status": "in_progress", "done_stocks": [], "done_quarters": []})
        phase_data["status"] = "in_progress"
        done = set(phase_data.get("done_quarters", []))
        done.add(quarter_key)
        phase_data["done_quarters"] = sorted(done)
        self.save()

    def mark_stock_done_quarter(self, phase: str, symbol: str):
        """标记基本面某只股票某个季度已完成 (通过 done_stocks 记录 symbol:Q key)"""
        self.mark_stock_done(phase, symbol)


class RateLimiter:
    """限流器"""

    def __init__(self, base=BASE_DELAY, minute=MINUTE_DELAY):
        self.base = base
        self.minute = minute
        self.consecutive_fails = 0

    def wait(self, is_minute=False):
        time.sleep(self.minute if is_minute else self.base)

    def wait_retry(self, attempt):
        delay = min(self.base * (2 ** attempt), MAX_BACKOFF)
        time.sleep(delay)

    def record_success(self):
        self.consecutive_fails = 0

    def record_failure(self):
        self.consecutive_fails += 1
        if self.consecutive_fails >= CONSECUTIVE_FAIL_THRESHOLD:
            logger.warning(f"连续失败 {self.consecutive_fails} 次，等待 {LONG_PAUSE}s...")
            time.sleep(LONG_PAUSE)
            self.consecutive_fails = 0


# ══════════════════════════════════════════════════════════════════════
#  主下载器
# ══════════════════════════════════════════════════════════════════════

class BaoStockFullDownloader:
    def __init__(self, output_dir: Path, start: str, end: str,
                 rate_limiter: RateLimiter, progress: ProgressTracker,
                 force: bool = False, quarter: str = None,
                 stock_range: tuple = None):
        self.output = output_dir
        self.start = start
        self.end = end
        self.rl = rate_limiter
        self.progress = progress
        self.force = force
        self.quarter = quarter           # e.g. "2024Q3" — 仅处理该季度
        self.stock_range = stock_range   # e.g. (0, 1377) — 股票池切片

        self.unified = UnifiedDataFetcher()
        self.standard = BaoStockFetcher()
        self.min_fetcher = BaoStock5MinFetcher(timeout=60)
        self.splitter = DataSplitter()
        self.stock_pool = []

        # 统计
        self.stats = {}

    def ensure_dirs(self):
        for subdir in ["stocks", "stocks_5m", "exrights",
                        "fundamentals", "valuation", "metadata"]:
            (self.output / subdir).mkdir(parents=True, exist_ok=True)

    def _slice_pool(self) -> list:
        """按 --stock-range 切片股票池"""
        if self.stock_range:
            lo, hi = self.stock_range
            return self.stock_pool[lo:hi]
        return self.stock_pool

    def _range_label(self) -> str:
        """生成范围标签，用于日志"""
        if self.stock_range:
            lo, hi = self.stock_range
            return f" [{lo}:{hi}]"
        return ""

    # ── 股票池发现 ─────────────────────────────────────────────────
    def discover_stock_pool(self) -> list:
        """获取 A 股股票池: 优先从已有日线目录读取，fallback 到 BaoStock API"""
        if self.stock_pool:
            logger.info(f"使用指定股票池: {len(self.stock_pool)} 只")
            return self.stock_pool

        # 1. 从已有 cn/stocks 目录读取 (快速、可靠)
        cn_stocks_dir = PROJECT_ROOT / "SimTradeData" / "data" / "cn" / "stocks"
        if cn_stocks_dir.exists():
            def is_a_share(code: str) -> bool:
                prefix = code.split(".")[0][:3]
                return prefix in A_SHARE_PREFIXES and code not in INDEX_CODES
            codes = sorted(
                s for s in (f.stem for f in cn_stocks_dir.glob("*.parquet"))
                if is_a_share(s)
            )
            if codes:
                self.stock_pool = codes
                logger.info(f"从已有日线读取股票池: {len(codes)} 只 A 股")
                return self.stock_pool

        # 2. 从输出目录已有数据读取
        out_stocks_dir = self.output / "stocks"
        if out_stocks_dir.exists():
            codes = sorted(f.stem for f in out_stocks_dir.glob("*.parquet"))
            if codes:
                self.stock_pool = codes
                logger.info(f"从输出目录读取股票池: {len(codes)} 只")
                return self.stock_pool

        # 3. Fallback: BaoStock API (可能较慢或超时)
        logger.info("尝试从 BaoStock API 发现股票池...")
        import baostock as bs

        all_stocks = set()
        current = pd.Timestamp(self.start)
        end_ts = pd.Timestamp(self.end)
        timeout_per_query = 10  # 秒

        while current <= end_ts:
            date_str = current.strftime("%Y-%m-%d")
            try:
                import signal
                def _handler(signum, frame):
                    raise TimeoutError()
                signal.signal(signal.SIGALRM, _handler)
                signal.alarm(timeout_per_query)
                rs = bs.query_all_stock(day=date_str)
                signal.alarm(0)
                if rs and rs.error_code == "0":
                    while rs.next():
                        code = rs.get_row_data()[0]
                        ptrade_code = convert_to_ptrade_code(code, "baostock")
                        all_stocks.add(ptrade_code)
                self.rl.wait()
            except Exception as e:
                logger.warning(f"采样 {date_str} 失败: {e}")
                try:
                    signal.alarm(0)
                except Exception:
                    pass

            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        def is_a_share(code: str) -> bool:
            prefix = code.split(".")[0][:3]
            return prefix in A_SHARE_PREFIXES and code not in INDEX_CODES

        self.stock_pool = sorted(s for s in all_stocks if is_a_share(s))
        logger.info(f"股票池: {len(self.stock_pool)} 只 A 股")
        return self.stock_pool

    # ── Phase 0: 元数据 ───────────────────────────────────────────
    def phase0_metadata(self):
        phase = "phase0_metadata"
        if self.progress.is_phase_done(phase) and not self.force:
            logger.info("[Phase 0] 已完成，跳过")
            return

        logger.info("=" * 60)
        logger.info("[Phase 0] 下载元数据")
        logger.info("=" * 60)

        import baostock as bs

        # 交易日历
        logger.info("  交易日历...")
        try:
            cal = self.standard.fetch_trade_calendar(self.start, self.end)
            if not cal.empty:
                trade_days = cal[cal["is_trading_day"] == "1"].rename(
                    columns={"calendar_date": "trade_date"}
                )
                trade_days[["trade_date"]].to_parquet(
                    self.output / "metadata" / "trade_days.parquet", index=False
                )
                logger.info(f"    {len(trade_days)} 个交易日")
        except Exception as e:
            logger.error(f"    交易日历失败: {e}")

        self.rl.wait()

        # 基准指数 (CSI300)
        logger.info("  基准指数 (CSI300)...")
        try:
            bench = self.unified.fetch_index_data("000300.SS", self.start, self.end)
            if not bench.empty:
                bench_out = bench.rename(columns={"amount": "money"})
                if "date" not in bench_out.columns and bench_out.index.name == "date":
                    bench_out = bench_out.reset_index()
                cols = [c for c in ["date", "open", "high", "low", "close", "volume", "money"]
                        if c in bench_out.columns]
                bench_out[cols].to_parquet(
                    self.output / "metadata" / "benchmark.parquet", index=False
                )
                logger.info(f"    {len(bench_out)} 天")
        except Exception as e:
            logger.error(f"    基准指数失败: {e}")

        self.rl.wait()

        # 指数成分股
        logger.info("  指数成分股...")
        rows = []
        # 月末采样日期
        dates = pd.date_range(self.start, self.end, freq="ME")
        for d in dates:
            for idx_code in INDEX_SAMPLE_CODES:
                try:
                    df = self.standard.fetch_index_stocks(idx_code, d.strftime("%Y-%m-%d"))
                    if not df.empty:
                        codes = [convert_to_ptrade_code(c, "baostock") for c in df["code"]]
                        rows.append({
                            "date": d.strftime("%Y%m%d"),
                            "index_code": idx_code,
                            "symbols": json.dumps(codes, ensure_ascii=False),
                        })
                except Exception as e:
                    logger.warning(f"    {idx_code} {d.strftime('%Y-%m-%d')}: {e}")
                self.rl.wait()

        if rows:
            pd.DataFrame(rows).to_parquet(
                self.output / "metadata" / "index_constituents.parquet", index=False
            )
            logger.info(f"    {len(rows)} 条记录")

        # 股票元信息
        logger.info("  股票基本信息...")
        meta_rows = []
        for sym in self.stock_pool[:500]:
            try:
                basic = self.standard.fetch_stock_basic(sym)
                if not basic.empty:
                    info = {"symbol": sym}
                    for col in ["code_name", "ipoDate", "outDate", "status"]:
                        if col in basic.columns:
                            info[col] = basic[col].values[0]
                    meta_rows.append(info)
            except Exception:
                pass
            self.rl.wait()

        if meta_rows:
            pd.DataFrame(meta_rows).to_parquet(
                self.output / "metadata" / "stock_metadata.parquet", index=False
            )
            logger.info(f"    {len(meta_rows)} 只")

        self.progress.mark_phase_done(phase)
        logger.info("[Phase 0] 完成")

    # ── Phase 1: 日线+估值 ────────────────────────────────────────
    def phase1_daily(self):
        phase = "phase1_daily"
        done = self.progress.get_done_stocks(phase) if not self.force else set()
        pending = [s for s in self.stock_pool if s not in done]

        logger.info("=" * 60)
        logger.info(f"[Phase 1] 日线+估值: {len(pending)} 只待下载 (已完成 {len(done)})")
        logger.info("=" * 60)

        ok = no_data = fail = 0
        for i, sym in enumerate(tqdm(pending, desc="日线", ncols=100)):
            for attempt in range(MAX_RETRIES):
                try:
                    df = self.unified.fetch_unified_daily_data(sym, self.start, self.end)
                    if df.empty:
                        no_data += 1
                        break

                    split = self.splitter.split_data(df)

                    # 日线
                    market = split.get("market")
                    if market is not None and not market.empty:
                        mkt = market.copy()
                        if "amount" in mkt.columns:
                            mkt = mkt.rename(columns={"amount": "money"})

                        # DataSplitter 返回 date 为 DatetimeIndex，需转回列
                        if "date" not in mkt.columns and mkt.index.name == "date":
                            mkt = mkt.reset_index()

                        # 复权因子会从 BaoStock 获取 preclose
                        # 计算 high_limit / low_limit
                        mkt = self._add_price_limits(mkt, sym)

                        cols = ["date", "open", "close", "high", "low",
                                "high_limit", "low_limit", "preclose", "volume", "money"]
                        available = [c for c in cols if c in mkt.columns]
                        mkt[available].to_parquet(
                            self.output / "stocks" / f"{sym}.parquet", index=False
                        )

                    # 估值
                    val = split.get("valuation")
                    if val is not None and not val.empty:
                        val_out = val.copy()
                        # DataSplitter 返回 date 为 DatetimeIndex
                        if "date" not in val_out.columns and val_out.index.name == "date":
                            val_out = val_out.reset_index()
                        rename_map = {
                            "peTTM": "pe_ttm", "pbMRQ": "pb",
                            "psTTM": "ps_ttm", "pcfNcfTTM": "pcf",
                            "turn": "turnover_rate",
                        }
                        val_out = val_out.rename(columns=rename_map)
                        val_cols = [c for c in ["date", "pe_ttm", "pb", "ps_ttm", "pcf", "turnover_rate"]
                                    if c in val_out.columns]
                        val_out[val_cols].to_parquet(
                            self.output / "valuation" / f"{sym}.parquet", index=False
                        )

                    ok += 1
                    self.rl.record_success()
                    break

                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"  {sym}: 重试 {attempt+1} - {e}")
                        self.rl.wait_retry(attempt)
                    else:
                        logger.error(f"  {sym}: 失败 - {e}")
                        fail += 1
                        self.rl.record_failure()

            self.progress.mark_stock_done(phase, sym)
            self.rl.wait()

            if (i + 1) % BATCH_SIZE == 0:
                logger.info(f"  [batch] {i+1}/{len(pending)} ok={ok} no_data={no_data} fail={fail}")
                time.sleep(BATCH_PAUSE)

        self.progress.mark_phase_done(phase)
        logger.info(f"[Phase 1] 完成: ok={ok} no_data={no_data} fail={fail}")

    @staticmethod
    def _add_price_limits(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """计算涨跌停价格"""
        if "preclose" not in df.columns:
            df["high_limit"] = float("nan")
            df["low_limit"] = float("nan")
            return df

        prefix = symbol[:3]
        is_cx = prefix in CHINEXT_STAR_PREFIXES

        if is_cx:
            cutoff = pd.Timestamp("2020-08-24")
            dates = pd.to_datetime(df["date"])
            after = dates >= cutoff
            df["high_limit"] = float("nan")
            df["low_limit"] = float("nan")

            prec = pd.to_numeric(df["preclose"], errors="coerce")
            df.loc[after, "high_limit"] = (prec[after] * 1.20).round(2)
            df.loc[after, "low_limit"] = (prec[after] * 0.80).round(2)
            df.loc[~after, "high_limit"] = (prec[~after] * 1.10).round(2)
            df.loc[~after, "low_limit"] = (prec[~after] * 0.90).round(2)
        else:
            prec = pd.to_numeric(df["preclose"], errors="coerce")
            df["high_limit"] = (prec * 1.10).round(2)
            df["low_limit"] = (prec * 0.90).round(2)

        return df

    @staticmethod
    def _compute_forward_factors(df: pd.DataFrame) -> pd.DataFrame:
        """Compute exer_forward_a/b from raw dividend data.

        Algorithm: backward accumulation from most recent to oldest.
        Formula: 前复权价 = exer_forward_a * 未复权价 + exer_forward_b
        """
        if df.empty:
            return df

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
        return df

    # ── Phase 2: 除权除息 + 前复权因子 ────────────────────────────
    def phase2_exrights(self):
        phase = "phase2_exrights"
        done = self.progress.get_done_stocks(phase) if not self.force else set()
        pending = [s for s in self.stock_pool if s not in done]

        logger.info("=" * 60)
        logger.info(f"[Phase 2] 除权除息+前复权因子: {len(pending)} 只待下载 (已完成 {len(done)})")
        logger.info("=" * 60)

        ok = no_data = fail = 0
        for i, sym in enumerate(tqdm(pending, desc="除权因子", ncols=100)):
            for attempt in range(MAX_RETRIES):
                try:
                    start_year = int(self.start[:4])
                    end_year = int(self.end[:4])
                    df = self.standard.fetch_dividend_data_range(sym, start_year, end_year)
                    if df is not None and not df.empty:
                        # Compute exer_forward_a/b from raw dividend data
                        df = self._compute_forward_factors(df)
                        out_cols = [c for c in ["date", "allotted_ps", "rationed_ps",
                                                 "rationed_px", "bonus_ps", "dividend",
                                                 "exer_forward_a", "exer_forward_b"]
                                    if c in df.columns]
                        df[out_cols].to_parquet(
                            self.output / "exrights" / f"{sym}.parquet", index=False
                        )
                        ok += 1
                    else:
                        no_data += 1
                    self.rl.record_success()
                    break

                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"  {sym}: 重试 {attempt+1} - {e}")
                        self.rl.wait_retry(attempt)
                    else:
                        logger.error(f"  {sym}: 失败 - {e}")
                        fail += 1
                        self.rl.record_failure()

            self.progress.mark_stock_done(phase, sym)
            self.rl.wait()

            if (i + 1) % BATCH_SIZE == 0:
                logger.info(f"  [batch] {i+1}/{len(pending)} ok={ok} no_data={no_data} fail={fail}")
                time.sleep(BATCH_PAUSE)

        self.progress.mark_phase_done(phase)
        logger.info(f"[Phase 2] 完成: ok={ok} no_data={no_data} fail={fail}")

    # ── Phase 3: 季度基本面 ───────────────────────────────────────
    def phase3_fundamentals(self):
        phase = "phase3_fundamentals"
        done_stocks = self.progress.get_done_stocks(phase) if not self.force else set()
        done_quarters = self.progress.get_done_quarters(phase) if not self.force else set()

        start_y, start_m = int(self.start[:4]), int(self.start[5:7])
        end_y, end_m = int(self.end[:4]), int(self.end[5:7])

        quarters = []
        y, q = start_y, (start_m - 1) // 3 + 1
        while (y, q) <= (end_y, (end_m - 1) // 3 + 1):
            quarters.append((y, q))
            q += 1
            if q > 4:
                q = 1
                y += 1

        # 按 --quarter 过滤
        if self.quarter:
            qy, qq = int(self.quarter[:4]), int(self.quarter[5:])
            quarters = [(qy, qq)]

        pending_q = [(y, q) for y, q in quarters if f"{y}Q{q}" not in done_quarters]

        label = self.quarter or "全部"
        logger.info("=" * 60)
        logger.info(f"[Phase 3] 季度基本面 ({label}): {len(pending_q)} 个季度待下载, {len(self.stock_pool)} 只股票")
        logger.info("=" * 60)

        # 累加器: symbol -> list of DataFrames
        accum = {}

        for qi, (year, quarter) in enumerate(pending_q):
            qkey = f"{year}Q{quarter}"
            logger.info(f"  季度 {qi+1}/{len(pending_q)}: {qkey}")

            # 从季度级进度读取已完成股票
            q_done = self.progress.get_done_stocks(f"{phase}:{qkey}") if not self.force else set()

            q_ok = q_skip = 0
            stocks_to_do = [s for s in self.stock_pool
                            if s not in q_done]

            for sym in tqdm(stocks_to_do, desc=f"  {qkey}", ncols=100, leave=False):
                for attempt in range(MAX_RETRIES):
                    try:
                        df = self.standard.fetch_quarterly_fundamentals(sym, year, quarter)
                        if df is not None and not df.empty:
                            accum.setdefault(sym, []).append(df)
                            q_ok += 1
                        else:
                            q_skip += 1
                        self.rl.record_success()
                        break

                    except Exception as e:
                        if attempt < MAX_RETRIES - 1:
                            self.rl.wait_retry(attempt)
                        else:
                            logger.warning(f"    {sym} {qkey}: {e}")
                            self.rl.record_failure()

                self.progress.mark_stock_done(f"{phase}:{qkey}", sym)
                self.rl.wait()

            self.progress.mark_quarter_done(phase, qkey)
            logger.info(f"    {qkey}: ok={q_ok} skip={q_skip}")

        # 写出: 每只股票合并所有季度 (与已有文件追加合并)
        logger.info("  写出基本面 parquet...")
        written = 0
        for sym, dfs in accum.items():
            if dfs:
                try:
                    new = pd.concat(dfs, ignore_index=True)
                    fp = self.output / "fundamentals" / f"{sym}.parquet"
                    if fp.exists():
                        existing = pd.read_parquet(fp)
                        merged = pd.concat([existing, new], ignore_index=True).drop_duplicates()
                    else:
                        merged = new
                    if "end_date" in merged.columns:
                        merged = merged.sort_values("end_date")
                    merged.to_parquet(fp, index=False)
                    written += 1
                except Exception as e:
                    logger.warning(f"    {sym} 写出失败: {e}")

        # 仅当未指定 --quarter 时标记整个 phase 完成
        if not self.quarter:
            self.progress.mark_phase_done(phase)
        logger.info(f"[Phase 3] {self.quarter or '全部'} 完成: {written} 只写出")

    # ── Phase 4: 5 分钟线 ─────────────────────────────────────────
    def phase4_5min(self):
        phase = "phase4_5min"
        done = self.progress.get_done_stocks(phase) if not self.force else set()
        # 文件级续传
        pool = self._slice_pool()
        pending = [s for s in pool
                   if s not in done and not (self.output / "stocks_5m" / f"{s}.parquet").exists()]

        range_label = self._range_label()
        logger.info("=" * 60)
        logger.info(f"[Phase 4] 5分钟线{range_label}: {len(pending)} 只待下载")
        logger.info("=" * 60)

        ok = no_data = fail = 0
        for i, sym in enumerate(tqdm(pending, desc="5min", ncols=100)):
            for attempt in range(MAX_RETRIES):
                try:
                    df = self.min_fetcher.fetch_5min_bars(
                        sym, self.start, self.end, adjustflag="3", frequency="5"
                    )
                    if df.empty:
                        no_data += 1
                        break

                    out = df[["open", "high", "low", "close", "volume", "money"]].copy()
                    out.to_parquet(self.output / "stocks_5m" / f"{sym}.parquet")
                    ok += 1
                    self.rl.record_success()
                    break

                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"  {sym}: 重试 {attempt+1} - {e}")
                        self.rl.wait_retry(attempt)
                    else:
                        logger.error(f"  {sym}: 失败 - {e}")
                        fail += 1
                        self.rl.record_failure()

            self.progress.mark_stock_done(phase, sym)
            self.rl.wait(is_minute=True)

            if (i + 1) % BATCH_SIZE == 0:
                logger.info(f"  [batch] {i+1}/{len(pending)} ok={ok} no_data={no_data} fail={fail}")
                time.sleep(BATCH_PAUSE)

        self.progress.mark_phase_done(phase)
        logger.info(f"[Phase 4] 完成: ok={ok} no_data={no_data} fail={fail}")

    # ── 执行入口 ──────────────────────────────────────────────────
    def run(self, phases: str = "all"):
        self.ensure_dirs()

        # Log daily quota status
        limiter = get_baostock_rate_limiter()
        status = limiter.get_status()
        logger.info(
            "BaoStock daily quota: %d/%d (%.1f%%) used",
            status["count"], status["limit"], status["percentage"],
        )

        # 登录
        self.unified.login()
        self.standard.login()
        self.min_fetcher.login()

        try:
            # 股票池发现
            if phases in ("all", "metadata"):
                self.discover_stock_pool()
            elif not self.stock_pool:
                # 其他阶段需要股票池
                # 尝试从已有日线目录读取
                stocks_dir = self.output / "stocks"
                if stocks_dir.exists():
                    self.stock_pool = sorted(
                        f.stem for f in stocks_dir.glob("*.parquet")
                    )
                    logger.info(f"从已有日线读取股票池: {len(self.stock_pool)} 只")
                else:
                    logger.error("无股票池，请先运行 --phase metadata 或 --phase all")
                    return

            # 分阶段执行
            phase_map = {
                "metadata": self.phase0_metadata,
                "daily": self.phase1_daily,
                "exrights": self.phase2_exrights,
                "fundamentals": self.phase3_fundamentals,
                "5min": self.phase4_5min,
            }

            if phases == "all":
                for name, func in phase_map.items():
                    try:
                        func()
                    except BaoStockBlacklistError as e:
                        logger.critical("BLACKLISTED: %s", e)
                        print("\n*** CRITICAL: IP 被 BaoStock 黑名单 ***")
                        print("    %s" % e)
                        sys.exit(2)
                    except BaoStockDailyLimitExceeded as e:
                        logger.warning("Daily limit: %s", e)
                        print("\n*** 日配额已达上限 (%d%%), 停止下载 ***" % int(status["percentage"]))
                        print("    明日自动重置，可继续续传。")
                        return
            else:
                func = phase_map.get(phases)
                if func:
                    try:
                        func()
                    except BaoStockBlacklistError as e:
                        logger.critical("BLACKLISTED: %s", e)
                        print("\n*** CRITICAL: IP 被 BaoStock 黑名单 ***")
                        sys.exit(2)
                    except BaoStockDailyLimitExceeded as e:
                        logger.warning("Daily limit: %s", e)
                        print("\n*** 日配额已达上限, 停止下载 ***")
                        return
                else:
                    logger.error(f"未知阶段: {phases}，可选: {', '.join(phase_map.keys())}")

        finally:
            self.min_fetcher.logout()
            self.standard.logout()
            self.unified.logout()

    def show_status(self):
        """显示当前下载进度"""
        print("\n" + "=" * 60)
        print("BaoStock 全套数据下载 — 进度")
        print("=" * 60)
        print(f"输出目录: {self.output}")
        print(f"日期范围: {self.start} ~ {self.end}")
        print()

        for subdir in ["stocks", "stocks_5m", "exrights",
                        "fundamentals", "valuation", "metadata"]:
            d = self.output / subdir
            count = len(list(d.glob("*.parquet"))) if d.exists() else 0
            print(f"  {subdir:15s}: {count:>5} 文件")

        # 读取 progress
        if self.progress.file.exists():
            print()
            for phase_name, phase_data in self.progress.data.get("phases", {}).items():
                status = phase_data.get("status", "?")
                done = len(phase_data.get("done_stocks", []))
                print(f"  {phase_name:30s}: {status} ({done} done)")

        print("=" * 60)


# ══════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="BaoStock 全套 A 股数据下载")
    parser.add_argument("--phase", choices=[
        "all", "metadata", "daily", "exrights", "fundamentals", "5min"
    ], default="all", help="执行阶段 (default: all)")
    parser.add_argument("--start-date", default=START_DATE)
    parser.add_argument("--end-date", default=END_DATE)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--delay", type=float, default=BASE_DELAY)
    parser.add_argument("--minute-delay", type=float, default=MINUTE_DELAY)
    parser.add_argument("--force", action="store_true", help="忽略续传，重新下载")
    parser.add_argument("--status", action="store_true", help="显示当前进度")
    parser.add_argument("--stocks", type=str, help="指定股票代码 (逗号分隔)")
    parser.add_argument("--quarter", type=str, help="指定单个季度 (如 2024Q3), 用于 Phase 3 并行")
    parser.add_argument("--stock-range", type=str, help="指定股票范围 (如 0-1377), 用于 Phase 4/5 并行")
    parser.add_argument("--no-lock", action="store_true", help="跳过进程锁 (并行下载时使用)")
    args = parser.parse_args()

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)

    progress = ProgressTracker(output / "download_progress.json")
    rl = RateLimiter(base=args.delay, minute=args.minute_delay)

    # 解析 --stock-range
    stock_range = None
    if args.stock_range:
        parts = args.stock_range.split("-")
        stock_range = (int(parts[0]), int(parts[1]))

    dl = BaoStockFullDownloader(
        output_dir=output,
        start=args.start_date,
        end=args.end_date,
        rate_limiter=rl,
        progress=progress,
        force=args.force,
        quarter=args.quarter,
        stock_range=stock_range,
    )

    if args.stocks:
        dl.stock_pool = [s.strip() for s in args.stocks.split(",")]

    if args.status:
        dl.show_status()
        return

    if args.no_lock:
        dl.run(phases=args.phase)
    else:
        with ProcessLock(output / ".download.lock"):
            dl.run(phases=args.phase)

    dl.show_status()


if __name__ == "__main__":
    main()
