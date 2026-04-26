# -*- coding: utf-8 -*-
"""
5分钟数据聚合验证

将5分钟K线聚合为60分钟和日线，与实际下载的数据比对，验证正确性。

Usage:
    python scripts/validate_5min_aggregation.py
    python scripts/validate_5min_aggregation.py --sample 50    # 每组抽样50只
    python scripts/validate_5min_aggregation.py --tol 0.01     # 价格容差
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── paths ──────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent / "data" / "cn"
BASE_LAB = Path(__file__).resolve().parent.parent.parent / "SimTradeLab" / "data" / "cn"

DIR_5M = BASE / "stocks_5m"
DIR_60M = BASE_LAB / "stocks_60m"
DIR_DAILY = BASE / "stocks"

# ── aggregation helpers ────────────────────────────────────────────────

# A-share 60min bar end-times
PERIOD_MAP = {
    0: "10:30",   # 9:35 ~ 10:30 (slot 0)
    1: "11:30",   # 10:35 ~ 11:30 (slot 1)
    2: "14:00",   # 13:05 ~ 14:00 (slot 2)
    3: "15:00",   # 14:05 ~ 15:00 (slot 3)
}


def _slot_index(dt: pd.Timestamp) -> int:
    """Assign a 5min bar to one of 4 hourly slots."""
    h, m = dt.hour, dt.minute
    if h == 9 or (h == 10 and m <= 30):
        return 0
    if h == 10 or (h == 11 and m <= 30):
        return 1
    if h == 13 or (h == 14 and m == 0):
        return 2
    return 3


def agg_5min_to_60min(df5: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 5min bars into 60min bars (4 bars/day)."""
    df = df5.copy()
    df["date"] = df.index.date
    df["slot"] = df.index.map(_slot_index)

    agg = (
        df.groupby(["date", "slot"])
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            money=("money", "sum"),
        )
        .reset_index()
    )
    return agg


def agg_5min_to_daily(df5: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 5min bars into daily bars."""
    df = df5.copy()
    df["date"] = df.index.date

    agg = (
        df.groupby("date")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            money=("money", "sum"),
        )
        .reset_index()
    )
    return agg


# ── loading helpers ────────────────────────────────────────────────────

def load_5min(symbol: str) -> pd.DataFrame:
    fp = DIR_5M / f"{symbol}.parquet"
    if not fp.exists():
        return pd.DataFrame()
    df = pd.read_parquet(fp)
    # Some files have datetime as column (RangeIndex), others as DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex) and "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
    return df


def load_60min(symbol: str) -> pd.DataFrame:
    fp = DIR_60M / f"{symbol}.parquet"
    return pd.read_parquet(fp) if fp.exists() else pd.DataFrame()


def load_daily(symbol: str) -> pd.DataFrame:
    fp = DIR_DAILY / f"{symbol}.parquet"
    return pd.read_parquet(fp) if fp.exists() else pd.DataFrame()


def parse_60min_time(t: str) -> tuple:
    """Parse 60min 'time' column → (date_str, HH:MM)."""
    # t = '20250102103000000' → date='2025-01-02', time='10:30'
    return f"{t[:4]}-{t[4:6]}-{t[6:8]}", f"{t[8:10]}:{t[10:12]}"


# ── comparison logic ───────────────────────────────────────────────────

def compare_60min(symbol: str, tol: float) -> dict:
    """Compare aggregated 5min→60min against real 60min for one symbol."""
    df5 = load_5min(symbol)
    df60 = load_60min(symbol)
    if df5.empty or df60.empty:
        return {"symbol": symbol, "status": "missing_data"}

    # Aggregate
    agg = agg_5min_to_60min(df5)
    if agg.empty:
        return {"symbol": symbol, "status": "agg_empty"}

    # Build comparable DataFrame from aggregated
    agg_rows = {}
    for _, row in agg.iterrows():
        d = str(row["date"])
        slot = row["slot"]
        hm = PERIOD_MAP[slot]
        key = f"{d}_{hm}"
        agg_rows[key] = {
            "date": d,
            "time": hm,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "money": row["money"],
        }
    agg_df = pd.DataFrame(agg_rows).T

    # Parse real 60min data
    real_rows = {}
    for _, row in df60.iterrows():
        d, hm = parse_60min_time(str(row["time"]))
        key = f"{d}_{hm}"
        real_rows[key] = {
            "date": d,
            "time": hm,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "money": row["amount"],
        }
    real_df = pd.DataFrame(real_rows).T

    # Find overlapping keys (only compare dates in both)
    common_keys = set(agg_df.index) & set(real_df.index)
    if not common_keys:
        return {"symbol": symbol, "status": "no_overlap", "agg_range": f"{len(agg_df)} bars", "real_range": f"{len(real_df)} bars"}

    result = {"symbol": symbol, "status": "ok", "overlap_bars": len(common_keys)}
    price_fields = ["open", "high", "low", "close"]
    vol_fields = ["volume", "money"]
    all_fields = price_fields + vol_fields

    total_cells = 0
    match_cells = 0
    mismatches = {f: 0 for f in all_fields}

    for key in sorted(common_keys):
        a = agg_df.loc[key]
        r = real_df.loc[key]
        for f in all_fields:
            total_cells += 1
            av = float(a[f])
            rv = float(r[f])
            if f in price_fields:
                ok = abs(av - rv) <= tol
            else:
                ok = abs(av - rv) <= max(tol, rv * 1e-4)  # 0.01% tolerance for volume
            if ok:
                match_cells += 1
            else:
                mismatches[f] += 1

    result["total_cells"] = total_cells
    result["match_cells"] = match_cells
    result["match_rate"] = match_cells / total_cells * 100 if total_cells else 0
    result["mismatches"] = mismatches
    return result


def compare_daily(symbol: str, tol: float) -> dict:
    """Compare aggregated 5min→daily against real daily for one symbol."""
    df5 = load_5min(symbol)
    dfd = load_daily(symbol)
    if df5.empty or dfd.empty:
        return {"symbol": symbol, "status": "missing_data"}

    agg = agg_5min_to_daily(df5)
    if agg.empty:
        return {"symbol": symbol, "status": "agg_empty"}

    # Build comparable: agg has 'date' column with date objects
    agg["date_str"] = agg["date"].apply(lambda d: str(d)[:10])
    agg_idx = agg.set_index("date_str")

    # Real daily: date is a column (not index), stored as string or date
    if "date" in dfd.columns:
        dfd["date_str"] = dfd["date"].apply(lambda d: str(d)[:10])
    else:
        dfd["date_str"] = dfd.index.astype(str).str[:10]
    real_idx = dfd.set_index("date_str")

    common = set(agg_idx.index) & set(real_idx.index)
    if not common:
        return {"symbol": symbol, "status": "no_overlap"}

    result = {"symbol": symbol, "status": "ok", "overlap_days": len(common)}
    price_fields = ["open", "high", "low", "close"]
    vol_fields = ["volume", "money"]
    all_fields = price_fields + vol_fields

    total_cells = 0
    match_cells = 0
    mismatches = {f: 0 for f in all_fields}
    # Track some example mismatches
    examples = []

    for d in sorted(common):
        a = agg_idx.loc[d]
        r = real_idx.loc[d]
        for f in all_fields:
            total_cells += 1
            av = float(a[f])
            rv = float(r[f])
            if f in price_fields:
                ok = abs(av - rv) <= tol
            else:
                ok = abs(av - rv) <= max(tol, rv * 1e-4)
            if ok:
                match_cells += 1
            else:
                mismatches[f] += 1
                if len(examples) < 10:
                    examples.append(f"{d} {f}: agg={av:.2f} real={rv:.2f} diff={av-rv:.2f}")

    result["total_cells"] = total_cells
    result["match_cells"] = match_cells
    result["match_rate"] = match_cells / total_cells * 100 if total_cells else 0
    result["mismatches"] = mismatches
    result["examples"] = examples
    return result


# ── main ───────────────────────────────────────────────────────────────

def select_stocks(n: int) -> dict:
    """Select n stocks for each prefix group (00, 30, 60)."""
    s5m = set(f.stem for f in DIR_5M.glob("*.parquet"))
    s60m = set(f.stem for f in DIR_60M.glob("*.parquet"))
    sd = set(f.stem for f in DIR_DAILY.glob("*.parquet"))
    common = sorted(s5m & s60m & sd)

    rng = np.random.RandomState(42)
    groups = {}
    for prefix in ["00", "30", "60"]:
        pool = [s for s in common if s.startswith(prefix)]
        rng.shuffle(pool)
        groups[prefix] = pool[:n]

    return groups


def main():
    parser = argparse.ArgumentParser(description="Validate 5min aggregation against real data")
    parser.add_argument("--sample", type=int, default=100, help="Stocks per group (default: 100)")
    parser.add_argument("--tol", type=float, default=0.005, help="Price tolerance (default: 0.005)")
    args = parser.parse_args()

    print("=" * 70)
    print("5分钟聚合验证")
    print("=" * 70)

    groups = select_stocks(args.sample)
    total_selected = sum(len(v) for v in groups.values())
    print(f"抽样: {args.sample} 只/组 × 3 组 = {total_selected} 只")
    print(f"价格容差: {args.tol}")
    print(f"数据目录:")
    print(f"  5min:  {DIR_5M}")
    print(f"  60min: {DIR_60M}")
    print(f"  daily: {DIR_DAILY}")
    print()

    # ── 60min comparison ──────────────────────────────────────────
    print("─" * 70)
    print("【60分钟聚合验证】5min → 60min vs 实际60min")
    print("─" * 70)

    results_60 = []
    for prefix, stocks in groups.items():
        for sym in stocks:
            r = compare_60min(sym, args.tol)
            r["prefix"] = prefix
            results_60.append(r)

    ok_60 = [r for r in results_60 if r["status"] == "ok"]
    print(f"\n有效比对: {len(ok_60)}/{len(results_60)}")

    if ok_60:
        rates = [r["match_rate"] for r in ok_60]
        total_cells = sum(r["total_cells"] for r in ok_60)
        total_match = sum(r["match_cells"] for r in ok_60)
        print(f"总匹配率: {total_match}/{total_cells} = {total_match/total_cells*100:.4f}%")
        print(f"按股匹配率: min={min(rates):.2f}%, median={np.median(rates):.2f}%, "
              f"max={max(rates):.2f}%, mean={np.mean(rates):.2f}%")

        # Mismatch breakdown
        field_totals = {}
        for r in ok_60:
            for f, cnt in r["mismatches"].items():
                field_totals[f] = field_totals.get(f, 0) + cnt
        if any(v > 0 for v in field_totals.values()):
            print("字段不匹配数:", {k: v for k, v in field_totals.items() if v > 0})
        else:
            print("所有字段完全匹配！")

        # Per-group stats
        for prefix in ["00", "30", "60"]:
            grp = [r for r in ok_60 if r["prefix"] == prefix]
            if grp:
                rates_g = [r["match_rate"] for r in grp]
                print(f"  {prefix}*: {len(grp)} 只, "
                      f"mean={np.mean(rates_g):.2f}%, min={min(rates_g):.2f}%")

    # Show non-ok statuses
    for r in results_60:
        if r["status"] != "ok":
            print(f"  [skip] {r['symbol']}: {r['status']}")

    # ── daily comparison ──────────────────────────────────────────
    print()
    print("─" * 70)
    print("【日线聚合验证】5min → daily vs 实际日线")
    print("─" * 70)

    results_d = []
    for prefix, stocks in groups.items():
        for sym in stocks:
            r = compare_daily(sym, args.tol)
            r["prefix"] = prefix
            results_d.append(r)

    ok_d = [r for r in results_d if r["status"] == "ok"]
    print(f"\n有效比对: {len(ok_d)}/{len(results_d)}")

    if ok_d:
        rates = [r["match_rate"] for r in ok_d]
        total_cells = sum(r["total_cells"] for r in ok_d)
        total_match = sum(r["match_cells"] for r in ok_d)
        print(f"总匹配率: {total_match}/{total_cells} = {total_match/total_cells*100:.4f}%")
        print(f"按股匹配率: min={min(rates):.2f}%, median={np.median(rates):.2f}%, "
              f"max={max(rates):.2f}%, mean={np.mean(rates):.2f}%")

        # Mismatch breakdown
        field_totals = {}
        for r in ok_d:
            for f, cnt in r["mismatches"].items():
                field_totals[f] = field_totals.get(f, 0) + cnt
        if any(v > 0 for v in field_totals.values()):
            print("字段不匹配数:", {k: v for k, v in field_totals.items() if v > 0})

            # Show examples
            all_examples = []
            for r in ok_d:
                all_examples.extend(r.get("examples", []))
            if all_examples:
                print("\n不匹配示例 (前10):")
                for ex in all_examples[:10]:
                    print(f"  {ex}")
        else:
            print("所有字段完全匹配！")

        # Per-group stats
        for prefix in ["00", "30", "60"]:
            grp = [r for r in ok_d if r["prefix"] == prefix]
            if grp:
                rates_g = [r["match_rate"] for r in grp]
                print(f"  {prefix}*: {len(grp)} 只, "
                      f"mean={np.mean(rates_g):.2f}%, min={min(rates_g):.2f}%")

    # Show non-ok statuses
    for r in results_d:
        if r["status"] != "ok":
            print(f"  [skip] {r['symbol']}: {r['status']}")

    print()
    print("=" * 70)
    print("验证完成")


if __name__ == "__main__":
    main()
