"""
验证脚本 v3（最终版）: 用5分钟K线聚合为30分钟K线，与BaoStock原生30分钟K线对比。

关键修正:
  - key格式统一: date串(YYYY-MM-DD) + time串(YYYYMMDDHHmmss00000)
  - stocks_5m仅有SZ股票，时间范围2025年
  - BaoStock 30m含2025+2026，仅比较重叠部分

聚合规则 (6根5m → 1根30m):
  open  = 第一根5m的open
  high  = 6根5m中high的最大值
  low   = 6根5m中low的最小值
  close = 最后一根5m的close
  volume = 6根volume之和
  amount = 6根money之和

BaoStock 30min每日8个时间窗口:
  10:00, 10:30, 11:00, 11:30, 13:30, 14:00, 14:30, 15:00
"""

import os
import numpy as np
import pandas as pd

BASE = r"C:\Users\Admin\SynologyDrive\PtradeProjects\SimTradeLab\data\cn"
DIR_5M = os.path.join(BASE, "stocks_5m")
DIR_30M = os.path.join(BASE, "stocks_30m")

# 抽样股票（stocks_5m目录只有深市股票）
SAMPLE_STOCKS = {
    "主板-深市": ["000001.SZ", "000002.SZ", "000063.SZ", "000333.SZ",
                 "000651.SZ", "000725.SZ", "000858.SZ", "001289.SZ"],
    "创业板":   ["300001.SZ", "300059.SZ", "300124.SZ", "300142.SZ",
                 "300750.SZ", "301269.SZ"],
}

# BaoStock 30min 时间戳后缀 (HHmmss + 00000)
_BS_TIME_SUFFIXES = [
    "100000000", "103000000", "110000000", "113000000",
    "133000000", "140000000", "143000000", "150000000",
]
# 每个30min窗口对应的5min起始偏移(从9:30算起, 分钟数)
_WINDOW_OFFSETS = [30, 60, 90, 120, 180, 210, 240, 270]


def load_5m(code):
    p = os.path.join(DIR_5M, f"{code}.parquet")
    return pd.read_parquet(p) if os.path.exists(p) else None


def load_30m(code):
    p = os.path.join(DIR_30M, f"{code}.parquet")
    return pd.read_parquet(p) if os.path.exists(p) else None


def aggregate_5m_to_30m(df5):
    """5m → 30m 聚合"""
    df5 = df5.copy()
    if not isinstance(df5.index, pd.DatetimeIndex):
        df5.index = pd.to_datetime(df5.index)
    df5["_mo"] = df5.index.hour * 60 + df5.index.minute - 570  # offset from 9:30

    rows = []
    for dval, grp in df5.groupby(df5.index.date):
        ds = dval.strftime("%Y-%m-%d")
        d8 = dval.strftime("%Y%m%d")
        for i, tgt in enumerate(_WINDOW_OFFSETS):
            wmins = [tgt - 25, tgt - 20, tgt - 15, tgt - 10, tgt - 5, tgt]
            win = grp[grp["_mo"].isin(wmins)].sort_index()
            if len(win) == 0:
                continue
            bs_key = f"{ds}{d8}{_BS_TIME_SUFFIXES[i]}"
            rows.append({
                "bs_key": bs_key,
                "o": win.iloc[0]["open"],
                "h": win["high"].max(),
                "l": win["low"].min(),
                "c": win.iloc[-1]["close"],
                "v": float(win["volume"].sum()),
                "a": float(win["money"].sum()),
            })
    return pd.DataFrame(rows)


def validate_one(code, board):
    """单股验证，返回详细结果"""
    df5 = load_5m(code)
    df30 = load_30m(code)

    if df5 is None:
        return {"code": code, "board": board, "status": "NO_5M"}
    if df30 is None:
        return {"code": code, "board": board, "status": "NO_30M"}

    agg = aggregate_5m_to_30m(df5)
    bs = df30.copy()
    bs["bs_key"] = bs["date"].astype(str) + bs["time"].astype(str)

    m = pd.merge(agg, bs[["bs_key", "open", "high", "low", "close",
                            "volume", "amount"]],
                  on="bs_key", how="inner", suffixes=("_agg", "_bs"))

    if len(m) == 0:
        return {"code": code, "board": board, "status": "NO_OVERLAP",
                "n_agg": len(agg), "n_bs": len(bs)}

    def chk(ca, cb, rtol=1e-4, atol=2.0):
        a, b = m[ca], m[cb]
        re_err = ((a - b).abs() / b.clip(lower=1e-10)).abs()
        ab_err = (a - b).abs()
        ok = (re_err < rtol) | (ab_err < atol)
        return {"n": len(m), "ok": int(ok.sum()),
                "max_re": float(re_err.max()),
                "mean_re": float(re_err.mean()),
                "max_ab": float(ab_err.max())}

    res = {
        "code": code, "board": board, "status": "OK",
        "n_agg": len(agg), "n_bs": len(bs), "n_match": len(m),
        "open": chk("o", "open"),
        "high": chk("h", "high"),
        "low": chk("l", "low"),
        "close": chk("c", "close"),
        "volume": chk("v", "volume"),
        "amount": chk("a", "amount"),
        "_m": m,
    }
    return res


def show_diff(res, n=3):
    """打印差异样例"""
    m = res["_m"]
    for fld in ["c", "o", "h", "l"]:
        m[f"_e{fld}"] = ((m[fld] - m[{"o":"open","h":"high","l":"low","c":"close"}[fld]])
                          .abs() / m[{"o":"open","h":"high","l":"low","c":"close"}[fld]].clip(lower=1e-10) * 100)

    diffs = m[(m["_ec"] > 0.01) | (m["_eo"] > 0.01) |
              (m["_eh"] > 0.01) | (m["_el"] > 0.01)].head(n)
    if len(diffs) == 0:
        print(f"     无显著差异!")
        return
    for _, r in diffs.iterrows():
        print(f"     [{r['bs_key']}]")
        print(f"       聚合 O={r['o']:.4f} H={r['h']:.4f} L={r['l']:.4f} C={r['c']:.4f}")
        print(f"       BS源 O={r['open']:.4f} H={r['high']:.4f} L={r['low']:.4f} C={r['close']:.4f}")


def main():
    print("=" * 72)
    print("  [验证] 5分钟K线 → 30分钟K线 聚合对比 (v3)")
    print("  数据源: stocks_5m(聚合) vs stocks_30m(BaoStock原生)")
    print("=" * 72)

    results = []
    for board, codes in SAMPLE_STOCKS.items():
        print(f"\n{'─'*64}")
        print(f"  板块: {board}")
        print(f"{'─'*64}")

        for code in codes:
            r = validate_one(code, board)
            results.append(r)

            if r["status"] != "OK":
                print(f"  ⚠️  [{code}] {r['status']} "
                      f"(agg={r.get('n_agg',0)} bs={r.get('n_bs',0)})")
                continue

            flds = ["open", "high", "low", "close", "volume", "amount"]
            all_ok = all(r[f]["ok"] == r[f]["n"] for f in flds)
            worst = max(flds, key=lambda f: 1 - r[f]["ok"]/r[f]["n"])
            wr = r[worst]
            pct = wr["ok"] / wr["n"] * 100

            icon = "✅" if all_ok else ("🔶" if pct > 95 else "❌")
            print(f"  {icon} [{code}] 匹配={r['n_match']}根 "
                  f"(覆盖BS {r['n_match']/r['n_bs']*100:.1f}% | "
                  f"覆盖聚合 {r['n_match']/r['n_agg']*100:.1f}%)"
                  f"\n       最差字段={worst} 匹配率={pct:.2f}% "
                  f"最大误差={wr['max_re']:.8f}")

            if not all_ok:
                show_diff(r, n=2)

    # ===========================================================
    # 汇总
    # ===========================================================
    print(f"\n{'='*72}")
    print(f"  汇总报告")
    print(f"{'='*72}")

    ok_r = [r for r in results if r["status"] == "OK"]
    print(f"\n  验证股票: {len(results)} 只 | 可用: {len(ok_r)} 只")

    if ok_r:
        total_bars = sum(r["n_match"] for r in ok_r)
        print(f"  总匹配K线: {total_bars:,} 根")

        print(f"\n  {'字段':<8} {'匹配':>10} {'总数':>10} {'率':>8} {'最大相对误差':>14}")
        print(f'  {"-"*54}')
        for f in ["open", "high", "low", "close", "volume", "amount"]:
            sm = sum(r[f]["ok"] for r in ok_r)
            st = sum(r[f]["n"] for r in ok_r)
            me = max(r[f]["max_re"] for r in ok_r)
            p = sm / st * 100
            mk = "✅" if p > 99.99 else ("⚠️" if p > 95 else "❌")
            print(f"  {mk} {f:<6} {sm:>10,d} {st:>10,d} {p:>7.3f}% {me:>13.8f}")

        # 覆盖率明细
        print(f"\n  覆盖率明细 (5m数据仅含2025年, 30m含2025+2026):")
        for r in ok_r:
            print(f"    {r['code']:12s}: BS覆盖 {r['n_match']/r['n_bs']*100:5.1f}% "
                  f"({r['n_match']:>5d}/{r['n_bs']:>5d}) | "
                  f"聚合覆盖 {r['n_match']/r['n_agg']*100:5.1f}% "
                  f"({r['n_match']:>5d}/{r['n_agg']:>5d})")

    print(f"\n{'='*72}")
    print(f"  验证完成!")
    print(f"{'='*72}")


if __name__ == "__main__":
    main()
