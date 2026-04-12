# -*- coding: utf-8 -*-
"""补充缺失的指数日线数据"""
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
import pandas as pd

STOCKS_DIR = Path(r"C:\Users\Admin\SynologyDrive\PtradeProjects\SimTradeLab\data\cn\stocks")

MISSING_INDICES = {
    "sz.399001": ("399001.SZ", "深证成指"),
    "sz.399006": ("399006.SZ", "创业板指"),
    "sh.000688": ("000688.SS", "科创50"),
}

print("=" * 60)
print("补充缺失的指数日线数据")
print("=" * 60)

fetcher = BaoStockFetcher()
fetcher.login()

end_date = datetime.now().strftime("%Y-%m-%d")

for bs_code, (pt_code, name) in MISSING_INDICES.items():
    output_path = STOCKS_DIR / f"{pt_code}.parquet"

    if output_path.exists():
        df = pd.read_parquet(output_path)
        print(f"  跳过 {name} ({pt_code}): 已有 {len(df)} 条")
        continue

    print(f"  下载 {name} ({bs_code} -> {pt_code})...")

    try:
        rs = fetcher._make_request(
            "query_history_k_data_plus",
            code=bs_code,
            fields="date,open,high,low,close,volume,amount",
            start_date="2024-01-01",
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if len(data_list) > 0:
            df = pd.DataFrame(data_list, columns=rs.fields)
            for col in df.columns:
                if col != "date":
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df.to_parquet(output_path, index=False)
            sz = output_path.stat().st_size / 1024
            print(f"    成功! {len(data_list)}条, {sz:.1f}KB")
            print(f"    close范围: {df['close'].min():.2f} ~ {df['close'].max():.2f}")
        else:
            print(f"    无数据")

    except Exception as e:
        print(f"    失败: {e}")

fetcher.logout()

print("\n" + "=" * 60)
print("完成!")
print("=" * 60)

for pt_code, (bs_code, name) in MISSING_INDICES.items():
    path = STOCKS_DIR / f"{pt_code}.parquet"
    if path.exists():
        df = pd.read_parquet(path)
        print(f"  {pt_code} ({name}): {len(df)}条")
    else:
        print(f"  {pt_code} ({name}): 缺失!")
