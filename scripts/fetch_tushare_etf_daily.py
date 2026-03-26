#!/usr/bin/env python3
"""从 Tushare 下载或增量更新 A 股 ETF 日线数据。

用法：
  export TUSHARE_TOKEN=your_token
  python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json
  python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json --full

行为说明：
- 默认模式：如果本地 CSV 已存在，则执行增量更新
- --full：从配置中的 start_date 开始强制全量刷新
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 Tushare 拉取 ETF 日线数据")
    parser.add_argument("--config", default="scripts/fetch_config.json", help="拉取配置 JSON 路径")
    parser.add_argument("--token", default=None, help="Tushare token；如果不传则读取 TUSHARE_TOKEN 环境变量")
    parser.add_argument("--full", action="store_true", help="强制执行全量刷新，而不是增量更新")
    return parser.parse_args()


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_symbol(ts_code: str) -> str:
    mapping = {
        "510300.SH": "hs300",
        "510500.SH": "zz500",
        "159915.SZ": "cyb",
        "510880.SH": "dividend",
    }
    return mapping.get(ts_code, ts_code.replace(".", "_").lower())


def infer_incremental_start(csv_path: Path) -> Optional[str]:
    """根据已有本地 CSV 推导下一次拉取的开始日期。

    如果文件不存在或内容不可用，则返回 None；否则返回 YYYYMMDD。
    """
    if not csv_path.exists():
        return None
    try:
        import pandas as pd
        df = pd.read_csv(csv_path)
        if df.empty or "date" not in df.columns:
            return None
        last_date = str(df["date"].dropna().iloc[-1])
        dt = pd.to_datetime(last_date, format="%Y-%m-%d", errors="coerce")
        if pd.isna(dt):
            return None
        next_dt = dt + pd.Timedelta(days=1)
        return next_dt.strftime("%Y%m%d")
    except Exception:
        return None


def merge_with_existing(out_path: Path, new_df) -> int:
    """将新拉取的数据与已有 CSV 合并，并按日期去重。"""
    import pandas as pd

    if out_path.exists():
        old_df = pd.read_csv(out_path)
        merged = pd.concat([old_df, new_df], ignore_index=True)
    else:
        merged = new_df.copy()

    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    merged.to_csv(out_path, index=False)
    return len(merged)


def main() -> int:
    args = parse_args()
    cfg = load_config(args.config)

    token = args.token or os.getenv("TUSHARE_TOKEN")
    if not token:
        print("[错误] 缺少 Tushare token，请设置 TUSHARE_TOKEN 或通过 --token 传入。", file=sys.stderr)
        return 1

    try:
        import tushare as ts
        import pandas as pd
    except Exception as e:
        print("[错误] 缺少 Python 依赖，请先运行：pip install -r scripts/requirements.txt", file=sys.stderr)
        print(f"[详情] {e}", file=sys.stderr)
        return 1

    output_dir = Path(cfg.get("output_dir", "data/raw"))
    output_dir.mkdir(parents=True, exist_ok=True)

    symbols = cfg.get("symbols", [])
    start_date_cfg = cfg.get("start_date")
    end_date = cfg.get("end_date")

    if not symbols or not start_date_cfg:
        print("[错误] 配置文件必须包含 symbols 和 start_date", file=sys.stderr)
        return 1

    print(f"[信息] 数据源=tushare symbols={len(symbols)} 默认开始日期={start_date_cfg} 结束日期={end_date} 全量模式={args.full}")
    pro = ts.pro_api(token)

    for ts_code in symbols:
        out_name = normalize_symbol(ts_code) + ".csv"
        out_path = output_dir / out_name

        start_date = start_date_cfg
        if not args.full:
            inferred = infer_incremental_start(out_path)
            if inferred:
                start_date = inferred

        print(f"[信息] 正在拉取 {ts_code} -> {out_path.name} start={start_date} end={end_date}")
        df = pro.fund_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="trade_date,open,high,low,close,vol,amount",
        )

        if df is None or df.empty:
            print(f"[信息] {ts_code} 没有新增数据")
            continue

        df = df.rename(columns={"trade_date": "date"}).copy()
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        df = df.sort_values("date").reset_index(drop=True)

        final_rows = merge_with_existing(out_path, df)
        print(f"[信息] 已写入 {out_path} total_rows={final_rows} fetched_rows={len(df)}")

    print("[信息] 数据拉取完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
