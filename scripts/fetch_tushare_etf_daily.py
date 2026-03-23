#!/usr/bin/env python3
"""Download or incrementally update A-share ETF daily bars from Tushare.

Usage:
  export TUSHARE_TOKEN=your_token
  python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json
  python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json --full

Behavior:
- Default mode: incremental update if local CSV exists
- --full: force full refresh from config start_date
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch ETF daily bars from Tushare")
    parser.add_argument("--config", default="scripts/fetch_config.json", help="Path to fetch config JSON")
    parser.add_argument("--token", default=None, help="Tushare token; if omitted uses TUSHARE_TOKEN env var")
    parser.add_argument("--full", action="store_true", help="Force full refresh instead of incremental update")
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
    """Infer next start date from existing local CSV.

    Returns YYYYMMDD or None if file does not exist / is unusable.
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
    """Merge newly fetched rows with existing CSV and deduplicate by date."""
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
        print("[ERROR] Missing Tushare token. Set TUSHARE_TOKEN or pass --token.", file=sys.stderr)
        return 1

    try:
        import tushare as ts
        import pandas as pd
    except Exception as e:
        print("[ERROR] Missing Python dependencies. Run: pip install -r scripts/requirements.txt", file=sys.stderr)
        print(f"[DETAIL] {e}", file=sys.stderr)
        return 1

    output_dir = Path(cfg.get("output_dir", "data/raw"))
    output_dir.mkdir(parents=True, exist_ok=True)

    symbols = cfg.get("symbols", [])
    start_date_cfg = cfg.get("start_date")
    end_date = cfg.get("end_date")

    if not symbols or not start_date_cfg:
        print("[ERROR] config must include symbols and start_date", file=sys.stderr)
        return 1

    print(f"[INFO] provider=tushare symbols={len(symbols)} default_start={start_date_cfg} end_date={end_date} full={args.full}")
    pro = ts.pro_api(token)

    for ts_code in symbols:
        out_name = normalize_symbol(ts_code) + ".csv"
        out_path = output_dir / out_name

        start_date = start_date_cfg
        if not args.full:
            inferred = infer_incremental_start(out_path)
            if inferred:
                start_date = inferred

        print(f"[INFO] fetching {ts_code} -> {out_path.name} start={start_date} end={end_date}")
        df = pro.fund_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="trade_date,open,high,low,close,vol,amount",
        )

        if df is None or df.empty:
            print(f"[INFO] no new rows for {ts_code}")
            continue

        df = df.rename(columns={"trade_date": "date"}).copy()
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        df = df.sort_values("date").reset_index(drop=True)

        final_rows = merge_with_existing(out_path, df)
        print(f"[INFO] wrote {out_path} total_rows={final_rows} fetched_rows={len(df)}")

    print("[INFO] fetch complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
