#!/usr/bin/env python3
"""Build processed ETF datasets from raw CSV files.

Current responsibilities:
- read multiple raw ETF CSV files
- keep only common aligned dates across all selected assets
- normalize column order and sort ascending by date
- write one processed file per asset to data/processed/
- write an alignment manifest for traceability

Usage:
  python scripts/build_processed_etf_data.py --config scripts/fetch_config.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build processed ETF data layer")
    p.add_argument("--config", default="scripts/fetch_config.json", help="Fetch config JSON path")
    p.add_argument("--raw-dir", default="data/raw", help="Raw data directory")
    p.add_argument("--processed-dir", default="data/processed", help="Processed data directory")
    return p.parse_args()


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


def main() -> int:
    args = parse_args()
    cfg = load_config(args.config)

    try:
        import pandas as pd
    except Exception as e:
        print("[ERROR] Missing pandas. Run: pip install -r scripts/requirements.txt")
        print(f"[DETAIL] {e}")
        return 1

    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    symbols = cfg.get("symbols", [])
    if not symbols:
        print("[ERROR] config must include symbols")
        return 1

    dfs = {}
    for ts_code in symbols:
        name = normalize_symbol(ts_code)
        path = raw_dir / f"{name}.csv"
        if not path.exists():
            print(f"[ERROR] missing raw csv: {path}")
            return 1
        df = pd.read_csv(path)
        required = ["date", "open", "close"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"[ERROR] {path} missing columns: {missing}")
            return 1
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
        if df["date"].isnull().any():
            print(f"[ERROR] {path} has invalid date rows")
            return 1
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        dfs[name] = df

    common_dates = None
    for name, df in dfs.items():
        s = set(df["date"])
        common_dates = s if common_dates is None else common_dates.intersection(s)

    common_dates = sorted(common_dates)
    if not common_dates:
        print("[ERROR] no common dates across selected assets")
        return 1

    manifest = {
        "asset_count": len(dfs),
        "aligned_rows": len(common_dates),
        "start_date": common_dates[0].strftime("%Y-%m-%d"),
        "end_date": common_dates[-1].strftime("%Y-%m-%d"),
        "assets": [],
    }

    for name, df in dfs.items():
        out = df[df["date"].isin(common_dates)].copy()
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        desired_cols = [c for c in ["date", "open", "high", "low", "close", "vol", "amount"] if c in out.columns]
        out = out[desired_cols]
        out_path = processed_dir / f"{name}.csv"
        out.to_csv(out_path, index=False)
        manifest["assets"].append({
            "name": name,
            "path": str(out_path),
            "rows": len(out),
        })
        print(f"[INFO] wrote processed {out_path} rows={len(out)}")

    manifest_path = processed_dir / "alignment_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[INFO] wrote manifest {manifest_path}")
    print("[INFO] processed layer build complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
