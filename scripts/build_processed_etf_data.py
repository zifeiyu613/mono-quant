#!/usr/bin/env python3
"""根据 raw CSV 构建 processed ETF 数据集。

当前职责：
- 读取多个 raw ETF CSV 文件
- 仅保留所有资产的共同对齐交易日
- 统一字段顺序并按日期升序输出
- 为每个资产生成一份 data/processed/ 下的 CSV
- 生成对齐清单与摘要文件，便于追溯

用法：
  python scripts/build_processed_etf_data.py --config scripts/fetch_config.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="构建 processed ETF 数据层")
    p.add_argument("--config", default="scripts/fetch_config.json", help="拉取配置 JSON 路径")
    p.add_argument("--raw-dir", default="data/raw", help="raw 数据目录")
    p.add_argument("--processed-dir", default="data/processed", help="processed 数据目录")
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
        print("[错误] 缺少 pandas，请先运行：pip install -r scripts/requirements.txt")
        print(f"[详情] {e}")
        return 1

    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    symbols = cfg.get("symbols", [])
    if not symbols:
        print("[错误] 配置文件必须包含 symbols")
        return 1

    dfs = {}
    raw_stats = {}
    for ts_code in symbols:
        name = normalize_symbol(ts_code)
        path = raw_dir / f"{name}.csv"
        if not path.exists():
            print(f"[错误] 缺少 raw CSV：{path}")
            return 1
        df = pd.read_csv(path)
        required = ["date", "open", "close"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"[错误] {path} 缺少字段：{missing}")
            return 1
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
        if df["date"].isnull().any():
            print(f"[错误] {path} 存在非法日期记录")
            return 1
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        dfs[name] = df
        raw_stats[name] = {
            "raw_rows": len(df),
            "raw_start_date": df["date"].iloc[0].strftime("%Y-%m-%d"),
            "raw_end_date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        }

    common_dates = None
    for _, df in dfs.items():
        s = set(df["date"])
        common_dates = s if common_dates is None else common_dates.intersection(s)

    common_dates = sorted(common_dates)
    if not common_dates:
        print("[错误] 所选资产之间没有共同交易日")
        return 1

    manifest = {
        "asset_count": len(dfs),
        "aligned_rows": len(common_dates),
        "start_date": common_dates[0].strftime("%Y-%m-%d"),
        "end_date": common_dates[-1].strftime("%Y-%m-%d"),
        "assets": [],
    }

    summary = {
        "data_layer": "processed",
        "asset_count": len(dfs),
        "aligned_rows": len(common_dates),
        "aligned_start_date": common_dates[0].strftime("%Y-%m-%d"),
        "aligned_end_date": common_dates[-1].strftime("%Y-%m-%d"),
        "assets": [],
    }

    summary_lines = [
        "=== Processed 数据摘要 ===",
        f"数据层: processed",
        f"资产数量: {len(dfs)}",
        f"对齐行数: {len(common_dates)}",
        f"对齐开始日期: {common_dates[0].strftime('%Y-%m-%d')}",
        f"对齐结束日期: {common_dates[-1].strftime('%Y-%m-%d')}",
        "",
        "分资产摘要：",
    ]

    for name, df in dfs.items():
        out = df[df["date"].isin(common_dates)].copy()
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        desired_cols = [c for c in ["date", "open", "high", "low", "close", "vol", "amount"] if c in out.columns]
        out = out[desired_cols]
        out_path = processed_dir / f"{name}.csv"
        out.to_csv(out_path, index=False)

        dropped_rows = raw_stats[name]["raw_rows"] - len(out)
        asset_summary = {
            "name": name,
            "path": str(out_path),
            "raw_rows": raw_stats[name]["raw_rows"],
            "processed_rows": len(out),
            "dropped_rows": dropped_rows,
            "raw_start_date": raw_stats[name]["raw_start_date"],
            "raw_end_date": raw_stats[name]["raw_end_date"],
            "processed_start_date": out["date"].iloc[0],
            "processed_end_date": out["date"].iloc[-1],
            "fully_aligned": len(out) == len(common_dates),
        }

        manifest["assets"].append({
            "name": name,
            "path": str(out_path),
            "rows": len(out),
        })
        summary["assets"].append(asset_summary)
        summary_lines.extend([
            f"- {name}",
            f"  raw 行数: {asset_summary['raw_rows']}",
            f"  processed 行数: {asset_summary['processed_rows']}",
            f"  丢弃行数: {asset_summary['dropped_rows']}",
            f"  raw 区间: {asset_summary['raw_start_date']} -> {asset_summary['raw_end_date']}",
            f"  processed 区间: {asset_summary['processed_start_date']} -> {asset_summary['processed_end_date']}",
            f"  是否完全对齐: {asset_summary['fully_aligned']}",
            f"  文件路径: {asset_summary['path']}",
        ])
        print(f"[信息] 已写入 processed 文件 {out_path} rows={len(out)}")

    manifest_path = processed_dir / "alignment_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[信息] 已写入对齐清单 {manifest_path}")

    summary_json_path = processed_dir / "processed_summary.json"
    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"[信息] 已写入摘要 JSON {summary_json_path}")

    summary_txt_path = processed_dir / "processed_summary.txt"
    with open(summary_txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines) + "\n")
    print(f"[信息] 已写入摘要 TXT {summary_txt_path}")

    print("[信息] processed 数据层构建完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
