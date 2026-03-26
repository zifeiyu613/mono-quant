#!/usr/bin/env python3
"""校验 Rust 量化项目使用的本地 ETF CSV 文件。

校验项：
- 必要字段是否存在
- 日期是否可解析、是否唯一
- 行是否按日期升序排列
- open/high/low/close 是否为正数
- 必要字段是否缺失

用法：
  python scripts/validate_etf_csv.py --dir data/raw
  python scripts/validate_etf_csv.py --file data/raw/hs300.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="校验 ETF CSV 文件")
    p.add_argument("--dir", default=None, help="包含 CSV 文件的目录")
    p.add_argument("--file", default=None, help="需要校验的单个 CSV 文件")
    return p.parse_args()


def validate_one(path: Path) -> tuple[bool, list[str], list[str]]:
    import pandas as pd

    errors: list[str] = []
    warnings: list[str] = []
    required = ["date", "open", "close"]

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return False, [f"读取 CSV 失败：{e}"], []

    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        errors.append(f"缺少必要字段：{missing_cols}")
        return False, errors, warnings

    if df.empty:
        errors.append("文件为空")
        return False, errors, warnings

    if df[required].isnull().any().any():
        errors.append("必要字段存在空值")

    parsed_dates = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    if parsed_dates.isnull().any():
        errors.append("date 字段存在非法的 YYYY-MM-DD 日期")
    else:
        if not parsed_dates.is_monotonic_increasing:
            warnings.append("日期未按升序排列")
        if parsed_dates.duplicated().any():
            errors.append("存在重复日期")

    for col in [c for c in ["open", "high", "low", "close"] if c in df.columns]:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.isnull().any():
            errors.append(f"字段 {col} 存在非数字值")
        elif (numeric <= 0).any():
            errors.append(f"字段 {col} 存在非正价格")

    if "high" in df.columns and "low" in df.columns:
        hi = pd.to_numeric(df["high"], errors="coerce")
        lo = pd.to_numeric(df["low"], errors="coerce")
        if (hi < lo).any():
            errors.append("存在 high < low 的记录")

    return len(errors) == 0, errors, warnings


def main() -> int:
    args = parse_args()
    paths = []
    if args.file:
        paths = [Path(args.file)]
    elif args.dir:
        paths = sorted(Path(args.dir).glob("*.csv"))
    else:
        print("[错误] 请传入 --dir 或 --file", file=sys.stderr)
        return 1

    if not paths:
        print("[错误] 未找到任何 CSV 文件", file=sys.stderr)
        return 1

    all_ok = True
    for path in paths:
        ok, errors, warnings = validate_one(path)
        status = "通过" if ok else "失败"
        print(f"[{status}] {path}")
        for w in warnings:
            print(f"  [警告] {w}")
        for e in errors:
            print(f"  [错误] {e}")
        if ok and not warnings:
            print("  [信息] 校验通过，且没有警告")
        all_ok = all_ok and ok

    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
