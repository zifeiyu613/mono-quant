#!/usr/bin/env python3
"""Validate local ETF CSV files used by the Rust quant project.

Checks:
- required columns exist
- date sortable and unique
- rows sorted ascending by date
- open/high/low/close positive where present
- no missing values in required fields

Usage:
  python scripts/validate_etf_csv.py --dir data/raw
  python scripts/validate_etf_csv.py --file data/raw/hs300.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate ETF CSV files")
    p.add_argument("--dir", default=None, help="Directory containing CSV files")
    p.add_argument("--file", default=None, help="Single CSV file to validate")
    return p.parse_args()


def validate_one(path: Path) -> tuple[bool, list[str], list[str]]:
    import pandas as pd

    errors: list[str] = []
    warnings: list[str] = []
    required = ["date", "open", "close"]

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return False, [f"failed to read csv: {e}"], []

    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        errors.append(f"missing required columns: {missing_cols}")
        return False, errors, warnings

    if df.empty:
        errors.append("file is empty")
        return False, errors, warnings

    if df[required].isnull().any().any():
        errors.append("required columns contain null values")

    parsed_dates = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    if parsed_dates.isnull().any():
        errors.append("date column contains invalid YYYY-MM-DD values")
    else:
        if not parsed_dates.is_monotonic_increasing:
            warnings.append("dates are not sorted ascending")
        if parsed_dates.duplicated().any():
            errors.append("duplicate dates found")

    for col in [c for c in ["open", "high", "low", "close"] if c in df.columns]:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.isnull().any():
            errors.append(f"column {col} contains non-numeric values")
        elif (numeric <= 0).any():
            errors.append(f"column {col} contains non-positive prices")

    if "high" in df.columns and "low" in df.columns:
        hi = pd.to_numeric(df["high"], errors="coerce")
        lo = pd.to_numeric(df["low"], errors="coerce")
        if (hi < lo).any():
            errors.append("some rows have high < low")

    return len(errors) == 0, errors, warnings


def main() -> int:
    args = parse_args()
    paths = []
    if args.file:
        paths = [Path(args.file)]
    elif args.dir:
        paths = sorted(Path(args.dir).glob("*.csv"))
    else:
        print("[ERROR] pass --dir or --file", file=sys.stderr)
        return 1

    if not paths:
        print("[ERROR] no csv files found", file=sys.stderr)
        return 1

    all_ok = True
    for path in paths:
        ok, errors, warnings = validate_one(path)
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {path}")
        for w in warnings:
            print(f"  [WARN] {w}")
        for e in errors:
            print(f"  [ERROR] {e}")
        if ok and not warnings:
            print("  [INFO] validation passed cleanly")
        all_ok = all_ok and ok

    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
