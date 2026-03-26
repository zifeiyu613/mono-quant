#!/usr/bin/env python3
"""从免费数据源下载 ETF 日线数据。

当前固定使用 `AkShare` 的东方财富 ETF 历史行情接口。

用法：
  python scripts/fetch_etf_daily.py --config scripts/fetch_config.json
  python scripts/fetch_etf_daily.py --config scripts/fetch_config.json --full
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="拉取 ETF 日线数据")
    parser.add_argument("--config", default="scripts/fetch_config.json", help="拉取配置 JSON 路径")
    parser.add_argument("--full", action="store_true", help="强制执行全量刷新，而不是增量更新")
    return parser.parse_args()


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def normalize_symbol(ts_code: str) -> str:
    mapping = {
        "510300.SH": "hs300",
        "510500.SH": "zz500",
        "159915.SZ": "cyb",
        "510880.SH": "dividend",
    }
    return mapping.get(ts_code, ts_code.replace(".", "_").lower())


def provider_symbol(raw_symbol: str) -> str:
    return raw_symbol.split(".")[0]


def infer_incremental_start(csv_path: Path) -> Optional[str]:
    """根据已有本地 CSV 推导下一次拉取的开始日期。"""
    if not csv_path.exists():
        return None

    try:
        import pandas as pd

        dataframe = pd.read_csv(csv_path)
        if dataframe.empty or "date" not in dataframe.columns:
            return None
        last_date = str(dataframe["date"].dropna().iloc[-1])
        parsed = pd.to_datetime(last_date, format="%Y-%m-%d", errors="coerce")
        if pd.isna(parsed):
            return None
        return (parsed + pd.Timedelta(days=1)).strftime("%Y%m%d")
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

    merged = sanitize_dataframe(merged)
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    merged.to_csv(out_path, index=False)
    return len(merged)


def sanitize_dataframe(dataframe):
    import pandas as pd

    cleaned = dataframe.copy()
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    numeric_columns = [column for column in ["open", "high", "low", "close", "vol", "amount"] if column in cleaned.columns]
    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    required_columns = [column for column in ["date", "open", "high", "low", "close"] if column in cleaned.columns]
    cleaned = cleaned.dropna(subset=required_columns)
    return cleaned


def fetch_from_akshare(symbol: str, start_date: str, end_date: str, adjust: str):
    try:
        import akshare as ak
    except Exception as exc:
        raise RuntimeError(
            "缺少 akshare 依赖，请先运行：pip install -r scripts/requirements.txt"
        ) from exc

    dataframe = ak.fund_etf_hist_em(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    if dataframe is None or dataframe.empty:
        return dataframe

    column_map = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
        "成交额": "amount",
    }
    dataframe = dataframe.rename(columns=column_map).copy()
    missing_columns = [column for column in column_map.values() if column not in dataframe.columns]
    if missing_columns:
        raise RuntimeError(f"akshare 返回字段缺失：{missing_columns}")

    dataframe["date"] = dataframe["date"].astype(str)
    desired_columns = ["date", "open", "high", "low", "close", "vol", "amount"]
    dataframe = dataframe[desired_columns]
    return dataframe.sort_values("date").reset_index(drop=True)

def main() -> int:
    args = parse_args()
    config = load_config(args.config)

    symbols = config.get("symbols", [])
    start_date_cfg = config.get("start_date")
    end_date = config.get("end_date") or date.today().strftime("%Y%m%d")
    adjust = config.get("adjust", "hfq")
    output_dir = Path(config.get("output_dir", "data/raw"))
    output_dir.mkdir(parents=True, exist_ok=True)

    if not symbols or not start_date_cfg:
        print("[错误] 配置文件必须包含 symbols 和 start_date", file=sys.stderr)
        return 1

    print(
        f"[信息] 数据源=akshare symbols={len(symbols)} 默认开始日期={start_date_cfg} "
        f"结束日期={end_date} 全量模式={args.full}"
    )

    for raw_symbol in symbols:
        out_name = normalize_symbol(raw_symbol) + ".csv"
        out_path = output_dir / out_name
        start_date = start_date_cfg
        if not args.full:
            inferred = infer_incremental_start(out_path)
            if inferred:
                start_date = inferred

        provider_code = provider_symbol(raw_symbol)
        print(
            f"[信息] 正在拉取 {raw_symbol} -> {out_path.name} "
            f"provider_code={provider_code} start={start_date} end={end_date}"
        )

        try:
            dataframe = fetch_from_akshare(
                symbol=provider_code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
        except Exception as exc:
            print(f"[错误] 拉取 {raw_symbol} 失败：{exc}", file=sys.stderr)
            return 1

        if dataframe is None or dataframe.empty:
            print(f"[信息] {raw_symbol} 没有新增数据")
            continue

        final_rows = merge_with_existing(out_path, dataframe)
        print(f"[信息] 已写入 {out_path} total_rows={final_rows} fetched_rows={len(dataframe)}")

    print("[信息] 数据拉取完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
