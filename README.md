# mono-quant

一个面向 A股 ETF 的 Rust 量化项目原型。

## 当前能力
- 读取单个 ETF CSV（日线）
- 运行双均线策略（MA Cross）
- 运行多 ETF Top N 动量轮动
- 输出 `equity_curve.csv`
- 输出 `rebalance_log.csv`
- 输出 `holdings_trace.csv`
- 输出 `asset_contribution.csv`
- 输出批量实验总表、实验索引、阶段报告
- 支持 Tushare ETF 日线真实数据下载
- 支持增量更新与基础数据校验

## 快速开始

### 1. 单 ETF 双均线
```bash
cargo run -- --config configs/ma_single.json
```

### 2. 多 ETF 动量轮动
```bash
cargo run -- --config configs/momentum_topn.json
```

### 3. 批量实验
```bash
cargo run -- --config configs/momentum_batch.json
```

## 真实数据接入（Tushare）
安装依赖：
```bash
pip install -r scripts/requirements.txt
```

设置 token：
```bash
export TUSHARE_TOKEN=你的token
```

### 全量拉取
```bash
python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json --full
```

### 增量更新
```bash
python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json
```

### 校验数据
```bash
python scripts/validate_etf_csv.py --dir data/raw
```

### 一条命令更新 + 校验
```bash
./scripts/update_and_validate.sh scripts/fetch_config.json
```

更多说明见：
- `docs/real-data.md`

## 目录结构
```text
configs/
data/raw/
docs/
output/
scripts/
src/
```
