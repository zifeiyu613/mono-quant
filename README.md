# mono-quant

一个基于 Rust 的 A股 ETF 量化研究与回测项目。

## 当前能力
- 读取单个 ETF CSV（日线）
- 运行双均线策略（MA Cross）
- 运行多 ETF Top N 动量轮动
- 运行单资产 Buy & Hold 基准
- 运行多资产等权 Buy & Hold 基准
- 运行双动量策略（相对动量 + 绝对动量过滤 + 防守资产回退）
- 运行风险开关轮动策略（风险资产最强者 / 防守资产切换）
- 输出 `equity_curve.csv`
- 输出 `rebalance_log.csv`
- 输出 `holdings_trace.csv`
- 输出 `asset_contribution.csv`
- 输出批量实验总表、实验索引、阶段报告
- 支持免费 ETF 日线真实数据下载（默认 `AkShare`）
- 支持增量更新与基础数据校验
- 支持 processed 层构建（共同日期对齐 + 标准化输出）
- 支持 processed 层摘要输出（summary + manifest）
- 多资产回测默认优先读取 `data/processed/`
- 支持 walk-forward 多窗口样本外评估
- 支持成本敏感性摘要与假设证据置信度输出
- 支持最小风控：样本门槛、单资产权重上限、单日亏损停机、最大回撤停机、调仓换手上限
- `max_single_asset_weight` 现在会校验资产池分散度是否足够，避免静默突破权重上限
- 支持停机冷静期：触发停机后可空仓一段交易日，再回到后续调仓点

## 快速开始

### 0. 安装 Python 依赖（推荐使用本地 `.venv`）
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r scripts/requirements.txt
```

### 1. 单 ETF 双均线
```bash
cargo run -- --config configs/ma_single.json
```

### 2. 准备真实数据（推荐先拉多年历史）
```bash
./scripts/prepare_data.sh scripts/fetch_config.json
```

> 当前默认 `scripts/fetch_config.json` 已把 `start_date` 调整为 `20200101`，用于满足 P0 阶段“多年份样本”要求。

### 3. 多 ETF 动量轮动（processed-first）
```bash
cargo run -- --config configs/momentum_topn.json
```

### 4. 批量实验（processed-first）
```bash
cargo run -- --config configs/momentum_batch.json
```

### 5. 单资产 Buy & Hold 基准（processed-first）
```bash
cargo run -- --config configs/buy_hold_single.json
```

### 6. 多资产等权 Buy & Hold 基准（processed-first）
```bash
cargo run -- --config configs/buy_hold_equal_weight.json
```

### 7. 双动量策略（processed-first）
```bash
cargo run -- --config configs/dual_momentum.json
```

### 8. 风险开关轮动策略（processed-first）
```bash
cargo run -- --config configs/risk_off_rotation.json
```

### 9. 批量研究治理输出
`momentum_batch` 现在支持可选的研究治理配置，会在批量实验完成后额外输出：
- `hypothesis_assessment.csv`
- `hypothesis_assessment_in_sample.csv`
- `hypothesis_assessment_out_of_sample.csv`
- `walk_forward_plan.txt`
- `walk_forward_assessment_detail.csv`
- `walk_forward_assessment_summary.csv`
- `cost_sensitivity_detail.csv`
- `cost_sensitivity_summary.csv`
- `research_evidence_summary.csv`
- `research_plan.txt`
- `research_decision_auto.txt`
- `research_decision.txt`
- `governance_summary.txt`
- 更新后的 `stage_report.txt`
- `risk_events.csv`（触发风控时）
- `risk_summary.txt`
- `batch_results.csv` / `experiment_index.csv` 中的 `halt_event_type` / `halt_reason`

适合用来记录：
- 当前研究主题
- 本轮研究假设
- 样本内 / 样本外假设支持度评估
- 多窗口样本外一致性
- 成本变化下的结论稳定性
- 每个假设的置信度与主要失效条件
- 自动研究状态与人工最终决策
- 当前研究状态与下一步建议

如需验证人工覆写决策示例，可运行：
```bash
cargo run -- --config configs/momentum_batch_review.json
```

## 真实数据工作流
安装依赖（推荐在本地 `.venv` 中执行）：
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r scripts/requirements.txt
```

当前默认数据源为免费 `AkShare`，无需 token。

### 全量拉取 raw 数据
```bash
python scripts/fetch_etf_daily.py --config scripts/fetch_config.json --full
```

### 增量更新 raw 数据
```bash
python scripts/fetch_etf_daily.py --config scripts/fetch_config.json
```

### 校验 raw 数据
```bash
python scripts/validate_etf_csv.py --dir data/raw
```

### 校验脚本自测
```bash
python -m unittest scripts/test_validate_etf_csv.py
```

### 构建 processed 层
```bash
python scripts/build_processed_etf_data.py --config scripts/fetch_config.json
```

### 一条命令完成更新 + 校验 + processed 构建
```bash
./scripts/prepare_data.sh scripts/fetch_config.json
```

> 如果多资产回测提示缺少 `data/processed/*.csv`、`alignment_manifest.json`、
> `processed_summary.json` 或 `processed_summary.txt`，
> 先运行上面的 `prepare_data.sh`。

> 原始文件会按规范化名称落到 `data/raw/`，当前默认是：
> `hs300.csv`、`zz500.csv`、`cyb.csv`、`dividend.csv`。
> 单资产默认配置也应优先使用这些规范化文件名。

更多说明见：
- `docs/real-data.md`
- `docs/research-workflow.md`
- `docs/strategy-live-plan.md`
- `docs/strategy-architecture.md`

## 目录结构
```text
configs/
data/raw/
data/processed/
docs/
output/
scripts/
src/
```
