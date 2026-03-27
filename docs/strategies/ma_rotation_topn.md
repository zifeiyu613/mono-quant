# `ma_rotation_topn` 策略说明

## 1. 策略定位

`ma_rotation_topn` 可以理解成“均线过滤版的 `momentum_topn`”。

它先要求资产处在均线多头结构里，再从这些资产里继续择强。

也就是说，它的核心思想是：

**先确认趋势没有坏掉，再做相对强弱轮动。**

## 2. 信号规则

每到调仓日：

1. 对每个资产计算 `fast` / `slow` 均线
2. 只保留 `fast > slow` 的资产
3. 对保留下来的资产按 `lookback` 收益从高到低排序
4. 选出前 `top_n` 个资产等权配置
5. 若没有资产通过均线过滤，则进入 `defensive_asset`，若未配置则空仓

## 3. 关键参数

- `fast`
- `slow`
- `lookback`
- `rebalance_freq`
- `top_n`
- `defensive_asset`

这里的关键不是单独某一个参数，而是：

- 均线负责“趋势是否成立”
- `lookback` 负责“趋势成立时谁更强”

## 4. 优点

- 比纯动量轮动更强调趋势过滤
- 比单资产均线择时更有横向选择能力
- 逻辑比复杂风险模型更直观，便于后续人工审核

## 5. 缺点

- 过滤条件变多后，可能错过早期趋势
- 横盘市里容易出现“无资产通过过滤”
- 参数更多，研究时更容易出现样本依赖

## 6. 更适合的用途

- 研究候选
- 与 `momentum_topn` 对比“直接追强”与“先做均线过滤再追强”的差别
- 与 `ma_timing_single` 对比“单资产均线开关”与“多资产均线轮动”的差别

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/ma_rotation_topn.json`
- `configs/daily_signal_ma_rotation_topn.json`
