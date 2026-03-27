# `breakout_timing_single` 策略说明

## 1. 策略定位

`breakout_timing_single` 是单资产趋势跟随策略的一种更直观版本。

和均线择时不同，它不看均线关系，而是看：

**当前价格有没有突破过去一段时间的高点。**

如果突破，则认为趋势仍在延续；如果没有突破，则切到防守资产或空仓。

## 2. 信号规则

每到调仓日：

1. 读取 `benchmark_asset` 当前收盘价
2. 找出过去 `lookback` 个交易日的最高收盘价
3. 若当前收盘价大于等于这段时间最高价，则持有 `benchmark_asset`
4. 否则进入 `defensive_asset`，若未配置则空仓

## 3. 关键参数

- `benchmark_asset`
- `lookback`
- `rebalance_freq`
- `defensive_asset`

其中：

- `lookback` 越长，突破信号越少，但通常更稳
- `lookback` 越短，信号越灵敏，但噪音也更大

## 4. 优点

- 规则非常容易解释
- 比均线策略更贴近“顺势突破”的直觉
- 适合放进人工审核流程，因为触发条件一眼就能看懂

## 5. 缺点

- 震荡市里容易反复失效
- 只适合强趋势资产，不适合长期横盘标的
- 仍然只观察单一风险资产

## 6. 更适合的用途

- 低复杂度运行候选
- 与 `ma_timing_single` 对比“突破确认”与“均线趋势”的差别
- 与 `absolute_momentum_single` 对比“价格结构过滤”与“收益门槛过滤”的差别

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/breakout_timing_single.json`
- `configs/daily_signal_breakout_timing_single.json`
