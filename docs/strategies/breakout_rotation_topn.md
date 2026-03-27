# `breakout_rotation_topn` 策略说明

## 1. 策略定位

`breakout_rotation_topn` 可以理解成“多资产版的突破择时”。

它先要求资产本身出现突破，再在这些突破资产里继续择强。

也就是说，它不是单纯追最近涨得多，而是强调：

**先确认趋势站上新高，再在确认后的资产里做轮动。**

## 2. 信号规则

每到调仓日：

1. 对每个风险资产检查当前收盘价是否突破过去 `lookback` 个交易日最高收盘价
2. 只保留触发突破的资产
3. 按同一窗口的收益从高到低排序
4. 选出前 `top_n` 个资产等权配置
5. 若没有资产触发突破，则进入 `defensive_asset`，若未配置则空仓

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`
- `defensive_asset`

这里的核心是：先做突破过滤，再做强弱排序。

## 4. 优点

- 比纯相对动量更强调趋势确认
- 比单资产突破更有横向选择能力
- 逻辑清晰，适合纳入运行前审核流程

## 5. 缺点

- 横盘环境下容易长期没有突破资产
- 趋势刚启动但尚未创新高时，可能介入偏慢
- 同时叠加“突破 + 排序”后，样本环境变化时更容易退化

## 6. 更适合的用途

- 研究候选
- 与 `breakout_timing_single` 对比“单资产突破”与“多资产突破轮动”的差别
- 与 `momentum_topn` 对比“先确认趋势再排序”与“直接按收益排序”的差别

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/breakout_rotation_topn.json`
- `configs/daily_signal_breakout_rotation_topn.json`
