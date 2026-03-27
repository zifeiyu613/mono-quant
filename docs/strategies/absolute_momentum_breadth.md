# `absolute_momentum_breadth` 策略说明

## 1. 策略定位

`absolute_momentum_breadth` 是把单资产绝对动量开关扩成多资产版本的策略。

它不是问“某一个资产值不值得持有”，而是问：

**当前资产池里，有多少资产仍然处在正向趋势里？**

只要资产满足绝对动量门槛，就纳入组合；都不满足时，再回退到防守资产或空仓。

## 2. 信号规则

每到调仓日：

1. 对每个风险资产计算 `lookback` 区间收益
2. 保留所有收益大于等于 `absolute_momentum_floor` 的资产
3. 对入选资产等权配置
4. 若没有任何资产达标，则进入 `defensive_asset`，若未配置则空仓

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `absolute_momentum_floor`
- `defensive_asset`

这个策略最关键的地方不是“谁最强”，而是“当前还有多少资产整体可做”。

## 4. 优点

- 比单资产绝对动量更有分散化
- 比 `momentum_topn` 更克制，不会强迫持有弱资产
- 很适合做“市场广度仍然健康吗”的中间层对照

## 5. 缺点

- 当达标资产数量频繁变化时，仓位结构会更跳
- 强势行情里，收益弹性通常不如集中持有最强资产
- 如果多数资产长期低波动小涨，策略可能过度分散

## 6. 更适合的用途

- 研究候选
- 与 `absolute_momentum_single` 对比“单点开关”与“组合广度过滤”的差别
- 与 `dual_momentum` 对比“只看绝对门槛”与“相对排序 + 绝对过滤”的差别

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/absolute_momentum_breadth.json`
- `configs/daily_signal_absolute_momentum_breadth.json`
