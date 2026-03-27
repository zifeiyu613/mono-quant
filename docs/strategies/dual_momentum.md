# `dual_momentum` 策略说明

## 1. 策略定位

`dual_momentum` 是 `momentum_topn` 的自然升级版。

它同时做两件事：

- 看相对动量：谁更强
- 看绝对动量：整体值不值得持有

因此它比纯相对动量更接近“可运行策略”。

## 2. 信号规则

每到调仓日：

1. 对风险资产按 `lookback` 收益做相对动量排序
2. 过滤掉低于 `absolute_momentum_floor` 的资产
3. 从剩余资产中选前 `top_n`
4. 如果一个都不满足，则进入 `defensive_asset` 或空仓

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`
- `absolute_momentum_floor`
- `defensive_asset`

其中最关键的是：

- `top_n` 决定分散程度
- `absolute_momentum_floor` 决定防守切换是否积极

## 4. 优点

- 比纯相对动量更能规避整体走弱环境
- 同时保留横向比较和纵向过滤
- 是从“研究”走向“运行”的很自然一步

## 5. 缺点

- 参数比 `momentum_topn` 更多
- 如果绝对门槛过高，可能长时间防守，收益弹性下降
- 如果绝对门槛过低，又会退化成接近纯相对动量

## 6. 更适合的用途

- 研究候选 + 运行候选
- 与 `momentum_topn` 对比绝对过滤是否真的有价值

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/dual_momentum.json`
- `configs/daily_signal_dual_momentum.json`
