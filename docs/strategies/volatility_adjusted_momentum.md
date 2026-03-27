# `volatility_adjusted_momentum` 策略说明

## 1. 策略定位

`volatility_adjusted_momentum` 可以理解成 `momentum_topn` 的一个更稳健版本。

它不只看谁涨得多，还看谁涨得“更平稳”。  
核心思想是：

**同样是上涨，波动更小的上涨，优先级更高。**

## 2. 信号规则

每到调仓日：

1. 对每个资产计算 `lookback` 区间收益
2. 计算同一窗口内的日收益波动率
3. 用 `收益 / 波动` 作为排序分数
4. 选出分数最高的前 `top_n` 个资产
5. 对入选资产等权配置

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`

这个策略最关键的地方在于，它偏好：

- 上涨持续性更好
- 回撤过程相对更平滑

而不是单纯追逐短期涨幅最大的资产。

## 4. 优点

- 比纯动量排序更强调稳定性
- 对“暴涨暴跌型强势资产”更克制
- 很适合作为 `momentum_topn` 的平滑化对照组

## 5. 缺点

- 可能错过最强、最猛的趋势行情
- 如果市场最强资产本来就是高波动资产，收益弹性可能被压制
- 依然属于主动轮动策略，对样本环境敏感

## 6. 更适合的用途

- 研究候选
- 与 `momentum_topn` 对比“强势优先”与“稳健强势优先”的差别
- 与 `dual_momentum` 对比“风险调整排序”是否比“绝对过滤”更有效

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/volatility_adjusted_momentum.json`
- `configs/daily_signal_volatility_adjusted_momentum.json`
