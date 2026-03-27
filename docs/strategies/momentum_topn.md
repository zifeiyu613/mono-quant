# `momentum_topn` 策略说明

## 1. 策略定位

`momentum_topn` 是当前项目里最经典的多资产相对动量轮动策略。

它做的事情很直接：

- 先看一段时间内谁涨得更强
- 再把资金分配给最强的前 `N` 个资产

这是整个策略池里的核心研究候选之一。

## 2. 信号规则

每到调仓日：

1. 对全部资产计算 `lookback` 区间收益
2. 按收益从高到低排序
3. 选出前 `top_n` 个资产
4. 对入选资产做等权配置

## 3. 关键参数

- `lookback`
  - 回看窗口，决定“强弱”怎么定义
- `rebalance_freq`
  - 调仓频率
- `top_n`
  - 每次持有几个资产
- `risk.max_single_asset_weight`
  - 会影响最小持仓数要求，避免单资产权重过高

## 4. 优点

- 逻辑简洁，研究可解释性强
- 很适合做参数扫描和 walk-forward
- 在趋势明显、风格延续较好的环境里通常表现较好

## 5. 缺点

- 只有相对强弱，没有绝对过滤
- 市场整体转弱时，也可能在“烂里挑最强”
- 调仓频率偏高时，成本侵蚀可能很明显

## 6. 更适合的用途

- 研究主策略
- 参数实验母体
- 作为 `dual_momentum` 的基础对照

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/momentum_topn.json`
