# `reversal_bottomn` 策略说明

## 1. 策略定位

`reversal_bottomn` 是给当前动量策略池准备的一组反向对照。

它不追涨，而是做一件很直接的事：

**从最近一段时间跌得最多的资产里，选出最弱的几个持有。**

这个策略未必适合直接运行，但很适合作为研究里的“反命题”。

## 2. 信号规则

每到调仓日：

1. 对每个资产计算 `lookback` 区间收益
2. 按收益从低到高排序
3. 选出最弱的前 `top_n` 个资产
4. 对入选资产等权配置

如果你把 `momentum_topn` 理解成“追强”，那 `reversal_bottomn` 就是“抄弱”。

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`

这组参数决定的是：

- 你看多短或多长的弱势窗口
- 多久重新挑一次“最弱资产”
- 每次分散到几个反转候选

## 4. 优点

- 很适合作为动量策略的反向对照组
- 能帮助判断当前样本更偏“趋势延续”还是“超跌反弹”
- 规则简单，和 `momentum_topn` 共用同一套输出框架

## 5. 缺点

- 很容易接到“越跌越买”的刀口
- 对市场 regime 很敏感
- 实盘解释成本通常高于趋势类策略

## 6. 更适合的用途

- 研究对照组
- 与 `momentum_topn` 对照“延续”与“反转”哪类逻辑更适合当前 ETF 池
- 与 `volatility_adjusted_momentum` 对照“追强稳健”与“抄弱反弹”的差别

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/reversal_bottomn.json`
- `configs/daily_signal_reversal_bottomn.json`
