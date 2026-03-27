# `low_volatility_topn` 策略说明

## 1. 策略定位

`low_volatility_topn` 是给当前动量系策略池补的一条“低波动偏好”对照。

它不追最强收益，而是优先选择：

**最近一段时间波动最小、走势最平稳的资产。**

## 2. 信号规则

每到调仓日：

1. 计算每个资产在 `lookback` 窗口内的日收益波动率
2. 按波动率从低到高排序
3. 选出最平稳的前 `top_n` 个资产
4. 若没有可选资产，则进入 `defensive_asset`，若未配置则空仓

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`
- `defensive_asset`

这个策略的核心，不是“谁涨最多”，而是“谁最稳”。

## 4. 优点

- 能和动量类策略形成明确对照
- 回撤和换手通常更容易控制
- 很适合观察当前 ETF 池是否存在“低波稳健收益”特征

## 5. 缺点

- 牛市里容易明显跑输强趋势资产
- 对快速切换行情的反应会偏慢
- 低波不等于高收益，策略上限通常不高

## 6. 更适合的用途

- 研究对照组
- 与 `volatility_adjusted_momentum` 对比“低波优先”与“收益/波动比优先”的差别
- 与 `buy_hold_equal_weight` 对比“低波筛选”是否真的带来更稳表现

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/low_volatility_topn.json`
- `configs/daily_signal_low_volatility_topn.json`
