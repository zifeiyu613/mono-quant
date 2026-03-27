# 策略说明总览

本目录按“策略用途 + 信号规则 + 适用场景”的方式整理项目中的每个策略。

如果你想先看“当前应该优先推进哪个策略”，建议先读：

- `docs/strategy-selection-guide.md`

## 策略清单

- `ma_single`：`docs/strategies/ma_single.md`
- `buy_hold_single`：`docs/strategies/buy_hold_single.md`
- `buy_hold_equal_weight`：`docs/strategies/buy_hold_equal_weight.md`
- `momentum_topn`：`docs/strategies/momentum_topn.md`
- `absolute_momentum_breadth`：`docs/strategies/absolute_momentum_breadth.md`
- `volatility_adjusted_momentum`：`docs/strategies/volatility_adjusted_momentum.md`
- `reversal_bottomn`：`docs/strategies/reversal_bottomn.md`
- `absolute_momentum_single`：`docs/strategies/absolute_momentum_single.md`
- `dual_momentum`：`docs/strategies/dual_momentum.md`
- `risk_off_rotation`：`docs/strategies/risk_off_rotation.md`
- `ma_timing_single`：`docs/strategies/ma_timing_single.md`
- `relative_strength_pair`：`docs/strategies/relative_strength_pair.md`
- `breakout_rotation_topn`：`docs/strategies/breakout_rotation_topn.md`
- `breakout_timing_single`：`docs/strategies/breakout_timing_single.md`

## 推荐阅读顺序

如果你刚接触这个项目，建议按下面顺序阅读：

1. `buy_hold_single`
2. `buy_hold_equal_weight`
3. `absolute_momentum_single`
4. `absolute_momentum_breadth`
5. `ma_timing_single`
6. `breakout_timing_single`
7. `relative_strength_pair`
8. `momentum_topn`
9. `volatility_adjusted_momentum`
10. `breakout_rotation_topn`
11. `reversal_bottomn`
12. `dual_momentum`
13. `risk_off_rotation`
14. `ma_single`

## 如何使用这些文档

每份文档都尽量回答同一组问题：

- 这个策略到底在做什么
- 它的信号生成规则是什么
- 关键参数分别影响什么
- 优点、缺点和常见误用是什么
- 更适合拿来做 benchmark、研究候选，还是运行候选

这样做的目的是让后续新增策略时，也能保持统一的说明口径。
