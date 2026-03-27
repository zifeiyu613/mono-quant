# 策略选择手册

这份文档不是讲“策略怎么实现”，而是讲：

- 当前项目里有哪些策略
- 它们分别适合做什么
- 如果目标是尽快进入实际运行，优先该看谁

本文基于项目当前策略池与最近一次扩展对比结果整理。  
当前对比快照来自：

- `output/strategy_compare_extended_v1_processed/comparison.csv`
- `output/strategy_compare_extended_v1_processed/comparison_summary.txt`

当前数据快照结束日期为：**2026-03-26**

---

## 一、先说结论

如果以“现在就要开始筛选实盘前候选”为目标，建议这样看：

### 1. 基准策略

优先保留：

- `buy_hold_single`
- `buy_hold_equal_weight`

原因：

- 它们是最简单、最不容易误读的基准
- 所有主动策略都必须持续对照这两个策略
- 如果主动策略长期打不过它们，就没有继续复杂化的必要

### 2. 研究主候选与补充对照

优先保留：

- `low_volatility_topn`
- `absolute_momentum_breadth`
- `dual_momentum`
- `volatility_adjusted_momentum`
- `ma_timing_single`
- `breakout_timing_single`
- `breakout_rotation_topn`

原因：

- `low_volatility_topn` 这一轮直接冲到主动策略第一名，收益和回撤都非常强
- `absolute_momentum_breadth` 这轮新增后，收益和回撤都进入主动候选第一梯队
- `dual_momentum` 目前是主动策略里最完整、也最像“候选主策略”的一个
- `volatility_adjusted_momentum` 当前是新增策略里最强的多资产主动轮动候选
- `ma_timing_single` 和 `breakout_timing_single` 都更简单，更适合做“低复杂度运行候选”
- `breakout_rotation_topn` 收益不高，但低换手、未停机，适合保留为“趋势确认型”补充对照
- 这七者能形成一组非常好的对照：
  - 最低波动率偏好
  - 多资产绝对动量广度
  - 多资产主动轮动
  - 风险调整后的主动轮动
  - 单资产趋势过滤
  - 单资产突破过滤
  - 多资产突破确认

### 3. 当前应降级的策略

当前建议降级观察：

- `absolute_momentum_single`
- `ma_rotation_topn`
- `reversal_bottomn`
- `momentum_topn`
- `risk_off_rotation`

原因：

- 它们在当前样本下都期末进入风控停机
- 总收益也落后于当前一线候选或基准组
- 这不等于它们“没价值”，但说明现阶段不该优先把它们推向运行层

---

## 二、策略分类矩阵

| 策略 | 类型 | 复杂度 | 主要用途 | 当前建议 |
| --- | --- | --- | --- | --- |
| `buy_hold_single` | 单资产基准 | 低 | 最低对照组 | 必留 |
| `buy_hold_equal_weight` | 多资产基准 | 低 | 分散化对照组 | 必留 |
| `low_volatility_topn` | 多资产低波轮动 | 中 | 研究主候选 / 稳健型运行候选 | 优先 |
| `absolute_momentum_breadth` | 多资产绝对动量广度 | 中 | 研究主候选 / 运行候选 | 优先 |
| `absolute_momentum_single` | 单资产开关 | 低 | 运行候选 / 防守开关验证 | 保留 |
| `breakout_timing_single` | 单资产突破择时 | 低到中 | 低复杂度运行候选 | 观察 |
| `ma_timing_single` | 单资产均线择时 | 低到中 | 低复杂度运行候选 | 优先 |
| `ma_rotation_topn` | 多资产均线轮动 | 中 | 趋势过滤研究对照 | 降级观察 |
| `relative_strength_pair` | 双资产切换 | 低到中 | 审核友好型运行对照 | 观察 |
| `momentum_topn` | 多资产相对动量 | 中 | 研究母策略 / 参数实验 | 降级观察 |
| `volatility_adjusted_momentum` | 多资产风险调整动量 | 中 | 研究候选 / 平滑化轮动 | 新增观察 |
| `breakout_rotation_topn` | 多资产突破轮动 | 中 | 趋势确认型轮动对照 | 观察 |
| `reversal_bottomn` | 多资产反转轮动 | 中 | 反命题研究对照 | 研究保留 |
| `dual_momentum` | 多资产双动量 | 中 | 研究主候选 / 运行候选 | 优先 |
| `risk_off_rotation` | 风险开关轮动 | 中 | 进攻 / 防守切换研究 | 降级观察 |
| `ma_single` | 原始单资产均线 | 低 | 原型验证 / 教学样例 | 保留但不作为主框架候选 |

---

## 三、如何理解每类策略

### 1. 基准类

包括：

- `buy_hold_single`
- `buy_hold_equal_weight`

你可以把它们理解成“什么都不做”和“只做最朴素分散”。

这两类策略最大的价值不是收益高，而是：

- 提供解释锚点
- 降低自我欺骗风险
- 让你知道主动策略到底有没有真实增益

### 2. 单资产过滤类

包括：

- `absolute_momentum_single`
- `ma_timing_single`
- `breakout_timing_single`

它们的共同特点是：

- 只关注一个核心风险资产
- 再配一个防守资产或空仓
- 信号解释成本低
- 更容易接入 daily signal 和人工审核

如果你想尽快走向实际运行，这一类非常重要，因为它们：

- 比多资产轮动更容易理解
- 更容易人工判断“这次切换合不合理”
- 更适合作为第一批半自动运行候选

### 3. 双资产轻轮动类

包括：

- `relative_strength_pair`

它的特点是：

- 只比较两个资产
- 规则比多资产排序更直观
- 仍然保留最小的横向择强能力

这一类的价值在于：

- 很适合人工审核
- 很适合做运行层最小切换骨架
- 很适合放在单资产择时和多资产轮动之间当中间层

### 4. 多资产主动轮动类

包括：

- `absolute_momentum_breadth`
- `low_volatility_topn`
- `momentum_topn`
- `volatility_adjusted_momentum`
- `breakout_rotation_topn`
- `reversal_bottomn`
- `dual_momentum`
- `risk_off_rotation`

它们的共同特点是：

- 能做横向比较
- 潜在收益弹性更高
- 同时也更依赖参数和样本环境

在这组里，目前最值得继续推进的是：

- `low_volatility_topn`
- `absolute_momentum_breadth`
- `volatility_adjusted_momentum`
- `dual_momentum`

其中：

- `low_volatility_topn` 代表“直接优先持有最平稳的资产”
- `absolute_momentum_breadth` 代表“先看整个资产池还有多少可做资产”
- `volatility_adjusted_momentum` 代表“风险调整后的排序”
- `dual_momentum` 代表“相对动量 + 绝对过滤 + 防守回退”

这四者都比 `momentum_topn` 更成熟，也比 `risk_off_rotation` 更不容易把组合过度集中到单一逻辑上。

---

## 四、当前快照下的策略判断

下面这张表基于 `2026-03-26` 截止的扩展对比结果。

| 排名 | 策略 | 总收益 | 最大回撤 | 调仓次数 | 期末风控停机 | 当前判断 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `buy_hold_equal_weight` | 48.64% | -33.41% | 1 | 否 | 当前最强基准 |
| 2 | `low_volatility_topn` | 42.13% | -18.14% | 57 | 否 | 第 6 批新增后直接成为最强主动策略 |
| 3 | `volatility_adjusted_momentum` | 28.57% | -20.20% | 42 | 否 | 风险调整排序仍然有效 |
| 4 | `breakout_timing_single` | 27.54% | -16.52% | 23 | 否 | 当前最强低复杂度运行候选 |
| 5 | `absolute_momentum_breadth` | 21.79% | -18.46% | 45 | 否 | 第四批新增后仍稳居一线 |
| 6 | `dual_momentum` | 20.79% | -18.92% | 41 | 否 | 当前仍然是核心主动候选 |
| 7 | `buy_hold_single` | 18.27% | -38.47% | 1 | 否 | 单资产长期持有基准 |
| 8 | `ma_timing_single` | 8.06% | -20.68% | 27 | 否 | 低复杂度运行候选 |
| 9 | `breakout_rotation_topn` | 5.93% | -17.49% | 7 | 否 | 低换手、未停机，但收益偏弱 |
| 10 | `reversal_bottomn` | 17.61% | -19.86% | 48 | 是 | 适合做反转对照，不适合先前推 |
| 11 | `relative_strength_pair` | 16.48% | -20.63% | 28 | 是 | 轻量双资产切换，对照价值大于当前运行价值 |
| 12 | `absolute_momentum_single` | 5.71% | -19.93% | 27 | 是 | 可保留，但暂不前推 |
| 13 | `ma_rotation_topn` | 5.10% | -28.31% | 27 | 是 | 均线过滤版轮动当前样本下不强 |
| 14 | `momentum_topn` | -6.17% | -19.24% | 38 | 是 | 当前样本下弱化 |
| 15 | `risk_off_rotation` | -9.78% | -21.78% | 37 | 是 | 当前样本下弱化 |

### 这一轮最重要的观察

#### 观察 1：最优结果来自最简单的多资产基准

`buy_hold_equal_weight` 当前排第一，这说明：

- 当前 ETF 池在这个样本期里，本身已经存在明显的分散化收益
- 主动轮动并不天然优于简单分散
- 后续如果要继续推主动策略，必须证明自己在更多窗口里持续优于这个基准

#### 观察 2：`low_volatility_topn` 是第 6 批里最强的新候选

`low_volatility_topn` 这一轮非常亮眼：

- 总收益达到 `42.13%`
- 最大回撤只有 `-18.14%`
- 期末没有停机

这说明在当前 ETF 池和当前样本下，“直接优先持有低波资产”不是保守到没收益，反而跑出了非常强的风险收益比。

#### 观察 3：新增后的主动轮动里，`volatility_adjusted_momentum` 仍然很值得重点追踪

`volatility_adjusted_momentum` 当前同时满足下面两点：

- 期末没有处于风控停机
- 收益明显高于其他主动轮动候选

这说明“收益 / 波动”的排序方式，至少在当前样本下，比纯收益排序更有效。

#### 观察 4：`absolute_momentum_breadth` 仍然是高质量候选

`absolute_momentum_breadth` 这一轮同时具备：

- 期末未停机
- 总收益高于 `dual_momentum`
- 回撤也控制在主动候选第一梯队

这说明“先用绝对门槛筛掉整体偏弱资产，再对剩余资产等权分散”这条思路，当前样本下是成立的。

#### 观察 5：`dual_momentum` 仍然保留核心位置

虽然 `absolute_momentum_breadth` 这轮成绩更强，但 `dual_momentum` 仍然有两个重要价值：

- 逻辑更成熟，之前已经做了更多修复和验证
- 绝对动量过滤 + 防守回退，仍然很适合运行层解释

所以如果现在要继续推进主动策略，优先级可以是：

**`low_volatility_topn` > `volatility_adjusted_momentum` ≈ `absolute_momentum_breadth` ≈ `dual_momentum`**

#### 观察 6：低复杂度运行候选里，`breakout_timing_single` 当前最强

当前样本下：

- `breakout_timing_single` 未停机，且收益高于其他低复杂度择时候选
- `ma_timing_single` 未停机
- `absolute_momentum_single` 期末处于停机状态

这说明在当前这组 ETF 和当前样本下，“突破确认”比“均线趋势过滤”和“简单绝对收益门槛”都更强。

但这仍然只是当前快照，不代表未来一定如此。

#### 观察 7：`breakout_rotation_topn` 适合保留，但更像低换手观察组

`breakout_rotation_topn` 这一轮的特点很鲜明：

- 未停机
- 最大回撤只有 `-17.49%`
- 但总收益只有 `5.93%`
- 调仓次数只有 `7` 次

这说明它更像一条“很克制、很慢”的趋势确认策略。

如果你要的是：

- 信号少一点
- 调仓少一点
- 逻辑直观一点

它有保留价值；但如果你要的是更强收益弹性，它还不够。

#### 观察 8：`ma_rotation_topn` 当前样本下不成立，不宜继续前推

`ma_rotation_topn` 这轮的问题很直接：

- 总收益只有 `5.10%`
- 最大回撤达到 `-28.31%`
- 期末还处于风控停机

这说明“均线过滤 + TopN 轮动”在当前样本下，并没有形成比现有主候选更好的平衡。

它可以保留在研究池，但不应该继续前推。

#### 观察 9：`relative_strength_pair` 有保留价值，但当前更像审核友好型对照

`relative_strength_pair` 这轮结果有两个特点：

- 总收益 16.48%，并不差
- 但期末仍触发风控停机，说明稳定性还不够

因此它更适合作为：

- 低复杂度切换策略对照
- 运行层人工审核模板
- 后续继续调参的双资产骨架

而不是现在就取代 `breakout_timing_single` 或 `dual_momentum`。

#### 观察 10：`reversal_bottomn` 适合保留在研究池，作为反命题验证

`reversal_bottomn` 这轮并没有跑出负收益，说明当前样本里并非完全没有反转机会。

但它仍然：

- 期末触发风控停机
- 调仓次数最高
- 运行解释成本明显高于趋势类策略

所以它的价值更偏：

- 检验“当前市场更偏趋势还是反转”
- 给 `momentum_topn` 提供反向对照

#### 观察 11：`momentum_topn` 和 `risk_off_rotation` 暂时不适合继续前推

它们更适合：

- 保留在研究池
- 后续做参数重检
- 观察在其他样本窗口里是否恢复优势

而不适合马上进入“优先运行候选”

---

## 五、如果目标是实际运行，建议怎么选

### 路线 A：最稳妥

保留以下三类：

- 基准：`buy_hold_equal_weight`
- 主动主候选：`low_volatility_topn`、`volatility_adjusted_momentum`、`absolute_momentum_breadth`、`dual_momentum`
- 低复杂度候选：`breakout_timing_single`
- 补充对照：`breakout_rotation_topn`

这是目前最合理的一组：

- 有基准
- 有主动多资产
- 有低复杂度可执行候选
- 有低换手的趋势确认对照

### 路线 B：更偏保守运行

如果你更在乎“信号好解释、人工好审核”，优先级可以是：

1. `breakout_timing_single`
2. `breakout_rotation_topn`
3. `ma_timing_single`
4. `low_volatility_topn`
5. `absolute_momentum_breadth`
6. `dual_momentum`

这条路线的特点是：

- 先把低复杂度策略跑顺
- 再逐步上多资产轮动

### 路线 C：继续研究优先

如果下一阶段重点还是做研究而不是运行，建议保留：

- `momentum_topn`
- `reversal_bottomn`
- `dual_momentum`
- `risk_off_rotation`
- `buy_hold_equal_weight`

因为这组最适合继续做：

- 参数敏感性
- walk-forward
- 样本内 / 样本外验证

---

## 六、当前推荐的实际推进顺序

按“最快接近实际运行”的标准，我建议后续顺序是：

1. 继续把 `dual_momentum` 作为主动主候选
2. 把 `low_volatility_topn`、`volatility_adjusted_momentum` 和 `absolute_momentum_breadth` 放在同一梯队继续对照
3. 把 `breakout_timing_single` 推进为低复杂度运行候选
4. 把 `breakout_rotation_topn` 保留为低换手趋势确认对照
5. 保留 `ma_timing_single` 作为第二低复杂度对照
6. 保留 `buy_hold_equal_weight` 作为长期对照基准
7. 暂时不把 `ma_rotation_topn` / `reversal_bottomn` / `momentum_topn` / `risk_off_rotation` 继续往运行层前推
8. 后续再补：
-   人工覆写
-   执行回写
-   paper trading

---

## 七、配套文档入口

如需看每个策略的详细说明，请继续看：

- `docs/strategies/README.md`
- `docs/strategies/buy_hold_single.md`
- `docs/strategies/buy_hold_equal_weight.md`
- `docs/strategies/absolute_momentum_breadth.md`
- `docs/strategies/absolute_momentum_single.md`
- `docs/strategies/low_volatility_topn.md`
- `docs/strategies/ma_rotation_topn.md`
- `docs/strategies/relative_strength_pair.md`
- `docs/strategies/momentum_topn.md`
- `docs/strategies/breakout_rotation_topn.md`
- `docs/strategies/volatility_adjusted_momentum.md`
- `docs/strategies/reversal_bottomn.md`
- `docs/strategies/dual_momentum.md`
- `docs/strategies/risk_off_rotation.md`
- `docs/strategies/ma_timing_single.md`
- `docs/strategies/breakout_timing_single.md`
- `docs/strategies/ma_single.md`
