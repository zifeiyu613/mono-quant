# 研究治理工作流说明

当前项目已经在 `momentum_batch` 上支持一个轻量版研究治理闭环，用来把“批量实验”进一步升级成“带研究主题和假设的研究过程”。

## 一、适用场景

适合下面这类问题：

- 当前这一轮实验到底在回答什么问题？
- 哪个假设已经获得较强支持？
- 哪个假设证据不足或暂不支持？
- 这一轮之后应该继续推进、继续收敛，还是先暂停？

这套机制主要对应批量实验场景，而不是单次回测。

## 二、配置方式

在 `configs/momentum_batch.json` 中可以增加 `research` 段：

```json
{
  "research": {
    "topic": "A股风格 ETF 动量轮动参数收敛",
    "round": "round_1",
    "objective": "验证 lookback、Top N 和调仓频率对收益、回撤与成本的影响方向",
    "sample_split": {
      "mode": "ratio",
      "in_sample_ratio": 0.7
    },
    "hypotheses": [
      {
        "id": "H1",
        "statement": "较短 lookback 更适合当前 ETF 池",
        "rule": "prefer_short_lookback",
        "preferred_max_lookback": 20,
        "min_return_delta": 0.003
      }
    ]
  }
}
```

### `sample_split`

用于开启样本内 / 样本外拆分评估，当前支持两种方式：

- `mode = "ratio"`：按比例拆分，例如 `0.7`
- `mode = "date"`：按边界日期拆分，例如 `2024-09-01`

### `decision_override`

用于在自动研究决策之上追加人工覆写，例如：

```json
{
  "decision_override": {
    "final_state": "testing",
    "recommended_action": "保留 H1 和 H3，优先增加样本外验证与更长时间窗复核",
    "reason": "当前样本外长度仍偏短，先不把自动结论直接当成最终研究结论。",
    "owner": "will",
    "decided_at": "2026-03-26"
  }
}
```

## 三、当前支持的规则

### `prefer_short_lookback`

把实验分成两组：

- 偏好组：`lookback <= preferred_max_lookback`
- 基线组：`lookback > preferred_max_lookback`

### `prefer_higher_top_n`

把实验分成两组：

- 偏好组：`top_n >= preferred_min_top_n`
- 基线组：`top_n == 1`

### `prefer_slower_rebalance`

把实验分成两组：

- 偏好组：`rebalance_freq >= preferred_min_rebalance_freq`
- 基线组：`rebalance_freq < preferred_min_rebalance_freq`

## 四、自动评估逻辑

每个假设会按下面三个维度做简单打分：

- 偏好组平均收益是否更高
- 偏好组平均最大回撤是否更优
- 偏好组平均总成本是否更低

然后把总分映射成支持度等级：

- `strongly_supported`
- `partially_supported`
- `inconclusive`
- `not_supported`
- `rejected`

这是一个轻量版、规则驱动的研究治理方案，目标是先把研究判断结构化，而不是追求复杂评分模型。

## 五、样本内 / 样本外评估

当配置了 `sample_split` 后，系统会：

1. 先按对齐后的共同交易日切分样本
2. 对每组参数分别跑：
   - 全样本
   - 样本内
   - 样本外
3. 分别生成三套假设支持度评估

这比只看全样本更可靠，因为它能帮助你区分：

- 全样本看起来有效，但样本外不稳定
- 样本内强支持，但样本外仍需继续验证
- 样本外也保持支持，结论更可信

## 六、状态机升级

当前实现的研究状态包括：

- `exploring`
- `testing`
- `refining`
- `validated`
- `paused`

一个简化理解是：

- `testing`：样本内已有支持，但样本外证据还不充分
- `validated`：样本外也出现较稳定支持
- `refining`：全样本仍有支持，但还需要继续收敛

## 七、输出文件

当 `momentum_batch` 配置了 `research` 后，输出目录会额外生成：

- `hypothesis_assessment.csv`：每个假设的自动评估结果
- `hypothesis_assessment_in_sample.csv`：样本内假设评估
- `hypothesis_assessment_out_of_sample.csv`：样本外假设评估
- `research_plan.txt`：研究主题、轮次、目标与假设列表
- `research_decision_auto.txt`：自动研究决策
- `research_decision.txt`：最终研究决策（可能含人工覆写）
- `governance_summary.txt`：样本拆分、自动状态、最终状态、决策来源摘要
- `stage_report.txt`：带研究状态的阶段性报告

原有输出仍会保留：

- `batch_results.csv`
- `batch_results_in_sample.csv`
- `batch_results_out_of_sample.csv`
- `experiment_index.csv`
- `batch_summary.txt`
- `experiments/exp_*/`

## 八、人工覆写决策

自动决策的价值是统一标准，但最终研究判断仍然可以由研究者人工覆写。

当前覆写机制会保留：

- 自动状态
- 自动决策理由
- 最终人工状态
- 覆写原因
- 覆写人
- 覆写时间

这能避免两个常见问题：

- 研究者完全丢失自动结论
- 最终决策没有明确责任和理由

## 九、使用建议

- 一轮批量实验只保留一个明确研究主题
- 假设数量先控制在 2 到 4 个
- 每个假设尽量只回答一个具体问题
- 先用自动评分快速筛选，再人工复核关键实验目录
- 样本外评估优先级应高于全样本观感
- 如果做人工覆写，务必写明原因和负责人

## 十、下一步可以继续做什么

如果后续继续扩展，最值得补的是：

1. 支持更通用的假设比较规则
2. 给状态转移加入核心假设权重
3. 支持跨轮次研究状态演化
4. 给研究决策增加人工审批历史
