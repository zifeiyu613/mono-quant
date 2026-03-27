use crate::config::{
    DecisionOverrideConfig, HypothesisConfig, ResearchConfig, SampleSplitConfig, WalkForwardConfig,
};
use crate::report::HypothesisAssessmentRow;
use anyhow::{anyhow, bail};
use chrono::NaiveDate;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    Rejected,
    NotSupported,
    Inconclusive,
    PartiallySupported,
    StronglySupported,
}

impl SupportLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            SupportLevel::Rejected => "rejected",
            SupportLevel::NotSupported => "not_supported",
            SupportLevel::Inconclusive => "inconclusive",
            SupportLevel::PartiallySupported => "partially_supported",
            SupportLevel::StronglySupported => "strongly_supported",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchRowView {
    pub lookback: usize,
    pub rebalance_freq: usize,
    pub top_n: usize,
    pub unit_cost: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub total_cost_paid: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HypothesisAssessment {
    pub hypothesis_id: String,
    pub statement: String,
    pub rule: String,
    pub preferred_group: String,
    pub baseline_group: String,
    pub preferred_count: usize,
    pub baseline_count: usize,
    pub preferred_avg_return: f64,
    pub baseline_avg_return: f64,
    pub preferred_avg_drawdown: f64,
    pub baseline_avg_drawdown: f64,
    pub preferred_avg_cost: f64,
    pub baseline_avg_cost: f64,
    pub score: i32,
    pub support_level: SupportLevel,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchDecision {
    pub topic: String,
    pub round: String,
    pub objective: Option<String>,
    pub state: String,
    pub recommended_action: String,
    pub strongest_hypothesis: Option<String>,
    pub weakest_hypothesis: Option<String>,
    pub rationale: String,
    pub decision_source: String,
    pub basis: String,
    pub override_reason: Option<String>,
    pub override_owner: Option<String>,
    pub override_decided_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SampleSplitPlan {
    pub mode: String,
    pub split_date: NaiveDate,
    pub in_sample_start: NaiveDate,
    pub in_sample_end: NaiveDate,
    pub out_sample_start: NaiveDate,
    pub out_sample_end: NaiveDate,
    pub in_sample_rows: usize,
    pub out_sample_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardWindowPlan {
    pub window_index: usize,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    pub train_rows: usize,
    pub test_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardAssessmentRow {
    pub window_index: usize,
    pub train_start: String,
    pub train_end: String,
    pub test_start: String,
    pub test_end: String,
    pub train_rows: usize,
    pub test_rows: usize,
    pub hypothesis_id: String,
    pub statement: String,
    pub support_level: String,
    pub score: i32,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardSummaryRow {
    pub hypothesis_id: String,
    pub statement: String,
    pub total_windows: usize,
    pub supported_windows: usize,
    pub rejected_windows: usize,
    pub strongest_support: String,
    pub weakest_support: String,
    pub average_score: f64,
    pub consistency_ratio: f64,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CostSensitivityDetailRow {
    pub unit_cost: f64,
    pub hypothesis_id: String,
    pub statement: String,
    pub support_level: String,
    pub score: i32,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CostSensitivitySummaryRow {
    pub hypothesis_id: String,
    pub statement: String,
    pub cost_bucket_count: usize,
    pub stable_bucket_count: usize,
    pub strongest_support: String,
    pub weakest_support: String,
    pub score_spread: i32,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceSummaryRow {
    pub hypothesis_id: String,
    pub statement: String,
    pub full_sample_support: String,
    pub in_sample_support: String,
    pub out_of_sample_support: String,
    pub walk_forward_supported_windows: usize,
    pub walk_forward_total_windows: usize,
    pub cost_bucket_count: usize,
    pub stable_cost_bucket_count: usize,
    pub confidence_score: i32,
    pub confidence_level: String,
    pub failure_condition: String,
    pub rationale: String,
}

#[derive(Clone, Copy)]
pub struct EvidenceSummaryInput<'a> {
    pub full_assessments: &'a [HypothesisAssessment],
    pub in_sample_assessments: Option<&'a [HypothesisAssessment]>,
    pub out_of_sample_assessments: Option<&'a [HypothesisAssessment]>,
    pub walk_forward_summaries: &'a [WalkForwardSummaryRow],
    pub cost_summaries: &'a [CostSensitivitySummaryRow],
    pub data_start: Option<NaiveDate>,
    pub data_end: Option<NaiveDate>,
}

#[derive(Debug, Clone)]
struct AssessmentSummary {
    strong: usize,
    partial: usize,
    inconclusive: usize,
    rejected: usize,
    not_supported: usize,
}

pub fn assess_hypotheses(
    research: &ResearchConfig,
    rows: &[BatchRowView],
) -> Vec<HypothesisAssessment> {
    research
        .hypotheses
        .iter()
        .map(|hypothesis| assess_one(hypothesis, rows))
        .collect()
}

fn assess_one(hypothesis: &HypothesisConfig, rows: &[BatchRowView]) -> HypothesisAssessment {
    let min_return_delta = hypothesis.min_return_delta.unwrap_or(0.01);

    let (preferred_group, baseline_group, preferred_rows, baseline_rows) =
        match hypothesis.rule.as_str() {
            "prefer_short_lookback" => {
                let threshold = hypothesis.preferred_max_lookback.unwrap_or(20);
                (
                    format!("lookback <= {}", threshold),
                    format!("lookback > {}", threshold),
                    rows.iter()
                        .filter(|row| row.lookback <= threshold)
                        .cloned()
                        .collect::<Vec<_>>(),
                    rows.iter()
                        .filter(|row| row.lookback > threshold)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            }
            "prefer_higher_top_n" => {
                let threshold = hypothesis.preferred_min_top_n.unwrap_or(2);
                (
                    format!("top_n >= {}", threshold),
                    "top_n = 1".to_string(),
                    rows.iter()
                        .filter(|row| row.top_n >= threshold)
                        .cloned()
                        .collect::<Vec<_>>(),
                    rows.iter()
                        .filter(|row| row.top_n == 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            }
            "prefer_slower_rebalance" => {
                let threshold = hypothesis.preferred_min_rebalance_freq.unwrap_or(60);
                (
                    format!("rebalance_freq >= {}", threshold),
                    format!("rebalance_freq < {}", threshold),
                    rows.iter()
                        .filter(|row| row.rebalance_freq >= threshold)
                        .cloned()
                        .collect::<Vec<_>>(),
                    rows.iter()
                        .filter(|row| row.rebalance_freq < threshold)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            }
            _ => {
                return HypothesisAssessment {
                    hypothesis_id: hypothesis.id.clone(),
                    statement: hypothesis.statement.clone(),
                    rule: hypothesis.rule.clone(),
                    preferred_group: "N/A".to_string(),
                    baseline_group: "N/A".to_string(),
                    preferred_count: 0,
                    baseline_count: 0,
                    preferred_avg_return: 0.0,
                    baseline_avg_return: 0.0,
                    preferred_avg_drawdown: 0.0,
                    baseline_avg_drawdown: 0.0,
                    preferred_avg_cost: 0.0,
                    baseline_avg_cost: 0.0,
                    score: 0,
                    support_level: SupportLevel::Inconclusive,
                    rationale: format!("不支持的规则类型：{}", hypothesis.rule),
                };
            }
        };

    if preferred_rows.is_empty() || baseline_rows.is_empty() {
        return HypothesisAssessment {
            hypothesis_id: hypothesis.id.clone(),
            statement: hypothesis.statement.clone(),
            rule: hypothesis.rule.clone(),
            preferred_group,
            baseline_group,
            preferred_count: preferred_rows.len(),
            baseline_count: baseline_rows.len(),
            preferred_avg_return: 0.0,
            baseline_avg_return: 0.0,
            preferred_avg_drawdown: 0.0,
            baseline_avg_drawdown: 0.0,
            preferred_avg_cost: 0.0,
            baseline_avg_cost: 0.0,
            score: 0,
            support_level: SupportLevel::Inconclusive,
            rationale: "偏好组或基线组缺少可用实验，当前无法完成比较".to_string(),
        };
    }

    let preferred_avg_return = avg(&preferred_rows, |row| row.total_return);
    let baseline_avg_return = avg(&baseline_rows, |row| row.total_return);
    let preferred_avg_drawdown = avg(&preferred_rows, |row| row.max_drawdown);
    let baseline_avg_drawdown = avg(&baseline_rows, |row| row.max_drawdown);
    let preferred_avg_cost = avg(&preferred_rows, |row| row.total_cost_paid);
    let baseline_avg_cost = avg(&baseline_rows, |row| row.total_cost_paid);

    let return_delta = preferred_avg_return - baseline_avg_return;
    let drawdown_improvement = preferred_avg_drawdown - baseline_avg_drawdown;
    let cost_delta = preferred_avg_cost - baseline_avg_cost;

    let mut score = 0;
    if return_delta >= min_return_delta {
        score += 2;
    } else if return_delta > 0.0 {
        score += 1;
    } else if return_delta <= -min_return_delta {
        score -= 2;
    } else if return_delta < 0.0 {
        score -= 1;
    }

    if drawdown_improvement >= 0.01 {
        score += 1;
    } else if drawdown_improvement <= -0.01 {
        score -= 1;
    }

    if cost_delta <= 0.0 {
        score += 1;
    } else {
        score -= 1;
    }

    let support_level = support_level_from_score(score);
    let rationale = format!(
        "preferred avg return {:.2}% vs baseline {:.2}%, preferred avg max drawdown {:.2}% vs baseline {:.2}%, preferred avg cost {:.6} vs baseline {:.6}",
        preferred_avg_return * 100.0,
        baseline_avg_return * 100.0,
        preferred_avg_drawdown * 100.0,
        baseline_avg_drawdown * 100.0,
        preferred_avg_cost,
        baseline_avg_cost,
    );

    HypothesisAssessment {
        hypothesis_id: hypothesis.id.clone(),
        statement: hypothesis.statement.clone(),
        rule: hypothesis.rule.clone(),
        preferred_group,
        baseline_group,
        preferred_count: preferred_rows.len(),
        baseline_count: baseline_rows.len(),
        preferred_avg_return,
        baseline_avg_return,
        preferred_avg_drawdown,
        baseline_avg_drawdown,
        preferred_avg_cost,
        baseline_avg_cost,
        score,
        support_level,
        rationale,
    }
}

fn avg<F>(rows: &[BatchRowView], accessor: F) -> f64
where
    F: Fn(&BatchRowView) -> f64,
{
    let sum: f64 = rows.iter().map(accessor).sum();
    sum / rows.len() as f64
}

fn support_level_from_score(score: i32) -> SupportLevel {
    if score >= 3 {
        SupportLevel::StronglySupported
    } else if score >= 1 {
        SupportLevel::PartiallySupported
    } else if score == 0 {
        SupportLevel::Inconclusive
    } else if score <= -3 {
        SupportLevel::Rejected
    } else {
        SupportLevel::NotSupported
    }
}

pub fn build_sample_split_plan(
    split_cfg: &SampleSplitConfig,
    aligned_dates: &[NaiveDate],
) -> anyhow::Result<SampleSplitPlan> {
    if aligned_dates.len() < 4 {
        bail!("对齐交易日不足，无法构建样本内/样本外切分");
    }

    let split_index = match split_cfg.mode.as_str() {
        "ratio" => {
            let ratio = split_cfg.in_sample_ratio.unwrap_or(0.7);
            if !(0.0..1.0).contains(&ratio) {
                bail!("in_sample_ratio 必须介于 0 和 1 之间");
            }
            let index = ((aligned_dates.len() as f64) * ratio).floor() as usize;
            index.clamp(1, aligned_dates.len() - 2)
        }
        "date" => {
            let split_date = split_cfg
                .split_date
                .as_ref()
                .ok_or_else(|| anyhow!("当 sample_split.mode=date 时必须提供 split_date"))?;
            let boundary = NaiveDate::parse_from_str(split_date, "%Y-%m-%d")
                .map_err(|_| anyhow!("split_date 必须使用 YYYY-MM-DD"))?;
            let index = aligned_dates
                .iter()
                .position(|date| *date >= boundary)
                .ok_or_else(|| anyhow!("split_date 超出了当前对齐交易日范围"))?;
            index.clamp(1, aligned_dates.len() - 2)
        }
        other => bail!("不支持的 sample_split.mode：{}", other),
    };

    let split_date = aligned_dates[split_index];
    Ok(SampleSplitPlan {
        mode: split_cfg.mode.clone(),
        split_date,
        in_sample_start: aligned_dates[0],
        in_sample_end: split_date,
        out_sample_start: aligned_dates[split_index + 1],
        out_sample_end: *aligned_dates.last().unwrap(),
        in_sample_rows: split_index + 1,
        out_sample_rows: aligned_dates.len() - split_index - 1,
    })
}

pub fn build_walk_forward_windows(
    walk_cfg: &WalkForwardConfig,
    aligned_dates: &[NaiveDate],
) -> anyhow::Result<Vec<WalkForwardWindowPlan>> {
    if aligned_dates.len() < 6 {
        bail!("对齐交易日不足，无法构建 walk-forward 窗口");
    }
    if !(0.0..1.0).contains(&walk_cfg.train_ratio) {
        bail!("walk_forward.train_ratio 必须介于 0 和 1 之间");
    }
    if !(0.0..1.0).contains(&walk_cfg.test_ratio) {
        bail!("walk_forward.test_ratio 必须介于 0 和 1 之间");
    }

    let total_rows = aligned_dates.len();
    let mut train_rows = ((total_rows as f64) * walk_cfg.train_ratio).floor() as usize;
    let mut test_rows = ((total_rows as f64) * walk_cfg.test_ratio).floor() as usize;

    train_rows = train_rows
        .max(walk_cfg.min_train_rows.unwrap_or(0))
        .clamp(2, total_rows - 2);
    test_rows = test_rows.max(walk_cfg.min_test_rows.unwrap_or(0)).max(2);

    if train_rows + test_rows > total_rows {
        bail!("walk-forward 初始训练窗口与测试窗口之和超过了可用交易日数量");
    }

    let max_windows = walk_cfg.max_windows.unwrap_or(usize::MAX);
    let mut windows = Vec::new();
    let mut train_end_index = train_rows - 1;

    while train_end_index + test_rows < total_rows && windows.len() < max_windows {
        let test_start_index = train_end_index + 1;
        let test_end_index = test_start_index + test_rows - 1;
        windows.push(WalkForwardWindowPlan {
            window_index: windows.len() + 1,
            train_start: aligned_dates[0],
            train_end: aligned_dates[train_end_index],
            test_start: aligned_dates[test_start_index],
            test_end: aligned_dates[test_end_index],
            train_rows: train_end_index + 1,
            test_rows,
        });
        train_end_index = test_end_index;
    }

    if windows.is_empty() {
        bail!("当前配置下无法生成任何 walk-forward 测试窗口");
    }

    Ok(windows)
}

pub fn decide_research_state(
    research: &ResearchConfig,
    full_assessments: &[HypothesisAssessment],
    in_sample_assessments: Option<&[HypothesisAssessment]>,
    out_of_sample_assessments: Option<&[HypothesisAssessment]>,
) -> ResearchDecision {
    let strongest = full_assessments
        .iter()
        .max_by_key(|item| item.support_level);
    let weakest = full_assessments
        .iter()
        .min_by_key(|item| item.support_level);
    let full_summary = summarize_assessments(full_assessments);
    let in_sample_summary = in_sample_assessments.map(summarize_assessments);
    let out_sample_summary = out_of_sample_assessments.map(summarize_assessments);

    let (state, recommended_action, rationale, basis) = if let Some(out_summary) =
        out_sample_summary.as_ref()
    {
        if out_summary.strong > 0 && out_summary.rejected == 0 && out_summary.not_supported == 0 {
            (
                    "validated".to_string(),
                    "进入下一轮研究，并把已验证假设转成更细的执行或扩展问题".to_string(),
                    "样本外评估已经出现强支持且没有明显被否定的核心假设，说明结论开始具备跨样本稳定性。".to_string(),
                    "out_of_sample".to_string(),
                )
        } else if in_sample_summary
            .as_ref()
            .map(|summary| summary.strong + summary.partial > 0)
            .unwrap_or(false)
            && (out_summary.inconclusive > 0 || out_summary.partial > 0)
        {
            (
                "testing".to_string(),
                "继续保持当前方向，但优先补样本外与成本敏感性验证".to_string(),
                "样本内已有支持信号，但样本外证据还在形成中，更适合视为验证阶段而不是直接确认。"
                    .to_string(),
                "in_sample_plus_out_of_sample".to_string(),
            )
        } else if full_summary.strong > 0 || full_summary.partial > 0 {
            (
                "refining".to_string(),
                "保留当前方向，但优先缩小参数空间并补验证实验".to_string(),
                "全样本仍有部分支持，但样本外没有形成足够一致的确认信号，适合继续收敛。"
                    .to_string(),
                "full_sample".to_string(),
            )
        } else if full_summary.rejected + full_summary.not_supported == full_assessments.len()
            && !full_assessments.is_empty()
        {
            (
                "paused".to_string(),
                "暂停当前方向，重写研究假设或更换资产池后再继续".to_string(),
                "全样本与样本外都缺乏支持，继续追加同类实验的价值有限。".to_string(),
                "full_sample".to_string(),
            )
        } else {
            (
                "exploring".to_string(),
                "保留探索状态，增加样本与验证维度后再决策".to_string(),
                "当前证据仍偏探索性，支持与否都不够集中。".to_string(),
                "full_sample".to_string(),
            )
        }
    } else if full_summary.strong > 0
        && full_summary.rejected == 0
        && full_summary.not_supported == 0
    {
        (
            "refining".to_string(),
            "继续推进当前方向，并优先补充样本外验证".to_string(),
            "全样本已经出现较强支持，但尚未做样本外拆分，适合先进入更严格的验证流程。".to_string(),
            "full_sample".to_string(),
        )
    } else if full_summary.strong > 0 || full_summary.partial > 0 {
        (
            "exploring".to_string(),
            "保留探索状态，增加样本与验证维度后再决策".to_string(),
            "已有初步支持，但在没有样本外验证时还不适合进一步升级状态。".to_string(),
            "full_sample".to_string(),
        )
    } else if full_summary.rejected + full_summary.not_supported == full_assessments.len()
        && !full_assessments.is_empty()
    {
        (
            "paused".to_string(),
            "暂停当前方向，重写研究假设或更换资产池后再继续".to_string(),
            "当前假设整体缺乏支持，继续加实验的边际收益较低。".to_string(),
            "full_sample".to_string(),
        )
    } else {
        (
            "exploring".to_string(),
            "保留探索状态，增加样本与验证维度后再决策".to_string(),
            "结果仍偏探索性，现阶段更适合补证据而不是直接下结论。".to_string(),
            "full_sample".to_string(),
        )
    };

    ResearchDecision {
        topic: research.topic.clone(),
        round: research.round.clone(),
        objective: research.objective.clone(),
        state,
        recommended_action,
        strongest_hypothesis: strongest.map(|item| item.hypothesis_id.clone()),
        weakest_hypothesis: weakest.map(|item| item.hypothesis_id.clone()),
        rationale,
        decision_source: "automatic".to_string(),
        basis,
        override_reason: None,
        override_owner: None,
        override_decided_at: None,
    }
}

pub fn apply_manual_override(
    auto_decision: &ResearchDecision,
    override_cfg: &DecisionOverrideConfig,
) -> ResearchDecision {
    ResearchDecision {
        topic: auto_decision.topic.clone(),
        round: auto_decision.round.clone(),
        objective: auto_decision.objective.clone(),
        state: override_cfg.final_state.clone(),
        recommended_action: override_cfg
            .recommended_action
            .clone()
            .unwrap_or_else(|| auto_decision.recommended_action.clone()),
        strongest_hypothesis: auto_decision.strongest_hypothesis.clone(),
        weakest_hypothesis: auto_decision.weakest_hypothesis.clone(),
        rationale: format!(
            "manual override applied. auto rationale: {} | override reason: {}",
            auto_decision.rationale, override_cfg.reason
        ),
        decision_source: "manual_override".to_string(),
        basis: auto_decision.basis.clone(),
        override_reason: Some(override_cfg.reason.clone()),
        override_owner: override_cfg.owner.clone(),
        override_decided_at: override_cfg.decided_at.clone(),
    }
}

pub fn render_walk_forward_plan(windows: &[WalkForwardWindowPlan]) -> String {
    let mut lines = vec![
        "=== Walk-Forward 计划 ===".to_string(),
        format!("窗口数量: {}", windows.len()),
    ];

    for window in windows {
        lines.push(format!(
            "- 窗口 {}: 训练 {} -> {} ({} 行), 测试 {} -> {} ({} 行)",
            window.window_index,
            window.train_start,
            window.train_end,
            window.train_rows,
            window.test_start,
            window.test_end,
            window.test_rows
        ));
    }

    lines.join("\n") + "\n"
}

pub fn walk_forward_detail_rows(
    windows: &[WalkForwardWindowPlan],
    window_assessments: &[Vec<HypothesisAssessment>],
) -> Vec<WalkForwardAssessmentRow> {
    let mut rows = Vec::new();

    for (window, assessments) in windows.iter().zip(window_assessments.iter()) {
        for assessment in assessments {
            rows.push(WalkForwardAssessmentRow {
                window_index: window.window_index,
                train_start: window.train_start.to_string(),
                train_end: window.train_end.to_string(),
                test_start: window.test_start.to_string(),
                test_end: window.test_end.to_string(),
                train_rows: window.train_rows,
                test_rows: window.test_rows,
                hypothesis_id: assessment.hypothesis_id.clone(),
                statement: assessment.statement.clone(),
                support_level: assessment.support_level.as_str().to_string(),
                score: assessment.score,
                rationale: assessment.rationale.clone(),
            });
        }
    }

    rows
}

pub fn summarize_walk_forward_assessments(
    research: &ResearchConfig,
    window_assessments: &[Vec<HypothesisAssessment>],
) -> Vec<WalkForwardSummaryRow> {
    let mut rows = Vec::new();

    for hypothesis in &research.hypotheses {
        let mut scores = Vec::new();
        let mut levels = Vec::new();
        for assessments in window_assessments {
            if let Some(assessment) = assessments
                .iter()
                .find(|item| item.hypothesis_id == hypothesis.id)
            {
                scores.push(assessment.score);
                levels.push(assessment.support_level);
            }
        }

        if levels.is_empty() {
            continue;
        }

        let supported_windows = levels
            .iter()
            .filter(|level| is_positive_support(**level))
            .count();
        let rejected_windows = levels
            .iter()
            .filter(|level| matches!(level, SupportLevel::Rejected | SupportLevel::NotSupported))
            .count();
        let strongest_support = *levels.iter().max().unwrap();
        let weakest_support = *levels.iter().min().unwrap();
        let average_score =
            scores.iter().map(|score| *score as f64).sum::<f64>() / scores.len() as f64;
        let consistency_ratio = supported_windows as f64 / levels.len() as f64;

        rows.push(WalkForwardSummaryRow {
            hypothesis_id: hypothesis.id.clone(),
            statement: hypothesis.statement.clone(),
            total_windows: levels.len(),
            supported_windows,
            rejected_windows,
            strongest_support: strongest_support.as_str().to_string(),
            weakest_support: weakest_support.as_str().to_string(),
            average_score,
            consistency_ratio,
            summary: format!(
                "在 {} 个窗口中有 {} 个窗口保持正向支持，拒绝/不支持窗口 {} 个",
                levels.len(),
                supported_windows,
                rejected_windows
            ),
        });
    }

    rows
}

pub fn cost_sensitivity_detail_rows(
    research: &ResearchConfig,
    rows: &[BatchRowView],
) -> Vec<CostSensitivityDetailRow> {
    let unit_costs = unique_unit_costs(rows);
    let mut details = Vec::new();

    for unit_cost in unit_costs {
        let scoped_rows: Vec<BatchRowView> = rows
            .iter()
            .filter(|row| (row.unit_cost - unit_cost).abs() < 1e-12)
            .cloned()
            .collect();
        for assessment in assess_hypotheses(research, &scoped_rows) {
            details.push(CostSensitivityDetailRow {
                unit_cost,
                hypothesis_id: assessment.hypothesis_id,
                statement: assessment.statement,
                support_level: assessment.support_level.as_str().to_string(),
                score: assessment.score,
                rationale: assessment.rationale,
            });
        }
    }

    details
}

pub fn summarize_cost_sensitivity(
    research: &ResearchConfig,
    details: &[CostSensitivityDetailRow],
) -> Vec<CostSensitivitySummaryRow> {
    let mut rows = Vec::new();

    for hypothesis in &research.hypotheses {
        let scoped: Vec<&CostSensitivityDetailRow> = details
            .iter()
            .filter(|item| item.hypothesis_id == hypothesis.id)
            .collect();
        if scoped.is_empty() {
            continue;
        }

        let first_support = parse_support_level(&scoped[0].support_level);
        let mut strongest_support = first_support;
        let mut weakest_support = first_support;
        let mut min_score = scoped[0].score;
        let mut max_score = scoped[0].score;
        let mut stable_bucket_count = 0usize;

        for item in &scoped {
            let level = parse_support_level(&item.support_level);
            if level == first_support {
                stable_bucket_count += 1;
            }
            strongest_support = strongest_support.max(level);
            weakest_support = weakest_support.min(level);
            min_score = min_score.min(item.score);
            max_score = max_score.max(item.score);
        }

        rows.push(CostSensitivitySummaryRow {
            hypothesis_id: hypothesis.id.clone(),
            statement: hypothesis.statement.clone(),
            cost_bucket_count: scoped.len(),
            stable_bucket_count,
            strongest_support: strongest_support.as_str().to_string(),
            weakest_support: weakest_support.as_str().to_string(),
            score_spread: max_score - min_score,
            summary: format!(
                "共比较 {} 个成本桶，其中 {} 个桶保持与首个成本桶相同的支持等级",
                scoped.len(),
                stable_bucket_count
            ),
        });
    }

    rows
}

pub fn build_evidence_summary(
    research: &ResearchConfig,
    input: EvidenceSummaryInput<'_>,
) -> Vec<EvidenceSummaryRow> {
    let walk_map: BTreeMap<String, WalkForwardSummaryRow> = input
        .walk_forward_summaries
        .iter()
        .cloned()
        .map(|item| (item.hypothesis_id.clone(), item))
        .collect();
    let cost_map: BTreeMap<String, CostSensitivitySummaryRow> = input
        .cost_summaries
        .iter()
        .cloned()
        .map(|item| (item.hypothesis_id.clone(), item))
        .collect();
    let data_span_days = input
        .data_start
        .zip(input.data_end)
        .map(|(start, end)| (end - start).num_days())
        .unwrap_or_default();

    research
        .hypotheses
        .iter()
        .map(|hypothesis| {
            let full_level = find_support_level(input.full_assessments, &hypothesis.id)
                .unwrap_or(SupportLevel::Inconclusive);
            let in_sample_level = input
                .in_sample_assessments
                .and_then(|items| find_support_level(items, &hypothesis.id));
            let out_sample_level = input
                .out_of_sample_assessments
                .and_then(|items| find_support_level(items, &hypothesis.id));
            let walk_summary = walk_map.get(&hypothesis.id);
            let cost_summary = cost_map.get(&hypothesis.id);

            let mut confidence_score = support_confidence_points(Some(full_level), 25);
            confidence_score += support_confidence_points(in_sample_level, 15);
            confidence_score += support_confidence_points(out_sample_level, 25);

            if let Some(summary) = walk_summary {
                let walk_points = if summary.total_windows == 0 {
                    0
                } else {
                    ((summary.supported_windows as f64 / summary.total_windows as f64) * 20.0)
                        .round() as i32
                };
                confidence_score +=
                    walk_points.saturating_sub((summary.rejected_windows as i32) * 4);
            }

            if let Some(summary) = cost_summary {
                let mut cost_points = 0;
                if summary.cost_bucket_count >= 2 {
                    cost_points += 5;
                }
                if summary.stable_bucket_count == summary.cost_bucket_count {
                    cost_points += 10;
                } else if summary.stable_bucket_count + 1 >= summary.cost_bucket_count {
                    cost_points += 6;
                }
                if parse_support_level(&summary.weakest_support) >= SupportLevel::PartiallySupported
                {
                    cost_points += 5;
                }
                confidence_score += cost_points.min(15);
            }

            confidence_score = confidence_score.clamp(0, 100);
            let confidence_level = match confidence_score {
                80..=100 => "高",
                60..=79 => "中",
                40..=59 => "观察中",
                _ => "低",
            };

            let mut failure_conditions = Vec::new();
            if data_span_days < 365 * 3 {
                failure_conditions.push("历史样本仍不足 3 年");
            }
            if !is_positive_support_option(out_sample_level) {
                failure_conditions.push("样本外支持仍不足");
            }
            if let Some(summary) = walk_summary {
                if summary.supported_windows < summary.total_windows {
                    failure_conditions.push("walk-forward 窗口稳定性不足");
                }
            } else {
                failure_conditions.push("尚未启用 walk-forward 验证");
            }
            if let Some(summary) = cost_summary {
                if summary.cost_bucket_count < 2 {
                    failure_conditions.push("成本敏感性覆盖不足");
                } else if summary.stable_bucket_count < summary.cost_bucket_count {
                    failure_conditions.push("结论对成本变化较敏感");
                }
            } else {
                failure_conditions.push("尚未输出成本敏感性结果");
            }
            if !is_positive_support(full_level) {
                failure_conditions.push("全样本支持也不够稳定");
            }
            if failure_conditions.is_empty() {
                failure_conditions.push("当前未发现明显失效条件，但仍需持续监控");
            }

            let walk_supported_windows =
                walk_summary.map(|item| item.supported_windows).unwrap_or(0);
            let walk_total_windows = walk_summary.map(|item| item.total_windows).unwrap_or(0);
            let cost_bucket_count = cost_summary.map(|item| item.cost_bucket_count).unwrap_or(0);
            let stable_cost_bucket_count = cost_summary
                .map(|item| item.stable_bucket_count)
                .unwrap_or(0);

            EvidenceSummaryRow {
                hypothesis_id: hypothesis.id.clone(),
                statement: hypothesis.statement.clone(),
                full_sample_support: full_level.as_str().to_string(),
                in_sample_support: in_sample_level
                    .map(|level| level.as_str().to_string())
                    .unwrap_or_else(|| "not_run".to_string()),
                out_of_sample_support: out_sample_level
                    .map(|level| level.as_str().to_string())
                    .unwrap_or_else(|| "not_run".to_string()),
                walk_forward_supported_windows: walk_supported_windows,
                walk_forward_total_windows: walk_total_windows,
                cost_bucket_count,
                stable_cost_bucket_count,
                confidence_score,
                confidence_level: confidence_level.to_string(),
                failure_condition: failure_conditions.join("；"),
                rationale: format!(
                    "全样本={}，样本内={}，样本外={}，walk-forward={}/{}, 成本稳定桶={}/{}",
                    full_level.as_str(),
                    in_sample_level
                        .map(|level| level.as_str())
                        .unwrap_or("not_run"),
                    out_sample_level
                        .map(|level| level.as_str())
                        .unwrap_or("not_run"),
                    walk_supported_windows,
                    walk_total_windows,
                    stable_cost_bucket_count,
                    cost_bucket_count
                ),
            }
        })
        .collect()
}

pub fn assessments_to_rows(assessments: &[HypothesisAssessment]) -> Vec<HypothesisAssessmentRow> {
    assessments
        .iter()
        .map(|item| HypothesisAssessmentRow {
            hypothesis_id: item.hypothesis_id.clone(),
            statement: item.statement.clone(),
            rule: item.rule.clone(),
            preferred_group: item.preferred_group.clone(),
            baseline_group: item.baseline_group.clone(),
            preferred_count: item.preferred_count,
            baseline_count: item.baseline_count,
            preferred_avg_return: item.preferred_avg_return,
            baseline_avg_return: item.baseline_avg_return,
            preferred_avg_drawdown: item.preferred_avg_drawdown,
            baseline_avg_drawdown: item.baseline_avg_drawdown,
            preferred_avg_cost: item.preferred_avg_cost,
            baseline_avg_cost: item.baseline_avg_cost,
            score: item.score,
            support_level: item.support_level.as_str().to_string(),
            rationale: item.rationale.clone(),
        })
        .collect()
}

pub fn render_research_plan(research: &ResearchConfig) -> String {
    let mut lines = vec![
        "=== 研究计划 ===".to_string(),
        format!("研究主题: {}", research.topic),
        format!("研究轮次: {}", research.round),
    ];

    if let Some(objective) = &research.objective {
        lines.push(format!("研究目标: {}", objective));
    }
    if let Some(split_cfg) = &research.sample_split {
        lines.push(format!("样本切分模式: {}", split_cfg.mode));
        if let Some(split_date) = &split_cfg.split_date {
            lines.push(format!("样本切分日期: {}", split_date));
        }
        if let Some(in_sample_ratio) = split_cfg.in_sample_ratio {
            lines.push(format!("样本内比例: {:.2}", in_sample_ratio));
        }
    }
    if let Some(walk_cfg) = &research.walk_forward {
        lines.push(format!(
            "walk-forward 训练比例: {:.2}",
            walk_cfg.train_ratio
        ));
        lines.push(format!("walk-forward 测试比例: {:.2}", walk_cfg.test_ratio));
        if let Some(max_windows) = walk_cfg.max_windows {
            lines.push(format!("walk-forward 最大窗口数: {}", max_windows));
        }
    }
    if let Some(override_cfg) = &research.decision_override {
        lines.push(format!("人工覆写最终状态: {}", override_cfg.final_state));
        lines.push(format!("人工覆写原因: {}", override_cfg.reason));
    }

    lines.push("研究假设：".to_string());
    for hypothesis in &research.hypotheses {
        lines.push(format!(
            "- {}: {} [{}]",
            hypothesis.id, hypothesis.statement, hypothesis.rule
        ));
    }

    lines.join("\n") + "\n"
}

pub fn render_research_decision(
    title: &str,
    decision: &ResearchDecision,
    full_assessments: &[HypothesisAssessment],
    in_sample_assessments: Option<&[HypothesisAssessment]>,
    out_of_sample_assessments: Option<&[HypothesisAssessment]>,
    evidence_summaries: &[EvidenceSummaryRow],
) -> String {
    let mut lines = vec![
        format!("=== {} ===", title),
        format!("研究主题: {}", decision.topic),
        format!("研究轮次: {}", decision.round),
        format!("当前状态: {}", decision.state),
        format!("建议动作: {}", decision.recommended_action),
        format!("决策理由: {}", decision.rationale),
        format!("决策来源: {}", decision.decision_source),
        format!("决策依据: {}", decision.basis),
    ];

    if let Some(objective) = &decision.objective {
        lines.push(format!("研究目标: {}", objective));
    }
    if let Some(strongest) = &decision.strongest_hypothesis {
        lines.push(format!("最强假设: {}", strongest));
    }
    if let Some(weakest) = &decision.weakest_hypothesis {
        lines.push(format!("最弱假设: {}", weakest));
    }
    if let Some(reason) = &decision.override_reason {
        lines.push(format!("覆写原因: {}", reason));
    }
    if let Some(owner) = &decision.override_owner {
        lines.push(format!("覆写人: {}", owner));
    }
    if let Some(decided_at) = &decision.override_decided_at {
        lines.push(format!("覆写时间: {}", decided_at));
    }

    lines.push("全样本假设评估：".to_string());
    for assessment in full_assessments {
        lines.push(format!(
            "- {}: {} (score={}, {})",
            assessment.hypothesis_id,
            assessment.statement,
            assessment.score,
            assessment.support_level.as_str()
        ));
    }
    if let Some(assessments) = in_sample_assessments {
        lines.push("样本内假设评估：".to_string());
        for assessment in assessments {
            lines.push(format!(
                "- {}: {} (score={}, {})",
                assessment.hypothesis_id,
                assessment.statement,
                assessment.score,
                assessment.support_level.as_str()
            ));
        }
    }
    if let Some(assessments) = out_of_sample_assessments {
        lines.push("样本外假设评估：".to_string());
        for assessment in assessments {
            lines.push(format!(
                "- {}: {} (score={}, {})",
                assessment.hypothesis_id,
                assessment.statement,
                assessment.score,
                assessment.support_level.as_str()
            ));
        }
    }
    if !evidence_summaries.is_empty() {
        lines.push("证据摘要：".to_string());
        for evidence in evidence_summaries {
            lines.push(format!(
                "- {}: 置信度={}({}), 主要失效条件={}",
                evidence.hypothesis_id,
                evidence.confidence_level,
                evidence.confidence_score,
                evidence.failure_condition
            ));
        }
    }

    lines.join("\n") + "\n"
}

pub fn render_governance_summary(
    plan: Option<&SampleSplitPlan>,
    walk_forward_windows: Option<&[WalkForwardWindowPlan]>,
    auto_decision: &ResearchDecision,
    final_decision: &ResearchDecision,
    evidence_summaries: &[EvidenceSummaryRow],
) -> String {
    let mut lines = vec![
        "=== 治理摘要 ===".to_string(),
        format!("自动状态: {}", auto_decision.state),
        format!("最终状态: {}", final_decision.state),
        format!("最终决策来源: {}", final_decision.decision_source),
        format!("决策依据: {}", auto_decision.basis),
    ];

    if let Some(plan) = plan {
        lines.push(format!("样本切分模式: {}", plan.mode));
        lines.push(format!("样本切分日期: {}", plan.split_date));
        lines.push(format!(
            "样本内区间: {} -> {} ({} 行)",
            plan.in_sample_start, plan.in_sample_end, plan.in_sample_rows
        ));
        lines.push(format!(
            "样本外区间: {} -> {} ({} 行)",
            plan.out_sample_start, plan.out_sample_end, plan.out_sample_rows
        ));
    } else {
        lines.push("样本切分: 未启用".to_string());
    }

    if let Some(windows) = walk_forward_windows {
        lines.push(format!("walk-forward 窗口数: {}", windows.len()));
    } else {
        lines.push("walk-forward: 未启用".to_string());
    }

    if let Some(reason) = &final_decision.override_reason {
        lines.push(format!("人工覆写原因: {}", reason));
    }
    if !evidence_summaries.is_empty() {
        lines.push("假设置信度概览：".to_string());
        for evidence in evidence_summaries {
            lines.push(format!(
                "- {}: {}({}) | {}",
                evidence.hypothesis_id,
                evidence.confidence_level,
                evidence.confidence_score,
                evidence.failure_condition
            ));
        }
    }

    lines.join("\n") + "\n"
}

fn unique_unit_costs(rows: &[BatchRowView]) -> Vec<f64> {
    let mut values: Vec<f64> = rows.iter().map(|row| row.unit_cost).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values.dedup_by(|a, b| (*a - *b).abs() < 1e-12);
    values
}

fn find_support_level(
    assessments: &[HypothesisAssessment],
    hypothesis_id: &str,
) -> Option<SupportLevel> {
    assessments
        .iter()
        .find(|item| item.hypothesis_id == hypothesis_id)
        .map(|item| item.support_level)
}

fn parse_support_level(value: &str) -> SupportLevel {
    match value {
        "strongly_supported" => SupportLevel::StronglySupported,
        "partially_supported" => SupportLevel::PartiallySupported,
        "inconclusive" => SupportLevel::Inconclusive,
        "not_supported" => SupportLevel::NotSupported,
        _ => SupportLevel::Rejected,
    }
}

fn is_positive_support(level: SupportLevel) -> bool {
    matches!(
        level,
        SupportLevel::StronglySupported | SupportLevel::PartiallySupported
    )
}

fn is_positive_support_option(level: Option<SupportLevel>) -> bool {
    level.map(is_positive_support).unwrap_or(false)
}

fn support_confidence_points(level: Option<SupportLevel>, max_points: i32) -> i32 {
    match level.unwrap_or(SupportLevel::Rejected) {
        SupportLevel::StronglySupported => max_points,
        SupportLevel::PartiallySupported => (max_points as f64 * 0.75).round() as i32,
        SupportLevel::Inconclusive => (max_points as f64 * 0.4).round() as i32,
        SupportLevel::NotSupported => (max_points as f64 * 0.15).round() as i32,
        SupportLevel::Rejected => 0,
    }
}

fn summarize_assessments(assessments: &[HypothesisAssessment]) -> AssessmentSummary {
    let mut summary = AssessmentSummary {
        strong: 0,
        partial: 0,
        inconclusive: 0,
        rejected: 0,
        not_supported: 0,
    };

    for assessment in assessments {
        match assessment.support_level {
            SupportLevel::StronglySupported => summary.strong += 1,
            SupportLevel::PartiallySupported => summary.partial += 1,
            SupportLevel::Inconclusive => summary.inconclusive += 1,
            SupportLevel::Rejected => summary.rejected += 1,
            SupportLevel::NotSupported => summary.not_supported += 1,
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalkForwardConfig;
    use chrono::Duration;

    fn mock_dates(count: usize) -> Vec<NaiveDate> {
        let start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        (0..count)
            .map(|offset| start + Duration::days(offset as i64))
            .collect()
    }

    #[test]
    fn walk_forward_windows_use_expanding_train_window() {
        let cfg = WalkForwardConfig {
            train_ratio: 0.5,
            test_ratio: 0.25,
            min_train_rows: None,
            min_test_rows: None,
            max_windows: Some(2),
        };
        let dates = mock_dates(260);

        let windows = build_walk_forward_windows(&cfg, &dates).unwrap();

        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].train_rows, 130);
        assert_eq!(windows[0].test_rows, 65);
        assert_eq!(windows[1].train_rows, 195);
        assert_eq!(windows[1].test_rows, 65);
    }

    #[test]
    fn evidence_summary_flags_short_history_and_missing_out_sample() {
        let research = ResearchConfig {
            topic: "topic".to_string(),
            round: "round".to_string(),
            objective: None,
            sample_split: None,
            walk_forward: None,
            decision_override: None,
            hypotheses: vec![HypothesisConfig {
                id: "H1".to_string(),
                statement: "statement".to_string(),
                rule: "prefer_short_lookback".to_string(),
                preferred_max_lookback: Some(20),
                preferred_min_top_n: None,
                preferred_min_rebalance_freq: None,
                min_return_delta: None,
            }],
        };
        let full = vec![HypothesisAssessment {
            hypothesis_id: "H1".to_string(),
            statement: "statement".to_string(),
            rule: "prefer_short_lookback".to_string(),
            preferred_group: "a".to_string(),
            baseline_group: "b".to_string(),
            preferred_count: 1,
            baseline_count: 1,
            preferred_avg_return: 0.02,
            baseline_avg_return: 0.01,
            preferred_avg_drawdown: -0.1,
            baseline_avg_drawdown: -0.12,
            preferred_avg_cost: 0.001,
            baseline_avg_cost: 0.002,
            score: 3,
            support_level: SupportLevel::StronglySupported,
            rationale: "ok".to_string(),
        }];

        let summary = build_evidence_summary(
            &research,
            EvidenceSummaryInput {
                full_assessments: &full,
                in_sample_assessments: None,
                out_of_sample_assessments: None,
                walk_forward_summaries: &[],
                cost_summaries: &[],
                data_start: NaiveDate::from_ymd_opt(2024, 1, 1),
                data_end: NaiveDate::from_ymd_opt(2024, 12, 31),
            },
        );

        assert_eq!(summary.len(), 1);
        assert!(summary[0].failure_condition.contains("历史样本仍不足 3 年"));
        assert!(summary[0].failure_condition.contains("样本外支持仍不足"));
    }
}
