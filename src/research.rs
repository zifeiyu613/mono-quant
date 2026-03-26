use crate::config::{DecisionOverrideConfig, HypothesisConfig, ResearchConfig, SampleSplitConfig};
use crate::report::HypothesisAssessmentRow;
use anyhow::{anyhow, bail};
use chrono::NaiveDate;
use serde::Serialize;

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

    let (preferred_group, baseline_group, preferred_rows, baseline_rows) = match hypothesis.rule.as_str() {
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
            rationale: "preferred group or baseline group has no experiments".to_string(),
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
        bail!("not enough aligned dates to build in-sample/out-of-sample split");
    }

    let split_index = match split_cfg.mode.as_str() {
        "ratio" => {
            let ratio = split_cfg.in_sample_ratio.unwrap_or(0.7);
            if !(0.0..1.0).contains(&ratio) {
                bail!("in_sample_ratio must be between 0 and 1");
            }
            let index = ((aligned_dates.len() as f64) * ratio).floor() as usize;
            index.clamp(1, aligned_dates.len() - 2)
        }
        "date" => {
            let split_date = split_cfg
                .split_date
                .as_ref()
                .ok_or_else(|| anyhow!("split_date is required when sample_split.mode=date"))?;
            let boundary = NaiveDate::parse_from_str(split_date, "%Y-%m-%d")
                .map_err(|_| anyhow!("split_date must use YYYY-MM-DD"))?;
            let index = aligned_dates
                .iter()
                .position(|date| *date >= boundary)
                .ok_or_else(|| anyhow!("split_date is after the available aligned date range"))?;
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

pub fn decide_research_state(
    research: &ResearchConfig,
    full_assessments: &[HypothesisAssessment],
    in_sample_assessments: Option<&[HypothesisAssessment]>,
    out_of_sample_assessments: Option<&[HypothesisAssessment]>,
) -> ResearchDecision {
    let strongest = full_assessments.iter().max_by_key(|item| item.support_level);
    let weakest = full_assessments.iter().min_by_key(|item| item.support_level);
    let full_summary = summarize_assessments(full_assessments);
    let in_sample_summary = in_sample_assessments.map(summarize_assessments);
    let out_sample_summary = out_of_sample_assessments.map(summarize_assessments);

    let (state, recommended_action, rationale, basis) =
        if let Some(out_summary) = out_sample_summary.as_ref() {
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
                    "样本内已有支持信号，但样本外证据还在形成中，更适合视为验证阶段而不是直接确认。".to_string(),
                    "in_sample_plus_out_of_sample".to_string(),
                )
            } else if full_summary.strong > 0 || full_summary.partial > 0 {
                (
                    "refining".to_string(),
                    "保留当前方向，但优先缩小参数空间并补验证实验".to_string(),
                    "全样本仍有部分支持，但样本外没有形成足够一致的确认信号，适合继续收敛。".to_string(),
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
        } else if full_summary.strong > 0 && full_summary.rejected == 0 && full_summary.not_supported == 0 {
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

    lines.join("\n") + "\n"
}

pub fn render_governance_summary(
    plan: Option<&SampleSplitPlan>,
    auto_decision: &ResearchDecision,
    final_decision: &ResearchDecision,
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

    if let Some(reason) = &final_decision.override_reason {
        lines.push(format!("人工覆写原因: {}", reason));
    }

    lines.join("\n") + "\n"
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
