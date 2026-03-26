use anyhow::Context;
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;

#[derive(Debug, Serialize)]
pub struct EquityRow {
    pub date: String,
    pub equity: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct RebalanceRow {
    pub date: String,
    pub selected_assets: String,
    pub turnover_amount: f64,
    pub cost: f64,
    pub equity_before: f64,
    pub equity_after: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct HoldingTraceRow {
    pub date: String,
    pub asset: String,
    pub value: f64,
    pub weight: f64,
    pub total_equity: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct ContributionRow {
    pub date: String,
    pub asset: String,
    pub daily_contribution: f64,
    pub cumulative_contribution: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct RiskEventRow {
    pub date: String,
    pub event_type: String,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct ExperimentIndexRow {
    pub experiment_id: String,
    pub lookback: usize,
    pub rebalance_freq: usize,
    pub top_n: usize,
    pub unit_cost: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub trade_count: usize,
    pub total_cost_paid: f64,
    pub final_equity: f64,
    pub halted_by_risk: bool,
    pub halt_event_type: String,
    pub halt_reason: String,
    pub top_contributor: String,
    pub worst_contributor: String,
    pub output_dir: String,
}

#[derive(Debug, Serialize)]
pub struct HypothesisAssessmentRow {
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
    pub support_level: String,
    pub rationale: String,
}

/// 在写入结果文件前，确保输出目录存在。
pub fn ensure_output_dir(path: &str) -> anyhow::Result<()> {
    fs::create_dir_all(path).with_context(|| format!("创建输出目录失败：{}", path))?;
    Ok(())
}

/// 将任意可序列化行数组写入 CSV 文件。
pub fn write_csv_rows<T: Serialize>(path: &str, rows: &[T]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("创建 CSV 文件失败：{}", path))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// 将组合净值曲线写入 CSV 文件。
pub fn write_equity_curve(path: &str, rows: &[EquityRow]) -> anyhow::Result<()> {
    write_csv_rows(path, rows)
}

/// 将调仓事件和成本信息写入 CSV 文件。
pub fn write_rebalance_log(path: &str, rows: &[RebalanceRow]) -> anyhow::Result<()> {
    write_csv_rows(path, rows)
}

/// 将每日持仓市值和权重快照写入 CSV 文件。
pub fn write_holdings_trace(path: &str, rows: &[HoldingTraceRow]) -> anyhow::Result<()> {
    write_csv_rows(path, rows)
}

/// 将逐资产的每日归因和累计归因写入 CSV 文件。
pub fn write_contributions(path: &str, rows: &[ContributionRow]) -> anyhow::Result<()> {
    write_csv_rows(path, rows)
}

/// 将实验索引写入 CSV，便于后续定位批量实验输出。
pub fn write_experiment_index(path: &str, rows: &[ExperimentIndexRow]) -> anyhow::Result<()> {
    write_csv_rows(path, rows)
}

/// 将研究治理中的假设评估结果写入 CSV 文件。
pub fn write_hypothesis_assessments(
    path: &str,
    rows: &[HypothesisAssessmentRow],
) -> anyhow::Result<()> {
    write_csv_rows(path, rows)
}

/// 将纯文本诊断或摘要内容写入文件。
pub fn write_diagnostics(path: &str, content: &str) -> anyhow::Result<()> {
    let mut f = File::create(path).with_context(|| format!("创建诊断文件失败：{}", path))?;
    f.write_all(content.as_bytes())?;
    Ok(())
}
