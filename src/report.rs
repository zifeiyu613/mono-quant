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
    pub top_contributor: String,
    pub worst_contributor: String,
    pub output_dir: String,
}

/// Ensure the output directory exists before writing any result artifacts.
pub fn ensure_output_dir(path: &str) -> anyhow::Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create output dir: {}", path))?;
    Ok(())
}

/// Write the portfolio equity curve to a CSV file.
pub fn write_equity_curve(path: &str, rows: &[EquityRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create equity csv: {}", path))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Write rebalance events and cost information to a CSV file.
pub fn write_rebalance_log(path: &str, rows: &[RebalanceRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create rebalance csv: {}", path))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Write daily holdings value and weight snapshots to a CSV file.
pub fn write_holdings_trace(path: &str, rows: &[HoldingTraceRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create holdings trace csv: {}", path))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Write per-asset daily and cumulative contribution records to a CSV file.
pub fn write_contributions(path: &str, rows: &[ContributionRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create contribution csv: {}", path))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Write experiment index rows so batch outputs can be navigated later.
pub fn write_experiment_index(path: &str, rows: &[ExperimentIndexRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create experiment index csv: {}", path))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Write a plain-text diagnostics or summary file.
pub fn write_diagnostics(path: &str, content: &str) -> anyhow::Result<()> {
    let mut f = File::create(path).with_context(|| format!("failed to create diagnostics: {}", path))?;
    f.write_all(content.as_bytes())?;
    Ok(())
}
