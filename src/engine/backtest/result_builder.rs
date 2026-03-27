use super::{BacktestSummary, MomentumTopNResult};
use crate::metrics::max_drawdown;
use crate::report::{ContributionRow, HoldingTraceRow, RebalanceRow, RiskEventRow};
use chrono::NaiveDate;
use std::collections::HashMap;

pub(super) struct RotationResultBuildInput {
    pub(super) total_equity: f64,
    pub(super) trade_count: usize,
    pub(super) total_cost_paid: f64,
    pub(super) halted_by_risk: bool,
    pub(super) halt_reason: Option<String>,
    pub(super) equity_curve: Vec<(NaiveDate, f64)>,
    pub(super) rebalances: Vec<RebalanceRow>,
    pub(super) holdings_trace: Vec<HoldingTraceRow>,
    pub(super) contributions: Vec<ContributionRow>,
    pub(super) risk_events: Vec<RiskEventRow>,
    pub(super) contribution_sum: HashMap<String, f64>,
}

pub(super) fn build_rotation_result(input: RotationResultBuildInput) -> MomentumTopNResult {
    let RotationResultBuildInput {
        total_equity,
        trade_count,
        total_cost_paid,
        halted_by_risk,
        halt_reason,
        equity_curve,
        rebalances,
        holdings_trace,
        contributions,
        risk_events,
        contribution_sum,
    } = input;

    let only_curve: Vec<f64> = equity_curve.iter().map(|(_, equity)| *equity).collect();
    let mut contrib_vec: Vec<(String, f64)> = contribution_sum.into_iter().collect();
    contrib_vec.sort_by(|a, b| b.1.total_cmp(&a.1));
    let top_contributor = contrib_vec.first().cloned();
    let worst_contributor = contrib_vec.last().cloned();

    let summary = BacktestSummary {
        total_return: total_equity - 1.0,
        max_drawdown: max_drawdown(&only_curve),
        trade_count,
        total_cost_paid,
        final_equity: total_equity,
        halted_by_risk,
        halt_reason,
    };

    MomentumTopNResult {
        summary,
        equity_curve,
        rebalances,
        holdings_trace,
        contributions,
        risk_events,
        top_contributor,
        worst_contributor,
    }
}
