use crate::config::RiskConfig;
use crate::data::{intersect_dates, Bar};
use crate::report::{ContributionRow, HoldingTraceRow, RebalanceRow, RiskEventRow};
use chrono::NaiveDate;
use performance_ops::{
    append_holdings_trace_rows, apply_daily_returns_and_record_contribution,
    portfolio_equity_or_total,
};
use rebalance_ops::{
    current_rebalance_base_equity, execute_equal_weight_rebalance, execute_to_cash_rebalance,
    EqualWeightRebalanceSpec,
};
use result_builder::{build_rotation_result, RotationResultBuildInput};
use risk_controls::{
    apply_asset_universe_guard, guard_selection_count, handle_cooldown_transition,
    maybe_trigger_daily_loss_stop, maybe_trigger_drawdown_stop, RiskState,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct BacktestSummary {
    pub total_return: f64,
    pub max_drawdown: f64,
    pub trade_count: usize,
    pub total_cost_paid: f64,
    pub final_equity: f64,
    pub halted_by_risk: bool,
    pub halt_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MomentumTopNResult {
    pub summary: BacktestSummary,
    pub equity_curve: Vec<(NaiveDate, f64)>,
    pub rebalances: Vec<RebalanceRow>,
    pub holdings_trace: Vec<HoldingTraceRow>,
    pub contributions: Vec<ContributionRow>,
    pub risk_events: Vec<RiskEventRow>,
    pub top_contributor: Option<(String, f64)>,
    pub worst_contributor: Option<(String, f64)>,
}

#[derive(Debug, Clone, Copy)]
pub struct RotationBacktestConfig<'a> {
    pub lookback: usize,
    pub rebalance_freq: usize,
    pub commission: f64,
    pub slippage: f64,
    pub risk: Option<&'a RiskConfig>,
}

mod ma_backtest;
mod performance_ops;
mod rebalance_ops;
mod result_builder;
mod risk_controls;
mod rotation_wrappers;
pub use ma_backtest::run_ma_backtest;
pub use rotation_wrappers::{
    run_absolute_momentum_breadth_backtest, run_absolute_momentum_single_backtest,
    run_adaptive_dual_momentum_backtest, run_breakdown_timing_single_backtest,
    run_breakout_rotation_topn_backtest, run_breakout_timing_single_backtest,
    run_buy_hold_equal_weight_backtest, run_buy_hold_single_backtest,
    run_defensive_pair_rotation_backtest, run_dual_momentum_backtest,
    run_low_volatility_topn_backtest, run_ma_rotation_topn_backtest, run_ma_timing_single_backtest,
    run_momentum_topn_backtest, run_relative_strength_pair_backtest, run_reversal_bottomn_backtest,
    run_risk_off_rotation_backtest, run_volatility_adjusted_momentum_backtest,
    run_volatility_target_rotation_backtest,
};

fn run_rotation_backtest<F>(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    lookback: usize,
    rebalance_freq: usize,
    commission: f64,
    slippage: f64,
    risk: Option<&RiskConfig>,
    mut select_assets: F,
) -> MomentumTopNResult
where
    F: FnMut(usize, &[NaiveDate], &HashMap<String, HashMap<NaiveDate, Bar>>) -> Vec<String>,
{
    let dates = intersect_dates(asset_maps);
    let mut aligned_closes: HashMap<String, Vec<f64>> = HashMap::with_capacity(asset_maps.len());
    for (asset, bars) in asset_maps {
        let mut closes = Vec::with_capacity(dates.len());
        for date in &dates {
            closes.push(bars.get(date).unwrap().close);
        }
        aligned_closes.insert(asset.clone(), closes);
    }

    let unit_cost = commission + slippage;
    let cooldown_days = risk.and_then(|cfg| cfg.stop_cooldown_days);
    let mut total_equity = 1.0;
    let mut holdings_value: HashMap<String, f64> = HashMap::new();
    let mut peak_equity = total_equity;
    let mut equity_curve = vec![(dates[lookback], total_equity)];
    let mut rebalance_rows = Vec::new();
    let mut holdings_trace = Vec::new();
    let mut contribution_rows = Vec::new();
    let mut risk_events = Vec::new();
    let mut contribution_sum: HashMap<String, f64> = HashMap::new();
    let mut total_cost_paid = 0.0;
    let mut trade_count = 0usize;
    let mut risk_state = RiskState::default();

    apply_asset_universe_guard(
        asset_maps.len(),
        lookback,
        &dates,
        risk,
        &mut risk_events,
        &mut risk_state,
    );

    for i in lookback..dates.len() - 1 {
        let date = dates[i];
        let next_date = dates[i + 1];
        let previous_equity = total_equity;
        let in_cooldown = handle_cooldown_transition(i, date, &mut risk_events, &mut risk_state);

        if !risk_state.permanently_stopped
            && !in_cooldown
            && (i - lookback).is_multiple_of(rebalance_freq)
        {
            let mut selected = select_assets(i, &dates, asset_maps);
            let mut seen = HashSet::new();
            selected.retain(|asset| asset_maps.contains_key(asset) && seen.insert(asset.clone()));

            let total_before = current_rebalance_base_equity(&holdings_value, total_equity);
            total_equity = total_before;

            let skip_rebalance = guard_selection_count(
                date,
                selected.len(),
                total_before,
                risk,
                &mut rebalance_rows,
                &mut risk_events,
            );

            if !skip_rebalance && selected.is_empty() {
                let effect = execute_to_cash_rebalance(
                    date,
                    total_before,
                    unit_cost,
                    &mut holdings_value,
                    &mut rebalance_rows,
                );
                total_equity = effect.equity_after;
                total_cost_paid += effect.cost_paid;
                trade_count += effect.trade_count_inc;
            } else if !skip_rebalance {
                let effect = execute_equal_weight_rebalance(
                    EqualWeightRebalanceSpec {
                        date,
                        selected: &selected,
                        total_before,
                        unit_cost,
                        max_turnover_limit: risk.and_then(|cfg| cfg.max_rebalance_turnover),
                    },
                    &mut holdings_value,
                    &mut rebalance_rows,
                    &mut risk_events,
                );
                total_equity = effect.equity_after;
                total_cost_paid += effect.cost_paid;
                trade_count += effect.trade_count_inc;
            }
        }

        apply_daily_returns_and_record_contribution(
            i,
            next_date,
            &aligned_closes,
            &mut holdings_value,
            &mut contribution_sum,
            &mut contribution_rows,
        );
        total_equity = portfolio_equity_or_total(&holdings_value, total_equity);
        let daily_return = if previous_equity > 0.0 {
            total_equity / previous_equity - 1.0
        } else {
            0.0
        };
        if !risk_state.permanently_stopped
            && !in_cooldown
            && !holdings_value.is_empty()
            && maybe_trigger_daily_loss_stop(
                next_date,
                daily_return,
                i,
                cooldown_days,
                risk,
                &mut risk_events,
                &mut risk_state,
            )
        {
            holdings_value.clear();
        }
        if total_equity > peak_equity {
            peak_equity = total_equity;
        }
        let current_drawdown = if peak_equity > 0.0 {
            total_equity / peak_equity - 1.0
        } else {
            0.0
        };
        if !risk_state.permanently_stopped
            && !in_cooldown
            && !holdings_value.is_empty()
            && maybe_trigger_drawdown_stop(
                next_date,
                current_drawdown,
                i,
                cooldown_days,
                risk,
                &mut risk_events,
                &mut risk_state,
            )
        {
            holdings_value.clear();
        }
        equity_curve.push((next_date, total_equity));
        append_holdings_trace_rows(
            next_date,
            total_equity,
            &holdings_value,
            &mut holdings_trace,
        );
    }

    build_rotation_result(RotationResultBuildInput {
        total_equity,
        trade_count,
        total_cost_paid,
        halted_by_risk: risk_state.halted_by_risk,
        halt_reason: risk_state.halt_reason,
        equity_curve,
        rebalances: rebalance_rows,
        holdings_trace,
        contributions: contribution_rows,
        risk_events,
        contribution_sum,
    })
}

pub fn effective_selected_count(
    requested_top_n: usize,
    available_assets: usize,
    risk: Option<&RiskConfig>,
) -> usize {
    if available_assets == 0 {
        return 0;
    }
    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = required_asset_count_for_max_weight(max_weight);
        requested_top_n.max(required_assets).min(available_assets)
    } else {
        requested_top_n.min(available_assets)
    }
}

pub fn required_asset_count_for_max_weight(max_weight: f64) -> usize {
    (1.0 / max_weight).ceil() as usize
}

#[cfg(test)]
mod tests;
