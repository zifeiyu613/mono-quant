use crate::engine::portfolio::compute_turnover_amount;
use crate::report::{RebalanceRow, RiskEventRow};
use chrono::NaiveDate;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RebalanceEffect {
    pub(super) equity_after: f64,
    pub(super) cost_paid: f64,
    pub(super) trade_count_inc: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct EqualWeightRebalanceSpec<'a> {
    pub(super) date: NaiveDate,
    pub(super) selected: &'a [String],
    pub(super) total_before: f64,
    pub(super) unit_cost: f64,
    pub(super) max_turnover_limit: Option<f64>,
}

pub(super) fn current_rebalance_base_equity(
    holdings_value: &HashMap<String, f64>,
    total_equity: f64,
) -> f64 {
    if holdings_value.is_empty() {
        total_equity
    } else {
        holdings_value.values().sum::<f64>()
    }
}

pub(super) fn execute_to_cash_rebalance(
    date: NaiveDate,
    total_before: f64,
    unit_cost: f64,
    holdings_value: &mut HashMap<String, f64>,
    rebalance_rows: &mut Vec<RebalanceRow>,
) -> RebalanceEffect {
    let target_values: HashMap<String, f64> = HashMap::new();
    let turnover_amount = compute_turnover_amount(holdings_value, &target_values);
    let cost = turnover_amount * unit_cost;
    let equity_after = total_before - cost;

    holdings_value.clear();
    rebalance_rows.push(RebalanceRow {
        date: date.to_string(),
        selected_assets: "TO_CASH".to_string(),
        turnover_amount,
        cost,
        equity_before: total_before,
        equity_after,
    });

    RebalanceEffect {
        equity_after,
        cost_paid: cost,
        trade_count_inc: usize::from(turnover_amount > 0.0),
    }
}

pub(super) fn execute_equal_weight_rebalance(
    spec: EqualWeightRebalanceSpec<'_>,
    holdings_value: &mut HashMap<String, f64>,
    rebalance_rows: &mut Vec<RebalanceRow>,
    risk_events: &mut Vec<RiskEventRow>,
) -> RebalanceEffect {
    let target_weight = 1.0 / spec.selected.len() as f64;
    let mut target_values = HashMap::new();
    for asset in spec.selected {
        target_values.insert(asset.clone(), spec.total_before * target_weight);
    }

    let turnover_amount = compute_turnover_amount(holdings_value, &target_values);
    let turnover_ratio = if spec.total_before > 0.0 {
        turnover_amount / spec.total_before
    } else {
        0.0
    };

    if let Some(limit) = spec.max_turnover_limit {
        if turnover_ratio > limit {
            risk_events.push(RiskEventRow {
                date: spec.date.to_string(),
                event_type: "turnover_guard".to_string(),
                detail: format!(
                    "本次调仓换手率 {:.2}% 超过上限 {:.2}%，已跳过调仓",
                    turnover_ratio * 100.0,
                    limit * 100.0
                ),
            });
            rebalance_rows.push(RebalanceRow {
                date: spec.date.to_string(),
                selected_assets: "SKIPPED_BY_RISK".to_string(),
                turnover_amount,
                cost: 0.0,
                equity_before: spec.total_before,
                equity_after: spec.total_before,
            });
            return RebalanceEffect {
                equity_after: spec.total_before,
                cost_paid: 0.0,
                trade_count_inc: 0,
            };
        }
    }

    let cost = turnover_amount * spec.unit_cost;
    let equity_after = spec.total_before - cost;

    holdings_value.clear();
    for asset in spec.selected {
        holdings_value.insert(asset.clone(), equity_after * target_weight);
    }

    rebalance_rows.push(RebalanceRow {
        date: spec.date.to_string(),
        selected_assets: spec.selected.join("|"),
        turnover_amount,
        cost,
        equity_before: spec.total_before,
        equity_after,
    });

    RebalanceEffect {
        equity_after,
        cost_paid: cost,
        trade_count_inc: usize::from(turnover_amount > 0.0),
    }
}
