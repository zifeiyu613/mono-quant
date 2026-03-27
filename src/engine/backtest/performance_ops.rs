use crate::report::{ContributionRow, HoldingTraceRow};
use chrono::NaiveDate;
use std::collections::HashMap;

pub(super) fn apply_daily_returns_and_record_contribution(
    index: usize,
    next_date: NaiveDate,
    aligned_closes: &HashMap<String, Vec<f64>>,
    holdings_value: &mut HashMap<String, f64>,
    contribution_sum: &mut HashMap<String, f64>,
    contribution_rows: &mut Vec<ContributionRow>,
) {
    let equity_before_move = holdings_value.values().sum::<f64>();
    for (asset, value) in holdings_value.iter_mut() {
        let closes = aligned_closes.get(asset).unwrap();
        let today_close = closes[index];
        let next_close = closes[index + 1];
        let ret = next_close / today_close - 1.0;
        let current_value = *value;
        let weight = if equity_before_move > 0.0 {
            current_value / equity_before_move
        } else {
            0.0
        };
        let daily_contribution = weight * ret;
        let asset_name = asset.clone();
        let cum = contribution_sum.entry(asset_name.clone()).or_insert(0.0);
        *cum += daily_contribution;
        contribution_rows.push(ContributionRow {
            date: next_date.to_string(),
            asset: asset_name,
            daily_contribution,
            cumulative_contribution: *cum,
        });
        *value *= 1.0 + ret;
    }
}

pub(super) fn portfolio_equity_or_total(
    holdings_value: &HashMap<String, f64>,
    total_equity: f64,
) -> f64 {
    if holdings_value.is_empty() {
        total_equity
    } else {
        holdings_value.values().sum::<f64>()
    }
}

pub(super) fn append_holdings_trace_rows(
    next_date: NaiveDate,
    total_equity: f64,
    holdings_value: &HashMap<String, f64>,
    holdings_trace: &mut Vec<HoldingTraceRow>,
) {
    for (asset, value) in holdings_value {
        let weight = if total_equity > 0.0 {
            *value / total_equity
        } else {
            0.0
        };
        holdings_trace.push(HoldingTraceRow {
            date: next_date.to_string(),
            asset: asset.clone(),
            value: *value,
            weight,
            total_equity,
        });
    }
}
