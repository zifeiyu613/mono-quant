use crate::data::{intersect_dates, Bar};
use crate::engine::portfolio::compute_turnover_amount;
use crate::metrics::max_drawdown;
use crate::report::{ContributionRow, HoldingTraceRow, RebalanceRow};
use crate::strategy::momentum_topn::rank_assets_by_lookback;
use chrono::NaiveDate;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct BacktestSummary {
    pub total_return: f64,
    pub max_drawdown: f64,
    pub trade_count: usize,
    pub total_cost_paid: f64,
    pub final_equity: f64,
}

#[derive(Debug, Clone)]
pub struct MomentumTopNResult {
    pub summary: BacktestSummary,
    pub equity_curve: Vec<(NaiveDate, f64)>,
    pub rebalances: Vec<RebalanceRow>,
    pub holdings_trace: Vec<HoldingTraceRow>,
    pub contributions: Vec<ContributionRow>,
    pub top_contributor: Option<(String, f64)>,
    pub worst_contributor: Option<(String, f64)>,
}

/// 运行单资产均线交叉回测，使用收盘到收盘收益和简化交易成本模型。
pub fn run_ma_backtest(
    bars: &[Bar],
    signals: &[i8],
    commission: f64,
    slippage: f64,
    stamp_tax_sell: f64,
) -> (BacktestSummary, Vec<f64>) {
    let mut equity = 1.0;
    let mut curve = vec![equity];
    let mut position = 0.0;
    let mut pending_signal = 0_i8;
    let mut trade_count = 0usize;
    let mut total_cost_paid = 0.0;

    for i in 1..bars.len() {
        if pending_signal == 1 && position == 0.0 {
            let cost = commission + slippage;
            equity *= 1.0 - cost;
            total_cost_paid += cost;
            position = 1.0;
            trade_count += 1;
        } else if pending_signal == -1 && position == 1.0 {
            let cost = commission + slippage + stamp_tax_sell;
            equity *= 1.0 - cost;
            total_cost_paid += cost;
            position = 0.0;
            trade_count += 1;
        }
        pending_signal = 0;

        let daily_ret = bars[i].close / bars[i - 1].close - 1.0;
        equity *= 1.0 + position * daily_ret;
        curve.push(equity);

        if signals[i] != 0 {
            pending_signal = signals[i];
        }
    }

    let summary = BacktestSummary {
        total_return: equity - 1.0,
        max_drawdown: max_drawdown(&curve),
        trade_count,
        total_cost_paid,
        final_equity: equity,
    };
    (summary, curve)
}

/// 运行多资产 Top N 动量轮动回测，并返回净值、持仓、归因和调仓诊断结果。
pub fn run_momentum_topn_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    lookback: usize,
    rebalance_freq: usize,
    top_n: usize,
    commission: f64,
    slippage: f64,
) -> MomentumTopNResult {
    let dates = intersect_dates(asset_maps);
    let unit_cost = commission + slippage;
    let mut total_equity = 1.0;
    let mut holdings_value: HashMap<String, f64> = HashMap::new();
    let mut equity_curve = vec![(dates[lookback], total_equity)];
    let mut rebalance_rows = Vec::new();
    let mut holdings_trace = Vec::new();
    let mut contribution_rows = Vec::new();
    let mut contribution_sum: HashMap<String, f64> = HashMap::new();
    let mut total_cost_paid = 0.0;
    let mut trade_count = 0usize;

    for i in lookback..dates.len() - 1 {
        let date = dates[i];
        let next_date = dates[i + 1];

        if (i - lookback) % rebalance_freq == 0 {
            let ranking = rank_assets_by_lookback(asset_maps, &dates, i, lookback);
            let selected: Vec<String> = ranking.into_iter().take(top_n).map(|x| x.0).collect();

            let total_before = if holdings_value.is_empty() {
                total_equity
            } else {
                holdings_value.values().sum::<f64>()
            };
            total_equity = total_before;

            let target_weight = 1.0 / selected.len() as f64;
            let mut target_values = HashMap::new();
            for asset in &selected {
                target_values.insert(asset.clone(), total_equity * target_weight);
            }

            let turnover_amount = compute_turnover_amount(&holdings_value, &target_values);
            let cost = turnover_amount * unit_cost;
            total_equity -= cost;
            total_cost_paid += cost;

            holdings_value.clear();
            for asset in &selected {
                holdings_value.insert(asset.clone(), total_equity * target_weight);
            }

            trade_count += 1;
            rebalance_rows.push(RebalanceRow {
                date: date.to_string(),
                selected_assets: selected.join("|"),
                turnover_amount,
                cost,
                equity_before: total_before,
                equity_after: total_equity,
            });
        }

        let equity_before_move = if holdings_value.is_empty() {
            total_equity
        } else {
            holdings_value.values().sum::<f64>()
        };

        let keys: Vec<String> = holdings_value.keys().cloned().collect();
        for asset in keys {
            if let Some(v) = holdings_value.get_mut(&asset) {
                let bars = asset_maps.get(&asset).unwrap();
                let today_close = bars.get(&date).unwrap().close;
                let next_close = bars.get(&next_date).unwrap().close;
                let ret = next_close / today_close - 1.0;
                let current_value = *v;
                let weight = if equity_before_move > 0.0 {
                    current_value / equity_before_move
                } else {
                    0.0
                };
                let daily_contribution = weight * ret;
                let cum = contribution_sum.entry(asset.clone()).or_insert(0.0);
                *cum += daily_contribution;
                contribution_rows.push(ContributionRow {
                    date: next_date.to_string(),
                    asset: asset.clone(),
                    daily_contribution,
                    cumulative_contribution: *cum,
                });
                *v *= 1.0 + ret;
            }
        }

        total_equity = holdings_value.values().sum::<f64>();
        equity_curve.push((next_date, total_equity));

        for (asset, value) in &holdings_value {
            let weight = if total_equity > 0.0 { *value / total_equity } else { 0.0 };
            holdings_trace.push(HoldingTraceRow {
                date: next_date.to_string(),
                asset: asset.clone(),
                value: *value,
                weight,
                total_equity,
            });
        }
    }

    let only_curve: Vec<f64> = equity_curve.iter().map(|(_, e)| *e).collect();
    let mut contrib_vec: Vec<(String, f64)> = contribution_sum.into_iter().collect();
    contrib_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    let top_contributor = contrib_vec.first().cloned();
    let worst_contributor = contrib_vec.last().cloned();

    let summary = BacktestSummary {
        total_return: total_equity - 1.0,
        max_drawdown: max_drawdown(&only_curve),
        trade_count,
        total_cost_paid,
        final_equity: total_equity,
    };

    MomentumTopNResult {
        summary,
        equity_curve,
        rebalances: rebalance_rows,
        holdings_trace,
        contributions: contribution_rows,
        top_contributor,
        worst_contributor,
    }
}
