use crate::config::RiskConfig;
use crate::data::{intersect_dates, Bar};
use crate::engine::portfolio::compute_turnover_amount;
use crate::metrics::max_drawdown;
use crate::report::{ContributionRow, HoldingTraceRow, RebalanceRow, RiskEventRow};
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
        halted_by_risk: false,
        halt_reason: None,
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
    risk: Option<&RiskConfig>,
) -> MomentumTopNResult {
    let dates = intersect_dates(asset_maps);
    let unit_cost = commission + slippage;
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
    let mut halted_by_risk = false;
    let mut halt_reason = None;

    for i in lookback..dates.len() - 1 {
        let date = dates[i];
        let next_date = dates[i + 1];

        if !halted_by_risk && (i - lookback) % rebalance_freq == 0 {
            let ranking = rank_assets_by_lookback(asset_maps, &dates, i, lookback);
            let selected_count = effective_selected_count(top_n, ranking.len(), risk);
            let selected: Vec<String> = ranking
                .into_iter()
                .take(selected_count)
                .map(|item| item.0)
                .collect();

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
            let turnover_ratio = if total_before > 0.0 {
                turnover_amount / total_before
            } else {
                0.0
            };
            if let Some(limit) = risk.and_then(|cfg| cfg.max_rebalance_turnover) {
                if turnover_ratio > limit {
                    risk_events.push(RiskEventRow {
                        date: date.to_string(),
                        event_type: "turnover_guard".to_string(),
                        detail: format!(
                            "本次调仓换手率 {:.2}% 超过上限 {:.2}%，已跳过调仓",
                            turnover_ratio * 100.0,
                            limit * 100.0
                        ),
                    });
                    rebalance_rows.push(RebalanceRow {
                        date: date.to_string(),
                        selected_assets: "SKIPPED_BY_RISK".to_string(),
                        turnover_amount,
                        cost: 0.0,
                        equity_before: total_before,
                        equity_after: total_before,
                    });
                    continue;
                }
            }
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

        if !holdings_value.is_empty() {
            total_equity = holdings_value.values().sum::<f64>();
        }
        if total_equity > peak_equity {
            peak_equity = total_equity;
        }
        let current_drawdown = if peak_equity > 0.0 {
            total_equity / peak_equity - 1.0
        } else {
            0.0
        };
        if !halted_by_risk {
            if let Some(limit) = risk.and_then(|cfg| cfg.max_drawdown_limit) {
                if current_drawdown <= -limit {
                    halted_by_risk = true;
                    let reason = format!(
                        "当前回撤 {:.2}% 触发最大回撤上限 {:.2}%，组合已切换为空仓",
                        current_drawdown * 100.0,
                        limit * 100.0
                    );
                    halt_reason = Some(reason.clone());
                    risk_events.push(RiskEventRow {
                        date: next_date.to_string(),
                        event_type: "drawdown_stop".to_string(),
                        detail: reason,
                    });
                    holdings_value.clear();
                }
            }
        }
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
        halted_by_risk,
        halt_reason,
    };

    MomentumTopNResult {
        summary,
        equity_curve,
        rebalances: rebalance_rows,
        holdings_trace,
        contributions: contribution_rows,
        risk_events,
        top_contributor,
        worst_contributor,
    }
}

fn effective_selected_count(
    requested_top_n: usize,
    available_assets: usize,
    risk: Option<&RiskConfig>,
) -> usize {
    if available_assets == 0 {
        return 0;
    }
    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = (1.0 / max_weight).ceil() as usize;
        requested_top_n.max(required_assets).min(available_assets)
    } else {
        requested_top_n.min(available_assets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn bar(date: &str, close: f64) -> Bar {
        Bar {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            open: close,
            close,
        }
    }

    fn sample_asset_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
        let mut maps = HashMap::new();
        let asset_a = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 110.0),
            bar("2024-01-03", 90.0),
            bar("2024-01-04", 80.0),
            bar("2024-01-05", 70.0),
        ];
        let asset_b = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 101.0),
            bar("2024-01-03", 102.0),
            bar("2024-01-04", 103.0),
            bar("2024-01-05", 104.0),
        ];
        let asset_c = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 100.5),
            bar("2024-01-03", 101.0),
            bar("2024-01-04", 101.5),
            bar("2024-01-05", 102.0),
        ];
        maps.insert(
            "a".to_string(),
            asset_a.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "b".to_string(),
            asset_b.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "c".to_string(),
            asset_c.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps
    }

    #[test]
    fn drawdown_guard_halts_strategy() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_drawdown_limit: Some(0.05),
            max_rebalance_turnover: None,
        };

        let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert!(result.summary.halted_by_risk);
        assert!(result
            .risk_events
            .iter()
            .any(|event| event.event_type == "drawdown_stop"));
    }

    #[test]
    fn turnover_guard_skips_rebalance() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: Some(0.0),
        };

        let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert!(result
            .risk_events
            .iter()
            .any(|event| event.event_type == "turnover_guard"));
        assert!(result
            .rebalances
            .iter()
            .any(|row| row.selected_assets == "SKIPPED_BY_RISK"));
    }

    #[test]
    fn max_single_asset_weight_expands_selection_count() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: Some(0.4),
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
        };

        let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert!(result
            .rebalances
            .iter()
            .any(|row| row.selected_assets.split('|').count() >= 3));
    }
}
