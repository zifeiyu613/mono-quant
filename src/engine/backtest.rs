use crate::config::RiskConfig;
use crate::data::{intersect_dates, Bar};
use crate::engine::portfolio::compute_turnover_amount;
use crate::metrics::max_drawdown;
use crate::report::{ContributionRow, HoldingTraceRow, RebalanceRow, RiskEventRow};
use crate::strategy::absolute_momentum_breadth::select_absolute_momentum_breadth;
use crate::strategy::absolute_momentum_single::select_absolute_momentum_single;
use crate::strategy::breakout_rotation_topn::select_breakout_rotation_topn;
use crate::strategy::breakout_timing_single::select_breakout_timing_single;
use crate::strategy::dual_momentum::select_dual_momentum_assets;
use crate::strategy::low_volatility_topn::rank_assets_by_low_volatility;
use crate::strategy::ma_timing_single::select_ma_timing_single;
use crate::strategy::ma_rotation_topn::rank_assets_by_ma_rotation;
use crate::strategy::momentum_topn::rank_assets_by_lookback;
use crate::strategy::relative_strength_pair::select_relative_strength_pair;
use crate::strategy::risk_off_rotation::select_risk_off_rotation_asset;
use crate::strategy::reversal_bottomn::rank_assets_by_reversal;
use crate::strategy::volatility_adjusted_momentum::rank_assets_by_volatility_adjusted_momentum;
use chrono::NaiveDate;
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
    run_rotation_backtest(
        asset_maps,
        lookback,
        rebalance_freq,
        commission,
        slippage,
        risk,
        |i, dates, maps| {
            let ranking = rank_assets_by_lookback(maps, dates, i, lookback);
            let selected_count = effective_selected_count(top_n, ranking.len(), risk);
            ranking
                .into_iter()
                .take(selected_count)
                .map(|item| item.0)
                .collect()
        },
    )
}

/// 运行波动调整动量轮动回测：按“收益 / 波动”排序，持有前 N 名。
pub fn run_volatility_adjusted_momentum_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    lookback: usize,
    rebalance_freq: usize,
    top_n: usize,
    commission: f64,
    slippage: f64,
    risk: Option<&RiskConfig>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        lookback,
        rebalance_freq,
        commission,
        slippage,
        risk,
        |i, dates, maps| {
            let ranking = rank_assets_by_volatility_adjusted_momentum(maps, dates, i, lookback);
            let selected_count = effective_selected_count(top_n, ranking.len(), risk);
            ranking
                .into_iter()
                .take(selected_count)
                .map(|item| item.0)
                .collect()
        },
    )
}

/// 运行反转 Bottom N 回测：选择回看期最弱的 N 个资产，做均值回归对照。
pub fn run_reversal_bottomn_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    lookback: usize,
    rebalance_freq: usize,
    top_n: usize,
    commission: f64,
    slippage: f64,
    risk: Option<&RiskConfig>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        lookback,
        rebalance_freq,
        commission,
        slippage,
        risk,
        |i, dates, maps| {
            let ranking = rank_assets_by_reversal(maps, dates, i, lookback);
            let selected_count = effective_selected_count(top_n, ranking.len(), risk);
            ranking
                .into_iter()
                .take(selected_count)
                .map(|item| item.0)
                .collect()
        },
    )
}

/// 运行单资产买入并持有回测（默认只在首个可交易日建仓）。
pub fn run_buy_hold_single_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    benchmark_asset: &str,
    commission: f64,
    slippage: f64,
    risk: Option<&RiskConfig>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        0,
        usize::MAX,
        commission,
        slippage,
        risk,
        |_i, _dates, maps| {
            if maps.contains_key(benchmark_asset) {
                vec![benchmark_asset.to_string()]
            } else {
                Vec::new()
            }
        },
    )
}

/// 运行多资产等权买入并持有回测（默认只在首个可交易日建仓）。
pub fn run_buy_hold_equal_weight_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    commission: f64,
    slippage: f64,
    risk: Option<&RiskConfig>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        0,
        usize::MAX,
        commission,
        slippage,
        risk,
        |_i, _dates, maps| {
            let mut names: Vec<String> = maps.keys().cloned().collect();
            names.sort();
            names
        },
    )
}

/// 运行双动量回测：相对动量排名 + 绝对动量过滤，不满足时可回退到防守资产。
pub fn run_dual_momentum_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    top_n: usize,
    absolute_momentum_floor: f64,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_dual_momentum_assets(
                maps,
                dates,
                i,
                cfg.lookback,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
            )
        },
    )
}

/// 运行单资产绝对动量开关回测：基准资产达标则持有，否则进入防守资产或空仓。
pub fn run_absolute_momentum_single_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    benchmark_asset: &str,
    absolute_momentum_floor: f64,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_absolute_momentum_single(
                maps,
                dates,
                i,
                cfg.lookback,
                benchmark_asset,
                absolute_momentum_floor,
                defensive_asset,
            )
        },
    )
}

/// 运行多资产绝对动量广度回测：持有所有满足绝对动量门槛的资产，否则回退到防守资产或空仓。
pub fn run_absolute_momentum_breadth_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    absolute_momentum_floor: f64,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_absolute_momentum_breadth(
                maps,
                dates,
                i,
                cfg.lookback,
                absolute_momentum_floor,
                defensive_asset,
            )
        },
    )
}

/// 运行低波动 Top N 回测：按回看窗口波动率从低到高选择最平稳的前 N 个资产。
pub fn run_low_volatility_topn_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    top_n: usize,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            let ranking = rank_assets_by_low_volatility(
                maps,
                dates,
                i,
                cfg.lookback,
                defensive_asset,
            );
            let selected_count = effective_selected_count(top_n, ranking.len(), cfg.risk);
            let selected: Vec<String> = ranking
                .into_iter()
                .take(selected_count)
                .map(|item| item.0)
                .collect();
            if selected.is_empty() {
                if let Some(defensive) = defensive_asset {
                    return vec![defensive.to_string()];
                }
            }
            selected
        },
    )
}

/// 运行单资产均线择时回测：快线高于慢线则持有基准资产，否则进入防守资产或空仓。
pub fn run_ma_timing_single_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    benchmark_asset: &str,
    fast: usize,
    slow: usize,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        slow.saturating_sub(1),
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_ma_timing_single(maps, dates, i, fast, slow, benchmark_asset, defensive_asset)
        },
    )
}

/// 运行均线过滤 Top N 回测：先保留快线高于慢线的资产，再按回看收益排序选前 N。
pub fn run_ma_rotation_topn_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    fast: usize,
    slow: usize,
    top_n: usize,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    let required_lookback = slow.saturating_sub(1).max(cfg.lookback);
    run_rotation_backtest(
        asset_maps,
        required_lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            let ranking = rank_assets_by_ma_rotation(
                maps,
                dates,
                i,
                fast,
                slow,
                cfg.lookback,
                defensive_asset,
            );
            let selected_count = effective_selected_count(top_n, ranking.len(), cfg.risk);
            let selected: Vec<String> = ranking
                .into_iter()
                .take(selected_count)
                .map(|item| item.0)
                .collect();
            if selected.is_empty() {
                if let Some(defensive) = defensive_asset {
                    return vec![defensive.to_string()];
                }
            }
            selected
        },
    )
}

/// 运行单资产突破择时回测：突破历史高点则持有基准资产，否则进入防守资产或空仓。
pub fn run_breakout_timing_single_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    benchmark_asset: &str,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_breakout_timing_single(
                maps,
                dates,
                i,
                cfg.lookback,
                benchmark_asset,
                defensive_asset,
            )
        },
    )
}

/// 运行多资产突破轮动回测：从触发突破的资产中按强度择优持有。
pub fn run_breakout_rotation_topn_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    top_n: usize,
    defensive_asset: Option<&str>,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_breakout_rotation_topn(
                maps,
                dates,
                i,
                cfg.lookback,
                top_n,
                defensive_asset,
            )
        },
    )
}

/// 运行双资产相对强弱切换回测：在两个候选之间择强持有。
pub fn run_relative_strength_pair_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    primary_asset: &str,
    alternate_asset: &str,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_relative_strength_pair(
                maps,
                dates,
                i,
                cfg.lookback,
                primary_asset,
                alternate_asset,
            )
        },
    )
}

/// 运行风险开关轮动回测：风险资产最强者通过门槛则持有，否则回退到防守资产。
pub fn run_risk_off_rotation_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    risk_assets: &[String],
    absolute_momentum_floor: f64,
    defensive_asset: &str,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_risk_off_rotation_asset(
                maps,
                dates,
                i,
                cfg.lookback,
                risk_assets,
                absolute_momentum_floor,
                defensive_asset,
            )
        },
    )
}

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
    F: FnMut(
        usize,
        &[NaiveDate],
        &HashMap<String, HashMap<NaiveDate, Bar>>,
    ) -> Vec<String>,
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
    let mut halted_by_risk = false;
    let mut permanently_stopped = false;
    let mut halt_reason = None;
    let mut cooldown_until_index: Option<usize> = None;

    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = required_asset_count_for_max_weight(max_weight);
        if asset_maps.len() < required_assets {
            let reason = format!(
                "资产池数量 {} 低于单资产权重上限 {:.2}% 所需的最少资产数 {}，组合已停止运行",
                asset_maps.len(),
                max_weight * 100.0,
                required_assets
            );
            risk_events.push(RiskEventRow {
                date: dates
                    .get(lookback)
                    .or_else(|| dates.first())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "N/A".to_string()),
                event_type: "asset_universe_stop".to_string(),
                detail: reason.clone(),
            });
            halt_reason = Some(reason);
            halted_by_risk = true;
        }
    }

    for i in lookback..dates.len() - 1 {
        let date = dates[i];
        let next_date = dates[i + 1];
        let previous_equity = total_equity;
        let mut in_cooldown = false;

        if let Some(until_index) = cooldown_until_index {
            if i < until_index {
                in_cooldown = true;
            } else {
                risk_events.push(RiskEventRow {
                    date: date.to_string(),
                    event_type: "cooldown_recovery".to_string(),
                    detail: "风控冷静期结束，组合恢复为可调仓状态，后续将在下一次调仓点重新入场".to_string(),
                });
                cooldown_until_index = None;
                halted_by_risk = false;
                halt_reason = None;
            }
        }

        if !permanently_stopped
            && !in_cooldown
            && (i - lookback).is_multiple_of(rebalance_freq)
        {
            let mut selected = select_assets(i, &dates, asset_maps);
            let mut seen = HashSet::new();
            selected.retain(|asset| asset_maps.contains_key(asset) && seen.insert(asset.clone()));

            let total_before = if holdings_value.is_empty() {
                total_equity
            } else {
                holdings_value.values().sum::<f64>()
            };
            total_equity = total_before;

            let mut skip_rebalance = false;
            if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
                let required_assets = required_asset_count_for_max_weight(max_weight);
                if !selected.is_empty() && selected.len() < required_assets {
                    risk_events.push(RiskEventRow {
                        date: date.to_string(),
                        event_type: "selection_guard".to_string(),
                        detail: format!(
                            "本次选中资产数量 {} 小于单资产权重上限 {:.2}% 要求的最少资产数 {}，已跳过调仓",
                            selected.len(),
                            max_weight * 100.0,
                            required_assets
                        ),
                    });
                    rebalance_rows.push(RebalanceRow {
                        date: date.to_string(),
                        selected_assets: "SKIPPED_BY_RISK".to_string(),
                        turnover_amount: 0.0,
                        cost: 0.0,
                        equity_before: total_before,
                        equity_after: total_before,
                    });
                    skip_rebalance = true;
                }
            }

            if !skip_rebalance && selected.is_empty() {
                let target_values: HashMap<String, f64> = HashMap::new();
                let turnover_amount = compute_turnover_amount(&holdings_value, &target_values);
                let cost = turnover_amount * unit_cost;
                total_equity -= cost;
                total_cost_paid += cost;
                holdings_value.clear();
                if turnover_amount > 0.0 {
                    trade_count += 1;
                }
                rebalance_rows.push(RebalanceRow {
                    date: date.to_string(),
                    selected_assets: "TO_CASH".to_string(),
                    turnover_amount,
                    cost,
                    equity_before: total_before,
                    equity_after: total_equity,
                });
            } else if !skip_rebalance {
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
                    } else {
                        let cost = turnover_amount * unit_cost;
                        total_equity -= cost;
                        total_cost_paid += cost;

                        holdings_value.clear();
                        for asset in &selected {
                            holdings_value.insert(asset.clone(), total_equity * target_weight);
                        }

                        if turnover_amount > 0.0 {
                            trade_count += 1;
                        }
                        rebalance_rows.push(RebalanceRow {
                            date: date.to_string(),
                            selected_assets: selected.join("|"),
                            turnover_amount,
                            cost,
                            equity_before: total_before,
                            equity_after: total_equity,
                        });
                    }
                } else {
                    let cost = turnover_amount * unit_cost;
                    total_equity -= cost;
                    total_cost_paid += cost;

                    holdings_value.clear();
                    for asset in &selected {
                        holdings_value.insert(asset.clone(), total_equity * target_weight);
                    }

                    if turnover_amount > 0.0 {
                        trade_count += 1;
                    }
                    rebalance_rows.push(RebalanceRow {
                        date: date.to_string(),
                        selected_assets: selected.join("|"),
                        turnover_amount,
                        cost,
                        equity_before: total_before,
                        equity_after: total_equity,
                    });
                }
            }
        }

        let equity_before_move = if holdings_value.is_empty() {
            total_equity
        } else {
            holdings_value.values().sum::<f64>()
        };

        for (asset, value) in holdings_value.iter_mut() {
            let closes = aligned_closes.get(asset).unwrap();
            let today_close = closes[i];
            let next_close = closes[i + 1];
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

        if !holdings_value.is_empty() {
            total_equity = holdings_value.values().sum::<f64>();
        }
        let daily_return = if previous_equity > 0.0 {
            total_equity / previous_equity - 1.0
        } else {
            0.0
        };
        if !permanently_stopped && !in_cooldown && !holdings_value.is_empty() {
            if let Some(limit) = risk.and_then(|cfg| cfg.max_daily_loss_limit) {
                if daily_return <= -limit {
                    halted_by_risk = true;
                    let mut reason = format!(
                        "单日组合收益 {:.2}% 触发单日亏损上限 {:.2}%",
                        daily_return * 100.0,
                        limit * 100.0
                    );
                    if let Some(days) = cooldown_days {
                        reason.push_str(&format!("，组合已切换为空仓并进入 {} 个交易日冷静期", days));
                        cooldown_until_index = Some(i + days + 1);
                    } else {
                        reason.push_str("，组合已切换为空仓");
                        permanently_stopped = true;
                    }
                    halt_reason = Some(reason.clone());
                    risk_events.push(RiskEventRow {
                        date: next_date.to_string(),
                        event_type: "daily_loss_stop".to_string(),
                        detail: reason,
                    });
                    holdings_value.clear();
                }
            }
        }
        if total_equity > peak_equity {
            peak_equity = total_equity;
        }
        let current_drawdown = if peak_equity > 0.0 {
            total_equity / peak_equity - 1.0
        } else {
            0.0
        };
        if !permanently_stopped && !in_cooldown && !holdings_value.is_empty() {
            if let Some(limit) = risk.and_then(|cfg| cfg.max_drawdown_limit) {
                if current_drawdown <= -limit {
                    halted_by_risk = true;
                    let mut reason = format!(
                        "当前回撤 {:.2}% 触发最大回撤上限 {:.2}%",
                        current_drawdown * 100.0,
                        limit * 100.0
                    );
                    if let Some(days) = cooldown_days {
                        reason.push_str(&format!("，组合已切换为空仓并进入 {} 个交易日冷静期", days));
                        cooldown_until_index = Some(i + days + 1);
                    } else {
                        reason.push_str("，组合已切换为空仓");
                        permanently_stopped = true;
                    }
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

    fn sample_two_asset_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
        let mut maps = HashMap::new();
        let asset_a = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 102.0),
            bar("2024-01-03", 104.0),
            bar("2024-01-04", 106.0),
            bar("2024-01-05", 108.0),
        ];
        let asset_b = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 101.0),
            bar("2024-01-03", 102.0),
            bar("2024-01-04", 103.0),
            bar("2024-01-05", 104.0),
        ];
        maps.insert(
            "a".to_string(),
            asset_a.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "b".to_string(),
            asset_b.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps
    }

    fn sample_cooldown_asset_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
        let mut maps = HashMap::new();
        let asset_a = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 110.0),
            bar("2024-01-03", 90.0),
            bar("2024-01-04", 92.0),
            bar("2024-01-05", 94.0),
            bar("2024-01-08", 120.0),
            bar("2024-01-09", 121.0),
            bar("2024-01-10", 122.0),
        ];
        let asset_b = vec![
            bar("2024-01-01", 100.0),
            bar("2024-01-02", 101.0),
            bar("2024-01-03", 102.0),
            bar("2024-01-04", 103.0),
            bar("2024-01-05", 104.0),
            bar("2024-01-08", 105.0),
            bar("2024-01-09", 106.0),
            bar("2024-01-10", 107.0),
        ];
        maps.insert(
            "a".to_string(),
            asset_a.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "b".to_string(),
            asset_b.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps
    }

    #[test]
    fn drawdown_guard_halts_strategy() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: None,
            max_drawdown_limit: Some(0.05),
            max_rebalance_turnover: None,
            stop_cooldown_days: None,
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
            max_daily_loss_limit: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: Some(0.0),
            stop_cooldown_days: None,
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
            max_daily_loss_limit: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
            stop_cooldown_days: None,
        };

        let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert!(result
            .rebalances
            .iter()
            .any(|row| row.selected_assets.split('|').count() >= 3));
    }

    #[test]
    fn daily_loss_guard_halts_strategy() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: Some(0.1),
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
            stop_cooldown_days: None,
        };

        let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert!(result.summary.halted_by_risk);
        assert!(result
            .risk_events
            .iter()
            .any(|event| event.event_type == "daily_loss_stop"));
    }

    #[test]
    fn asset_universe_guard_halts_when_weight_cap_cannot_be_satisfied() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: Some(0.4),
            max_daily_loss_limit: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
            stop_cooldown_days: None,
        };

        let result =
            run_momentum_topn_backtest(&sample_two_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert!(result.summary.halted_by_risk);
        assert!(result
            .risk_events
            .iter()
            .any(|event| event.event_type == "asset_universe_stop"));
    }

    #[test]
    fn cooldown_recovery_allows_reentry_after_stop() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: Some(0.1),
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
            stop_cooldown_days: Some(2),
        };

        let result = run_momentum_topn_backtest(
            &sample_cooldown_asset_maps(),
            1,
            1,
            1,
            0.0,
            0.0,
            Some(&risk),
        );

        assert!(!result.summary.halted_by_risk);
        assert!(result
            .risk_events
            .iter()
            .any(|event| event.event_type == "daily_loss_stop"));
        assert!(result
            .risk_events
            .iter()
            .any(|event| event.event_type == "cooldown_recovery"));
        assert!(result.rebalances.len() >= 2);
    }

    #[test]
    fn buy_hold_equal_weight_builds_initial_equal_portfolio() {
        let result = run_buy_hold_equal_weight_backtest(&sample_asset_maps(), 0.0, 0.0, None);
        assert!(!result.rebalances.is_empty());
        let first = &result.rebalances[0];
        assert!(first.selected_assets.contains("a"));
        assert!(first.selected_assets.contains("b"));
        assert!(first.selected_assets.contains("c"));
    }

    #[test]
    fn dual_momentum_falls_back_to_defensive_asset() {
        let result = run_dual_momentum_backtest(
            &sample_asset_maps(),
            RotationBacktestConfig {
                lookback: 1,
                rebalance_freq: 1,
                commission: 0.0,
                slippage: 0.0,
                risk: None,
            },
            1,
            0.2,
            Some("b"),
        );

        assert!(!result.rebalances.is_empty());
        assert!(result
            .rebalances
            .iter()
            .any(|row| row.selected_assets == "b"));
    }

    #[test]
    fn skipped_rebalance_still_records_each_equity_point() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: Some(0.0),
            stop_cooldown_days: None,
        };

        let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

        assert_eq!(result.equity_curve.len(), 4);
        assert_eq!(
            result.equity_curve.last().map(|(date, _)| *date),
            Some(NaiveDate::parse_from_str("2024-01-05", "%Y-%m-%d").unwrap())
        );
        assert!(result
            .equity_curve
            .iter()
            .all(|(_, equity)| (*equity - 1.0).abs() < 1e-12));
    }

    #[test]
    fn zero_turnover_rebalance_does_not_increase_trade_count() {
        let result = run_momentum_topn_backtest(&sample_two_asset_maps(), 1, 1, 1, 0.0, 0.0, None);

        assert_eq!(result.summary.trade_count, 1);
    }

    #[test]
    fn cooldown_recovery_clears_terminal_halt_reason() {
        let risk = RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: Some(0.1),
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
            stop_cooldown_days: Some(2),
        };

        let result = run_momentum_topn_backtest(
            &sample_cooldown_asset_maps(),
            1,
            1,
            1,
            0.0,
            0.0,
            Some(&risk),
        );

        assert!(!result.summary.halted_by_risk);
        assert!(result.summary.halt_reason.is_none());
    }
}
