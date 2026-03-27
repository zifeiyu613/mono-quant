use super::{
    effective_selected_count, run_rotation_backtest, MomentumTopNResult, RotationBacktestConfig,
};
use crate::config::RiskConfig;
use crate::data::Bar;
use crate::strategy::absolute_momentum_breadth::select_absolute_momentum_breadth;
use crate::strategy::absolute_momentum_single::select_absolute_momentum_single;
use crate::strategy::adaptive_dual_momentum::select_adaptive_dual_momentum_assets;
use crate::strategy::breakdown_timing_single::select_breakdown_timing_single;
use crate::strategy::breakout_rotation_topn::select_breakout_rotation_topn;
use crate::strategy::breakout_timing_single::select_breakout_timing_single;
use crate::strategy::defensive_pair_rotation::select_defensive_pair_rotation_asset;
use crate::strategy::dual_momentum::select_dual_momentum_assets;
use crate::strategy::low_volatility_topn::rank_assets_by_low_volatility;
use crate::strategy::ma_rotation_topn::rank_assets_by_ma_rotation;
use crate::strategy::ma_timing_single::select_ma_timing_single;
use crate::strategy::momentum_topn::rank_assets_by_lookback;
use crate::strategy::relative_strength_pair::select_relative_strength_pair;
use crate::strategy::reversal_bottomn::rank_assets_by_reversal;
use crate::strategy::risk_off_rotation::select_risk_off_rotation_asset;
use crate::strategy::volatility_adjusted_momentum::rank_assets_by_volatility_adjusted_momentum;
use crate::strategy::volatility_target_rotation::select_volatility_target_rotation_assets;
use chrono::NaiveDate;
use std::collections::HashMap;

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

/// 运行波动目标轮动回测：当组合波动超出目标时自动降低风险资产占比，并回退到防守资产。
pub fn run_volatility_target_rotation_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    top_n: usize,
    target_volatility: f64,
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
            select_volatility_target_rotation_assets(
                maps,
                dates,
                i,
                cfg.lookback,
                top_n,
                target_volatility,
                defensive_asset,
            )
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

/// 运行自适应双动量回测：根据广度动态调整持仓集中度与绝对动量门槛。
pub fn run_adaptive_dual_momentum_backtest(
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
            select_adaptive_dual_momentum_assets(
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
            let ranking =
                rank_assets_by_low_volatility(maps, dates, i, cfg.lookback, defensive_asset);
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

/// 运行单资产跌破择时回测：跌破回看窗口低点则进入防守资产或空仓，否则持有基准资产。
pub fn run_breakdown_timing_single_backtest(
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
            select_breakdown_timing_single(
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
            select_breakout_rotation_topn(maps, dates, i, cfg.lookback, top_n, defensive_asset)
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

/// 运行防守资产对轮动回测：在两类防守资产中择强持有。
pub fn run_defensive_pair_rotation_backtest(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    cfg: RotationBacktestConfig<'_>,
    primary_defensive_asset: &str,
    secondary_defensive_asset: &str,
) -> MomentumTopNResult {
    run_rotation_backtest(
        asset_maps,
        cfg.lookback,
        cfg.rebalance_freq,
        cfg.commission,
        cfg.slippage,
        cfg.risk,
        |i, dates, maps| {
            select_defensive_pair_rotation_asset(
                maps,
                dates,
                i,
                cfg.lookback,
                primary_defensive_asset,
                secondary_defensive_asset,
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
