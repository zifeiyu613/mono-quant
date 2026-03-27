use super::RotationStrategySpec;
use crate::config::RiskConfig;
use crate::data::Bar;
use crate::engine::backtest::{
    effective_selected_count, run_absolute_momentum_breadth_backtest,
    run_absolute_momentum_single_backtest, run_adaptive_dual_momentum_backtest,
    run_breakdown_timing_single_backtest, run_breakout_rotation_topn_backtest,
    run_breakout_timing_single_backtest, run_buy_hold_equal_weight_backtest,
    run_buy_hold_single_backtest, run_defensive_pair_rotation_backtest, run_dual_momentum_backtest,
    run_low_volatility_topn_backtest, run_ma_rotation_topn_backtest, run_ma_timing_single_backtest,
    run_momentum_topn_backtest, run_relative_strength_pair_backtest, run_reversal_bottomn_backtest,
    run_risk_off_rotation_backtest, run_volatility_adjusted_momentum_backtest,
    run_volatility_target_rotation_backtest, MomentumTopNResult, RotationBacktestConfig,
};
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

impl RotationStrategySpec {
    pub fn run(
        &self,
        asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
        commission: f64,
        slippage: f64,
        risk: Option<&RiskConfig>,
    ) -> MomentumTopNResult {
        let make_cfg = |lookback: usize, rebalance_freq: usize| RotationBacktestConfig {
            lookback,
            rebalance_freq,
            commission,
            slippage,
            risk,
        };

        match self {
            Self::MomentumTopN {
                lookback,
                rebalance_freq,
                top_n,
            } => run_momentum_topn_backtest(
                asset_maps,
                *lookback,
                *rebalance_freq,
                *top_n,
                commission,
                slippage,
                risk,
            ),
            Self::VolatilityAdjustedMomentum {
                lookback,
                rebalance_freq,
                top_n,
            } => run_volatility_adjusted_momentum_backtest(
                asset_maps,
                *lookback,
                *rebalance_freq,
                *top_n,
                commission,
                slippage,
                risk,
            ),
            Self::LowVolatilityTopN {
                lookback,
                rebalance_freq,
                top_n,
                defensive_asset,
            } => run_low_volatility_topn_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *top_n,
                defensive_asset.as_deref(),
            ),
            Self::ReversalBottomN {
                lookback,
                rebalance_freq,
                top_n,
            } => run_reversal_bottomn_backtest(
                asset_maps,
                *lookback,
                *rebalance_freq,
                *top_n,
                commission,
                slippage,
                risk,
            ),
            Self::BuyHoldSingle { benchmark_asset } => run_buy_hold_single_backtest(
                asset_maps,
                benchmark_asset,
                commission,
                slippage,
                risk,
            ),
            Self::BuyHoldEqualWeight => {
                run_buy_hold_equal_weight_backtest(asset_maps, commission, slippage, risk)
            }
            Self::AbsoluteMomentumBreadth {
                lookback,
                rebalance_freq,
                absolute_momentum_floor,
                defensive_asset,
            } => run_absolute_momentum_breadth_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::AbsoluteMomentumSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                absolute_momentum_floor,
                defensive_asset,
            } => run_absolute_momentum_single_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                benchmark_asset,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::DualMomentum {
                lookback,
                rebalance_freq,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
            } => run_dual_momentum_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *top_n,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::AdaptiveDualMomentum {
                lookback,
                rebalance_freq,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
            } => run_adaptive_dual_momentum_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *top_n,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::VolatilityTargetRotation {
                lookback,
                rebalance_freq,
                top_n,
                target_volatility,
                defensive_asset,
            } => run_volatility_target_rotation_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *top_n,
                *target_volatility,
                defensive_asset.as_deref(),
            ),
            Self::RiskOffRotation {
                lookback,
                rebalance_freq,
                risk_assets,
                absolute_momentum_floor,
                defensive_asset,
            } => run_risk_off_rotation_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                risk_assets,
                *absolute_momentum_floor,
                defensive_asset,
            ),
            Self::MaTimingSingle {
                benchmark_asset,
                fast,
                slow,
                rebalance_freq,
                defensive_asset,
            } => run_ma_timing_single_backtest(
                asset_maps,
                make_cfg(slow.saturating_sub(1), *rebalance_freq),
                benchmark_asset,
                *fast,
                *slow,
                defensive_asset.as_deref(),
            ),
            Self::MaRotationTopN {
                fast,
                slow,
                lookback,
                rebalance_freq,
                top_n,
                defensive_asset,
            } => run_ma_rotation_topn_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *fast,
                *slow,
                *top_n,
                defensive_asset.as_deref(),
            ),
            Self::BreakoutTimingSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                defensive_asset,
            } => run_breakout_timing_single_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                benchmark_asset,
                defensive_asset.as_deref(),
            ),
            Self::BreakdownTimingSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                defensive_asset,
            } => run_breakdown_timing_single_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                benchmark_asset,
                defensive_asset.as_deref(),
            ),
            Self::BreakoutRotationTopN {
                lookback,
                rebalance_freq,
                top_n,
                defensive_asset,
            } => run_breakout_rotation_topn_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                *top_n,
                defensive_asset.as_deref(),
            ),
            Self::RelativeStrengthPair {
                benchmark_asset,
                defensive_asset,
                lookback,
                rebalance_freq,
            } => run_relative_strength_pair_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                benchmark_asset,
                defensive_asset,
            ),
            Self::DefensivePairRotation {
                benchmark_asset,
                defensive_asset,
                lookback,
                rebalance_freq,
            } => run_defensive_pair_rotation_backtest(
                asset_maps,
                make_cfg(*lookback, *rebalance_freq),
                benchmark_asset,
                defensive_asset,
            ),
        }
    }

    pub fn is_rebalance_due(&self, index: usize) -> bool {
        match self {
            Self::MomentumTopN {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::VolatilityAdjustedMomentum {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::LowVolatilityTopN {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::ReversalBottomN {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::AbsoluteMomentumBreadth {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::AbsoluteMomentumSingle {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::DualMomentum {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::AdaptiveDualMomentum {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::VolatilityTargetRotation {
                lookback,
                rebalance_freq,
                ..
            }
            | Self::RiskOffRotation {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback).is_multiple_of(*rebalance_freq),
            Self::MaTimingSingle {
                slow,
                rebalance_freq,
                ..
            } => {
                let lookback = slow.saturating_sub(1);
                index >= lookback && (index - lookback).is_multiple_of(*rebalance_freq)
            }
            Self::MaRotationTopN {
                slow,
                lookback,
                rebalance_freq,
                ..
            } => {
                let required_lookback = slow.saturating_sub(1).max(*lookback);
                index >= required_lookback
                    && (index - required_lookback).is_multiple_of(*rebalance_freq)
            }
            Self::BreakoutTimingSingle {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback).is_multiple_of(*rebalance_freq),
            Self::BreakdownTimingSingle {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback).is_multiple_of(*rebalance_freq),
            Self::BreakoutRotationTopN {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback).is_multiple_of(*rebalance_freq),
            Self::RelativeStrengthPair {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback).is_multiple_of(*rebalance_freq),
            Self::DefensivePairRotation {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback).is_multiple_of(*rebalance_freq),
            Self::BuyHoldSingle { .. } | Self::BuyHoldEqualWeight => false,
        }
    }

    pub fn preview_selected_assets(
        &self,
        asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
        dates: &[NaiveDate],
        index: usize,
        risk: Option<&RiskConfig>,
    ) -> Vec<String> {
        match self {
            Self::MomentumTopN {
                lookback, top_n, ..
            } => {
                let ranking = rank_assets_by_lookback(asset_maps, dates, index, *lookback);
                let selected_count = effective_selected_count(*top_n, ranking.len(), risk);
                ranking
                    .into_iter()
                    .take(selected_count)
                    .map(|item| item.0)
                    .collect()
            }
            Self::VolatilityAdjustedMomentum {
                lookback, top_n, ..
            } => {
                let ranking = rank_assets_by_volatility_adjusted_momentum(
                    asset_maps, dates, index, *lookback,
                );
                let selected_count = effective_selected_count(*top_n, ranking.len(), risk);
                ranking
                    .into_iter()
                    .take(selected_count)
                    .map(|item| item.0)
                    .collect()
            }
            Self::LowVolatilityTopN {
                lookback,
                top_n,
                defensive_asset,
                ..
            } => {
                let ranking = rank_assets_by_low_volatility(
                    asset_maps,
                    dates,
                    index,
                    *lookback,
                    defensive_asset.as_deref(),
                );
                let selected_count = effective_selected_count(*top_n, ranking.len(), risk);
                ranking
                    .into_iter()
                    .take(selected_count)
                    .map(|item| item.0)
                    .collect()
            }
            Self::ReversalBottomN {
                lookback, top_n, ..
            } => {
                let ranking = rank_assets_by_reversal(asset_maps, dates, index, *lookback);
                let selected_count = effective_selected_count(*top_n, ranking.len(), risk);
                ranking
                    .into_iter()
                    .take(selected_count)
                    .map(|item| item.0)
                    .collect()
            }
            Self::BuyHoldSingle { benchmark_asset } => vec![benchmark_asset.clone()],
            Self::BuyHoldEqualWeight => {
                let mut names: Vec<String> = asset_maps.keys().cloned().collect();
                names.sort();
                names
            }
            Self::AbsoluteMomentumBreadth {
                lookback,
                absolute_momentum_floor,
                defensive_asset,
                ..
            } => select_absolute_momentum_breadth(
                asset_maps,
                dates,
                index,
                *lookback,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::AbsoluteMomentumSingle {
                benchmark_asset,
                lookback,
                absolute_momentum_floor,
                defensive_asset,
                ..
            } => select_absolute_momentum_single(
                asset_maps,
                dates,
                index,
                *lookback,
                benchmark_asset,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::DualMomentum {
                lookback,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
                ..
            } => select_dual_momentum_assets(
                asset_maps,
                dates,
                index,
                *lookback,
                *top_n,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::AdaptiveDualMomentum {
                lookback,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
                ..
            } => select_adaptive_dual_momentum_assets(
                asset_maps,
                dates,
                index,
                *lookback,
                *top_n,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
            ),
            Self::VolatilityTargetRotation {
                lookback,
                top_n,
                target_volatility,
                defensive_asset,
                ..
            } => select_volatility_target_rotation_assets(
                asset_maps,
                dates,
                index,
                *lookback,
                *top_n,
                *target_volatility,
                defensive_asset.as_deref(),
            ),
            Self::RiskOffRotation {
                lookback,
                risk_assets,
                absolute_momentum_floor,
                defensive_asset,
                ..
            } => select_risk_off_rotation_asset(
                asset_maps,
                dates,
                index,
                *lookback,
                risk_assets,
                *absolute_momentum_floor,
                defensive_asset,
            ),
            Self::MaTimingSingle {
                benchmark_asset,
                fast,
                slow,
                defensive_asset,
                ..
            } => select_ma_timing_single(
                asset_maps,
                dates,
                index,
                *fast,
                *slow,
                benchmark_asset,
                defensive_asset.as_deref(),
            ),
            Self::MaRotationTopN {
                fast,
                slow,
                lookback,
                top_n,
                defensive_asset,
                ..
            } => {
                let ranking = rank_assets_by_ma_rotation(
                    asset_maps,
                    dates,
                    index,
                    *fast,
                    *slow,
                    *lookback,
                    defensive_asset.as_deref(),
                );
                let selected_count = effective_selected_count(*top_n, ranking.len(), risk);
                ranking
                    .into_iter()
                    .take(selected_count)
                    .map(|item| item.0)
                    .collect()
            }
            Self::BreakoutTimingSingle {
                benchmark_asset,
                lookback,
                defensive_asset,
                ..
            } => select_breakout_timing_single(
                asset_maps,
                dates,
                index,
                *lookback,
                benchmark_asset,
                defensive_asset.as_deref(),
            ),
            Self::BreakdownTimingSingle {
                benchmark_asset,
                lookback,
                defensive_asset,
                ..
            } => select_breakdown_timing_single(
                asset_maps,
                dates,
                index,
                *lookback,
                benchmark_asset,
                defensive_asset.as_deref(),
            ),
            Self::BreakoutRotationTopN {
                lookback,
                top_n,
                defensive_asset,
                ..
            } => select_breakout_rotation_topn(
                asset_maps,
                dates,
                index,
                *lookback,
                *top_n,
                defensive_asset.as_deref(),
            ),
            Self::RelativeStrengthPair {
                benchmark_asset,
                defensive_asset,
                lookback,
                ..
            } => select_relative_strength_pair(
                asset_maps,
                dates,
                index,
                *lookback,
                benchmark_asset,
                defensive_asset,
            ),
            Self::DefensivePairRotation {
                benchmark_asset,
                defensive_asset,
                lookback,
                ..
            } => select_defensive_pair_rotation_asset(
                asset_maps,
                dates,
                index,
                *lookback,
                benchmark_asset,
                defensive_asset,
            ),
        }
    }
}
