use super::RotationStrategySpec;
use crate::config::AppConfig;
use anyhow::{anyhow, Result};

impl RotationStrategySpec {
    pub fn from_app_config(cfg: &AppConfig) -> Result<Self> {
        match cfg.strategy.as_str() {
            "momentum_topn" => Ok(Self::MomentumTopN {
                lookback: cfg
                    .lookback
                    .ok_or_else(|| anyhow!("momentum_topn 需要提供 lookback"))?,
                rebalance_freq: require_positive_usize(
                    "momentum_topn",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("momentum_topn 需要提供 rebalance_freq"))?,
                )?,
                top_n: cfg
                    .top_n
                    .ok_or_else(|| anyhow!("momentum_topn 需要提供 top_n"))?,
            }),
            "volatility_adjusted_momentum" => Ok(Self::VolatilityAdjustedMomentum {
                lookback: require_positive_usize(
                    "volatility_adjusted_momentum",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("volatility_adjusted_momentum 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "volatility_adjusted_momentum",
                    "rebalance_freq",
                    cfg.rebalance_freq.ok_or_else(|| {
                        anyhow!("volatility_adjusted_momentum 需要提供 rebalance_freq")
                    })?,
                )?,
                top_n: require_positive_usize(
                    "volatility_adjusted_momentum",
                    "top_n",
                    cfg.top_n
                        .ok_or_else(|| anyhow!("volatility_adjusted_momentum 需要提供 top_n"))?,
                )?,
            }),
            "low_volatility_topn" => Ok(Self::LowVolatilityTopN {
                lookback: require_positive_usize(
                    "low_volatility_topn",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("low_volatility_topn 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "low_volatility_topn",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("low_volatility_topn 需要提供 rebalance_freq"))?,
                )?,
                top_n: require_positive_usize(
                    "low_volatility_topn",
                    "top_n",
                    cfg.top_n
                        .ok_or_else(|| anyhow!("low_volatility_topn 需要提供 top_n"))?,
                )?,
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "reversal_bottomn" => Ok(Self::ReversalBottomN {
                lookback: require_positive_usize(
                    "reversal_bottomn",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("reversal_bottomn 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "reversal_bottomn",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("reversal_bottomn 需要提供 rebalance_freq"))?,
                )?,
                top_n: require_positive_usize(
                    "reversal_bottomn",
                    "top_n",
                    cfg.top_n
                        .ok_or_else(|| anyhow!("reversal_bottomn 需要提供 top_n"))?,
                )?,
            }),
            "buy_hold_single" => Ok(Self::BuyHoldSingle {
                benchmark_asset: cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("buy_hold_single 需要提供 benchmark_asset"))?,
            }),
            "buy_hold_equal_weight" => Ok(Self::BuyHoldEqualWeight),
            "absolute_momentum_breadth" => Ok(Self::AbsoluteMomentumBreadth {
                lookback: require_positive_usize(
                    "absolute_momentum_breadth",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("absolute_momentum_breadth 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "absolute_momentum_breadth",
                    "rebalance_freq",
                    cfg.rebalance_freq.ok_or_else(|| {
                        anyhow!("absolute_momentum_breadth 需要提供 rebalance_freq")
                    })?,
                )?,
                absolute_momentum_floor: cfg.absolute_momentum_floor.unwrap_or(0.0),
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "absolute_momentum_single" => Ok(Self::AbsoluteMomentumSingle {
                benchmark_asset: cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("absolute_momentum_single 需要提供 benchmark_asset"))?,
                lookback: require_positive_usize(
                    "absolute_momentum_single",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("absolute_momentum_single 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "absolute_momentum_single",
                    "rebalance_freq",
                    cfg.rebalance_freq.ok_or_else(|| {
                        anyhow!("absolute_momentum_single 需要提供 rebalance_freq")
                    })?,
                )?,
                absolute_momentum_floor: cfg.absolute_momentum_floor.unwrap_or(0.0),
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "dual_momentum" => Ok(Self::DualMomentum {
                lookback: cfg
                    .lookback
                    .ok_or_else(|| anyhow!("dual_momentum 需要提供 lookback"))?,
                rebalance_freq: require_positive_usize(
                    "dual_momentum",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("dual_momentum 需要提供 rebalance_freq"))?,
                )?,
                top_n: cfg
                    .top_n
                    .ok_or_else(|| anyhow!("dual_momentum 需要提供 top_n"))?,
                absolute_momentum_floor: cfg.absolute_momentum_floor.unwrap_or(0.0),
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "adaptive_dual_momentum" => Ok(Self::AdaptiveDualMomentum {
                lookback: cfg
                    .lookback
                    .ok_or_else(|| anyhow!("adaptive_dual_momentum 需要提供 lookback"))?,
                rebalance_freq: require_positive_usize(
                    "adaptive_dual_momentum",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("adaptive_dual_momentum 需要提供 rebalance_freq"))?,
                )?,
                top_n: cfg
                    .top_n
                    .ok_or_else(|| anyhow!("adaptive_dual_momentum 需要提供 top_n"))?,
                absolute_momentum_floor: cfg.absolute_momentum_floor.unwrap_or(0.0),
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "volatility_target_rotation" => Ok(Self::VolatilityTargetRotation {
                lookback: require_positive_usize(
                    "volatility_target_rotation",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("volatility_target_rotation 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "volatility_target_rotation",
                    "rebalance_freq",
                    cfg.rebalance_freq.ok_or_else(|| {
                        anyhow!("volatility_target_rotation 需要提供 rebalance_freq")
                    })?,
                )?,
                top_n: require_positive_usize(
                    "volatility_target_rotation",
                    "top_n",
                    cfg.top_n
                        .ok_or_else(|| anyhow!("volatility_target_rotation 需要提供 top_n"))?,
                )?,
                target_volatility: require_positive_f64(
                    "volatility_target_rotation",
                    "target_volatility",
                    cfg.target_volatility.ok_or_else(|| {
                        anyhow!("volatility_target_rotation 需要提供 target_volatility")
                    })?,
                )?,
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "risk_off_rotation" => {
                let lookback = cfg
                    .lookback
                    .ok_or_else(|| anyhow!("risk_off_rotation 需要提供 lookback"))?;
                let rebalance_freq = require_positive_usize(
                    "risk_off_rotation",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("risk_off_rotation 需要提供 rebalance_freq"))?,
                )?;
                let defensive_asset = cfg
                    .defensive_asset
                    .clone()
                    .ok_or_else(|| anyhow!("risk_off_rotation 需要提供 defensive_asset"))?;
                let mut risk_assets = if let Some(items) = &cfg.risk_assets {
                    items.clone()
                } else if let Some(asset_files) = &cfg.asset_files {
                    asset_files
                        .keys()
                        .filter(|asset| **asset != defensive_asset)
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };
                risk_assets.retain(|asset| asset != &defensive_asset);
                if risk_assets.is_empty() {
                    return Err(anyhow!(
                        "risk_off_rotation 需要至少 1 个风险资产，请设置 risk_assets 或在 asset_files 中提供除 defensive_asset 之外的资产"
                    ));
                }
                Ok(Self::RiskOffRotation {
                    lookback,
                    rebalance_freq,
                    risk_assets,
                    absolute_momentum_floor: cfg.absolute_momentum_floor.unwrap_or(0.0),
                    defensive_asset,
                })
            }
            "ma_timing_single" => {
                let benchmark_asset = cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("ma_timing_single 需要提供 benchmark_asset"))?;
                let fast = require_positive_usize(
                    "ma_timing_single",
                    "fast",
                    cfg.fast
                        .ok_or_else(|| anyhow!("ma_timing_single 需要提供 fast"))?,
                )?;
                let slow = require_positive_usize(
                    "ma_timing_single",
                    "slow",
                    cfg.slow
                        .ok_or_else(|| anyhow!("ma_timing_single 需要提供 slow"))?,
                )?;
                if fast >= slow {
                    return Err(anyhow!("ma_timing_single 要求 fast < slow"));
                }
                let rebalance_freq = require_positive_usize(
                    "ma_timing_single",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("ma_timing_single 需要提供 rebalance_freq"))?,
                )?;
                Ok(Self::MaTimingSingle {
                    benchmark_asset,
                    fast,
                    slow,
                    rebalance_freq,
                    defensive_asset: cfg.defensive_asset.clone(),
                })
            }
            "ma_rotation_topn" => {
                let fast = require_positive_usize(
                    "ma_rotation_topn",
                    "fast",
                    cfg.fast
                        .ok_or_else(|| anyhow!("ma_rotation_topn 需要提供 fast"))?,
                )?;
                let slow = require_positive_usize(
                    "ma_rotation_topn",
                    "slow",
                    cfg.slow
                        .ok_or_else(|| anyhow!("ma_rotation_topn 需要提供 slow"))?,
                )?;
                if fast >= slow {
                    return Err(anyhow!("ma_rotation_topn 要求 fast < slow"));
                }
                Ok(Self::MaRotationTopN {
                    fast,
                    slow,
                    lookback: require_positive_usize(
                        "ma_rotation_topn",
                        "lookback",
                        cfg.lookback
                            .ok_or_else(|| anyhow!("ma_rotation_topn 需要提供 lookback"))?,
                    )?,
                    rebalance_freq: require_positive_usize(
                        "ma_rotation_topn",
                        "rebalance_freq",
                        cfg.rebalance_freq
                            .ok_or_else(|| anyhow!("ma_rotation_topn 需要提供 rebalance_freq"))?,
                    )?,
                    top_n: require_positive_usize(
                        "ma_rotation_topn",
                        "top_n",
                        cfg.top_n
                            .ok_or_else(|| anyhow!("ma_rotation_topn 需要提供 top_n"))?,
                    )?,
                    defensive_asset: cfg.defensive_asset.clone(),
                })
            }
            "breakout_timing_single" => Ok(Self::BreakoutTimingSingle {
                benchmark_asset: cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("breakout_timing_single 需要提供 benchmark_asset"))?,
                lookback: require_positive_usize(
                    "breakout_timing_single",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("breakout_timing_single 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "breakout_timing_single",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("breakout_timing_single 需要提供 rebalance_freq"))?,
                )?,
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "breakdown_timing_single" => Ok(Self::BreakdownTimingSingle {
                benchmark_asset: cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("breakdown_timing_single 需要提供 benchmark_asset"))?,
                lookback: require_positive_usize(
                    "breakdown_timing_single",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("breakdown_timing_single 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "breakdown_timing_single",
                    "rebalance_freq",
                    cfg.rebalance_freq.ok_or_else(|| {
                        anyhow!("breakdown_timing_single 需要提供 rebalance_freq")
                    })?,
                )?,
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "breakout_rotation_topn" => Ok(Self::BreakoutRotationTopN {
                lookback: require_positive_usize(
                    "breakout_rotation_topn",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("breakout_rotation_topn 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "breakout_rotation_topn",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("breakout_rotation_topn 需要提供 rebalance_freq"))?,
                )?,
                top_n: require_positive_usize(
                    "breakout_rotation_topn",
                    "top_n",
                    cfg.top_n
                        .ok_or_else(|| anyhow!("breakout_rotation_topn 需要提供 top_n"))?,
                )?,
                defensive_asset: cfg.defensive_asset.clone(),
            }),
            "relative_strength_pair" => Ok(Self::RelativeStrengthPair {
                benchmark_asset: cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("relative_strength_pair 需要提供 benchmark_asset"))?,
                defensive_asset: cfg
                    .defensive_asset
                    .clone()
                    .ok_or_else(|| anyhow!("relative_strength_pair 需要提供 defensive_asset"))?,
                lookback: require_positive_usize(
                    "relative_strength_pair",
                    "lookback",
                    cfg.lookback
                        .ok_or_else(|| anyhow!("relative_strength_pair 需要提供 lookback"))?,
                )?,
                rebalance_freq: require_positive_usize(
                    "relative_strength_pair",
                    "rebalance_freq",
                    cfg.rebalance_freq
                        .ok_or_else(|| anyhow!("relative_strength_pair 需要提供 rebalance_freq"))?,
                )?,
            }),
            "defensive_pair_rotation" => {
                Ok(Self::DefensivePairRotation {
                    benchmark_asset: cfg.benchmark_asset.clone().ok_or_else(|| {
                        anyhow!("defensive_pair_rotation 需要提供 benchmark_asset")
                    })?,
                    defensive_asset: cfg.defensive_asset.clone().ok_or_else(|| {
                        anyhow!("defensive_pair_rotation 需要提供 defensive_asset")
                    })?,
                    lookback: require_positive_usize(
                        "defensive_pair_rotation",
                        "lookback",
                        cfg.lookback
                            .ok_or_else(|| anyhow!("defensive_pair_rotation 需要提供 lookback"))?,
                    )?,
                    rebalance_freq: require_positive_usize(
                        "defensive_pair_rotation",
                        "rebalance_freq",
                        cfg.rebalance_freq.ok_or_else(|| {
                            anyhow!("defensive_pair_rotation 需要提供 rebalance_freq")
                        })?,
                    )?,
                })
            }
            other => Err(anyhow!("不支持的轮动策略：{}", other)),
        }
    }
}

fn require_positive_usize(strategy: &str, field: &str, value: usize) -> Result<usize> {
    if value == 0 {
        return Err(anyhow!("{} 的 {} 必须大于 0", strategy, field));
    }
    Ok(value)
}

fn require_positive_f64(strategy: &str, field: &str, value: f64) -> Result<f64> {
    if value <= 0.0 {
        return Err(anyhow!("{} 的 {} 必须大于 0", strategy, field));
    }
    Ok(value)
}
