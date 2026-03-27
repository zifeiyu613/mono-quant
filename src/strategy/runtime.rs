use crate::config::{AppConfig, RiskConfig};
use crate::data::Bar;
use crate::engine::backtest::{
    required_asset_count_for_max_weight, run_absolute_momentum_breadth_backtest,
    run_absolute_momentum_single_backtest, run_breakout_rotation_topn_backtest,
    run_breakout_timing_single_backtest,
    run_buy_hold_equal_weight_backtest, run_buy_hold_single_backtest, run_dual_momentum_backtest,
    run_ma_timing_single_backtest, run_momentum_topn_backtest, run_risk_off_rotation_backtest,
    run_relative_strength_pair_backtest, run_reversal_bottomn_backtest,
    run_volatility_adjusted_momentum_backtest, MomentumTopNResult,
};
use crate::strategy::absolute_momentum_breadth::select_absolute_momentum_breadth;
use crate::strategy::absolute_momentum_single::select_absolute_momentum_single;
use crate::strategy::breakout_rotation_topn::select_breakout_rotation_topn;
use crate::strategy::breakout_timing_single::select_breakout_timing_single;
use crate::strategy::dual_momentum::select_dual_momentum_assets;
use crate::strategy::ma_timing_single::select_ma_timing_single;
use crate::strategy::momentum_topn::rank_assets_by_lookback;
use crate::strategy::relative_strength_pair::select_relative_strength_pair;
use crate::strategy::risk_off_rotation::select_risk_off_rotation_asset;
use crate::strategy::reversal_bottomn::rank_assets_by_reversal;
use crate::strategy::volatility_adjusted_momentum::rank_assets_by_volatility_adjusted_momentum;
use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum RotationStrategySpec {
    MomentumTopN {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
    },
    VolatilityAdjustedMomentum {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
    },
    ReversalBottomN {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
    },
    BuyHoldSingle {
        benchmark_asset: String,
    },
    BuyHoldEqualWeight,
    AbsoluteMomentumBreadth {
        lookback: usize,
        rebalance_freq: usize,
        absolute_momentum_floor: f64,
        defensive_asset: Option<String>,
    },
    AbsoluteMomentumSingle {
        benchmark_asset: String,
        lookback: usize,
        rebalance_freq: usize,
        absolute_momentum_floor: f64,
        defensive_asset: Option<String>,
    },
    DualMomentum {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
        absolute_momentum_floor: f64,
        defensive_asset: Option<String>,
    },
    RiskOffRotation {
        lookback: usize,
        rebalance_freq: usize,
        risk_assets: Vec<String>,
        absolute_momentum_floor: f64,
        defensive_asset: String,
    },
    MaTimingSingle {
        benchmark_asset: String,
        fast: usize,
        slow: usize,
        rebalance_freq: usize,
        defensive_asset: Option<String>,
    },
    BreakoutTimingSingle {
        benchmark_asset: String,
        lookback: usize,
        rebalance_freq: usize,
        defensive_asset: Option<String>,
    },
    BreakoutRotationTopN {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
        defensive_asset: Option<String>,
    },
    RelativeStrengthPair {
        benchmark_asset: String,
        defensive_asset: String,
        lookback: usize,
        rebalance_freq: usize,
    },
}

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
                    cfg
                        .rebalance_freq
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
                    cfg.lookback.ok_or_else(|| {
                        anyhow!("volatility_adjusted_momentum 需要提供 lookback")
                    })?,
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
                    cfg
                        .rebalance_freq
                        .ok_or_else(|| anyhow!("dual_momentum 需要提供 rebalance_freq"))?,
                )?,
                top_n: cfg
                    .top_n
                    .ok_or_else(|| anyhow!("dual_momentum 需要提供 top_n"))?,
                absolute_momentum_floor: cfg.absolute_momentum_floor.unwrap_or(0.0),
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
                    cfg.fast.ok_or_else(|| anyhow!("ma_timing_single 需要提供 fast"))?,
                )?;
                let slow = require_positive_usize(
                    "ma_timing_single",
                    "slow",
                    cfg.slow.ok_or_else(|| anyhow!("ma_timing_single 需要提供 slow"))?,
                )?;
                if fast >= slow {
                    return Err(anyhow!("ma_timing_single 要求 fast < slow"));
                }
                let rebalance_freq = require_positive_usize(
                    "ma_timing_single",
                    "rebalance_freq",
                    cfg
                        .rebalance_freq
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
                    cfg.rebalance_freq.ok_or_else(|| {
                        anyhow!("breakout_timing_single 需要提供 rebalance_freq")
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
            other => Err(anyhow!("不支持的轮动策略：{}", other)),
        }
    }

    pub fn required_lookback(&self) -> usize {
        match self {
            Self::MomentumTopN { lookback, .. } => *lookback,
            Self::VolatilityAdjustedMomentum { lookback, .. } => *lookback,
            Self::ReversalBottomN { lookback, .. } => *lookback,
            Self::BuyHoldSingle { .. } => 0,
            Self::BuyHoldEqualWeight => 0,
            Self::AbsoluteMomentumBreadth { lookback, .. } => *lookback,
            Self::AbsoluteMomentumSingle { lookback, .. } => *lookback,
            Self::DualMomentum { lookback, .. } => *lookback,
            Self::RiskOffRotation { lookback, .. } => *lookback,
            Self::MaTimingSingle { slow, .. } => slow.saturating_sub(1),
            Self::BreakoutTimingSingle { lookback, .. } => *lookback,
            Self::BreakoutRotationTopN { lookback, .. } => *lookback,
            Self::RelativeStrengthPair { lookback, .. } => *lookback,
        }
    }

    pub fn summary_title(&self) -> &'static str {
        match self {
            Self::MomentumTopN { .. } => "动量轮动摘要",
            Self::VolatilityAdjustedMomentum { .. } => "波动调整动量摘要",
            Self::ReversalBottomN { .. } => "反转 BottomN 摘要",
            Self::BuyHoldSingle { .. } => "Buy & Hold 单资产摘要",
            Self::BuyHoldEqualWeight => "Buy & Hold 等权摘要",
            Self::AbsoluteMomentumBreadth { .. } => "多资产绝对动量广度摘要",
            Self::AbsoluteMomentumSingle { .. } => "单资产绝对动量开关摘要",
            Self::DualMomentum { .. } => "双动量摘要",
            Self::RiskOffRotation { .. } => "风险开关轮动摘要",
            Self::MaTimingSingle { .. } => "单资产均线择时摘要",
            Self::BreakoutTimingSingle { .. } => "单资产突破择时摘要",
            Self::BreakoutRotationTopN { .. } => "多资产突破轮动摘要",
            Self::RelativeStrengthPair { .. } => "双资产相对强弱切换摘要",
        }
    }

    pub fn detail_rows(&self) -> Vec<(String, String)> {
        match self {
            Self::MomentumTopN {
                lookback,
                rebalance_freq,
                top_n,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
            ],
            Self::VolatilityAdjustedMomentum {
                lookback,
                rebalance_freq,
                top_n,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
                ("ranking_mode".to_string(), "return_over_volatility".to_string()),
            ],
            Self::ReversalBottomN {
                lookback,
                rebalance_freq,
                top_n,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
                ("ranking_mode".to_string(), "reversal_bottomn".to_string()),
            ],
            Self::BuyHoldSingle { benchmark_asset } => {
                vec![("benchmark_asset".to_string(), benchmark_asset.clone())]
            }
            Self::BuyHoldEqualWeight => {
                vec![("weight_mode".to_string(), "equal_weight".to_string())]
            }
            Self::AbsoluteMomentumBreadth {
                lookback,
                rebalance_freq,
                absolute_momentum_floor,
                defensive_asset,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                (
                    "absolute_momentum_floor".to_string(),
                    format!("{:.4}", absolute_momentum_floor),
                ),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "空仓".to_string()),
                ),
                ("selection_mode".to_string(), "breadth_positive".to_string()),
            ],
            Self::AbsoluteMomentumSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                absolute_momentum_floor,
                defensive_asset,
            } => vec![
                ("benchmark_asset".to_string(), benchmark_asset.clone()),
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                (
                    "absolute_momentum_floor".to_string(),
                    format!("{:.4}", absolute_momentum_floor),
                ),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "空仓".to_string()),
                ),
            ],
            Self::DualMomentum {
                lookback,
                rebalance_freq,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
                (
                    "absolute_momentum_floor".to_string(),
                    format!("{:.4}", absolute_momentum_floor),
                ),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "未配置".to_string()),
                ),
            ],
            Self::RiskOffRotation {
                lookback,
                rebalance_freq,
                risk_assets,
                absolute_momentum_floor,
                defensive_asset,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("risk_assets".to_string(), risk_assets.join(",")),
                (
                    "absolute_momentum_floor".to_string(),
                    format!("{:.4}", absolute_momentum_floor),
                ),
                ("defensive_asset".to_string(), defensive_asset.clone()),
            ],
            Self::MaTimingSingle {
                benchmark_asset,
                fast,
                slow,
                rebalance_freq,
                defensive_asset,
            } => vec![
                ("benchmark_asset".to_string(), benchmark_asset.clone()),
                ("fast".to_string(), fast.to_string()),
                ("slow".to_string(), slow.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "空仓".to_string()),
                ),
            ],
            Self::BreakoutTimingSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                defensive_asset,
            } => vec![
                ("benchmark_asset".to_string(), benchmark_asset.clone()),
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "空仓".to_string()),
                ),
            ],
            Self::BreakoutRotationTopN {
                lookback,
                rebalance_freq,
                top_n,
                defensive_asset,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "空仓".to_string()),
                ),
                ("selection_mode".to_string(), "breakout_then_rank".to_string()),
            ],
            Self::RelativeStrengthPair {
                benchmark_asset,
                defensive_asset,
                lookback,
                rebalance_freq,
            } => vec![
                ("benchmark_asset".to_string(), benchmark_asset.clone()),
                ("defensive_asset".to_string(), defensive_asset.clone()),
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
            ],
        }
    }

    pub fn required_assets(&self) -> Vec<&str> {
        match self {
            Self::BuyHoldSingle { benchmark_asset } => vec![benchmark_asset.as_str()],
            Self::AbsoluteMomentumBreadth {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::RelativeStrengthPair {
                benchmark_asset,
                defensive_asset,
                ..
            } => vec![benchmark_asset.as_str(), defensive_asset.as_str()],
            Self::BreakoutRotationTopN {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::BreakoutTimingSingle {
                benchmark_asset,
                defensive_asset,
                ..
            }
            | Self::MaTimingSingle {
                benchmark_asset,
                defensive_asset,
                ..
            } => {
                let mut required = vec![benchmark_asset.as_str()];
                if let Some(defensive_asset) = defensive_asset {
                    required.push(defensive_asset.as_str());
                }
                required
            }
            Self::AbsoluteMomentumSingle {
                benchmark_asset,
                defensive_asset,
                ..
            } => {
                let mut required = vec![benchmark_asset.as_str()];
                if let Some(defensive_asset) = defensive_asset {
                    required.push(defensive_asset.as_str());
                }
                required
            }
            Self::DualMomentum {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::RiskOffRotation {
                risk_assets,
                defensive_asset,
                ..
            } => {
                let mut required = risk_assets.iter().map(|asset| asset.as_str()).collect::<Vec<_>>();
                required.push(defensive_asset.as_str());
                required
            }
            _ => Vec::new(),
        }
    }

    pub fn run(
        &self,
        asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
        commission: f64,
        slippage: f64,
        risk: Option<&RiskConfig>,
    ) -> MomentumTopNResult {
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
                *lookback,
                *rebalance_freq,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
                commission,
                slippage,
                risk,
            ),
            Self::AbsoluteMomentumSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                absolute_momentum_floor,
                defensive_asset,
            } => run_absolute_momentum_single_backtest(
                asset_maps,
                benchmark_asset,
                *lookback,
                *rebalance_freq,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
                commission,
                slippage,
                risk,
            ),
            Self::DualMomentum {
                lookback,
                rebalance_freq,
                top_n,
                absolute_momentum_floor,
                defensive_asset,
            } => run_dual_momentum_backtest(
                asset_maps,
                *lookback,
                *rebalance_freq,
                *top_n,
                *absolute_momentum_floor,
                defensive_asset.as_deref(),
                commission,
                slippage,
                risk,
            ),
            Self::RiskOffRotation {
                lookback,
                rebalance_freq,
                risk_assets,
                absolute_momentum_floor,
                defensive_asset,
            } => run_risk_off_rotation_backtest(
                asset_maps,
                *lookback,
                *rebalance_freq,
                risk_assets,
                *absolute_momentum_floor,
                defensive_asset,
                commission,
                slippage,
                risk,
            ),
            Self::MaTimingSingle {
                benchmark_asset,
                fast,
                slow,
                rebalance_freq,
                defensive_asset,
            } => run_ma_timing_single_backtest(
                asset_maps,
                benchmark_asset,
                *fast,
                *slow,
                *rebalance_freq,
                defensive_asset.as_deref(),
                commission,
                slippage,
                risk,
            ),
            Self::BreakoutTimingSingle {
                benchmark_asset,
                lookback,
                rebalance_freq,
                defensive_asset,
            } => run_breakout_timing_single_backtest(
                asset_maps,
                benchmark_asset,
                *lookback,
                *rebalance_freq,
                defensive_asset.as_deref(),
                commission,
                slippage,
                risk,
            ),
            Self::BreakoutRotationTopN {
                lookback,
                rebalance_freq,
                top_n,
                defensive_asset,
            } => run_breakout_rotation_topn_backtest(
                asset_maps,
                *lookback,
                *rebalance_freq,
                *top_n,
                defensive_asset.as_deref(),
                commission,
                slippage,
                risk,
            ),
            Self::RelativeStrengthPair {
                benchmark_asset,
                defensive_asset,
                lookback,
                rebalance_freq,
            } => run_relative_strength_pair_backtest(
                asset_maps,
                benchmark_asset,
                defensive_asset,
                *lookback,
                *rebalance_freq,
                commission,
                slippage,
                risk,
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
            | Self::RiskOffRotation {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback) % *rebalance_freq == 0,
            Self::MaTimingSingle {
                slow,
                rebalance_freq,
                ..
            } => {
                let lookback = slow.saturating_sub(1);
                index >= lookback && (index - lookback) % *rebalance_freq == 0
            }
            Self::BreakoutTimingSingle {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback) % *rebalance_freq == 0,
            Self::BreakoutRotationTopN {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback) % *rebalance_freq == 0,
            Self::RelativeStrengthPair {
                lookback,
                rebalance_freq,
                ..
            } => index >= *lookback && (index - *lookback) % *rebalance_freq == 0,
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
                let ranking =
                    rank_assets_by_volatility_adjusted_momentum(asset_maps, dates, index, *lookback);
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
        }
    }
}

fn require_positive_usize(strategy: &str, field: &str, value: usize) -> Result<usize> {
    if value == 0 {
        return Err(anyhow!("{} 的 {} 必须大于 0", strategy, field));
    }
    Ok(value)
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
        let required_assets = required_asset_count_for_max_weight(max_weight);
        requested_top_n.max(required_assets).min(available_assets)
    } else {
        requested_top_n.min(available_assets)
    }
}

#[cfg(test)]
mod tests {
    use super::RotationStrategySpec;
    use crate::config::AppConfig;

    #[test]
    fn dual_momentum_requires_defensive_asset_when_configured() {
        let spec = RotationStrategySpec::DualMomentum {
            lookback: 20,
            rebalance_freq: 20,
            top_n: 2,
            absolute_momentum_floor: 0.0,
            defensive_asset: Some("dividend".to_string()),
        };

        assert_eq!(spec.required_assets(), vec!["dividend"]);
    }

    #[test]
    fn momentum_topn_rejects_zero_rebalance_freq() {
        let cfg = AppConfig {
            experiment_name: "test".to_string(),
            strategy: "momentum_topn".to_string(),
            data_file: None,
            asset_files: None,
            compare_configs: None,
            source_config: None,
            benchmark_asset: None,
            risk_assets: None,
            defensive_asset: None,
            fast: None,
            slow: None,
            lookback: Some(20),
            rebalance_freq: Some(0),
            top_n: Some(2),
            absolute_momentum_floor: None,
            lookbacks: None,
            rebalance_freqs: None,
            top_ns: None,
            unit_costs: None,
            commission: None,
            slippage: None,
            stamp_tax_sell: None,
            risk: None,
            manual_override: None,
            execution_input: None,
            research: None,
            output_dir: "output/test".to_string(),
        };

        let err = RotationStrategySpec::from_app_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("rebalance_freq 必须大于 0"));
    }

    #[test]
    fn ma_timing_single_rejects_fast_not_less_than_slow() {
        let cfg = AppConfig {
            experiment_name: "test".to_string(),
            strategy: "ma_timing_single".to_string(),
            data_file: None,
            asset_files: None,
            compare_configs: None,
            source_config: None,
            benchmark_asset: Some("hs300".to_string()),
            risk_assets: None,
            defensive_asset: None,
            fast: Some(20),
            slow: Some(20),
            lookback: None,
            rebalance_freq: Some(20),
            top_n: None,
            absolute_momentum_floor: None,
            lookbacks: None,
            rebalance_freqs: None,
            top_ns: None,
            unit_costs: None,
            commission: None,
            slippage: None,
            stamp_tax_sell: None,
            risk: None,
            manual_override: None,
            execution_input: None,
            research: None,
            output_dir: "output/test".to_string(),
        };

        let err = RotationStrategySpec::from_app_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("fast < slow"));
    }
}
