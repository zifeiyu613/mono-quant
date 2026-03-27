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
    LowVolatilityTopN {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
        defensive_asset: Option<String>,
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
    AdaptiveDualMomentum {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
        absolute_momentum_floor: f64,
        defensive_asset: Option<String>,
    },
    VolatilityTargetRotation {
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
        target_volatility: f64,
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
    MaRotationTopN {
        fast: usize,
        slow: usize,
        lookback: usize,
        rebalance_freq: usize,
        top_n: usize,
        defensive_asset: Option<String>,
    },
    BreakoutTimingSingle {
        benchmark_asset: String,
        lookback: usize,
        rebalance_freq: usize,
        defensive_asset: Option<String>,
    },
    BreakdownTimingSingle {
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
    DefensivePairRotation {
        benchmark_asset: String,
        defensive_asset: String,
        lookback: usize,
        rebalance_freq: usize,
    },
}

pub fn is_processed_rotation_strategy(strategy: &str) -> bool {
    matches!(
        strategy,
        "buy_hold_single"
            | "buy_hold_equal_weight"
            | "absolute_momentum_breadth"
            | "absolute_momentum_single"
            | "low_volatility_topn"
            | "volatility_adjusted_momentum"
            | "reversal_bottomn"
            | "momentum_topn"
            | "dual_momentum"
            | "adaptive_dual_momentum"
            | "volatility_target_rotation"
            | "risk_off_rotation"
            | "ma_timing_single"
            | "ma_rotation_topn"
            | "breakout_rotation_topn"
            | "relative_strength_pair"
            | "defensive_pair_rotation"
            | "breakout_timing_single"
            | "breakdown_timing_single"
    )
}

mod execute;
mod parse;

impl RotationStrategySpec {
    pub fn required_lookback(&self) -> usize {
        match self {
            Self::MomentumTopN { lookback, .. } => *lookback,
            Self::VolatilityAdjustedMomentum { lookback, .. } => *lookback,
            Self::LowVolatilityTopN { lookback, .. } => *lookback,
            Self::ReversalBottomN { lookback, .. } => *lookback,
            Self::BuyHoldSingle { .. } => 0,
            Self::BuyHoldEqualWeight => 0,
            Self::AbsoluteMomentumBreadth { lookback, .. } => *lookback,
            Self::AbsoluteMomentumSingle { lookback, .. } => *lookback,
            Self::DualMomentum { lookback, .. } => *lookback,
            Self::AdaptiveDualMomentum { lookback, .. } => *lookback,
            Self::VolatilityTargetRotation { lookback, .. } => *lookback,
            Self::RiskOffRotation { lookback, .. } => *lookback,
            Self::MaTimingSingle { slow, .. } => slow.saturating_sub(1),
            Self::MaRotationTopN { slow, lookback, .. } => slow.saturating_sub(1).max(*lookback),
            Self::BreakoutTimingSingle { lookback, .. } => *lookback,
            Self::BreakdownTimingSingle { lookback, .. } => *lookback,
            Self::BreakoutRotationTopN { lookback, .. } => *lookback,
            Self::RelativeStrengthPair { lookback, .. } => *lookback,
            Self::DefensivePairRotation { lookback, .. } => *lookback,
        }
    }

    pub fn summary_title(&self) -> &'static str {
        match self {
            Self::MomentumTopN { .. } => "动量轮动摘要",
            Self::VolatilityAdjustedMomentum { .. } => "波动调整动量摘要",
            Self::LowVolatilityTopN { .. } => "低波动 TopN 摘要",
            Self::ReversalBottomN { .. } => "反转 BottomN 摘要",
            Self::BuyHoldSingle { .. } => "Buy & Hold 单资产摘要",
            Self::BuyHoldEqualWeight => "Buy & Hold 等权摘要",
            Self::AbsoluteMomentumBreadth { .. } => "多资产绝对动量广度摘要",
            Self::AbsoluteMomentumSingle { .. } => "单资产绝对动量开关摘要",
            Self::DualMomentum { .. } => "双动量摘要",
            Self::AdaptiveDualMomentum { .. } => "自适应双动量摘要",
            Self::VolatilityTargetRotation { .. } => "波动目标轮动摘要",
            Self::RiskOffRotation { .. } => "风险开关轮动摘要",
            Self::MaTimingSingle { .. } => "单资产均线择时摘要",
            Self::MaRotationTopN { .. } => "均线过滤 TopN 摘要",
            Self::BreakoutTimingSingle { .. } => "单资产突破择时摘要",
            Self::BreakdownTimingSingle { .. } => "单资产跌破择时摘要",
            Self::BreakoutRotationTopN { .. } => "多资产突破轮动摘要",
            Self::RelativeStrengthPair { .. } => "双资产相对强弱切换摘要",
            Self::DefensivePairRotation { .. } => "防守资产对轮动摘要",
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
                (
                    "ranking_mode".to_string(),
                    "return_over_volatility".to_string(),
                ),
            ],
            Self::LowVolatilityTopN {
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
                        .unwrap_or_else(|| "未配置".to_string()),
                ),
                ("ranking_mode".to_string(), "lowest_volatility".to_string()),
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
            Self::AdaptiveDualMomentum {
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
                (
                    "adaptive_mode".to_string(),
                    "breadth_tier_topn_and_floor".to_string(),
                ),
            ],
            Self::VolatilityTargetRotation {
                lookback,
                rebalance_freq,
                top_n,
                target_volatility,
                defensive_asset,
            } => vec![
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
                (
                    "target_volatility".to_string(),
                    format!("{:.4}", target_volatility),
                ),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "未配置".to_string()),
                ),
                (
                    "selection_mode".to_string(),
                    "vol_target_dynamic_risk_count".to_string(),
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
            Self::MaRotationTopN {
                fast,
                slow,
                lookback,
                rebalance_freq,
                top_n,
                defensive_asset,
            } => vec![
                ("fast".to_string(), fast.to_string()),
                ("slow".to_string(), slow.to_string()),
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                ("top_n".to_string(), top_n.to_string()),
                (
                    "defensive_asset".to_string(),
                    defensive_asset
                        .clone()
                        .unwrap_or_else(|| "空仓".to_string()),
                ),
                (
                    "selection_mode".to_string(),
                    "ma_filter_then_topn".to_string(),
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
            Self::BreakdownTimingSingle {
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
                (
                    "selection_mode".to_string(),
                    "breakout_then_rank".to_string(),
                ),
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
            Self::DefensivePairRotation {
                benchmark_asset,
                defensive_asset,
                lookback,
                rebalance_freq,
            } => vec![
                (
                    "primary_defensive_asset".to_string(),
                    benchmark_asset.clone(),
                ),
                (
                    "secondary_defensive_asset".to_string(),
                    defensive_asset.clone(),
                ),
                ("lookback".to_string(), lookback.to_string()),
                ("rebalance_freq".to_string(), rebalance_freq.to_string()),
                (
                    "selection_mode".to_string(),
                    "defensive_pair_strength".to_string(),
                ),
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
            Self::LowVolatilityTopN {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::RelativeStrengthPair {
                benchmark_asset,
                defensive_asset,
                ..
            }
            | Self::DefensivePairRotation {
                benchmark_asset,
                defensive_asset,
                ..
            } => vec![benchmark_asset.as_str(), defensive_asset.as_str()],
            Self::BreakoutRotationTopN {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::MaRotationTopN {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::BreakoutTimingSingle {
                benchmark_asset,
                defensive_asset,
                ..
            }
            | Self::BreakdownTimingSingle {
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
            }
            | Self::AdaptiveDualMomentum {
                defensive_asset: Some(defensive_asset),
                ..
            }
            | Self::VolatilityTargetRotation {
                defensive_asset: Some(defensive_asset),
                ..
            } => vec![defensive_asset.as_str()],
            Self::RiskOffRotation {
                risk_assets,
                defensive_asset,
                ..
            } => {
                let mut required = risk_assets
                    .iter()
                    .map(|asset| asset.as_str())
                    .collect::<Vec<_>>();
                required.push(defensive_asset.as_str());
                required
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests;
