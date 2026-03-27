use crate::config::{AppConfig, RiskConfig};
use crate::data::Bar;
use crate::engine::backtest::{
    run_buy_hold_equal_weight_backtest, run_buy_hold_single_backtest, run_dual_momentum_backtest,
    run_momentum_topn_backtest, run_risk_off_rotation_backtest, MomentumTopNResult,
};
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
    BuyHoldSingle {
        benchmark_asset: String,
    },
    BuyHoldEqualWeight,
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
}

impl RotationStrategySpec {
    pub fn from_app_config(cfg: &AppConfig) -> Result<Self> {
        match cfg.strategy.as_str() {
            "momentum_topn" => Ok(Self::MomentumTopN {
                lookback: cfg
                    .lookback
                    .ok_or_else(|| anyhow!("momentum_topn 需要提供 lookback"))?,
                rebalance_freq: cfg
                    .rebalance_freq
                    .ok_or_else(|| anyhow!("momentum_topn 需要提供 rebalance_freq"))?,
                top_n: cfg
                    .top_n
                    .ok_or_else(|| anyhow!("momentum_topn 需要提供 top_n"))?,
            }),
            "buy_hold_single" => Ok(Self::BuyHoldSingle {
                benchmark_asset: cfg
                    .benchmark_asset
                    .clone()
                    .ok_or_else(|| anyhow!("buy_hold_single 需要提供 benchmark_asset"))?,
            }),
            "buy_hold_equal_weight" => Ok(Self::BuyHoldEqualWeight),
            "dual_momentum" => Ok(Self::DualMomentum {
                lookback: cfg
                    .lookback
                    .ok_or_else(|| anyhow!("dual_momentum 需要提供 lookback"))?,
                rebalance_freq: cfg
                    .rebalance_freq
                    .ok_or_else(|| anyhow!("dual_momentum 需要提供 rebalance_freq"))?,
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
                let rebalance_freq = cfg
                    .rebalance_freq
                    .ok_or_else(|| anyhow!("risk_off_rotation 需要提供 rebalance_freq"))?;
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
            other => Err(anyhow!("不支持的轮动策略：{}", other)),
        }
    }

    pub fn required_lookback(&self) -> usize {
        match self {
            Self::MomentumTopN { lookback, .. } => *lookback,
            Self::BuyHoldSingle { .. } => 0,
            Self::BuyHoldEqualWeight => 0,
            Self::DualMomentum { lookback, .. } => *lookback,
            Self::RiskOffRotation { lookback, .. } => *lookback,
        }
    }

    pub fn summary_title(&self) -> &'static str {
        match self {
            Self::MomentumTopN { .. } => "动量轮动摘要",
            Self::BuyHoldSingle { .. } => "Buy & Hold 单资产摘要",
            Self::BuyHoldEqualWeight => "Buy & Hold 等权摘要",
            Self::DualMomentum { .. } => "双动量摘要",
            Self::RiskOffRotation { .. } => "风险开关轮动摘要",
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
            Self::BuyHoldSingle { benchmark_asset } => {
                vec![("benchmark_asset".to_string(), benchmark_asset.clone())]
            }
            Self::BuyHoldEqualWeight => {
                vec![("weight_mode".to_string(), "equal_weight".to_string())]
            }
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
        }
    }

    pub fn required_assets(&self) -> Vec<&str> {
        match self {
            Self::BuyHoldSingle { benchmark_asset } => vec![benchmark_asset.as_str()],
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
        }
    }
}
