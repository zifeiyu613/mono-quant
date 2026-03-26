use anyhow::Context;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct SampleSplitConfig {
    pub mode: String,
    pub split_date: Option<String>,
    pub in_sample_ratio: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WalkForwardConfig {
    pub train_ratio: f64,
    pub test_ratio: f64,
    pub min_train_rows: Option<usize>,
    pub min_test_rows: Option<usize>,
    pub max_windows: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DecisionOverrideConfig {
    pub final_state: String,
    pub recommended_action: Option<String>,
    pub reason: String,
    pub owner: Option<String>,
    pub decided_at: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ResearchConfig {
    pub topic: String,
    pub round: String,
    pub objective: Option<String>,
    pub sample_split: Option<SampleSplitConfig>,
    pub walk_forward: Option<WalkForwardConfig>,
    pub decision_override: Option<DecisionOverrideConfig>,
    #[serde(default)]
    pub hypotheses: Vec<HypothesisConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HypothesisConfig {
    pub id: String,
    pub statement: String,
    pub rule: String,
    pub preferred_max_lookback: Option<usize>,
    pub preferred_min_top_n: Option<usize>,
    pub preferred_min_rebalance_freq: Option<usize>,
    pub min_return_delta: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RiskConfig {
    pub min_aligned_days: Option<usize>,
    pub max_single_asset_weight: Option<f64>,
    pub max_daily_loss_limit: Option<f64>,
    pub max_drawdown_limit: Option<f64>,
    pub max_rebalance_turnover: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub experiment_name: String,
    #[serde(default = "default_strategy")]
    pub strategy: String,
    pub data_file: Option<String>,
    pub asset_files: Option<HashMap<String, String>>,
    pub fast: Option<usize>,
    pub slow: Option<usize>,
    pub lookback: Option<usize>,
    pub rebalance_freq: Option<usize>,
    pub top_n: Option<usize>,
    pub lookbacks: Option<Vec<usize>>,
    pub rebalance_freqs: Option<Vec<usize>>,
    pub top_ns: Option<Vec<usize>>,
    pub unit_costs: Option<Vec<f64>>,
    pub commission: Option<f64>,
    pub slippage: Option<f64>,
    pub stamp_tax_sell: Option<f64>,
    pub risk: Option<RiskConfig>,
    pub research: Option<ResearchConfig>,
    pub output_dir: String,
}

fn default_strategy() -> String {
    "ma_single".to_string()
}

/// 读取并解析 JSON 配置文件，返回强类型的 `AppConfig`。
pub fn load_config(path: &str) -> anyhow::Result<AppConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("读取配置文件失败：{}", path))?;
    let cfg: AppConfig = serde_json::from_str(&content)
        .with_context(|| format!("解析 JSON 配置失败：{}", path))?;
    Ok(cfg)
}
