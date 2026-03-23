use anyhow::Context;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

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
    pub output_dir: String,
}

fn default_strategy() -> String {
    "ma_single".to_string()
}

/// Load and parse the JSON config file into a strongly typed AppConfig.
pub fn load_config(path: &str) -> anyhow::Result<AppConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path))?;
    let cfg: AppConfig = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse json config: {}", path))?;
    Ok(cfg)
}
