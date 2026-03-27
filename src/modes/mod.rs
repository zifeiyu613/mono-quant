use super::*;

mod batch;
mod compare;
mod daily_signal;

pub(super) fn run_momentum_batch(cfg: &config::AppConfig, config_path: &str) -> anyhow::Result<()> {
    batch::run_momentum_batch(cfg, config_path)
}

pub(super) fn run_strategy_compare(
    compare_cfg: &config::AppConfig,
    compare_config_path: &str,
) -> anyhow::Result<()> {
    compare::run_strategy_compare(compare_cfg, compare_config_path)
}

pub(super) fn run_daily_signal(
    daily_cfg: &config::AppConfig,
    daily_config_path: &str,
) -> anyhow::Result<()> {
    daily_signal::run_daily_signal(daily_cfg, daily_config_path)
}
