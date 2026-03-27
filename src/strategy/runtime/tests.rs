use super::{is_processed_rotation_strategy, RotationStrategySpec};
use crate::config::AppConfig;

fn base_test_config(strategy: &str) -> AppConfig {
    AppConfig {
        experiment_name: "test".to_string(),
        strategy: strategy.to_string(),
        data_file: None,
        asset_files: None,
        compare_configs: None,
        source_config: None,
        benchmark_asset: None,
        risk_assets: None,
        defensive_asset: None,
        fast: None,
        slow: None,
        lookback: None,
        rebalance_freq: None,
        top_n: None,
        absolute_momentum_floor: None,
        target_volatility: None,
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
    }
}

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
    let mut cfg = base_test_config("momentum_topn");
    cfg.lookback = Some(20);
    cfg.rebalance_freq = Some(0);
    cfg.top_n = Some(2);

    let err = RotationStrategySpec::from_app_config(&cfg).unwrap_err();
    assert!(err.to_string().contains("rebalance_freq 必须大于 0"));
}

#[test]
fn ma_timing_single_rejects_fast_not_less_than_slow() {
    let mut cfg = base_test_config("ma_timing_single");
    cfg.benchmark_asset = Some("hs300".to_string());
    cfg.fast = Some(20);
    cfg.slow = Some(20);
    cfg.rebalance_freq = Some(20);

    let err = RotationStrategySpec::from_app_config(&cfg).unwrap_err();
    assert!(err.to_string().contains("fast < slow"));
}

#[test]
fn processed_rotation_strategy_whitelist_is_explicit() {
    assert!(is_processed_rotation_strategy("momentum_topn"));
    assert!(is_processed_rotation_strategy("breakout_timing_single"));
    assert!(is_processed_rotation_strategy("breakdown_timing_single"));
    assert!(is_processed_rotation_strategy("defensive_pair_rotation"));
    assert!(is_processed_rotation_strategy("adaptive_dual_momentum"));
    assert!(is_processed_rotation_strategy("volatility_target_rotation"));
    assert!(!is_processed_rotation_strategy("ma_single"));
    assert!(!is_processed_rotation_strategy("strategy_compare"));
}

#[test]
fn breakdown_timing_single_parses_required_fields() {
    let mut cfg = base_test_config("breakdown_timing_single");
    cfg.benchmark_asset = Some("hs300".to_string());
    cfg.defensive_asset = Some("dividend".to_string());
    cfg.lookback = Some(20);
    cfg.rebalance_freq = Some(5);

    let spec = RotationStrategySpec::from_app_config(&cfg).unwrap();
    assert_eq!(spec.required_lookback(), 20);
}

#[test]
fn defensive_pair_rotation_parses_required_fields() {
    let mut cfg = base_test_config("defensive_pair_rotation");
    cfg.benchmark_asset = Some("dividend".to_string());
    cfg.defensive_asset = Some("bond".to_string());
    cfg.lookback = Some(20);
    cfg.rebalance_freq = Some(5);

    let spec = RotationStrategySpec::from_app_config(&cfg).unwrap();
    assert_eq!(spec.required_lookback(), 20);
    assert_eq!(spec.required_assets(), vec!["dividend", "bond"]);
}

#[test]
fn adaptive_dual_momentum_parses_required_fields() {
    let mut cfg = base_test_config("adaptive_dual_momentum");
    cfg.defensive_asset = Some("dividend".to_string());
    cfg.lookback = Some(20);
    cfg.rebalance_freq = Some(5);
    cfg.top_n = Some(2);
    cfg.absolute_momentum_floor = Some(0.0);

    let spec = RotationStrategySpec::from_app_config(&cfg).unwrap();
    assert_eq!(spec.required_lookback(), 20);
    assert_eq!(spec.required_assets(), vec!["dividend"]);
}

#[test]
fn volatility_target_rotation_parses_required_fields() {
    let mut cfg = base_test_config("volatility_target_rotation");
    cfg.defensive_asset = Some("dividend".to_string());
    cfg.lookback = Some(20);
    cfg.rebalance_freq = Some(5);
    cfg.top_n = Some(2);
    cfg.target_volatility = Some(0.02);

    let spec = RotationStrategySpec::from_app_config(&cfg).unwrap();
    assert_eq!(spec.required_lookback(), 20);
    assert_eq!(spec.required_assets(), vec!["dividend"]);
}

#[test]
fn volatility_target_rotation_rejects_non_positive_target_volatility() {
    let mut cfg = base_test_config("volatility_target_rotation");
    cfg.lookback = Some(20);
    cfg.rebalance_freq = Some(5);
    cfg.top_n = Some(2);
    cfg.target_volatility = Some(0.0);

    let err = RotationStrategySpec::from_app_config(&cfg).unwrap_err();
    assert!(err.to_string().contains("target_volatility 必须大于 0"));
}
