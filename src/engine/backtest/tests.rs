use super::*;
use chrono::NaiveDate;

fn bar(date: &str, close: f64) -> Bar {
    Bar {
        date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
        open: close,
        close,
    }
}

fn sample_asset_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
    let mut maps = HashMap::new();
    let asset_a = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 110.0),
        bar("2024-01-03", 90.0),
        bar("2024-01-04", 80.0),
        bar("2024-01-05", 70.0),
    ];
    let asset_b = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 101.0),
        bar("2024-01-03", 102.0),
        bar("2024-01-04", 103.0),
        bar("2024-01-05", 104.0),
    ];
    let asset_c = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 100.5),
        bar("2024-01-03", 101.0),
        bar("2024-01-04", 101.5),
        bar("2024-01-05", 102.0),
    ];
    maps.insert(
        "a".to_string(),
        asset_a.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps.insert(
        "b".to_string(),
        asset_b.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps.insert(
        "c".to_string(),
        asset_c.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps
}

fn sample_two_asset_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
    let mut maps = HashMap::new();
    let asset_a = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 102.0),
        bar("2024-01-03", 104.0),
        bar("2024-01-04", 106.0),
        bar("2024-01-05", 108.0),
    ];
    let asset_b = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 101.0),
        bar("2024-01-03", 102.0),
        bar("2024-01-04", 103.0),
        bar("2024-01-05", 104.0),
    ];
    maps.insert(
        "a".to_string(),
        asset_a.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps.insert(
        "b".to_string(),
        asset_b.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps
}

fn sample_cooldown_asset_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
    let mut maps = HashMap::new();
    let asset_a = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 110.0),
        bar("2024-01-03", 90.0),
        bar("2024-01-04", 92.0),
        bar("2024-01-05", 94.0),
        bar("2024-01-08", 120.0),
        bar("2024-01-09", 121.0),
        bar("2024-01-10", 122.0),
    ];
    let asset_b = vec![
        bar("2024-01-01", 100.0),
        bar("2024-01-02", 101.0),
        bar("2024-01-03", 102.0),
        bar("2024-01-04", 103.0),
        bar("2024-01-05", 104.0),
        bar("2024-01-08", 105.0),
        bar("2024-01-09", 106.0),
        bar("2024-01-10", 107.0),
    ];
    maps.insert(
        "a".to_string(),
        asset_a.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps.insert(
        "b".to_string(),
        asset_b.into_iter().map(|item| (item.date, item)).collect(),
    );
    maps
}

#[test]
fn drawdown_guard_halts_strategy() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: None,
        max_daily_loss_limit: None,
        max_drawdown_limit: Some(0.05),
        max_rebalance_turnover: None,
        stop_cooldown_days: None,
    };

    let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

    assert!(result.summary.halted_by_risk);
    assert!(result
        .risk_events
        .iter()
        .any(|event| event.event_type == "drawdown_stop"));
}

#[test]
fn turnover_guard_skips_rebalance() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: None,
        max_daily_loss_limit: None,
        max_drawdown_limit: None,
        max_rebalance_turnover: Some(0.0),
        stop_cooldown_days: None,
    };

    let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

    assert!(result
        .risk_events
        .iter()
        .any(|event| event.event_type == "turnover_guard"));
    assert!(result
        .rebalances
        .iter()
        .any(|row| row.selected_assets == "SKIPPED_BY_RISK"));
}

#[test]
fn max_single_asset_weight_expands_selection_count() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: Some(0.4),
        max_daily_loss_limit: None,
        max_drawdown_limit: None,
        max_rebalance_turnover: None,
        stop_cooldown_days: None,
    };

    let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

    assert!(result
        .rebalances
        .iter()
        .any(|row| row.selected_assets.split('|').count() >= 3));
}

#[test]
fn daily_loss_guard_halts_strategy() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: None,
        max_daily_loss_limit: Some(0.1),
        max_drawdown_limit: None,
        max_rebalance_turnover: None,
        stop_cooldown_days: None,
    };

    let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

    assert!(result.summary.halted_by_risk);
    assert!(result
        .risk_events
        .iter()
        .any(|event| event.event_type == "daily_loss_stop"));
}

#[test]
fn asset_universe_guard_halts_when_weight_cap_cannot_be_satisfied() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: Some(0.4),
        max_daily_loss_limit: None,
        max_drawdown_limit: None,
        max_rebalance_turnover: None,
        stop_cooldown_days: None,
    };

    let result =
        run_momentum_topn_backtest(&sample_two_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

    assert!(result.summary.halted_by_risk);
    assert!(result
        .risk_events
        .iter()
        .any(|event| event.event_type == "asset_universe_stop"));
}

#[test]
fn cooldown_recovery_allows_reentry_after_stop() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: None,
        max_daily_loss_limit: Some(0.1),
        max_drawdown_limit: None,
        max_rebalance_turnover: None,
        stop_cooldown_days: Some(2),
    };

    let result = run_momentum_topn_backtest(
        &sample_cooldown_asset_maps(),
        1,
        1,
        1,
        0.0,
        0.0,
        Some(&risk),
    );

    assert!(!result.summary.halted_by_risk);
    assert!(result
        .risk_events
        .iter()
        .any(|event| event.event_type == "daily_loss_stop"));
    assert!(result
        .risk_events
        .iter()
        .any(|event| event.event_type == "cooldown_recovery"));
    assert!(result.rebalances.len() >= 2);
}

#[test]
fn buy_hold_equal_weight_builds_initial_equal_portfolio() {
    let result = run_buy_hold_equal_weight_backtest(&sample_asset_maps(), 0.0, 0.0, None);
    assert!(!result.rebalances.is_empty());
    let first = &result.rebalances[0];
    assert!(first.selected_assets.contains("a"));
    assert!(first.selected_assets.contains("b"));
    assert!(first.selected_assets.contains("c"));
}

#[test]
fn dual_momentum_falls_back_to_defensive_asset() {
    let result = run_dual_momentum_backtest(
        &sample_asset_maps(),
        RotationBacktestConfig {
            lookback: 1,
            rebalance_freq: 1,
            commission: 0.0,
            slippage: 0.0,
            risk: None,
        },
        1,
        0.2,
        Some("b"),
    );

    assert!(!result.rebalances.is_empty());
    assert!(result
        .rebalances
        .iter()
        .any(|row| row.selected_assets == "b"));
}

#[test]
fn skipped_rebalance_still_records_each_equity_point() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: None,
        max_daily_loss_limit: None,
        max_drawdown_limit: None,
        max_rebalance_turnover: Some(0.0),
        stop_cooldown_days: None,
    };

    let result = run_momentum_topn_backtest(&sample_asset_maps(), 1, 1, 1, 0.0, 0.0, Some(&risk));

    assert_eq!(result.equity_curve.len(), 4);
    assert_eq!(
        result.equity_curve.last().map(|(date, _)| *date),
        Some(NaiveDate::parse_from_str("2024-01-05", "%Y-%m-%d").unwrap())
    );
    assert!(result
        .equity_curve
        .iter()
        .all(|(_, equity)| (*equity - 1.0).abs() < 1e-12));
}

#[test]
fn zero_turnover_rebalance_does_not_increase_trade_count() {
    let result = run_momentum_topn_backtest(&sample_two_asset_maps(), 1, 1, 1, 0.0, 0.0, None);

    assert_eq!(result.summary.trade_count, 1);
}

#[test]
fn cooldown_recovery_clears_terminal_halt_reason() {
    let risk = RiskConfig {
        min_aligned_days: None,
        max_single_asset_weight: None,
        max_daily_loss_limit: Some(0.1),
        max_drawdown_limit: None,
        max_rebalance_turnover: None,
        stop_cooldown_days: Some(2),
    };

    let result = run_momentum_topn_backtest(
        &sample_cooldown_asset_maps(),
        1,
        1,
        1,
        0.0,
        0.0,
        Some(&risk),
    );

    assert!(!result.summary.halted_by_risk);
    assert!(result.summary.halt_reason.is_none());
}
