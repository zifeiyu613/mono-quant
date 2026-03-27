use super::required_asset_count_for_max_weight;
use crate::config::RiskConfig;
use crate::report::{RebalanceRow, RiskEventRow};
use chrono::NaiveDate;

#[derive(Debug, Clone, Default)]
pub(super) struct RiskState {
    pub(super) halted_by_risk: bool,
    pub(super) permanently_stopped: bool,
    pub(super) halt_reason: Option<String>,
    pub(super) cooldown_until_index: Option<usize>,
}

pub(super) fn apply_asset_universe_guard(
    asset_count: usize,
    lookback: usize,
    dates: &[NaiveDate],
    risk: Option<&RiskConfig>,
    risk_events: &mut Vec<RiskEventRow>,
    state: &mut RiskState,
) {
    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = required_asset_count_for_max_weight(max_weight);
        if asset_count < required_assets {
            let reason = format!(
                "资产池数量 {} 低于单资产权重上限 {:.2}% 所需的最少资产数 {}，组合已停止运行",
                asset_count,
                max_weight * 100.0,
                required_assets
            );
            risk_events.push(RiskEventRow {
                date: dates
                    .get(lookback)
                    .or_else(|| dates.first())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "N/A".to_string()),
                event_type: "asset_universe_stop".to_string(),
                detail: reason.clone(),
            });
            state.halt_reason = Some(reason);
            state.halted_by_risk = true;
        }
    }
}

pub(super) fn handle_cooldown_transition(
    index: usize,
    date: NaiveDate,
    risk_events: &mut Vec<RiskEventRow>,
    state: &mut RiskState,
) -> bool {
    if let Some(until_index) = state.cooldown_until_index {
        if index < until_index {
            return true;
        }
        risk_events.push(RiskEventRow {
            date: date.to_string(),
            event_type: "cooldown_recovery".to_string(),
            detail: "风控冷静期结束，组合恢复为可调仓状态，后续将在下一次调仓点重新入场"
                .to_string(),
        });
        state.cooldown_until_index = None;
        state.halted_by_risk = false;
        state.halt_reason = None;
    }
    false
}

pub(super) fn guard_selection_count(
    date: NaiveDate,
    selected_len: usize,
    total_before: f64,
    risk: Option<&RiskConfig>,
    rebalance_rows: &mut Vec<RebalanceRow>,
    risk_events: &mut Vec<RiskEventRow>,
) -> bool {
    if selected_len == 0 {
        return false;
    }
    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = required_asset_count_for_max_weight(max_weight);
        if selected_len < required_assets {
            risk_events.push(RiskEventRow {
                date: date.to_string(),
                event_type: "selection_guard".to_string(),
                detail: format!(
                    "本次选中资产数量 {} 小于单资产权重上限 {:.2}% 要求的最少资产数 {}，已跳过调仓",
                    selected_len,
                    max_weight * 100.0,
                    required_assets
                ),
            });
            rebalance_rows.push(RebalanceRow {
                date: date.to_string(),
                selected_assets: "SKIPPED_BY_RISK".to_string(),
                turnover_amount: 0.0,
                cost: 0.0,
                equity_before: total_before,
                equity_after: total_before,
            });
            return true;
        }
    }
    false
}

pub(super) fn maybe_trigger_daily_loss_stop(
    date: NaiveDate,
    daily_return: f64,
    index: usize,
    cooldown_days: Option<usize>,
    risk: Option<&RiskConfig>,
    risk_events: &mut Vec<RiskEventRow>,
    state: &mut RiskState,
) -> bool {
    if let Some(limit) = risk.and_then(|cfg| cfg.max_daily_loss_limit) {
        if daily_return <= -limit {
            let mut reason = format!(
                "单日组合收益 {:.2}% 触发单日亏损上限 {:.2}%",
                daily_return * 100.0,
                limit * 100.0
            );
            if let Some(days) = cooldown_days {
                reason.push_str(&format!("，组合已切换为空仓并进入 {} 个交易日冷静期", days));
                state.cooldown_until_index = Some(index + days + 1);
            } else {
                reason.push_str("，组合已切换为空仓");
                state.permanently_stopped = true;
            }
            state.halt_reason = Some(reason.clone());
            state.halted_by_risk = true;
            risk_events.push(RiskEventRow {
                date: date.to_string(),
                event_type: "daily_loss_stop".to_string(),
                detail: reason,
            });
            return true;
        }
    }
    false
}

pub(super) fn maybe_trigger_drawdown_stop(
    date: NaiveDate,
    current_drawdown: f64,
    index: usize,
    cooldown_days: Option<usize>,
    risk: Option<&RiskConfig>,
    risk_events: &mut Vec<RiskEventRow>,
    state: &mut RiskState,
) -> bool {
    if let Some(limit) = risk.and_then(|cfg| cfg.max_drawdown_limit) {
        if current_drawdown <= -limit {
            let mut reason = format!(
                "当前回撤 {:.2}% 触发最大回撤上限 {:.2}%",
                current_drawdown * 100.0,
                limit * 100.0
            );
            if let Some(days) = cooldown_days {
                reason.push_str(&format!("，组合已切换为空仓并进入 {} 个交易日冷静期", days));
                state.cooldown_until_index = Some(index + days + 1);
            } else {
                reason.push_str("，组合已切换为空仓");
                state.permanently_stopped = true;
            }
            state.halt_reason = Some(reason.clone());
            state.halted_by_risk = true;
            risk_events.push(RiskEventRow {
                date: date.to_string(),
                event_type: "drawdown_stop".to_string(),
                detail: reason,
            });
            return true;
        }
    }
    false
}
