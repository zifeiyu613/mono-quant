use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

fn realized_volatility(
    bars: &HashMap<NaiveDate, Bar>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
) -> Option<f64> {
    if lookback == 0 {
        return None;
    }

    let mut mean = 0.0;
    let mut m2 = 0.0;
    let mut count = 0usize;
    for idx in (i - lookback + 1)..=i {
        let today = bars.get(&dates[idx])?.close;
        let prev = bars.get(&dates[idx - 1])?.close;
        let ret = today / prev - 1.0;

        count += 1;
        let delta = ret - mean;
        mean += delta / count as f64;
        let delta2 = ret - mean;
        m2 += delta * delta2;
    }

    if count == 0 {
        return None;
    }
    Some((m2 / count as f64).sqrt())
}

/// 波动目标轮动：先按动量筛选风险资产，再按“目标波动 / 实际波动”收缩风险资产数量。
pub fn select_volatility_target_rotation_assets(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    top_n: usize,
    target_volatility: f64,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let defensive_name = defensive_asset.unwrap_or_default();
    let mut ranking: Vec<(String, f64)> = asset_maps
        .iter()
        .filter(|(name, _)| name.as_str() != defensive_name)
        .map(|(name, bars)| {
            let now = bars.get(&dates[i]).unwrap().close;
            let past = bars.get(&dates[i - lookback]).unwrap().close;
            let ret = now / past - 1.0;
            (name.clone(), ret)
        })
        .collect();
    ranking.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    if ranking.is_empty() {
        if let Some(defensive) = defensive_asset {
            if asset_maps.contains_key(defensive) {
                return vec![defensive.to_string()];
            }
        }
        return Vec::new();
    }

    let selected_count = top_n.max(1).min(ranking.len());
    let risk_candidates: Vec<String> = ranking
        .iter()
        .take(selected_count)
        .map(|item| item.0.clone())
        .collect();

    let mut vol_sum = 0.0;
    let mut vol_count = 0usize;
    for name in &risk_candidates {
        let Some(bars) = asset_maps.get(name) else {
            continue;
        };
        if let Some(vol) = realized_volatility(bars, dates, i, lookback) {
            vol_sum += vol;
            vol_count += 1;
        }
    }
    if vol_count == 0 {
        return risk_candidates;
    }

    let portfolio_volatility = vol_sum / vol_count as f64;
    if portfolio_volatility <= target_volatility {
        return risk_candidates;
    }

    let ratio = (target_volatility / portfolio_volatility).clamp(0.0, 1.0);
    let mut risk_count = ((selected_count as f64) * ratio).floor() as usize;

    let has_defensive = defensive_asset
        .map(|name| asset_maps.contains_key(name))
        .unwrap_or(false);

    if !has_defensive && risk_count == 0 {
        risk_count = 1;
    }

    if risk_count == 0 {
        if let Some(defensive) = defensive_asset {
            return vec![defensive.to_string()];
        }
        return Vec::new();
    }

    let mut selected: Vec<String> = ranking.into_iter().take(risk_count).map(|item| item.0).collect();
    if let Some(defensive) = defensive_asset {
        if has_defensive && !selected.iter().any(|name| name == defensive) {
            selected.push(defensive.to_string());
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(date: &str, close: f64) -> Bar {
        Bar {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            open: close,
            close,
        }
    }

    fn sample_dates() -> Vec<NaiveDate> {
        vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-04", "%Y-%m-%d").unwrap(),
        ]
    }

    #[test]
    fn keeps_risk_assets_when_volatility_below_target() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 101.0),
                bar("2024-01-03", 102.0),
                bar("2024-01-04", 103.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "b".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.8),
                bar("2024-01-03", 101.5),
                bar("2024-01-04", 102.2),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.2),
                bar("2024-01-03", 100.4),
                bar("2024-01-04", 100.6),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected = select_volatility_target_rotation_assets(
            &maps,
            &dates,
            3,
            3,
            2,
            0.05,
            Some("dividend"),
        );

        assert_eq!(selected, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn adds_defensive_when_volatility_above_target() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 110.0),
                bar("2024-01-03", 90.0),
                bar("2024-01-04", 120.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "b".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 104.0),
                bar("2024-01-03", 95.0),
                bar("2024-01-04", 109.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.2),
                bar("2024-01-03", 100.1),
                bar("2024-01-04", 100.4),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected = select_volatility_target_rotation_assets(
            &maps,
            &dates,
            3,
            3,
            2,
            0.10,
            Some("dividend"),
        );

        assert_eq!(
            selected,
            vec!["a".to_string(), "dividend".to_string()]
        );
    }
}
