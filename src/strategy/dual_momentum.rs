use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 双动量选股：先做相对动量排序，再做绝对动量过滤。
pub fn select_dual_momentum_assets(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    top_n: usize,
    absolute_momentum_floor: f64,
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

    let selected: Vec<String> = ranking
        .into_iter()
        .filter(|(_, ret)| *ret >= absolute_momentum_floor)
        .take(top_n.max(1))
        .map(|(name, _)| name)
        .collect();

    if selected.is_empty() {
        if let Some(defensive) = defensive_asset {
            return vec![defensive.to_string()];
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

    fn sample_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
        let mut maps = HashMap::new();
        let risk_a = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 106.0)];
        let risk_b = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 103.0)];
        let dividend = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 110.0)];
        maps.insert(
            "risk_a".to_string(),
            risk_a.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "risk_b".to_string(),
            risk_b.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "dividend".to_string(),
            dividend.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps
    }

    #[test]
    fn defensive_asset_is_only_used_as_fallback() {
        let maps = sample_maps();
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];

        let selected = select_dual_momentum_assets(&maps, &dates, 1, 1, 1, 0.0, Some("dividend"));

        assert_eq!(selected, vec!["risk_a".to_string()]);
    }
}
