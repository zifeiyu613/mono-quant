use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

fn adaptive_targets(
    configured_top_n: usize,
    base_floor: f64,
    breadth_ratio: f64,
) -> (usize, f64) {
    let top_n = configured_top_n.max(1);

    if breadth_ratio >= 2.0 / 3.0 {
        return (top_n, base_floor);
    }
    if breadth_ratio >= 1.0 / 3.0 {
        return (top_n.min(2), base_floor.max(0.0));
    }

    (1, base_floor.max(0.02))
}

/// 自适应双动量：按市场广度动态调整 `top_n` 与绝对动量门槛，再做“相对排序 + 绝对过滤”。
pub fn select_adaptive_dual_momentum_assets(
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

    if ranking.is_empty() {
        return Vec::new();
    }

    let breadth_count = ranking.iter().filter(|(_, ret)| *ret >= 0.0).count();
    let breadth_ratio = breadth_count as f64 / ranking.len() as f64;
    let (adaptive_top_n, adaptive_floor) = adaptive_targets(top_n, absolute_momentum_floor, breadth_ratio);

    let selected: Vec<String> = ranking
        .into_iter()
        .filter(|(_, ret)| *ret >= adaptive_floor)
        .take(adaptive_top_n)
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

    fn sample_dates() -> Vec<NaiveDate> {
        vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ]
    }

    #[test]
    fn keeps_configured_topn_when_breadth_is_high() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 106.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "b".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 105.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "c".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 102.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 101.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let selected = select_adaptive_dual_momentum_assets(
            &maps,
            &dates,
            1,
            1,
            2,
            0.0,
            Some("dividend"),
        );

        assert_eq!(selected, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_when_breadth_is_very_low() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 99.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "b".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 98.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "c".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 99.5)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 100.2)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let selected = select_adaptive_dual_momentum_assets(
            &maps,
            &dates,
            1,
            1,
            3,
            0.0,
            Some("dividend"),
        );

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
