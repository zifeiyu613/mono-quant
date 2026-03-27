use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 风险开关轮动：风险资产若满足绝对动量门槛则持有最强者，否则回退到防守资产。
pub fn select_risk_off_rotation_asset(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    risk_assets: &[String],
    absolute_momentum_floor: f64,
    defensive_asset: &str,
) -> Vec<String> {
    let mut best_asset: Option<(String, f64)> = None;
    for asset in risk_assets {
        if let Some(bars) = asset_maps.get(asset) {
            let now = bars.get(&dates[i]).unwrap().close;
            let past = bars.get(&dates[i - lookback]).unwrap().close;
            let ret = now / past - 1.0;
            match &best_asset {
                Some((_, best_ret)) if ret <= *best_ret => {}
                _ => best_asset = Some((asset.clone(), ret)),
            }
        }
    }

    if let Some((asset, ret)) = best_asset {
        if ret >= absolute_momentum_floor {
            return vec![asset];
        }
    }

    if asset_maps.contains_key(defensive_asset) {
        vec![defensive_asset.to_string()]
    } else {
        Vec::new()
    }
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
        let hs300 = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 101.0)];
        let zz500 = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 99.0)];
        let dividend = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 100.2)];
        maps.insert(
            "hs300".to_string(),
            hs300.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "zz500".to_string(),
            zz500.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps.insert(
            "dividend".to_string(),
            dividend.into_iter().map(|item| (item.date, item)).collect(),
        );
        maps
    }

    #[test]
    fn selects_best_risk_asset_when_floor_passes() {
        let maps = sample_maps();
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let selected = select_risk_off_rotation_asset(
            &maps,
            &dates,
            1,
            1,
            &["hs300".to_string(), "zz500".to_string()],
            0.0,
            "dividend",
        );
        assert_eq!(selected, vec!["hs300".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_when_floor_fails() {
        let maps = sample_maps();
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let selected = select_risk_off_rotation_asset(
            &maps,
            &dates,
            1,
            1,
            &["hs300".to_string(), "zz500".to_string()],
            0.02,
            "dividend",
        );
        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}

