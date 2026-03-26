use anyhow::{bail, Context};
use chrono::NaiveDate;
use csv::Reader;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Deserialize, Clone)]
pub struct Bar {
    pub date: NaiveDate,
    pub open: f64,
    pub close: f64,
}

/// 读取 CSV 文件，并按顺序返回 K 线数据数组。
pub fn read_bars(path: &str) -> anyhow::Result<Vec<Bar>> {
    let mut rdr = Reader::from_path(path)
        .with_context(|| format!("打开 CSV 失败：{}", path))?;
    let mut bars = Vec::new();
    for row in rdr.deserialize() {
        let bar: Bar = row.with_context(|| format!("反序列化 CSV 行失败：{}", path))?;
        bars.push(bar);
    }
    if bars.len() < 2 {
        bail!("{} 的数据行数不足", path);
    }
    Ok(bars)
}

/// 读取 CSV 文件，并转成按日期索引的映射，便于多资产日期对齐。
pub fn read_bars_map(path: &str) -> anyhow::Result<HashMap<NaiveDate, Bar>> {
    let bars = read_bars(path)?;
    let mut map = HashMap::new();
    for bar in bars {
        map.insert(bar.date, bar);
    }
    Ok(map)
}

/// 计算所有资产可用交易日的交集，并返回排序后的共同交易日历。
pub fn intersect_dates(asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>) -> Vec<NaiveDate> {
    let mut common: Option<HashSet<NaiveDate>> = None;
    for bars in asset_maps.values() {
        let dates: HashSet<NaiveDate> = bars.keys().cloned().collect();
        common = match common {
            None => Some(dates),
            Some(existing) => Some(existing.intersection(&dates).cloned().collect()),
        };
    }
    let mut out: Vec<NaiveDate> = common.unwrap_or_default().into_iter().collect();
    out.sort();
    out
}

/// 按闭区间日期过滤多资产数据。
pub fn filter_asset_maps_by_date_range(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    start: NaiveDate,
    end: NaiveDate,
) -> HashMap<String, HashMap<NaiveDate, Bar>> {
    asset_maps
        .iter()
        .map(|(name, bars)| {
            let filtered = bars
                .iter()
                .filter(|(date, _)| **date >= start && **date <= end)
                .map(|(date, bar)| (*date, bar.clone()))
                .collect();
            (name.clone(), filtered)
        })
        .collect()
}
