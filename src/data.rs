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

/// Read a CSV file into an ordered vector of bars.
pub fn read_bars(path: &str) -> anyhow::Result<Vec<Bar>> {
    let mut rdr = Reader::from_path(path)
        .with_context(|| format!("failed to open csv: {}", path))?;
    let mut bars = Vec::new();
    for row in rdr.deserialize() {
        let bar: Bar = row.with_context(|| format!("failed to deserialize row in {}", path))?;
        bars.push(bar);
    }
    if bars.len() < 2 {
        bail!("not enough rows in {}", path);
    }
    Ok(bars)
}

/// Read a CSV file into a date-indexed map for alignment across assets.
pub fn read_bars_map(path: &str) -> anyhow::Result<HashMap<NaiveDate, Bar>> {
    let bars = read_bars(path)?;
    let mut map = HashMap::new();
    for bar in bars {
        map.insert(bar.date, bar);
    }
    Ok(map)
}

/// Intersect all available trading dates across assets and return a sorted common calendar.
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
