//! Table statistics, HyperLogLog NDV, reservoir sampling, and ANALYZE helpers.
//!
//! Incremental DML trackers keep approximate `row_count` / index NDV hot.
//! [`crate::engine::TakyonicEngine::analyze_table`] / [`crate::executor::AnalyzeExec`]
//! rebuild rich per-column stats (null fraction, NDV, min/max, MCV, histogram)
//! and persist them under `data_dir/STATS` for the cost-based optimizer.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, TakyonicError};
use crate::schema::{Record, TableSchema};

/// On-disk stats file name under `data_dir`.
pub const STATS_FILE: &str = "STATS";

/// Prefer IndexScan only when estimated matching rows are below this fraction
/// of the table (otherwise sequential scan wins).
pub const INDEX_SELECTIVITY_THRESHOLD: f64 = 0.05;

/// Reservoir sample size for MCV / histogram construction.
pub const ANALYZE_SAMPLE_SIZE: usize = 10_000;

/// Assumed rows per “page” for coarse `page_count` estimates.
const ROWS_PER_PAGE: u64 = 100;

/// Number of HyperLogLog registers (`2^p`).
const HLL_P: u32 = 14;
const HLL_M: usize = 1 << HLL_P; // 16384

/// Most-common-value list length retained after ANALYZE.
const MCV_LIMIT: usize = 10;

/// Histogram bucket count (equal-height boundaries from the sample).
const HIST_BUCKETS: usize = 8;

/// Per-column statistics gathered by `ANALYZE` (and optionally refined online).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnStats {
    /// Column / field name.
    pub column: String,
    /// Fraction of null / empty values in `[0, 1]`.
    pub null_frac: f64,
    /// Estimated number of distinct values (HyperLogLog or exact).
    pub ndv: u64,
    /// Lexicographic minimum non-null value (string form).
    pub min: Option<String>,
    /// Lexicographic maximum non-null value.
    pub max: Option<String>,
    /// Most common values: `(value, absolute frequency)` sorted by frequency desc.
    pub mcv: Vec<(String, u64)>,
    /// Equal-height histogram boundary values (sorted).
    pub histogram: Vec<String>,
}

/// Statistics for one table.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TableStats {
    /// Approximate live row count.
    pub row_count: u64,
    /// Coarse page estimate (`ceil(row_count / ROWS_PER_PAGE)`).
    pub page_count: u64,
    /// Distinct indexed values per **index name** (incremental tracker / ANALYZE).
    pub distinct: BTreeMap<String, u64>,
    /// Per-column rich stats (keyed by column name).
    pub columns: BTreeMap<String, ColumnStats>,
}

impl TableStats {
    /// Estimated rows matching an equality probe on `index` (uniform `row/NDV`).
    pub fn eq_cost(&self, index: &str) -> u64 {
        let ndv = self.distinct.get(index).copied().unwrap_or(0).max(1);
        (self.row_count / ndv).max(1)
    }

    /// Selectivity of an equality predicate (`1/NDV`), clamped to `(0, 1]`.
    pub fn eq_selectivity(&self, index: &str) -> f64 {
        let ndv = self.distinct.get(index).copied().unwrap_or(1).max(1) as f64;
        (1.0 / ndv).clamp(f64::MIN_POSITIVE, 1.0)
    }

    /// Estimated rows for `column = literal`, using MCV when available.
    pub fn eq_rows_for_column(&self, column: &str, literal: Option<&str>) -> u64 {
        let rows = self.row_count.max(1);
        if let Some(cs) = self.columns.get(column) {
            if let Some(lit) = literal {
                if let Some((_, freq)) = cs.mcv.iter().find(|(v, _)| v == lit) {
                    return (*freq).max(1);
                }
            }
            let ndv = cs.ndv.max(1);
            return (rows / ndv).max(1);
        }
        // Fall back to index-named distinct if column matches an index entry key.
        if let Some(&ndv) = self.distinct.get(column) {
            return (rows / ndv.max(1)).max(1);
        }
        rows
    }

    /// Whether an equality IndexScan is cheaper than a sequential scan.
    ///
    /// Uses MCV-aware row estimates and [`INDEX_SELECTIVITY_THRESHOLD`].
    pub fn prefer_index_scan(&self, column: &str, literal: Option<&str>) -> bool {
        if self.row_count == 0 {
            return true;
        }
        let est = self.eq_rows_for_column(column, literal);
        let threshold = ((self.row_count as f64) * INDEX_SELECTIVITY_THRESHOLD)
            .ceil()
            .max(1.0) as u64;
        est <= threshold
    }
}

/// HyperLogLog++-style sketch for cardinality estimation (xxh3 hashed).
#[derive(Clone, Debug)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// Create an empty sketch.
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; HLL_M],
        }
    }

    /// Observe a string value.
    pub fn add(&mut self, value: &str) {
        let hash = xxh3_64(value.as_bytes());
        let idx = (hash & ((HLL_M as u64) - 1)) as usize;
        let w = hash >> HLL_P;
        let rho = (w.trailing_zeros() + 1).min(64) as u8;
        if rho > self.registers[idx] {
            self.registers[idx] = rho;
        }
    }

    /// Estimate cardinality (standard HLL alpha_m correction).
    pub fn count(&self) -> u64 {
        let m = HLL_M as f64;
        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2f64.powi(-(r as i32)))
            .sum();
        let alpha = match HLL_M {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let raw = alpha * m * m / sum;
        // Small-range correction.
        let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
        let est = if raw <= 2.5 * m && zeros > 0.0 {
            m * (m / zeros).ln()
        } else {
            raw
        };
        est.round().max(0.0) as u64
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Reservoir sampler (Algorithm R) holding up to `capacity` records.
#[derive(Debug)]
pub struct ReservoirSampler {
    capacity: usize,
    seen: u64,
    sample: Vec<Record>,
}

impl ReservoirSampler {
    /// Create a sampler with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: 0,
            sample: Vec::new(),
        }
    }

    /// Observe one row.
    pub fn offer(&mut self, record: Record) {
        self.seen = self.seen.saturating_add(1);
        if self.sample.len() < self.capacity {
            self.sample.push(record);
            return;
        }
        // Replace with probability capacity/seen.
        let j = (xxh3_64(
            &[
                self.seen.to_le_bytes().as_slice(),
                record
                    .fields
                    .values()
                    .next()
                    .map(|s| s.as_bytes())
                    .unwrap_or(b""),
            ]
            .concat(),
        ) as u64)
            % self.seen;
        if (j as usize) < self.capacity {
            self.sample[j as usize] = record;
        }
    }

    /// Borrow the current sample.
    pub fn sample(&self) -> &[Record] {
        &self.sample
    }

    /// Total rows observed.
    pub fn seen(&self) -> u64 {
        self.seen
    }
}

/// In-memory distinct-value sets used to maintain accurate NDV.
#[derive(Default)]
struct DistinctTracker {
    values: HashMap<String, HashSet<String>>,
    refs: HashMap<String, HashMap<String, u64>>,
}

impl DistinctTracker {
    fn observe_insert(&mut self, index: &str, value: &str) {
        let refs = self.refs.entry(index.to_string()).or_default();
        let n = refs.entry(value.to_string()).or_insert(0);
        *n += 1;
        if *n == 1 {
            self.values
                .entry(index.to_string())
                .or_default()
                .insert(value.to_string());
        }
    }

    fn observe_delete(&mut self, index: &str, value: &str) {
        let Some(refs) = self.refs.get_mut(index) else {
            return;
        };
        let Some(n) = refs.get_mut(value) else {
            return;
        };
        *n = n.saturating_sub(1);
        if *n == 0 {
            refs.remove(value);
            if let Some(set) = self.values.get_mut(index) {
                set.remove(value);
            }
        }
    }

    fn ndv(&self, index: &str) -> u64 {
        self.values.get(index).map(|s| s.len() as u64).unwrap_or(0)
    }
}

/// Catalog of per-table statistics + distinct trackers.
#[derive(Default)]
pub struct StatsCatalog {
    stats: RwLock<BTreeMap<String, TableStats>>,
    trackers: RwLock<BTreeMap<String, DistinctTracker>>,
}

impl StatsCatalog {
    /// Create an empty stats catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure a table has a stats entry (call on schema register).
    pub fn register_table(&self, schema: &TableSchema) {
        let mut stats = self.stats.write();
        let entry = stats.entry(schema.name.clone()).or_default();
        for idx in &schema.indexes {
            entry.distinct.entry(idx.name.clone()).or_insert(0);
        }
        let mut trackers = self.trackers.write();
        trackers.entry(schema.name.clone()).or_default();
    }

    /// Snapshot stats for `table`.
    pub fn get(&self, table: &str) -> TableStats {
        self.stats.read().get(table).cloned().unwrap_or_default()
    }

    /// Record an inserted row (and its indexed column values).
    pub fn on_insert(&self, table: &str, index_values: &[(String, String)]) {
        let mut trackers = self.trackers.write();
        let tracker = trackers.entry(table.to_string()).or_default();
        for (index, value) in index_values {
            tracker.observe_insert(index, value);
        }
        let mut stats = self.stats.write();
        let st = stats.entry(table.to_string()).or_default();
        st.row_count = st.row_count.saturating_add(1);
        st.page_count = pages_for(st.row_count);
        for (index, _) in index_values {
            st.distinct.insert(index.clone(), tracker.ndv(index));
        }
    }

    /// Record a deleted row.
    pub fn on_delete(&self, table: &str, index_values: &[(String, String)]) {
        let mut trackers = self.trackers.write();
        let tracker = trackers.entry(table.to_string()).or_default();
        for (index, value) in index_values {
            tracker.observe_delete(index, value);
        }
        let mut stats = self.stats.write();
        let st = stats.entry(table.to_string()).or_default();
        st.row_count = st.row_count.saturating_sub(1);
        st.page_count = pages_for(st.row_count);
        for (index, _) in index_values {
            st.distinct.insert(index.clone(), tracker.ndv(index));
        }
    }

    /// Record indexed values during CREATE INDEX backfill (does not change row_count).
    pub fn on_index_backfill(&self, table: &str, index_values: &[(String, String)]) {
        let mut trackers = self.trackers.write();
        let tracker = trackers.entry(table.to_string()).or_default();
        for (index, value) in index_values {
            tracker.observe_insert(index, value);
        }
        let mut stats = self.stats.write();
        let st = stats.entry(table.to_string()).or_default();
        for (index, _) in index_values {
            st.distinct.insert(index.clone(), tracker.ndv(index));
        }
    }

    /// Replace stats after ANALYZE / full rebuild.
    pub fn replace(&self, table: &str, new_stats: TableStats) {
        self.stats.write().insert(table.to_string(), new_stats);
    }

    /// Load all tables from an in-memory map (engine open).
    pub fn load_all(&self, all: BTreeMap<String, TableStats>) {
        *self.stats.write() = all;
    }

    /// Snapshot every table's stats for persistence.
    pub fn snapshot_all(&self) -> BTreeMap<String, TableStats> {
        self.stats.read().clone()
    }
}

fn pages_for(row_count: u64) -> u64 {
    row_count.div_ceil(ROWS_PER_PAGE).max(if row_count == 0 { 0 } else { 1 })
}

/// Build [`TableStats`] from a full table scan + reservoir/HLL.
pub fn compute_table_stats(schema: &TableSchema, records: &[Record]) -> TableStats {
    let row_count = records.len() as u64;
    let mut column_names: HashSet<String> = HashSet::new();
    column_names.insert(schema.primary_key.clone());
    for idx in &schema.indexes {
        column_names.insert(idx.column.clone());
    }
    for r in records {
        for k in r.fields.keys() {
            column_names.insert(k.clone());
        }
    }

    let mut hlls: HashMap<String, HyperLogLog> = HashMap::new();
    let mut nulls: HashMap<String, u64> = HashMap::new();
    let mut mins: HashMap<String, String> = HashMap::new();
    let mut maxs: HashMap<String, String> = HashMap::new();
    let mut reservoir = ReservoirSampler::new(ANALYZE_SAMPLE_SIZE);

    for col in &column_names {
        hlls.insert(col.clone(), HyperLogLog::new());
        nulls.insert(col.clone(), 0);
    }

    for record in records {
        reservoir.offer(record.clone());
        for col in &column_names {
            match record.get(col) {
                None | Some("") => {
                    *nulls.entry(col.clone()).or_default() += 1;
                }
                Some(v) => {
                    hlls.get_mut(col).unwrap().add(v);
                    mins
                        .entry(col.clone())
                        .and_modify(|m| {
                            if v < m.as_str() {
                                *m = v.to_string();
                            }
                        })
                        .or_insert_with(|| v.to_string());
                    maxs
                        .entry(col.clone())
                        .and_modify(|m| {
                            if v > m.as_str() {
                                *m = v.to_string();
                            }
                        })
                        .or_insert_with(|| v.to_string());
                }
            }
        }
    }

    // Exact NDV from sample frequencies scaled; prefer HLL for large tables,
    // exact HashSet count when row_count fits in the sample (or is small).
    let use_exact = row_count as usize <= ANALYZE_SAMPLE_SIZE;
    let sample = reservoir.sample();

    let mut columns = BTreeMap::new();
    for col in &column_names {
        let null_count = *nulls.get(col).unwrap_or(&0);
        let null_frac = if row_count == 0 {
            0.0
        } else {
            null_count as f64 / row_count as f64
        };
        let ndv = if use_exact {
            let mut set = HashSet::new();
            for r in records {
                if let Some(v) = r.get(col) {
                    if !v.is_empty() {
                        set.insert(v.to_string());
                    }
                }
            }
            set.len() as u64
        } else {
            hlls.get(col).map(|h| h.count()).unwrap_or(0).max(1)
        };

        let (mcv, histogram) = mcv_and_histogram(sample, col, row_count, sample.len() as u64);

        columns.insert(
            col.clone(),
            ColumnStats {
                column: col.clone(),
                null_frac,
                ndv,
                min: mins.get(col).cloned(),
                max: maxs.get(col).cloned(),
                mcv,
                histogram,
            },
        );
    }

    let mut distinct = BTreeMap::new();
    for idx in &schema.indexes {
        let ndv = columns
            .get(&idx.column)
            .map(|c| c.ndv)
            .unwrap_or(1);
        distinct.insert(idx.name.clone(), ndv);
    }

    TableStats {
        row_count,
        page_count: pages_for(row_count),
        distinct,
        columns,
    }
}

fn mcv_and_histogram(
    sample: &[Record],
    column: &str,
    table_rows: u64,
    sample_len: u64,
) -> (Vec<(String, u64)>, Vec<String>) {
    let mut freq: HashMap<String, u64> = HashMap::new();
    for r in sample {
        if let Some(v) = r.get(column) {
            if !v.is_empty() {
                *freq.entry(v.to_string()).or_default() += 1;
            }
        }
    }
    let mut pairs: Vec<(String, u64)> = freq.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    pairs.truncate(MCV_LIMIT);

    // Scale sample frequencies to table row estimates.
    let scale = if sample_len == 0 {
        1.0
    } else {
        table_rows as f64 / sample_len as f64
    };
    let mcv: Vec<(String, u64)> = pairs
        .iter()
        .map(|(v, c)| (v.clone(), ((*c as f64) * scale).round().max(1.0) as u64))
        .collect();

    // Sorted unique sample values for equal-height histogram boundaries.
    let mut all: Vec<String> = sample
        .iter()
        .filter_map(|r| r.get(column).filter(|s| !s.is_empty()).map(str::to_string))
        .collect();
    all.sort();
    all.dedup();
    let histogram = equal_height_boundaries(&all, HIST_BUCKETS);
    (mcv, histogram)
}

fn equal_height_boundaries(sorted_unique: &[String], buckets: usize) -> Vec<String> {
    if sorted_unique.is_empty() || buckets == 0 {
        return Vec::new();
    }
    if sorted_unique.len() <= buckets {
        return sorted_unique.to_vec();
    }
    let mut out = Vec::with_capacity(buckets + 1);
    for i in 0..=buckets {
        let idx = i * (sorted_unique.len() - 1) / buckets;
        out.push(sorted_unique[idx].clone());
    }
    out.dedup();
    out
}

/// Path to the durable stats file.
pub fn stats_path(data_dir: &Path) -> PathBuf {
    data_dir.join(STATS_FILE)
}

/// Load persisted table statistics (empty map if missing).
pub fn load_stats(data_dir: &Path) -> Result<BTreeMap<String, TableStats>> {
    let path = stats_path(data_dir);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let text = fs::read_to_string(&path)?;
    parse_stats(&text)
}

/// Parse STATS file text (Raft `StatsReplace` payload).
pub fn parse_stats(text: &str) -> Result<BTreeMap<String, TableStats>> {
    let mut tables: BTreeMap<String, TableStats> = BTreeMap::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let tag = parts.next().ok_or_else(|| {
            TakyonicError::Engine(format!("stats line {}: empty", lineno + 1))
        })?;
        match tag {
            "TABLE" => {
                let name = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: TABLE missing name", lineno + 1))
                })?;
                let st = tables.entry(name.to_string()).or_default();
                for kv in parts {
                    if let Some((k, v)) = kv.split_once('=') {
                        match k {
                            "rows" => st.row_count = v.parse().unwrap_or(0),
                            "pages" => st.page_count = v.parse().unwrap_or(0),
                            _ => {}
                        }
                    }
                }
            }
            "COL" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: COL missing table", lineno + 1))
                })?;
                let column = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: COL missing column", lineno + 1))
                })?;
                let st = tables.entry(table.to_string()).or_default();
                let mut cs = ColumnStats {
                    column: column.to_string(),
                    ..Default::default()
                };
                for kv in parts {
                    if let Some((k, v)) = kv.split_once('=') {
                        match k {
                            "null_frac" => cs.null_frac = v.parse().unwrap_or(0.0),
                            "ndv" => cs.ndv = v.parse().unwrap_or(0),
                            "min" => cs.min = Some(unescape(v)),
                            "max" => cs.max = Some(unescape(v)),
                            _ => {}
                        }
                    }
                }
                st.columns.insert(column.to_string(), cs);
            }
            "MCV" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: MCV missing table", lineno + 1))
                })?;
                let column = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: MCV missing column", lineno + 1))
                })?;
                let st = tables.entry(table.to_string()).or_default();
                let cs = st.columns.entry(column.to_string()).or_insert_with(|| {
                    ColumnStats {
                        column: column.to_string(),
                        ..Default::default()
                    }
                });
                for pair in parts {
                    if let Some((v, f)) = pair.rsplit_once(':') {
                        if let Ok(freq) = f.parse::<u64>() {
                            cs.mcv.push((unescape(v), freq));
                        }
                    }
                }
            }
            "HIST" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: HIST missing table", lineno + 1))
                })?;
                let column = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: HIST missing column", lineno + 1))
                })?;
                let rest: Vec<&str> = parts.collect();
                let joined = rest.join(" ");
                let st = tables.entry(table.to_string()).or_default();
                let cs = st.columns.entry(column.to_string()).or_insert_with(|| {
                    ColumnStats {
                        column: column.to_string(),
                        ..Default::default()
                    }
                });
                cs.histogram = joined
                    .split('|')
                    .filter(|s| !s.is_empty())
                    .map(unescape)
                    .collect();
            }
            "IDX" => {
                let table = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: IDX missing table", lineno + 1))
                })?;
                let index = parts.next().ok_or_else(|| {
                    TakyonicError::Engine(format!("stats line {}: IDX missing name", lineno + 1))
                })?;
                let mut ndv = 0u64;
                for kv in parts {
                    if let Some(("ndv", v)) = kv.split_once('=') {
                        ndv = v.parse().unwrap_or(0);
                    }
                }
                tables
                    .entry(table.to_string())
                    .or_default()
                    .distinct
                    .insert(index.to_string(), ndv);
            }
            other => {
                return Err(TakyonicError::Engine(format!(
                    "stats line {}: unknown tag `{other}`",
                    lineno + 1
                )));
            }
        }
    }
    Ok(tables)
}

/// Serialize stats to STATS file text (Raft `StatsReplace` payload).
pub fn encode_stats(stats: &BTreeMap<String, TableStats>) -> String {
    let mut out = String::from("# Takyonic table statistics\n");
    for (name, st) in stats {
        out.push_str(&format!(
            "TABLE {name} rows={} pages={}\n",
            st.row_count, st.page_count
        ));
        for (col, cs) in &st.columns {
            out.push_str(&format!(
                "COL {name} {col} null_frac={:.6} ndv={}",
                cs.null_frac, cs.ndv
            ));
            if let Some(min) = &cs.min {
                out.push_str(&format!(" min={}", escape(min)));
            }
            if let Some(max) = &cs.max {
                out.push_str(&format!(" max={}", escape(max)));
            }
            out.push('\n');
            if !cs.mcv.is_empty() {
                out.push_str(&format!("MCV {name} {col}"));
                for (v, freq) in &cs.mcv {
                    out.push_str(&format!(" {}:{}", escape(v), freq));
                }
                out.push('\n');
            }
            if !cs.histogram.is_empty() {
                let hist: Vec<String> = cs.histogram.iter().map(|s| escape(s)).collect();
                out.push_str(&format!("HIST {name} {col} {}\n", hist.join("|")));
            }
        }
        for (idx, ndv) in &st.distinct {
            out.push_str(&format!("IDX {name} {idx} ndv={ndv}\n"));
        }
    }
    out
}

/// Atomically rewrite the stats file.
pub fn save_stats(data_dir: &Path, stats: &BTreeMap<String, TableStats>) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    let path = stats_path(data_dir);
    let tmp = data_dir.join(format!("{STATS_FILE}.tmp"));
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(encode_stats(stats).as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    if let Ok(dir) = fs::File::open(data_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(' ', "\\s")
        .replace('|', "\\p")
        .replace(':', "\\c")
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('s') => out.push(' '),
                Some('p') => out.push('|'),
                Some('c') => out.push(':'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{IndexDef, TableSchema};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn eq_cost_prefers_high_ndv() {
        let mut st = TableStats {
            row_count: 10_000,
            page_count: 100,
            distinct: BTreeMap::new(),
            columns: BTreeMap::new(),
        };
        st.distinct.insert("status".into(), 2);
        st.distinct.insert("city".into(), 200);
        assert!(st.eq_cost("city") < st.eq_cost("status"));
        assert_eq!(st.eq_cost("city"), 50);
        assert_eq!(st.eq_cost("status"), 5_000);
    }

    #[test]
    fn prefer_index_uses_mcv_skew() {
        let mut st = TableStats {
            row_count: 1000,
            page_count: 10,
            distinct: BTreeMap::new(),
            columns: BTreeMap::new(),
        };
        st.distinct.insert("idx_dept".into(), 2);
        st.columns.insert(
            "department".into(),
            ColumnStats {
                column: "department".into(),
                null_frac: 0.0,
                ndv: 2,
                min: Some("Engineering".into()),
                max: Some("Sales".into()),
                mcv: vec![("Sales".into(), 950), ("Engineering".into(), 50)],
                histogram: vec![],
            },
        );
        assert!(
            !st.prefer_index_scan("department", Some("Sales")),
            "frequent MCV must not prefer IndexScan"
        );
        assert!(
            st.prefer_index_scan("department", Some("Engineering")),
            "rare value should prefer IndexScan"
        );
    }

    #[test]
    fn tracker_updates_ndv() {
        let cat = StatsCatalog::new();
        let schema = TableSchema::new("users", "id", vec![IndexDef::new("city", "city")]);
        cat.register_table(&schema);
        cat.on_insert("users", &[("city".into(), "A".into())]);
        cat.on_insert("users", &[("city".into(), "A".into())]);
        cat.on_insert("users", &[("city".into(), "B".into())]);
        let st = cat.get("users");
        assert_eq!(st.row_count, 3);
        assert_eq!(st.distinct.get("city").copied(), Some(2));
        cat.on_delete("users", &[("city".into(), "A".into())]);
        let st = cat.get("users");
        assert_eq!(st.row_count, 2);
        assert_eq!(st.distinct.get("city").copied(), Some(2));
        cat.on_delete("users", &[("city".into(), "A".into())]);
        assert_eq!(cat.get("users").distinct.get("city").copied(), Some(1));
    }

    #[test]
    fn hyperloglog_estimates_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..5_000 {
            hll.add(&format!("v{i}"));
        }
        let est = hll.count();
        // Allow ±20% for this sketch size.
        assert!(
            (4_000..=6_000).contains(&est),
            "HLL estimate {est} out of range for 5000 distinct"
        );
    }

    #[test]
    fn compute_stats_ndv_minmax() {
        let schema = TableSchema::new(
            "employees",
            "id",
            vec![IndexDef::new("idx_dept", "department")],
        );
        let records = vec![
            Record::new()
                .set("id", "1")
                .set("department", "Sales")
                .set("salary", "100"),
            Record::new()
                .set("id", "2")
                .set("department", "Sales")
                .set("salary", "200"),
            Record::new()
                .set("id", "3")
                .set("department", "Engineering")
                .set("salary", "300"),
        ];
        let st = compute_table_stats(&schema, &records);
        assert_eq!(st.row_count, 3);
        assert_eq!(st.columns.get("department").unwrap().ndv, 2);
        assert_eq!(
            st.columns.get("department").unwrap().min.as_deref(),
            Some("Engineering")
        );
        assert_eq!(
            st.columns.get("department").unwrap().max.as_deref(),
            Some("Sales")
        );
        assert_eq!(st.distinct.get("idx_dept").copied(), Some(2));
        assert!(st.columns.get("department").unwrap().mcv[0].1 >= 1);
    }

    #[test]
    fn stats_file_roundtrip() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-stats-{nanos}"));
        fs::create_dir_all(&root).unwrap();

        let mut map = BTreeMap::new();
        let mut st = TableStats {
            row_count: 100,
            page_count: 1,
            distinct: BTreeMap::new(),
            columns: BTreeMap::new(),
        };
        st.distinct.insert("idx_dept".into(), 3);
        st.columns.insert(
            "department".into(),
            ColumnStats {
                column: "department".into(),
                null_frac: 0.0,
                ndv: 3,
                min: Some("A".into()),
                max: Some("C".into()),
                mcv: vec![("B".into(), 50)],
                histogram: vec!["A".into(), "B".into(), "C".into()],
            },
        );
        map.insert("employees".into(), st);
        save_stats(&root, &map).unwrap();
        let loaded = load_stats(&root).unwrap();
        assert_eq!(loaded.get("employees").unwrap().row_count, 100);
        assert_eq!(
            loaded
                .get("employees")
                .unwrap()
                .columns
                .get("department")
                .unwrap()
                .ndv,
            3
        );
        assert_eq!(
            loaded
                .get("employees")
                .unwrap()
                .columns
                .get("department")
                .unwrap()
                .mcv[0]
                .0,
            "B"
        );
        let _ = fs::remove_dir_all(root);
    }
}
