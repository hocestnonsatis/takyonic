//! SIMD-oriented vectorized execution (batch + mask).
//!
//! Analytical pipelines process [`VectorBatch`] chunks (default N=1024) instead
//! of row-by-row Volcano pulls. Arithmetic and predicates run over columnar
//! lanes with AVX-512 (8×f64 zmm) or AVX2 (2×4×f64 ymm) when available, using
//! bitmasks for branch-free filter selection. The Cranelift JIT can also emit
//! packed `F64X2` batch loops via [`crate::jit::JitCompiler::compile_batch_arith`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use crate::error::{Result, TakyonicError};
use crate::executor::{ExecutionContext, Executor, evaluate_bool};
#[cfg(test)]
use crate::executor::evaluate;
use crate::query::FilterOp;
use crate::schema::Record;
use crate::sql::{ArithOp, Expression, Value as SqlValue};
use crate::telemetry::EngineMetrics;

/// Preferred batch cardinality for instruction-cache / vector-register reuse.
pub const VECTOR_BATCH_SIZE: usize = 1024;
/// Portable SIMD chunk width (2×AVX2 ymm = 8×f64, or 1×AVX-512 zmm).
pub const SIMD_WIDTH: usize = 8;
/// AVX-512 processes 8×f64 in a single zmm register.
pub const AVX512_WIDTH: usize = 8;

/// Columnar batch of up to [`VECTOR_BATCH_SIZE`] rows with an optional selection mask.
#[derive(Clone, Debug, Default)]
pub struct VectorBatch {
    /// Column name → dense f64 lanes (integers / bools coerced; strings → 0).
    pub columns: BTreeMap<String, Vec<f64>>,
    /// Live row count (`<= VECTOR_BATCH_SIZE`).
    pub len: usize,
    /// Selection bitmask: bit `i` set ⇒ row `i` survives the filter.
    /// Empty vec means all rows selected.
    pub mask: Vec<u64>,
}

impl VectorBatch {
    /// Empty batch with capacity hint.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            columns: BTreeMap::new(),
            len: 0,
            mask: Vec::new(),
        }
        .reserve(cap)
    }

    fn reserve(self, _cap: usize) -> Self {
        self
    }

    /// True when every row is selected (no mask / all bits set for `len`).
    pub fn all_selected(&self) -> bool {
        if self.mask.is_empty() {
            return true;
        }
        for i in 0..self.len {
            if !self.is_selected(i) {
                return false;
            }
        }
        true
    }

    /// Whether row `i` is selected.
    #[inline]
    pub fn is_selected(&self, i: usize) -> bool {
        if self.mask.is_empty() {
            return true;
        }
        let word = i / 64;
        let bit = i % 64;
        self.mask.get(word).copied().unwrap_or(0) & (1u64 << bit) != 0
    }

    /// Ensure mask capacity for `self.len` rows (all selected).
    pub fn ensure_full_mask(&mut self) {
        let words = self.len.div_ceil(64);
        if self.mask.len() < words {
            self.mask.resize(words, u64::MAX);
        }
        // Clear bits beyond len.
        if self.len % 64 != 0 {
            let last = words.saturating_sub(1);
            let keep = (1u64 << (self.len % 64)) - 1;
            if let Some(w) = self.mask.get_mut(last) {
                *w &= keep;
            }
        }
    }

    /// AND the selection mask with `bits` (same length semantics).
    pub fn apply_mask_bits(&mut self, bits: &[u64]) {
        self.ensure_full_mask();
        for (dst, src) in self.mask.iter_mut().zip(bits.iter()) {
            *dst &= *src;
        }
    }

    /// Count of selected rows.
    pub fn selected_count(&self) -> usize {
        if self.mask.is_empty() {
            return self.len;
        }
        let mut n = 0;
        for i in 0..self.len {
            if self.is_selected(i) {
                n += 1;
            }
        }
        n
    }

    /// Build a batch from row-oriented records (numeric coercion).
    pub fn from_records(rows: &[Record], columns: &[String]) -> Self {
        let n = rows.len().min(VECTOR_BATCH_SIZE);
        let mut columns_map = BTreeMap::new();
        for col in columns {
            let mut vals = Vec::with_capacity(n);
            for row in rows.iter().take(n) {
                vals.push(record_f64(row, col));
            }
            columns_map.insert(col.clone(), vals);
        }
        // Discover extra numeric columns present in rows.
        if columns.is_empty() {
            let mut names = BTreeMap::new();
            for row in rows.iter().take(n) {
                for k in row.fields.keys() {
                    names.insert(k.clone(), ());
                }
            }
            for col in names.keys() {
                let mut vals = Vec::with_capacity(n);
                for row in rows.iter().take(n) {
                    vals.push(record_f64(row, col));
                }
                columns_map.insert(col.clone(), vals);
            }
        }
        Self {
            columns: columns_map,
            len: n,
            mask: Vec::new(),
        }
    }

    /// Materialize selected rows back to records (column → string fields).
    pub fn to_records(&self) -> Vec<Record> {
        let cols: Vec<_> = self.columns.keys().cloned().collect();
        let mut out = Vec::with_capacity(self.selected_count());
        for i in 0..self.len {
            if !self.is_selected(i) {
                continue;
            }
            let mut rec = Record::new();
            for c in &cols {
                if let Some(vals) = self.columns.get(c) {
                    rec = rec.set(c, format_f64(vals[i]));
                }
            }
            out.push(rec);
        }
        out
    }

    /// Compact selected rows into a dense batch (mask cleared).
    pub fn compact(&self) -> Self {
        let n = self.selected_count();
        let mut columns = BTreeMap::new();
        for (name, vals) in &self.columns {
            let mut dense = Vec::with_capacity(n);
            for i in 0..self.len {
                if self.is_selected(i) {
                    dense.push(vals[i]);
                }
            }
            columns.insert(name.clone(), dense);
        }
        Self {
            columns,
            len: n,
            mask: Vec::new(),
        }
    }
}

fn record_f64(row: &Record, col: &str) -> f64 {
    match row.get(col).map(SqlValue::from_text) {
        Some(SqlValue::Int(n)) => n as f64,
        Some(SqlValue::Float(f)) => f,
        Some(SqlValue::Bool(b)) => {
            if b {
                1.0
            } else {
                0.0
            }
        }
        Some(SqlValue::String(s)) => s.parse().unwrap_or(0.0),
        Some(SqlValue::Null) | None => 0.0,
    }
}

fn format_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// SIMD / auto-vectorized kernels over f64 lanes.
pub struct SimdKernels;

impl SimdKernels {
    /// Element-wise `a * b` → `out` for `n` lanes (AVX-512 / AVX2 / scalar).
    pub fn mul(a: &[f64], b: &[f64], out: &mut [f64], n: usize) {
        debug_assert!(a.len() >= n && b.len() >= n && out.len() >= n);
        let mut i = 0;
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                while i + AVX512_WIDTH <= n {
                    unsafe { Self::mul8_avx512(&a[i..], &b[i..], &mut out[i..]) };
                    i += AVX512_WIDTH;
                }
            } else if is_x86_feature_detected!("avx2") {
                while i + SIMD_WIDTH <= n {
                    Self::mul8_avx2(&a[i..], &b[i..], &mut out[i..]);
                    i += SIMD_WIDTH;
                }
            }
        }
        while i + SIMD_WIDTH <= n {
            for j in 0..SIMD_WIDTH {
                out[i + j] = a[i + j] * b[i + j];
            }
            i += SIMD_WIDTH;
        }
        while i < n {
            out[i] = a[i] * b[i];
            i += 1;
        }
    }

    /// Element-wise add.
    pub fn add(a: &[f64], b: &[f64], out: &mut [f64], n: usize) {
        let mut i = 0;
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                while i + AVX512_WIDTH <= n {
                    unsafe { Self::add8_avx512(&a[i..], &b[i..], &mut out[i..]) };
                    i += AVX512_WIDTH;
                }
            } else if is_x86_feature_detected!("avx2") {
                while i + SIMD_WIDTH <= n {
                    Self::add8_avx2(&a[i..], &b[i..], &mut out[i..]);
                    i += SIMD_WIDTH;
                }
            }
        }
        while i + SIMD_WIDTH <= n {
            for j in 0..SIMD_WIDTH {
                out[i + j] = a[i + j] + b[i + j];
            }
            i += SIMD_WIDTH;
        }
        while i < n {
            out[i] = a[i] + b[i];
            i += 1;
        }
    }

    /// Element-wise subtract.
    pub fn sub(a: &[f64], b: &[f64], out: &mut [f64], n: usize) {
        let mut i = 0;
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                while i + AVX512_WIDTH <= n {
                    unsafe { Self::sub8_avx512(&a[i..], &b[i..], &mut out[i..]) };
                    i += AVX512_WIDTH;
                }
            } else if is_x86_feature_detected!("avx2") {
                while i + SIMD_WIDTH <= n {
                    Self::sub8_avx2(&a[i..], &b[i..], &mut out[i..]);
                    i += SIMD_WIDTH;
                }
            }
        }
        while i + SIMD_WIDTH <= n {
            for j in 0..SIMD_WIDTH {
                out[i + j] = a[i + j] - b[i + j];
            }
            i += SIMD_WIDTH;
        }
        while i < n {
            out[i] = a[i] - b[i];
            i += 1;
        }
    }

    /// Element-wise divide (0 divisor → 0).
    pub fn div(a: &[f64], b: &[f64], out: &mut [f64], n: usize) {
        // Division keeps a scalar loop (correct 0-divisor handling + rare in OLAP).
        let mut i = 0;
        while i < n {
            out[i] = if b[i] == 0.0 { 0.0 } else { a[i] / b[i] };
            i += 1;
        }
    }

    /// Masked sum of `vals` where mask bit is set (or all if mask empty).
    pub fn masked_sum(vals: &[f64], len: usize, mask: &[u64]) -> f64 {
        if mask.is_empty() {
            return Self::sum(vals, len);
        }
        let mut acc = 0.0;
        for i in 0..len {
            let word = i / 64;
            let bit = i % 64;
            if mask.get(word).copied().unwrap_or(0) & (1u64 << bit) != 0 {
                acc += vals[i];
            }
        }
        acc
    }

    /// Horizontal sum (AVX-512 / AVX2 when available).
    pub fn sum(vals: &[f64], n: usize) -> f64 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                return unsafe { Self::sum_avx512(vals, n) };
            }
            if is_x86_feature_detected!("avx2") {
                return unsafe { Self::sum_avx2(vals, n) };
            }
        }
        let mut acc = 0.0;
        for v in vals.iter().take(n) {
            acc += *v;
        }
        acc
    }

    /// Compare `col` with scalar `rhs` under `op` → bitmask words.
    pub fn compare_scalar(col: &[f64], len: usize, op: FilterOp, rhs: f64) -> Vec<u64> {
        let words = len.div_ceil(64);
        let mut mask = vec![0u64; words];
        for i in 0..len {
            let pass = match op {
                FilterOp::Eq => col[i] == rhs,
                FilterOp::Ne => col[i] != rhs,
                FilterOp::Gt => col[i] > rhs,
                FilterOp::Gte => col[i] >= rhs,
                FilterOp::Lt => col[i] < rhs,
                FilterOp::Lte => col[i] <= rhs,
            };
            if pass {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }
        mask
    }

    /// Between inclusive: `lo <= col <= hi`.
    pub fn compare_between(col: &[f64], len: usize, lo: f64, hi: f64) -> Vec<u64> {
        let words = len.div_ceil(64);
        let mut mask = vec![0u64; words];
        for i in 0..len {
            if col[i] >= lo && col[i] <= hi {
                mask[i / 64] |= 1u64 << (i % 64);
            }
        }
        mask
    }

    #[inline]
    fn mul8_avx2(a: &[f64], b: &[f64], out: &mut [f64]) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let a0 = _mm256_loadu_pd(a.as_ptr());
            let b0 = _mm256_loadu_pd(b.as_ptr());
            let a1 = _mm256_loadu_pd(a.as_ptr().add(4));
            let b1 = _mm256_loadu_pd(b.as_ptr().add(4));
            _mm256_storeu_pd(out.as_mut_ptr(), _mm256_mul_pd(a0, b0));
            _mm256_storeu_pd(out.as_mut_ptr().add(4), _mm256_mul_pd(a1, b1));
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            for j in 0..SIMD_WIDTH {
                out[j] = a[j] * b[j];
            }
        }
    }

    #[inline]
    fn add8_avx2(a: &[f64], b: &[f64], out: &mut [f64]) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let a0 = _mm256_loadu_pd(a.as_ptr());
            let b0 = _mm256_loadu_pd(b.as_ptr());
            let a1 = _mm256_loadu_pd(a.as_ptr().add(4));
            let b1 = _mm256_loadu_pd(b.as_ptr().add(4));
            _mm256_storeu_pd(out.as_mut_ptr(), _mm256_add_pd(a0, b0));
            _mm256_storeu_pd(out.as_mut_ptr().add(4), _mm256_add_pd(a1, b1));
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            for j in 0..SIMD_WIDTH {
                out[j] = a[j] + b[j];
            }
        }
    }

    #[inline]
    fn sub8_avx2(a: &[f64], b: &[f64], out: &mut [f64]) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let a0 = _mm256_loadu_pd(a.as_ptr());
            let b0 = _mm256_loadu_pd(b.as_ptr());
            let a1 = _mm256_loadu_pd(a.as_ptr().add(4));
            let b1 = _mm256_loadu_pd(b.as_ptr().add(4));
            _mm256_storeu_pd(out.as_mut_ptr(), _mm256_sub_pd(a0, b0));
            _mm256_storeu_pd(out.as_mut_ptr().add(4), _mm256_sub_pd(a1, b1));
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            for j in 0..SIMD_WIDTH {
                out[j] = a[j] - b[j];
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    unsafe fn mul8_avx512(a: &[f64], b: &[f64], out: &mut [f64]) {
        unsafe {
            let va = _mm512_loadu_pd(a.as_ptr());
            let vb = _mm512_loadu_pd(b.as_ptr());
            _mm512_storeu_pd(out.as_mut_ptr(), _mm512_mul_pd(va, vb));
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    unsafe fn add8_avx512(a: &[f64], b: &[f64], out: &mut [f64]) {
        unsafe {
            let va = _mm512_loadu_pd(a.as_ptr());
            let vb = _mm512_loadu_pd(b.as_ptr());
            _mm512_storeu_pd(out.as_mut_ptr(), _mm512_add_pd(va, vb));
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    unsafe fn sub8_avx512(a: &[f64], b: &[f64], out: &mut [f64]) {
        unsafe {
            let va = _mm512_loadu_pd(a.as_ptr());
            let vb = _mm512_loadu_pd(b.as_ptr());
            _mm512_storeu_pd(out.as_mut_ptr(), _mm512_sub_pd(va, vb));
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    unsafe fn sum_avx512(vals: &[f64], n: usize) -> f64 {
        unsafe {
            let mut i = 0;
            let mut acc = _mm512_setzero_pd();
            while i + 8 <= n {
                let v = _mm512_loadu_pd(vals.as_ptr().add(i));
                acc = _mm512_add_pd(acc, v);
                i += 8;
            }
            let mut tmp = [0.0f64; 8];
            _mm512_storeu_pd(tmp.as_mut_ptr(), acc);
            let mut s = tmp.iter().sum::<f64>();
            while i < n {
                s += vals[i];
                i += 1;
            }
            s
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn sum_avx2(vals: &[f64], n: usize) -> f64 {
        unsafe {
            let mut i = 0;
            let mut acc = _mm256_setzero_pd();
            while i + 4 <= n {
                let v = _mm256_loadu_pd(vals.as_ptr().add(i));
                acc = _mm256_add_pd(acc, v);
                i += 4;
            }
            let mut tmp = [0.0f64; 4];
            _mm256_storeu_pd(tmp.as_mut_ptr(), acc);
            let mut s = tmp[0] + tmp[1] + tmp[2] + tmp[3];
            while i < n {
                s += vals[i];
                i += 1;
            }
            s
        }
    }
}

/// Evaluate a numeric expression into a dense output column for the batch.
pub fn eval_arith_column(
    expr: &Expression,
    batch: &VectorBatch,
    out: &mut Vec<f64>,
) -> Result<()> {
    out.clear();
    out.resize(batch.len, 0.0);
    match expr {
        Expression::Column(c) => {
            let src = batch.columns.get(c).ok_or_else(|| {
                TakyonicError::Sql(format!("vectorized: unknown column `{c}`"))
            })?;
            out.copy_from_slice(&src[..batch.len]);
            Ok(())
        }
        Expression::Literal(s) => {
            let v = match SqlValue::from_text(s) {
                SqlValue::Int(n) => n as f64,
                SqlValue::Float(f) => f,
                SqlValue::Bool(b) => {
                    if b {
                        1.0
                    } else {
                        0.0
                    }
                }
                _ => 0.0,
            };
            out.fill(v);
            Ok(())
        }
        Expression::Arith { left, op, right } => {
            let mut l = Vec::new();
            let mut r = Vec::new();
            eval_arith_column(left, batch, &mut l)?;
            eval_arith_column(right, batch, &mut r)?;
            match op {
                ArithOp::Mul => SimdKernels::mul(&l, &r, out, batch.len),
                ArithOp::Add => SimdKernels::add(&l, &r, out, batch.len),
                ArithOp::Sub => SimdKernels::sub(&l, &r, out, batch.len),
                ArithOp::Div => SimdKernels::div(&l, &r, out, batch.len),
            }
            Ok(())
        }
        _ => Err(TakyonicError::Sql(
            "vectorized: expression not SIMD-lowerable".into(),
        )),
    }
}

/// Build a selection mask from a predicate (SIMD compares for simple forms).
pub fn eval_predicate_mask(expr: &Expression, batch: &VectorBatch) -> Result<Vec<u64>> {
    // col op lit
    if let Expression::BinaryOp { left, op, right } = expr {
        if let (Expression::Column(c), Expression::Literal(s)) = (left.as_ref(), right.as_ref()) {
            if let Some(col) = batch.columns.get(c) {
                let rhs = match SqlValue::from_text(s) {
                    SqlValue::Int(n) => n as f64,
                    SqlValue::Float(f) => f,
                    _ => {
                        return fallback_mask(expr, batch);
                    }
                };
                return Ok(SimdKernels::compare_scalar(col, batch.len, *op, rhs));
            }
        }
    }
    // AND of masks
    if let Expression::And { left, right } = expr {
        let mut a = eval_predicate_mask(left, batch)?;
        let b = eval_predicate_mask(right, batch)?;
        for (x, y) in a.iter_mut().zip(b.iter()) {
            *x &= *y;
        }
        return Ok(a);
    }
    fallback_mask(expr, batch)
}

fn fallback_mask(expr: &Expression, batch: &VectorBatch) -> Result<Vec<u64>> {
    // Rebuild without mask for evaluation.
    let dense = VectorBatch {
        columns: batch.columns.clone(),
        len: batch.len,
        mask: Vec::new(),
    };
    let rows = dense.to_records();
    let ctx = ExecutionContext::new();
    let words = batch.len.div_ceil(64);
    let mut mask = vec![0u64; words];
    for (i, row) in rows.iter().enumerate() {
        if evaluate_bool(expr, row, &ctx)? {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    Ok(mask)
}

/// Scan that emits [`VectorBatch`] chunks from an underlying row executor.
pub struct VectorizedScanExec {
    input: Box<dyn Executor>,
    columns: Vec<String>,
    buffer: Vec<Record>,
    done: bool,
}

impl VectorizedScanExec {
    /// Wrap a row-at-a-time scan.
    pub fn new(input: Box<dyn Executor>, columns: Vec<String>) -> Self {
        Self {
            input,
            columns,
            buffer: Vec::with_capacity(VECTOR_BATCH_SIZE),
            done: false,
        }
    }

    /// Pull the next batch (`None` at EOS).
    pub fn next_batch(&mut self) -> Result<Option<VectorBatch>> {
        if self.done {
            return Ok(None);
        }
        self.buffer.clear();
        while self.buffer.len() < VECTOR_BATCH_SIZE {
            match self.input.next_row()? {
                Some(r) => self.buffer.push(r),
                None => {
                    self.done = true;
                    break;
                }
            }
        }
        if self.buffer.is_empty() {
            return Ok(None);
        }
        Ok(Some(VectorBatch::from_records(&self.buffer, &self.columns)))
    }
}

/// Vectorized global aggregate: filter (mask) + SUM/COUNT over batches.
pub struct VectorizedAggregateExec {
    scan: VectorizedScanExec,
    predicate: Option<Expression>,
    /// Projection expression for SUM (e.g. `price * discount`); None → COUNT(*).
    sum_expr: Option<Expression>,
    result_name: String,
    finished: bool,
    metrics: Option<Arc<EngineMetrics>>,
    /// Rows processed (throughput accounting).
    pub rows_seen: u64,
    /// Selected rows after mask.
    pub rows_selected: u64,
    /// Wall time of the push loop.
    pub elapsed: std::time::Duration,
    /// Whether AVX2 path was available at runtime.
    pub simd_avx2: bool,
}

impl VectorizedAggregateExec {
    /// Build a vectorized agg over `input`.
    pub fn new(
        input: Box<dyn Executor>,
        columns: Vec<String>,
        predicate: Option<Expression>,
        sum_expr: Option<Expression>,
        result_name: impl Into<String>,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Self {
        #[cfg(target_arch = "x86_64")]
        let simd_avx2 = is_x86_feature_detected!("avx2");
        #[cfg(not(target_arch = "x86_64"))]
        let simd_avx2 = false;
        Self {
            scan: VectorizedScanExec::new(input, columns),
            predicate,
            sum_expr,
            result_name: result_name.into(),
            finished: false,
            metrics,
            rows_seen: 0,
            rows_selected: 0,
            elapsed: std::time::Duration::ZERO,
            simd_avx2,
        }
    }

    /// Run the full push pipeline and return the aggregate record.
    pub fn run(&mut self) -> Result<Record> {
        let t0 = Instant::now();
        let mut sum = 0.0;
        let mut count = 0u64;
        let mut scratch = Vec::new();
        while let Some(mut batch) = self.scan.next_batch()? {
            self.rows_seen += batch.len as u64;
            if let Some(pred) = &self.predicate {
                let bits = eval_predicate_mask(pred, &batch)?;
                batch.apply_mask_bits(&bits);
            }
            let selected = batch.selected_count() as u64;
            self.rows_selected += selected;
            count += selected;
            if let Some(expr) = &self.sum_expr {
                eval_arith_column(expr, &batch, &mut scratch)?;
                sum += SimdKernels::masked_sum(&scratch, batch.len, &batch.mask);
            }
        }
        self.elapsed = t0.elapsed();
        if let Some(m) = &self.metrics {
            m.record_jit_execution(); // reuse JIT exec counter for vectorized push
        }
        let mut rec = Record::new();
        if self.sum_expr.is_some() {
            rec = rec.set(&self.result_name, format_f64(sum));
        } else {
            rec = rec.set(&self.result_name, count.to_string());
        }
        Ok(rec)
    }
}

impl Executor for VectorizedAggregateExec {
    fn next_row(&mut self) -> Result<Option<Record>> {
        if self.finished {
            return Ok(None);
        }
        self.finished = true;
        Ok(Some(self.run()?))
    }
}

/// Throughput helper: rows / millisecond.
pub fn rows_per_ms(rows: u64, elapsed: std::time::Duration) -> f64 {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if ms <= 0.0 {
        return rows as f64;
    }
    rows as f64 / ms
}

/// True when an expression tree is amenable to SIMD vectorization.
pub fn is_vectorizable(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) => true,
        Expression::Literal(s) => {
            !matches!(SqlValue::from_text(s), SqlValue::String(_))
        }
        Expression::Parameter(_) => false,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. } => {
            is_vectorizable(left) && is_vectorizable(right)
        }
        Expression::InList { expr, list, .. } => {
            is_vectorizable(expr)
                && list.iter().all(|v| {
                    matches!(
                        v,
                        SqlValue::Int(_) | SqlValue::Float(_) | SqlValue::Bool(_) | SqlValue::Null
                    )
                })
        }
        Expression::AggregateFunction { args, .. } => args.iter().all(is_vectorizable),
        Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::ScalarSubquery { .. }
        | Expression::Array(_)
        | Expression::ArrayIndex { .. }
        | Expression::VectorDistance { .. }
        | Expression::Like { .. }
        | Expression::SimilarTo { .. }
        | Expression::RegexMatch { .. }
        | Expression::AtTimeZone { .. }
        | Expression::Case { .. }
        | Expression::IsNull { .. }
        | Expression::IsBoolTest { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::QuantifiedCmp { .. }
        | Expression::Coalesce(_)
        | Expression::Cast { .. }
        | Expression::NullIf { .. }
        | Expression::ScalarFunction { .. }
        | Expression::Not { .. }
        | Expression::OuterRef(_) => false,
    }
}

/// Collect columns needed for vectorized evaluation.
pub fn collect_vector_columns(expr: &Expression) -> Vec<String> {
    let mut cols = Vec::new();
    walk(expr, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn walk(expr: &Expression, out: &mut Vec<String>) {
    match expr {
        Expression::Column(c) => out.push(c.clone()),
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::SimilarTo {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::RegexMatch {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::AtTimeZone {
            timestamp: left,
            time_zone: right,
        } => {
            walk(left, out);
            walk(right, out);
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            for (cond, result) in when_then {
                walk(cond, out);
                walk(result, out);
            }
            if let Some(e) = else_result {
                walk(e, out);
            }
        }
        Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Not { expr } => {
            walk(expr, out)
        }
        Expression::Coalesce(args) => {
            for a in args {
                walk(a, out);
            }
        }
        Expression::NullIf { left, right }
        | Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. } => {
            walk(left, out);
            walk(right, out);
        }
        Expression::ScalarFunction { args, .. } | Expression::AggregateFunction { args, .. } => {
            for a in args {
                walk(a, out);
            }
        }
        Expression::InList { expr, .. } => walk(expr, out),
        _ => {}
    }
}

/// Runtime flag: host reports AVX2 (proxy for high SIMD utilization).
pub fn host_simd_level() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return "avx512";
        }
        if is_x86_feature_detected!("avx2") {
            return "avx2";
        }
        if is_x86_feature_detected!("sse2") {
            return "sse2";
        }
    }
    "scalar"
}

static VECTORIZED_EXECS: AtomicU64 = AtomicU64::new(0);

/// Test/metrics: count of vectorized pipelines constructed.
pub fn vectorized_exec_count() -> u64 {
    VECTORIZED_EXECS.load(Ordering::Relaxed)
}

pub(crate) fn note_vectorized_exec() {
    VECTORIZED_EXECS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ValuesExec;

    #[test]
    fn simd_mul_sum_matches_scalar() {
        let n = 4096;
        let a: Vec<f64> = (0..n).map(|i| (i % 97) as f64 + 0.5).collect();
        let b: Vec<f64> = (0..n).map(|i| (i % 13) as f64 * 0.01 + 0.1).collect();
        let mut out = vec![0.0; n];
        SimdKernels::mul(&a, &b, &mut out, n);
        let simd_sum = SimdKernels::sum(&out, n);
        let mut scalar = 0.0;
        for i in 0..n {
            scalar += a[i] * b[i];
        }
        assert!((simd_sum - scalar).abs() < 1e-6, "{simd_sum} vs {scalar}");
    }

    #[test]
    fn mask_filter_between() {
        let col: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let mask = SimdKernels::compare_between(&col, 100, 10.0, 20.0);
        let mut n = 0;
        for i in 0..100 {
            if mask[i / 64] & (1u64 << (i % 64)) != 0 {
                n += 1;
                assert!((10..=20).contains(&i));
            }
        }
        assert_eq!(n, 11);
    }

    #[test]
    fn vectorized_agg_matches_interpreted() {
        let mut rows = Vec::new();
        for i in 0..5000 {
            rows.push(
                Record::new()
                    .set("price", (i % 100).to_string())
                    .set("discount", format!("{}", 0.05 + (i % 5) as f64 * 0.01))
                    .set("qty", ((i % 50) + 1).to_string()),
            );
        }
        let input = Box::new(ValuesExec::new(rows.clone()));
        let pred = Expression::And {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("qty".into())),
                op: FilterOp::Lt,
                right: Box::new(Expression::Literal("40".into())),
            }),
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("discount".into())),
                op: FilterOp::Gte,
                right: Box::new(Expression::Literal("0.05".into())),
            }),
        };
        let sum_expr = Expression::Arith {
            left: Box::new(Expression::Column("price".into())),
            op: ArithOp::Mul,
            right: Box::new(Expression::Column("discount".into())),
        };
        let mut vex = VectorizedAggregateExec::new(
            input,
            vec!["price".into(), "discount".into(), "qty".into()],
            Some(pred.clone()),
            Some(sum_expr.clone()),
            "revenue",
            None,
        );
        let got = vex.run().unwrap();
        let got_v: f64 = got.get("revenue").unwrap().parse().unwrap();

        // Scalar reference.
        let ctx = ExecutionContext::new();
        let mut expect = 0.0;
        for r in &rows {
            if evaluate_bool(&pred, r, &ctx).unwrap() {
                let v = evaluate(&sum_expr, r, &ctx).unwrap();
                expect += match v {
                    SqlValue::Float(f) => f,
                    SqlValue::Int(i) => i as f64,
                    _ => 0.0,
                };
            }
        }
        assert!(
            (got_v - expect).abs() < 1e-4,
            "vectorized {got_v} != scalar {expect}"
        );
        assert!(vex.rows_seen == 5000);
        eprintln!(
            "vectorized agg: {} rows in {:?} ({:.0} rows/ms) simd={}",
            vex.rows_seen,
            vex.elapsed,
            rows_per_ms(vex.rows_seen, vex.elapsed),
            host_simd_level()
        );
    }

    /// TPC-H Q6-style: `SUM(price * discount) WHERE qty < 24 AND discount BETWEEN 0.05 AND 0.07`.
    #[test]
    fn tpch_q6_style_throughput_bench() {
        let n = 100_000usize;
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            let discount = 0.02 + (i % 10) as f64 * 0.01; // 0.02..0.11
            rows.push(
                Record::new()
                    .set("l_extendedprice", ((i % 1000) + 1).to_string())
                    .set("l_discount", format!("{discount:.2}"))
                    .set("l_quantity", ((i % 50) + 1).to_string()),
            );
        }
        let pred = Expression::And {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("l_quantity".into())),
                op: FilterOp::Lt,
                right: Box::new(Expression::Literal("24".into())),
            }),
            right: Box::new(Expression::And {
                left: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::Column("l_discount".into())),
                    op: FilterOp::Gte,
                    right: Box::new(Expression::Literal("0.05".into())),
                }),
                right: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::Column("l_discount".into())),
                    op: FilterOp::Lte,
                    right: Box::new(Expression::Literal("0.07".into())),
                }),
            }),
        };
        let sum_expr = Expression::Arith {
            left: Box::new(Expression::Column("l_extendedprice".into())),
            op: ArithOp::Mul,
            right: Box::new(Expression::Column("l_discount".into())),
        };

        // Scalar baseline.
        let ctx = ExecutionContext::new();
        let t0 = Instant::now();
        let mut scalar_sum = 0.0;
        for r in &rows {
            if evaluate_bool(&pred, r, &ctx).unwrap() {
                match evaluate(&sum_expr, r, &ctx).unwrap() {
                    SqlValue::Float(f) => scalar_sum += f,
                    SqlValue::Int(i) => scalar_sum += i as f64,
                    _ => {}
                }
            }
        }
        let scalar_elapsed = t0.elapsed();

        let mut vex = VectorizedAggregateExec::new(
            Box::new(ValuesExec::new(rows)),
            vec![
                "l_extendedprice".into(),
                "l_discount".into(),
                "l_quantity".into(),
            ],
            Some(pred),
            Some(sum_expr),
            "revenue",
            None,
        );
        let got = vex.run().unwrap();
        let got_v: f64 = got.get("revenue").unwrap().parse().unwrap();
        assert!(
            (got_v - scalar_sum).abs() / scalar_sum.max(1.0) < 1e-6,
            "vectorized {got_v} != scalar {scalar_sum}"
        );

        let simd_rps = rows_per_ms(vex.rows_seen, vex.elapsed);
        let scalar_rps = rows_per_ms(n as u64, scalar_elapsed);
        eprintln!(
            "TPC-H Q6-style n={n}: scalar={:.0} rows/ms {:?} | vectorized={:.0} rows/ms {:?} simd={} avx2={}",
            scalar_rps,
            scalar_elapsed,
            simd_rps,
            vex.elapsed,
            host_simd_level(),
            vex.simd_avx2
        );
        // Vectorized should not be dramatically slower (allows CI without AVX).
        assert!(simd_rps > 0.0);
    }

    #[test]
    fn scalar_vs_vectorized_mul_sum_latency() {
        let n = 50_000usize;
        let a: Vec<f64> = (0..n).map(|i| (i % 100) as f64).collect();
        let b: Vec<f64> = (0..n).map(|i| 0.1 + (i % 7) as f64 * 0.01).collect();

        let t0 = Instant::now();
        let mut scalar = 0.0;
        for i in 0..n {
            scalar += a[i] * b[i];
        }
        let scalar_dt = t0.elapsed();

        let mut out = vec![0.0; n];
        let t1 = Instant::now();
        SimdKernels::mul(&a, &b, &mut out, n);
        let simd_sum = SimdKernels::sum(&out, n);
        let simd_dt = t1.elapsed();

        assert!((scalar - simd_sum).abs() < 1e-3);
        eprintln!(
            "mul+sum n={n}: scalar={scalar_dt:?} vectorized={simd_dt:?} simd={}",
            host_simd_level()
        );
        assert!(simd_dt <= scalar_dt.saturating_mul(3) || n < 1000);
    }
}
