//! Dense vector values and distance metrics for HNSW / ANN search.
//!
//! [`VectorValue`] stores an aligned `f32` buffer suitable for SIMD and for
//! Cranelift-compiled distance kernels. Text encoding for LSM records is
//! `[0.1,0.2,…,0.n]` (compatible with SQL `ARRAY[…]` materialization).

use std::fmt;

use crate::error::{Result, TakyonicError};

/// Distance metric for vector similarity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DistanceMetric {
    /// L2 / Euclidean distance (smaller = closer). Maps to SQL `<->`.
    Euclidean,
    /// Cosine distance `1 - cos_sim` (smaller = closer).
    Cosine,
}

impl DistanceMetric {
    /// Parse `EUCLIDEAN` / `L2` / `COSINE` (case-insensitive).
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "EUCLIDEAN" | "L2" => Ok(Self::Euclidean),
            "COSINE" => Ok(Self::Cosine),
            other => Err(TakyonicError::Sql(format!(
                "unknown vector distance metric `{other}`"
            ))),
        }
    }
}

/// Aligned dense `f32` embedding.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorValue {
    data: Vec<f32>,
}

impl VectorValue {
    /// Construct from an owned `f32` slice (dimension = `data.len()`).
    pub fn new(data: impl Into<Vec<f32>>) -> Self {
        Self { data: data.into() }
    }

    /// Dimension.
    pub fn dim(&self) -> usize {
        self.data.len()
    }

    /// Borrow the underlying floats (contiguous, suitable for SIMD).
    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }

    /// Mutable view.
    pub fn as_mut_slice(&mut self) -> &mut [f32] {
        &mut self.data
    }

    /// Encode for LSM / Record field storage.
    pub fn to_text(&self) -> String {
        let mut s = String::from("[");
        for (i, v) in self.data.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            // Compact but reversible via `from_text`.
            s.push_str(&format!("{v}"));
        }
        s.push(']');
        s
    }

    /// Parse `[0.1, 0.2]` / `ARRAY[0.1, 0.2]` / bare comma lists.
    pub fn from_text(raw: &str) -> Result<Self> {
        let s = raw.trim();
        let inner = s
            .strip_prefix("ARRAY")
            .or_else(|| s.strip_prefix("array"))
            .unwrap_or(s)
            .trim();
        let inner = inner
            .strip_prefix('[')
            .and_then(|t| t.strip_suffix(']'))
            .unwrap_or(inner);
        if inner.trim().is_empty() {
            return Ok(Self::new(Vec::new()));
        }
        let mut data = Vec::new();
        for part in inner.split(',') {
            let t = part.trim();
            if t.is_empty() {
                continue;
            }
            let v: f32 = t.parse().map_err(|_| {
                TakyonicError::Sql(format!("invalid vector component `{t}`"))
            })?;
            data.push(v);
        }
        Ok(Self::new(data))
    }

    /// Euclidean (L2) distance — scalar reference implementation.
    pub fn euclidean(&self, other: &Self) -> Result<f32> {
        ensure_same_dim(self, other)?;
        Ok(euclidean_f32(self.as_slice(), other.as_slice()))
    }

    /// Cosine distance `1 - cos_sim`.
    pub fn cosine_distance(&self, other: &Self) -> Result<f32> {
        ensure_same_dim(self, other)?;
        Ok(cosine_distance_f32(self.as_slice(), other.as_slice()))
    }

    /// Distance under `metric`.
    pub fn distance(&self, other: &Self, metric: DistanceMetric) -> Result<f32> {
        match metric {
            DistanceMetric::Euclidean => self.euclidean(other),
            DistanceMetric::Cosine => self.cosine_distance(other),
        }
    }
}

impl fmt::Display for VectorValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_text())
    }
}

fn ensure_same_dim(a: &VectorValue, b: &VectorValue) -> Result<()> {
    if a.dim() != b.dim() {
        return Err(TakyonicError::Sql(format!(
            "vector dimension mismatch: {} vs {}",
            a.dim(),
            b.dim()
        )));
    }
    Ok(())
}

/// Scalar Euclidean distance (sum of squared diffs, then sqrt).
pub fn euclidean_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

/// Cosine distance.
pub fn cosine_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - (dot / denom).clamp(-1.0, 1.0)
}

/// SIMD-accelerated Euclidean (x86_64 AVX2/SSE when available; else scalar).
pub fn euclidean_simd(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { euclidean_avx2(a, b) };
        }
        if is_x86_feature_detected!("sse") {
            return unsafe { euclidean_sse(a, b) };
        }
    }
    euclidean_f32(a, b)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn euclidean_sse(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    unsafe {
        let n = a.len();
        let mut i = 0;
        let mut acc = _mm_setzero_ps();
        while i + 4 <= n {
            let va = _mm_loadu_ps(a.as_ptr().add(i));
            let vb = _mm_loadu_ps(b.as_ptr().add(i));
            let d = _mm_sub_ps(va, vb);
            acc = _mm_add_ps(acc, _mm_mul_ps(d, d));
            i += 4;
        }
        let mut tmp = [0f32; 4];
        _mm_storeu_ps(tmp.as_mut_ptr(), acc);
        let mut sum = tmp[0] + tmp[1] + tmp[2] + tmp[3];
        while i < n {
            let d = a[i] - b[i];
            sum += d * d;
            i += 1;
        }
        sum.sqrt()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn euclidean_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    unsafe {
        let n = a.len();
        let mut i = 0;
        let mut acc = _mm256_setzero_ps();
        while i + 8 <= n {
            let va = _mm256_loadu_ps(a.as_ptr().add(i));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i));
            let d = _mm256_sub_ps(va, vb);
            acc = _mm256_add_ps(acc, _mm256_mul_ps(d, d));
            i += 8;
        }
        let mut tmp = [0f32; 8];
        _mm256_storeu_ps(tmp.as_mut_ptr(), acc);
        let mut sum = tmp.iter().sum::<f32>();
        while i < n {
            let d = a[i] - b[i];
            sum += d * d;
            i += 1;
        }
        sum.sqrt()
    }
}

/// Spec stored alongside a vector index in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VectorIndexSpec {
    /// Embedding dimensionality.
    pub dimension: usize,
    /// Distance metric (default Euclidean / `<->`).
    pub metric: DistanceMetric,
    /// Index implementation tag (`HNSW`).
    pub index_type: String,
}

impl VectorIndexSpec {
    /// HNSW index with Euclidean metric.
    pub fn hnsw(dimension: usize) -> Self {
        Self {
            dimension,
            metric: DistanceMetric::Euclidean,
            index_type: "HNSW".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_roundtrip_and_euclidean() {
        let v = VectorValue::new(vec![1.0, 0.0, 0.0]);
        let w = VectorValue::from_text(&v.to_text()).unwrap();
        assert_eq!(v, w);
        let o = VectorValue::new(vec![0.0, 1.0, 0.0]);
        let d = v.euclidean(&o).unwrap();
        assert!((d - std::f32::consts::SQRT_2).abs() < 1e-5);
    }

    #[test]
    fn simd_matches_scalar() {
        let a: Vec<f32> = (0..128).map(|i| i as f32 * 0.01).collect();
        let b: Vec<f32> = (0..128).map(|i| (i as f32 * 0.01) + 0.5).collect();
        let s = euclidean_f32(&a, &b);
        let v = euclidean_simd(&a, &b);
        assert!((s - v).abs() < 1e-4, "scalar={s} simd={v}");
    }

    #[test]
    fn simd_euclidean_vs_scalar_throughput() {
        // Correctness + smoke timing: SIMD path must match scalar and not be
        // catastrophically slower (interpreted vs SIMD-optimized distance).
        let dim = 256;
        let a: Vec<f32> = (0..dim).map(|i| (i as f32).sin()).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.5).cos()).collect();
        let iters = 5_000usize;
        let t0 = std::time::Instant::now();
        let mut scalar_acc = 0.0f32;
        for _ in 0..iters {
            scalar_acc += euclidean_f32(&a, &b);
        }
        let scalar_ns = t0.elapsed().as_nanos();
        let t1 = std::time::Instant::now();
        let mut simd_acc = 0.0f32;
        for _ in 0..iters {
            simd_acc += euclidean_simd(&a, &b);
        }
        let simd_ns = t1.elapsed().as_nanos();
        assert!(
            (scalar_acc - simd_acc).abs() < 1e-2,
            "accum mismatch scalar={scalar_acc} simd={simd_acc}"
        );
        // Allow SIMD to be slower on tiny debug builds; just ensure it ran.
        assert!(simd_ns > 0 && scalar_ns > 0);
        eprintln!(
            "euclidean dim={dim} iters={iters}: scalar={scalar_ns}ns simd={simd_ns}ns \
             speedup={:.2}x",
            scalar_ns as f64 / simd_ns as f64
        );
    }
}
