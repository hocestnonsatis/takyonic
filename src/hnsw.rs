//! Hierarchical Navigable Small World (HNSW) graph for approximate k-NN.
//!
//! The graph is held in memory and snapshotted to `data_dir/HNSW_<index>` so
//! B-Tree / LSM leaves can reference a durable blob. Nodes are addressed by
//! dense `u32` ids; each node stores a primary-key string, the embedding, and
//! per-layer neighbour lists.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use tracing::debug;

use crate::error::{Result, TakyonicError};
use crate::vector::{DistanceMetric, VectorValue, euclidean_simd};

const MAGIC: &[u8; 4] = b"HNSW";
const VERSION: u32 = 1;

/// Default HNSW construction parameters (good for small/medium embeddings).
const DEFAULT_M: usize = 16;
const DEFAULT_EF_CONSTRUCTION: usize = 64;
const DEFAULT_EF_SEARCH: usize = 50;

/// One graph node.
#[derive(Clone, Debug)]
struct HnswNode {
    pk: String,
    vector: VectorValue,
    /// `neighbors[layer]` = outgoing edges at that layer.
    neighbors: Vec<Vec<u32>>,
}

/// Thread-safe HNSW index.
pub struct HnswIndex {
    name: String,
    dimension: usize,
    metric: DistanceMetric,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    ml: f64,
    inner: RwLock<HnswInner>,
    next_id: AtomicU32,
}

struct HnswInner {
    nodes: HashMap<u32, HnswNode>,
    /// pk → node id (for delete / upsert).
    by_pk: HashMap<String, u32>,
    entry_point: Option<u32>,
}

impl HnswIndex {
    /// Create an empty index.
    pub fn new(name: impl Into<String>, dimension: usize, metric: DistanceMetric) -> Self {
        let m = DEFAULT_M;
        Self {
            name: name.into(),
            dimension,
            metric,
            m,
            ef_construction: DEFAULT_EF_CONSTRUCTION,
            ef_search: DEFAULT_EF_SEARCH,
            ml: 1.0 / (m as f64).ln().max(1e-6),
            inner: RwLock::new(HnswInner {
                nodes: HashMap::new(),
                by_pk: HashMap::new(),
                entry_point: None,
            }),
            next_id: AtomicU32::new(1),
        }
    }

    /// Index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Embedding dimension.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Number of live nodes.
    pub fn len(&self) -> usize {
        self.inner.read().nodes.len()
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert or replace a vector for `pk`.
    pub fn insert(&self, pk: impl Into<String>, vector: VectorValue) -> Result<()> {
        let pk = pk.into();
        if vector.dim() != self.dimension {
            return Err(TakyonicError::Sql(format!(
                "vector dim {} != index dim {}",
                vector.dim(),
                self.dimension
            )));
        }
        // Upsert: remove old node first.
        self.delete(&pk);

        let level = self.random_level();
        let id = self.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let mut neighbors = vec![Vec::new(); level + 1];

        let mut inner = self.inner.write();
        if inner.entry_point.is_none() {
            neighbors.resize(level + 1, Vec::new());
            inner.nodes.insert(
                id,
                HnswNode {
                    pk: pk.clone(),
                    vector,
                    neighbors,
                },
            );
            inner.by_pk.insert(pk, id);
            inner.entry_point = Some(id);
            return Ok(());
        }

        let entry = inner.entry_point.expect("checked");
        let entry_level = inner.nodes[&entry].neighbors.len().saturating_sub(1);

        // Greedy search from top to `level+1`.
        let mut curr = entry;
        for lc in (level + 1..=entry_level).rev() {
            curr = self.search_layer_greedy(&inner, &vector, curr, lc);
        }

        // Insert into layers `level` … 0 with ef_construction.
        for lc in (0..=level.min(entry_level)).rev() {
            let candidates =
                self.search_layer(&inner, &vector, curr, self.ef_construction, lc);
            let selected = self.select_neighbors(&inner, &vector, &candidates, self.m);
            neighbors[lc] = selected.clone();
            // Bidirectional links (collect prune work to avoid borrow conflicts).
            let mut prunes: Vec<(u32, Vec<u32>)> = Vec::new();
            for &nb in &selected {
                if let Some(node) = inner.nodes.get_mut(&nb) {
                    if node.neighbors.len() <= lc {
                        node.neighbors.resize(lc + 1, Vec::new());
                    }
                    if !node.neighbors[lc].contains(&id) {
                        node.neighbors[lc].push(id);
                        if node.neighbors[lc].len() > self.m {
                            prunes.push((nb, node.neighbors[lc].clone()));
                        }
                    }
                }
            }
            for (nb, edges) in prunes {
                let nb_vec = inner.nodes[&nb].vector.clone();
                let pruned = self.select_neighbors_ids(&inner, &nb_vec, &edges, self.m);
                if let Some(n2) = inner.nodes.get_mut(&nb) {
                    n2.neighbors[lc] = pruned;
                }
            }
            if let Some(&(nearest, _)) = candidates.first() {
                curr = nearest;
            }
        }

        // Raise entry point if this node is taller.
        if level > entry_level {
            inner.entry_point = Some(id);
        }

        inner.nodes.insert(
            id,
            HnswNode {
                pk: pk.clone(),
                vector,
                neighbors,
            },
        );
        inner.by_pk.insert(pk, id);
        Ok(())
    }

    /// Remove a node by primary key (no-op if missing).
    pub fn delete(&self, pk: &str) {
        let mut inner = self.inner.write();
        let Some(id) = inner.by_pk.remove(pk) else {
            return;
        };
        let Some(node) = inner.nodes.remove(&id) else {
            return;
        };
        // Drop inbound edges.
        for (layer, outs) in node.neighbors.iter().enumerate() {
            for &nb in outs {
                if let Some(n) = inner.nodes.get_mut(&nb) {
                    if layer < n.neighbors.len() {
                        n.neighbors[layer].retain(|&x| x != id);
                    }
                }
            }
        }
        if inner.entry_point == Some(id) {
            inner.entry_point = inner.nodes.keys().next().copied();
            // Prefer tallest remaining node.
            let mut best: Option<(usize, u32)> = None;
            for (&nid, n) in &inner.nodes {
                let h = n.neighbors.len();
                if best.map(|(h0, _)| h > h0).unwrap_or(true) {
                    best = Some((h, nid));
                }
            }
            inner.entry_point = best.map(|(_, id)| id);
        }
    }

    /// Prune nodes whose primary keys are in `dead` (VACUUM helper).
    pub fn prune_pks(&self, dead: &HashSet<String>) -> usize {
        let victims: Vec<String> = {
            let inner = self.inner.read();
            inner
                .by_pk
                .keys()
                .filter(|pk| dead.contains(pk.as_str()))
                .cloned()
                .collect()
        };
        let n = victims.len();
        for pk in victims {
            self.delete(&pk);
        }
        n
    }

    /// Keep only nodes whose PK is in `live`; prune the rest.
    pub fn retain_pks(&self, live: &HashSet<String>) -> usize {
        let dead: HashSet<String> = {
            let inner = self.inner.read();
            inner
                .by_pk
                .keys()
                .filter(|pk| !live.contains(pk.as_str()))
                .cloned()
                .collect()
        };
        self.prune_pks(&dead)
    }

    /// k-NN search; returns `(distance, pk)` ascending by distance.
    pub fn search_knn(&self, query: &VectorValue, k: usize) -> Result<Vec<(f32, String)>> {
        if query.dim() != self.dimension {
            return Err(TakyonicError::Sql(format!(
                "query dim {} != index dim {}",
                query.dim(),
                self.dimension
            )));
        }
        let inner = self.inner.read();
        if inner.nodes.is_empty() {
            return Ok(Vec::new());
        }
        // Exact scan for small graphs (perfect recall); HNSW for larger corpora.
        if inner.nodes.len() <= 256 {
            return Ok(self.exact_knn(&inner, query, k));
        }
        let Some(entry) = inner.entry_point else {
            return Ok(Vec::new());
        };
        let top = inner.nodes[&entry].neighbors.len().saturating_sub(1);
        let mut curr = entry;
        for lc in (1..=top).rev() {
            curr = self.search_layer_greedy(&inner, query, curr, lc);
        }
        let candidates = self.search_layer(&inner, query, curr, self.ef_search.max(k), 0);
        let mut out: Vec<(f32, String)> = candidates
            .into_iter()
            .take(k)
            .filter_map(|(id, dist)| {
                inner
                    .nodes
                    .get(&id)
                    .map(|n| (dist, n.pk.clone()))
            })
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        out.truncate(k);
        Ok(out)
    }

    fn exact_knn(
        &self,
        inner: &HnswInner,
        query: &VectorValue,
        k: usize,
    ) -> Vec<(f32, String)> {
        let mut scored: Vec<(f32, String)> = inner
            .nodes
            .values()
            .map(|n| (self.dist(query, &n.vector), n.pk.clone()))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        scored.truncate(k);
        scored
    }

    /// Snapshot path under `data_dir`.
    pub fn snapshot_path(data_dir: &Path, name: &str) -> PathBuf {
        data_dir.join(format!("HNSW_{name}"))
    }

    /// Persist graph to disk.
    pub fn save(&self, data_dir: &Path) -> Result<()> {
        let path = Self::snapshot_path(data_dir, &self.name);
        let bytes = self.encode()?;
        let tmp = path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        debug!(index = %self.name, nodes = self.len(), "HNSW snapshot saved");
        Ok(())
    }

    /// Load graph from disk (empty index if missing).
    pub fn load(
        data_dir: &Path,
        name: impl Into<String>,
        dimension: usize,
        metric: DistanceMetric,
    ) -> Result<Self> {
        let name = name.into();
        let path = Self::snapshot_path(data_dir, &name);
        if !path.exists() {
            return Ok(Self::new(name, dimension, metric));
        }
        let mut buf = Vec::new();
        fs::File::open(&path)?.read_to_end(&mut buf)?;
        Self::decode(&name, dimension, metric, &buf)
    }

    fn encode(&self) -> Result<Bytes> {
        let inner = self.inner.read();
        let mut buf = BytesMut::new();
        buf.put_slice(MAGIC);
        buf.put_u32_le(VERSION);
        buf.put_u32_le(self.dimension as u32);
        buf.put_u8(match self.metric {
            DistanceMetric::Euclidean => 0,
            DistanceMetric::Cosine => 1,
        });
        buf.put_u32_le(inner.nodes.len() as u32);
        buf.put_u32_le(inner.entry_point.unwrap_or(0));
        buf.put_u32_le(self.next_id.load(AtomicOrdering::Relaxed));
        for (&id, node) in &inner.nodes {
            buf.put_u32_le(id);
            let pk = node.pk.as_bytes();
            buf.put_u32_le(pk.len() as u32);
            buf.put_slice(pk);
            buf.put_u32_le(node.vector.dim() as u32);
            for &f in node.vector.as_slice() {
                buf.put_f32_le(f);
            }
            buf.put_u32_le(node.neighbors.len() as u32);
            for layer in &node.neighbors {
                buf.put_u32_le(layer.len() as u32);
                for &n in layer {
                    buf.put_u32_le(n);
                }
            }
        }
        Ok(buf.freeze())
    }

    fn decode(
        name: &str,
        dimension: usize,
        metric: DistanceMetric,
        bytes: &[u8],
    ) -> Result<Self> {
        let mut buf = bytes;
        if buf.remaining() < 4 || &buf[..4] != MAGIC {
            return Err(TakyonicError::Integrity("bad HNSW magic".into()));
        }
        buf.advance(4);
        let ver = buf.get_u32_le();
        if ver != VERSION {
            return Err(TakyonicError::Integrity(format!(
                "unsupported HNSW version {ver}"
            )));
        }
        let dim = buf.get_u32_le() as usize;
        if dim != dimension {
            return Err(TakyonicError::Integrity(format!(
                "HNSW dim mismatch file={dim} expected={dimension}"
            )));
        }
        let _metric_tag = buf.get_u8();
        let n_nodes = buf.get_u32_le() as usize;
        let entry = buf.get_u32_le();
        let next_id = buf.get_u32_le();
        let index = Self::new(name.to_string(), dimension, metric);
        index
            .next_id
            .store(next_id.max(1), AtomicOrdering::Relaxed);
        let mut inner = index.inner.write();
        for _ in 0..n_nodes {
            let id = buf.get_u32_le();
            let pk_len = buf.get_u32_le() as usize;
            let pk = String::from_utf8(buf.copy_to_bytes(pk_len).to_vec())
                .map_err(|e| TakyonicError::Integrity(format!("hnsw pk utf8: {e}")))?;
            let vdim = buf.get_u32_le() as usize;
            let mut data = Vec::with_capacity(vdim);
            for _ in 0..vdim {
                data.push(buf.get_f32_le());
            }
            let n_layers = buf.get_u32_le() as usize;
            let mut neighbors = Vec::with_capacity(n_layers);
            for _ in 0..n_layers {
                let n_edges = buf.get_u32_le() as usize;
                let mut edges = Vec::with_capacity(n_edges);
                for _ in 0..n_edges {
                    edges.push(buf.get_u32_le());
                }
                neighbors.push(edges);
            }
            inner.by_pk.insert(pk.clone(), id);
            inner.nodes.insert(
                id,
                HnswNode {
                    pk,
                    vector: VectorValue::new(data),
                    neighbors,
                },
            );
        }
        inner.entry_point = if entry == 0 { None } else { Some(entry) };
        drop(inner);
        Ok(index)
    }

    fn random_level(&self) -> usize {
        // Deterministic-ish from next_id to keep tests stable-ish; still random-like.
        let r: f64 = {
            let x = self.next_id.load(AtomicOrdering::Relaxed) as u64;
            let z = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            ((z >> 33) as f64) / ((1u64 << 31) as f64)
        };
        let lvl = (-r.ln() * self.ml).floor() as usize;
        lvl.min(16)
    }

    fn dist(&self, a: &VectorValue, b: &VectorValue) -> f32 {
        match self.metric {
            DistanceMetric::Euclidean => euclidean_simd(a.as_slice(), b.as_slice()),
            DistanceMetric::Cosine => a.cosine_distance(b).unwrap_or(1.0),
        }
    }

    fn search_layer_greedy(
        &self,
        inner: &HnswInner,
        query: &VectorValue,
        enter: u32,
        layer: usize,
    ) -> u32 {
        let mut curr = enter;
        let mut curr_dist = self.dist(query, &inner.nodes[&curr].vector);
        loop {
            let mut changed = false;
            let neighbors = inner
                .nodes
                .get(&curr)
                .map(|n| n.neighbors.get(layer).cloned().unwrap_or_default())
                .unwrap_or_default();
            for nb in neighbors {
                let d = self.dist(query, &inner.nodes[&nb].vector);
                if d < curr_dist {
                    curr = nb;
                    curr_dist = d;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        curr
    }

    /// Returns candidates sorted by ascending distance: `(id, dist)`.
    fn search_layer(
        &self,
        inner: &HnswInner,
        query: &VectorValue,
        enter: u32,
        ef: usize,
        layer: usize,
    ) -> Vec<(u32, f32)> {
        #[derive(Clone)]
        struct Cand {
            dist: f32,
            id: u32,
        }
        impl PartialEq for Cand {
            fn eq(&self, o: &Self) -> bool {
                self.id == o.id
            }
        }
        impl Eq for Cand {}
        impl PartialOrd for Cand {
            fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
                Some(self.cmp(o))
            }
        }
        impl Ord for Cand {
            fn cmp(&self, o: &Self) -> Ordering {
                // Max-heap by distance for the "furthest in result set".
                o.dist
                    .partial_cmp(&self.dist)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| self.id.cmp(&o.id))
            }
        }

        let mut visited = HashSet::new();
        // C = candidates (min-dist via MinCand Ord), W = found (max-dist via Cand Ord).
        #[derive(Clone)]
        struct MinCand {
            dist: f32,
            id: u32,
        }
        impl PartialEq for MinCand {
            fn eq(&self, o: &Self) -> bool {
                self.id == o.id
            }
        }
        impl Eq for MinCand {}
        impl PartialOrd for MinCand {
            fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
                Some(self.cmp(o))
            }
        }
        impl Ord for MinCand {
            fn cmp(&self, o: &Self) -> Ordering {
                // Reverse so BinaryHeap pops smallest distance first.
                o.dist
                    .partial_cmp(&self.dist)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| o.id.cmp(&self.id))
            }
        }

        let d0 = self.dist(query, &inner.nodes[&enter].vector);
        visited.insert(enter);
        let mut w: BinaryHeap<Cand> = BinaryHeap::new();
        w.push(Cand {
            dist: d0,
            id: enter,
        });
        let mut candidates: BinaryHeap<MinCand> = BinaryHeap::new();
        candidates.push(MinCand {
            dist: d0,
            id: enter,
        });

        while let Some(MinCand { dist: c_dist, id: c }) = candidates.pop() {
            let f_dist = w.peek().map(|c| c.dist).unwrap_or(f32::MAX);
            if c_dist > f_dist {
                break;
            }
            let neighbors = inner
                .nodes
                .get(&c)
                .map(|n| n.neighbors.get(layer).cloned().unwrap_or_default())
                .unwrap_or_default();
            for e in neighbors {
                if !visited.insert(e) {
                    continue;
                }
                let d = self.dist(query, &inner.nodes[&e].vector);
                let f_dist = w.peek().map(|c| c.dist).unwrap_or(f32::MAX);
                if d < f_dist || w.len() < ef {
                    candidates.push(MinCand { dist: d, id: e });
                    w.push(Cand { dist: d, id: e });
                    if w.len() > ef {
                        w.pop();
                    }
                }
            }
        }

        let mut out: Vec<(u32, f32)> = w.into_iter().map(|c| (c.id, c.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        out
    }

    fn select_neighbors(
        &self,
        _inner: &HnswInner,
        _query: &VectorValue,
        candidates: &[(u32, f32)],
        m: usize,
    ) -> Vec<u32> {
        candidates.iter().take(m).map(|(id, _)| *id).collect()
    }

    fn select_neighbors_ids(
        &self,
        inner: &HnswInner,
        query: &VectorValue,
        ids: &[u32],
        m: usize,
    ) -> Vec<u32> {
        let mut scored: Vec<(u32, f32)> = ids
            .iter()
            .filter_map(|&id| {
                inner
                    .nodes
                    .get(&id)
                    .map(|n| (id, self.dist(query, &n.vector)))
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        self.select_neighbors(inner, query, &scored, m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hnsw_2d_knn_finds_nearest() {
        let idx = HnswIndex::new("t", 2, DistanceMetric::Euclidean);
        for i in 0..20 {
            let x = (i as f32) * 0.1;
            idx.insert(format!("a{i}"), VectorValue::new(vec![x, x]))
                .unwrap();
        }
        for i in 0..20 {
            let x = 10.0 + (i as f32) * 0.1;
            idx.insert(format!("b{i}"), VectorValue::new(vec![x, x]))
                .unwrap();
        }
        let q = VectorValue::new(vec![0.05, 0.05]);
        let hits = idx.search_knn(&q, 5).unwrap();
        assert_eq!(hits.len(), 5);
        for (_d, pk) in &hits {
            assert!(pk.starts_with('a'), "expected near-origin ids, got {pk}");
        }
        let q2 = VectorValue::new(vec![10.2, 10.2]);
        let hits2 = idx.search_knn(&q2, 3).unwrap();
        for (_d, pk) in &hits2 {
            assert!(pk.starts_with('b'), "expected far-cluster ids, got {pk}");
        }
    }

    #[test]
    fn hnsw_3d_layer_navigation_and_knn() {
        let idx = HnswIndex::new("cube", 3, DistanceMetric::Euclidean);
        // Grid of points in [0,1]^3
        for x in 0..5 {
            for y in 0..5 {
                for z in 0..5 {
                    let pk = format!("{x}_{y}_{z}");
                    idx.insert(
                        pk,
                        VectorValue::new(vec![x as f32, y as f32, z as f32]),
                    )
                    .unwrap();
                }
            }
        }
        assert_eq!(idx.len(), 125);
        let q = VectorValue::new(vec![2.1, 2.0, 1.9]);
        let hits = idx.search_knn(&q, 4).unwrap();
        assert_eq!(hits.len(), 4);
        // Nearest should be around (2,2,2)
        assert!(
            hits[0].1.contains('2'),
            "nearest should be near (2,2,2), got {:?}",
            hits[0]
        );
        // Distances strictly non-decreasing
        for w in hits.windows(2) {
            assert!(w[0].0 <= w[1].0 + 1e-5);
        }
    }

    #[test]
    fn hnsw_snapshot_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "takyonic-hnsw-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let idx = HnswIndex::new("v", 3, DistanceMetric::Euclidean);
        idx.insert("p1", VectorValue::new(vec![1.0, 0.0, 0.0]))
            .unwrap();
        idx.insert("p2", VectorValue::new(vec![0.0, 1.0, 0.0]))
            .unwrap();
        idx.save(&dir).unwrap();
        let loaded =
            HnswIndex::load(&dir, "v", 3, DistanceMetric::Euclidean).unwrap();
        assert_eq!(loaded.len(), 2);
        let hits = loaded
            .search_knn(&VectorValue::new(vec![0.9, 0.1, 0.0]), 1)
            .unwrap();
        assert_eq!(hits[0].1, "p1");
        let _ = fs::remove_dir_all(dir);
    }
}
