# Unsafe / SIMD / JIT Memory-Safety Audit

Güncelleme: 2026-07-19

Independent review of `unsafe` blocks, SIMD kernels (AVX2/AVX-512), and
Cranelift JIT (`src/jit.rs`). Complements unit tests with an explicit checklist.

## Inventory

| Site | File | Kind | Precondition documented? | Equivalence test? | Status |
|------|------|------|--------------------------|-------------------|--------|
| `mul8_avx512` / `add8` / `sub8` / `sum_*` | `src/vectorized.rs` | SIMD intrinsics | TBD | Task 8 | Open |
| `euclidean_avx2` / `euclidean_sse` | `src/vector.rs` | SIMD distance | TBD | existing throughput + Task 8 | Open |
| `JitScalarFn` / `JitBatchBinOpFn` transmute | `src/jit.rs` | JIT fn ptr | TBD | Task 8 | Open |
| JIT scratch call sites | `src/jit.rs` | raw ptr + len | TBD | Task 8 | Open |
| SST `MmapOptions::map` | `src/sst.rs` | mmap | OS file lifetime | existing SST tests | Open |
| page zeroing / alloc | `src/page.rs` | layout | TBD | existing page tests | Open |
| test-only `env::set_var` | `src/reliability/mod.rs` | test | N/A | N/A | Accepted |

## Checklist (reviewer must answer Yes/No + notes)

### SIMD kernels (`vectorized.rs` / `vector.rs`)

- [ ] Every `unsafe` intrinsic call is gated by `is_x86_feature_detected!` **and** length ≥ lane width.
- [ ] Pointers passed to `_mm256_*` / `_mm512_*` are derived from `&[T]` / `&mut [T]` with `n` bounds checked (or `debug_assert!` + proven loop invariant).
- [ ] No aliasing of mutable output with inputs unless `out` is disjoint (document if in-place forbidden).
- [ ] Scalar tail handles `n % lane != 0`; empty `n=0` is safe.
- [ ] Float NaN/Inf behavior matches scalar (document if not required).

### Cranelift JIT (`jit.rs`)

- [ ] `transmute` of `*const u8` → fn pointer only on pointers returned by `JITModule::get_finalized_function`.
- [ ] Scalar JIT: scratch buffer length ≥ max column index read by compiled code.
- [ ] Batch JIT: `len` argument matches all three slice lengths; no call with `len > slice.len()`.
- [ ] Fallback path used when type is not JIT-lowerable (String etc.) — no partial compile UB.
- [ ] Module drop order: no fn pointer use after `JitCompiler` drop.

### mmap / pages

- [ ] Mmap lifetime ≤ file handle / SST registration; no use-after-unmap.
- [ ] Page `unsafe` only for alignment/zeroing with size == `PAGE_SIZE`.

## Findings

_(Filled in Task 9 after review pass.)_

## Residual risk

- Multi-day continuous soak may still miss rare JIT/SIMD races under concurrent BPM — tracked via Workstream A heartbeats.
