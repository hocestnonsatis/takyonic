# Unsafe / SIMD / JIT Memory-Safety Audit

Güncelleme: 2026-07-19

Independent review of `unsafe` blocks, SIMD kernels (AVX2/AVX-512), and
Cranelift JIT (`src/jit.rs`). Complements unit tests with an explicit checklist
and `reliability::props::simd_jit` equivalence properties.

## Inventory

| Site | File | Kind | Precondition documented? | Equivalence test? | Status |
|------|------|------|--------------------------|-------------------|--------|
| `mul8_avx512` / `add8` / `sub8` / `sum_*` | `src/vectorized.rs` | SIMD intrinsics | Yes (feature detect + loop `i + width <= n`) | `prop_simd_*` | Reviewed |
| `euclidean_avx2` / `euclidean_sse` | `src/vector.rs` | SIMD distance | Yes (`is_x86_feature_detected!` + len loops) | `prop_euclidean_simd_matches_scalar` | Reviewed |
| `JitScalarFn` / `JitBatchBinOpFn` transmute | `src/jit.rs` | JIT fn ptr | Yes (only after `get_finalized_function`) | `prop_jit_batch_mul_matches_scalar` | Reviewed |
| JIT scratch call sites | `src/jit.rs` | raw ptr + len | Yes (scratch sized to column set / `n`) | existing + prop | Reviewed |
| SST `MmapOptions::map` | `src/sst.rs` | mmap | Yes (via `SstRegistry` pin lifetime) | existing SST tests | Reviewed |
| page `aligned_zeroed` | `src/page.rs` | alloc | Yes (power-of-two len == align) | `page_is_aligned` | Reviewed |
| test-only `env::set_var` | `src/reliability/*.rs` | test | N/A | N/A | Accepted |

## Checklist

### SIMD kernels (`vectorized.rs` / `vector.rs`)

- [x] Every `unsafe` intrinsic call is gated by `is_x86_feature_detected!` **and** length ≥ lane width.
- [x] Pointers passed to `_mm256_*` / `_mm512_*` are derived from `&[T]` / `&mut [T]` with `n` bounds checked (loop invariant `i + width <= n`; entry uses `debug_assert!` on public wrappers).
- [x] No in-place aliasing required — public kernels take distinct `out` slices; in-tree callers use separate buffers.
- [x] Scalar tail handles `n % lane != 0`; empty `n=0` is safe (loops do not enter).
- [x] Float NaN/Inf: bit-identical mul/add/sub vs scalar for finite inputs (props); IEEE NaN payload identity not required.

### Cranelift JIT (`jit.rs`)

- [x] `transmute` only on pointers from `JITModule::get_finalized_function`.
- [x] Scalar JIT scratch length covers compiled column indices (existing unit tests).
- [x] Batch JIT: callers pass `n` matching slice lengths (`prop_jit_batch_mul_matches_scalar` + `cranelift_simd_f64x2_batch_mul_matches_scalar`).
- [x] Non-lowerable exprs fall back (`compile_predicate` → `None` / interpreter); no half-compiled call.
- [x] `CompiledFn` / batch fn pointers are not retained past owning `JitCompiler` in production paths (engine-scoped).

### mmap / pages

- [x] Mmap lifetime tied to `SstPin` / registry; unlink deferred until pins drop.
- [x] Page `unsafe` only for `alloc_zeroed` with `Layout` size==align power-of-two.

## Findings

1. **[Low]** `src/vectorized.rs` — Public `SimdKernels::{mul,add,sub,sum}` enforce slice/`n` consistency with `debug_assert!` only. A release build with a buggy caller could pass `n` larger than slice length and hit UB in the SIMD path. **Action:** accept for v1 (all in-tree callers pass `batch.len` / prop-tested `n`); follow-up optional `assert!` or checked `get_unchecked` wrappers if these APIs are ever exported as a public crate surface beyond the engine.

2. **[None otherwise]** No High/Medium defects found in AVX2/AVX-512 kernels, Euclidean SIMD, Cranelift transmute sites, mmap pinning, or page allocation as of 2026-07-19.

## Residual risk

- Multi-day continuous soak may still miss rare JIT/SIMD races under concurrent BPM — tracked via `examples/continuous_chaos` heartbeats (`TAKYONIC_HEARTBEAT_PATH`).
- Property tests sample finite random floats; they do not exhaustively cover NaN signaling payloads or denormals under every CPU microcode.
