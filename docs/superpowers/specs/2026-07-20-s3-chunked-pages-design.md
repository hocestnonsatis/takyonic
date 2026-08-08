# Design: S3 / Object-Store Write Architecture (Chunked Pages)

**Date:** 2026-07-20  
**Status:** Phases **2A–2C DONE** (2026-07-20).

## Goal

Eliminate O(n)×heap PutObject on every dirty page and stay under AWS 5 GiB single-Put limits for pages and SSTs.

## Decisions (shipped)

1. **Pages layout v2:** fixed 64 MiB chunks (`pages/v2/chunk-{id}`); RMW only touched chunk.
2. **Manifest:** `PagesLayout::{BlobV1,ChunkV2}` + V1→V2 migration on open.
3. **SST:** `max_sst_bytes` (1 GiB) split; upload as `sst/...` object keys.
4. **Multipart:** not required while chunks/SSTs ≤1 GiB.

## Proof (2C)

- V2 vs V1 upload math ~25.6× fewer bytes (64 MiB cycle).
- Heap offset past 5 GiB: V2 uploads ≪5 GiB; cold read OK.

## Non-goals

- Multipart upload API (deferred until objects >5 GiB required).

## Follow-up (DONE 2026-08-07)

Chunk dirty coalesce: BPM `flush_all` → one PutObject per touched chunk
(`docs/superpowers/specs/2026-08-07-s3-chunk-dirty-coalesce-design.md`).
