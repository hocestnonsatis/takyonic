# Design: S3 Chunk Dirty Coalesce (write-amp)

**Date:** 2026-08-07  
**Status:** COMPLETE (2026-08-07)  
**Continues:** [2026-07-20-s3-chunked-pages-design.md](2026-07-20-s3-chunked-pages-design.md)

## Problem

ChunkV2 caps PutObject size, but BPM `flush_all` / per-page write-through still
issues one full-chunk RMW PutObject **per dirty page**. N dirty pages in the
same chunk → N uploads of ~chunk_size bytes.

## Decision

1. **`DiskManager::write_page_snapshots_coalesced`** — group snapshots by
   `chunk_id`, write each page to the local `PAGES` file, then **one**
   `ObjectStorage::write` per touched chunk built from the local file range.
2. **`write_page`** — thin wrapper over coalesced API (single page → one chunk
   upload; eviction cost unchanged).
3. **`BufferPoolManager::flush_all`** — snapshot dirty frames, call coalesced
   write once → PutObject count ≤ number of distinct chunks.

## Non-goals

- Multipart upload
- Changing SST upload path
- Deferring remote durability across process crash without local `PAGES`
  (local write + chunk upload still happen in the same call)

## Acceptance

- InMemory / unit: N dirty pages spanning C chunks → `write_ops == C` (≪ N)
- Existing 2A–2C chunk tests remain green
