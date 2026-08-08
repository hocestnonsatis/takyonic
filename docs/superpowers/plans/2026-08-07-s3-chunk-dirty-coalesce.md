# Plan: S3 Chunk Dirty Coalesce

**Spec:** `docs/superpowers/specs/2026-08-07-s3-chunk-dirty-coalesce-design.md`

## Tasks

1. [x] RED: `coalesced_flush_all_uploads_once_per_chunk` (BPM + InMemoryObjectStore)
2. [x] `DiskManager::write_page_snapshots_coalesced` + `upload_chunk_with_overlays`
3. [x] Wire `write_page` + `BufferPoolManager::flush_all`
4. [x] GREEN + existing disk/BPM remote tests
5. [x] Mark design COMPLETE
