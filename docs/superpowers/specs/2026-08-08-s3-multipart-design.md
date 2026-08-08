# S3 multipart upload (Faz 4A)

**Date:** 2026-08-08  
**Status:** Implementing

## Problem

Single `PutObject` is capped at 5 GiB. Takyonic previously **refused** such
writes and relied on `max_sst_bytes` / ChunkV2 to stay under the limit.
Uncapped objects (or future policy changes) need true multipart upload.

## Design

1. **Threshold:** payloads with `len >= AWS_S3_PUT_OBJECT_MAX_BYTES` use
   multipart; smaller payloads keep single `PutObject` + `assert_put_object_size`.
2. **Part size:** default **8 MiB** (≥ AWS 5 MiB minimum for non-final parts).
3. **Backends:**
   - `AwsS3Client`: `CreateMultipartUpload` → `UploadPart` → `CompleteMultipartUpload`
     (abort on failure).
   - `InMemoryObjectStore`: simulate MPU (part counters) then store the full blob;
     tests lower `multipart_threshold` so we never allocate 5 GiB.
   - `LocalFileBackend`: no AWS limit — write the file directly (skip refuse).
4. **SST/chunk caps stay:** multipart is a safety net, not a reason to upload
   multi-GiB SSTs by default.

## Acceptance

- In-memory: threshold override → `multipart_uploads ≥ 1`, `multipart_parts ≥ 2`,
  round-trip read OK.
- `assert_put_object_size` still documents the single-Put limit.
- `cargo test --lib object_store` green; optional MinIO MPU smoke with `s3` feature.
