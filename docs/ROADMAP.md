# Takyonic Roadmap — DB Readiness

**Tarih:** 2026-08-02  
**Kaynak:** [DB readiness assessment](../memories.md) (237 lib tests PASS + canlı psql smoke)  
**Amaç:** Motor demosundan kullanılabilir NewSQL / SQL veritabanına geçiş yolu.

Bu dosya stratejik yol haritasıdır. Adım adım uygulama planları `docs/superpowers/plans/` altında ayrı açılır.

---

## Bugünkü durum (baseline)

| Katman | Durum |
|--------|--------|
| LSM + WAL + compaction + admission | Gerçek, chaos-doğrulanmış |
| Raft HA + OCC MVCC + ARIES txn WAL | Gerçek, crucible PASS |
| pgwire Simple/Extended + SCRAM | Dar demo yüzeyi çalışıyor |
| SQL DML/DQL (INNER JOIN, agg, CTE, index, VACUUM) | Bağlı |
| RBAC / HNSW / JIT / BPM | Bağlı |
| CREATE/DROP/ALTER TABLE, typed schema, pg_catalog | Bağlı (`\dt` / `\d` / DEFAULT / NOT NULL / UNIQUE) |
| MPP / partition remote scan | Bağlı (`mpp_enabled` / `--mpp`; SUM/COUNT/MIN/MAX/AVG + `REBALANCE TABLE`) |
| 2PC cross-shard | Bağlı + durable TC log (`TC_DECISIONS`) |
| S3 object store | Kütüphane + MinIO; server CLI; **multipart** ≥5 GiB |
| SERIAL / sequences | Dayanıklı `SEQUENCES` dosyası |
| COPY | Dosya yolu TSV + STDIN/STDOUT wire |

**Tek cümle (2026-08-08):** Güçlü NewSQL motor + pgwire SQL (A–HV1) + W–AA + Faz 1–4D (IANA TZ + Smart Client Session SQL); tam Postgres/ORM veya multi-DB değil. Sıradaki: Faz 4E+ (multi-DB, loom/TSan).

---

## Faz 1 — Üretim sertleştirme (2026-08-07)

| # | İş | Durum |
|---|-----|--------|
| 1A | S3 chunk dirty coalesce (`write_page_snapshots_coalesced` + BPM `flush_all`) | **DONE** |
| 1B | W–AA chaos: 2PC TC decide, COPY abandon prefix, MPP transient retry | **DONE** (`reliability/waa_chaos`) |
| 1C | Docs / bayat plan arşivi | **DONE** |

**Sıradaki:** Faz 2 seçici ORM (tip wire, COPY STDIN, driver smoke) — PG niche mikro-faz varsayılan değil.

### Faz 2 — Seçici ORM / tip yüzeyi (2026-08-07)

| # | İş | Durum |
|---|-----|--------|
| 2.1 | Describe UUID/BYTEA/NUMERIC/TIMESTAMPTZ (`catalog_type_to_pg`) | **DONE** |
| 2.2 | `information_schema.columns` udt_name / is_nullable / column_default | **DONE** |
| 2.3 | COPY FROM STDIN / TO STDOUT (session + pgwire CopyHandler) | **DONE** |
| 2.4 | `scripts/smoke-orm-types.sh` (psql) | **DONE** |

**Sıradaki:** Faz 3 dağıtık ürün derinliği (MPP agg/join, 2PC compose, partition ops).

### Faz 3 — Dağıtık ürün derinliği (2026-08-07)

| # | İş | Durum |
|---|-----|--------|
| 3.1 | MPP `DistAggKind` SUM/COUNT/MIN/MAX/AVG + session e2e | **DONE** |
| 3.2 | `REBALANCE TABLE` → hot→cold PMAP persist | **DONE** |
| 3.3 | 3-shard 2PC crash-after-decide + MPP mixed (`waa_twopc_three_shard_then_mpp_aggregate`) | **DONE** |
| 3.4 | Equi `DistributedJoin` remote path + EXPLAIN=exec (`session_mpp_distributed_join_e2e`) | **DONE** |
| 3.5 | L0/admission altında MPP+2PC+writer (`waa_mixed_mpp_twopc_under_l0_pressure`) | **DONE** |

**Sıradaki:** Faz 4E+ (multi-DB, loom/TSan) — ürün önceliği.

| # | İş | Durum |
|---|-----|--------|
| 4A.1 | `prefer_multipart` / part ranges + in-memory MPU counters | **DONE** |
| 4A.2 | `AwsS3Client` Create/UploadPart/Complete (+ abort on failure) | **DONE** (`--features s3`) |
| 4A.3 | Spec `docs/superpowers/specs/2026-08-08-s3-multipart-design.md` | **DONE** |

### Faz 4B — Minimal SSI (2026-08-08)

| # | İş | Durum |
|---|-----|--------|
| 4B.1 | `SET serializable` + `IsolationLevel` + SSI doom registry | **DONE** |
| 4B.2 | Write-skew e2e SSI + RR OCC regression | **DONE** |
| 4B.3 | Spec `docs/superpowers/specs/2026-08-08-ssi-minimal-design.md` | **DONE** |

### Faz 4C — IANA time zones (2026-08-08)

| # | İş | Durum |
|---|-----|--------|
| 4C.1 | `tzdb`/`tz-rs` + DST-aware `at_time_zone` / `TIMEZONE()` | **DONE** |
| 4C.2 | `SET`/`SHOW TimeZone` + `current_setting` + `LOCALTIMESTAMP` | **DONE** |
| 4C.3 | E2E: Istanbul / Denver winter+summer + GUC (`session_at_time_zone_e2e`, `session_timezone_guc_e2e`) | **DONE** |

**Sıradaki:** Faz 4D+ (Smart Client Session SQL, multi-DB, loom/TSan) — ürün önceliği.

### Faz 4D — Smart Client Session SQL (2026-08-08)

| # | İş | Durum |
|---|-----|--------|
| 4D.1 | Proto `ExecuteSessionSql` + leader ephemeral `SessionState` | **DONE** |
| 4D.2 | `TakyonicClient::execute_session_sql` + `SessionSqlResult` | **DONE** |
| 4D.3 | 3-node JOIN e2e (`three_node_smart_client_session_sql_join`) | **DONE** |
| 4D.4 | Spec `docs/superpowers/specs/2026-08-08-smart-client-session-sql-design.md` | **DONE** |

**Sıradaki:** Faz 4E+ (multi-DB, loom/TSan) — ürün önceliği.

---

## İlkeler

1. **Önce SQL ürün yüzeyi, sonra dağıtık iddia.** Tablo DDL ve tip sistemi olmadan MPP/2PC pazarlanamaz.
2. **Bağla veya düşür.** `serve_node` / Session path’e girmeyen alt sistemler ya entegre edilir ya da “experimental / test-only” diye belgelenir.
3. **EXPLAIN ile exec aynı hikâyeyi anlatsın.** Dağıtık plan gösterip local çalıştırmak yasak (veya EXPLAIN’te açıkça “local fallback” yazılır).
4. **TDD + mevcut suite kırılmaz.** Her faz `cargo test --lib` yeşil; yeni davranış için e2e/session testi.
5. **`main` üzerinde çalış** (proje tercihi); feature branch yok.

---

## Fazlar

### Faz A — SQL ürün yüzeyi (P0)

**Hedef:** Takyonic’e “tablo oluşturup sorgulayabildiğin bir veritabanı” demek.

| # | İş | Kabul kriteri |
|---|-----|----------------|
| A1 | `CREATE TABLE` / `DROP TABLE` | psql ile tablo oluştur/sil; bootstrap `users` isteğe bağlı kalır — **DONE 2026-08-02** |
| A2 | Typed columns + projection | `SELECT a,b` yalnızca a,b döner; OID/tip eşlemesi — **DONE 2026-08-02** (`Project` + catalog OID hints) |
| A3 | `ALTER TABLE` (minimal) | ADD/DROP COLUMN — **DONE 2026-08-02** (PK drop reddedilir; index cleanup) |
| A4 | Catalog Raft replication | DDL `RaftCommand` ile çoğalır; follower’da aynı şema — **DONE 2026-08-02** (`CatalogUpsert`/`CatalogDrop`, 3-node e2e) |
| A5 | AUTH/INDEX/STATS metadata tutarlılığı | Multi-node’da drift yok — **DONE 2026-08-02** (`AuthReplace`/`StatsReplace`; INDEX via A4 CatalogUpsert; leader-only DDL) |

**Çıkış kapısı**

- [x] `CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT); INSERT…; SELECT name FROM t;` session e2e PASS
- [x] Elle SQL ile ikinci tablo oluşturulabiliyor (local SessionState)
- [x] 3-node cluster’da DDL sonrası her replica aynı `CATALOG` (A4)
- [x] 3-node AUTH + ANALYZE STATS + INDEX catalog drift yok (A5)

**Dokunulan alanlar (beklenen):** `src/sql.rs`, `src/schema.rs`, `src/catalog.rs`, `src/engine.rs`, `src/raft.rs`, `src/pg.rs`, `proto/`, `takyonic-server`

---

### Faz B — Dağıtık txn gerçekliği (P0)

**Hedef:** Cross-shard atomiklik iddiası testten üretime.

| # | İş | Kabul kriteri |
|---|-----|----------------|
| B1 | `TwopcService` → `serve_node` | Her node gRPC’de 2PC dinler — **DONE 2026-08-02** |
| B2 | Engine + OCC + Raft prepare/commit path | Engine-backed 2PC — **DONE 2026-08-02** (`EngineShard`, TxnPrepare/Commit via Raft+LSM) |
| B3 | Client / Session API | `TakyonicClient::execute_dist_txn` — **DONE 2026-08-02** (explicit shard write-set) |
| B5 | Session SQL auto multi-shard COMMIT | `partition_txn_branches` + TC on `COMMIT` — **DONE 2026-08-03** (`session_multi_shard_commit_uses_2pc`; `attach_dist_shards` / RemoteShard) |
| B4 | Crash recovery e2e | PREPARED → Raft rebuild + presumed abort — **DONE 2026-08-02** (`twopc_recover_from_raft_log`, bank invariant) |

**Çıkış kapısı**

- [x] Engine 2PC path usable from Client (`execute_dist_txn`) + crash recover (B2–B4); Session SQL multi-shard via `attach_dist_shards` / SocketAddr workers (**B5 DONE 2026-08-03**)
- [x] 3 bağımsız Engine shard cross-commit e2e (B2)
- [x] Client cross-shard 2PC API (`execute_dist_txn`, B3)
- [x] TwopcService `serve_node` üzerinde Engine path (B1+B2)
- [x] PREPARED crash → recover → presumed abort; seed account unchanged (B4)

**Dokunulan alanlar:** `src/dtxn.rs`, `src/twopc_service.rs`, `src/network.rs`, `src/engine.rs`, `src/raft.rs`, `src/client.rs` / `src/pg.rs`

---

### Faz C — Dağıtık sorgu gerçekliği (P1)

**Hedef:** MPP/partitioning EXPLAIN ile exec aynı olsun.

| # | İş | Kabul kriteri |
|---|-----|----------------|
| C1 | Session → `Coordinator` when `mpp_enabled` | DistributedAggregate via Coordinator — **DONE 2026-08-02** (EXPLAIN+exec; Join → C2) |
| C2 | `DistributedScan` remote workers | Partition prune sonrası gerçek RemoteWorker fetch — **DONE 2026-08-02** (`execute_distributed_scan` + `GrpcFragmentDispatcher`; Join remote scan) |
| C3 | Shuffle backpressure e2e | 3-node agg + shuffle metrics artar — **DONE 2026-08-02** (`RemoteShuffleClient` retry; `mpp_shuffle_backpressure`; capacity-1 EOS fix) |
| C4 | INSERT routing | `Coordinator::execute_insert` ownership; broadcast yok — **DONE 2026-08-02** (Session partitioned INSERT → `execute_insert_rows`) |

**Çıkış kapısı**

- [x] `mpp_enabled=true` ile EXPLAIN `DistributedAggregate` + exec Coordinator (C1); Join exec C2
- [x] Partitioned table + range/hash prune tek worker’a iner (ölçülebilir)
- [x] `mpp_enabled=false` davranışı bugünkü local fallback ile uyumlu kalır
- [x] Partitioned INSERT ownership routing, broadcast yok (C4)

**Dokunulan alanlar:** `src/mpp.rs`, `src/shuffle.rs`, `src/shuffle_service.rs`, `src/executor.rs`, `src/partition.rs`, `src/pg.rs`, `src/config.rs`

---

### Faz D — Depolama–compute ürün yolu (P1)

**Hedef:** S3 decoupling server’dan açılabilir olsun.

| # | İş | Kabul kriteri |
|---|-----|----------------|
| D1 | Server/config: object store root + S3 endpoint | `takyonic-server` / compose ile MinIO’ya bağlanabilir — **DONE 2026-08-02** (`--object-store` / `--s3-*`; compose profile `s3`; Docker `--features s3`) |
| D2 | Manifest hydrate on open (default path) | Restart sonrası SST/pages S3’ten gelir — **DONE 2026-08-02** (`object_store_root` cold restart; hydrate logs; `object_store_root_cold_restart_hydrates_sst_and_kv`) |
| D3 | Multipart veya güvenli büyük PutObject | 1 GiB+ SST / chunk politikası belgelenir ve test edilir — **DONE 2026-08-02** (`assert_put_object_size` ≥5 GiB refuse; `max_sst_bytes`/chunk caps; no multipart) |
| D4 | docker-compose profile | Opsiyonel MinIO + 1 node S3-backed smoke — **DONE 2026-08-02** (`node-s3` + MinIO healthcheck; `scripts/smoke-s3-compose.sh` PASS; nested-runtime S3 fix)

**Çıkış kapısı**

- [x] `--features s3` server path’te dokümante + smoke (CLI/env + compose `s3` profile)
- [x] Faz 2C MinIO kanıtı bozulmaz; server e2e eklenir (`./scripts/smoke-s3-compose.sh`)

**Dokunulan alanlar:** `src/object_store.rs`, `src/manifest.rs`, `src/bin/takyonic-server.rs`, `src/config.rs`, `docker-compose.yml`

---

### Faz E — Postgres / istemci uyumu (P2)

**Hedef:** `psql` ve basit sürücüler “gerçek DB” hissi alsın; ORM hâlâ sınırlı olabilir.

| # | İş | Kabul kriteri |
|---|-----|----------------|
| E1 | `pg_catalog` / `information_schema` stub | `\dt`, temel introspection — **DONE 2026-08-02** (`src/pg_catalog.rs`; Session intercept; `session_psql_dt_and_information_schema`) |
| E2 | Extended Query Describe | Portal/statement field listesi dolu — **DONE 2026-08-02** (`describe_plan_fields`; catalog types; `describe_plan_fields_for_select_project_and_aggregate`) |
| E3 | OUTER JOIN (LEFT en az) | NestedLoop veya Hash outer — **DONE 2026-08-02** (HashJoin+NestedLoop Left; `session_left_outer_join_e2e`) |
| E4 | `UNION` / `DISTINCT` | Planner + exec — **DONE 2026-08-02** (`LogicalPlan::Union`/`Distinct`; `session_union_and_distinct_e2e`) |
| E5 | Gerçek `SET`/`SHOW` (minimal) | En az `search_path` / `transaction_isolation` no-op değil, state tutar — **DONE 2026-08-02** (Session GUC; `session_set_show_search_path_and_isolation`) |

**Çıkış kapısı**

- [x] `psql \dt` / information_schema tables+columns stub (E1); `\d table` → Faz G1
- [x] Prepared statement Describe boş değil (E2)
- [x] LEFT JOIN e2e PASS (E3)
- [x] UNION / DISTINCT (E4)
- [x] SET/SHOW `search_path` + `transaction_isolation` state (E5)

**Dokunulan alanlar:** `src/pg.rs`, `src/sql.rs`, `src/pg_catalog.rs`, `src/executor.rs`

---

### Faz F — Doğruluk ve ölçek cilası (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| F1 | `ApplyExec` correlated subquery | Büyük outer cardinality’de O(N×subquery) yerine Apply — **DONE 2026-08-02**; equi EXISTS/IN → HashSemiJoin unnest + streaming Apply — **DONE 2026-08-03** |
| F2 | BTree primary durable story | BTree tablolar LSM-mirror değilse net durability modeli — **DONE 2026-08-02** (LSM = SoT; BTree hydrate on open; `btree_table_survives_engine_reopen_via_lsm_hydrate`) |
| F3 | Demo bootstrap ayrımı | Boş cluster: migrate-on-empty veya SQL DDL; hardcoded `users` opsiyonel — **DONE 2026-08-02** (`ensure_demo_users` / `--no-demo-bootstrap`; default on + idempotent) |
| F4 | Smart Client zengin SQL | JOIN/agg/txn path Client’ta (veya “pgwire only” diye net sınır) — **DONE 2026-08-02** (pgwire-only boundary; `PGWIRE_ONLY_HINT`; client tests) |

---

### Faz G — Kalan ürün cilası (P3)

E1 çıkış kapısında açık kalan `\d` ve bilinen flaky suite sertleştirmesi.

| # | İş | Kabul kriteri |
|---|-----|----------------|
| G1 | psql `\d table` describe | `pg_attribute` stub; Column/Type/Nullable — **DONE 2026-08-02** (`session_psql_d_describe_table`) |
| G2 | Metrics overhead flake | Debug limit + best-of-N — **DONE 2026-08-02** (`metrics_overhead_under_one_percent`) |
| G3 | Catalog DDL cluster flake | Meta catalog install before `last_applied`; leader retry + port/suite locks — **DONE 2026-08-02** |

**Çıkış kapısı**

- [x] `\d`-shaped `pg_attribute` query returns typed columns (G1)
- [x] Metrics overhead test less load-sensitive in debug (G2)
- [x] `three_node_create_table_replicates_catalog` hardened; catalog apply-before-index fix (G3)

---

### Faz H — Outer join tamamı (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| H1 | RIGHT OUTER JOIN | HashJoin + NestedLoop Right; null-pad unmatched right — **DONE 2026-08-02** (`session_right_outer_join_e2e`) |
| H2 | FULL OUTER JOIN | Left + unmatched right dangling emit — **DONE 2026-08-02** (`session_full_outer_join_e2e`) |

**Çıkış kapısı**

- [x] `users RIGHT JOIN orders` returns unmatched right rows with null left cols (H1)
- [x] `users FULL OUTER JOIN orders` returns both unmatched sides (H2)

---

### Faz I — Tek-database sınırı (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| I1 | Startup `-d` / database gate | Yalnızca `postgres` (veya boş); aksi `3D000` — **DONE 2026-08-02** (`auth_source_rejects_non_default_database`) |

**Çıkış kapısı**

- [x] `psql -d appdb` fails with fatal invalid catalog; `-d postgres` still works (I1)

---

### Faz J — Isolation GUC sınırı (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| J1 | Honest SI boundary | Default `repeatable read`; reject `read uncommitted`. Originally also rejected `serializable` (**DONE 2026-08-02**); **superseded by Faz 4B** — `serializable` now accepted (minimal SSI). |

**Çıkış kapısı**

- [x] `SHOW transaction_isolation` defaults to `repeatable read` (J1)
- [x] `SET … 'read uncommitted'` rejected (J1; still true after 4B)
- [x] `SET … 'serializable'` accepted with SSI first-cut (Faz 4B; replaces J1 reject)

---

### Faz K — Set operators (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| K1 | `INTERSECT` / `EXCEPT` (+ ALL) | Set/multiset semantics over UnionExec — **DONE 2026-08-02** (`session_intersect_and_except_e2e`) |

**Çıkış kapısı**

- [x] `… INTERSECT …` / `… EXCEPT …` / `EXCEPT ALL` session e2e PASS (K1)

---

### Faz L — Pattern match (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| L1 | `LIKE` / `ILIKE` / `NOT LIKE` (+ `~~`) | `%`/`_` + ESCAPE; session e2e — **DONE 2026-08-02** (`session_like_and_ilike_e2e`) |

**Çıkış kapısı**

- [x] `WHERE name LIKE 'A%'` / `ILIKE` / `NOT LIKE` filter correctly (L1)

---

### Faz M — BETWEEN (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| M1 | `BETWEEN` / `NOT BETWEEN` | Rewrite to AND/OR comparisons — **DONE 2026-08-02** (`session_between_e2e`) |

**Çıkış kapısı**

- [x] `WHERE age BETWEEN 20 AND 30` / `NOT BETWEEN` session e2e PASS (M1)

---

### Faz N — CASE (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| N1 | `CASE WHEN` / simple `CASE` | Searched + simple (→ Eq WHENs); ELSE/NULL — **DONE 2026-08-02** (`session_case_when_e2e`) |

**Çıkış kapısı**

- [x] Searched + simple CASE in SELECT project correctly (N1)

---

### Faz O — NULL helpers (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| O1 | `IS NULL` / `IS NOT NULL` + `COALESCE` | Predicate + scalar first-non-null — **DONE 2026-08-02** (`session_is_null_and_coalesce_e2e`) |

**Çıkış kapısı**

- [x] `IS NOT NULL` / `COALESCE(...)` session e2e PASS (O1)

---

### Faz P — CAST / NULLIF (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| P1 | `CAST` / `::` / `TRY_CAST` + `NULLIF` | TEXT/INT/FLOAT/BOOL; soft cast → NULL — **DONE 2026-08-02** (`session_cast_and_nullif_e2e`) |

**Çıkış kapısı**

- [x] `CAST(age AS TEXT)`, `age::INT`, `NULLIF(name,'Ada')` session e2e PASS (P1)

---

### Faz Q — String scalars (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| Q1 | `LOWER`/`UPPER`/`LENGTH`/`TRIM`/`SUBSTRING` | ScalarFunction + SUBSTRING FROM/FOR — **DONE 2026-08-02** (`session_string_scalars_e2e`) |

**Çıkış kapısı**

- [x] LOWER/UPPER/TRIM/LENGTH/SUBSTRING session e2e PASS (Q1)

---

### Faz R — CONCAT / REPLACE / POSITION (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| R1 | `CONCAT` / `\|\|` / `REPLACE` / `POSITION` / `STRPOS` | Scalar string ops — **DONE 2026-08-02** (`session_concat_replace_position_e2e`) |

**Çıkış kapısı**

- [x] CONCAT / `||` / REPLACE / POSITION / STRPOS session e2e PASS (R1)

---

### Faz S — Math scalars (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| S1 | `ABS`/`ROUND`/`CEIL`/`FLOOR`/`MOD`/`POWER` + unary `-` | Scalar numeric ops — **DONE 2026-08-02** (`session_math_scalars_e2e`) |

**Çıkış kapısı**

- [x] ABS/ROUND/CEIL/FLOOR/MOD/POWER session e2e PASS (S1)

---

### Faz T — NOT + clock (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| T1 | `NOT` unary + `NOW()` / `CURRENT_DATE` / `CURRENT_TIMESTAMP` | Predicate invert + UTC clock text — **DONE 2026-08-02** (`session_not_and_now_e2e`) |

**Çıkış kapısı**

- [x] `WHERE NOT (…)` and `SELECT NOW()` session e2e PASS (T1)

---

### Faz U — GREATEST/LEAST + EXTRACT (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| U1 | `GREATEST`/`LEAST` + `EXTRACT`/`DATE_PART` | Multi-arg compare + timestamp field extract — **DONE 2026-08-02** (`session_greatest_least_extract_e2e`) |

**Çıkış kapısı**

- [x] GREATEST/LEAST + EXTRACT(YEAR/MONTH) session e2e PASS (U1)

---

### Faz V — INTERVAL (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| V1 | `INTERVAL` literal + date/ts ± INTERVAL (+ `*`/`/` scale) | Tagged duration + civil arithmetic — **DONE 2026-08-02** (`session_interval_arith_e2e`) |

**Çıkış kapısı**

- [x] INTERVAL display + date/timestamp ± INTERVAL session e2e PASS (V1)

---

### Faz W — DATE_TRUNC (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| W1 | `DATE_TRUNC(field, timestamp)` | Truncate to year/quarter/month/week/day/hour/minute/second — **DONE 2026-08-02** (`session_date_trunc_e2e`) |

**Çıkış kapısı**

- [x] DATE_TRUNC day/hour/month/year session e2e PASS (W1)

---

### Faz X — AGE (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| X1 | `AGE(ts)` / `AGE(ts, ts)` | Interval difference (display via INTERVAL) — **DONE 2026-08-02** (`session_age_e2e`) |

**Çıkış kapısı**

- [x] AGE two-arg + one-arg session e2e PASS (X1)

---

### Faz Y — TO_CHAR / TO_TIMESTAMP (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| Y1 | `TO_CHAR` / `TO_TIMESTAMP` (YYYY/MM/DD/HH24/MI/SS) | Format + parse round-trip — **DONE 2026-08-02** (`session_to_char_to_timestamp_e2e`) |

**Çıkış kapısı**

- [x] TO_CHAR + TO_TIMESTAMP session e2e PASS (Y1)

---

### Faz Z — GENERATE_SERIES (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| Z1 | `FROM generate_series(start, stop [, step])` | Integer series → Values — **DONE 2026-08-02** (`session_generate_series_e2e`) |

**Çıkış kapısı**

- [x] GENERATE_SERIES + alias + WHERE session e2e PASS (Z1)

---

### Faz AA — UNNEST (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AA1 | `FROM unnest(ARRAY[…])` (+ alias / WHERE) | Expand literal arrays → Values — **DONE 2026-08-02** (`session_unnest_e2e`) |

**Çıkış kapısı**

- [x] UNNEST array literal session e2e PASS (AA1)

---

### Faz AB — Array ops (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AB1 | `array_length` / `cardinality` / `arr[i]` / `ARRAY \|\| ARRAY` | Length + 1-based index + concat — **DONE 2026-08-02** (`session_array_ops_e2e`) |

**Çıkış kapısı**

- [x] array_length / cardinality / subscript / ARRAY_CAT session e2e PASS (AB1)

---

### Faz AC — Array contains (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AC1 | `ARRAY @>` / `<@` / `&&` | Contains / contained-by / overlap — **DONE 2026-08-02** (`session_array_contains_e2e`) |

**Çıkış kapısı**

- [x] `@>` / `<@` / `&&` session e2e PASS (AC1)

---

### Faz AD — JSON arrow (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AD1 | `JSON`/`JSONB` cast + `->` / `->>` + `jsonb_typeof` | Field/index extract — **DONE 2026-08-02** (`session_json_arrow_e2e`) |

**Çıkış kapısı**

- [x] JSON `->` / `->>` / typeof session e2e PASS (AD1)

---

### Faz AE — JSON path + containment (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AE1 | `#>` / `#>>` + JSON `@>` / `<@` (ARRAY `@>` preserved) | Path extract + containment — **DONE 2026-08-02** (`session_json_path_contains_e2e`) |

**Çıkış kapısı**

- [x] JSON path / containment + ARRAY `@>` regression session e2e PASS (AE1)

---

### Faz AF — jsonb_set + JSON \|\| (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AF1 | `jsonb_set` + JSON `\|\|` concat | Set path + object/array merge — **DONE 2026-08-02** (`session_jsonb_set_concat_e2e`) |

**Çıkış kapısı**

- [x] jsonb_set + JSON `||` session e2e PASS (AF1)

---

### Faz AG — jsonb_build_object / jsonb_build_array (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AG1 | `jsonb_build_object` + `jsonb_build_array` | Key/value object + array constructors — **DONE 2026-08-02** (`session_jsonb_build_object_array_e2e`) |

**Çıkış kapısı**

- [x] jsonb_build_object / jsonb_build_array session e2e PASS (AG1)

---

### Faz AH — jsonb_pretty + JSON `-` / `#-` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AH1 | `jsonb_pretty` + JSON `-` / `#-` | Pretty-print + key/index/path delete — **DONE 2026-08-02** (`session_jsonb_pretty_delete_e2e`) |

**Çıkış kapısı**

- [x] jsonb_pretty + JSON delete session e2e PASS (AH1)

---

### Faz AI — jsonb_insert + jsonb_strip_nulls (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AI1 | `jsonb_insert` + `jsonb_strip_nulls` | Path insert + recursive null strip — **DONE 2026-08-02** (`session_jsonb_insert_strip_nulls_e2e`) |

**Çıkış kapısı**

- [x] jsonb_insert + strip_nulls session e2e PASS (AI1)

---

### Faz AJ — to_json / to_jsonb (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AJ1 | `to_json` / `to_jsonb` / `array_to_json` | Scalar → JSON text — **DONE 2026-08-02** (`session_to_json_e2e`) |

**Çıkış kapısı**

- [x] to_json / to_jsonb / array_to_json session e2e PASS (AJ1)

---

### Faz AK — json_agg / jsonb_agg (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AK1 | `json_agg` / `jsonb_agg` | Aggregate → JSON array — **DONE 2026-08-02** (`session_json_agg_e2e`) |

**Çıkış kapısı**

- [x] json_agg / jsonb_agg session e2e PASS (AK1)

---

### Faz AL — json_object_agg / jsonb_object_agg (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AL1 | `json_object_agg` / `jsonb_object_agg` | Key/value aggregate → JSON object — **DONE 2026-08-02** (`session_json_object_agg_e2e`) |

**Çıkış kapısı**

- [x] json_object_agg / jsonb_object_agg session e2e PASS (AL1)

---

### Faz AM — row_to_json (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AM1 | `row_to_json` | Whole-row alias + `ROW(...)` / tuple — **DONE 2026-08-02** (`session_row_to_json_e2e`) |

**Çıkış kapısı**

- [x] row_to_json session e2e PASS (AM1)

---

### Faz AN — jsonb_array_elements / json_each (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AN1 | `jsonb_array_elements` + `json_each` (+ `_text`) | FROM SRFs over JSON literals — **DONE 2026-08-02** (`session_json_array_elements_each_e2e`) |

**Çıkış kapısı**

- [x] jsonb_array_elements / json_each session e2e PASS (AN1)

---

### Faz AO — json_array_length (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AO1 | `json_array_length` / `jsonb_array_length` | Array length scalar — **DONE 2026-08-02** (`session_json_array_length_e2e`) |

**Çıkış kapısı**

- [x] json_array_length session e2e PASS (AO1)

---

### Faz AP — CROSS JOIN LATERAL JSON SRFs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AP1 | `TableFactor::Function` + `CROSS JOIN` / multi-FROM | Literal LATERAL `jsonb_array_elements` — **DONE 2026-08-02**; correlated → **BN1** |

**Çıkış kapısı**

- [x] CROSS JOIN LATERAL JSON SRF (literal) session e2e PASS (AP1)

---

### Faz AQ — is_json / json_is_valid (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AQ1 | `is_json` / `json_is_valid` | PG `IS JSON` stand-in (sqlparser 0.62 lacks predicate) — **DONE 2026-08-02** (`session_is_json_e2e`) |

**Çıkış kapısı**

- [x] is_json / json_is_valid session e2e PASS (AQ1)

---

### Faz AR — jsonb_path_exists (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AR1 | `jsonb_path_exists` / `json_path_exists` | `{a,b}` path existence — **DONE 2026-08-02** (`session_jsonb_path_exists_e2e`) |

**Çıkış kapısı**

- [x] jsonb_path_exists session e2e PASS (AR1)

---

### Faz AS — jsonb_extract_path (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AS1 | `jsonb_extract_path` / `_text` | Variadic path segments — **DONE 2026-08-02** (`session_jsonb_extract_path_e2e`) |

**Çıkış kapısı**

- [x] jsonb_extract_path session e2e PASS (AS1)

---

### Faz AT — jsonb_object_keys (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AT1 | `jsonb_object_keys` / `json_object_keys` | FROM SRF of object keys — **DONE 2026-08-02** (`session_jsonb_object_keys_e2e`) |

**Çıkış kapısı**

- [x] jsonb_object_keys session e2e PASS (AT1)

---

### Faz AU — string_to_array / array_to_string (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AU1 | `string_to_array` + `array_to_string` | Text ↔ array conversion — **DONE 2026-08-02** (`session_string_array_convert_e2e`) |

**Çıkış kapısı**

- [x] string_to_array / array_to_string session e2e PASS (AU1)

---

### Faz AV — split_part (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AV1 | `split_part` | Delimiter field extract (1-based / negative from end) — **DONE 2026-08-02** (`session_split_part_e2e`) |

**Çıkış kapısı**

- [x] split_part session e2e PASS (AV1)

---

### Faz AW — regexp_split_to_array (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AW1 | `regexp_split_to_array` | Regex split → text[] display; optional `i` flag — **DONE 2026-08-02** (`session_regexp_split_to_array_e2e`) |

**Çıkış kapısı**

- [x] regexp_split_to_array session e2e PASS (AW1)

---

### Faz AX — regexp_split_to_table (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AX1 | `regexp_split_to_table` | FROM SRF of regex splits; optional `i` flag — **DONE 2026-08-02** (`session_regexp_split_to_table_e2e`) |

**Çıkış kapısı**

- [x] regexp_split_to_table session e2e PASS (AX1)

---

### Faz AY — regexp_replace (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AY1 | `regexp_replace` | Pattern replace; `g`/`i` flags; `\N` → `$N` — **DONE 2026-08-02** (`session_regexp_replace_e2e`) |

**Çıkış kapısı**

- [x] regexp_replace session e2e PASS (AY1)

---

### Faz AZ — regexp_like / regexp_matches (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| AZ1 | `regexp_like` + `regexp_matches` | Boolean match + FROM SRF of capture arrays (`g`/`i`) — **DONE 2026-08-02** (`session_regexp_like_and_matches_e2e`) |

**Çıkış kapısı**

- [x] regexp_like / regexp_matches session e2e PASS (AZ1)

---

### Faz BA — lpad / rpad / repeat (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BA1 | `lpad` + `rpad` + `repeat` | Pad/truncate + string repeat — **DONE 2026-08-02** (`session_lpad_rpad_repeat_e2e`) |

**Çıkış kapısı**

- [x] lpad / rpad / repeat session e2e PASS (BA1)

---

### Faz BB — left / right / reverse (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BB1 | `left` + `right` + `reverse` | Prefix/suffix/reverse chars — **DONE 2026-08-02** (`session_left_right_reverse_e2e`) |

**Çıkış kapısı**

- [x] left / right / reverse session e2e PASS (BB1)

---

### Faz BC — initcap / ascii / chr (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BC1 | `initcap` + `ascii` + `chr` | Title-case words + codepoint roundtrip — **DONE 2026-08-02** (`session_initcap_ascii_chr_e2e`) |

**Çıkış kapısı**

- [x] initcap / ascii / chr session e2e PASS (BC1)

---

### Faz BD — md5 / encode / decode (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BD1 | `md5` + `encode`/`decode` | MD5 hex + hex/base64 encode/decode — **DONE 2026-08-02** (`session_md5_encode_decode_e2e`) |

**Çıkış kapısı**

- [x] md5 / encode / decode session e2e PASS (BD1)

---

### Faz BE — starts_with / overlay (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BE1 | `starts_with` + `overlay` | Prefix bool + SQL `OVERLAY … PLACING … FROM … [FOR …]` — **DONE 2026-08-02** (`session_starts_with_overlay_e2e`) |

**Çıkış kapısı**

- [x] starts_with / overlay session e2e PASS (BE1)

---

### Faz BF — translate / btrim (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BF1 | `translate` + `btrim`/`ltrim`/`rtrim` | Char map/delete + charset trim — **DONE 2026-08-02** (`session_translate_btrim_e2e`) |

**Çıkış kapısı**

- [x] translate / btrim session e2e PASS (BF1)

---

### Faz BG — concat_ws / format (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BG1 | `concat_ws` + `format` | NULL-skip join + `%s`/`%I`/`%L`/`%%` — **DONE 2026-08-02** (`session_concat_ws_format_e2e`) |

**Çıkış kapısı**

- [x] concat_ws / format session e2e PASS (BG1)

---

### Faz BH — ends_with + GREATEST/LEAST NULL (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BH1 | `ends_with` + GREATEST/LEAST NULL-skip | Suffix match; GREATEST/LEAST ignore NULLs (PG) — **DONE 2026-08-02** (`session_starts_with_overlay_e2e`, `session_greatest_least_extract_e2e`) |

**Çıkış kapısı**

- [x] ends_with + GREATEST/LEAST NULL-skip e2e PASS (BH1)

---

### Faz BI — quote_ident / quote_literal (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BI1 | `quote_ident` + `quote_literal` | Identifier/literal quoting (shared with `format` %I/%L) — **DONE 2026-08-02** (`session_quote_ident_literal_e2e`) |

**Çıkış kapısı**

- [x] quote_ident / quote_literal session e2e PASS (BI1)

---

### Faz BJ — quote_nullable / width_bucket (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BJ1 | `quote_nullable` + `width_bucket` | NULL→`NULL` text + histogram buckets — **DONE 2026-08-02** (`session_quote_nullable_width_bucket_e2e`) |

**Çıkış kapısı**

- [x] quote_nullable / width_bucket session e2e PASS (BJ1)

---

### Faz BK — sign / trunc / div (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BK1 | `sign` + `trunc` + `div` | Signum, truncate-toward-zero, integer division — **DONE 2026-08-02** (`session_sign_trunc_div_e2e`) |

**Çıkış kapısı**

- [x] sign / trunc / div session e2e PASS (BK1)

---

### Faz BL — pi / sqrt / cbrt / log (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BL1 | `pi` + `sqrt`/`cbrt`/`ln`/`log`/`exp` | Core math + `log(x)` / `log(b,x)` — **DONE 2026-08-02** (`session_pi_sqrt_cbrt_log_e2e`) |

**Çıkış kapısı**

- [x] pi / sqrt / cbrt / log session e2e PASS (BL1)

---

### Faz BM — trig / radians / degrees (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BM1 | `sin`/`cos`/`tan`/`asin`/`acos`/`atan`/`atan2` + `radians`/`degrees` | Trig + angle conversion — **DONE 2026-08-02** (`session_trig_radians_degrees_e2e`) |

**Çıkış kapısı**

- [x] trig / radians / degrees session e2e PASS (BM1)

---

### Faz BN — Correlated LATERAL jsonb_array_elements (P2)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BN1 | `CROSS JOIN LATERAL jsonb_array_elements(col/expr)` | Per-outer-row expand via `LateralJsonArrayElements` — **DONE 2026-08-02** (`session_cross_join_lateral_json_srf_e2e` correlated docs path) |

**Çıkış kapısı**

- [x] Correlated LATERAL `jsonb_array_elements` session e2e PASS (BN1)

---

### Faz BO — Correlated LATERAL json_each / object_keys (P2)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BO1 | `CROSS JOIN LATERAL jsonb_each` / `jsonb_object_keys(col)` | Shared `LateralJsonSrf` — **DONE 2026-08-02** (`session_cross_join_lateral_json_each_keys_e2e`) |

**Çıkış kapısı**

- [x] Correlated LATERAL json_each / object_keys session e2e PASS (BO1)

---

### Faz BP — Correlated LATERAL UNNEST (P2)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BP1 | `CROSS JOIN LATERAL unnest(col/ARRAY[…])` | `LateralUnnest` + `Unnest.array: Expression` — **DONE 2026-08-02** (`session_cross_join_lateral_unnest_e2e`) |

**Çıkış kapısı**

- [x] Correlated LATERAL `unnest` session e2e PASS (BP1)

---

### Faz BQ — Correlated LATERAL regexp SRFs (P2)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BQ1 | `CROSS JOIN LATERAL regexp_split_to_table` / `regexp_matches` | `LateralRegexpSrf` — **DONE 2026-08-02** (`session_cross_join_lateral_regexp_srf_e2e`) |

**Çıkış kapısı**

- [x] Correlated LATERAL regexp SRF session e2e PASS (BQ1)

---

### Faz BR — WITH ORDINALITY (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BR1 | `generate_series` / `unnest` `WITH ORDINALITY` | 1-based ordinality column — **DONE 2026-08-02** (`session_generate_series_e2e` / `session_unnest_e2e`) |

**Çıkış kapısı**

- [x] WITH ORDINALITY session e2e PASS (BR1)

---

### Faz BS — TRIM custom characters (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BS1 | `TRIM(BOTH/LEADING/TRAILING chars FROM expr)` | PG TRIM FROM syntax via btrim/ltrim/rtrim — **DONE 2026-08-02** (`session_translate_btrim_e2e`) |

**Çıkış kapısı**

- [x] TRIM FROM custom characters session e2e PASS (BS1)

---

### Faz BT — JSON SRF WITH ORDINALITY (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BT1 | `jsonb_array_elements` / `jsonb_object_keys` `WITH ORDINALITY` | 1-based ordinality — **DONE 2026-08-02** (`session_json_array_elements_each_e2e`) |

**Çıkış kapısı**

- [x] JSON SRF WITH ORDINALITY session e2e PASS (BT1)

---

### Faz BU — `json_each` WITH ORDINALITY (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BU1 | `json_each` / `jsonb_each` / `*_text` `WITH ORDINALITY` | key, value, 1-based ordinality — **DONE 2026-08-02** (`session_json_array_elements_each_e2e`) |

**Çıkış kapısı**

- [x] `json_each` WITH ORDINALITY session e2e PASS (BU1)

---

### Faz BV — regexp SRF WITH ORDINALITY (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BV1 | `regexp_split_to_table` / `regexp_matches` `WITH ORDINALITY` | 1-based ordinality; LATERAL path — **DONE 2026-08-02** (`session_regexp_split_to_table_e2e`, `session_regexp_like_and_matches_e2e`) |

**Çıkış kapısı**

- [x] regexp SRF WITH ORDINALITY session e2e PASS (BV1)

---

### Faz BW — `string_agg` / `array_agg` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BW1 | `STRING_AGG(expr, delim)` / `ARRAY_AGG(expr)` | GROUP BY + global; text array display — **DONE 2026-08-02** (`session_string_agg_array_agg_e2e`) |

**Çıkış kapısı**

- [x] string_agg / array_agg session e2e PASS (BW1)

---

### Faz BX — `bool_and` / `bool_or` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BX1 | `BOOL_AND` / `EVERY` / `BOOL_OR` | NULL-skip; empty → NULL — **DONE 2026-08-02** (`session_bool_and_or_e2e`) |

**Çıkış kapısı**

- [x] bool_and / bool_or session e2e PASS (BX1)

---

### Faz BY — `bit_and` / `bit_or` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BY1 | `BIT_AND` / `BIT_OR` | integer bitwise agg; empty → NULL — **DONE 2026-08-02** (`session_bit_and_or_e2e`) |

**Çıkış kapısı**

- [x] bit_and / bit_or session e2e PASS (BY1)

---

### Faz BZ — Aggregate `FILTER (WHERE …)` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| BZ1 | `agg(...) FILTER (WHERE pred)` | Skip non-matching rows; output col `… filter` — **DONE 2026-08-02** (`session_aggregate_filter_e2e`) |

**Çıkış kapısı**

- [x] aggregate FILTER session e2e PASS (BZ1)

---

### Faz CA — `COUNT(DISTINCT …)` / distinct aggregates (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CA1 | `agg(DISTINCT expr)` | Unique inputs; JIT bypass for distinct — **DONE 2026-08-02** (`session_count_distinct_e2e`) |

**Çıkış kapısı**

- [x] COUNT(DISTINCT) session e2e PASS (CA1)

---

### Faz CB — Aggregate `ORDER BY` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CB1 | `string_agg` / `array_agg` `ORDER BY` in arg list | Sorted feed before aggregate — **DONE 2026-08-02** (`session_string_agg_order_by_e2e`) |

**Çıkış kapısı**

- [x] ordered aggregate session e2e PASS (CB1)

---

### Faz CC — `stddev` / `variance` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CC1 | `STDDEV[_POP|_SAMP]` / `VAR[_POP|_SAMP]` (+ aliases) | Welford; sample NULL if n<2 — **DONE 2026-08-02** (`session_stddev_variance_e2e`) |

**Çıkış kapısı**

- [x] stddev / variance session e2e PASS (CC1)

---

### Faz CD — `corr` / `covar` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CD1 | `CORR(y,x)` / `COVAR_POP` / `COVAR_SAMP` | Bivariate online; — **DONE 2026-08-02** (`session_corr_covar_e2e`) |

**Çıkış kapısı**

- [x] corr / covar session e2e PASS (CD1)

---

### Faz CE — linear regression aggs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CE1 | `REGR_SLOPE` / `REGR_INTERCEPT` / `REGR_R2` | OLS line fit — **DONE 2026-08-02** (`session_regr_e2e`) |

**Çıkış kapısı**

- [x] regr_* session e2e PASS (CE1)

---

### Faz CF — `regr_*` helpers (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CF1 | `REGR_COUNT` / `AVGX` / `AVGY` / `SXX` / `SYY` / `SXY` | Complete PG regr family — **DONE 2026-08-02** (`session_regr_e2e`) |

**Çıkış kapısı**

- [x] regr helper session e2e PASS (CF1)

---

### Faz CG — `UNNEST WITH OFFSET` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CG1 | `UNNEST(...) WITH OFFSET [AS name]` | 0-based offset column (BQ-style) — **DONE 2026-08-02** (`session_unnest_e2e`) |

**Çıkış kapısı**

- [x] UNNEST WITH OFFSET session e2e PASS (CG1)

---

### Faz CH — timestamp `generate_series` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CH1 | `generate_series(ts, ts, INTERVAL)` | Date/timestamp series (+ date-only display) — **DONE 2026-08-02** (`session_generate_series_e2e`) |

**Çıkış kapısı**

- [x] timestamp generate_series session e2e PASS (CH1)

---

### Faz CI — `MODE` aggregate (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CI1 | `MODE(expr)` / `MODE() WITHIN GROUP (ORDER BY …)` | Most-frequent value; ASC/DESC ties — **DONE 2026-08-02** (`session_mode_e2e`) |

**Çıkış kapısı**

- [x] MODE session e2e PASS (CI1)

---

### Faz CJ — `percentile_cont` / `percentile_disc` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CJ1 | `PERCENTILE_CONT/DISC(f) WITHIN GROUP (ORDER BY …)` | Continuous + discrete percentiles — **DONE 2026-08-02** (`session_percentile_e2e`) |

**Çıkış kapısı**

- [x] percentile session e2e PASS (CJ1)

---

### Faz CK — bare `HAVING` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CK1 | `HAVING` without `GROUP BY` | Global agg + HAVING-only aggregates — **DONE 2026-08-02** (`session_bare_having_e2e`) |

**Çıkış kapısı**

- [x] bare HAVING session e2e PASS (CK1)

---

### Faz CL — `ROW_NUMBER` window (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CL1 | `ROW_NUMBER() OVER (ORDER BY …)` | Ranking column; no PARTITION BY yet — **DONE 2026-08-02** (`session_row_number_e2e`) |

**Çıkış kapısı**

- [x] ROW_NUMBER session e2e PASS (CL1)

---

### Faz CM — `RANK` / `DENSE_RANK` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CM1 | `RANK()` / `DENSE_RANK() OVER (ORDER BY …)` | Tie ranks (skip vs dense) — **DONE 2026-08-02** (`session_rank_dense_rank_e2e`) |

**Çıkış kapısı**

- [x] RANK / DENSE_RANK session e2e PASS (CM1)

---

### Faz CN — window `PARTITION BY` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CN1 | `OVER (PARTITION BY … ORDER BY …)` | Ranking resets per partition — **DONE 2026-08-02** (`session_window_partition_by_e2e`) |

**Çıkış kapısı**

- [x] PARTITION BY session e2e PASS (CN1)

---

### Faz CO — `LAG` / `LEAD` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CO1 | `LAG`/`LEAD(value [, offset [, default]]) OVER …` | Offset + default; PARTITION BY — **DONE 2026-08-02** (`session_lag_lead_e2e`) |

**Çıkış kapısı**

- [x] LAG / LEAD session e2e PASS (CO1)

---

### Faz CP — `NTILE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CP1 | `NTILE(n) OVER …` | PG bucket sizing (extra rows in earlier buckets); PARTITION BY — **DONE 2026-08-02** (`session_ntile_e2e`) |

**Çıkış kapısı**

- [x] NTILE session e2e PASS (CP1)

---

### Faz CQ — `FIRST_VALUE` / `LAST_VALUE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CQ1 | `FIRST_VALUE`/`LAST_VALUE(expr) OVER …` | Full-partition (frames unsupported); PARTITION BY — **DONE 2026-08-02** (`session_first_last_value_e2e`) |

**Çıkış kapısı**

- [x] FIRST_VALUE / LAST_VALUE session e2e PASS (CQ1)

---

### Faz CR — `NTH_VALUE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CR1 | `NTH_VALUE(expr, n) OVER …` | 1-based full-partition; NULL if n > size — **DONE 2026-08-02** (`session_nth_value_e2e`) |

**Çıkış kapısı**

- [x] NTH_VALUE session e2e PASS (CR1)

---

### Faz CS — `PERCENT_RANK` / `CUME_DIST` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CS1 | `PERCENT_RANK()` / `CUME_DIST() OVER …` | RANK-based PR; peer-aware CD — **DONE 2026-08-02** (`session_percent_rank_cume_dist_e2e`) |

**Çıkış kapısı**

- [x] PERCENT_RANK / CUME_DIST session e2e PASS (CS1)

---

### Faz CT — `ROWS` window frames (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CT1 | `ROWS BETWEEN … AND …` on value windows | UNBOUNDED / CURRENT ROW / n PRECEDING|FOLLOWING; RANGE rejected — **DONE 2026-08-02** (`session_window_rows_frame_e2e`) |

**Çıkış kapısı**

- [x] ROWS frame session e2e PASS (CT1)

---

### Faz CU — Named `WINDOW` clause (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CU1 | `WINDOW w AS (…)` + `OVER w` / `OVER (w …)` | Named resolve + refine inherit — **DONE 2026-08-02** (`session_named_window_e2e`) |

**Çıkış kapısı**

- [x] Named WINDOW session e2e PASS (CU1)

---

### Faz CV — Window aggregates (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CV1 | `SUM`/`AVG`/`COUNT`/`MIN`/`MAX` OVER … | Partition + ROWS frames (running sums) — **DONE 2026-08-02** (`session_window_agg_e2e`) |

**Çıkış kapısı**

- [x] Window aggregate session e2e PASS (CV1)

---

### Faz CW — `RANGE` window frames (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CW1 | `RANGE BETWEEN …` peer-aware frames | UNBOUNDED / CURRENT ROW; value offsets rejected — **DONE 2026-08-02** (`session_window_range_frame_e2e`) |

**Çıkış kapısı**

- [x] RANGE frame session e2e PASS (CW1)

---

### Faz CX — `RANGE` value offsets (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CX1 | `RANGE n PRECEDING/FOLLOWING` | Single numeric ORDER BY; peer expansion — **DONE 2026-08-02** (`session_window_range_frame_e2e`) |

**Çıkış kapısı**

- [x] RANGE value-offset session e2e PASS (CX1)

---

### Faz CY — `GROUPS` window frames (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CY1 | `GROUPS BETWEEN …` peer-group offsets | Requires ORDER BY; n PRECEDING/FOLLOWING — **DONE 2026-08-02** (`session_window_groups_frame_e2e`) |

**Çıkış kapısı**

- [x] GROUPS frame session e2e PASS (CY1)

---

### Faz CZ — `STRING_AGG` / `ARRAY_AGG` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| CZ1 | `STRING_AGG`/`ARRAY_AGG` as window aggs | Partition + ROWS running concat — **DONE 2026-08-02** (`session_window_string_array_agg_e2e`) |

**Çıkış kapısı**

- [x] STRING_AGG / ARRAY_AGG window session e2e PASS (CZ1)

---

### Faz DA — `BOOL_*` / `JSON*_AGG` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DA1 | `BOOL_AND`/`BOOL_OR`/`EVERY`/`JSON_AGG`/`JSONB_AGG` OVER … | Partition window aggs — **DONE 2026-08-02** (`session_window_bool_json_agg_e2e`) |

**Çıkış kapısı**

- [x] BOOL/JSON window agg session e2e PASS (DA1)

---

### Faz DB — `STDDEV` / `VAR` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DB1 | `STDDEV[_POP|_SAMP]` / `VAR[_POP|_SAMP]` / `VARIANCE` OVER … | Partition stats; sample NULL for n<2 — **DONE 2026-08-02** (`session_window_stddev_var_e2e`) |

**Çıkış kapısı**

- [x] STDDEV/VAR window session e2e PASS (DB1)

---

### Faz DC — Window `FILTER` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DC1 | `agg(…) FILTER (WHERE …) OVER …` | Aggregate windows only; ranking rejected — **DONE 2026-08-02** (`session_window_filter_e2e`) |

**Çıkış kapısı**

- [x] Window FILTER session e2e PASS (DC1)

---

### Faz DD — `CORR` / `COVAR` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DD1 | `CORR`/`COVAR_POP`/`COVAR_SAMP` OVER … | Two-arg bivariate window aggs — **DONE 2026-08-02** (`session_window_corr_covar_e2e`) |

**Çıkış kapısı**

- [x] CORR/COVAR window session e2e PASS (DD1)

---

### Faz DE — `REGR_*` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DE1 | `REGR_SLOPE`/`INTERCEPT`/`R2`/`COUNT`/`AVGX`/`AVGY`/`SXX`/`SYY`/`SXY` OVER … | Linear-regression window aggs — **DONE 2026-08-02** (`session_window_regr_e2e`) |

**Çıkış kapısı**

- [x] REGR_* window session e2e PASS (DE1)

---

### Faz DF — `BIT_*` / `MODE` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DF1 | `BIT_AND`/`BIT_OR`/`MODE` OVER … | Partition window aggs — **DONE 2026-08-02** (`session_window_bit_mode_e2e`) |

**Çıkış kapısı**

- [x] BIT/MODE window session e2e PASS (DF1)

---

### Faz DG — `JSON*_OBJECT_AGG` OVER (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DG1 | `JSON_OBJECT_AGG`/`JSONB_OBJECT_AGG` OVER … | Key/value window object agg — **DONE 2026-08-02** (`session_window_json_object_agg_e2e`) |

**Çıkış kapısı**

- [x] JSON_OBJECT_AGG window session e2e PASS (DG1)

---

### Faz DH — Window `IGNORE NULLS` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DH1 | `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` IGNORE NULLS | Skip NULLs in offset/frame scan — **DONE 2026-08-02** (`session_window_ignore_nulls_e2e`) |

**Çıkış kapısı**

- [x] IGNORE NULLS window session e2e PASS (DH1)

---

### Faz DI — `DISTINCT ON` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DI1 | `SELECT DISTINCT ON (exprs) … ORDER BY …` | First row per ON-key after sort; ORDER BY leading keys must match — **DONE 2026-08-02** (`session_distinct_on_e2e`) |

**Çıkış kapısı**

- [x] DISTINCT ON session e2e PASS (DI1)

---

### Faz DJ — Window `EXCLUDE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DJ1 | `EXCLUDE CURRENT ROW` / `GROUP` / `TIES` / `NO OTHERS` | Frame member filtering (sqlparser preprocess sentinel) — **DONE 2026-08-02** (`session_window_exclude_e2e`) |

**Çıkış kapısı**

- [x] Window EXCLUDE session e2e PASS (DJ1)

---

### Faz DK — `FETCH FIRST … WITH TIES` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DK1 | `FETCH FIRST n ROWS ONLY` / `WITH TIES` | Wire ignored `Query::fetch`; ties keep ORDER BY peers — **DONE 2026-08-02** (`session_fetch_with_ties_e2e`) |

**Çıkış kapısı**

- [x] FETCH WITH TIES session e2e PASS (DK1)

---

### Faz DL — `ORDER BY … NULLS FIRST|LAST` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DL1 | NULLS FIRST / LAST (+ PG defaults) | ASC→NULLS LAST, DESC→NULLS FIRST when omitted — **DONE 2026-08-02** (`session_order_by_nulls_first_last_e2e`) |

**Çıkış kapısı**

- [x] NULLS FIRST/LAST session e2e PASS (DL1)

---

### Faz DM — `TRUNCATE TABLE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DM1 | `TRUNCATE [TABLE] name` (+ `IF EXISTS`) | MVCC delete-all; tag `TRUNCATE TABLE` — **DONE 2026-08-02** (`session_truncate_table_e2e`) |

**Çıkış kapısı**

- [x] TRUNCATE TABLE session e2e PASS (DM1)

---

### Faz DN — `IS [NOT] DISTINCT FROM` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DN1 | NULL-safe equality / inequality | Both-NULL not distinct; one-NULL distinct — **DONE 2026-08-02** (`session_is_distinct_from_e2e`) |

**Çıkış kapısı**

- [x] IS DISTINCT FROM session e2e PASS (DN1)

---

### Faz DO — `IS [NOT] TRUE|FALSE|UNKNOWN` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DO1 | Ternary boolean tests | `IS TRUE`/`FALSE`/`UNKNOWN` (+ NOT); result never NULL — **DONE 2026-08-03** (`session_is_true_false_unknown_e2e`) |

**Çıkış kapısı**

- [x] IS TRUE/FALSE/UNKNOWN session e2e PASS (DO1)

---

### Faz DP — BinaryOp / boolean NULL (3VL) (P2)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DP1 | Comparison + AND/OR/NOT three-valued logic | NULL operand → UNKNOWN; WHERE discards UNKNOWN — **DONE 2026-08-03** (`session_binary_op_null_propagates_e2e`) |

**Çıkış kapısı**

- [x] BinaryOp NULL propagate session e2e PASS (DP1)

---

### Faz DQ — `ANY` / `SOME` / `ALL` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DQ1 | Quantified comparisons | `op ANY\|SOME\|ALL (ARRAY[…])` 3VL; `= ANY`/`<> ALL` subquery → IN/NOT IN — **DONE 2026-08-03** (`session_any_all_quantified_e2e`) |

**Çıkış kapısı**

- [x] ANY/ALL quantified session e2e PASS (DQ1)

---

### Faz DR — DI–DN session e2e restore (P2)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DR1 | Re-add DI–DN session e2es after pg.rs history restore | DISTINCT ON / EXCLUDE / FETCH TIES / NULLS / TRUNCATE / IS DISTINCT FROM — **DONE 2026-08-03** |

**Çıkış kapısı**

- [x] DI–DN session e2es PASS again (DR1)

---

### Faz DS — `SIMILAR TO` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DS1 | `SIMILAR TO` / `NOT SIMILAR TO` (+ ESCAPE) | SQL regex dialect → POSIX; `%`/`_`/`|`/`*`/`+`/`()` — **DONE 2026-08-03** (`session_similar_to_e2e`) |

**Çıkış kapısı**

- [x] SIMILAR TO session e2e PASS (DS1)

---

### Faz DT — `VALUES` rows (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DT1 | Bare `VALUES` + `FROM (VALUES …) AS t(cols)` | PG `columnN` defaults; alias rename; joinable — **DONE 2026-08-03** (`session_values_clause_e2e`) |

**Çıkış kapısı**

- [x] VALUES clause session e2e PASS (DT1)

---

### Faz DU — POSIX regex ops `~` / `~*` / `!~` / `!~*` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DU1 | `~` / `~*` / `!~` / `!~*` | `Expression::RegexMatch` via `regexp_like` — **DONE 2026-08-03** (`session_regex_match_ops_e2e`) |

**Çıkış kapısı**

- [x] POSIX regex ops session e2e PASS (DU1)

---

### Faz DV — `RETURNING` on INSERT / UPDATE / DELETE (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DV1 | `RETURNING` / `RETURNING *` | Logical+physical DML + session e2e — **DONE 2026-08-03** (`session_dml_returning_e2e`) |

**Çıkış kapısı**

- [x] DML RETURNING session e2e PASS (DV1)

---

### Faz DW — `LIKE` / `ILIKE` `ANY` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DW1 | `LIKE`/`ILIKE` `[NOT] ANY (ARRAY[…])` | `Expression::Like.any` + 3VL — **DONE 2026-08-03** (`session_like_any_e2e`) |

**Çıkış kapısı**

- [x] LIKE/ILIKE ANY session e2e PASS (DW1)

---

### Faz DX — `GROUP BY ALL` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DX1 | `GROUP BY ALL` | Expand to non-aggregate SELECT exprs — **DONE 2026-08-03** (`session_group_by_all_e2e`) |

**Çıkış kapısı**

- [x] GROUP BY ALL session e2e PASS (DX1)

---

### Faz DY — `ORDER BY ALL` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DY1 | `ORDER BY ALL [ASC\|DESC]` | Expand to SELECT-list exprs (PG dialect Identifier rewrite) — **DONE 2026-08-03** (`session_order_by_all_e2e`) |

**Çıkış kapısı**

- [x] ORDER BY ALL session e2e PASS (DY1)

---

### Faz DZ — `ON CONFLICT DO NOTHING` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| DZ1 | `INSERT … ON CONFLICT [ (cols) ] DO NOTHING` | Skip existing PK rows; RETURNING only inserts — **DONE 2026-08-03** (`session_insert_on_conflict_do_nothing_e2e`) |

**Çıkış kapısı**

- [x] ON CONFLICT DO NOTHING session e2e PASS (DZ1)

---

### Faz EA — `ON CONFLICT DO UPDATE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EA1 | `INSERT … ON CONFLICT DO UPDATE SET … [WHERE]` | `EXCLUDED.col` + optional WHERE — **DONE 2026-08-03** (`session_insert_on_conflict_do_update_e2e`) |

**Çıkış kapısı**

- [x] ON CONFLICT DO UPDATE session e2e PASS (EA1)

---

### Faz EB — `CREATE TABLE AS SELECT` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EB1 | `CREATE TABLE [IF NOT EXISTS] t [(cols)] AS SELECT …` | First output col = PK; seed rows — **DONE 2026-08-03** (`session_create_table_as_select_e2e`) |

**Çıkış kapısı**

- [x] CREATE TABLE AS SELECT session e2e PASS (EB1)

---

### Faz EC — `AT TIME ZONE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EC1 | `timestamp AT TIME ZONE zone` | Offset + IANA/DST (`tzdb`); **DONE 2026-08-03** offset; **DONE 2026-08-08** IANA (`session_at_time_zone_e2e`) |

**Çıkış kapısı**

- [x] AT TIME ZONE session e2e PASS (EC1)

---

### Faz ED — `INSERT … SELECT` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| ED1 | `INSERT INTO t (cols) SELECT …` | Positional map; ON CONFLICT / RETURNING — **DONE 2026-08-03** (`session_insert_select_e2e`) |

**Çıkış kapısı**

- [x] INSERT…SELECT session e2e PASS (ED1)

---

### Faz EE — `ALTER TABLE RENAME` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EE1 | `RENAME COLUMN` + `RENAME TO` | Rewrite rows; PK rename OK — **DONE 2026-08-03** (`session_alter_table_rename_e2e`) |

**Çıkış kapısı**

- [x] ALTER TABLE RENAME session e2e PASS (EE1)

---

### Faz EF — `MAKE_DATE` / `MAKE_TIME` / `MAKE_TIMESTAMP` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EF1 | `MAKE_DATE` / `MAKE_TIME` / `MAKE_TIMESTAMP` | Range-checked constructors — **DONE 2026-08-03** (`session_make_date_time_timestamp_e2e`) |

**Çıkış kapısı**

- [x] MAKE_* session e2e PASS (EF1)

---

### Faz EG — `TO_DATE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EG1 | `TO_DATE(text, format)` | Date-only parse via TO_TIMESTAMP templates — **DONE 2026-08-03** (`session_to_date_e2e`) |

**Çıkış kapısı**

- [x] TO_DATE session e2e PASS (EG1)

---

### Faz EH — `MAKE_INTERVAL` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EH1 | `MAKE_INTERVAL(y,m,w,d,h,mi,s)` | 1..7 positional args; year/month≈365/30d — **DONE 2026-08-03** (`session_make_interval_e2e`) |

**Çıkış kapısı**

- [x] MAKE_INTERVAL session e2e PASS (EH1)

---

### Faz EI — `ISFINITE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EI1 | `ISFINITE(timestamp\|interval)` | false for ±infinity; NULL→NULL — **DONE 2026-08-03** (`session_isfinite_e2e`) |

**Çıkış kapısı**

- [x] ISFINITE session e2e PASS (EI1)

---

### Faz EJ — clock / statement timestamps (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EJ1 | `CLOCK_TIMESTAMP` / `STATEMENT_TIMESTAMP` / `TRANSACTION_TIMESTAMP` | Stmt time frozen on `ExecutionContext`; `NOW` aligned — **DONE 2026-08-03** (`session_clock_statement_timestamps_e2e`) |

**Çıkış kapısı**

- [x] clock/statement timestamp session e2e PASS (EJ1)

---

### Faz EK — `timezone()` function (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EK1 | `TIMEZONE(zone, timestamp)` | Same as `AT TIME ZONE` (zone-first args) — **DONE 2026-08-03** (`session_timezone_fn_e2e`) |

**Çıkış kapısı**

- [x] TIMEZONE() session e2e PASS (EK1)

---

### Faz EL — `DATE_BIN` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EL1 | `DATE_BIN(stride, source [, origin])` | Floor onto interval grid; default origin 2001-01-01 — **DONE 2026-08-03** (`session_date_bin_e2e`) |

**Çıkış kapısı**

- [x] DATE_BIN session e2e PASS (EL1)

---

### Faz EM — `ALTER COLUMN TYPE` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EM1 | `ALTER TABLE … ALTER COLUMN … [SET DATA] TYPE` | Catalog type update (no USING) — **DONE 2026-08-03** (`session_alter_column_type_e2e`) |

**Çıkış kapısı**

- [x] ALTER COLUMN TYPE session e2e PASS (EM1)

---

### Faz EN — `JUSTIFY_*` intervals (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EN1 | `JUSTIFY_HOURS` / `JUSTIFY_DAYS` / `JUSTIFY_INTERVAL` | Display folds hours→days, days→30d months — **DONE 2026-08-03** (`session_justify_interval_e2e`) |

**Çıkış kapısı**

- [x] JUSTIFY_* session e2e PASS (EN1)

---

### Faz EO — `EXTRACT(EPOCH)` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EO1 | `EXTRACT(EPOCH FROM …)` / `DATE_PART('epoch', …)` | Unix seconds (`f64`); timestamps + intervals; offset → UTC — **DONE 2026-08-03** (`session_extract_epoch_e2e`) |

**Çıkış kapısı**

- [x] EXTRACT(EPOCH) session e2e PASS (EO1)

---

### Faz EP — `OVERLAPS` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EP1 | `(start, end\|interval) OVERLAPS (…)` | Half-open period overlap; interval length OK — **DONE 2026-08-03** (`session_overlaps_e2e`) |

**Çıkış kapısı**

- [x] OVERLAPS session e2e PASS (EP1)

---

### Faz EQ — `TIMEOFDAY` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EQ1 | `TIMEOFDAY()` | Live wall-clock text (`… UTC`) — **DONE 2026-08-03** (`session_timeofday_e2e`) |

**Çıkış kapısı**

- [x] TIMEOFDAY session e2e PASS (EQ1)

---

### Faz ER — `TO_NUMBER` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| ER1 | `TO_NUMBER(text, format)` | Subset `9`/`0`/`D`/`G`/`S`/`FM`/`.`/`,` — **DONE 2026-08-03** (`session_to_number_e2e`) |

**Çıkış kapısı**

- [x] TO_NUMBER session e2e PASS (ER1)

---

### Faz ES — session identity (`CURRENT_USER` / schema) (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| ES1 | `CURRENT_USER` / `SESSION_USER` / `USER` / `CURRENT_SCHEMA()` / `CURRENT_CATALOG` | Session auth + `search_path` head + `postgres` catalog — **DONE 2026-08-03** (`session_current_user_schema_catalog_e2e`) |

**Çıkış kapısı**

- [x] Session identity e2e PASS (ES1)

---

### Faz ET — `VERSION()` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| ET1 | `VERSION()` | PG-compatible banner with Takyonic crate version — **DONE 2026-08-03** (`session_version_e2e`) |

**Çıkış kapısı**

- [x] VERSION() session e2e PASS (ET1)

---

### Faz EU — `current_schemas` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EU1 | `current_schemas(bool)` | Array from `search_path`; `true` prepends `pg_catalog`; multi-schema `SET` — **DONE 2026-08-03** (`session_current_schemas_e2e`) |

**Çıkış kapısı**

- [x] current_schemas session e2e PASS (EU1)

---

### Faz EV — `pg_backend_pid` / recovery (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EV1 | `pg_backend_pid()` / `pg_is_in_recovery()` | OS pid; recovery always `false` (primary) — **DONE 2026-08-03** (`session_pg_backend_pid_recovery_e2e`) |

**Çıkış kapısı**

- [x] pg_backend_pid / recovery session e2e PASS (EV1)

---

### Faz EW — `pg_typeof` / encoding (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EW1 | `pg_typeof(expr)` / `getdatabaseencoding()` | Runtime type names + `UTF8` — **DONE 2026-08-03** (`session_pg_typeof_encoding_e2e`) |

**Çıkış kapısı**

- [x] pg_typeof / encoding session e2e PASS (EW1)

---

### Faz EX — `pg_size_pretty` / client encoding (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EX1 | `pg_size_pretty(n)` / `pg_client_encoding()` | Binary unit pretty-print + `UTF8` — **DONE 2026-08-03** (`session_pg_size_pretty_encoding_e2e`) |

**Çıkış kapısı**

- [x] pg_size_pretty / client encoding session e2e PASS (EX1)

---

### Faz EY — `OCTET_LENGTH` / `BIT_LENGTH` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EY1 | `OCTET_LENGTH` / `BIT_LENGTH` | UTF-8 byte length / ×8 — **DONE 2026-08-03** (`session_octet_bit_length_e2e`) |

**Çıkış kapısı**

- [x] OCTET_LENGTH / BIT_LENGTH session e2e PASS (EY1)

---

### Faz EZ — `num_nonnulls` / `num_nulls` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| EZ1 | `num_nonnulls` / `num_nulls` | Count non-NULL / NULL args — **DONE 2026-08-03** (`session_num_nulls_nonnulls_e2e`) |

**Çıkış kapısı**

- [x] num_nonnulls / num_nulls session e2e PASS (EZ1)

---

### Faz FA — `random` / `setseed` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FA1 | `random()` / `setseed(x)` | Uniform `[0,1)`; seed in `[-1,1]` reproducible — **DONE 2026-08-03** (`session_random_setseed_e2e`) |

**Çıkış kapısı**

- [x] random / setseed session e2e PASS (FA1)

---

### Faz FB — `CURRENT_ROLE` / `gen_random_uuid` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FB1 | `CURRENT_ROLE` / `gen_random_uuid()` | Role = session user; UUID v4 text — **DONE 2026-08-03** (`session_current_role_gen_random_uuid_e2e`) |

**Çıkış kapısı**

- [x] CURRENT_ROLE / gen_random_uuid session e2e PASS (FB1)

---

### Faz FC — `pg_sleep` / `pg_column_size` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FC1 | `pg_sleep(sec)` / `pg_column_size(any)` | Sleep (non-neg); approximate datum bytes — **DONE 2026-08-03** (`session_pg_sleep_column_size_e2e`) |

**Çıkış kapısı**

- [x] pg_sleep / pg_column_size session e2e PASS (FC1)

---

### Faz FD — `txid_current` / postmaster start (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FD1 | `txid_current()` / `pg_current_xact_id()` / `pg_postmaster_start_time()` | Statement-scoped xid; frozen process start — **DONE 2026-08-03** (`session_txid_postmaster_start_e2e`) |

**Çıkış kapısı**

- [x] txid / postmaster start session e2e PASS (FD1)

---

### Faz FE — `current_setting` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FE1 | `current_setting(name [, missing_ok])` | Known GUCs; unknown → error / NULL with `missing_ok` — **DONE 2026-08-03** (`session_current_setting_e2e`) |

**Çıkış kapısı**

- [x] current_setting session e2e PASS (FE1)

---

### Faz FF — `set_config` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FF1 | `set_config(name, value, is_local)` | Writable GUCs; LOCAL requires txn + restores on end — **DONE 2026-08-03** (`session_set_config_e2e`) |

**Çıkış kapısı**

- [x] set_config session e2e PASS (FF1)

---

### Faz FG — `has_table_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FG1 | `has_table_privilege([user,] table, privilege)` | RBAC-backed; comma-list = any — **DONE 2026-08-03** (`session_has_table_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_table_privilege session e2e PASS (FG1)

---

### Faz FH — `inet_*` session endpoints (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FH1 | `inet_server_addr/port` + `inet_client_addr/port` | NULL when unset; TCP via `set_net_info` — **DONE 2026-08-03** (`session_inet_addr_port_e2e`) |

**Çıkış kapısı**

- [x] inet_* session e2e PASS (FH1)

---

### Faz FI — `has_schema_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FI1 | `has_schema_privilege([user,] schema, privilege)` | USAGE/CREATE; default public USAGE; no GRANT ON SCHEMA yet — **DONE 2026-08-03** (`session_has_schema_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_schema_privilege session e2e PASS (FI1)

---

### Faz FJ — `has_database_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FJ1 | `has_database_privilege([user,] database, privilege)` | CONNECT/CREATE/TEMP; only `postgres` exists — **DONE 2026-08-03** (`session_has_database_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_database_privilege session e2e PASS (FJ1)

---

### Faz FK — pgwire `inet_*` wire-up (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FK1 | Listen + peer → `SessionState` net info | `set_listen_addr` + `session_arc_for`; unspecified IP → NULL addr — **DONE 2026-08-03** (`net_info_from_endpoints_*`) |

**Çıkış kapısı**

- [x] pgwire inet net_info helpers + server bind PASS (FK1)

---

### Faz FL — `has_column_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FL1 | `has_column_privilege([user,] table, column, privilege)` | SELECT/INSERT/UPDATE via table ACL; REFERENCES = superuser — **DONE 2026-08-03** (`session_has_column_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_column_privilege session e2e PASS (FL1)

---

### Faz FM — `has_any_column_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FM1 | `has_any_column_privilege([user,] table, privilege)` | Any-column check ≡ table ACL until per-column GRANT — **DONE 2026-08-03** (`session_has_any_column_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_any_column_privilege session e2e PASS (FM1)

---

### Faz FN — `COMMENT ON` / descriptions (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FN1 | `COMMENT ON TABLE|COLUMN` + `obj_description` / `col_description` | In-memory comments; name-based lookup — **DONE 2026-08-03** (`session_comment_obj_col_description_e2e`) |

**Çıkış kapısı**

- [x] COMMENT / description session e2e PASS (FN1)

---

### Faz FO — durable comments (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FO1 | Persist `COMMENT ON` under `data_dir/COMMENTS` | Load on open; survive engine restart — **DONE 2026-08-03** (`comments_file_roundtrip_and_engine_reload`) |

**Çıkış kapısı**

- [x] COMMENTS file roundtrip PASS (FO1)

---

### Faz FP — `shobj_description` / shared COMMENT (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FP1 | `COMMENT ON ROLE\|DATABASE` + `shobj_description(name, catalog)` | `pg_authid`/`pg_roles`/`pg_database`; durable — **DONE 2026-08-03** (`session_shobj_description_e2e`) |

**Çıkış kapısı**

- [x] shobj_description session e2e PASS (FP1)

---

### Faz FQ — Raft-replicate COMMENTS (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FQ1 | `RaftCommand::CommentsReplace` + install on apply | Leader proposes COMMENTS blob; followers install like AUTH/STATS — **DONE 2026-08-03** (`comments_replace_installs_via_apply_committed`) |

**Çıkış kapısı**

- [x] CommentsReplace encode/apply PASS (FQ1)

---

### Faz FR — `GRANT ON SCHEMA` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FR1 | `GRANT`/`REVOKE` `ON SCHEMA` + `has_schema_privilege` grants | AUTH `SGRANT` rows; CREATE via grant — **DONE 2026-08-03** (`session_grant_on_schema_e2e`) |

**Çıkış kapısı**

- [x] GRANT ON SCHEMA session e2e PASS (FR1)

---

### Faz FS — column GRANT ACL (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FS1 | `GRANT`/`REVOKE` `SELECT\|UPDATE (col)` + `has_column_privilege` | AUTH `CGRANT`; column vs table ACL — **DONE 2026-08-03** (`session_grant_column_acl_e2e`) |

**Çıkış kapısı**

- [x] column GRANT ACL session e2e PASS (FS1)

---

### Faz FT — OID `obj_description` / `to_regclass` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FT1 | `to_regclass` + OID-form `obj_description` / `col_description` | Synthetic relation OIDs; attnum columns — **DONE 2026-08-03** (`session_oid_obj_col_description_e2e`) |

**Çıkış kapısı**

- [x] OID description / to_regclass session e2e PASS (FT1)

---

### Faz FU — `format_type` / `pg_get_userbyid` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FU1 | `format_type(oid, typmod)` + `pg_get_userbyid(oid)` | Common type OIDs; synthetic role OIDs — **DONE 2026-08-03** (`session_format_type_pg_get_userbyid_e2e`) |

**Çıkış kapısı**

- [x] format_type / pg_get_userbyid session e2e PASS (FU1)

---

### Faz FV — `to_regrole` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FV1 | `to_regrole(name)` ↔ `pg_get_userbyid` | AUTH role → synthetic OID; missing → NULL — **DONE 2026-08-03** (`session_to_regrole_e2e`) |

**Çıkış kapısı**

- [x] to_regrole session e2e PASS (FV1)

---

### Faz FW — `pg_relation_size` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FW1 | `pg_relation_size` / `pg_table_size` / `pg_total_relation_size` | Name or OID; heap vs total heuristic — **DONE 2026-08-03** (`session_pg_relation_size_e2e`) |

**Çıkış kapısı**

- [x] pg_relation_size session e2e PASS (FW1)

---

### Faz FX — `pg_indexes_size` / `pg_database_size` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FX1 | `pg_indexes_size` + `pg_database_size` | Indexes = total−heap; DB = sum of totals — **DONE 2026-08-03** (`session_pg_indexes_database_size_e2e`) |

**Çıkış kapısı**

- [x] pg_indexes_size / pg_database_size session e2e PASS (FX1)

---

### Faz FY — `to_regnamespace` / `to_regtype` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FY1 | `to_regnamespace` + `to_regtype` | Builtin schemas; type name↔OID with `format_type` — **DONE 2026-08-03** (`session_to_regnamespace_regtype_e2e`) |

**Çıkış kapısı**

- [x] to_regnamespace / to_regtype session e2e PASS (FY1)

---

### Faz FZ — `has_function_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| FZ1 | `has_function_privilege` | EXECUTE on known scalars (`is_known_sql_function`); superuser always true — **DONE 2026-08-03** (`session_has_function_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_function_privilege session e2e PASS (FZ1)

---

### Faz GA — `pg_has_role` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GA1 | `pg_has_role` | MEMBER/USAGE/SET via AUTH memberships; name or OID; superuser always true — **DONE 2026-08-03** (`session_pg_has_role_e2e`) |

**Çıkış kapısı**

- [x] pg_has_role session e2e PASS (GA1)

---

### Faz GB — `has_type_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GB1 | `has_type_privilege` | USAGE on known types (`to_regtype` set); name or OID; superuser always true — **DONE 2026-08-03** (`session_has_type_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_type_privilege session e2e PASS (GB1)

---

### Faz GC — encoding name ↔ id (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GC1 | `pg_encoding_to_char` / `pg_char_to_encoding` | UTF8=6 (+ SQL_ASCII/LATIN1/WIN1252); unknown → `""` / `-1` — **DONE 2026-08-03** (`session_pg_encoding_char_e2e`) |

**Çıkış kapısı**

- [x] encoding char↔id session e2e PASS (GC1)

---

### Faz GD — catalog visibility (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GD1 | `pg_table_is_visible` / `pg_type_is_visible` | search_path for tables; known types always visible — **DONE 2026-08-03** (`session_pg_table_type_is_visible_e2e`) |

**Çıkış kapısı**

- [x] table/type visibility session e2e PASS (GD1)

---

### Faz GE — `to_regproc` / function visibility (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GE1 | `to_regproc` + `pg_function_is_visible` | Known scalars → OID; name/OID visibility — **DONE 2026-08-03** (`session_to_regproc_function_visible_e2e`) |

**Çıkış kapısı**

- [x] to_regproc / pg_function_is_visible session e2e PASS (GE1)

---

### Faz GF — `pg_relation_is_updatable` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GF1 | `pg_relation_is_updatable` | Ordinary tables → 28 (UPDATE\|DELETE\|INSERT); missing → 0 — **DONE 2026-08-03** (`session_pg_relation_is_updatable_e2e`) |

**Çıkış kapısı**

- [x] pg_relation_is_updatable session e2e PASS (GF1)

---

### Faz GG — `pg_column_is_updatable` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GG1 | `pg_column_is_updatable` | Column name/attnum on updatable table → true; missing → false — **DONE 2026-08-03** (`session_pg_column_is_updatable_e2e`) |

**Çıkış kapısı**

- [x] pg_column_is_updatable session e2e PASS (GG1)

---

### Faz GH — `pg_get_indexdef` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GH1 | `pg_get_indexdef` | Reconstruct `CREATE INDEX … USING btree/hnsw (col)`; missing → NULL — **DONE 2026-08-03** (`session_pg_get_indexdef_e2e`) |

**Çıkış kapısı**

- [x] pg_get_indexdef session e2e PASS (GH1)

---

### Faz GI — `pg_describe_object` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GI1 | `pg_describe_object` | classid/objid/objsubid → table/column/index/type/schema/role/function text — **DONE 2026-08-03** (`session_pg_describe_object_e2e`) |

**Çıkış kapısı**

- [x] pg_describe_object session e2e PASS (GI1)

---

### Faz GJ — `pg_identify_object` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GJ1 | `pg_identify_object` | Scalar `identity` field (schema-qualified); missing → NULL — **DONE 2026-08-03** (`session_pg_identify_object_e2e`) |

**Çıkış kapısı**

- [x] pg_identify_object session e2e PASS (GJ1)

---

### Faz GK — `to_regoper` / operator visibility (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GK1 | `to_regoper` + `pg_operator_is_visible` | Known ops (`=`, `<->`, JSON/`~~`, …); name/OID — **DONE 2026-08-03** (`session_to_regoper_operator_visible_e2e`) |

**Çıkış kapısı**

- [x] to_regoper / pg_operator_is_visible session e2e PASS (GK1)

---

### Faz GL — `to_regcollation` / collation visibility (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GL1 | `to_regcollation` + `pg_collation_is_visible` | `default`/`C`/`POSIX`/`ucs_basic`; name/OID — **DONE 2026-08-03** (`session_to_regcollation_visible_e2e`) |

**Çıkış kapısı**

- [x] to_regcollation / pg_collation_is_visible session e2e PASS (GL1)

---

### Faz GM — `pg_jit_available` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GM1 | `pg_jit_available()` | Always `true` (Cranelift JIT linked) — **DONE 2026-08-03** (`session_pg_jit_available_e2e`) |

**Çıkış kapısı**

- [x] pg_jit_available session e2e PASS (GM1)

---

### Faz GN — advisory locks (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GN1 | `pg_try/advisory_lock` + unlock(+_all) | Session-scoped exclusive keys (bigint or int×2); non-blocking lock — **DONE 2026-08-03** (`session_pg_advisory_lock_e2e`) |

**Çıkış kapısı**

- [x] advisory lock session e2e PASS (GN1)

---

### Faz GO — shared advisory locks (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GO1 | `pg_try/advisory_lock_shared` + unlock_shared | Shared holders compatible; exclusive conflicts — **DONE 2026-08-03** (`session_pg_advisory_lock_shared_e2e`) |

**Çıkış kapısı**

- [x] shared advisory lock session e2e PASS (GO1)

---

### Faz GP — transaction advisory locks (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GP1 | `pg_try/advisory_xact_lock` | Exclusive; auto-release on COMMIT/ROLLBACK / auto-commit — **DONE 2026-08-03** (`session_pg_advisory_xact_lock_e2e`) |

**Çıkış kapısı**

- [x] xact advisory lock session e2e PASS (GP1)

---

### Faz GQ — shared transaction advisory locks (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GQ1 | `pg_try/advisory_xact_lock_shared` | Shared xact holders; exclusive conflicts; auto-release — **DONE 2026-08-03** (`session_pg_advisory_xact_lock_shared_e2e`) |

**Çıkış kapısı**

- [x] shared xact advisory lock session e2e PASS (GQ1)

---

### Faz GR — `current_query` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GR1 | `current_query()` | Echoes executing SQL text (simple query); NULL if unknown — **DONE 2026-08-03** (`session_current_query_e2e`) |

**Çıkış kapısı**

- [x] current_query session e2e PASS (GR1)

---

### Faz GS — `txid_status` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GS1 | `txid_status` / `pg_xact_status` | Current→in progress; past→committed; invalid/future→NULL — **DONE 2026-08-03** (`session_txid_status_e2e`) |

**Çıkış kapısı**

- [x] txid_status session e2e PASS (GS1)

---

### Faz GT — config/log admin stubs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GT1 | `pg_reload_conf` / `pg_rotate_logfile` | Both return true (no-op success stubs) — **DONE 2026-08-03** (`session_pg_reload_rotate_logfile_e2e`) |

**Çıkış kapısı**

- [x] pg_reload_conf / pg_rotate_logfile session e2e PASS (GT1)

---

### Faz GU — NOTIFY stubs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GU1 | `pg_notify` + `pg_notification_queue_usage` | Accept channel/payload (no delivery); queue usage 0 — **DONE 2026-08-03** (`session_pg_notify_queue_usage_e2e`) |

**Çıkış kapısı**

- [x] pg_notify / queue usage session e2e PASS (GU1)

---

### Faz GV — snapshot export (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GV1 | `pg_export_snapshot` + `pg_current_snapshot` / `txid_current_snapshot` | Opaque export id; `xmin:xmax:` text snapshot — **DONE 2026-08-03** (`session_pg_snapshot_e2e`) |

**Çıkış kapısı**

- [x] snapshot export session e2e PASS (GV1)

---

### Faz GW — snapshot introspection (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GW1 | `pg_snapshot_xmin/xmax` + `pg_visible_in_snapshot` | Parse `xmin:xmax:xip` text; classic visibility — **DONE 2026-08-03** (`session_pg_snapshot_inspect_e2e`) |

**Çıkış kapısı**

- [x] snapshot inspect session e2e PASS (GW1)

---

### Faz GX — backend signal stubs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GX1 | `pg_cancel_backend` / `pg_terminate_backend` | True only for this process pid (no-op); else false — **DONE 2026-08-03** (`session_pg_signal_backend_e2e`) |

**Çıkış kapısı**

- [x] cancel/terminate backend session e2e PASS (GX1)

---

### Faz GY — WAL LSN stubs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GY1 | `pg_current_wal_{lsn,insert_lsn,flush_lsn}` + `pg_wal_lsn_diff` | Synthetic `hi/lo` LSN; byte diff — **DONE 2026-08-03** (`session_pg_wal_lsn_e2e`) |

**Çıkış kapısı**

- [x] WAL LSN session e2e PASS (GY1)

---

### Faz GZ — WAL file name (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| GZ1 | `pg_walfile_name` / `pg_walfile_name_offset` | 16 MiB segment layout; offset as `name,off` text — **DONE 2026-08-03** (`session_pg_walfile_name_e2e`) |

**Çıkış kapısı**

- [x] walfile name session e2e PASS (GZ1)

---

### Faz HA — WAL switch (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HA1 | `pg_switch_wal` / `pg_switch_xlog` | Advance synthetic LSN to next 16 MiB segment — **DONE 2026-08-03** (`session_pg_switch_wal_e2e`) |

**Çıkış kapısı**

- [x] pg_switch_wal session e2e PASS (HA1)

---

### Faz HB — standby WAL status stubs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HB1 | `pg_last_wal_{receive,replay}_lsn` + replay timestamp + `pg_is_wal_replay_paused` | Primary: LSNs/timestamp NULL; paused false — **DONE 2026-08-03** (`session_pg_standby_wal_e2e`) |

**Çıkış kapısı**

- [x] standby WAL status session e2e PASS (HB1)

---

### Faz HC — WAL replay pause/resume (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HC1 | `pg_wal_replay_pause` / `pg_wal_replay_resume` | Toggle process-global paused flag; `pg_is_wal_replay_paused` reflects it — **DONE 2026-08-03** (`session_pg_wal_replay_pause_e2e`) |

**Çıkış kapısı**

- [x] wal replay pause/resume session e2e PASS (HC1)

---

### Faz HD — backup control stubs (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HD1 | `pg_is_in_backup` / `pg_backup_start_time` / `pg_backup_start`/`stop` (+ aliases) | Process-global backup flag + LSN labels — **DONE 2026-08-03** (`session_pg_backup_e2e`) |

**Çıkış kapısı**

- [x] backup control session e2e PASS (HD1)

---

### Faz HE — restore points (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HE1 | `pg_create_restore_point(name)` | Named restore point → current WAL LSN — **DONE 2026-08-03** (`session_pg_create_restore_point_e2e`) |

**Çıkış kapısı**

- [x] create restore point session e2e PASS (HE1)

---

### Faz HF — promote stub (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HF1 | `pg_promote([wait [, wait_seconds]])` | Primary stub → false (not a standby) — **DONE 2026-08-03** (`session_pg_promote_e2e`) |

**Çıkış kapısı**

- [x] pg_promote session e2e PASS (HF1)

---

### Faz HG — `pg_size_bytes` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HG1 | `pg_size_bytes(text)` | Inverse of `pg_size_pretty` (bytes/kB/MB/…) — **DONE 2026-08-03** (`session_pg_size_bytes_e2e`) |

**Çıkış kapısı**

- [x] pg_size_bytes session e2e PASS (HG1)

---

### Faz HH — listening channels (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HH1 | `pg_listening_channels()` | Empty array until LISTEN exists — **DONE 2026-08-03** (`session_pg_notify_queue_usage_e2e`) |

**Çıkış kapısı**

- [x] pg_listening_channels session e2e PASS (HH1)

---

### Faz HI — LISTEN / UNLISTEN (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HI1 | `LISTEN` / `UNLISTEN` / `UNLISTEN *` | Session channel set; `pg_listening_channels()` reflects it — **DONE 2026-08-03** (`session_listen_unlisten_e2e`) |

**Çıkış kapısı**

- [x] LISTEN/UNLISTEN session e2e PASS (HI1)

---

### Faz HJ — NOTIFY delivery (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HJ1 | `NOTIFY` + queue delivery | Statement + `pg_notify` enqueue to LISTENers; `pg_notification_queue_usage` — **DONE 2026-08-03** (`session_notify_delivery_e2e`) |

**Çıkış kapısı**

- [x] NOTIFY delivery session e2e PASS (HJ1)

---

### Faz HK — `pg_conf_load_time` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HK1 | `pg_conf_load_time()` | Timestamp; advances on `pg_reload_conf()` — **DONE 2026-08-03** (`session_pg_conf_load_time_e2e`) |

**Çıkış kapısı**

- [x] pg_conf_load_time session e2e PASS (HK1)

---

### Faz HL — sequences (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HL1 | `nextval` / `currval` / `lastval` / `setval` | In-memory named sequences; session currval/lastval — **DONE 2026-08-03** (`session_sequence_nextval_e2e`) |

**Çıkış kapısı**

- [x] sequence scalars session e2e PASS (HL1)

---

### Faz HM — CREATE/DROP SEQUENCE (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HM1 | `CREATE SEQUENCE` / `DROP SEQUENCE` | START/INCREMENT; IF NOT EXISTS / IF EXISTS — **DONE 2026-08-03** (`session_create_drop_sequence_e2e`) |

**Çıkış kapısı**

- [x] CREATE/DROP SEQUENCE session e2e PASS (HM1)

---

### Faz HN — ALTER SEQUENCE / serial link (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HN1 | `ALTER SEQUENCE` + `pg_get_serial_sequence` | RESTART/INCREMENT/OWNED BY; serial lookup — **DONE 2026-08-03** (`session_alter_sequence_serial_e2e`) |

**Çıkış kapısı**

- [x] ALTER SEQUENCE / pg_get_serial_sequence session e2e PASS (HN1)

---

### Faz HO — SERIAL columns (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HO1 | `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` | Map to INT/BIGINT/SMALLINT; auto `{t}_{c}_seq` + OWNED BY — **DONE 2026-08-03** (`session_create_table_serial_e2e`) |

**Çıkış kapısı**

- [x] SERIAL CREATE TABLE session e2e PASS (HO1)

---

### Faz HP — SERIAL INSERT / DROP cleanup (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HP1 | Omitted SERIAL cols + DROP TABLE | `nextval` fill on INSERT; DROP TABLE drops owned seqs — **DONE 2026-08-03** (`session_serial_insert_default_and_drop_e2e`) |

**Çıkış kapısı**

- [x] SERIAL insert default / DROP cleanup session e2e PASS (HP1)

---

### Faz HQ — ALTER ADD/DROP SERIAL (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HQ1 | `ALTER TABLE ADD/DROP COLUMN SERIAL` | Add creates seq+OWNED BY; drop removes it — **DONE 2026-08-03** (`session_alter_add_drop_serial_column_e2e`) |

**Çıkış kapısı**

- [x] ALTER ADD/DROP SERIAL session e2e PASS (HQ1)

---

### Faz HR — `pg_sequence_last_value` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HR1 | `pg_sequence_last_value(regclass)` | Last issued value; NULL until first nextval — **DONE 2026-08-03** (`session_pg_sequence_last_value_e2e`) |

**Çıkış kapısı**

- [x] pg_sequence_last_value session e2e PASS (HR1)

---

### Faz HS — RENAME SEQUENCE (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HS1 | `ALTER SEQUENCE … RENAME TO` | Rename + update OWNED BY links — **DONE 2026-08-03** (`session_alter_sequence_rename_e2e`) |

**Çıkış kapısı**

- [x] ALTER SEQUENCE RENAME TO session e2e PASS (HS1)

---

### Faz HT — `has_sequence_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HT1 | `has_sequence_privilege` | USAGE/SELECT/UPDATE/ALL; missing seq errors — **DONE 2026-08-03** (`session_has_sequence_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_sequence_privilege session e2e PASS (HT1)

---

### Faz HU — `has_tablespace_privilege` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HU1 | `has_tablespace_privilege` | CREATE/ALL on `pg_default`/`pg_global`; missing → error — **DONE 2026-08-03** (`session_has_tablespace_privilege_e2e`) |

**Çıkış kapısı**

- [x] has_tablespace_privilege session e2e PASS (HU1)

---

### Faz HV — `pg_tablespace_location` (P3)

| # | İş | Kabul kriteri |
|---|-----|----------------|
| HV1 | `pg_tablespace_location` | `pg_default`/`pg_global`/OID → `''`; missing → error — **DONE 2026-08-03** (`session_pg_tablespace_location_e2e`) |

**Çıkış kapısı**

- [x] pg_tablespace_location session e2e PASS (HV1)

---

## Faz W — Dağıtık commit dayanıklılığı (P0) — 2026-08-07

| # | İş | Kabul kriteri |
|---|-----|----------------|
| W1 | Durable TC decision log (`TC_DECISIONS`) + shared engine TC | Crash-after-decide → reopen recover COMMIT — **DONE** (`twopc_tc_crash_after_decide_recovers_commit`) |
| W2 | Session uses `engine.txn_coordinator()` | Ephemeral `TransactionCoordinator::new` kaldırıldı — **DONE** |

---

## Faz X — Kimlik / şema (P0) — 2026-08-07

| # | İş | Kabul |
|---|-----|--------|
| X1 | Durable `SEQUENCES` file | reopen `nextval` continues — **DONE** (`session_serial_survives_engine_reopen`) |
| X2 | Column `DEFAULT` | literal defaults fill — **DONE** |
| X3 | `NOT NULL` | insert null rejected — **DONE** |
| X4 | `UNIQUE` + auto `uq_*` index | duplicate rejected — **DONE** (`session_default_not_null_unique_e2e`) |

---

## Faz Y — Tip coerce (P1) — 2026-08-07

| # | İş | Kabul |
|---|-----|--------|
| Y1 | UUID / BYTEA / NUMERIC / TIMESTAMPTZ validate on INSERT | soft coerce in `validate_record_against_catalog` — **DONE** |

---

## Faz Z — MPP ürün yolu (P1) — 2026-08-07

| # | İş | Kabul |
|---|-----|--------|
| Z1 | `--mpp` / `TAKYONIC_MPP` server flag | **DONE** |
| Z2 | Honest DistributedAggregate (SUM/COUNT only) | **DONE** (`is_simple_distributed_agg`) |

---

## Faz AA — COPY dosya (P1) — 2026-08-07

| # | İş | Kabul |
|---|-----|--------|
| AA1 | `COPY table FROM|TO 'path'` (TSV) | **DONE** (`session_copy_file_roundtrip_e2e`) |

---

## Önerilen sıra

```text
A (SQL DDL + typed + catalog Raft)
    → B (2PC wire)
        → C (MPP exec)
            → D (S3 server)
                → E (pg compat)
                    → F (polish)
```

**Paralel öneri:** A4/A5 tasarımı B ile aynı Raft komut çerçevesini paylaşabilir; C, A tamamlanmadan da başlayabilir ama ürün mesajı A+B olmadan “NewSQL” dememeli.

**Bilerek ertelenenler**

- Tam PostgreSQL uyumu / her SQL diyalekti
- Gerçek full Cahill SSI — 4B: minimal doom first-cut; deeper SSI later
- Çoklu database (I1: yalnızca `postgres` kabul; gerçek multi-DB yok)
- BPM loom/TSan CI (araç zinciri kısıtı; ayrı hardening)
- Full Cahill SSI (4B: minimal doom first-cut)

---

## Başarı metrikleri

| Metrik | Baseline (2026-08-02) | Faz A sonrası | Faz B+C sonrası |
|--------|------------------------|---------------|-----------------|
| Lib tests | 237 PASS | ≥ baseline + DDL/catalog suite | + 2PC/MPP e2e |
| psql CREATE TABLE | FAIL | PASS | PASS |
| Cross-shard txn | test-only | test-only | server SQL/client PASS |
| MPP exec | local fallback | local fallback | `mpp_enabled` gerçek dağıtık |
| ORM migrate | imkânsız | hâlâ zor (E yoksa) | stub catalog ile kısmi |

---

## İlgili belgeler

- `docs/ARCHITECTURE.md` — mevcut istek yaşam döngüsü
- `docs/RELIABILITY.md` — chaos / crucible
- `docs/superpowers/plans/2026-08-07-s3-chunk-dirty-coalesce.md` — Faz 1A write-amp
- `docs/superpowers/plans/2026-07-20-2pc-production-path.md` — 2PC path (COMPLETE)
- `docs/superpowers/plans/2026-07-20-s3-chunked-pages.md` — S3 chunk V2 (COMPLETE)
- Assessment canvas (lokal): `canvases/takyonic-db-assessment.canvas.tsx`

---

## Değişiklik günlüğü

| Tarih | Not |
|-------|-----|
| 2026-08-02 | İlk sürüm — readiness assessment P0–P3 → Faz A–F |
| 2026-08-02 | Loop tick0: A1 local CREATE/DROP TABLE + CATALOG `COLUMN`; 240 lib tests PASS |
| 2026-08-02 | Loop tick1: A2 Project + catalog OID hints; 242+ lib tests |
| 2026-08-02 | Loop tick2: A3 ALTER TABLE ADD/DROP COLUMN; 244 lib tests |
| 2026-08-02 | Loop tick63: AV1 `split_part`; 365 lib tests PASS |
| 2026-08-02 | Loop tick64: AW1 `regexp_split_to_array` (+ `regex` dep); 367 lib tests PASS |
| 2026-08-02 | Loop tick65: AX1 `regexp_split_to_table` SRF; 369 lib tests PASS |
| 2026-08-02 | Loop tick66: AY1 `regexp_replace`; 371 lib tests PASS |
| 2026-08-02 | Loop tick67: AZ1 `regexp_like` + `regexp_matches`; 373 lib tests PASS |
| 2026-08-02 | Loop tick68: BA1 `lpad`/`rpad`/`repeat`; 375 lib tests PASS |
| 2026-08-02 | Loop tick69: BB1 `left`/`right`/`reverse`; 377 lib tests PASS |
| 2026-08-02 | Loop tick70: BC1 `initcap`/`ascii`/`chr`; 379 lib tests PASS |
| 2026-08-02 | Loop tick71: BD1 `md5`/`encode`/`decode`; 381 lib tests PASS |
| 2026-08-02 | Loop tick72: BE1 `starts_with`/`overlay`; 383 lib tests PASS |
| 2026-08-02 | Loop tick73: BF1 `translate`/`btrim`/`ltrim`/`rtrim`; 385 lib tests PASS |
| 2026-08-02 | Loop tick74: BG1 `concat_ws`/`format`; 387 lib tests PASS |
| 2026-08-02 | Loop tick75: BH1 `ends_with` + GREATEST/LEAST NULL-skip; 387 lib tests PASS |
| 2026-08-02 | Loop tick76: BI1 `quote_ident`/`quote_literal`; 389 lib tests PASS |
| 2026-08-02 | Loop tick77: BJ1 `quote_nullable`/`width_bucket`; 391 lib tests PASS |
| 2026-08-02 | Loop tick78: BK1 `sign`/`trunc`/`div`; 393 lib tests PASS |
| 2026-08-02 | Loop tick79: BL1 `pi`/`sqrt`/`cbrt`/`ln`/`log`/`exp`; 395 lib tests PASS |
| 2026-08-02 | Loop tick80: BM1 trig + `radians`/`degrees`; 397 lib tests PASS |
| 2026-08-02 | Loop tick81: BN1 correlated LATERAL `jsonb_array_elements`; 397 lib tests PASS |
| 2026-08-02 | Loop tick82: BO1 correlated LATERAL `json_each`/`object_keys`; 398 lib tests PASS |
| 2026-08-02 | Loop tick83: BP1 correlated LATERAL `unnest`; 399 lib tests PASS |
| 2026-08-02 | Loop tick84: BQ1 correlated LATERAL regexp SRFs; 400 lib tests PASS |
| 2026-08-02 | Loop tick85: BR1 WITH ORDINALITY generate_series/unnest; 400 lib tests PASS |
| 2026-08-02 | Loop tick86: BS1 TRIM FROM custom characters; 400 lib tests PASS |
| 2026-08-02 | Loop tick87: BT1 JSON SRF WITH ORDINALITY; 400 lib tests PASS |
| 2026-08-02 | Loop tick88: BU1 json_each WITH ORDINALITY; 400 lib tests PASS |
| 2026-08-02 | Loop tick89: BV1 regexp SRF WITH ORDINALITY; 400 lib tests PASS |
| 2026-08-02 | Loop tick90: BW1 string_agg / array_agg; 401 lib tests PASS |
| 2026-08-02 | Loop tick91: BX1 bool_and / bool_or / every; 402 lib tests PASS |
| 2026-08-02 | Loop tick92: BY1 bit_and / bit_or; 403 lib tests PASS |
| 2026-08-02 | Loop tick93: BZ1 aggregate FILTER (WHERE); 404 lib tests PASS |
| 2026-08-02 | Loop tick94: CA1 COUNT(DISTINCT) / distinct aggs; 405 lib tests PASS |
| 2026-08-02 | Loop tick95: CB1 aggregate ORDER BY (string_agg/array_agg); 406 lib tests PASS |
| 2026-08-02 | Loop tick96: CC1 stddev / variance aggregates; 407 lib tests PASS |
| 2026-08-02 | Loop tick97: CD1 corr / covar aggregates; 408 lib tests PASS |
| 2026-08-02 | Loop tick98: CE1 regr_slope / intercept / r2; 409 lib tests PASS |
| 2026-08-02 | Loop tick99: CF1 regr_count/avgx/avgy/sxx/syy/sxy; 409 lib tests PASS |
| 2026-08-02 | Loop tick100: CG1 UNNEST WITH OFFSET; 409 lib tests PASS |
| 2026-08-02 | Loop tick101: CH1 timestamp generate_series; 409 lib tests PASS |
| 2026-08-02 | Loop tick102: CI1 MODE / WITHIN GROUP; 410 lib tests PASS |
| 2026-08-02 | Loop tick103: CJ1 percentile_cont/disc; 411 lib tests PASS |
| 2026-08-02 | Loop tick104: CK1 bare HAVING; 413 lib tests PASS |
| 2026-08-02 | Loop tick105: CL1 ROW_NUMBER() OVER; 415 lib tests PASS |
| 2026-08-02 | Loop tick106: CM1 RANK/DENSE_RANK; 416 lib tests PASS |
| 2026-08-02 | Loop tick107: CN1 window PARTITION BY; 417 lib tests PASS |
| 2026-08-02 | Loop tick108: CO1 LAG/LEAD; 418 lib tests PASS |
| 2026-08-02 | Loop tick109: CP1 NTILE; 419 lib tests PASS |
| 2026-08-02 | Loop tick110: CQ1 FIRST_VALUE/LAST_VALUE; 420 lib tests PASS |
| 2026-08-02 | Loop tick111: CR1 NTH_VALUE; 421 lib tests PASS |
| 2026-08-02 | Loop tick112: CS1 PERCENT_RANK/CUME_DIST; 422 lib tests PASS |
| 2026-08-02 | Loop tick113: CT1 ROWS window frames; 423 lib tests PASS |
| 2026-08-02 | Loop tick114: CU1 named WINDOW; 424 lib tests PASS |
| 2026-08-02 | Loop tick115: CV1 window aggregates; 425 lib tests PASS |
| 2026-08-02 | Loop tick116: CW1 RANGE frames; 426 lib tests PASS |
| 2026-08-02 | Loop tick117: CX1 RANGE value offsets; 426 lib tests PASS |
| 2026-08-02 | Loop tick118: CY1 GROUPS frames; 427 lib tests PASS |
| 2026-08-02 | Loop tick119: CZ1 STRING_AGG/ARRAY_AGG OVER; 428 lib tests PASS |
| 2026-08-02 | Loop tick120: DA1 BOOL/JSON window aggs; 429 lib tests PASS |
| 2026-08-02 | Loop tick121: DB1 STDDEV/VAR OVER; 430 lib tests PASS |
| 2026-08-02 | Loop tick122: DC1 window FILTER; 431 lib tests PASS |
| 2026-08-02 | Loop tick123: DD1 CORR/COVAR OVER; 432 lib tests PASS |
| 2026-08-02 | Loop tick124: DE1 REGR_* OVER; 433 lib tests PASS |
| 2026-08-02 | Loop tick125: DF1 BIT_*/MODE OVER; 434 lib tests PASS |
| 2026-08-02 | Loop tick126: DG1 JSON_OBJECT_AGG OVER; 435 lib tests PASS |
| 2026-08-02 | Loop tick127: DH1 IGNORE NULLS; 436 lib tests PASS |
| 2026-08-02 | Loop tick128: DI1 DISTINCT ON; 437 lib tests PASS |
| 2026-08-02 | Loop tick129: DJ1 window EXCLUDE; 438 lib tests PASS |
| 2026-08-02 | Loop tick130: DK1 FETCH WITH TIES; 439 lib tests PASS |
| 2026-08-02 | Loop tick131: DL1 ORDER BY NULLS FIRST/LAST; 440 lib tests PASS |
| 2026-08-02 | Loop tick132: DM1 TRUNCATE TABLE; 442 lib tests PASS |
| 2026-08-02 | Loop tick133: DN1 IS DISTINCT FROM; 444 lib tests PASS |
| 2026-08-03 | Loop tick134: DO1 IS TRUE/FALSE/UNKNOWN; 446 lib tests PASS |
| 2026-08-03 | Loop tick135: DP1 BinaryOp/AND/OR/NOT 3VL NULL; 447 lib tests PASS |
| 2026-08-03 | Loop tick136: DQ1 ANY/SOME/ALL quantified; 435 lib tests PASS (pg.rs history restore) |
| 2026-08-03 | Loop tick137: DR1 restore DI–DN session e2es; 441 lib tests PASS |
| 2026-08-03 | Loop tick138: DS1 SIMILAR TO; 444 lib tests PASS |
| 2026-08-03 | Loop tick139: DT1 VALUES clause; 446 lib tests PASS |
| 2026-08-03 | Loop tick140: DU1 ~ / ~* / !~ / !~*; 448 lib tests PASS |
| 2026-08-03 | Loop tick76: DV1 RETURNING on INSERT/UPDATE/DELETE; 452 lib tests PASS |
| 2026-08-03 | Loop tick77: DW1 LIKE/ILIKE ANY; 454 lib tests PASS |
| 2026-08-03 | Loop tick78: DX1 GROUP BY ALL; 456 lib tests PASS |
| 2026-08-03 | Loop tick79: DY1 ORDER BY ALL; 458 lib tests PASS |
| 2026-08-03 | Loop tick80: DZ1 ON CONFLICT DO NOTHING; 460 lib tests PASS |
| 2026-08-03 | Loop tick81: EA1 ON CONFLICT DO UPDATE; 462 lib tests PASS |
| 2026-08-03 | Loop tick82: EB1 CREATE TABLE AS SELECT; 464 lib tests PASS |
| 2026-08-03 | Loop tick83: EC1 AT TIME ZONE (offset zones); 466 lib tests PASS |
| 2026-08-03 | Loop tick84: ED1 INSERT…SELECT; 468 lib tests PASS |
| 2026-08-03 | Loop tick85: EE1 ALTER TABLE RENAME COLUMN/TO; 469 lib tests PASS |
| 2026-08-03 | Loop tick86: EF1 MAKE_DATE/TIME/TIMESTAMP; 471 lib tests PASS |
| 2026-08-03 | Loop tick87: EG1 TO_DATE; 473 lib tests PASS |
| 2026-08-03 | Loop tick88: EH1 MAKE_INTERVAL; 475 lib tests PASS |
| 2026-08-03 | Loop tick89: EI1 ISFINITE; 477 lib tests PASS |
| 2026-08-03 | Loop tick90: EJ1 CLOCK/STATEMENT/TRANSACTION_TIMESTAMP; 479 lib tests PASS |
| 2026-08-03 | Loop tick91: EK1 TIMEZONE(zone, ts); 481 lib tests PASS |
| 2026-08-03 | Loop tick92: EL1 DATE_BIN; 483 lib tests PASS |
| 2026-08-03 | Loop tick93: EM1 ALTER COLUMN TYPE; 484 lib tests PASS |
| 2026-08-03 | Loop tick94: EN1 JUSTIFY_HOURS/DAYS/INTERVAL; 486 lib tests PASS |
| 2026-08-03 | Loop tick95: EO1 EXTRACT(EPOCH)/DATE_PART epoch; 488 lib tests PASS |
| 2026-08-03 | Loop tick96: EP1 OVERLAPS periods; 490 lib tests PASS |
| 2026-08-03 | Loop tick97: EQ1 TIMEOFDAY(); 491 lib tests PASS |
| 2026-08-03 | Loop tick98: ER1 TO_NUMBER; 493 lib tests PASS |
| 2026-08-03 | Loop tick99: ES1 CURRENT_USER/SCHEMA/CATALOG; 495 lib tests PASS |
| 2026-08-03 | Loop tick100: ET1 VERSION(); 497 lib tests PASS |
| 2026-08-03 | Loop tick101: EU1 current_schemas(bool); 499 lib tests PASS |
| 2026-08-03 | Loop tick102: EV1 pg_backend_pid/pg_is_in_recovery; 501 lib tests PASS |
| 2026-08-03 | Loop tick103: EW1 pg_typeof/getdatabaseencoding; 503 lib tests PASS |
| 2026-08-03 | Loop tick104: EX1 pg_size_pretty/pg_client_encoding; 505 lib tests PASS |
| 2026-08-03 | Loop tick105: EY1 OCTET_LENGTH/BIT_LENGTH; 506 lib tests PASS |
| 2026-08-03 | Loop tick106: EZ1 num_nonnulls/num_nulls; 508 lib tests PASS |
| 2026-08-03 | Loop tick107: FA1 random/setseed; 510 lib tests PASS |
| 2026-08-03 | Loop tick108: FB1 CURRENT_ROLE/gen_random_uuid; 512 lib tests PASS |
| 2026-08-03 | Loop tick109: FC1 pg_sleep/pg_column_size; 514 lib tests PASS |
| 2026-08-03 | Loop tick110: FD1 txid_current/pg_postmaster_start_time; 516 lib tests PASS |
| 2026-08-03 | Loop tick111: FE1 current_setting; 518 lib tests PASS |
| 2026-08-03 | Loop tick112: FF1 set_config; 520 lib tests PASS |
| 2026-08-03 | Loop tick113: FG1 has_table_privilege; 522 lib tests PASS |
| 2026-08-03 | Loop tick114: FH1 inet_server/client addr+port; 524 lib tests PASS |
| 2026-08-03 | Loop tick115: FI1 has_schema_privilege; 527 lib tests PASS |
| 2026-08-03 | Loop tick116: FJ1 has_database_privilege; 530 lib tests PASS |
| 2026-08-03 | Loop tick117: FK1 pgwire inet wire-up; 532 lib tests PASS |
| 2026-08-03 | Loop tick118: FL1 has_column_privilege; 535 lib tests PASS |
| 2026-08-03 | Loop tick119: FM1 has_any_column_privilege; 537 lib tests PASS |
| 2026-08-03 | Loop tick120: FN1 COMMENT ON + obj/col_description; 539 lib tests PASS |
| 2026-08-03 | Loop tick121: FO1 durable COMMENTS file; 540 lib tests PASS |
| 2026-08-03 | Loop tick122: FP1 shobj_description + COMMENT ON ROLE/DATABASE; 541 lib tests PASS |
| 2026-08-03 | Loop tick123: FQ1 Raft CommentsReplace; 542 lib tests PASS |
| 2026-08-03 | Loop restore after OOM + tick124: FR1 GRANT ON SCHEMA; 544 lib tests PASS |
| 2026-08-03 | Loop tick125: FS1 column GRANT ACL; 546 lib tests PASS |
| 2026-08-03 | Loop tick126: FT1 to_regclass + OID descriptions; 548 lib tests PASS |
| 2026-08-03 | Loop tick127: FU1 format_type + pg_get_userbyid; 551 lib tests PASS |
| 2026-08-03 | Loop tick128: FV1 to_regrole; 552 lib tests PASS |
| 2026-08-03 | Loop tick129: FW1 pg_relation_size family; 553 lib tests PASS |
| 2026-08-03 | Loop tick130: FX1 pg_indexes_size + pg_database_size; 554 lib tests PASS |
| 2026-08-03 | Loop tick131: FY1 to_regnamespace + to_regtype; 556 lib tests PASS |
| 2026-08-03 | Loop tick132: FZ1 has_function_privilege; 559 lib tests PASS |
| 2026-08-03 | Loop tick133: GA1 pg_has_role; 562 lib tests PASS |
| 2026-08-03 | Loop tick134: GB1 has_type_privilege; 565 lib tests PASS |
| 2026-08-03 | Loop tick135: GC1 pg_encoding_to_char / pg_char_to_encoding; 567 lib tests PASS |
| 2026-08-03 | Loop tick136: GD1 pg_table_is_visible / pg_type_is_visible; 570 lib tests PASS |
| 2026-08-03 | Loop tick137: GE1 to_regproc + pg_function_is_visible; 572 lib tests PASS |
| 2026-08-03 | Loop tick138: GF1 pg_relation_is_updatable; 575 lib tests PASS |
| 2026-08-03 | Loop tick139: GG1 pg_column_is_updatable; 578 lib tests PASS |
| 2026-08-03 | Loop tick140: GH1 pg_get_indexdef; 581 lib tests PASS |
| 2026-08-03 | Loop tick141: GI1 pg_describe_object; 584 lib tests PASS |
| 2026-08-03 | Loop tick142: GJ1 pg_identify_object; 587 lib tests PASS |
| 2026-08-03 | Loop tick143: GK1 to_regoper + pg_operator_is_visible; 589 lib tests PASS |
| 2026-08-03 | Loop tick144: GL1 to_regcollation + pg_collation_is_visible; 591 lib tests PASS |
| 2026-08-03 | Loop tick145: GM1 pg_jit_available; 592 lib tests PASS |
| 2026-08-03 | Loop tick146: GN1 advisory locks; 595 lib tests PASS |
| 2026-08-03 | Loop tick147: GO1 shared advisory locks; 598 lib tests PASS |
| 2026-08-03 | Loop tick148: GP1 xact advisory locks; 601 lib tests PASS |
| 2026-08-03 | Loop tick149: GQ1 shared xact advisory locks; 604 lib tests PASS |
| 2026-08-03 | Loop tick150: GR1 current_query; 605 lib tests PASS |
| 2026-08-03 | Loop tick151: GS1 txid_status / pg_xact_status; 607 lib tests PASS |
| 2026-08-03 | Loop tick152: GT1 pg_reload_conf / pg_rotate_logfile; 608 lib tests PASS |
| 2026-08-03 | Loop tick153: GU1 pg_notify / notification_queue_usage; 610 lib tests PASS |
| 2026-08-03 | Loop tick154: GV1 pg_export/current_snapshot; 612 lib tests PASS |
| 2026-08-03 | Loop tick155: GW1 snapshot xmin/xmax/visible; 614 lib tests PASS |
| 2026-08-03 | Loop tick156: GX1 pg_cancel/terminate_backend; 616 lib tests PASS |
| 2026-08-03 | Loop tick157: GY1 WAL LSN stubs; 618 lib tests PASS |
| 2026-08-03 | Loop tick158: GZ1 pg_walfile_name(+offset); 620 lib tests PASS |
| 2026-08-03 | Loop tick159: HA1 pg_switch_wal / xlog; 622 lib tests PASS |
| 2026-08-03 | Loop tick160: HB1 standby WAL receive/replay stubs; 624 lib tests PASS |
| 2026-08-03 | Loop tick161: HC1 wal replay pause/resume; 626 lib tests PASS |
| 2026-08-03 | Loop tick162: HD1 backup start/stop stubs; 628 lib tests PASS |
| 2026-08-03 | Loop tick163: HE1 pg_create_restore_point; 630 lib tests PASS |
| 2026-08-03 | Loop tick164: HF1 pg_promote stub; 632 lib tests PASS |
| 2026-08-03 | Loop tick165: HG1 pg_size_bytes; 634 lib tests PASS |
| 2026-08-03 | Loop tick166: HH1 pg_listening_channels; 634 lib tests PASS |
| 2026-08-03 | Loop tick179: HU1 has_tablespace_privilege; 661 lib tests PASS |
| 2026-08-03 | Loop tick180: HV1 pg_tablespace_location; 663 lib tests PASS |
| 2026-08-07 | Faz W–AA üretim yolu DONE (TC log, sequences, types, MPP, COPY) |
| 2026-08-07 | Faz 1A–1C: S3 chunk coalesce + W–AA chaos + docs polish |
| 2026-08-07 | Faz 2: ORM types Describe + COPY STDIN/STDOUT + smoke-orm-types.sh |
