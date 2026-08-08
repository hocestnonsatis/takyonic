# Takyonic

[![CI](https://github.com/hocestnonsatis/takyonic/actions/workflows/ci.yml/badge.svg)](https://github.com/hocestnonsatis/takyonic/actions/workflows/ci.yml)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Fast writes. Strong consistency. Familiar SQL.**

Takyonic is a distributed, MVCC-based, Raft-driven NewSQL database built from
scratch in Rust. It combines a purpose-built storage engine and consensus
implementation with cost-based query planning and PostgreSQL wire compatibility,
so existing tools such as `psql` can speak to it directly.

## The five pillars

- **LSM-Tree storage engine** — Ordered memtables feed checksummed, memory-mapped
  SSTables. Bloom filters, leveled compaction, pinned file lifetimes, and separate
  rapid/haul compaction pools keep reads safe and background I/O away from the
  durability path.
- **Custom Raft consensus** — Leader election, batched replication, quorum
  commits, snapshots, and recovery are implemented in-tree. Dynamic membership
  uses one-at-a-time, single-server configuration changes with immediate-effect
  quorums.
- **MVCC + OCC transactions** — Snapshot-isolated reads use versioned internal
  keys, while optimistic validation rejects conflicting commits. A cluster-wide
  apply index provides commit timestamps and failover-safe conflict tracking.
- **Cost-based optimizer** — Table cardinality and per-index distinct-value
  statistics let the planner choose selective secondary indexes and apply
  residual predicates without paying statistics costs on the read path.
- **PostgreSQL wire protocol** — Takyonic accepts PostgreSQL simple-query traffic
  on port `5433`, translates SQL into its logical plan, and returns native
  PostgreSQL result messages.

## Built to take a hit

Takyonic's crash-recovery crucible repeatedly interrupts write storms with
`kill -9`, injects torn WAL tails, and verifies every acknowledged write after
restart. The engine survived 28/28 chaos cycles with zero lost acknowledgements.

Group commit has delivered **15k+ operations/second on mobile ARM hardware**,
while desktop NVMe systems scale further with additional concurrency and I/O
headroom. The repository also includes leader-assassination, follower
resurrection, snapshot catch-up, and topology-mutation stress harnesses.

> Benchmark results depend on hardware, filesystem, durability settings, value
> size, and workload. Run the included harnesses on your target system.

## Quick start

Prerequisites: Rust 1.85 or newer and a PostgreSQL client.

Start the single-node demo server:

```bash
cargo run --release --bin takyonic-server
```

In another terminal, connect with `psql`:

```bash
PGPASSWORD=password psql -h 127.0.0.1 -p 5433 -U postgres -d postgres
```

By default the server seeds a demo `users` table when it is missing
(`--demo-bootstrap` / `TAKYONIC_DEMO_BOOTSTRAP=1`). Disable for an empty
catalog (`--no-demo-bootstrap` / `TAKYONIC_DEMO_BOOTSTRAP=0`) and create
tables with SQL DDL instead.

```sql
-- With demo bootstrap (default):
INSERT INTO users (id, name, city, status)
VALUES (1, 'Ada', 'London', 'active');
SELECT * FROM users WHERE status = 'active';

-- Empty catalog:
CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT);
INSERT INTO items (id, name) VALUES (1, 'widget');
```

The demo pgwire endpoint authenticates with SCRAM-SHA-256. Default role:
`postgres` / `password`. Takyonic is **single-database**: connect with
`-d postgres` (or omit `-d`); any other database name is rejected
(`SQLSTATE 3D000`).

Optional Tier-2 object storage (storage–compute decoupling):

```bash
# Local POSIX mirror
cargo run --release --bin takyonic-server -- --object-store ./objects

# MinIO / S3 (binary must be built with `--features s3`; Docker image includes it)
cargo run --release --features s3 --bin takyonic-server -- \
  --s3-endpoint http://127.0.0.1:9000 --s3-bucket takyonic \
  --s3-access-key minioadmin --s3-secret-key minioadmin

# Compose: MinIO + one S3-backed node on host :5436
docker compose --profile s3 up --build node-s3 minio

# Fast host-side smoke (MinIO via compose + local `--features s3` server):
./scripts/smoke-s3-compose.sh
```

Env equivalents: `TAKYONIC_OBJECT_STORE`, `TAKYONIC_S3_ENDPOINT`,
`TAKYONIC_S3_BUCKET`, `TAKYONIC_S3_ACCESS_KEY`, `TAKYONIC_S3_SECRET_KEY`.

**PutObject policy (no multipart):** SST uploads are capped at `max_sst_bytes`
(default 1 GiB) via flush/compaction split; BPM pages use ChunkV2 (default
64 MiB chunks). Checkpoint flushes coalesce dirty pages to **one PutObject per
touched chunk**. Single-object writes ≥5 GiB are refused so AWS/MinIO PutObject
limits are never exceeded.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the request lifecycle,
durability boundaries, SST-backed Raft snapshots, and dynamic membership model.

## Development

```bash
cargo check
cargo test --release
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) before participating. For usage help, see
[SUPPORT.md](SUPPORT.md). Report suspected vulnerabilities privately as
described in [SECURITY.md](SECURITY.md).

## License

Takyonic is licensed under either of the following, at your option:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)
