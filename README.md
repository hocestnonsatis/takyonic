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
PGPASSWORD=any psql -h 127.0.0.1 -p 5433 -U admin -d postgres
```

The server registers a demo `users` table. Try:

```sql
INSERT INTO users (id, name, city, status)
VALUES (1, 'Ada', 'London', 'active');

SELECT * FROM users WHERE status = 'active';
```

The demo pgwire endpoint accepts any username and password; place it behind
appropriate authentication before exposing it to an untrusted network.

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

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening
a pull request.

## License

Takyonic is licensed under either of the following, at your option:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)
