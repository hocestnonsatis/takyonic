# Contributing to Takyonic

Thank you for helping improve Takyonic. Storage and consensus changes can have
subtle correctness consequences, so contributions should be focused,
well-tested, and explicit about their durability and failure assumptions.

## Before you start

- Search existing issues and pull requests before opening a duplicate.
- For large features or protocol/storage-format changes, open an issue first so
  the design can be discussed before implementation.
- Never include credentials, private datasets, or machine-specific build paths
  in a contribution.

## Development setup

Install Rust 1.85 or newer with the `rustfmt` and `clippy` components:

```bash
rustup component add rustfmt clippy
git clone https://github.com/hocestnonsatis/takyonic.git
cd takyonic
cargo check
```

## Making a change

1. Fork the repository and create a focused topic branch.
2. Keep public APIs documented and avoid unrelated refactors.
3. Add tests that demonstrate the intended behavior and, for bug fixes, fail
   without the fix.
4. Preserve the on-disk compatibility and crash-safety invariants described in
   [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), or clearly document any
   intentional format change.
5. Update user-facing documentation when behavior or configuration changes.

Please do not replace the in-tree storage or Raft implementations with an
external database engine. Compaction work must remain isolated from the
Raft/WAL durability path, and memory-mapped SSTables must not be removed while
readers hold pins.

## Required checks

Run the same checks as CI before submitting:

```bash
cargo fmt --all -- --check
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test --release --all-features
```

When changing recovery, replication, snapshots, transactions, or compaction,
also run the relevant example stress harness. Include the command, workload,
hardware, and result in the pull request so performance claims are reproducible.

## Pull requests

Use a concise title and explain:

- the problem and why the change is needed;
- the chosen design and important alternatives;
- correctness, compatibility, and performance implications; and
- exactly how the change was tested.

Keep commits reviewable and use imperative commit subjects (for example,
`fix snapshot installation after follower restart`). All CI checks must pass
before merge.

## Reporting security issues

Do not open a public issue for a suspected vulnerability. Follow
[SECURITY.md](SECURITY.md) so maintainers can investigate and coordinate
disclosure privately.

## License

By contributing, you agree that your contribution is licensed under the
repository's dual MIT OR Apache-2.0 license.
