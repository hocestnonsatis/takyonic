## Summary

<!-- Explain the problem and why this change is needed. -->

## Approach

<!-- Describe the chosen design and important alternatives considered. -->

## Correctness and compatibility

<!--
Describe effects on durability, Raft safety, MVCC behavior, on-disk or wire
formats, performance, and existing clients. Write "None" where appropriate.
-->

## Test plan

<!-- List the exact commands, workloads, and relevant results. -->

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo check --all-targets --all-features`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --release --all-features`
- [ ] Relevant stress or failure harnesses were run, or are not applicable.
- [ ] User-facing documentation was updated, or is not applicable.

## Checklist

- [ ] This change is focused and contains no unrelated refactoring.
- [ ] Tests demonstrate new behavior or reproduce the fixed bug.
- [ ] No credentials, private data, or machine-specific paths are included.
- [ ] Any intentional compatibility or format change is clearly documented.
