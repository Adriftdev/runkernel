# Contributing

runkernel is a small Rust workspace. Keep changes library-first, deterministic, and easy to test.

## Local Checks

Run these before submitting changes:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Design Guidelines

- Prefer explicit Rust APIs over configuration magic.
- Keep task execution semantics observable through events and results.
- Do not add YAML, remote workers, Kubernetes, or a web UI before the local engine is stable.
- Add tests for behavior that changes scheduling, caching, rollback, or public result types.
- Keep examples small and runnable.

## Cache Changes

Cache behavior must stay deterministic. Invalid glob patterns should fail clearly; missing matches should not fail by default.
