# Publishing runkernel Crates

## Crates

Publish these crates in dependency order:

1. `runkernel`
2. `runkernel-cli-support`
3. `runkernel-cli`

The example packages are marked `publish = false` and should not be published.

## Preflight

Run:

```bash
cargo test
cargo package -p runkernel
```

Before `runkernel` is published to crates.io, downstream verification for
`runkernel-cli-support` and `runkernel-cli` will fail with `no matching package
named runkernel found`. After `runkernel` is available in the index, verify:

```bash
cargo package -p runkernel-cli-support
cargo package -p runkernel-cli
```

## Publish

Run:

```bash
cargo publish -p runkernel
cargo publish -p runkernel-cli-support
cargo publish -p runkernel-cli
```

Wait for each published crate to appear in the crates.io index before publishing
the next dependent crate.
