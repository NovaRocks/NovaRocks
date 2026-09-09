# Connector binding benchmark harness

This root-workspace benchmark package freezes the NCP-2R4 workload identity before
product edits. It writes stable JSON containing the git revision, build profile,
sample settings, workload SHA-256, supported measurements, and explicit
unsupported measurements.

Run it from the repository root:

```bash
cargo run --manifest-path tools/connector-binding-bench/Cargo.toml -- \
  --output logs/connector-binding-bench/<timestamp>/before.json \
  --profile dev-opt
```

The harness does not report a synthetic microbenchmark as a product metric.
Measurements that need crate-private FE/BE instrumentation remain explicitly
unsupported until the NCP-2R4 adapter exists. The workload file, JSON schema,
and case names are immutable after T00; later work may only replace an
unsupported adapter with a semantically equivalent product measurement.
