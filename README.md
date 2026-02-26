# NovaRocks

NovaRocks is a Rust-based compute engine project under the StarRocks ecosystem.

It is currently an independent project and keeps FE protocol compatibility through a C++ shim layer, with execution semantics implemented in Rust. At this stage, the repository is intended for learning and experimentation, not production use.

## Project Goals

- This project is primarily created to study StarRocks architecture and code design.
- Most of the code in this repository is AI-generated.
- The project has not yet gone through detailed, production-grade testing.
- Current practical uses:
  - learning StarRocks BE design ideas
  - acting as a StarRocks BE mock server for rapid vibe-coding development（Friendly for building on macOS）

## Design Principles

- Arrow-first execution model: NovaRocks uses Arrow `RecordBatch` (wrapped as `Chunk`) as the in-memory data format. This is the base of vectorized processing and keeps execution batch-oriented instead of row-oriented.
- Protocol and execution separation: the C++ shim is used as the FE protocol/brpc gateway, while plan lowering, operator semantics, and runtime behavior are implemented in Rust.
- Strict semantics over implicit fallback: execution follows FE-provided plan/type metadata directly; unsupported or ambiguous semantics are expected to fail fast instead of silently degrading behavior.
- Pipeline-native runtime: plans are lowered into a pipeline graph with drivers/operators/dependencies, then scheduled by dedicated runtime executors for parallel execution.
- Exchange-aware distributed flow: inter-fragment data transfer is handled through gRPC-based exchange with chunk encode/decode, sender EOS tracking, and cancellation-aware cleanup.
- Experimental support for using non-StarRocks native formats as StarRocks data files (for example, Parquet) is in progress and not production-ready.
Share-nothing mode is not supported; only share-data mode is supported.

## Prerequisites

- Rust toolchain (edition 2024, `cargo`)
- C/C++ build toolchain
- Third-party root directory: `./thirdparty`
- If setting `STARROCKS_THIRDPARTY`, point it to the thirdparty root (for example `/path/to/starrocks/thirdparty`), not `.../installed`

If third-party dependencies are missing, build them first:

```bash
./thirdparty/build-thirdparty.sh
```

## Build

```bash
./build.sh
```

Build with external StarRocks thirdparty:

```bash
export STARROCKS_THIRDPARTY=/path/to/starrocks/thirdparty
./build.sh build
```

## Configuration

NovaRocks loads config in this order:

1. `--config <path>`
2. `NOVAROCKS_CONFIG=<path>`
3. `./novarocks.toml`

Useful files in this repo:

- `novarocks.toml` (local runtime config)
- `novarocks.toml.example` (extended documented template)

## Run

CLI usage:

```bash
novarocks [run|start|stop|restart] [--config <path>]
```

Run through control script:

```bash
# foreground (default)
./bin/novarocksctl start

# daemon mode
./bin/novarocksctl start --daemon

# stop daemon
./bin/novarocksctl stop

# restart daemon
./bin/novarocksctl restart
```

Run built binary directly:

```bash
./target/debug/novarocks run --config ./novarocks.toml
```

## Development Workflow

```bash
cargo fmt --all
cargo clippy --all-targets --all-features
./build.sh
cargo test
```

## License

Apache License 2.0. See `LICENSE.txt`.
