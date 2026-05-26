# Benchmark Data Bootstrap

This directory contains source-controlled scripts for standard benchmark data
bootstrap. Large generated data is not stored in git.

Generated local outputs are ignored under:

- `cache/`
- `generated/`
- `parquet/`

## Scope

The bootstrap path supports these standard benchmark data sets:

- SSB SF1 through `ssb-dbgen`
- TPC-H SF1 through `tpch-dbgen`
- TPC-DS 1GB through `dsdgen`

Benchmark data must come from standard generators. Spark is only used to
convert generated pipe-delimited raw files into Iceberg tables. TPC-H and
TPC-DS schemas are parsed from the generator-provided DDL files so table
definitions stay aligned with the pinned generator revision.

The Spark conversion pins Parquet row group and page sizing. This keeps the
generated benchmark files close to the physical layout expected by the current
reader and avoids pathological tiny-page scans from Spark defaults.

## Manual Bootstrap

Start or reuse the shared Iceberg REST, MinIO, and Spark test fixture:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

Start NovaRocks standalone-server with the generated worktree config:

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell, source the same environment and run the bootstrap script:

```bash
source docker/iceberg-rest/runtime/current/env.sh
sql-tests/bootstrap/bootstrap_benchmark_data.sh \
  --suite ssb \
  --scale 1 \
  --mysql-port "$NOVA_ENV_MYSQL_PORT"
```

Use `--suite tpc-h --scale 1` for TPC-H SF1 and `--suite tpc-ds --scale 1GB`
for TPC-DS 1GB. The TPC-DS `1GB` label is passed to `dsdgen` as scale `1`.

Use `--rebuild` to regenerate even when the readiness check succeeds.

## Runner Auto Bootstrap

The SQL test runner auto bootstraps benchmark data before verifying supported
benchmark suites. For example:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --mode verify
```

Use `--suite tpc-h` or `--suite tpc-ds` for the other benchmark suites. Override
scales with `--benchmark-scale`, for example `--benchmark-scale tpc-ds=10GB`.

Disable benchmark auto bootstrap when you need to inspect runner behavior
without preparing data:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --mode verify \
  --no-auto-bootstrap-benchmark-data
```

## Manual Generator Cache Recovery

Generator cache is recoverable from the pinned standard generator archives in
`benchmark_tools.toml`. For SSB:

```bash
mkdir -p sql-tests/bootstrap/cache
curl -fsSL \
  https://github.com/greenlion/ssb-dbgen/archive/d006a6c49ff1a145a7d4ac7d837427627b213091.zip \
  -o sql-tests/bootstrap/cache/ssb-dbgen-d006a6c49ff1a145a7d4ac7d837427627b213091.zip
```

Expected SHA-256:

```text
fe38fc04bfffec954dd9a5264be295768edc2227fbafc2cb58fa7ca3ad459f3d
```

Verify the cached archive:

```bash
shasum -a 256 sql-tests/bootstrap/cache/ssb-dbgen-d006a6c49ff1a145a7d4ac7d837427627b213091.zip
```

TPC-H and TPC-DS archives are pinned by commit and downloaded from GitHub
`codeload` URLs; their expected hashes are recorded in
`sql-tests/bootstrap/benchmark_tools.toml`.

## Dry Run

The bootstrap script can print resolved paths without generating data,
uploading data, or invoking Spark:

```bash
source docker/iceberg-rest/runtime/current/env.sh
sql-tests/bootstrap/bootstrap_benchmark_data.sh \
  --suite ssb \
  --scale 1 \
  --mysql-port "$NOVA_ENV_MYSQL_PORT" \
  --dry-run
```

Expected output includes:

```text
DRY_RUN suite=ssb scale=1
```

For TPC-DS, dry-run output also shows the normalized generator scale:

```text
DRY_RUN suite=tpc-ds scale=1GB generator_scale=1
```
