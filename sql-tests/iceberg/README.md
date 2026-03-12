Iceberg SQL tests for NovaRocks.

This suite targets the locally supported Iceberg scope:
- Hadoop catalog
- Parquet read/write path
- Configurable S3-compatible object storage

Recommended invocation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/sr.conf \
  --mode verify
```

Required config variables follow the local `tests/sql-test-runner/conf/sr.conf` style:
- `iceberg_catalog_type`
- `iceberg_catalog_warehouse`
- `oss_ak`
- `oss_sk`
- `oss_endpoint`

The runner also supports `${uuid0}` style placeholders and will auto-expand them
per test run to avoid catalog/database/table name collisions.
