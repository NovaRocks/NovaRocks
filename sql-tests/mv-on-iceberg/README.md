MV-on-Iceberg SQL tests for NovaRocks.

This suite isolates materialized view cases whose base tables live in an
Iceberg catalog, so they can evolve independently from the local OLAP-only
`materialized-view` suite.

Coverage notes:
- MV build, refresh, rewrite, and inactive-state behavior over a local
  Hadoop-style Iceberg catalog
- base-table drop/recreate regressions and the current post-recreate behavior
  returned by NovaRocks

Recommended invocation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg \
  --config tests/sql-test-runner/conf/sr.conf \
  --mode verify
```

This suite uses the local Iceberg/S3-compatible placeholders from
`tests/sql-test-runner/conf/sr.conf`, including `${iceberg_catalog_type}`,
`${iceberg_catalog_warehouse}`, `${oss_ak}`, `${oss_sk}`, and `${oss_endpoint}`.
