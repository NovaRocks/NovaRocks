MV-on-Iceberg SQL tests for NovaRocks.

This suite isolates managed-lake materialized view cases whose base tables live
in an Iceberg catalog, so they can evolve independently from the local
OLAP-only `materialized-view` suite.

Coverage notes:
- managed-lake MV build, refresh, rewrite, and inactive-state behavior over a
  local Hadoop-style Iceberg catalog
- base-table drop/recreate regression coverage, including table-identity
  invalidation when a recreated Iceberg table reuses the same snapshot id
- incremental refresh over append and delete snapshots, including Iceberg v3
  row-lineage / Puffin deletion-vector delete projection
- aggregate MV retraction over Iceberg equality-delete snapshots
- projection/filter MV row deletion over Iceberg position-delete,
  equality-delete, and v3 row-lineage/Puffin deletion-vector snapshots
- projection/filter MV hidden primary-key projection when PK columns are not
  part of the user-visible MV output
- aggregate MV IVM coverage for COUNT/SUM/AVG/MIN/MAX and refresh policy
  fallbacks such as INSERT OVERWRITE

Iceberg-backed MV targets created with
`PROPERTIES('storage_engine' = 'iceberg')` live in `sql-tests/iceberg-ivm`.

Recommended invocation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --mode verify
```

This suite uses the local Iceberg/S3-compatible placeholders from
`tests/sql-test-runner/conf/standalone_managed_lake.conf`, plus the suite-level
Iceberg defaults injected by the runner. Managed-lake cases require
`${managed_lake_warehouse}` in addition to `${iceberg_catalog_type}`,
`${iceberg_catalog_warehouse}`, `${oss_ak}`, `${oss_sk}`, and `${oss_endpoint}`.
