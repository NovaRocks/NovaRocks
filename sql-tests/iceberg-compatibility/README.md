# Iceberg Compatibility SQL Suite

This suite validates cross-engine Iceberg compatibility.

The cases create Iceberg format-v3 tables with Spark through the workspace REST
Catalog and MinIO object store, then read the tables through NovaRocks. Current
coverage includes:

- basic Parquet reads
- primitive/date/timestamp/decimal/NULL reads
- ARRAY/MAP/STRUCT/nested field reads
- partitioned-table filtering and aggregation
- Spark-side schema evolution, including DROP plus re-ADD of the same name
- Spark-side partition evolution across historical specs
- Spark-written row-level DELETE, UPDATE, and MERGE visibility
- Spark-created refs with NovaRocks time-travel reads
- NovaRocks snapshot/history metadata-table reads over Spark commits
- NovaRocks Iceberg MV refresh over Spark-written base-table commits

Run it against the generated local environment (see
[`docker/iceberg-rest/README.md`](../../docker/iceberg-rest/README.md) for
how to bring it up):

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```
