Iceberg IVM SQL tests for NovaRocks.

This suite is for materialized views whose target storage engine is Iceberg:
`CREATE MATERIALIZED VIEW ... PROPERTIES('storage_engine' = 'iceberg')`.

Scope:
- Iceberg-backed MV target creation in the active Iceberg catalog/database
- manual refresh into a normal Iceberg target table
- append-only incremental refresh over an Iceberg base table
- row-lineage based incremental apply for base DELETE, UPDATE, and equality-delete changes
- metadata-only / no-op refresh behavior
- target catalog visibility and DROP cleanup

Managed-lake materialized views over Iceberg base tables remain in
`sql-tests/mv-on-iceberg`. Keeping the suites separate makes the Iceberg-backed
MV target path usable as a clean regression gate for Iceberg-backed MV work.

Recommended invocation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg-ivm \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --mode verify
```
