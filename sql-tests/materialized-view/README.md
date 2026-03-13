Materialized view SQL tests for NovaRocks.

This suite migrates the runnable coverage from `dev/test/sql/test_materialized_view`
into the file-level `sql-tests` runner model:
- one `.sql` file is one end-to-end MV case
- file-local `-- query N` steps share setup state
- one `.result` file snapshots all stable result sets for the case

Coverage notes:
- local OLAP rewrite, refresh, status, show/create, privilege, sync-MV, nested-MV,
  partition compensation, and partition-mapping scenarios are covered directly
- external-environment-heavy dev/test cases are consolidated into self-hosted local
  cases where they exercise the same FE-planned MV rewrite and refresh semantics
- Iceberg-backed MV cases are maintained separately in `sql-tests/mv-on-iceberg`

Recommended invocation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite materialized-view \
  --config tests/sql-test-runner/conf/sr.conf \
  --mode verify
```

The runner expands `${uuid0}` style placeholders per run to avoid catalog,
database, and table name collisions.
