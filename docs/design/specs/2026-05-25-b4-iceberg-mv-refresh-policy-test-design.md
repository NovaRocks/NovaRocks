# B4 Iceberg MV Refresh Policy Test Design

## Context

B4 refresh scheduler support now includes refresh policy metadata, user-visible
refresh status, recovery guards, and retry/backoff behavior. The current SQL
coverage was first added under `sql-tests/mv-on-iceberg`, but that suite covers
managed-lake MV targets over Iceberg base tables. The main supported path for
this work is Iceberg target materialized views created with
`PROPERTIES ('storage_engine' = 'iceberg')`.

The test plan must treat Iceberg target MVs as the primary SQL regression gate.
Managed-lake target coverage is not a B4 acceptance focus.

## Goals

- Move the refresh policy SQL coverage to `sql-tests/iceberg-ivm`.
- Exercise the Iceberg target MV path with
  `PROPERTIES ('storage_engine' = 'iceberg')`.
- Verify user-visible refresh policy metadata through `SHOW MATERIALIZED VIEWS`.
- Keep scheduler timing-sensitive behavior in Rust tests where fake state and
  deterministic clocks make failures easier to diagnose.
- Avoid adding B4-7 MV rewrite coverage; rewrite logic is out of scope.

## Non-Goals

- Do not use `mv-on-iceberg` as the primary B4 refresh scheduler coverage.
- Do not build a full restart end-to-end SQL test for this pass.
- Do not depend on wall-clock sleeps for SQL verification of automatic refresh.
- Do not introduce broad refactors outside the test surface needed for B4
  coverage.

## SQL Coverage

Add or migrate a case to:

`sql-tests/iceberg-ivm/sql/iceberg_ivm_refresh_policy_metadata.sql`

The case creates an Iceberg catalog/database/table, then creates an Iceberg
target MV:

```sql
CREATE MATERIALIZED VIEW ...
DISTRIBUTED BY HASH(...) BUCKETS 1
REFRESH ASYNC EVERY INTERVAL 5 MINUTE
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT ...
```

The SQL assertions should cover:

- MV name is visible in `SHOW MATERIALIZED VIEWS`.
- `StorageEngine` is `iceberg`.
- `RefreshMode` is `ASYNC_INTERVAL` after create.
- `RefreshState` and `RetryAfterTime` columns are present.
- Initial automatic state is `PENDING`.
- `ALTER MATERIALIZED VIEW ... PAUSE REFRESH` sets `RefreshPaused=true` and
  state `PAUSED`.
- `ALTER MATERIALIZED VIEW ... SET REFRESH ASYNC ON CHANGE` followed by
  `RESUME REFRESH` sets `RefreshMode=ASYNC_ON_CHANGE`,
  `RefreshPaused=false`, and state `PENDING`.

The existing `mv-on-iceberg` SQL case for refresh policy metadata must be
removed after the Iceberg target case is added, so reviewers do not mistake
managed-lake target coverage for the primary B4 path.

## Rust Coverage

Keep and extend deterministic Rust tests for scheduler semantics that are not
stable enough for SQL golden tests:

- Periodic policy due/not-due enqueue decisions.
- Paused MV does not enqueue; resumed metadata can enqueue again.
- Snapshot-watch policy can enqueue when an Iceberg base snapshot advances.
- Active refresh is not enqueued again.
- `CommitUnknown` active refresh blocks automatic scheduling and maps to
  `BLOCKED_RECOVERY`.
- Transient failures use bounded exponential backoff and write
  `next_refresh_after_ms`.
- Successful refresh clears scheduler error and resets failure attempts.
- Non-retryable user errors are stored with `USER_ERROR:` and do not schedule
  automatic retry.
- `SHOW MATERIALIZED VIEWS` status derivation covers `PAUSED`, `RUNNING`,
  `BLOCKED_RECOVERY`, `FAILED_BACKOFF`, `FAILED_USER_ERROR`, `PENDING`, and
  `MANUAL`.

Where a status test needs target-storage context, use an Iceberg target MV
fixture or directly assert the stored definition has `storage_engine = "iceberg"`.

## Verification Commands

Use the generated Iceberg test environment when available:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Wait for the `NOVAROCKS_READY mysql_port=...` line before running SQL tests.

Run the migrated SQL case:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_refresh_policy_metadata \
  --mode verify
```

Run focused Rust checks:

```bash
cargo test --lib engine::mv_scheduler -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::show_materialized_views_exposes -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
cargo fmt --check
```

## Acceptance

This test pass is complete when:

- The refresh policy SQL regression lives under `iceberg-ivm` and uses an
  Iceberg target MV.
- The old `mv-on-iceberg` refresh-policy metadata case is no longer presented as
  B4 primary coverage.
- SQL verification proves the user-facing Iceberg target MV metadata path.
- Rust verification proves deterministic scheduler guards, retry/backoff, and
  status derivation.
- No B4-7 rewrite behavior is added or asserted.
