# Standalone Lake Metadata Design

## Summary

This design replaces the current standalone local managed-table model based on a single parquet file with a StarRocks lake-style managed-table model.

The new standalone mode keeps a small single-node control plane in-process, while reusing the existing StarRocks lake data plane already implemented in NovaRocks:

- tablet metadata in object storage
- txn log and publish-version flow
- native tablet snapshot loading and scan

This design does not preserve compatibility with the current standalone local managed parquet tables.

## Goals

- Make standalone managed tables use StarRocks lake-style tablet metadata and shared-data layout.
- Make object storage the primary and required backend for managed tables in phase 1.
- Support `CREATE TABLE`, `INSERT`, `SELECT`, and restart recovery through the new tablet-based path.
- Reuse the existing StarRocks lake write, publish, and native read stack as much as possible.
- Keep the standalone planner single-node and simpler than StarRocks FE.

## Non-Goals

- Compatibility with the current standalone local parquet managed-table metadata.
- A distributed FE metadata service.
- Full StarRocks FE feature parity in phase 1.
- Primary-key tables, schema change, rollup, MV, function metadata, privilege metadata, or full partitioning support in phase 1.
- A cross-store strong atomic commit protocol spanning SQLite and object storage.

## Phase 1 Scope

Phase 1 supports only one managed-table shape:

- object-store-backed managed tables only
- one logical partition per table
- one base index per table
- `DUP_KEYS` table model only
- hash bucketing to multiple tablets

Unsupported features must fail fast with explicit errors:

- `PRIMARY_KEYS`
- rollup indexes
- schema change
- materialized views
- range/list partitions
- local filesystem as a first-class managed-table backend

## Current Repository Reuse

The design intentionally reuses the existing lake path already present in the repository.

Existing modules to reuse:

- tablet metadata creation: `src/connector/starrocks/lake/schema.rs`
- publish-version flow: `src/connector/starrocks/lake/transactions.rs`
- tablet runtime registry: `src/connector/starrocks/lake/context.rs`
- tablet snapshot loading: `src/formats/starrocks/metadata.rs`
- native tablet scan reader: `src/connector/starrocks/scan/reader.rs`

Existing standalone local managed-table paths that should be retired for managed tables:

- local parquet create/drop metadata path in `src/standalone/engine.rs`
- local parquet insert-rewrite path in `src/standalone/engine.rs`

## Architecture

### Control Plane

Standalone keeps a small local control plane persisted in SQLite.

The control plane is authoritative for:

- names and IDs
- table schema identity
- partition and tablet topology
- visible versions
- transaction allocation and recovery state

The control plane is not authoritative for:

- rowset membership details
- segment metadata
- delete vectors
- tablet-internal version file contents

Those remain in the StarRocks lake data plane.

### Data Plane

The data plane stays StarRocks lake-style and object-store-backed:

- `meta/` for metadata files
- `log/` for txn logs and related files
- `data/` for segments and delvec files

Tablet metadata, rowsets, and version visibility inside one tablet are managed through the existing lake code paths and object-store layout.

## Control Plane Metadata Model

The current lightweight standalone metadata store becomes the authoritative standalone control-plane store.

Recommended logical schema:

### `global_meta`

Stores:

- warehouse URI
- object-store profile reference or serialized config
- `next_db_id`
- `next_table_id`
- `next_partition_id`
- `next_index_id`
- `next_tablet_id`
- `next_txn_id`

### `databases`

Stores:

- `db_id`
- `name`

### `tables`

Stores:

- `table_id`
- `db_id`
- `name`
- `keys_type`
- `bucket_num`
- `current_schema_id`
- `state`

### `table_schemas`

Stores:

- `schema_id`
- `table_id`
- `schema_version`
- canonical `TabletSchemaPb` bytes

This table is the schema source of truth because the downstream data plane already centers around `TabletSchemaPb`.

### `table_columns`

Stores flattened column definitions for analyzer and planner use:

- `schema_id`
- `ordinal`
- `column_name`
- logical type
- nullability

### `partitions`

Phase 1 still stores partitions explicitly even though only one partition is supported.

Stores:

- `partition_id`
- `table_id`
- `name`
- `visible_version`
- `next_version`
- `state`

### `indexes`

Stores:

- `index_id`
- `table_id`
- `partition_id`
- `index_type`
- `state`

Phase 1 only uses one base index, but explicit index metadata avoids later refactoring.

### `tablets`

Stores:

- `tablet_id`
- `partition_id`
- `index_id`
- `bucket_seq`
- `tablet_root_path`

`tablet_root_path` must be an object-store path and should be based on numeric IDs, not table names, to keep rename semantics decoupled from physical layout.

### `txns`

Stores:

- `txn_id`
- `table_id`
- `partition_id`
- `base_version`
- `commit_version`
- `state`
- retry and recovery timestamps

This table is the standalone control-plane recovery anchor. It does not replace lake txn logs.

## Planner and Catalog Model

The current `TableDef` model is too thin and too file-oriented for tablet-based managed tables.

The planner side should be split into two DTO layers:

### `LogicalTableDef`

Used by analyzer and logical planning. Contains only:

- database and table name
- columns
- nullability
- key model and planner-visible table properties

### `PhysicalTableLayout`

Used by lowering and scan/codegen. Contains:

- `table_id`
- `partition_id`
- `visible_version`
- `schema_id`
- tablet list
- each tablet `tablet_id`
- each tablet `tablet_root_path`

Analyzer and logical planning must not directly depend on rowset, segment, or txn-log details.

When lowering a managed-table scan, codegen resolves `LogicalTableDef` to `PhysicalTableLayout` and then emits native lake scan inputs.

## Query Path

For `SELECT`:

1. Resolve table names through the control-plane snapshot.
2. Analyzer and planner operate on logical table metadata only.
3. Lowering resolves the table to a physical tablet layout.
4. The scan layer uses:
   - visible version from the control plane
   - tablet root paths from the control plane
   - existing tablet snapshot and native reader code

This makes standalone managed-table reads tablet-based rather than file-based.

## DDL Flow

### `CREATE TABLE`

Flow:

1. Validate that standalone managed-table warehouse configuration exists and is an object-store URI.
2. Allocate all IDs in a SQLite transaction:
   - table
   - schema
   - partition
   - base index
   - tablets
3. Persist control-plane rows with table state `CREATING`.
4. Commit SQLite.
5. For each tablet, call the existing lake-tablet creation path to write version-1 metadata into object storage.
6. After all tablets succeed, open a short SQLite transaction:
   - set table state to `ACTIVE`
   - set partition `visible_version = 1`
   - set partition `next_version = 2`
7. Commit SQLite.

Failure handling:

- No query may observe tables in `CREATING`.
- Partial object-store initialization is resolved during restart recovery.

## DML Flow

### `INSERT`

Flow:

1. Start a SQLite transaction.
2. Read partition `visible_version` as `base_version`.
3. Allocate `txn_id` and `commit_version`.
4. Persist a `txns` row in state `PREPARED`.
5. Commit SQLite.
6. Execute write planning and bucket rows to tablets.
7. Write segments and txn logs through the existing StarRocks lake write path.
8. Mark txn state as `WRITTEN`.
9. Call existing `publish_version`.
10. If publish succeeds, open a SQLite transaction:
    - set partition `visible_version = commit_version`
    - move txn state to `VISIBLE`
11. Commit SQLite.

Queries use the control-plane `visible_version`, not the highest version that might already exist in object storage.

## Atomicity Model

Phase 1 does not implement a distributed atomic protocol spanning SQLite and object storage.

Instead:

- data-plane commit point: successful `publish_version`
- control-plane commit point: SQLite `visible_version` advance

Consistency is achieved through a recoverable transaction state machine plus idempotent restart recovery.

This keeps the design simple enough for standalone while making failures recoverable.

## Restart Recovery

On startup:

1. Load the entire control-plane snapshot from SQLite.
2. Rebuild in-memory catalog and physical layout snapshots.
3. Rebuild tablet runtime registry entries from control-plane tablet metadata.
4. Scan transactions not in terminal states.
5. For each non-terminal txn:
   - if the target version is already visible in object storage, finalize SQLite state to `VISIBLE`
   - if txn logs exist but publish did not complete, retry `publish_version`
   - if the write is incomplete, mark the txn `ABORTED` or `FAILED` and clean up as allowed by policy

For tables in `CREATING`:

- if all expected tablet version-1 metadata files exist, finalize the table to `ACTIVE`
- otherwise mark as failed and hide it from queries

No query should observe:

- `CREATING`
- `DROPPING`
- `PREPARED`
- `WRITTEN`

## Error Handling

Failure policy for phase 1:

- fail fast on unsupported features
- make write operations retryable
- make recovery idempotent
- keep intermediate states explicit in SQLite

Suggested stable states:

- `CREATING`
- `ACTIVE`
- `PREPARED`
- `WRITTEN`
- `VISIBLE`
- `ABORTED`
- `FAILED`

## Object Storage Requirements

Phase 1 requires real object-storage semantics.

Requirements:

- standalone managed tables cannot be created without warehouse configuration
- warehouse root must be object-store-backed
- MinIO is the default test backend
- local filesystem may be used only as a narrow test helper, not as the production design target

## Testing Strategy

### Unit Tests

Cover:

- control-plane ID allocation
- state transitions
- recovery decisions
- physical tablet layout generation

No compatibility tests with the current standalone local parquet metadata are required.

### Integration Tests

Use MinIO or equivalent S3-compatible object storage.

Cover:

- `CREATE TABLE`
- `INSERT`
- `SELECT`
- restart recovery
- multi-tablet reads
- publish retry behavior

### Fault Injection Tests

Inject process failure at key boundaries:

- after SQLite `PREPARED`
- after txn log write
- after `publish_version`
- before SQLite `VISIBLE`

After restart, the system must converge to a valid final state.

### End-to-End SQL Tests

Use the standalone MySQL-compatible server path and validate:

- `CREATE DATABASE`
- `CREATE TABLE`
- `INSERT VALUES`
- `SELECT`
- restart then `SELECT`

Existing standalone local managed-table tests should be replaced by managed lake-table tests.

## Implementation Plan Shape

The implementation should proceed in stages:

1. Introduce the new control-plane metadata schema and in-memory snapshot model.
2. Replace standalone managed-table DDL with tablet-based object-store-backed creation.
3. Replace standalone managed-table scan lowering with tablet-layout-driven native lake scan.
4. Replace standalone managed-table insert flow with txn allocation plus publish-version integration.
5. Add restart recovery and failure-injection coverage.
6. Remove obsolete standalone local managed-table metadata and write paths.

## Acceptance Criteria

Phase 1 is complete when the following is true on MinIO or another S3-compatible backend:

- standalone can create managed lake tables
- standalone can insert into managed lake tables
- standalone can query managed lake tables
- standalone can restart and recover managed lake table visibility
- a crash at any single point in the write path converges correctly after restart

## Explicit Decisions

- No compatibility with the current standalone local parquet managed-table metadata.
- No local-file managed-table backend as a phase-1 target.
- No parallel metadata system separate from the StarRocks lake data plane.
- No attempt to replicate the full StarRocks FE edit-log and image design in standalone.

