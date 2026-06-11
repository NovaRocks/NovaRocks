# Iceberg IMV Aggregate / JoinAggregate Logical Cutover Design

Date: 2026-05-31

## 1. Goal

Cut over Iceberg-backed aggregate and join-aggregate materialized view refresh
to a full IMV logical rewrite outcome.

After this design, the aggregate refresh path no longer owns the incremental
algebra through shape-specific SQL helpers or manual target-state lookup. The
IMV rewrite/execution plan expresses:

- base `Delta` / `Version` scan binding from the refresh snapshot pin;
- action-column propagation and signed aggregate state input;
- single-base `Delta(Aggregate(child))`;
- inner/cross join aggregate delta algebra;
- refresh-only target MV state lookup;
- old-state plus delta-state merge;
- the apply change stream consumed by the Iceberg target commit path.

The refresh lifecycle still owns transaction boundaries: refresh intent,
staging branch, commit, publish, abort, recovery, and metadata finalize.

## 2. Non-goals

- Do not add new aggregate functions.
- Do not support outer, semi, anti, or null-aware join aggregate rewrite.
- Do not support `UNION ALL` aggregate families in this cutover.
- Do not expose target MV hidden state columns to user SQL.
- Do not allow a silent full-refresh or legacy fallback path after cutover.
- Do not move Iceberg transaction/publish/recovery semantics into the optimizer.

## 3. Current State

Projection/filter refresh is already cut over to `execute_query_with_options`
with `mv_refresh_ctx = Some(ctx)`, so it consumes `run_imv_rewrite` output.

Aggregate and join aggregate still keep their core incremental semantics in
`src/engine/mv/iceberg_refresh.rs`:

- `iceberg_aggregate_incremental_delta_select_sql` renders signed aggregate
  state SQL through `ivm_delta_aggregate.rs`.
- `incremental_refresh_iceberg_aggregate_mv` executes the delta source query,
  materializes `delta_chunks`, then calls `apply_iceberg_aggregate_delta_chunks`.
- `apply_iceberg_aggregate_delta_chunks` derives affected partitions, loads
  touched target state, merges old/delta state, and builds target changes.
- join aggregate refresh uses join branch helpers before feeding aggregate
  state apply.
- `try_run_imv_rewrite_pipeline` is still telemetry-only for aggregate/join
  paths.

That split leaves aggregate semantics outside the IMV pipeline even though scan
binding, action propagation, validation, and projection/filter cutover already
live there.

## 4. Architecture

The cutover target is a full logical IMV plan:

```text
canonical MV SELECT
  -> Delta(root)
  -> Delta(Scan) / Version(Scan) binding
  -> action column propagation
  -> aggregate delta-state rewrite
  -> join-delta branch expansion when needed
  -> touched group / partition derivation
  -> refresh-only target-state scan
  -> logical old-state + delta-state merge
  -> apply change stream
  -> Iceberg target commit lifecycle
```

### 4.1 Rewrite Components

`imv::aggregate_rewrite`

- Matches `Delta(Aggregate(child))`.
- Rewrites it into a signed aggregate-state plan.
- Group keys remain positionally compatible with `AggregateMvLayout`.
- Aggregate outputs become internal state columns (`__agg_state_*`) using the
  existing state combinator function family:
  `<kind>_state_signed(arg, __change_op)`.
- The action column must be non-null `Int8` and must not appear in visible
  output.

`imv::join_delta`

- Matches aggregate input that contains an inner/cross join.
- Rewrites join delta algebra as:

```text
Delta(A join B)
  = Delta(A) join Version(B, From)
    UNION ALL
    Version(A, To) join Delta(B)
```

- Each branch owns branch-local action columns and base identity.
- Version sides must use pinned `from` / `to` snapshots; they must never read
  current snapshot.
- Unsupported join types fail during rewrite.

`imv::target_state`

- Introduces a refresh-only target-state relation.
- It is not constructible from user SQL.
- It carries target table UUID, target snapshot id, aggregate layout contract,
  hidden row-id column, physical state columns, and touched group/partition
  constraints.

`imv::aggregate_merge`

- Adds a logical merge boundary that combines delta state and old target state.
- It emits a physical apply change stream:
  - DELETE rows by hidden row id for replaced/removed groups;
  - INSERT rows containing the new physical aggregate MV row;
  - no visible leakage of hidden row id, state columns, or action column.

### 4.2 Execution Boundary

The optimizer/rewrite output may describe target-state lookup and state merge,
but it must not own Iceberg transaction semantics. The refresh driver remains
responsible for:

- capturing `RefreshSnapshotPin`;
- constructing `IcebergMvRefreshContext`;
- beginning and aborting refresh intent;
- ensuring and publishing the staging branch;
- committing the Iceberg transaction;
- finalizing MV metadata with the captured pin's snapshot and UUID maps.

The apply sink consumes the logical change stream. It must not re-compute
aggregate state merge semantics internally.

## 5. Data Flow

### 5.1 Single-base Aggregate

Input:

```sql
SELECT group_keys, aggregates
FROM base
WHERE predicate
GROUP BY group_keys
```

Rewrite:

```text
Delta(Aggregate(Filter/Project/Scan(base)))
  -> Aggregate(
       Delta(Filter/Project/Scan(base)),
       group_keys,
       <kind>_state_signed(arg, __change_op)
     )
  -> TargetStateScan(target, touched groups/partitions)
  -> AggregateStateMerge(old_state, delta_state)
  -> ApplyChangeStream
```

The delta scan is bound to `(previous_snapshot_id, pinned_snapshot_id]`.
No-op deltas finalize metadata only and do not create a target Iceberg snapshot.

### 5.2 Join Aggregate

Input:

```sql
SELECT group_keys, aggregates
FROM left JOIN right ON condition
GROUP BY group_keys
```

Rewrite:

```text
Delta(Aggregate(Join(left, right)))
  -> Aggregate(
       UnionAll(
         Join(Delta(left), Version(right, From)),
         Join(Version(left, To), Delta(right))
       ),
       group_keys,
       <kind>_state_signed(arg, branch_action)
     )
  -> TargetStateScan(target, touched groups/partitions)
  -> AggregateStateMerge(old_state, delta_state)
  -> ApplyChangeStream
```

Both sides must have previous and pinned snapshots. Partial previous-snapshot
state remains invalid and requires MV recreation.

## 6. Target State Scan Contract

`ScanSource::IcebergMvTargetState` is a refresh-only source. It must contain:

- target catalog / namespace / table identity;
- target table UUID;
- target snapshot id used as the old-state read point;
- aggregate layout version and physical column list;
- hidden row-id column name and type;
- required state columns;
- touched row-id set or an equivalent bounded group filter;
- target partition allow-list when the target is partitioned.

For partitioned targets, the scan must not run without an affected partition
allow-list. For unpartitioned targets, it may scan all target files but must
still filter by touched row id before merge.

The scan is invalid outside MV refresh. Codegen/lowering must reject it unless
an active `IcebergMvRefreshContext` and target state runtime handle are present.

## 7. State Merge Contract

The logical merge node must preserve the existing `AggregateMvLayout` contract:

- physical column order is stable;
- hidden row id type is `Utf8` and non-null;
- visible columns and state columns match target schema by name, type, and
  nullability;
- state union uses the existing per-kind state combinator semantics;
- visible output is materialized from merged state using existing layout rules;
- removed groups produce DELETE-only changes;
- replaced groups produce DELETE + INSERT changes;
- new groups produce INSERT changes.

If the merge produces no changes, refresh finalizes metadata only.

## 8. Error Handling

All cutover failures are fail-fast:

- unresolved `Delta` / `Version` markers;
- missing refresh pin entry;
- UUID drift;
- stale or expired baseline snapshot;
- action column missing, nullable, wrong type, or visible;
- unsupported aggregate function;
- unsupported join type;
- branch identity ambiguity;
- target-state scan missing touched group/partition constraints;
- target state schema/layout mismatch;
- state merge output schema/order/nullability mismatch;
- codegen/lowering seeing a refresh-only source without refresh context.

There is no legacy fallback flag in the cutover target.

## 9. Observability

Rewrite trace and EXPLAIN output must make the cutover visible without
exposing hidden state contents:

- aggregate delta-state rewrite rule matched/skipped/failed;
- join delta branch expansion;
- bound delta/version scan windows;
- target-state scan target and constraint summary;
- aggregate-state merge node summary;
- metadata-only refresh reason.

Trace output may include row/partition counts, snapshot ids, table UUIDs, and
rule names. It must not dump hidden state bytes.

## 10. Tests

Unit tests:

- `Delta(Aggregate)` rewrites to signed state aggregate.
- Join aggregate rewrites to the two-branch inner/cross join delta algebra.
- Action column is used as the signed-state argument and does not leak.
- Target-state scan validation rejects missing constraints.
- Merge validation rejects schema/order/nullability mismatch.
- Unsupported join types fail during rewrite.

Planner/codegen tests:

- `IcebergMvTargetState` is refresh-only.
- Target-state scan carries snapshot/layout/group/partition constraints.
- `IcebergVersionTable` is executable only in refresh context and reads the
  pinned snapshot.
- Refresh-only sources are rejected by normal query execution.

SQL tests:

- single-base aggregate append delta;
- single-base aggregate position/equality delete delta;
- single-base aggregate deleted data file delta;
- no-op delta metadata-only refresh;
- join aggregate left-only delta;
- join aggregate right-only delta;
- join aggregate both-side delta;
- join key update multiplicity;
- hidden row id, action column, and state columns do not appear in visible
  output;
- refresh metadata records pinned snapshot and UUID maps.

Regression:

- the full `iceberg-ivm` suite remains green;
- new cutover cases prove aggregate and join aggregate no longer use the old
  signed SQL / branch helper execution path;
- EXPLAIN or rewrite trace contains aggregate rewrite, join delta, target-state
  scan, and state merge evidence.

## 11. Rollout

This design intentionally treats aggregate and join aggregate as one cutover
unit. Implementation may still land in reviewable checkpoints, but the accepted
end state has no legacy fallback and no partial default path.

Before enabling the default path, every supported aggregate/join aggregate
shape must either:

- produce a fully executable logical target-state merge plan; or
- fail fast with an explicit unsupported-shape error.
