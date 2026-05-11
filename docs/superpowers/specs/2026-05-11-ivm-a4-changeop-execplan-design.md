# IVM-A4 Change-Op Row Stream for ExecPlan

**Date**: 2026-05-11
**Status**: Accepted for design discussion; implementation plan pending user review
**Scope**: Introduce a direct delta scan/source that emits internal change-op row streams for NovaRocks standalone IVM plans without changing the global `Chunk` ABI in the first phase.

## Background

NovaRocks currently keeps Iceberg MV delta semantics outside the normal execution
pipeline:

- `src/connector/iceberg/changes.rs` plans file-level and row-level deltas as
  `IcebergChangeBatch`.
- `materialize_changes` turns those deltas into two external result branches:
  `MaterializedChanges { inserts, deletes }`.
- `src/connector/starrocks/managed/ivm_change_stream.rs` bridges those branches
  into managed-lake MV refresh as `IvmChangeStream { inserts, deletes }`.

This is enough for some current single-table projection and aggregate refresh
paths, because the caller can handle insert and delete branches separately.
It is not a general ExecPlan capability. Once a delta enters a normal plan,
rows become ordinary rows; operators cannot tell whether a row is an insertion
or a retraction.

## Problem

The A4 issue is not simply "add a hidden column to `Chunk`". The real problem is
that NovaRocks lacks a plan-level contract for change rows:

```text
row = positive contribution / insert / upsert
-row = negative contribution / delete / retract
```

Without that contract, IVM semantics must stay in specialized outer code that
knows about `inserts` and `deletes` branches. That does not scale to plans where
delta rows pass through filter, project, union, aggregate, join, and finally a
sink.

For example, for:

```sql
SELECT customer, SUM(amount)
FROM orders
GROUP BY customer;
```

an Iceberg COW update from `(Alice, 100)` to `(Alice, 80)` should contribute:

```text
Alice, 100, -1
Alice,  80, +1
```

The MV state should move from `100` to `80`. If both rows enter aggregate as
ordinary positive rows, the delta becomes `+180`, which is wrong.

For a projection MV with primary-key semantics, it can still be valid to handle
removed files as deletes and added files as upserts. That is a special case
where the MV row shape is close to the base-table row shape. A4 is about the
more general case where query operators transform delta rows before the sink.

## StarRocks Reference

StarRocks currently implements IVM primarily in the FE optimizer:

- It injects a logical `LogicalDeltaOperator` around the MV query plan.
- The marker carries an internal action column named `__ACTION__`.
- Delta rewrite rules push the marker through project, filter, union, join, and
  aggregate.
- Iceberg append-only scan currently resolves delta by projecting
  `__ACTION__ = 0`.
- For primary-key MV load, `__ACTION__` is projected into the load `__op`
  column.

This is useful as a reference because it keeps delta semantics in the plan layer
instead of changing the physical chunk ABI first.

It should not be copied blindly. The current StarRocks Iceberg scan rule is
append-only oriented: it only accepts append-only TVR traits and emits a constant
UPSERT action. NovaRocks needs explicit support for COW overwrite and delete
branches that produce retract rows. NovaRocks also needs stricter fail-fast
behavior when unsupported operators receive retractable deltas.

## Design Decision

Use a staged design, but make the first implementation converge at the source
boundary rather than at an adapter after materialization:

1. First introduce a direct IVM delta scan/source that emits a plan-level
   internal change-op column.
2. Do not change the global `Chunk` structure or ordinary scan path in phase 1.
3. Treat `__change_op` as a special internal slot owned by the IVM planner and
   sink.
4. Preserve it through row-preserving operators.
5. Require IVM-aware operators and sinks to consume it or explicitly reject it.

The first-phase contract is:

```text
__change_op: Int8, non-null
+1 = insert / upsert / positive contribution
-1 = delete / retract / negative contribution
```

`__change_op` is not a user column. It must not be visible in ordinary SELECT
output, and user SQL must not be allowed to reference it as a normal field.

## Source Boundary

The change-op is generated at the delta source boundary, not by ordinary scan.

Ordinary scans stay unchanged:

```text
Scan(snapshot/files) -> ordinary rows
```

IVM delta sources produce tagged rows:

```text
Iceberg delta scan/source
  added files or inserted rows  -> __change_op = +1
  deleted rows or removed files -> __change_op = -1
```

For phase 1, the implementation target is a real source-level contract:

```text
IcebergChangeBatch
  -> IvmDeltaSource / IcebergDeltaScan
       reads added files or inserted rows with __change_op = +1
       reads deleted rows or removed files with __change_op = -1
  -> downstream ExecPlan operators
```

`IvmChangeStream { inserts, deletes }` may remain as a compatibility wrapper for
old call sites while the refactor lands, but it is not the final semantic
boundary for A4. The source itself must be able to emit tagged chunks, so later
operators consume a single row-stream contract instead of reconstructing
semantics from separate external `QueryResult` branches.

An adapter that appends `__change_op` to already-materialized insert/delete
results is acceptable only in focused unit tests or as a temporary migration
shim hidden behind the new source API. It should not be the production shape
that A4 claims complete.

## Operator Rules

Operators fall into three groups.

### Preserve

These operators are row-preserving with respect to change-op and should keep the
internal slot aligned with each surviving row:

- filter
- project
- union all
- exchange
- simple fetch/result buffering inside an IVM internal plan

Filter must filter the data columns and `__change_op` with the same row mask.
Project must keep the internal slot even if user-visible projection does not
refer to it. Union all must map each child action column into the final internal
action column.

### Consume

These operators or sinks understand the change-op and transform it into MV state
changes:

- projection or primary-key MV sink
- aggregate MV merge path
- future join IVM rules and sinks

Projection MV sinks route `+1` to insert/upsert and `-1` to delete. Aggregate MV
paths treat `+1` rows as positive contribution and `-1` rows as negative
contribution. Invalid values, nulls, or missing `__change_op` are hard errors.

### Reject

Operators whose retract semantics are not implemented must fail fast when used
in a retractable delta plan:

- distinct
- limit
- top-n / sort when result ordering interacts with retract semantics
- analytic/window operators
- non-proven outer join patterns
- aggregate functions that cannot retract correctly, such as MIN/MAX without a
  fallback policy

Rejecting these plans is better than treating delete rows as inserts and
returning stale or incorrect MV data.

## Aggregate Semantics

Phase 1 does not require a brand-new generic aggregate engine for all retractable
aggregates. It should standardize how the existing aggregate MV path receives
its delta input.

The current code already has aggregate-specific machinery:

- `rewrite_select_sql_for_state`
- `materialize_aggregate_result_chunks`
- `negate_aggregate_state_chunks`
- `merge_aggregate_state_batches`

Phase 1 should let those paths consume the delta source's unified change-op
stream instead of depending on an outer `inserts` / `deletes` branch boundary as
the final semantic source. The implementation can still internally split
positive and negative rows when applying existing state-merge code, but that
split must happen after reading a tagged source stream, not before the data
enters the source boundary.

## Join Semantics

Join IVM needs both row change-op and snapshot version binding.

For inner join:

```text
Delta(R join S)
  = Delta(R) join Version(FROM, S)
    union all
    Version(TO, R) join Delta(S)
```

The first phase should not claim full join IVM support. It should define enough
contract so future join rules can produce tagged output rows, and it should
reject unsupported join patterns explicitly.

## Sink Rules

IVM sinks must validate and consume `__change_op`:

- missing internal slot -> error
- null action -> error
- action outside `+1` / `-1` -> error
- `+1` -> write insert/upsert contribution
- `-1` -> write delete/retract contribution

Ordinary query sinks should never see `__change_op`. If an ordinary result path
receives it, the planner has leaked internal state and should fail before
returning user-visible output.

## Non-Goals

Phase 1 does not include:

- Changing `Chunk` to always carry hidden per-row metadata.
- Making ordinary scans aware of IVM change semantics.
- Supporting every SQL operator under retractable deltas.
- Full multi-table join IVM.
- Replacing all current aggregate MV state-merge code.
- Exposing `__change_op` to user SQL.

## Testing

### Unit and Plan Tests

1. Build an `IcebergChangeBatch` fixture and verify the delta source emits
   chunks with `__change_op = +1` for inserted rows and `__change_op = -1` for
   retracted rows.
2. Verify filter preserves change-op alignment with filtered rows.
3. Verify project keeps the internal slot even when user-visible output does not
   include it.
4. Verify union all merges child action columns correctly.
5. Verify unsupported retractable plans fail with explicit errors.
6. Verify ordinary SELECT output and schema do not include `__change_op`.

### End-to-End Tests

Projection COW update:

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT id, customer, amount FROM orders;
```

After a base-table COW update, incremental refresh should delete the old row and
upsert the new row.

Aggregate SUM update:

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT customer, SUM(amount)
FROM orders
GROUP BY customer;
```

After changing `Alice` from `100` to `80`, the MV should change from `100` to
`80`, not to `180` and not to an empty result.

Unsupported operators:

Use a small retractable delta plan with limit/window/distinct or an unsupported
aggregate shape and assert it returns a clear unsupported-retract error.

## Acceptance Criteria

- IVM delta plans have a documented internal `__change_op` slot contract.
- Delta source directly produces tagged insert and delete rows.
- Safe operators preserve the tag.
- IVM sinks consume the tag and validate illegal values.
- Unsupported retractable operator paths fail fast.
- Ordinary query paths remain schema-compatible and do not expose the internal
  slot.
- At least projection COW update and aggregate SUM update are covered by focused
  tests.

## Open Implementation Notes

- Prefer an internal slot id allocator instead of relying on a magic user-visible
  column name alone.
- The planner should keep an explicit "this plan carries change-op" flag, so
  capability checks do not have to infer semantics from column names.
- If phase 1 shows hot-path overhead, introduce a later physical optimization:
  per-batch `AllInsert` / `AllDelete` fast path or hidden `Chunk` metadata.
  That optimization should preserve the same logical contract.
