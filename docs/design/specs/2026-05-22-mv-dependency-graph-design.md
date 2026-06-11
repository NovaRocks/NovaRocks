# MV Dependency Graph and MV-on-MV Refresh Design

**Status:** Draft for user review
**Date:** 2026-05-22
**Related roadmap:** `/Users/harbor/Documents/Obsidian/NovaRocks TODO/IVM-B2-mv-dependency.md`

---

## 1. Goal

Implement the B path from IVM-B2: introduce explicit materialized-view
dependency graph metadata so NovaRocks can safely support MV-on-MV definitions
and refresh a requested MV after refreshing its upstream MV dependencies.

The selected cascade semantics are **upstream pull only**:

```text
REFRESH MATERIALIZED VIEW target_mv
  -> refresh every upstream MV dependency in topological order
  -> refresh target_mv
```

Refreshing `target_mv` does not automatically refresh downstream MVs that depend
on `target_mv`.

## 2. Scope

Included:

- Persist MV dependency edges during `CREATE MATERIALIZED VIEW`.
- Allow a MV SELECT to reference an existing MV target when the reference can be
  resolved unambiguously.
- Reject dependency cycles at DDL time.
- Refresh upstream MV dependencies before the requested target MV.
- Reject `DROP MATERIALIZED VIEW` and `DROP TABLE` when downstream MV
  dependencies still exist.
- Expose dependency information in `SHOW MATERIALIZED VIEWS`.
- Add repository, lifecycle, and SQL regression coverage.

Not included:

- Downstream push refresh.
- Cross-MV atomic transaction semantics.
- Refresh scheduler, async refresh queue, or background propagation.
- Support for forward references to not-yet-created MVs.
- Expanding IVM query-shape support beyond what the existing backends already
  accept.
- Physical SQLite DDL migration work from IVM-B1.

## 3. Existing Context

Current MV behavior is split across three layers:

- `src/engine/mv_flow.rs` owns statement-level routing and refresh lifecycle
  orchestration.
- `src/connector/backend.rs::MvBackend` owns backend-specific create, drop,
  list, and single-MV refresh lifecycle operations.
- `src/meta/repository/mv.rs::MvMetaRepository` stores MV definitions,
  Iceberg target lookup records, and refresh intent records in `meta_records`.

The TODO note describes a logical `mv_dependency` metadata table. In the current
repository architecture, this should be implemented as a new `MvMetaRepository`
record kind, not as a new physical SQLite table. SQLite remains only one
provider implementation behind `meta_records`.

## 4. Decision Summary

| Area | Decision |
|---|---|
| Refresh direction | Upstream pull only |
| Graph owner | `MvMetaRepository` |
| Refresh orchestrator | `mv_flow` |
| Backend responsibility | Refresh exactly one MV target per lifecycle call |
| Dependency storage | New repository record kind with downstream and upstream indexes |
| Cycle detection | DDL-time DFS/topological check before committing the new MV definition |
| Failure model | Sequential best effort; successful upstream refreshes are not rolled back if a later refresh fails |
| DROP behavior | Refuse to drop an object while downstream MV dependencies exist |
| SHOW behavior | Add dependency visibility without changing existing `BaseTables` semantics |

## 5. Dependency Metadata Model

Add a dependency record under `MvMetaRepository`.

```rust
StoredMvDependency {
    downstream_mv_id: i64,
    upstream: MvObjectRef,
    created_at_ms: i64,
}

MvObjectRef {
    catalog: Option<String>,
    database_or_namespace: String,
    name: String,
    object_type: MvObjectType,
    storage_engine: MvStorageEngineRef,
}

MvObjectType {
    Table,
    MaterializedView,
}

MvStorageEngineRef {
    ManagedLake,
    Iceberg,
    ExternalTable,
}
```

The exact Rust names may be adjusted to match local module conventions, but the
persisted semantics must remain explicit: an upstream dependency is not just a
string; it records whether the object is a base table or an MV and which storage
engine namespace owns it.

Use two lookup families:

```text
mv/dependency/by-downstream/<downstream_mv_id>/<upstream_key>
mv/dependency/by-upstream/<upstream_key>/<downstream_mv_id>
```

`by-downstream` supports refresh planning. `by-upstream` supports DROP guards
and leaves a clean future entrypoint for downstream propagation.

All object identifiers stored in lookup keys must use the same normalization
rules as existing MV target lookup keys. Display strings may preserve a stable
human-readable FQN, but lookup equality must not depend on user spelling.

## 6. CREATE MATERIALIZED VIEW Flow

The create flow should stay backend-owned for target creation, but dependency
resolution must be shared enough that both managed-lake and Iceberg-backed MVs
record graph edges consistently.

Recommended flow:

1. Analyze the MV SELECT with the existing analyzer path.
2. Resolve table references into dependency objects.
3. Classify each dependency as either a normal base table or an existing MV.
4. Reject unsupported managed-lake table dependencies as today.
5. Build the prospective dependency edges for the new MV.
6. Run cycle detection with the prospective edges included.
7. Commit the MV definition, target lookup, and dependency edges in one metadata
   transaction.

MV references are accepted only when the referenced object already exists and
can be resolved to an MV definition. Forward references are rejected because
they make cycle detection and refresh planning ambiguous.

Cycle errors should include the detected path:

```text
cannot create materialized view ice.analytics.mv2: dependency cycle detected: ice.analytics.mv2 -> ice.analytics.mv1 -> ice.analytics.mv2
```

## 7. Refresh Flow

`mv_flow::refresh_mv` remains the statement entrypoint.

Recommended flow:

1. Resolve the requested target MV and its storage engine.
2. Load the upstream MV closure from `MvMetaRepository`.
3. Topologically sort the closure so the deepest upstream MVs run first.
4. Append the requested target MV as the final refresh step.
5. Execute each step with the existing `run_refresh_lifecycle` path.
6. Stop on the first failure and report which upstream MV failed.

The orchestrator should execute a precomputed plan list rather than recursively
calling public refresh entrypoints that re-expand the graph. If recursion is
used internally, it must maintain a visited set to avoid duplicate refreshes.

Failure is sequential best effort:

- If upstream `A` refreshes successfully and upstream `B` fails, `A` remains
  refreshed.
- The requested target MV is not refreshed after an upstream failure.
- No cross-MV rollback is attempted.

Suggested error shape:

```text
cannot refresh materialized view ice.analytics.mv_final: upstream materialized view ice.analytics.mv_mid failed: <original error>
```

## 8. DROP Semantics

`DROP MATERIALIZED VIEW mv` must reject when `mv` has downstream MV
dependencies. The error should list at least the first few downstream MVs so the
user knows what must be dropped first.

`DROP TABLE base` must reject when any MV depends on that base table. This check
should use the new `by-upstream` index rather than scanning SQL text or only
consulting `base_table_refs`.

`IF EXISTS` only suppresses not-found errors. It must not suppress dependency
protection for an object that exists.

When a MV is dropped successfully, delete:

- the MV definition;
- target lookup records, if any;
- dependency edges where the MV is downstream;
- dependency edges where the MV is upstream, after confirming no downstream
  dependents remain.

## 9. SHOW MATERIALIZED VIEWS

Keep the existing `BaseTables` column semantics: it continues to show base table
references.

Add dependency visibility as a separate column:

```text
Dependencies
```

Use stable comma-separated display strings. Example:

```text
ice.analytics.orders, mv:ice.analytics.mv_orders
```

The `mv:` prefix distinguishes MV dependencies from base table dependencies
without changing old `BaseTables` consumers.

Information schema can follow later. B2 only needs the `SHOW MATERIALIZED
VIEWS` surface.

## 10. Testing Plan

Repository unit tests:

- create and query dependency edges by downstream MV;
- query reverse dependencies by upstream object;
- delete dependency edges when a MV definition is dropped;
- detect simple and multi-hop cycles;
- preserve non-cycle DAGs.

`mv_flow` lifecycle tests:

- `A -> B -> C` refreshes in `A, B, C` order when `C` is requested;
- a failure in `B` stops `C`;
- refreshing `B` does not refresh downstream `C`;
- duplicated upstream paths refresh each MV once.

SQL golden tests:

- MV-on-MV create succeeds for an existing upstream MV;
- creating a dependency cycle fails with an explicit message;
- refreshing a target MV refreshes upstream MVs first;
- dropping an upstream MV with downstream dependents fails;
- dropping a base table with MV dependents fails.

Suite placement:

- Iceberg-backed MV-on-MV SQL cases belong in `sql-tests/iceberg-ivm`.
- Managed-lake-only details should stay as focused unit tests or be added to
  `sql-tests/mv-on-iceberg` only when they exercise managed-lake target
  behavior.

## 11. Implementation Boundaries

Expected touch points:

- `src/meta/repository/mv.rs`
- `src/engine/mv_flow.rs`
- `src/connector/backend.rs`
- `src/connector/starrocks/managed/mv_ddl.rs`
- `src/engine/mv/iceberg_refresh.rs`
- `src/engine/mv/iceberg_backend.rs`
- `src/connector/starrocks/managed/backend.rs`
- `src/engine/statement.rs`
- `sql-tests/iceberg-ivm/`

The implementation should avoid teaching each backend how to cascade refreshes.
Backends should keep their single-target lifecycle contract. Graph traversal and
multi-MV orchestration belong above them in `mv_flow`.

## 12. Acceptance Criteria

- MV-on-MV no longer runs silently without dependency metadata.
- CREATE rejects dependency cycles before committing the new MV.
- REFRESH of a target MV refreshes all upstream MV dependencies first.
- REFRESH does not refresh downstream MVs.
- DROP protects base tables and upstream MVs that still have downstream MV
  dependents.
- `SHOW MATERIALIZED VIEWS` exposes dependencies in a stable column.
- Unit and SQL tests cover success paths, cycle rejection, refresh order, and
  drop protection.
