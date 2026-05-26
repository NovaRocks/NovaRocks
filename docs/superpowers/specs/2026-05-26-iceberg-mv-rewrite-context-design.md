# Iceberg MV Rewrite Context — Design

Date: 2026-05-26
Task: [`Iceberg MV rewrite context`](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/iceberg-mv-rewrite-context.md) (TODO List task 1)

## 1. Goal

Introduce a single, immutable, refresh-time context object that owns every
piece of metadata the Iceberg MV refresh path currently reconstructs from
`StoredMvDefinition`, `RefreshSnapshotPin`, target Iceberg table metadata,
and session state. The current single-call entry passes ≥12 positional
arguments through three layers of shape-specific helpers; the context
collapses that to `(state, &ctx, ...shape-specific args)`.

This change is **plumbing-only**. It must not alter refresh semantics, must
not regress the `iceberg-ivm` SQL suite, and must not move any commit / scheduler
/ scan-binding behaviour. It builds the boundary that tasks 2 / 3 / 4 (optimizer
foundation, Delta/Version markers, scan delta binding) will consume.

## 2. Non-Goals

- No change to refresh semantics or user-visible behaviour.
- No new shape coverage (no UNION ALL, join, aggregate capability changes).
- No optimizer rule pipeline, no marker operators (that is task 2 + 3).
- No mutable commit / lifecycle state in the context (deferred to
  `Refresh lifecycle hardening`, task 12).
- `state: Arc<StandaloneState>` is *not* stored in the context — it is a
  session-wide handle that callers continue to pass alongside `&ctx`.
- `refresh_id` / `staging_branch` are *not* in the context — they are
  allocated lazily inside the commit path and stay as separate arguments
  until task 12 consolidates them into a `RefreshAttempt` sub-structure.

## 3. Decision: 2 vs 3 vs combined

Tasks 2 (`Incremental MV optimizer foundation`) and 3 (`Logical Delta / Version
marker operators`) are designed together in a single follow-up spec but shipped
as two sequenced PRs:

- PR-α: optimizer foundation + no-op rule pipeline + tree-rewrite helpers +
  EXPLAIN/disable plumbing. Golden tests use no-op rules to lock "no behaviour
  change" against the current `iceberg-ivm` suite.
- PR-β: `LogicalDeltaOp` / `LogicalVersionOp`, convergence check, root-plan
  `Delta(root)` wrap. Golden locks "marker unresolved → fail fast".

Designing them together avoids over- or under-engineering task 2's API in the
absence of a concrete consumer; splitting the PR keeps review surface tractable.
Task 1 (this spec) does not depend on that decision — it stands alone.

## 4. Architecture

Two-layer context. The inner layer is read-only metadata that the future
optimizer rewrite rules (tasks 2 / 3 / 4 / 5) need; the outer layer adds the
execution handles only the refresh path needs.

```text
IcebergMvRewriteContext   (read-only metadata, future optimizer rules consume this)
   ↑
   └─ Arc inside ─┐
                  │
IcebergMvRefreshContext   (execution-time handles, current refresh path consumes this)
```

Rule of separation:

- A future `ImvRewriteRule` (task 2 / 3) MUST receive only
  `&IcebergMvRewriteContext` (or `Arc<IcebergMvRewriteContext>`). It MUST NOT
  see `iceberg::Catalog`, `IcebergCatalogEntry`, or `iceberg::table::Table`.
- The refresh path receives `&IcebergMvRefreshContext` and reaches into
  `.rewrite` whenever it needs rewrite metadata.

### 4.1 `IcebergMvRewriteContext`

Lives in a new module `src/engine/mv/refresh_context.rs`.

```rust
pub(crate) struct IcebergMvRewriteContext {
    // ---- Identity ----
    pub target: IcebergMvTarget,
    pub mv_id: i64,

    // ---- Session ----
    pub current_catalog: Option<String>,
    pub current_database: String,

    // ---- MV definition (post schema-contract rebind) ----
    pub mv_definition: Arc<StoredMvDefinition>,
    pub canonical_select_query: Arc<sqlparser::ast::Query>,

    // ---- Base table inputs ----
    pub base_refs: Arc<[IcebergTableRef]>,
    pub pin: Arc<RefreshSnapshotPin>,
    pub previous_snapshot_ids: BTreeMap<String, i64>,
    pub previous_table_uuids: BTreeMap<String, String>,

    // ---- Target table inputs (extracted from target_table.metadata()) ----
    pub target_snapshot_id: Option<i64>,
    pub target_table_uuid: String,
    pub target_schema: Arc<iceberg::spec::Schema>,

    // ---- Contracts ----
    pub schema_contract: Arc<MvSchemaContract>,
}
```

`MvSchemaContract` already nests:
- `target.visible_columns: Vec<TargetVisibleColumn>` — the user-facing
  required output columns (output_name, target_field_id, type_signature,
  nullable). This is the rewrite output contract; no separate
  `required_output_columns` field needs to be denormalized into the ctx —
  callers read `ctx.rewrite.schema_contract.target.visible_columns`.
- `target.hidden_apply_key: HiddenApplyKeyContract` — the row-identity
  apply key.
- `target.partition: Option<MvPartitionContract>` — the partition contract.

For these reasons the ctx stores `schema_contract` once and exposes the
sub-views through helper accessors rather than duplicating
`partition_contract` or `required_output_columns` as parallel fields. Earlier
drafts of this spec listed those as separate fields; consolidation here
reflects the actual `MvSchemaContract` shape.

Why `Arc` on most fields: future `ImvRewriteRule` implementations (task 2)
will build logical sub-plans that need to share fragments of context with
no clone cost. `Arc` keeps share-by-reference cheap without exposing a
lifetime parameter on every rule signature.

`previous_snapshot_ids` is kept separate from `pin` because the two concepts
have different lifetimes: `pin` is "what we captured at the start of *this*
refresh", `previous_snapshot_ids` is "what the *previous* successful refresh
committed". Merging them obscures that distinction.

### 4.2 `IcebergMvRefreshContext`

```rust
pub(crate) struct IcebergMvRefreshContext {
    pub rewrite: Arc<IcebergMvRewriteContext>,

    pub target_entry: Arc<IcebergCatalogEntry>,
    pub iceberg_catalog: Arc<dyn iceberg::Catalog>,
    pub target_table: iceberg::table::Table,
}
```

### 4.3 Construction site

`IcebergMvRefreshContext::new(...)` MUST be invoked **after** pin capture and
schema-contract rebind, because both can mutate the effective definition
(`effective_definition` in [iceberg_refresh.rs:1354](../../../src/engine/mv/iceberg_refresh.rs:1354))
and the context stores the post-rebind definition.

Three concrete construction points map to the three top-level shape branches:

| Shape | Construction site | Preconditions |
|---|---|---|
| `IncrementalMvShape::Aggregate(_)` / `JoinAggregate(_)` | top of `refresh_iceberg_aggregate_mv`, before dispatching to `refresh_single_aggregate_iceberg_mv` / `refresh_join_aggregate_iceberg_mv` | pin captured, schema rebind applied, partition contract parsed |
| `IncrementalMvShape::JoinProjectionFilter(_)` | top of `refresh_iceberg_join_mv` | same |
| `IncrementalMvShape::ProjectionFilter(_)` | inside `refresh_iceberg_mv`, after [line 1382](../../../src/engine/mv/iceberg_refresh.rs:1382) | same |

`refresh_iceberg_mv`, `refresh_iceberg_aggregate_mv`, and `refresh_iceberg_join_mv`
keep their current dispatch signatures — they are the *constructors* of the ctx.
Signature collapse happens **below** dispatch: `refresh_single_aggregate_iceberg_mv`,
`refresh_join_aggregate_iceberg_mv`, `first_refresh_iceberg_mv`,
`incremental_refresh_iceberg_mv`, and the equivalent join helpers all converge to
`(state: &Arc<StandaloneState>, ctx: &IcebergMvRefreshContext, ...shape-specific args)`.

### 4.4 Constructor API

```rust
impl IcebergMvRefreshContext {
    pub(crate) fn new(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
    ) -> Result<Self, String>;
}
```

The constructor:
1. Derives `previous_snapshot_ids` / `previous_table_uuids` from `mv_definition`.
2. Reads `target_snapshot_id`, `target_table_uuid`, and `target_schema` from
   `target_table.metadata()`. These are then stored inside the rewrite layer
   so future optimizer rules (task 2 / 3) never need to touch
   `iceberg::table::Table` — keeping the rewrite layer free of execution
   handles. Unit tests synthesise the rewrite layer directly with primitive
   inputs (a private `IcebergMvRewriteContext::from_parts(...)` helper) and
   exercise the full self-check without needing a real `iceberg::table::Table`.
3. Resolves `schema_contract` from `mv_definition` (absence is a construction
   error). `schema_contract.target.partition` and `target.visible_columns`
   ride along inside the same `Arc`.
4. Runs the self-check (§4.5).
5. Wraps everything into `Arc<IcebergMvRewriteContext>` inside the outer
   `IcebergMvRefreshContext`.

### 4.5 Self-check

All checks run inside the constructor. Failure returns `Err(String)` prefixed
with `IcebergMvRewriteContext::new`. The checks are the union of guards
currently duplicated across `refresh_iceberg_mv` and the three shape helpers —
plus two new cross-shape consistency checks:

| Check | Error message |
|---|---|
| `mv_definition.schema_contract.is_some()` | `missing schema contract on target {fqn}; rebuild or recreate the MV` |
| `base_refs.len() > 0` | `mv definition has no base table refs` |
| `pin.entries_len() == base_refs.len()` | `refresh pin covers {n} bases but definition has {m}` |
| For each `base_ref`: `pin.uuid(base_ref).is_some()` | `refresh pin missing uuid for base {fqn}` |
| For each base with a recorded previous uuid: `previous_table_uuids[fqn] == pin.uuid(fqn)` | `base table identity changed for {fqn}; incremental refresh unsafe, rebuild the MV` |
| Every `target_schema.fields()` field_id appears in `schema_contract.target.visible_columns` (and vice versa) | `target schema/contract column count mismatch: schema has {n}, contract has {m}` |
| `schema_contract.target.hidden_apply_key.column_name` resolves to an existing target schema column | `target apply-key column {col} not present in target schema` |

The last two checks are new — they consolidate consistency that current code
either does per-shape or relies on later commit-time validation to surface.
Hoisting them into ctx construction is consistent with the task acceptance
criterion "existing helpers no longer re-parse `base_table_refs`".

The previous-uuid identity check is currently in
[iceberg_refresh.rs:1394-1401](../../../src/engine/mv/iceberg_refresh.rs:1394)
for the projection/filter shape only. Hoisting it makes it uniform across
all three shapes.

## 5. Observability

### 5.1 Tracing summary

The rewrite context exposes a `summary()` method returning a `Debug`-only
view. No `Display` impl (to avoid accidental string interpolation across
call sites).

```rust
#[derive(Debug)]
pub(crate) struct CtxSummary<'a> {
    pub target: &'a IcebergMvTarget,
    pub mv_id: i64,
    pub base_count: usize,
    pub base_fqns: Vec<&'a str>,
    pub pinned_snapshots: Vec<(&'a str, i64)>,
    pub previous_snapshots: Vec<(&'a str, Option<i64>)>,
    pub target_snapshot_id: Option<i64>,
    pub schema_contract_version: u16,
    pub partition_contract_present: bool,
    pub visible_output_column_count: usize,
    pub hidden_apply_key_column: &'a str,
}
```

The summary is emitted **once per refresh attempt**, at the construction
point inside each shape helper:

```rust
tracing::info!(summary = ?ctx.rewrite.summary(),
               "iceberg MV refresh context constructed");
```

Each shape helper must not duplicate the log line — the construction site is
the canonical emission point.

### 5.2 Stable field ordering

`summary().pinned_snapshots`, `previous_snapshots`, and `base_fqns` MUST be
ordered by `base_refs` declared order, not by `BTreeMap` key order, so that
logs across runs are stable for the same MV.

## 6. Module layout

- New file: [`src/engine/mv/refresh_context.rs`](../../../src/engine/mv/refresh_context.rs)
  - `pub(crate) struct IcebergMvRewriteContext`
  - `pub(crate) struct IcebergMvRefreshContext`
  - `pub(crate) struct CtxSummary<'a>`
  - `impl IcebergMvRefreshContext { fn new(...) -> Result<Self, String> }`
  - `impl IcebergMvRewriteContext { fn summary(&self) -> CtxSummary<'_> }`
  - `#[cfg(test)] mod tests`
- Update: [`src/engine/mv/mod.rs`](../../../src/engine/mv/mod.rs)
  - Add `pub(crate) mod refresh_context;`
- Update: [`src/engine/mv/iceberg_refresh.rs`](../../../src/engine/mv/iceberg_refresh.rs)
  - Construct `IcebergMvRefreshContext` at the three sites in §4.3.
  - Collapse signatures of `refresh_single_aggregate_iceberg_mv`,
    `refresh_join_aggregate_iceberg_mv`, `first_refresh_iceberg_mv`,
    `incremental_refresh_iceberg_mv`, and the join-shape equivalents to
    `(state, ctx, ...shape-specific args)`.
  - Remove now-duplicate parsing of `base_table_refs`, `previous_snapshot_ids`,
    `previous_table_uuids`, `target_snapshot_id`, `target_table_uuid` inside
    those helpers.
- Update: [`src/engine/mv/iceberg_aggregate_state.rs`](../../../src/engine/mv/iceberg_aggregate_state.rs),
  [`src/engine/mv/iceberg_join_branch.rs`](../../../src/engine/mv/iceberg_join_branch.rs),
  [`src/engine/mv/iceberg_join_coalesce.rs`](../../../src/engine/mv/iceberg_join_coalesce.rs),
  [`src/engine/mv/iceberg_merge_sink.rs`](../../../src/engine/mv/iceberg_merge_sink.rs),
  [`src/engine/mv/iceberg_target_apply.rs`](../../../src/engine/mv/iceberg_target_apply.rs)
  - Migrate sub-helpers that today take individual `mv_definition` / `base_refs`
    / `target` / `iceberg_catalog` / `target_table` arguments to take `&ctx`.
  - The migration is mechanical; signatures that already take ≥4 of these
    arguments should be collapsed, while incidental call sites that take only
    one or two can keep their existing form to limit blast radius.

## 7. Testing

### 7.1 Unit tests (`#[cfg(test)] mod tests` in `refresh_context.rs`)

1. **Happy path** — synthesise a `StoredMvDefinition`, `Pin`, and target
   table metadata; assert every derived field (`previous_snapshot_ids`,
   `target_snapshot_id`, `target_table_uuid`).
2. **Missing schema contract** — assert error contains target fqn.
3. **Base count mismatch** — pin has fewer / more entries than `base_refs`.
4. **Base identity drift** — `previous_table_uuids[fqn]` differs from
   `pin.uuid(fqn)`; assert the unified identity check fires regardless of
   shape (this is the test that proves the hoist works).
5. **First refresh** — empty `previous_snapshot_ids`; constructor succeeds
   and does not treat missing previous as an error.
6. **Target schema / contract mismatch** — target schema has a field_id not
   present in `schema_contract.target.visible_columns`; constructor returns
   the mismatch error.
7. **Hidden apply key missing in target schema** — `hidden_apply_key.column_name`
   not in target schema; constructor returns the apply-key error.
8. **Summary ordering** — synthesise a definition with `base_refs` declared
   in `[b, a, c]` order; `summary().pinned_snapshots` must reflect that
   order, not BTreeMap key order.

### 7.2 SQL regression

The existing [`iceberg-ivm`](../../../tests/sql-test-runner) suite is the
canonical "no behaviour change" gate. No new SQL fixtures are added — task 1
intentionally has no new user-visible behaviour to assert.

### 7.3 Tracing assertion

Deferred. The repo does not currently have a tracing-capture test harness for
MV refresh; introducing one is scope-creep for task 1. Logged as a follow-up
for task 2 (`Incremental MV optimizer foundation`), where observability is
already in scope.

## 8. Risks

- **Module size of `iceberg_refresh.rs`**: the file is already 11,744 lines.
  Task 1 reduces argument counts but does not break the file up. Avoid the
  temptation to refactor the file further in the same PR — the task acceptance
  is plumbing only, and an unrelated split increases the regression surface.
- **Constructor argument count**: the constructor takes 11 parameters. This
  is acceptable as a once-per-shape call site, but if it grows further when
  task 2 starts adding rewrite-side metadata, prefer extracting a builder
  rather than positional growth.
- **`Arc` cloning ergonomics**: callers must remember to `Arc::clone(&ctx.rewrite.pin)`
  when handing a pin sub-component to a helper that already exists with an
  `&RefreshSnapshotPin` signature. The migration in §6 must check each
  existing call site to avoid accidental `&*` then re-`Arc::new` patterns.

## 9. Acceptance

This task is done when:

- `iceberg-ivm` SQL suite passes unchanged.
- `iceberg-rest` and `iceberg-compatibility` suites pass unchanged.
- Each of `refresh_single_aggregate_iceberg_mv`,
  `refresh_join_aggregate_iceberg_mv`, `first_refresh_iceberg_mv`,
  `incremental_refresh_iceberg_mv`, and the join-shape equivalents takes
  `(state, ctx, ...shape-specific args)` and no longer takes individual
  `target` / `mv_definition` / `base_refs` / `target_table` / `iceberg_catalog`
  parameters.
- A successful refresh emits exactly one
  `iceberg MV refresh context constructed` log line per refresh attempt
  with a populated `summary` field.
- A construction failure (e.g., missing schema contract, base identity
  drift) produces a deterministic error message and never partially mutates
  state.
- The seven self-check rules in §4.5 are covered by unit tests
  (happy path + each failure mode).

## 10. Out-of-scope follow-ups

- Task 2 — `Incremental MV optimizer foundation`: consumes
  `Arc<IcebergMvRewriteContext>` as the input to the new optimizer entrypoint.
- Task 3 — `Logical Delta / Version marker operators`: registers rules that
  receive `&IcebergMvRewriteContext`.
- Task 4 — `Iceberg scan delta/version binding`: scan binding rules read
  `pin` / `previous_snapshot_ids` from the rewrite context.
- Task 12 — `Refresh lifecycle hardening`: consolidates `refresh_id` and
  `staging_branch` into a `RefreshAttempt` sub-structure that may eventually
  hang off `IcebergMvRefreshContext`.
