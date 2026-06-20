# Optimizer Independent Module Design

## Goal

Make `src/sql/optimizer` an optimizer-owned module that does not depend on
planner/analyzer/codegen data structures. The optimizer should consume and
produce optimizer-native IR only:

- `OptExpr`
- `Operator`
- `ScalarArena`, `ScalarNode`, `ScalarId`
- `Memo`
- optimizer-owned property, stats, rule, and physical plan structures

Planner, analyzer, engine, and codegen may still call optimizer APIs, but
optimizer must not import planner/analyzer/codegen structures such as
`TypedExpr`, `LogicalPlanNode`, `ProjectItem`, `SortItem`, `AggregateCall`, or
`WindowExpr`.

## Current Baseline

PR #346 completed the first cleanup stage: optimizer rewrite production logic is
`ScalarId`-native. The remaining dependency problem is concentrated in explicit
bridge files:

- `src/sql/optimizer/scalar/mod.rs`
  - owns `intern_typed` and `materialize`
- `src/sql/optimizer/scalar_bridge.rs`
  - bridges `TypedExpr`, `ProjectItem`, `SortItem`, aggregate/window wrappers,
    and optimizer scalar wrappers
- `src/sql/optimizer/convert.rs`
  - bridges `LogicalPlanNode` and `OptExpr`
- `src/sql/optimizer/property.rs`
  - derives ordering properties from planner/analyzer sort metadata

This is clean enough for rule correctness, but not clean enough for module
independence.

## Recommended Boundary

Use a middle-ground boundary:

1. Optimizer must not depend on `sql::analysis`, `sql::planner`,
   `sql::codegen`, or `engine` data structures.
2. Shared primitive SQL semantics should live in a neutral module, not in
   `analysis` or `planner`.
3. Optimizer may depend on this neutral module for truly shared concepts:
   `BinOp`, `UnOp`, `JoinKind`, literal values, column ids, scalar data-type
   metadata, and similar common vocabulary.
4. Planner owns all conversion between planner/analyzer trees and optimizer IR.
5. Codegen owns all conversion from optimizer scalar handles to codegen-facing
   validation or lowering shapes.

This avoids duplicating every enum inside optimizer while still removing
planner/analyzer structural coupling.

## Target Structure

```text
src/sql/common/ or src/sql/ir/
  common scalar and relational vocabulary
  BinOp, UnOp, JoinKind, LiteralValue, ColumnId-facing helpers

src/sql/optimizer/
  mod.rs
  operator.rs
  opt_expr.rs
  scalar/
    ScalarArena, ScalarNode, ScalarId, SortKey
    no TypedExpr conversion APIs
  scalar_expr.rs
  memo.rs
  search.rs
  property.rs
  physical_plan.rs
  rewrite/**
  cascades_rules/**
  estimate/**
  stats/**

src/sql/planner/optimizer_bridge/
  scalar_from_typed.rs
  scalar_to_typed.rs
  logical_to_opt.rs
  opt_to_logical.rs
  property_from_planner.rs

src/sql/codegen/
  codegen-facing materialization or validation adapters
```

The exact common module name can be chosen during implementation. `sql::common`
is clearer if it will hold broad SQL vocabulary; `sql::ir` is better if it will
grow into a shared typed IR layer.

## Data Flow

### Planner To Optimizer

1. Analyzer/planner produces `LogicalPlanNode` containing `TypedExpr` and
   planner-side node payloads.
2. `planner::optimizer_bridge::logical_to_opt` converts the tree into `OptExpr`.
3. The bridge interns expressions into `ScalarArena`.
4. Optimizer receives only `OptExpr` plus `ScalarArena`.
5. Optimizer rewrite, memo copy-in, exploration, costing, implementation, and
   extraction stay native.

### Optimizer To Planner

1. Optimizer returns a `PhysicalPlanNode` or `OptExpr` with scalar handles.
2. Planner-side bridge materializes only where planner-side logical or
   distributed planning still needs planner structures.
3. Bridge output feeds distributed plan construction or existing planner/codegen
   boundaries.

### Optimizer To Codegen

1. Codegen receives optimizer physical plan plus `ScalarArena`.
2. Codegen adapters materialize expressions only at codegen validation/lowering
   boundaries.
3. Optimizer does not own codegen materialization functions.

## Component Responsibilities

### Optimizer Core

Optimizer core owns:

- scalar interning storage and scalar-native nodes
- structural scalar utilities
- logical and physical optimizer operators
- memo, rules, search, property, stats, and costing
- physical plan extraction

Optimizer core does not own:

- `TypedExpr` construction or inspection
- `LogicalPlanNode` construction or inspection
- planner projection/sort/aggregate/window wrappers
- codegen validation wrappers

### Planner Bridge

Planner bridge owns:

- `TypedExpr -> ScalarId`
- `ScalarId -> TypedExpr`
- `LogicalPlanNode -> OptExpr`
- `OptExpr -> LogicalPlanNode`
- planner property conversion, such as `SortItem -> OrderingSpec`
- bridge-only display metadata preservation

The bridge may use both planner/analyzer structures and optimizer-native
structures. It is the allowed dependency edge between both worlds.

### Common SQL Vocabulary

Common vocabulary owns shared enum/value definitions that are not structurally
planner-specific:

- scalar operators such as `BinOp` and `UnOp`
- join kind
- literal value representation
- column id and column metadata primitives, if not already neutral enough
- possibly data-type wrappers that should not be analysis-owned

This is intentionally smaller than a full duplicated optimizer AST.

## Error Handling

Bridge errors should stay explicit and early:

- `ColumnId::UNSET` remains rejected before optimizer scalar interning.
- Invalid logical plan stage is rejected before conversion into `OptExpr`.
- Unsupported expression variants fail at the bridge with clear messages.
- Optimizer internals should not need planner fallback behavior.

After the split, an optimizer error should indicate optimizer-native invalid
state. A bridge error should indicate invalid planner/analyzer input or an
unsupported conversion.

## Audit And Enforcement

Add or extend audits so this boundary is mechanically enforced:

1. `src/sql/optimizer` must not import:
   - `crate::sql::analysis`
   - `crate::sql::planner`
   - `crate::sql::codegen`
   - `crate::engine`
2. `src/sql/optimizer` must not reference:
   - `TypedExpr`
   - `LogicalPlanNode`
   - `ProjectItem`
   - `SortItem`
   - planner aggregate/window wrappers
   - `materialize` bridge APIs
3. Test modules may use planner/analyzer fixtures only behind explicit
   test-only helper modules, but production audit should ignore test-only code.
4. The allowlist should shrink to zero for planner/analyzer structural imports.

The current `tools/dev/audit_optimizer_typedexpr.py` can evolve into a broader
dependency audit, or a new audit can be added beside it while migration is in
progress.

## Migration Strategy

### Phase 1: Create Neutral Common Vocabulary

Move or re-export shared primitives from analysis/planner-owned modules into a
neutral module. Do this before moving bridges so optimizer does not gain new
dependency churn.

Initial candidates:

- `BinOp`
- `UnOp`
- `JoinKind`
- `LiteralValue`
- column id primitives if they are considered SQL-wide rather than optimizer
  specific

Keep compatibility re-exports during the migration to avoid one large flip.

### Phase 2: Move Scalar Bridges Out Of Optimizer

Move `intern_typed`, `materialize`, and `scalar_bridge.rs` responsibilities into
planner/codegen bridge modules.

Optimizer `ScalarArena` still owns scalar storage, deduplication, display
metadata needed by native consumers, and `ScalarNode`. It does not mention
`TypedExpr`.

### Phase 3: Move Plan Conversion Out Of Optimizer

Move `convert.rs` responsibilities into `planner::optimizer_bridge`.

Optimizer should expose native construction APIs only where needed:

- creating `OptExpr`
- creating operators
- copying `OptExpr` into memo
- running optimize/search

Planner bridge handles `LogicalPlanNode` conversion before calling optimizer.

### Phase 4: Move Property Bridges Out Of Optimizer

Change optimizer property APIs to accept optimizer-native sort keys or column-id
ordering descriptors.

Planner bridge converts `SortItem` and window partition/order metadata into
that native descriptor before entering optimizer.

### Phase 5: Move Codegen Materialization Out Of Optimizer

Codegen validation and lowering should call codegen/planner-side adapter
functions rather than `optimizer::scalar::materialize`.

This keeps optimizer scalar storage reusable without making optimizer own
codegen-facing shapes.

### Phase 6: Tighten Audits To Zero

Once all bridge code has moved:

- remove optimizer allowlist entries for planner/analyzer structures
- add import-level dependency audit
- run full Rust and SQL validation

## Testing Plan

Use focused tests during each phase and full validation at the end:

- `cargo fmt --check`
- `git diff --check`
- `cargo check --lib`
- focused unit tests for moved bridge modules
- `cargo test --lib sql::optimizer`
- `cargo test --lib planner`
- `cargo test --lib codegen`
- `cargo test --lib`
- `python3 tools/dev/audit_optimizer_typedexpr.py --strict`
- new dependency audit for optimizer import boundaries
- SQL optimizer suite verify
- representative TPC-DS smoke cases

For bridge moves, tests should compare before/after conversion results so the
migration does not change plan shape or expression metadata accidentally.

## Non-Goals

- Do not change optimizer rule behavior.
- Do not rewrite all scalar semantics from scratch.
- Do not duplicate every SQL enum inside optimizer unless the neutral common
  vocabulary proves insufficient.
- Do not move IMV rewrite back into optimizer.
- Do not remove planner/codegen materialization needs; only move their ownership
  out of optimizer.

## Open Decision

The preferred common module name is still open:

- `src/sql/common` emphasizes shared vocabulary.
- `src/sql/ir` emphasizes a future shared IR layer.

The implementation plan should choose one name before coding. The design
recommendation is `src/sql/common` unless the next roadmap explicitly wants a
larger shared IR abstraction.
