# Phase 1: CTE Multicast Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable CTE multi-reference queries to share computation via multicast instead of inlining, unblocking TPC-DS CTE-heavy queries that currently timeout.

**Architecture:** The analyzer gains CTE ref-counting to detect multi-referenced CTEs. The planner produces a `QueryPlan` with separate CTE subtrees and `CTEConsume` leaf nodes. A new fragment planner splits at CTE boundaries: each shared CTE becomes a fragment with `MultiCastDataStreamSink`, and consumers receive via `ExchangeNode`. An `ExecutionCoordinator` constructs per-fragment `TExecBatchPlanFragmentsParams` and submits them via existing `submit_exec_batch_plan_fragments`. The standalone server starts a gRPC exchange service so fragments communicate via the full gRPC path.

**Tech Stack:** Rust, Thrift (generated types), gRPC (tonic), Arrow RecordBatch

**Design Spec:** `docs/superpowers/specs/2026-03-31-cascades-multi-fragment-multicast-design.md` (Sections 4 & 6)

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `src/sql/cte.rs` | `CteId` newtype, `CTERegistry`, `CTEEntry` — CTE metadata shared between analyzer and planner |
| `src/sql/fragment.rs` | `FragmentPlan`, `PlanFragment`, `FragmentSinkType` — splits `QueryPlan` into fragment DAG at CTE boundaries |
| `src/standalone/coordinator.rs` | `ExecutionCoordinator` — assigns fragment/instance IDs, builds Thrift params, submits fragments, fetches results |

### Modified Files

| File | Change |
|------|--------|
| `src/sql/mod.rs` | Add `pub(crate) mod cte;` and `pub(crate) mod fragment;` |
| `src/sql/ir/mod.rs:92` | Add `CTEConsume` variant to `Relation` enum |
| `src/sql/plan/mod.rs:17` | Add `CTEConsume` variant to `LogicalPlan`; add `QueryPlan` struct |
| `src/sql/analyzer/mod.rs:52-121` | Two-pass CTE analysis with ref counting |
| `src/sql/analyzer/resolve_from.rs:120-163` | Emit `Relation::CTEConsume` for shared CTEs instead of inline |
| `src/sql/planner/mod.rs:15,655` | Plan `CTEConsume` relation; produce `QueryPlan` from `ResolvedQuery` |
| `src/sql/optimizer/mod.rs:32-35` | Handle `QueryPlan` (optimize each CTE plan + main plan); handle `CTEConsume` as leaf |
| `src/sql/optimizer/predicate_pushdown.rs` | Treat `CTEConsume` as leaf (no pushdown into it) |
| `src/sql/optimizer/column_pruning.rs` | Treat `CTEConsume` as leaf (no pruning into it) |
| `src/sql/physical/mod.rs:26-51` | New `MultiFragmentBuildResult` struct; new `emit_multi_fragment()` entry |
| `src/sql/physical/emitter/mod.rs:54-159` | Add `emit_cte_consume()` (exchange node emission); per-fragment emission support |
| `src/sql/physical/nodes.rs` | Add `build_exchange_node()` function |
| `src/standalone/engine.rs:3366-3509` | Replace `build_query_plan() → execute_plan()` with multi-fragment pipeline |
| `src/standalone/server.rs:280` | Start gRPC exchange service alongside MySQL server |

---

## Task 1: CTE Types and IR Foundation

**Files:**
- Create: `src/sql/cte.rs`
- Modify: `src/sql/mod.rs`
- Modify: `src/sql/ir/mod.rs:92-104`
- Modify: `src/sql/plan/mod.rs:17-58`

- [ ] **Step 1: Create `src/sql/cte.rs` with CTE registry types**

```rust
//! CTE (Common Table Expression) metadata types.

use crate::sql::ir::{OutputColumn, ResolvedQuery};

/// Unique identifier for a CTE within a query.
pub(crate) type CteId = u32;

/// Registry of shared CTEs produced by the analyzer.
/// Only contains CTEs with ref_count >= 2 (worth sharing).
#[derive(Clone, Debug, Default)]
pub(crate) struct CTERegistry {
    pub entries: Vec<CTEEntry>,
    next_id: CteId,
}

impl CTERegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a shared CTE and return its ID.
    pub fn register(
        &mut self,
        name: String,
        resolved_query: ResolvedQuery,
        output_columns: Vec<OutputColumn>,
    ) -> CteId {
        let id = self.next_id;
        self.next_id += 1;
        self.entries.push(CTEEntry {
            id,
            name,
            resolved_query,
            output_columns,
        });
        id
    }

    pub fn get(&self, id: CteId) -> Option<&CTEEntry> {
        self.entries.iter().find(|e| e.id == id)
    }
}

/// A single shared CTE definition.
#[derive(Clone, Debug)]
pub(crate) struct CTEEntry {
    pub id: CteId,
    pub name: String,
    pub resolved_query: ResolvedQuery,
    pub output_columns: Vec<OutputColumn>,
}
```

- [ ] **Step 2: Register the module in `src/sql/mod.rs`**

Add `pub(crate) mod cte;` to the module declarations in `src/sql/mod.rs`. Place it near the other module declarations (alongside `pub(crate) mod ir;`, `pub(crate) mod plan;`, etc.).

- [ ] **Step 3: Add `CTEConsume` to `Relation` enum**

In `src/sql/ir/mod.rs`, add a new variant after line 103 (`GenerateSeries`):

```rust
pub(crate) enum Relation {
    /// A base table scan.
    Scan(ScanRelation),
    /// A subquery in FROM: `(SELECT ...) AS alias`.
    Subquery {
        query: Box<ResolvedQuery>,
        alias: String,
    },
    /// A join between two relations.
    Join(Box<JoinRelation>),
    /// `TABLE(generate_series(start, end[, step]))`.
    GenerateSeries(GenerateSeriesRelation),
    /// Reference to a shared CTE (multi-referenced, not inlined).
    CTEConsume {
        cte_id: crate::sql::cte::CteId,
        alias: String,
        output_columns: Vec<OutputColumn>,
    },
}
```

- [ ] **Step 4: Add `CTEConsume` to `LogicalPlan` and add `QueryPlan` struct**

In `src/sql/plan/mod.rs`, add after the `Repeat` variant (line 37):

```rust
pub(crate) enum LogicalPlan {
    // ... existing variants ...
    Repeat(RepeatPlanNode),
    /// Reference to a shared CTE. Leaf node — execution receives data via exchange.
    CTEConsume(CTEConsumeNode),
}
```

Add the node struct after `SubqueryAliasNode` (around line 58):

```rust
/// Leaf node referencing a shared CTE. In physical execution, this becomes
/// an ExchangeNode receiving from the CTE's multicast fragment.
#[derive(Clone, Debug)]
pub(crate) struct CTEConsumeNode {
    pub cte_id: crate::sql::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}
```

Add the `QueryPlan` struct that holds CTE plans alongside the main plan:

```rust
/// A query plan with optional shared CTE subtrees.
/// CTE subtrees are planned and optimized independently.
#[derive(Clone, Debug)]
pub(crate) struct QueryPlan {
    /// Shared CTE plans (each will become a multicast fragment).
    pub cte_plans: Vec<CTEProducePlan>,
    /// The main query plan (may contain CTEConsume leaf nodes).
    pub main_plan: LogicalPlan,
    /// Output column metadata for the query result.
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}

/// A shared CTE subtree that will be executed once and multicast to consumers.
#[derive(Clone, Debug)]
pub(crate) struct CTEProducePlan {
    pub cte_id: crate::sql::cte::CteId,
    pub plan: LogicalPlan,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}
```

- [ ] **Step 5: Verify compilation**

Run: `cargo build 2>&1 | head -30`

Fix any compilation errors from the new types. The `emit_node()` match in `src/sql/physical/emitter/mod.rs` will need a placeholder arm:

```rust
LogicalPlan::CTEConsume(_node) => {
    Err("CTEConsume should be handled by multi-fragment emission".to_string())
}
```

Similarly add passthrough arms in any other match-on-LogicalPlan sites (optimizer passes).

- [ ] **Step 6: Commit**

```bash
git add src/sql/cte.rs src/sql/mod.rs src/sql/ir/mod.rs src/sql/plan/mod.rs src/sql/physical/emitter/mod.rs
git commit -m "feat(sql): add CTE types, CTEConsume IR/plan nodes, and QueryPlan struct"
```

---

## Task 2: Analyzer CTE Ref Counting

**Files:**
- Modify: `src/sql/analyzer/mod.rs:52-121`
- Modify: `src/sql/analyzer/resolve_from.rs:120-163`

- [ ] **Step 1: Add CTE ref counting to `AnalyzerContext`**

In `src/sql/analyzer/mod.rs`, add to `AnalyzerContext` (line 52-63):

```rust
pub(super) struct AnalyzerContext<'a> {
    pub(super) catalog: &'a dyn CatalogProvider,
    pub(super) current_database: &'a str,
    pub(super) ctes: std::collections::HashMap<String, (sqlast::Query, Vec<String>)>,
    pub(super) next_subquery_id: std::cell::Cell<usize>,
    pub(super) collected_subqueries: std::cell::RefCell<Vec<SubqueryInfo>>,
    /// Shared CTE registry: populated during two-pass analysis for CTEs with
    /// ref_count >= 2. Maps lowercase CTE name → CteId for lookup during FROM resolution.
    pub(super) shared_cte_ids: std::collections::HashMap<String, crate::sql::cte::CteId>,
    /// Accumulated shared CTE registry, built during analysis.
    pub(super) cte_registry: std::cell::RefCell<crate::sql::cte::CTERegistry>,
}
```

- [ ] **Step 2: Implement two-pass CTE analysis in `analyze_query()`**

Replace the WITH clause processing in `analyze_query()` (lines 77-99) with a two-pass approach:

Pass 1: Count CTE references in the query body text to determine ref_count. A pragmatic heuristic: count occurrences of each CTE name as a table reference in the AST. If a CTE name appears 2+ times as a `TableFactor::Table`, it's a candidate for sharing.

Pass 2: For shared CTEs (ref_count >= 2), analyze the CTE query once, register in `cte_registry`, and store the `CteId` in `shared_cte_ids`. For single-ref CTEs, keep the current inline behavior.

```rust
fn analyze_query(&self, query: &sqlast::Query) -> Result<ResolvedQuery, String> {
    let ctx = if let Some(with) = &query.with {
        // --- Pass 1: count CTE references in the query body ---
        let ref_counts = count_cte_references(&with.cte_tables, &query.body);

        let mut ctes = self.ctes.clone();
        let mut shared_cte_ids = self.shared_cte_ids.clone();

        for cte in &with.cte_tables {
            let name = cte.alias.name.value.to_lowercase();
            let col_aliases: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.value.to_lowercase())
                .collect();

            let count = ref_counts.get(&name).copied().unwrap_or(0);
            if count >= 2 {
                // Shared CTE: analyze once and register
                let child_ctx = AnalyzerContext {
                    catalog: self.catalog,
                    current_database: self.current_database,
                    ctes: ctes.clone(),
                    next_subquery_id: std::cell::Cell::new(0),
                    collected_subqueries: std::cell::RefCell::new(Vec::new()),
                    shared_cte_ids: shared_cte_ids.clone(),
                    cte_registry: std::cell::RefCell::new(
                        self.cte_registry.borrow().clone(),
                    ),
                };
                let mut resolved = child_ctx.analyze_query(&cte.query)?;

                // Apply column aliases
                if !col_aliases.is_empty() {
                    for (i, alias_name) in col_aliases.iter().enumerate() {
                        if let Some(col) = resolved.output_columns.get_mut(i) {
                            col.name = alias_name.clone();
                        }
                    }
                }

                let output_columns = resolved.output_columns.clone();
                let cte_id = self.cte_registry.borrow_mut().register(
                    name.clone(),
                    resolved,
                    output_columns,
                );
                shared_cte_ids.insert(name, cte_id);
            } else {
                // Single-ref CTE: keep for inline (existing behavior)
                ctes.insert(name, (*cte.query.clone(), col_aliases));
            }
        }

        AnalyzerContext {
            catalog: self.catalog,
            current_database: self.current_database,
            ctes,
            next_subquery_id: std::cell::Cell::new(0),
            collected_subqueries: std::cell::RefCell::new(Vec::new()),
            shared_cte_ids,
            cte_registry: std::cell::RefCell::new(
                self.cte_registry.borrow().clone(),
            ),
        }
    } else {
        // No WITH clause — clone context as before
        // ... existing code ...
    };

    // ... rest of analyze_query unchanged ...
}
```

Add the `count_cte_references` helper at the end of `mod.rs`:

```rust
/// Count how many times each CTE name appears as a table reference in the query body.
fn count_cte_references(
    cte_tables: &[sqlast::Cte],
    body: &sqlast::SetExpr,
) -> std::collections::HashMap<String, usize> {
    let cte_names: std::collections::HashSet<String> = cte_tables
        .iter()
        .map(|c| c.alias.name.value.to_lowercase())
        .collect();
    let mut counts = std::collections::HashMap::new();
    count_table_refs_in_set_expr(body, &cte_names, &mut counts);
    counts
}
```

Implement `count_table_refs_in_set_expr` as a recursive AST walker that visits all `TableFactor::Table` nodes and increments the count for names matching CTE names. Walk through `SetExpr::Select` (FROM + JOINs + subqueries in WHERE), `SetExpr::SetOperation`, etc.

- [ ] **Step 3: Update `analyze()` entry point to propagate CTE registry**

In `analyze()` (lines 33-46), initialize the new fields and return the registry:

```rust
pub(crate) fn analyze(
    query: &sqlast::Query,
    catalog: &impl CatalogProvider,
    current_database: &str,
) -> Result<(ResolvedQuery, crate::sql::cte::CTERegistry), String> {
    let ctx = AnalyzerContext {
        catalog,
        current_database,
        ctes: std::collections::HashMap::new(),
        next_subquery_id: std::cell::Cell::new(0),
        collected_subqueries: std::cell::RefCell::new(Vec::new()),
        shared_cte_ids: std::collections::HashMap::new(),
        cte_registry: std::cell::RefCell::new(crate::sql::cte::CTERegistry::new()),
    };
    let resolved = ctx.analyze_query(query)?;
    let registry = ctx.cte_registry.into_inner();
    Ok((resolved, registry))
}
```

- [ ] **Step 4: Update `resolve_from.rs` to emit `CTEConsume` for shared CTEs**

In `analyze_table_factor()` (around line 120-163), before the existing CTE inline logic, check if the CTE is shared:

```rust
// Check shared CTEs first (ref_count >= 2, registered in registry)
if parts.len() == 1 {
    if let Some(&cte_id) = self.shared_cte_ids.get(&tbl_lower) {
        let entry = self.cte_registry.borrow();
        let cte_entry = entry.get(cte_id)
            .ok_or_else(|| format!("internal: shared CTE id {} not found", cte_id))?;
        let output_columns = cte_entry.output_columns.clone();
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tbl.clone());

        // Build scope from CTE output columns
        let mut scope = AnalyzerScope::new();
        for col in &output_columns {
            scope.add_column(
                Some(alias_name.clone()),
                col.name.clone(),
                col.data_type.clone(),
                col.nullable,
            );
        }

        let relation = Relation::CTEConsume {
            cte_id,
            alias: alias_name,
            output_columns,
        };
        return Ok((relation, scope));
    }
}

// Existing inline CTE logic follows...
```

- [ ] **Step 5: Update all callers of `analyze()` to handle the new return type**

The main caller is `build_query_plan()` in `src/standalone/engine.rs:3366`. Update it:

```rust
let (resolved, cte_registry) = crate::sql::analyzer::analyze(query, catalog, current_database)?;
```

Also update any other callers (search for `analyzer::analyze(` in the codebase).

- [ ] **Step 6: Verify compilation**

Run: `cargo build 2>&1 | head -50`

Fix any issues. The most likely problems are scope/lifetime issues with `RefCell<CTERegistry>` borrows.

- [ ] **Step 7: Commit**

```bash
git add src/sql/analyzer/mod.rs src/sql/analyzer/resolve_from.rs src/standalone/engine.rs
git commit -m "feat(analyzer): two-pass CTE analysis with ref counting for shared CTEs"
```

---

## Task 3: Planner CTE Handling

**Files:**
- Modify: `src/sql/planner/mod.rs:15,655-706`

- [ ] **Step 1: Add `plan_query()` that produces `QueryPlan`**

Add a new public entry point in `src/sql/planner/mod.rs` that wraps the existing `plan()` and adds CTE produce plans:

```rust
use crate::sql::cte::CTERegistry;
use crate::sql::plan::{QueryPlan, CTEProducePlan, CTEConsumeNode};

/// Plan a resolved query with shared CTE subtrees into a QueryPlan.
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
) -> Result<QueryPlan, String> {
    let output_columns = resolved.output_columns.clone();
    let main_plan = plan(resolved)?;

    // Plan each shared CTE subtree independently
    let cte_plans = cte_registry
        .entries
        .into_iter()
        .map(|entry| {
            let cte_plan = plan(entry.resolved_query)?;
            Ok(CTEProducePlan {
                cte_id: entry.id,
                plan: cte_plan,
                output_columns: entry.output_columns,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(QueryPlan {
        cte_plans,
        main_plan,
        output_columns,
    })
}
```

- [ ] **Step 2: Handle `Relation::CTEConsume` in `plan_relation()`**

In `plan_relation()` (around line 655), add a match arm:

```rust
Relation::CTEConsume { cte_id, alias, output_columns } => {
    Ok(LogicalPlan::CTEConsume(CTEConsumeNode {
        cte_id,
        alias,
        output_columns,
    }))
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | head -30`

- [ ] **Step 4: Commit**

```bash
git add src/sql/planner/mod.rs
git commit -m "feat(planner): plan_query() produces QueryPlan with CTE subtrees"
```

---

## Task 4: Optimizer CTE Handling

**Files:**
- Modify: `src/sql/optimizer/mod.rs:28-35`
- Modify: `src/sql/optimizer/predicate_pushdown.rs`
- Modify: `src/sql/optimizer/column_pruning.rs`

- [ ] **Step 1: Add `optimize_query_plan()` entry point**

In `src/sql/optimizer/mod.rs`, add:

```rust
use crate::sql::plan::{QueryPlan, CTEProducePlan};

/// Optimize a QueryPlan: optimize each CTE subtree and the main plan independently.
pub(crate) fn optimize_query_plan(
    mut query_plan: QueryPlan,
    table_stats: &std::collections::HashMap<String, crate::sql::statistics::TableStatistics>,
) -> QueryPlan {
    // Optimize each CTE plan
    for cte in &mut query_plan.cte_plans {
        cte.plan = optimize(cte.plan.clone(), table_stats);
    }
    // Optimize main plan
    query_plan.main_plan = optimize(query_plan.main_plan, table_stats);
    query_plan
}
```

- [ ] **Step 2: Handle `CTEConsume` as a leaf node in predicate pushdown**

In `src/sql/optimizer/predicate_pushdown.rs`, find the recursive match on `LogicalPlan` variants. Add `CTEConsume` as a terminal case (predicates cannot be pushed through an exchange boundary):

```rust
LogicalPlan::CTEConsume(_) => {
    // Cannot push predicates into a CTE consumer — it's an opaque exchange boundary.
    // Wrap with Filter if there are pending predicates.
    wrap_with_filter(LogicalPlan::CTEConsume(node), predicates)
}
```

Where `wrap_with_filter` creates a `LogicalPlan::Filter` on top if predicates is non-empty, or returns the plan unchanged. Follow the existing pattern in the file for how other leaf nodes (like `Scan` or `Values`) handle pending predicates.

- [ ] **Step 3: Handle `CTEConsume` as a leaf node in column pruning**

In `src/sql/optimizer/column_pruning.rs`, add `CTEConsume` as a leaf case:

```rust
LogicalPlan::CTEConsume(node) => {
    // All output columns are needed — we can't prune columns across exchange.
    LogicalPlan::CTEConsume(node)
}
```

- [ ] **Step 4: Handle `CTEConsume` in join_reorder.rs if it has LogicalPlan matching**

Check `src/sql/optimizer/join_reorder.rs`. If it has a match on `LogicalPlan`, add `CTEConsume` as a leaf that returns itself unchanged (same pattern as `Scan`).

- [ ] **Step 5: Verify compilation and run existing tests**

Run: `cargo build && cargo test 2>&1 | tail -20`

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/
git commit -m "feat(optimizer): handle CTEConsume as opaque leaf in optimization passes"
```

---

## Task 5: Fragment Planner

**Files:**
- Create: `src/sql/fragment.rs`
- Modify: `src/sql/mod.rs`

- [ ] **Step 1: Create `src/sql/fragment.rs` with fragment DAG types**

```rust
//! Fragment planner — splits a QueryPlan into a fragment DAG at CTE boundaries.
//!
//! Phase 1: only CTE boundaries create fragment splits.
//! The non-CTE portion of the plan remains a single fragment.

use crate::sql::cte::CteId;
use crate::sql::plan::{LogicalPlan, QueryPlan, CTEProducePlan};
use crate::sql::ir::OutputColumn;

pub(crate) type FragmentId = u32;

/// A plan split into multiple fragments connected by exchange.
#[derive(Clone, Debug)]
pub(crate) struct FragmentPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
}

/// A single execution fragment.
#[derive(Clone, Debug)]
pub(crate) struct PlanFragment {
    pub id: FragmentId,
    pub plan: LogicalPlan,
    pub sink: FragmentSink,
    pub output_columns: Vec<OutputColumn>,
}

/// How a fragment's output is delivered.
#[derive(Clone, Debug)]
pub(crate) enum FragmentSink {
    /// Root fragment: results go to the client.
    Result,
    /// CTE multicast: one DataStreamSink per consumer fragment.
    MultiCast {
        cte_id: CteId,
        /// (consumer_fragment_id, exchange_node_id) for each consumer.
        /// Populated after the root fragment is emitted and exchange node IDs are known.
        consumers: Vec<(FragmentId, i32)>,
    },
}

/// Split a QueryPlan into a fragment DAG.
///
/// Returns a FragmentPlan where:
/// - Each shared CTE has its own fragment with MultiCast sink
/// - The main plan is the root fragment with Result sink
/// - CTEConsume nodes in the main plan reference CTE fragment IDs
///
/// The consumers field in MultiCast sinks is initially empty —
/// it gets populated during physical emission when exchange node IDs are assigned.
pub(crate) fn plan_fragments(query_plan: QueryPlan) -> FragmentPlan {
    let mut fragments = Vec::new();
    let mut next_id: FragmentId = 0;

    // Create a fragment for each shared CTE
    let mut cte_fragment_ids = std::collections::HashMap::new();
    for cte in query_plan.cte_plans {
        let frag_id = next_id;
        next_id += 1;
        cte_fragment_ids.insert(cte.cte_id, frag_id);
        fragments.push(PlanFragment {
            id: frag_id,
            plan: cte.plan,
            sink: FragmentSink::MultiCast {
                cte_id: cte.cte_id,
                consumers: Vec::new(), // populated during emission
            },
            output_columns: cte.output_columns,
        });
    }

    // Root fragment: the main plan
    let root_id = next_id;
    fragments.push(PlanFragment {
        id: root_id,
        plan: query_plan.main_plan,
        sink: FragmentSink::Result,
        output_columns: query_plan.output_columns,
    });

    FragmentPlan {
        fragments,
        root_fragment_id: root_id,
    }
}
```

- [ ] **Step 2: Register the module in `src/sql/mod.rs`**

Add `pub(crate) mod fragment;` to the module declarations.

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | head -20`

- [ ] **Step 4: Commit**

```bash
git add src/sql/fragment.rs src/sql/mod.rs
git commit -m "feat(sql): fragment planner splits QueryPlan at CTE boundaries"
```

---

## Task 6: Multi-Fragment Physical Emission

**Files:**
- Modify: `src/sql/physical/mod.rs:26-51`
- Modify: `src/sql/physical/emitter/mod.rs:54-159`
- Modify: `src/sql/physical/nodes.rs`

- [ ] **Step 1: Add `MultiFragmentBuildResult` and `FragmentBuildResult` to `src/sql/physical/mod.rs`**

```rust
use crate::sql::fragment::{FragmentId, FragmentPlan};

/// Result of emitting a multi-fragment plan.
pub(crate) struct MultiFragmentBuildResult {
    /// Per-fragment build results, in dependency order (CTE fragments first, root last).
    pub fragment_results: Vec<FragmentBuildResult>,
    /// Which fragment is the root (result sink).
    pub root_fragment_id: FragmentId,
}

/// Physical emission result for a single fragment.
pub(crate) struct FragmentBuildResult {
    pub fragment_id: FragmentId,
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    pub output_sink: data_sinks::TDataSink,
    pub output_columns: Vec<OutputColumn>,
    /// CTE ID if this is a multicast fragment.
    pub cte_id: Option<crate::sql::cte::CteId>,
    /// Exchange node IDs in this fragment that consume from CTE fragments.
    /// Maps cte_id → exchange_node_id (for back-filling multicast destinations).
    pub cte_exchange_nodes: Vec<(crate::sql::cte::CteId, i32)>,
}
```

- [ ] **Step 2: Add `emit_multi_fragment()` entry point**

In `src/sql/physical/mod.rs`:

```rust
/// Emit a multi-fragment plan into per-fragment Thrift structures.
pub(crate) fn emit_multi_fragment(
    fragment_plan: FragmentPlan,
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<MultiFragmentBuildResult, String> {
    emitter::emit_multi_fragment(fragment_plan, catalog, current_database)
}
```

- [ ] **Step 3: Add `build_exchange_node()` to `src/sql/physical/nodes.rs`**

```rust
/// Build a TPlanNode for an EXCHANGE_NODE that receives data from a remote fragment.
pub(super) fn build_exchange_node(
    node_id: i32,
    input_row_tuples: Vec<i32>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = input_row_tuples.clone();
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.exchange_node = Some(crate::plan_nodes::TExchangeNode::new(
        input_row_tuples,
        None::<crate::plan_nodes::TSortInfo>,     // no merging sort
        None::<i64>,                               // no offset
        Some(crate::partitions::TPartitionType::HASH_PARTITIONED),
        None::<bool>,                              // no parallel merge
        None::<crate::plan_nodes::TLateMaterializeMode>,
    ));
    node
}
```

- [ ] **Step 4: Add `emit_cte_consume()` to `ThriftEmitter`**

In `src/sql/physical/emitter/mod.rs`, replace the error placeholder in `emit_node()` for `CTEConsume` with:

```rust
LogicalPlan::CTEConsume(node) => self.emit_cte_consume(node),
```

Add the method:

```rust
fn emit_cte_consume(
    &mut self,
    node: crate::sql::plan::CTEConsumeNode,
) -> Result<EmitResult, String> {
    // Allocate a tuple for the exchange output
    let exchange_tuple_id = self.alloc_tuple();
    let exchange_node_id = self.alloc_node();

    let mut scope = ExprScope::new();
    let qualifier = Some(node.alias.as_str());

    // Register output columns — must match the CTE produce's output
    for col in &node.output_columns {
        let slot_id = self.alloc_slot();
        self.desc_builder.add_slot(
            slot_id,
            exchange_tuple_id,
            &col.name,
            &col.data_type,
            col.nullable,
            -1, // col_unique_id not needed for exchange
        );
        let binding = ColumnBinding {
            tuple_id: exchange_tuple_id,
            slot_id,
            data_type: col.data_type.clone(),
            nullable: col.nullable,
        };
        scope.add_column(
            qualifier.map(|s| s.to_string()),
            col.name.clone(),
            binding,
        );
    }
    self.desc_builder.add_tuple(exchange_tuple_id);

    let exchange_plan_node =
        nodes::build_exchange_node(exchange_node_id, vec![exchange_tuple_id]);

    // Record this exchange node's CTE association for coordinator to wire up
    self.cte_exchange_nodes.push((node.cte_id, exchange_node_id));

    Ok(EmitResult {
        plan_nodes: vec![exchange_plan_node],
        scope,
        tuple_ids: vec![exchange_tuple_id],
    })
}
```

Add `cte_exchange_nodes: Vec<(crate::sql::cte::CteId, i32)>` field to `ThriftEmitter`.

- [ ] **Step 5: Implement `emit_multi_fragment()` in the emitter**

In `src/sql/physical/emitter/mod.rs`, add:

```rust
pub(crate) fn emit_multi_fragment(
    fragment_plan: crate::sql::fragment::FragmentPlan,
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<super::MultiFragmentBuildResult, String> {
    // Use a single ThriftEmitter so tuple/slot/node IDs are globally unique
    // and the descriptor table is shared across all fragments.
    let mut emitter = ThriftEmitter::new(catalog, current_database);
    let mut fragment_results = Vec::new();

    // Emit CTE fragments first, then root
    for fragment in &fragment_plan.fragments {
        // Reset per-fragment state but keep global ID allocation
        let prev_scan_tables = std::mem::take(&mut emitter.scan_tables);
        let prev_cte_exchange_nodes = std::mem::take(&mut emitter.cte_exchange_nodes);

        let result = emitter.emit_node(fragment.plan.clone())?;

        let scan_tables = std::mem::replace(&mut emitter.scan_tables, prev_scan_tables);
        let cte_exchange_nodes = std::mem::replace(
            &mut emitter.cte_exchange_nodes,
            prev_cte_exchange_nodes,
        );

        // Build per-fragment exec_params from this fragment's scan tables
        let exec_params = nodes::build_exec_params_multi(
            &scan_tables
                .iter()
                .map(|(id, rt)| (*id, rt.clone()))
                .collect::<Vec<_>>(),
        )?;

        // Build output sink based on fragment type
        let output_sink = match &fragment.sink {
            crate::sql::fragment::FragmentSink::Result => {
                super::build_result_sink()
            }
            crate::sql::fragment::FragmentSink::MultiCast { .. } => {
                // Placeholder — actual sinks/destinations are filled by coordinator
                // after all fragments are emitted and exchange node IDs are known
                super::build_noop_sink()
            }
        };

        fragment_results.push(super::FragmentBuildResult {
            fragment_id: fragment.id,
            plan: plan_nodes::TPlan::new(result.plan_nodes),
            desc_tbl: thrift_descriptors::TDescriptorTable::default(), // filled below
            exec_params,
            output_sink,
            output_columns: fragment
                .output_columns
                .iter()
                .map(|c| super::OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: match &fragment.sink {
                crate::sql::fragment::FragmentSink::MultiCast { cte_id, .. } => Some(*cte_id),
                _ => None,
            },
            cte_exchange_nodes,
        });
    }

    // Build the shared descriptor table and assign to all fragments
    let desc_tbl = std::mem::replace(&mut emitter.desc_builder, DescBuilder::new()).build();
    for fr in &mut fragment_results {
        fr.desc_tbl = desc_tbl.clone();
    }

    Ok(super::MultiFragmentBuildResult {
        fragment_results,
        root_fragment_id: fragment_plan.root_fragment_id,
    })
}
```

- [ ] **Step 6: Add helper sink builders to `src/sql/physical/mod.rs`**

```rust
fn build_result_sink() -> data_sinks::TDataSink {
    let mut sink = data_sinks::TDataSink::default();
    sink.type_ = data_sinks::TDataSinkType::RESULT_SINK;
    sink.result_sink = Some(data_sinks::TResultSink::default());
    sink
}

fn build_noop_sink() -> data_sinks::TDataSink {
    let mut sink = data_sinks::TDataSink::default();
    sink.type_ = data_sinks::TDataSinkType::NOOP_SINK;
    sink
}
```

- [ ] **Step 7: Verify compilation**

Run: `cargo build 2>&1 | head -40`

Fix any issues — the most likely problems are missing imports and the `DescBuilder` type references. Adapt the helper functions to match the exact Thrift generated API (field names, `new()` vs struct literal, etc.).

- [ ] **Step 8: Commit**

```bash
git add src/sql/physical/
git commit -m "feat(physical): multi-fragment emission with exchange nodes for CTE consumers"
```

---

## Task 7: Execution Coordinator

**Files:**
- Create: `src/standalone/coordinator.rs`
- Modify: `src/standalone/mod.rs` (if exists, or `engine.rs` module structure)

- [ ] **Step 1: Create `src/standalone/coordinator.rs` with core types**

```rust
//! Execution coordinator for multi-fragment query execution.
//!
//! Assigns unique fragment instance IDs, builds TExecBatchPlanFragmentsParams
//! per fragment, wires up multicast sinks with exchange destinations,
//! and submits fragments via the existing internal_service path.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::sql::cte::CteId;
use crate::sql::fragment::FragmentId;
use crate::sql::physical::{FragmentBuildResult, MultiFragmentBuildResult};

/// Coordinates multi-fragment execution for a single query.
pub(crate) struct ExecutionCoordinator {
    build_result: MultiFragmentBuildResult,
    /// gRPC exchange address (host:port) for this NovaRocks instance.
    exchange_host: String,
    exchange_port: u16,
}

/// Unique ID for a fragment instance.
#[derive(Clone, Debug)]
struct FragmentInstanceId {
    hi: i64,
    lo: i64,
}

impl ExecutionCoordinator {
    pub fn new(
        build_result: MultiFragmentBuildResult,
        exchange_host: String,
        exchange_port: u16,
    ) -> Self {
        Self {
            build_result,
            exchange_host,
            exchange_port,
        }
    }
}
```

- [ ] **Step 2: Implement multicast sink wiring**

Add a method that fills in the `MultiCastDataStreamSink` for CTE produce fragments, based on the exchange node IDs discovered during emission of consumer fragments:

```rust
impl ExecutionCoordinator {
    /// Wire up multicast sinks: for each CTE produce fragment, build a
    /// TMultiCastDataStreamSink with one DataStreamSink per consumer exchange node.
    fn wire_multicast_sinks(&mut self) {
        // Collect exchange node IDs per CTE from all consumer fragments
        let mut cte_consumers: std::collections::HashMap<CteId, Vec<(FragmentId, i32)>> =
            std::collections::HashMap::new();
        for fr in &self.build_result.fragment_results {
            for &(cte_id, exchange_node_id) in &fr.cte_exchange_nodes {
                cte_consumers
                    .entry(cte_id)
                    .or_default()
                    .push((fr.fragment_id, exchange_node_id));
            }
        }

        // For each CTE produce fragment, build the multicast sink
        for fr in &mut self.build_result.fragment_results {
            if let Some(cte_id) = fr.cte_id {
                if let Some(consumers) = cte_consumers.get(&cte_id) {
                    let instance_ids = &self.instance_ids;
                    let sinks: Vec<crate::data_sinks::TDataStreamSink> = consumers
                        .iter()
                        .map(|(_frag_id, exchange_node_id)| {
                            let mut sink = crate::data_sinks::TDataStreamSink::default();
                            sink.dest_node_id = *exchange_node_id;
                            sink.output_partition = crate::partitions::TDataPartition::new(
                                crate::partitions::TPartitionType::UNPARTITIONED,
                                None::<Vec<crate::exprs::TExpr>>,
                                None::<Vec<crate::partitions::TRangePartition>>,
                                None::<Vec<crate::partitions::TBucketProperty>>,
                            );
                            sink
                        })
                        .collect();

                    let destinations: Vec<Vec<crate::data_sinks::TPlanFragmentDestination>> =
                        consumers
                            .iter()
                            .map(|(consumer_frag_id, _)| {
                                let inst_id = self.get_instance_id(*consumer_frag_id);
                                vec![crate::data_sinks::TPlanFragmentDestination::new(
                                    crate::types::TUniqueId::new(inst_id.hi, inst_id.lo),
                                    None::<crate::types::TNetworkAddress>,
                                    Some(crate::types::TNetworkAddress::new(
                                        self.exchange_host.clone(),
                                        self.exchange_port as i32,
                                    )),
                                    None::<i32>,
                                )]
                            })
                            .collect();

                    let multicast_sink = crate::data_sinks::TMultiCastDataStreamSink::new(
                        sinks, destinations,
                    );
                    fr.output_sink.type_ =
                        crate::data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK;
                    fr.output_sink.multi_cast_stream_sink = Some(multicast_sink);
                }
            }
        }
    }
}
```

- [ ] **Step 3: Implement fragment instance ID assignment and sender count computation**

```rust
impl ExecutionCoordinator {
    /// Assign unique fragment instance IDs.
    /// Uses query_id=1 and fragment_instance_id = (fragment_id + 10, fragment_id + 10).
    fn assign_instance_ids(&mut self) -> std::collections::HashMap<FragmentId, FragmentInstanceId> {
        let mut ids = std::collections::HashMap::new();
        for (i, fr) in self.build_result.fragment_results.iter().enumerate() {
            ids.insert(fr.fragment_id, FragmentInstanceId {
                hi: (i as i64) + 10,
                lo: (i as i64) + 10,
            });
        }
        ids
    }

    fn get_instance_id(&self, fragment_id: FragmentId) -> &FragmentInstanceId {
        self.instance_ids.get(&fragment_id).expect("instance ID not assigned")
    }

    /// Compute per_exch_num_senders for each fragment.
    /// For CTE multicast: each CTE produce fragment has 1 instance = 1 sender.
    fn compute_sender_counts(&self) -> std::collections::HashMap<FragmentId, BTreeMap<i32, i32>> {
        let mut result = std::collections::HashMap::new();
        for fr in &self.build_result.fragment_results {
            let mut per_exch = BTreeMap::new();
            for &(cte_id, exchange_node_id) in &fr.cte_exchange_nodes {
                // Find the CTE produce fragment's instance count (1 for Phase 1)
                let sender_count = 1i32;
                per_exch.insert(exchange_node_id, sender_count);
            }
            if !per_exch.is_empty() {
                result.insert(fr.fragment_id, per_exch);
            }
        }
        result
    }
}
```

- [ ] **Step 4: Implement `execute()` — the main coordination method**

```rust
impl ExecutionCoordinator {
    /// Execute the multi-fragment plan:
    /// 1. Assign instance IDs
    /// 2. Wire multicast sinks
    /// 3. Build TExecBatchPlanFragmentsParams per fragment
    /// 4. Submit in topological order (CTE fragments first, root last)
    /// 5. Fetch results from root fragment
    pub fn execute(mut self) -> Result<super::engine::QueryResult, String> {
        use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};

        let instance_ids = self.assign_instance_ids();
        self.instance_ids = instance_ids;
        self.wire_multicast_sinks();
        let sender_counts = self.compute_sender_counts();

        let query_id = crate::types::TUniqueId::new(1, 1);

        // Determine root fragment for result collection
        let root_id = self.build_result.root_fragment_id;
        let root_inst_id = self.get_instance_id(root_id).clone();

        // Submit fragments in order: CTE fragments first, root last
        for fr in &self.build_result.fragment_results {
            let inst_id = self.get_instance_id(fr.fragment_id);
            let finst = crate::types::TUniqueId::new(inst_id.hi, inst_id.lo);

            let per_exch_num_senders = sender_counts
                .get(&fr.fragment_id)
                .cloned()
                .unwrap_or_default();

            let fragment = crate::planner::TPlanFragment::new(
                Some(fr.plan.clone()),
                None::<Vec<crate::exprs::TExpr>>,    // output_exprs
                Some(fr.output_sink.clone()),
                crate::partitions::TDataPartition::new(
                    crate::partitions::TPartitionType::UNPARTITIONED,
                    None::<Vec<crate::exprs::TExpr>>,
                    None::<Vec<crate::partitions::TRangePartition>>,
                    None::<Vec<crate::partitions::TBucketProperty>>,
                ),
                None::<i64>,
                None::<i64>,
                None::<Vec<crate::data::TGlobalDict>>,
                None::<Vec<crate::data::TGlobalDict>>,
                None::<crate::planner::TCacheParam>,
                None::<BTreeMap<i32, crate::exprs::TExpr>>,
                None::<crate::planner::TGroupExecutionParam>,
            );

            let mut exec_params = fr.exec_params.clone();
            exec_params.query_id = query_id.clone();
            exec_params.fragment_instance_id = finst.clone();
            exec_params.per_exch_num_senders = per_exch_num_senders;

            // Use the existing execute_fragment path directly for in-process execution
            let pipeline_dop = std::thread::available_parallelism()
                .map(|p| p.get().min(4))
                .unwrap_or(4) as i32;

            crate::lower::fragment::execute_fragment(
                &fragment,
                Some(&fr.desc_tbl),
                Some(&exec_params),
                None, // query_opts
                None, // session_time_zone
                pipeline_dop,
                None, // group_execution_scan_dop
                None, // db_name
                None, // profiler
                None, // last_query_id
                None, // fe_addr
                None, // backend_num
                None, // mem_tracker
            )?;
        }

        // Fetch results from the root fragment's result buffer
        // The root fragment uses ResultSink which writes to the global result buffer
        // We need a different approach: execute root fragment with ResultSinkHandle

        todo!("integrate with result collection — see Step 5")
    }
}
```

- [ ] **Step 5: Implement proper result collection**

The root fragment must use a `ResultSinkHandle` to collect results. CTE fragments use exchange sinks (submitted via `submit_exec_batch_plan_fragments`). The root fragment can be executed directly via `execute_plan_with_pipeline`.

Refactor `execute()`: submit CTE fragments via `spawn_exec_fragment()` thread-based execution (matching the existing BE path), and execute the root fragment in-process with `ResultSinkHandle`.

The key insight: CTE fragments' output goes via gRPC exchange to the root fragment's ExchangeNode. So:
1. Start the gRPC server (done in Task 8)
2. Register exchange receivers for the root fragment's exchange nodes
3. Submit CTE fragments as thread-based execution (they'll send via gRPC)
4. Execute root fragment in-process (its ExchangeNodes will receive from gRPC)

Implement this two-path approach:

```rust
pub fn execute(mut self) -> Result<super::engine::QueryResult, String> {
    // ... ID assignment, wiring (from Step 4) ...

    let root_id = self.build_result.root_fragment_id;
    let query_id = crate::types::TUniqueId::new(1, 1);

    // 1. Submit non-root (CTE) fragments as background threads
    for fr in &self.build_result.fragment_results {
        if fr.fragment_id == root_id {
            continue; // root handled separately
        }
        let fragment = self.build_thrift_fragment(fr);
        let mut exec_params = self.build_exec_params(fr);
        let desc_tbl = fr.desc_tbl.clone();
        let pipeline_dop = self.pipeline_dop();

        std::thread::spawn(move || {
            if let Err(e) = crate::lower::fragment::execute_fragment(
                &fragment,
                Some(&desc_tbl),
                Some(&exec_params),
                None, None, pipeline_dop, None, None, None, None, None, None, None,
            ) {
                tracing::error!("CTE fragment execution failed: {}", e);
            }
        });
    }

    // 2. Execute root fragment in-process with ResultSinkHandle
    let root_fr = self.build_result.fragment_results
        .iter()
        .find(|f| f.fragment_id == root_id)
        .unwrap();

    let root_result = crate::standalone::engine::execute_fragment_for_result(
        root_fr,
        &self.compute_sender_counts(),
        self.pipeline_dop(),
    )?;

    Ok(root_result)
}
```

Add `execute_fragment_for_result()` to `engine.rs` — this is essentially the current `execute_plan()` but accepting a `FragmentBuildResult` and exchange sender counts.

- [ ] **Step 6: Verify compilation**

Run: `cargo build 2>&1 | head -50`

This task will have many compilation issues to work through. The key is getting the Thrift type construction right — match the exact generated API. Look at existing usage in `lower/fragment.rs` and `internal_service.rs` for reference.

- [ ] **Step 7: Commit**

```bash
git add src/standalone/coordinator.rs
git commit -m "feat(standalone): ExecutionCoordinator for multi-fragment CTE execution"
```

---

## Task 8: Engine Integration and gRPC Server

**Files:**
- Modify: `src/standalone/engine.rs:3366-3509`
- Modify: `src/standalone/server.rs:280`

- [ ] **Step 1: Start gRPC exchange server in standalone mode**

In `src/standalone/server.rs`, in the `serve_forever()` function (or the server startup path), add gRPC server startup before the MySQL listen loop:

```rust
async fn serve_forever(
    engine: StandaloneNovaRocks,
    mysql_port: u16,
    user: String,
) -> Result<(), String> {
    // Start gRPC exchange server for multi-fragment execution
    let grpc_port = mysql_port + 1000; // e.g. 9030 → 10030
    crate::common::app_config::set_http_port(grpc_port);
    crate::service::grpc_server::start_grpc_server("127.0.0.1")?;
    info!("standalone gRPC exchange server started on port {}", grpc_port);

    // ... existing MySQL server code ...
}
```

Verify that `start_grpc_server` can be called from standalone mode — it may have dependencies on global state (config, etc.). Adapt as needed. The key is that the gRPC exchange endpoint (`/proto.novarocks.NovaRocksGrpc/exchange`) is available for CTE fragments to send data to.

- [ ] **Step 2: Update `build_query_plan()` to use new pipeline**

Replace the current `build_query_plan()` in `src/standalone/engine.rs`:

```rust
fn build_query_plan(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
) -> Result<crate::sql::physical::MultiFragmentBuildResult, String> {
    let (resolved, cte_registry) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let output_columns = resolved.output_columns.clone();
    let query_plan = crate::sql::planner::plan_query(resolved, cte_registry)?;

    // Collect table stats from all plans (CTE + main)
    let mut all_stats = std::collections::HashMap::new();
    for cte in &query_plan.cte_plans {
        let stats = build_table_stats_from_plan(&cte.plan);
        all_stats.extend(stats);
    }
    let main_stats = build_table_stats_from_plan(&query_plan.main_plan);
    all_stats.extend(main_stats);

    let optimized = crate::sql::optimizer::optimize_query_plan(query_plan, &all_stats);
    let fragment_plan = crate::sql::fragment::plan_fragments(optimized);

    // If no CTE fragments, use single-fragment path (optimization for simple queries)
    if fragment_plan.fragments.len() == 1 {
        return build_single_fragment_plan(&fragment_plan.fragments[0], catalog, current_database);
    }

    crate::sql::physical::emit_multi_fragment(fragment_plan, catalog, current_database)
}
```

- [ ] **Step 3: Update query execution to use coordinator for multi-fragment plans**

```rust
fn execute_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
) -> Result<QueryResult, String> {
    let build_result = build_query_plan(query, catalog, current_database)?;

    if build_result.fragment_results.len() == 1 {
        // Single fragment — use existing direct execution path
        let fr = &build_result.fragment_results[0];
        return execute_plan_single_fragment(fr);
    }

    // Multi-fragment — use coordinator
    let coordinator = crate::standalone::coordinator::ExecutionCoordinator::new(
        build_result,
        "127.0.0.1".to_string(),
        crate::common::app_config::http_port(),
    );
    coordinator.execute()
}
```

- [ ] **Step 4: Keep single-fragment fast path**

Ensure that queries without shared CTEs (the majority) still go through the existing direct `execute_plan()` path. The `build_query_plan()` function should detect when there are no CTE fragments and produce a single-fragment result that the existing path can consume.

```rust
fn build_single_fragment_plan(
    fragment: &crate::sql::fragment::PlanFragment,
    catalog: &InMemoryCatalog,
    current_database: &str,
) -> Result<crate::sql::physical::MultiFragmentBuildResult, String> {
    // Delegate to existing single-fragment emission
    let result = crate::sql::physical::emit(
        fragment.plan.clone(),
        &fragment.output_columns.iter().map(|c| /* convert */ ).collect::<Vec<_>>(),
        catalog,
        current_database,
    )?;
    // Wrap in MultiFragmentBuildResult with single entry
    // ...
}
```

- [ ] **Step 5: Verify compilation**

Run: `cargo build 2>&1 | head -50`

This is the most integration-heavy step. Expect many type mismatches and missing conversions. Work through them methodically.

- [ ] **Step 6: Commit**

```bash
git add src/standalone/engine.rs src/standalone/server.rs
git commit -m "feat(standalone): integrate multi-fragment execution with gRPC exchange"
```

---

## Task 9: End-to-End Testing

**Files:**
- No new files — uses existing sql-tests infrastructure

- [ ] **Step 1: Test with a minimal CTE query**

Start the standalone server and connect via MySQL client:

```sql
-- Simple CTE used twice
WITH cte AS (SELECT 1 AS a, 2 AS b)
SELECT c1.a, c2.b FROM cte c1 JOIN cte c2 ON c1.a = c2.a;
```

Expected: returns `1, 2`.

If this fails, debug by checking:
- Does the analyzer produce a `CTERegistry` with one entry?
- Does the planner produce a `QueryPlan` with one CTE plan?
- Does the fragment planner produce 2 fragments?
- Does the emitter produce valid exchange nodes and multicast sinks?
- Does the coordinator submit both fragments?
- Does the gRPC exchange deliver data?

- [ ] **Step 2: Test with a more complex CTE**

```sql
WITH customer_total AS (
    SELECT 1 AS customer_id, 100 AS total
    UNION ALL
    SELECT 2, 200
)
SELECT a.customer_id, b.total
FROM customer_total a
JOIN customer_total b ON a.customer_id = b.customer_id
WHERE a.total > 50;
```

- [ ] **Step 3: Run TPC-DS CTE-heavy queries**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite tpc-ds \
    --mode verify \
    --only q1,q4,q11,q14,q23,q24,q39,q47,q57,q59,q64,q74,q75,q78,q83 \
    --query_timeout 300
```

These are TPC-DS queries known to heavily use CTEs. Track which ones now pass vs previously timed out.

- [ ] **Step 4: Run full TPC-DS regression**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite tpc-ds \
    --mode verify \
    --query_timeout 300
```

Compare results against the previous baseline of ~60-70 passing queries. The CTE multicast should improve the pass count.

- [ ] **Step 5: Commit any test fixes**

```bash
git add -A
git commit -m "test: validate CTE multicast with TPC-DS queries"
```

---

## Notes for Implementer

### Key Gotchas

1. **Thrift generated API**: The generated Rust Thrift types use `new()` constructors with positional args in field order. Check the generated code in `target/` or the `.thrift` IDL files for exact field order. Use `Default::default()` + field assignment when constructors are unwieldy.

2. **Exchange registration**: When a fragment's ExchangeNode starts executing, it registers with the global exchange registry (`src/runtime/exchange.rs`) using `ExchangeKey(finst_id_hi, finst_id_lo, node_id)`. The sender must use matching keys. Ensure the coordinator assigns consistent IDs.

3. **gRPC server in standalone mode**: The `start_grpc_server()` function depends on global config (`http_port()`, `starlet_port()`). You may need to set these before calling it. Check `src/common/app_config.rs` for how ports are configured.

4. **Thread synchronization**: CTE fragments run in background threads. The root fragment's ExchangeNodes will block until data arrives. Ensure CTE fragments are submitted (threads spawned) before the root fragment starts executing.

5. **Descriptor table sharing**: All fragments must share the same `TDescriptorTable` so that tuple/slot IDs are consistent. The `emit_multi_fragment()` function handles this by using a single `ThriftEmitter`.

### Fallback path

If a CTE has only 1 reference, the analyzer still inlines it (existing behavior). The `fragment_plan()` function will produce a single-fragment result, and the engine uses the existing fast path. This ensures zero regression for non-CTE queries.

### StarRocks reference

For implementation details, reference the StarRocks BE multicast handling:
- `~/worktree/starrocks/main/be/src/exec/pipeline/exchange/multi_cast_local_exchange.h`
- `~/worktree/starrocks/main/be/src/runtime/multi_cast_data_stream_sink.h`

And the FE fragment planning:
- `~/worktree/starrocks/main/fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java` (visitPhysicalCTEProduce/Consume)
