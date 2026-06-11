# Cascades Distribution Fragment Splitting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `PhysicalDistribution` into real fragment boundaries in the standalone Cascades path, and generalize multi-fragment execution so ordinary stream exchanges and CTE multicast share one coordinator path.

**Architecture:** Keep `search` and `extract` purely physical-plan focused. `PlanFragmentBuilder` becomes the only place that converts `PhysicalDistribution` / `PhysicalCTE*` nodes into fragment boundaries, shared `FragmentEdge` metadata, and `ExchangeNode`s. `ExecutionCoordinator` then wires those edges into Thrift sinks and launches non-root fragments before the root fragment.

**Tech Stack:** Rust, Cascades `PhysicalPlanNode`, Thrift `plan_nodes` / `data_sinks` / `partitions`, standalone engine/coordinator, existing exchange runtime

---

## File Structure

- `src/sql/physical/mod.rs`
  Shared multi-fragment output types. This file should own `FragmentEdge`, `FragmentEdgeKind`, and the upgraded `MultiFragmentBuildResult` so both builder and coordinator read the same metadata model.

- `src/sql/cascades/fragment_builder.rs`
  Converts `PhysicalPlanNode` into fragments. This is where `PhysicalDistribution` must stop being a pass-through and start producing completed child fragments, stream edges, and `ExchangeNode` placeholders.

- `src/sql/physical/nodes.rs`
  Owns low-level Thrift plan-node constructors. `build_exchange_node()` should accept an explicit partition type instead of always emitting `UNPARTITIONED`.

- `src/standalone/coordinator.rs`
  Owns fragment instance IDs, sink wiring, sender counts, and fragment launch order. This file should stop assuming “multi-fragment means CTE only”.

- `src/standalone/engine.rs`
  Keeps end-to-end tests close to the standalone execution path. Add non-CTE multi-fragment assertions here and rerun the existing CTE regression tests from here.

### Planned Decomposition

- Task 1 adds the shared edge model and makes `Gather` distribution create a real child fragment and stream edge.
- Task 2 extends the builder to compile `HashPartitioned` partition expressions, reject unsupported `DistributionSpec::Any`, and preserve root gather elision.
- Task 3 generalizes the coordinator to wire ordinary stream edges, then validates non-CTE execution and CTE regression end to end.

---

### Task 1: Shared Edge Model And Gather Split

**Files:**
- Modify: `src/sql/physical/mod.rs`
- Modify: `src/sql/cascades/fragment_builder.rs`
- Modify: `src/sql/physical/nodes.rs`
- Test: `src/sql/cascades/fragment_builder.rs`

- [ ] **Step 1: Write the failing builder test for a non-root `Gather` distribution**

Add this test module at the bottom of `src/sql/cascades/fragment_builder.rs` (or extend the existing one if present):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use arrow::datatypes::DataType;
    use tempfile::NamedTempFile;

    use crate::plan_nodes;
    use crate::sql::catalog::{CatalogProvider, ColumnDef, TableDef, TableStorage};
    use crate::sql::cascades::operator::{
        Operator, PhysicalDistributionOp, PhysicalScanOp, PhysicalSortOp,
    };
    use crate::sql::cascades::physical_plan::PhysicalPlanNode;
    use crate::sql::cascades::property::DistributionSpec;
    use crate::sql::ir::{ExprKind, OutputColumn, SortItem, TypedExpr};
    use crate::sql::statistics::Statistics;

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in scan-only builder tests".to_string())
        }
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]
    }

    fn id_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn stats() -> Statistics {
        Statistics {
            output_row_count: 3.0,
            column_statistics: HashMap::new(),
        }
    }

    fn scan_plan(path: PathBuf) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                    }],
                    storage: TableStorage::LocalParquetFile { path },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
        }
    }

    #[test]
    fn build_splits_gather_distribution_into_stream_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalSort(PhysicalSortOp {
                items: vec![SortItem {
                    expr: id_expr(),
                    asc: true,
                    nulls_first: false,
                }],
            }),
            children: vec![PhysicalPlanNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![scan_plan(file.path().to_path_buf())],
                stats: stats(),
                output_columns: output_columns(),
            }],
            stats: stats(),
            output_columns: output_columns(),
        };

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");

        assert_eq!(build.fragment_results.len(), 2);
        assert_eq!(build.edges.len(), 1);
        assert!(matches!(
            build.edges[0].edge_kind,
            crate::sql::physical::FragmentEdgeKind::Stream
        ));

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert!(root.plan.nodes.iter().any(|node| {
            node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE
        }));
    }
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cargo test build_splits_gather_distribution_into_stream_edge --lib`

Expected: FAIL because `PlanFragmentBuilder::build()` does not yet return `fragment_results` / `edges`, and `visit_distribution()` still passes the child through instead of creating a fragment boundary.

- [ ] **Step 3: Add shared edge metadata to `src/sql/physical/mod.rs`**

Insert these types near `MultiFragmentBuildResult`:

```rust
use crate::partitions;

pub(crate) enum FragmentEdgeKind {
    Stream,
    CteMulticast { cte_id: CteId },
}

pub(crate) struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub output_partition: partitions::TDataPartition,
    pub edge_kind: FragmentEdgeKind,
}

pub(crate) struct MultiFragmentBuildResult {
    pub fragment_results: Vec<FragmentBuildResult>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<FragmentEdge>,
}
```

- [ ] **Step 4: Replace the local builder result type with `MultiFragmentBuildResult`**

In `src/sql/cascades/fragment_builder.rs`, delete the local `BuildResult` struct and switch the builder to the shared type:

```rust
use crate::sql::physical::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult, OutputColumn,
};

pub(crate) struct PlanFragmentBuilder<'a> {
    catalog: &'a dyn CatalogProvider,
    current_database: &'a str,
    desc_builder: DescriptorTableBuilder,
    scan_tables: Vec<(i32, ResolvedTable)>,
    next_node_id: i32,
    next_slot_id: i32,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
    fragment_stack: Vec<FragmentId>,
    completed_fragments: Vec<FragmentBuildResult>,
    completed_edges: Vec<FragmentEdge>,
    cte_fragments: HashMap<CteId, usize>,
}

pub(crate) fn build(
    plan: &PhysicalPlanNode,
    catalog: &'a dyn CatalogProvider,
    current_database: &str,
) -> Result<MultiFragmentBuildResult, String> {
    let mut builder = PlanFragmentBuilder {
        catalog,
        current_database,
        desc_builder: DescriptorTableBuilder::new(),
        scan_tables: Vec::new(),
        next_node_id: 1,
        next_slot_id: 1,
        next_tuple_id: 1,
        next_fragment_id: 0,
        fragment_stack: Vec::new(),
        completed_fragments: Vec::new(),
        completed_edges: Vec::new(),
        cte_fragments: HashMap::new(),
    };

    let root_fragment_id = builder.alloc_fragment_id();
    builder.fragment_stack.push(root_fragment_id);
    let result = builder.visit(plan)?;

    let desc_tbl =
        std::mem::replace(&mut builder.desc_builder, DescriptorTableBuilder::new()).build();
    let exec_params = nodes::build_exec_params_multi(
        &builder
            .scan_tables
            .iter()
            .map(|(id, rt)| (*id, rt.clone()))
            .collect::<Vec<_>>(),
    )?;
    let output_columns = plan
        .output_columns
        .iter()
        .map(|c| OutputColumn {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        })
        .collect::<Vec<_>>();

    for fragment in &mut builder.completed_fragments {
        fragment.desc_tbl = desc_tbl.clone();
        fragment.exec_params = exec_params.clone();
    }

    let root_fragment = FragmentBuildResult {
        fragment_id: root_fragment_id,
        plan: plan_nodes::TPlan::new(result.plan_nodes),
        desc_tbl: desc_tbl.clone(),
        exec_params: exec_params.clone(),
        output_sink: build_result_sink(),
        output_columns,
        cte_id: None,
        cte_exchange_nodes: result.cte_exchange_nodes,
    };

    let mut fragment_results = builder.completed_fragments;
    fragment_results.push(root_fragment);

    Ok(MultiFragmentBuildResult {
        fragment_results,
        root_fragment_id,
        edges: builder.completed_edges,
    })
}
```

- [ ] **Step 5: Let `build_exchange_node()` accept the exchange partition type**

Change the helper in `src/sql/physical/nodes.rs`:

```rust
pub(crate) fn build_exchange_node(
    node_id: i32,
    input_row_tuples: Vec<i32>,
    partition_type: partitions::TPartitionType,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = input_row_tuples.clone();
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.exchange_node = Some(plan_nodes::TExchangeNode::new(
        input_row_tuples,
        None::<plan_nodes::TSortInfo>,
        None::<i64>,
        Some(partition_type),
        None::<bool>,
        None::<plan_nodes::TLateMaterializeMode>,
    ));
    node
}
```

Update every call site in `fragment_builder.rs` and `src/sql/physical/emitter/mod.rs` to pass `partitions::TPartitionType::UNPARTITIONED` for the existing CTE consume behavior.

- [ ] **Step 6: Implement `Gather` splitting in `visit_distribution()`**

Replace the pass-through implementation in `src/sql/cascades/fragment_builder.rs` with a real split for `DistributionSpec::Gather`:

```rust
fn unpartitioned_stream_partition() -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partitions::TPartitionType::UNPARTITIONED,
        None::<Vec<crate::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

fn current_fragment_id(&self) -> Result<FragmentId, String> {
    self.fragment_stack
        .last()
        .copied()
        .ok_or_else(|| "no active fragment id in builder".to_string())
}

fn visit_distribution(
    &mut self,
    op: &PhysicalDistributionOp,
    node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    if node.children.len() != 1 {
        return Err(format!(
            "PhysicalDistribution expected exactly 1 child, got {}",
            node.children.len()
        ));
    }

    if !matches!(op.spec, crate::sql::cascades::property::DistributionSpec::Gather) {
        return Err("Task 1 only implements Gather distribution splitting".to_string());
    }

    let parent_fragment_id = self.current_fragment_id()?;
    let child = self.visit(&node.children[0])?;
    let child_fragment_id = self.alloc_fragment_id();
    self.completed_fragments.push(FragmentBuildResult {
        fragment_id: child_fragment_id,
        plan: plan_nodes::TPlan::new(child.plan_nodes.clone()),
        desc_tbl: DescriptorTableBuilder::new().build(),
        exec_params: nodes::build_exec_params_multi(&[])?,
        output_sink: build_noop_sink(),
        output_columns: node.children[0]
            .output_columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect(),
        cte_id: None,
        cte_exchange_nodes: child.cte_exchange_nodes.clone(),
    });

    let exchange_node_id = self.alloc_node();
    let exchange_node = nodes::build_exchange_node(
        exchange_node_id,
        child.tuple_ids.clone(),
        partitions::TPartitionType::UNPARTITIONED,
    );

    self.completed_edges.push(FragmentEdge {
        source_fragment_id: child_fragment_id,
        target_fragment_id: parent_fragment_id,
        target_exchange_node_id: exchange_node_id,
        output_partition: unpartitioned_stream_partition(),
        edge_kind: FragmentEdgeKind::Stream,
    });

    Ok(VisitResult {
        plan_nodes: vec![exchange_node],
        scope: child.scope,
        tuple_ids: child.tuple_ids,
        cte_exchange_nodes: Vec::new(),
    })
}
```

- [ ] **Step 7: Run the focused test and make sure it passes**

Run: `cargo test build_splits_gather_distribution_into_stream_edge --lib`

Expected: PASS with `fragment_results.len() == 2`, one `Stream` edge, and one `EXCHANGE_NODE` in the root fragment.

- [ ] **Step 8: Format and commit**

Run: `cargo fmt`

Run:

```bash
git add src/sql/physical/mod.rs src/sql/cascades/fragment_builder.rs src/sql/physical/nodes.rs
git commit -m "feat(cascades): split gather distribution into stream edges"
```

---

### Task 2: Hash Partitioning, Root Gather Elision, And Builder Fail-Fast

**Files:**
- Modify: `src/sql/cascades/fragment_builder.rs`
- Modify: `src/sql/physical/nodes.rs`
- Test: `src/sql/cascades/fragment_builder.rs`

- [ ] **Step 1: Write failing tests for `HashPartitioned`, `Any`, and root gather elision**

Extend `src/sql/cascades/fragment_builder.rs` tests with:

```rust
#[test]
fn build_maps_hash_distribution_to_hash_partitioned_edge() {
    let file = NamedTempFile::new().expect("temp parquet path");
    let plan = PhysicalPlanNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned(vec![
                crate::sql::cascades::property::ColumnRef {
                    qualifier: None,
                    column: "id".to_string(),
                },
            ]),
        }),
        children: vec![scan_plan(file.path().to_path_buf())],
        stats: stats(),
        output_columns: output_columns(),
    };

    let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
    let edge = build.edges.first().expect("stream edge");
    assert_eq!(
        edge.output_partition.type_,
        partitions::TPartitionType::HASH_PARTITIONED
    );
    assert_eq!(
        edge.output_partition.partition_exprs.as_ref().map(|v| v.len()),
        Some(1)
    );
}

#[test]
fn build_rejects_any_distribution_in_fragment_builder() {
    let file = NamedTempFile::new().expect("temp parquet path");
    let plan = PhysicalPlanNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Any,
        }),
        children: vec![scan_plan(file.path().to_path_buf())],
        stats: stats(),
        output_columns: output_columns(),
    };

    let err = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default")
        .expect_err("distribution any must fail");
    assert!(err.contains("PhysicalDistribution(Any)"));
}

#[test]
fn build_elides_root_gather_distribution() {
    let file = NamedTempFile::new().expect("temp parquet path");
    let plan = PhysicalPlanNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Gather,
        }),
        children: vec![scan_plan(file.path().to_path_buf())],
        stats: stats(),
        output_columns: output_columns(),
    };

    let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
    assert_eq!(build.fragment_results.len(), 1);
    assert!(build.edges.is_empty());
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```bash
cargo test build_maps_hash_distribution_to_hash_partitioned_edge --lib
cargo test build_rejects_any_distribution_in_fragment_builder --lib
cargo test build_elides_root_gather_distribution --lib
```

Expected:

- `HashPartitioned` test fails because `visit_distribution()` only knows `Gather`
- `Any` test fails because builder still silently accepts / passes through
- root gather elision test fails because the root still becomes a fragment boundary

- [ ] **Step 3: Add a helper that converts `DistributionSpec` into `TDataPartition`**

In `src/sql/cascades/fragment_builder.rs`, add a builder helper that compiles hash partition expressions from the child output scope:

```rust
fn build_output_partition(
    &self,
    spec: &crate::sql::cascades::property::DistributionSpec,
    child_scope: &ExprScope,
) -> Result<partitions::TDataPartition, String> {
    match spec {
        crate::sql::cascades::property::DistributionSpec::Gather => Ok(
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
        ),
        crate::sql::cascades::property::DistributionSpec::HashPartitioned(cols) => {
            let mut partition_exprs = Vec::with_capacity(cols.len());
            for col in cols {
                let binding = child_scope
                    .resolve_column(col.qualifier.as_deref(), &col.column)?
                    .clone();
                let type_desc = type_infer::arrow_type_to_type_desc(&binding.data_type)?;
                partition_exprs.push(expr_compiler::build_slot_ref_texpr(
                    binding.slot_id,
                    binding.tuple_id,
                    type_desc,
                ));
            }
            Ok(partitions::TDataPartition::new(
                partitions::TPartitionType::HASH_PARTITIONED,
                Some(partition_exprs),
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ))
        }
        crate::sql::cascades::property::DistributionSpec::Any => {
            Err("PhysicalDistribution(Any) is not supported in fragment builder".to_string())
        }
    }
}
```

- [ ] **Step 4: Elide a root `Gather` before normal visitation**

At the top of `PlanFragmentBuilder::build()`, strip only the top-level gather wrapper:

```rust
let plan = match &plan.op {
    Operator::PhysicalDistribution(op)
        if matches!(op.spec, crate::sql::cascades::property::DistributionSpec::Gather) =>
    {
        plan.children
            .first()
            .ok_or_else(|| "root PhysicalDistribution(Gather) missing child".to_string())?
    }
    _ => plan,
};
```

Keep all inner `PhysicalDistribution` nodes untouched so join/aggregate enforcement still creates real fragment boundaries.

- [ ] **Step 5: Extend `visit_distribution()` to support `HashPartitioned`**

Replace the Task 1 “Gather only” branch with a general implementation:

```rust
let output_partition = self.build_output_partition(&op.spec, &child.scope)?;
let exchange_partition_type = output_partition.type_.clone();
let parent_fragment_id = self.current_fragment_id()?;

let exchange_node = nodes::build_exchange_node(
    exchange_node_id,
    child.tuple_ids.clone(),
    exchange_partition_type,
);

self.completed_edges.push(FragmentEdge {
    source_fragment_id: child_fragment_id,
    target_fragment_id: parent_fragment_id,
    target_exchange_node_id: exchange_node_id,
    output_partition,
    edge_kind: FragmentEdgeKind::Stream,
});
```

Preserve the existing fail-fast checks:

- child count must be exactly 1
- `DistributionSpec::Any` must error
- hash distribution without resolvable columns must bubble up as an error from `resolve_column()`

- [ ] **Step 6: Update any `build_exchange_node()` call sites that still assume no partition type**

For the CTE consume path, keep the existing behavior explicit:

```rust
let exchange_node = nodes::build_exchange_node(
    exchange_node_id,
    vec![exchange_tuple_id],
    partitions::TPartitionType::UNPARTITIONED,
);
```

- [ ] **Step 7: Run the focused builder tests and make sure they pass**

Run:

```bash
cargo test build_maps_hash_distribution_to_hash_partitioned_edge --lib
cargo test build_rejects_any_distribution_in_fragment_builder --lib
cargo test build_elides_root_gather_distribution --lib
```

Expected: PASS for all three tests.

- [ ] **Step 8: Format and commit**

Run: `cargo fmt`

Run:

```bash
git add src/sql/cascades/fragment_builder.rs src/sql/physical/nodes.rs
git commit -m "feat(cascades): add hash distribution partition mapping"
```

---

### Task 3: Generic Stream-Edge Coordination And End-To-End Validation

**Files:**
- Modify: `src/standalone/coordinator.rs`
- Modify: `src/standalone/engine.rs`
- Modify: `src/sql/cascades/fragment_builder.rs`
- Test: `src/standalone/engine.rs`

- [ ] **Step 1: Write failing end-to-end tests for non-CTE multi-fragment build, explain, and execution**

Add these tests to `src/standalone/engine.rs` inside the existing `#[cfg(test)]` module:

```rust
#[test]
fn embedded_query_builder_splits_non_cte_join_into_multiple_fragments() {
    let parquet = write_parquet_file();
    let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
    engine
        .register_parquet_table("tbl", parquet.path())
        .expect("register table");

    let catalog = engine.inner.catalog.read().unwrap();
    let stmt = crate::sql::parser::parse_sql_raw(
        "SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id ORDER BY 1",
    )
    .expect("parse query");
    let sqlparser::ast::Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let (resolved, cte_registry) =
        crate::sql::analyzer::analyze(&query, &catalog, DEFAULT_DATABASE).expect("analyze");
    let logical = crate::sql::planner::plan_query(resolved, cte_registry).expect("plan");
    let table_stats = super::build_table_stats_from_plan(&logical);
    let physical = crate::sql::cascades::optimize(logical, &table_stats).expect("optimize");
    let build_result = crate::sql::cascades::fragment_builder::PlanFragmentBuilder::build(
        &physical,
        &*catalog,
        DEFAULT_DATABASE,
    )
    .expect("build fragments");

    assert!(
        build_result.fragment_results.len() > 1,
        "fragments={}",
        build_result.fragment_results.len()
    );
    assert!(build_result.edges.iter().any(|edge| {
        matches!(edge.edge_kind, crate::sql::physical::FragmentEdgeKind::Stream)
    }));
}

#[test]
fn embedded_query_explain_for_non_cte_join_shows_physical_exchange() {
    let parquet = write_parquet_file();
    let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
    engine
        .register_parquet_table("tbl", parquet.path())
        .expect("register table");

    let session = engine.session();
    let explain = session
        .query("EXPLAIN SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id ORDER BY 1")
        .expect("execute explain");

    let text = explain
        .chunks
        .iter()
        .flat_map(|chunk| {
            let col = chunk.batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("string array");
            (0..arr.len())
                .map(|idx| arr.value(idx).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        text.contains("GATHER EXCHANGE") || text.contains("HASH EXCHANGE"),
        "text={text}"
    );
}

#[test]
fn embedded_query_executes_non_cte_join_through_stream_exchange() {
    let parquet = write_parquet_file();
    let engine = StandaloneNovaRocks::open(StandaloneOptions::default()).expect("open engine");
    engine
        .register_parquet_table("tbl", parquet.path())
        .expect("register table");

    let session = engine.session();
    let result = session
        .query("SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id ORDER BY 1")
        .expect("execute query");

    assert_eq!(result.row_count(), 3);
    let chunk = &result.chunks[0];
    assert_eq!(chunk.schema().field(0).name(), "id");
}
```

- [ ] **Step 2: Run the new non-CTE tests and verify they fail**

Run:

```bash
cargo test embedded_query_builder_splits_non_cte_join_into_multiple_fragments
cargo test embedded_query_explain_for_non_cte_join_shows_physical_exchange
cargo test embedded_query_executes_non_cte_join_through_stream_exchange
```

Expected:

- build test fails if ordinary `Stream` edges are not exposed
- execution test fails because the coordinator only wires CTE multicast
- explain test may pass already, which is acceptable; keep it as a regression guard

- [ ] **Step 3: Refactor the coordinator around generic fragment preparation helpers**

In `src/standalone/coordinator.rs`, introduce a small prepared-fragment container and helper skeletons:

```rust
struct PreparedFragment {
    build: FragmentBuildResult,
    thrift_fragment: planner::TPlanFragment,
    exec_params: crate::internal_service::TPlanFragmentExecParams,
}

fn assign_instance_ids(
    fragment_results: &[FragmentBuildResult],
) -> BTreeMap<FragmentId, (i64, i64)> {
    fragment_results
        .iter()
        .map(|fr| (fr.fragment_id, (fr.fragment_id as i64 + 100, fr.fragment_id as i64 + 1)))
        .collect()
}
```

Then restructure `execute()` so it prepares all fragments first, instead of immediately splitting them into “root” and “CTE” buckets.

- [ ] **Step 4: Implement `wire_stream_edges()` with fail-fast duplicate-target checks**

Add a helper in `src/standalone/coordinator.rs` that wires ordinary stream edges before CTE multicast:

```rust
fn wire_stream_edges(
    prepared: &mut BTreeMap<FragmentId, PreparedFragment>,
    edges: &[crate::sql::physical::FragmentEdge],
    instance_map: &BTreeMap<FragmentId, (i64, i64)>,
    brpc_addr: &types::TNetworkAddress,
    per_fragment_exch_num_senders: &mut BTreeMap<FragmentId, BTreeMap<i32, i32>>,
) -> Result<(), String> {
    let mut seen_targets = std::collections::BTreeSet::new();

    for edge in edges {
        if !matches!(edge.edge_kind, crate::sql::physical::FragmentEdgeKind::Stream) {
            continue;
        }

        let target_key = (edge.target_fragment_id, edge.target_exchange_node_id);
        if !seen_targets.insert(target_key) {
            return Err(format!(
                "stream exchange node {} in fragment {} has multiple upstream fragments; fan-in is not supported in this phase",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }

        let target_instance = instance_map
            .get(&edge.target_fragment_id)
            .ok_or_else(|| format!("missing target instance for fragment {}", edge.target_fragment_id))?;

        let prepared_source = prepared
            .get_mut(&edge.source_fragment_id)
            .ok_or_else(|| format!("missing source fragment {}", edge.source_fragment_id))?;

        let stream_sink = data_sinks::TDataStreamSink::new(
            edge.target_exchange_node_id,
            edge.output_partition.clone(),
            None::<bool>,
            None::<bool>,
            None::<i32>,
            None::<Vec<i32>>,
            None::<i64>,
        );
        let output_sink = data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::DATA_STREAM_SINK,
            Some(stream_sink),
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            None::<data_sinks::TIcebergTableSink>,
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
        );
        prepared_source.thrift_fragment.output_sink = Some(output_sink);
        prepared_source.exec_params.destinations = Some(vec![data_sinks::TPlanFragmentDestination::new(
            types::TUniqueId::new(target_instance.0, target_instance.1),
            None::<types::TNetworkAddress>,
            Some(brpc_addr.clone()),
            None::<i32>,
        )]);

        per_fragment_exch_num_senders
            .entry(edge.target_fragment_id)
            .or_default()
            .insert(edge.target_exchange_node_id, 1);
    }

    Ok(())
}
```

- [ ] **Step 5: Keep CTE multicast on the same edge-driven framework**

Rewrite the existing CTE section in `src/standalone/coordinator.rs` so it filters `build_result.edges` for `FragmentEdgeKind::CteMulticast { .. }` instead of depending on `cte_id` / `cte_exchange_nodes` as the primary source of truth:

```rust
for edge in edges {
    let crate::sql::physical::FragmentEdgeKind::CteMulticast { cte_id } = edge.edge_kind else {
        continue;
    };
    cte_consumers
        .entry(cte_id)
        .or_default()
        .push((edge.target_fragment_id, edge.target_exchange_node_id));
}
```

Keep the legacy `cte_id` / `cte_exchange_nodes` fields only as compatibility checks while you migrate; once the edge model is stable, treat them as derived / transitional.

- [ ] **Step 6: Simplify `execute_query()` to use the upgraded builder result directly**

In `src/standalone/engine.rs`, remove the local `BuildResult -> MultiFragmentBuildResult` conversion and switch to the shared result type:

```rust
let build_result = crate::sql::cascades::fragment_builder::PlanFragmentBuilder::build(
    &physical,
    catalog,
    current_database,
)?;

if build_result.fragment_results.len() == 1 {
    let frag = build_result.fragment_results.into_iter().next().unwrap();
    execute_plan(PlanBuildResult {
        plan: frag.plan,
        desc_tbl: frag.desc_tbl,
        exec_params: frag.exec_params,
        output_columns: frag.output_columns,
    })
} else {
    super::coordinator::ExecutionCoordinator::new(
        build_result,
        "127.0.0.1".to_string(),
        exchange_port,
    )
    .execute()
}
```

- [ ] **Step 7: Run the new non-CTE tests and make sure they pass**

Run:

```bash
cargo test embedded_query_builder_splits_non_cte_join_into_multiple_fragments
cargo test embedded_query_explain_for_non_cte_join_shows_physical_exchange
cargo test embedded_query_executes_non_cte_join_through_stream_exchange
```

Expected: PASS for all three tests.

- [ ] **Step 8: Re-run the existing CTE regression tests**

Run:

```bash
cargo test embedded_query_executes_single_use_cte_through_cascades
cargo test embedded_query_executes_multi_use_cte_through_multicast_reuse
cargo test embedded_query_explain_for_multi_use_cte_shows_physical_cte_nodes
```

Expected: PASS for all three tests, proving the coordinator still handles the current CTE multicast path.

- [ ] **Step 9: Format and commit**

Run: `cargo fmt`

Run:

```bash
git add src/standalone/coordinator.rs src/standalone/engine.rs src/sql/cascades/fragment_builder.rs
git commit -m "feat(cascades): wire stream edges through standalone coordinator"
```

---

## Self-Review Checklist

- Spec coverage:
  - `FragmentEdge` / `MultiFragmentBuildResult.edges` is covered by Task 1.
  - `visit_distribution()` real fragment split is covered by Tasks 1 and 2.
  - `HashPartitioned -> HASH_PARTITIONED + partition_exprs` is covered by Task 2.
  - root gather elision is covered by Task 2.
  - generic `ExecutionCoordinator` stream-edge wiring is covered by Task 3.
  - CTE multicast reuse on the same framework is covered by Task 3.
  - non-CTE build / explain / execute acceptance criteria are covered by Task 3 tests.

- Placeholder scan:
  - No `TODO`, `TBD`, “implement later”, or “similar to Task N” placeholders remain.

- Type consistency:
  - Plan uses `MultiFragmentBuildResult.fragment_results` and `MultiFragmentBuildResult.edges` consistently.
  - Plan uses `FragmentEdgeKind::Stream` and `FragmentEdgeKind::CteMulticast { cte_id }` consistently.
  - Plan keeps `build_exchange_node(node_id, input_row_tuples, partition_type)` consistent across builder and emitter call sites.

