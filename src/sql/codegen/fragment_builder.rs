//! PlanFragmentBuilder — converts a PhysicalPlanNode tree into Thrift TPlan
//! per fragment.
//!
//! Fragment boundaries are created at `PhysicalDistribution` nodes.
//! `PhysicalCTEProduce` / `PhysicalCTEConsume` create multicast fragments
//! whose sinks are wired by the `ExecutionCoordinator` after building.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::data_sinks;
use crate::exprs;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::partitions;
use crate::plan_nodes;
use crate::types;

use crate::sql::analysis::cte::CteId;
use crate::sql::catalog::CatalogProvider;
use crate::sql::codegen::FragmentId;
use crate::sql::codegen::descriptors::DescriptorTableBuilder;
use crate::sql::codegen::expr_compiler::{self, ExprCompiler};
use crate::sql::codegen::helpers::{
    agg_call_display_name, join_kind_to_op, split_and_conjuncts_typed, typed_expr_display_name,
};
use crate::sql::codegen::nodes;
use crate::sql::codegen::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::codegen::type_infer;
use crate::sql::codegen::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult, OutputColumn,
};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::operator::{
    AggMode, PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp,
    PhysicalDistributionOp, PhysicalExceptOp, PhysicalFilterOp, PhysicalGenerateSeriesOp,
    PhysicalHashAggregateOp, PhysicalHashJoinOp, PhysicalIntersectOp, PhysicalLimitOp,
    PhysicalNestLoopJoinOp, PhysicalProjectOp, PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp,
    PhysicalSubqueryAliasOp, PhysicalTableFunctionOp, PhysicalTopNOp, PhysicalUnionOp,
    PhysicalValuesOp, PhysicalWindowOp,
};
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;

use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::planner::plan::AggregateCall;

// ---------------------------------------------------------------------------
// Internal visitor result
// ---------------------------------------------------------------------------

struct VisitResult {
    /// Plan nodes in pre-order (top-down) traversal order.
    plan_nodes: Vec<plan_nodes::TPlanNode>,
    /// Scope describing the output columns with their physical bindings.
    scope: ExprScope,
    /// Tuple IDs in this subtree's output.
    tuple_ids: Vec<i32>,
    /// Exchange nodes in this fragment that consume from CTE fragments:
    /// `(cte_id, exchange_node_id)`.
    cte_exchange_nodes: Vec<(CteId, i32)>,
}

// ---------------------------------------------------------------------------
// Scan/join ownership metadata (used by RF planning)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ScanTupleOwner {
    pub scan_node_id: i32,
    pub fragment_id: FragmentId,
}

fn add_iceberg_equality_delete_required_columns(
    required: &mut std::collections::HashSet<String>,
    table: &crate::sql::catalog::TableDef,
) -> Result<(), String> {
    let crate::sql::catalog::TableStorage::S3ParquetFiles { files, .. } = &table.storage else {
        return Ok(());
    };
    let field_id_to_name: HashMap<i32, String> = table
        .iceberg_table
        .as_ref()
        .map(|iceberg| {
            iceberg
                .schema
                .fields
                .iter()
                .map(|field| (field.field_id, field.name.clone()))
                .collect()
        })
        .unwrap_or_default();
    for file in files {
        for delete_file in &file.delete_files {
            if delete_file.file_content != crate::sql::catalog::IcebergDeleteFileContent::Equality {
                continue;
            }
            if delete_file.equality_field_ids.is_empty() {
                for column in &delete_file.equality_column_names {
                    required.insert(column.to_lowercase());
                }
                continue;
            }
            for field_id in &delete_file.equality_field_ids {
                let column = field_id_to_name.get(field_id).ok_or_else(|| {
                    format!(
                        "iceberg equality-delete file {} references unknown field id {} in table {}",
                        delete_file.path, field_id, table.name
                    )
                })?;
                required.insert(column.to_lowercase());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// PlanFragmentBuilder
// ---------------------------------------------------------------------------

pub(crate) struct PlanFragmentBuilder<'a> {
    catalog: &'a dyn CatalogProvider,
    current_database: &'a str,
    desc_builder: DescriptorTableBuilder,
    scan_tables: Vec<nodes::PlannedScanTable>,
    next_node_id: i32,
    next_slot_id: i32,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
    /// Fragment ids for current visit context. Top is active fragment id.
    fragment_stack: Vec<FragmentId>,
    /// Fragments finalized during visitation (child fragments from distribution
    /// boundaries and CTE produce fragments).
    completed_fragments: Vec<FragmentBuildResult>,
    /// Fragment-to-fragment stream/multicast edges.
    completed_edges: Vec<FragmentEdge>,
    /// CTE ID -> index in `completed_fragments`.
    cte_fragments: HashMap<CteId, usize>,
    /// tuple_id -> owning scan node and fragment (for RF target identification).
    pub(crate) scan_tuple_owners: HashMap<i32, ScanTupleOwner>,
    /// hash join node_id -> fragment_id for RF eligibility.
    pub(crate) join_fragment_map: HashMap<i32, FragmentId>,
    /// hash join node_id -> JoinDistribution for RF join mode mapping.
    pub(crate) join_distributions: HashMap<i32, crate::sql::optimizer::operator::JoinDistribution>,
}

impl<'a> PlanFragmentBuilder<'a> {
    // -------------------------------------------------------------------
    // Public entry
    // -------------------------------------------------------------------

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
            scan_tuple_owners: HashMap::new(),
            join_fragment_map: HashMap::new(),
            join_distributions: HashMap::new(),
        };

        // Elide a root-level Gather: on a single node the top-level gather
        // adds an unnecessary fragment boundary.
        let plan = match &plan.op {
            Operator::PhysicalDistribution(op)
                if matches!(
                    op.spec,
                    crate::sql::optimizer::property::DistributionSpec::Gather
                ) =>
            {
                plan.children
                    .first()
                    .ok_or_else(|| "root PhysicalDistribution(Gather) missing child".to_string())?
            }
            _ => plan,
        };

        let root_fragment_id = builder.alloc_fragment_id();
        builder.fragment_stack.push(root_fragment_id);
        let result = builder.visit(plan)?;

        // Build the shared descriptor table and exec params.  All fragments
        // share the same descriptor table and scan ranges since the
        // coordinator rewires instance IDs and sinks after the fact.
        let desc_tbl =
            std::mem::replace(&mut builder.desc_builder, DescriptorTableBuilder::new()).build();

        let exec_params = nodes::build_exec_params_multi(&builder.scan_tables)?;

        let output_columns = plan
            .output_columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        // Build the root fragment with a result sink.
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

        // Patch all completed (child) fragments with the shared descriptor
        // table and exec params.
        for frag in &mut builder.completed_fragments {
            frag.desc_tbl = desc_tbl.clone();
            frag.exec_params = exec_params.clone();
        }

        // Assemble all fragments: completed child fragments first, then root.
        let mut fragment_results = builder.completed_fragments;
        fragment_results.push(root_fragment);

        // Runtime filter planning pass: identify RF opportunities and patch
        // join nodes with TRuntimeFilterDescription.
        let pipeline_dop = std::thread::available_parallelism()
            .map(|p| p.get().min(4))
            .unwrap_or(4) as i32;
        let rf_plan = crate::sql::optimizer::runtime_filter_planner::plan_runtime_filters(
            &mut fragment_results,
            &builder.scan_tuple_owners,
            &builder.join_fragment_map,
            &builder.join_distributions,
            pipeline_dop,
        );
        let rf_plan = if rf_plan.all_filters.is_empty() {
            None
        } else {
            Some(rf_plan)
        };

        Ok(MultiFragmentBuildResult {
            fragment_results,
            root_fragment_id,
            edges: builder.completed_edges,
            rf_plan,
        })
    }

    // -------------------------------------------------------------------
    // ID allocators
    // -------------------------------------------------------------------

    fn alloc_node(&mut self) -> i32 {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    fn alloc_slot(&mut self) -> i32 {
        let id = self.next_slot_id;
        self.next_slot_id += 1;
        id
    }

    fn alloc_tuple(&mut self) -> i32 {
        let id = self.next_tuple_id;
        self.next_tuple_id += 1;
        id
    }

    fn alloc_fragment_id(&mut self) -> FragmentId {
        let id = self.next_fragment_id;
        self.next_fragment_id += 1;
        id
    }

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        self.fragment_stack
            .last()
            .copied()
            .ok_or_else(|| "no active fragment id in builder".to_string())
    }

    // -------------------------------------------------------------------
    // Dispatcher
    // -------------------------------------------------------------------

    fn visit(&mut self, node: &PhysicalPlanNode) -> Result<VisitResult, String> {
        match &node.op {
            Operator::PhysicalScan(op) => self.visit_scan(op, node),
            Operator::PhysicalFilter(op) => self.visit_filter(op, node),
            Operator::PhysicalProject(op) => self.visit_project(op, node),
            Operator::PhysicalHashJoin(op) => self.visit_hash_join(op, node),
            Operator::PhysicalNestLoopJoin(op) => self.visit_nest_loop_join(op, node),
            Operator::PhysicalHashAggregate(op) => self.visit_hash_aggregate(op, node),
            Operator::PhysicalSort(op) => self.visit_sort(op, node),
            Operator::PhysicalTopN(op) => self.visit_physical_top_n(op, node),
            Operator::PhysicalLimit(op) => self.visit_limit(op, node),
            Operator::PhysicalWindow(op) => self.visit_window(op, node),
            Operator::PhysicalValues(op) => self.visit_values(op, node),
            Operator::PhysicalGenerateSeries(op) => self.visit_generate_series(op, node),
            Operator::PhysicalTableFunction(op) => self.visit_table_function(op, node),
            Operator::PhysicalSubqueryAlias(op) => self.visit_subquery_alias(op, node),
            Operator::PhysicalRepeat(op) => self.visit_repeat(op, node),
            Operator::PhysicalDistribution(op) => self.visit_distribution(op, node),
            Operator::PhysicalCTEAnchor(op) => self.visit_cte_anchor(op, node),
            Operator::PhysicalCTEProduce(op) => self.visit_cte_produce(op, node),
            Operator::PhysicalCTEConsume(op) => self.visit_cte_consume(op),
            Operator::PhysicalUnion(op) => self.visit_union(op, node),
            Operator::PhysicalIntersect(op) => self.visit_intersect(op, node),
            Operator::PhysicalExcept(op) => self.visit_except(op, node),
            // Logical operators should never appear in an extracted physical plan
            other if other.is_logical() => Err(format!(
                "unexpected logical operator in physical plan: {:?}",
                other
            )),
            other => Err(format!(
                "unhandled operator in fragment builder: {:?}",
                other
            )),
        }
    }

    // -------------------------------------------------------------------
    // Conjunct splitting helper
    // -------------------------------------------------------------------

    fn split_and_compile_conjuncts(
        &self,
        predicate: &TypedExpr,
        scope: &ExprScope,
    ) -> Result<Vec<exprs::TExpr>, String> {
        let conjuncts = split_and_conjuncts_typed(predicate);
        let mut results = Vec::new();
        for conj in conjuncts {
            let mut compiler = ExprCompiler::new(scope);
            results.push(compiler.compile_typed(conj)?);
        }
        Ok(results)
    }

    // -------------------------------------------------------------------
    // visit_scan
    // -------------------------------------------------------------------

    fn visit_scan(
        &mut self,
        op: &PhysicalScanOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let scan_tuple_id = self.alloc_tuple();
        let scan_node_id = self.alloc_node();

        let mut scope = ExprScope::new();
        let qualifier = op.alias.as_deref().or(Some(&op.table.name));
        let mut slot_to_column = HashMap::new();

        // Determine which columns to emit
        let mut required: Option<std::collections::HashSet<String>> = op
            .required_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());
        if let Some(required) = required.as_mut() {
            add_iceberg_equality_delete_required_columns(required, &op.table)?;
        }

        let physical_layout = self
            .catalog
            .get_physical_layout(&op.database, &op.table.name)?;
        let scan_table_id = physical_layout
            .as_ref()
            .map(|layout| layout.table_id)
            .or_else(|| {
                op.table
                    .iceberg_table
                    .is_some()
                    .then_some(synthetic_iceberg_table_id(scan_node_id))
            });
        if let Some(table_id) = scan_table_id {
            self.desc_builder
                .add_table_for_scan(table_id, &op.database, &op.table);
        }

        for (idx, col) in op.table.columns.iter().enumerate() {
            if let Some(ref req) = required
                && !req.contains(&col.name.to_lowercase())
            {
                continue;
            }
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            scope.add_column(
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            // When alias differs from table name, also register with original table name
            if op
                .alias
                .as_deref()
                .is_some_and(|a| !a.eq_ignore_ascii_case(&op.table.name))
            {
                scope.add_column(Some(op.table.name.clone()), col.name.clone(), binding);
            }
        }

        // Iceberg V3 row-lineage pseudo-columns (_row_id,
        // _last_updated_sequence_number): register in ExprScope and emit as
        // output slots so that SELECT _row_id references resolve in codegen
        // and the slot flows through to the HDFS_SCAN_NODE tuple descriptor.
        // Lowering picks up the slot by name via `is_iceberg_row_id` /
        // `is_iceberg_last_updated_sequence_number` to populate
        // IcebergVirtualSpec.
        //
        // Note: these pseudo-columns are NOT in `scan.columns`, so the column
        // pruning rule never adds them to `required_columns`. Always register
        // them regardless of `required`; the lowering layer only synthesises
        // the values for slots that are actually in the tuple descriptor.
        let meta_col_offset = op.table.columns.len();
        for (meta_idx, col) in op
            .table
            .iceberg_row_lineage_metadata_columns
            .iter()
            .enumerate()
        {
            let col_pos = (meta_col_offset + meta_idx) as i32;
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                col_pos,
            );
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            scope.add_column(
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            if op
                .alias
                .as_deref()
                .is_some_and(|a| !a.eq_ignore_ascii_case(&op.table.name))
            {
                scope.add_column(Some(op.table.name.clone()), col.name.clone(), binding);
            }
        }

        // Compile predicates pushed down by the optimizer
        let pushed_conjuncts = if op.predicates.is_empty() {
            vec![]
        } else {
            let mut conjuncts = Vec::new();
            for pred in &op.predicates {
                let mut compiler = ExprCompiler::new(&scope);
                conjuncts.push(compiler.compile_typed(pred)?);
            }
            conjuncts
        };

        let resolved = ResolvedTable {
            database: op.database.clone(),
            table: op.table.clone(),
            physical_layout,
            alias: op.alias.clone(),
        };
        self.desc_builder.add_tuple(scan_tuple_id, scan_table_id);

        let scan_plan_node = nodes::build_scan_node(
            scan_node_id,
            scan_tuple_id,
            &resolved,
            pushed_conjuncts.clone(),
        );
        self.scan_tables.push(nodes::PlannedScanTable {
            scan_node_id,
            resolved,
            min_max_conjuncts: pushed_conjuncts,
            slot_to_column,
        });

        // Track tuple -> scan node ownership for runtime filter planning.
        let current_frag = self.current_fragment_id()?;
        self.scan_tuple_owners.insert(
            scan_tuple_id,
            ScanTupleOwner {
                scan_node_id,
                fragment_id: current_frag,
            },
        );

        Ok(VisitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
            cte_exchange_nodes: Vec::new(),
        })
    }

    // -------------------------------------------------------------------
    // visit_filter
    // -------------------------------------------------------------------

    fn visit_filter(
        &mut self,
        op: &PhysicalFilterOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let mut child = self.visit(&node.children[0])?;

        let conjuncts = self.split_and_compile_conjuncts(&op.predicate, &child.scope)?;

        if !conjuncts.is_empty() {
            // Push conjuncts onto the first (scan) node if it has none yet
            if let Some(scan) = child.plan_nodes.first_mut() {
                let scan_node_id = scan.node_id;
                let extra_conjuncts = conjuncts.clone();
                if scan.conjuncts.is_none() {
                    scan.conjuncts = Some(conjuncts);
                } else {
                    scan.conjuncts.as_mut().unwrap().extend(conjuncts);
                }
                nodes::append_hdfs_scan_min_max_conjuncts(scan, &extra_conjuncts);
                if let Some(planned) = self
                    .scan_tables
                    .iter_mut()
                    .find(|planned| planned.scan_node_id == scan_node_id)
                {
                    planned.min_max_conjuncts.extend(extra_conjuncts);
                }
            }
        }

        Ok(child)
    }

    // -------------------------------------------------------------------
    // visit_project
    // -------------------------------------------------------------------

    fn visit_project(
        &mut self,
        op: &PhysicalProjectOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let project_tuple_id = self.alloc_tuple();
        let project_node_id = self.alloc_node();

        let mut output_columns = Vec::new();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = self.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("project expr `{name}` compiled to empty TExpr"))?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                output_columns.len() as i32,
            );
            slot_map.insert(slot_id, texpr);
            output_columns.push(OutputColumn {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable,
            });

            project_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: project_tuple_id,
                    slot_id,
                    data_type: data_type.clone(),
                    type_desc: Some(slot_type_desc.clone()),
                    nullable,
                },
            );

            // Also register with qualifier if the expression is a column ref.
            // Use add_qualified_alias to avoid pushing a duplicate entry into
            // the ordered list (which would inflate iter_columns and break
            // UNION output slot counts).
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
            } = item.expr.kind
            {
                project_scope.add_qualified_alias(
                    q.clone(),
                    column.clone(),
                    ColumnBinding {
                        tuple_id: project_tuple_id,
                        slot_id,
                        data_type,
                        type_desc: Some(slot_type_desc),
                        nullable,
                    },
                );
            }
        }

        self.desc_builder.add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        // Pre-order: project first, then child nodes
        let mut plan_nodes = vec![project_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: project_scope,
            tuple_ids: vec![project_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_hash_join
    // -------------------------------------------------------------------

    fn visit_hash_join(
        &mut self,
        op: &PhysicalHashJoinOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let left = self.visit(&node.children[0])?;
        let right = self.visit(&node.children[1])?;

        let join_op = join_kind_to_op(op.join_type);
        let join_node_id = self.alloc_node();

        // Track join node -> fragment for runtime filter planning.
        if let Ok(frag_id) = self.current_fragment_id() {
            self.join_fragment_map.insert(join_node_id, frag_id);
        }
        self.join_distributions
            .insert(join_node_id, op.distribution.clone());

        // Compile eq conditions.  Pairs are pre-oriented by JoinToHashJoin so
        // that pair.0 references the left child and pair.1 references the right
        // in the common case.  However, orientation can fail when the same
        // column name appears in both children (e.g. self-join on a CTE) or
        // when logical_props is missing for a child group.  We therefore try
        // the natural order first, then the swapped order as a fallback, and
        // demote only when neither compiles successfully.
        let mut eq_join_conjuncts = Vec::new();
        let mut demoted_eq_exprs: Vec<crate::sql::analysis::TypedExpr> = Vec::new();
        for eq in &op.eq_conditions {
            let expr_a = &eq.left;
            let expr_b = &eq.right;
            // Try natural order: expr_a on left, expr_b on right.
            let natural = ExprCompiler::new(&left.scope)
                .compile_typed(expr_a)
                .ok()
                .and_then(|lt| {
                    ExprCompiler::new(&right.scope)
                        .compile_typed(expr_b)
                        .ok()
                        .map(|rt| (lt, rt))
                });
            // Try swapped order: expr_b on left, expr_a on right.
            // Needed when JoinCommutativity swapped children but the
            // eq_condition columns still reference the original order.
            let result = natural.or_else(|| {
                ExprCompiler::new(&left.scope)
                    .compile_typed(expr_b)
                    .ok()
                    .and_then(|lt| {
                        ExprCompiler::new(&right.scope)
                            .compile_typed(expr_a)
                            .ok()
                            .map(|rt| (lt, rt))
                    })
            });
            if let Some((lt, rt)) = result {
                eq_join_conjuncts.push(plan_nodes::TEqJoinCondition {
                    left: lt,
                    right: rt,
                    opcode: Some(if eq.null_safe {
                        crate::opcodes::TExprOpcode::EQ_FOR_NULL
                    } else {
                        crate::opcodes::TExprOpcode::EQ
                    }),
                });
            } else {
                // Both sides belong to the same child — demote to other_condition
                // compiled with a merged scope.
                demoted_eq_exprs.push(crate::sql::analysis::TypedExpr {
                    kind: crate::sql::analysis::ExprKind::BinaryOp {
                        left: Box::new(expr_a.clone()),
                        op: if eq.null_safe {
                            crate::sql::analysis::BinOp::EqForNull
                        } else {
                            crate::sql::analysis::BinOp::Eq
                        },
                        right: Box::new(expr_b.clone()),
                    },
                    data_type: arrow::datatypes::DataType::Boolean,
                    nullable: false,
                });
            }
        }

        // Compile other conditions (including any eq pairs demoted above).
        let mut other_join_conjuncts = Vec::new();
        {
            let mut merged = ExprScope::new();
            merged.merge(&left.scope);
            merged.merge(&right.scope);
            let mut compiler = ExprCompiler::new(&merged);
            for demoted in &demoted_eq_exprs {
                other_join_conjuncts.push(compiler.compile_typed(demoted)?);
            }
            if let Some(ref cond) = op.other_condition {
                other_join_conjuncts.push(compiler.compile_typed(cond)?);
            }
        }

        let join_plan_node = nodes::build_hash_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            eq_join_conjuncts,
            other_join_conjuncts,
        );

        // Widen nullable flags on the join's null-producing side(s). Note: this
        // is the tuple-level widening needed by the descriptor table and the
        // runtime's null-padding for SEMI/ANTI pruned columns. The authoritative
        // source of column-level nullability is `node.output_columns`, populated
        // by stats::derive_output_columns via widen_for_join_kind. This match
        // intentionally mirrors that widening at the tuple level — a per-slot
        // nullability mechanism would let us drive both from output_columns,
        // but is out of scope here.
        match op.join_type {
            JoinKind::LeftOuter | JoinKind::LeftAnti | JoinKind::LeftSemi => {
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightOuter | JoinKind::RightAnti | JoinKind::RightSemi => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::FullOuter => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }

        // tuple_ids always includes both sides — the join node's row_tuples
        // must reference all probe and build tuples.
        let mut merged_tuple_ids = left.tuple_ids.clone();
        merged_tuple_ids.extend(&right.tuple_ids);

        // Output scope: SEMI/ANTI joins only expose the surviving side's
        // columns to downstream operators (preventing stale column
        // references when multiple SEMI joins are chained).
        let merged_scope = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => left.scope,
            JoinKind::RightSemi | JoinKind::RightAnti => right.scope,
            _ => {
                let mut scope = left.scope;
                scope.merge(&right.scope);
                scope
            }
        };

        // Pre-order: join node, then left subtree, then right subtree
        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);
        let mut cte_exchange_nodes = left.cte_exchange_nodes;
        cte_exchange_nodes.extend(right.cte_exchange_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
            cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_nest_loop_join
    // -------------------------------------------------------------------

    fn visit_nest_loop_join(
        &mut self,
        op: &PhysicalNestLoopJoinOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let left = self.visit(&node.children[0])?;
        let right = self.visit(&node.children[1])?;

        let join_op = join_kind_to_op(op.join_type);
        let join_node_id = self.alloc_node();

        let join_conjuncts = if let Some(ref cond) = op.condition {
            let mut merged = ExprScope::new();
            merged.merge(&left.scope);
            merged.merge(&right.scope);
            let conjuncts = split_and_conjuncts_typed(cond);
            let mut results = Vec::new();
            for conj in conjuncts {
                let mut compiler = ExprCompiler::new(&merged);
                results.push(compiler.compile_typed(conj)?);
            }
            results
        } else {
            vec![]
        };

        let join_plan_node = nodes::build_nestloop_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            join_conjuncts,
        );

        // Widen nullable for outer/anti join nullable side tuples.
        match op.join_type {
            JoinKind::LeftOuter | JoinKind::LeftAnti => {
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightOuter | JoinKind::RightAnti => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::FullOuter => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }

        // tuple_ids always includes both sides for the join node.
        let mut merged_tuple_ids = left.tuple_ids.clone();
        merged_tuple_ids.extend(&right.tuple_ids);

        // Output scope: SEMI/ANTI only expose surviving side.
        let merged_scope = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => left.scope,
            JoinKind::RightSemi | JoinKind::RightAnti => right.scope,
            _ => {
                let mut scope = left.scope;
                scope.merge(&right.scope);
                scope
            }
        };

        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);
        let mut cte_exchange_nodes = left.cte_exchange_nodes;
        cte_exchange_nodes.extend(right.cte_exchange_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
            cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_hash_aggregate
    // -------------------------------------------------------------------

    fn visit_hash_aggregate(
        &mut self,
        op: &PhysicalHashAggregateOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;
        let need_finalize = matches!(op.mode, AggMode::Single | AggMode::Global);

        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions (same for all modes — the child scope
        // has the correct columns for both scan-level and Local-output contexts).
        for (idx, gb_expr) in op.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = self.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("group by expr `{name}` compiled to empty TExpr"))?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                type_desc: Some(slot_type_desc),
                nullable,
            };
            agg_scope.add_column(None, name, binding.clone());
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
            } = gb_expr.kind
            {
                agg_scope.add_qualified_alias(q.clone(), column.clone(), binding);
            }
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions — mode-dependent.
        let agg_start_col = op.group_by.len();
        let mut aggregate_functions = Vec::new();

        debug_assert_eq!(
            op.is_merge.len(),
            op.aggregates.len(),
            "PhysicalHashAggregate (node_id={}): is_merge.len() = {}, aggregates.len() = {}",
            agg_node_id,
            op.is_merge.len(),
            op.aggregates.len(),
        );

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if op.is_merge[idx] {
                // Global (merge) phase: the child scope contains the Local's
                // output.  Each intermediate aggregate column sits at position
                // group_by.len() + idx in the child scope's ordered columns.
                let child_columns: Vec<_> = child.scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Global agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                // Single or Local: compile against child scope normally.
                let mut compiler = ExprCompiler::new(&child.scope);
                compiler.compile_aggregate_call_typed(agg_call).map_err(|err| {
                    let available = child
                        .scope
                        .iter_columns()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "failed to compile aggregate `{}` in {:?} mode against child scope [{}]: {}",
                        agg_call_display_name(agg_call),
                        op.mode,
                        available,
                        err
                    )
                })?
            };

            let data_type = if need_finalize {
                agg_call.result_type.clone()
            } else {
                texpr
                    .nodes
                    .first()
                    .and_then(|root| root.fn_.as_ref())
                    .and_then(|func| func.aggregate_fn.as_ref())
                    .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type))
                    .unwrap_or_else(|| agg_call.result_type.clone())
            };
            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let slot_id = self.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            let slot_type_desc = if need_finalize {
                texpr
                    .nodes
                    .first()
                    .map(|root| root.type_.clone())
                    .ok_or_else(|| format!("aggregate `{name}` compiled to empty TExpr"))?
            } else {
                texpr
                    .nodes
                    .first()
                    .and_then(|root| root.fn_.as_ref())
                    .and_then(|func| func.aggregate_fn.as_ref())
                    .map(|agg_fn| agg_fn.intermediate_type.clone())
                    .unwrap_or_else(|| {
                        texpr
                            .nodes
                            .first()
                            .map(|root| root.type_.clone())
                            .unwrap_or_else(|| {
                                crate::lower::thrift::type_lowering::scalar_type_desc(
                                    crate::types::TPrimitiveType::NULL_TYPE,
                                )
                            })
                    })
            };
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                col_pos,
            );
            agg_scope.add_column(
                None,
                name,
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type,
                    type_desc: Some(slot_type_desc),
                    nullable,
                },
            );
            aggregate_functions.push(texpr);
        }

        self.desc_builder.add_tuple(agg_tuple_id, None);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            need_finalize,
        );

        // Pre-order: agg first, then child nodes
        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_sort
    // -------------------------------------------------------------------

    fn visit_sort(
        &mut self,
        op: &PhysicalSortOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let sort_node_id = self.alloc_node();
        let sort_tuple_id = *child.tuple_ids.last().unwrap();

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = -1;
        sort_plan_node.row_tuples = vec![sort_tuple_id];
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: false,
            offset: None,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        // Pre-order: sort first, then child
        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_physical_top_n — Sort + Limit as a single operator
    // -------------------------------------------------------------------

    fn visit_physical_top_n(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::optimizer::operator::TopNPhase;
        match (op.phase, op.is_split) {
            // Single-stage (today's behavior) and PARTIAL both emit a single
            // SORT_NODE and return. PARTIAL's output is consumed by the
            // FINAL+split visitor without a fragment boundary.
            (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {
                self.visit_physical_top_n_single_or_partial(op, node)
            }
            // FINAL+split: adds a fragment boundary + merging EXCHANGE_NODE.
            (TopNPhase::Final, true) => self.visit_physical_top_n_final_split(op, node),
        }
    }

    fn visit_physical_top_n_single_or_partial(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let sort_node_id = self.alloc_node();
        let sort_tuple_id = *child.tuple_ids.last().unwrap();

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = op.limit.unwrap_or(-1);
        sort_plan_node.row_tuples = vec![sort_tuple_id];
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: true,
            offset: op.offset,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    /// FINAL+split TopN: close the partial fragment (ending in a SORT_NODE) and
    /// start a coordinator fragment whose root is a merging EXCHANGE_NODE. The
    /// receive side does the k-way merge and applies offset/limit — no final
    /// SORT_NODE is needed because the pre-sorted input streams already give
    /// the merged output its order.
    fn visit_physical_top_n_final_split(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let VisitResult {
            plan_nodes: child_plan_nodes,
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes,
        } = child;

        // PARTIAL should have emitted a SORT_NODE at the head.
        let partial_sort_info = child_plan_nodes
            .first()
            .and_then(|n| n.sort_node.as_ref())
            .map(|s| s.sort_info.clone())
            .ok_or_else(|| {
                let got = child_plan_nodes
                    .first()
                    .map(|n| format!("{:?}", n.node_type))
                    .unwrap_or_else(|| "<empty>".to_string());
                format!(
                    "FINAL+split TopN (node_id={}): expected PARTIAL child's root to be SORT_NODE, got {}",
                    child_plan_nodes
                        .first()
                        .map(|n| n.node_id)
                        .unwrap_or(-1),
                    got
                )
            })?;

        // Close the partial fragment with Unpartitioned/Gather sender into the merging exchange.
        let gather_spec = crate::sql::optimizer::property::DistributionSpec::Gather;
        let output_partition = self.build_output_partition(&gather_spec, &child_scope)?;
        let exchange_partition_type = output_partition.type_;

        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(child_plan_nodes),
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
            cte_exchange_nodes,
        });

        let exchange_node_id = self.alloc_node();
        let exchange_node = nodes::build_merging_exchange_node(
            exchange_node_id,
            child_tuple_ids.clone(),
            exchange_partition_type,
            partial_sort_info,
            op.limit,
            op.offset,
        );

        self.completed_edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            edge_kind: FragmentEdgeKind::Stream,
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes: Vec::new(),
        })
    }

    // -------------------------------------------------------------------
    // visit_limit
    // -------------------------------------------------------------------

    fn visit_limit(
        &mut self,
        op: &PhysicalLimitOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let mut child = self.visit(&node.children[0])?;

        if let Some(top) = child.plan_nodes.first_mut() {
            if top.node_type == plan_nodes::TPlanNodeType::SORT_NODE {
                top.limit = op.limit.unwrap_or(-1);
                let sort_node = top
                    .sort_node
                    .as_mut()
                    .ok_or_else(|| "SORT_NODE missing sort payload".to_string())?;
                sort_node.offset = op.offset;
            } else {
                if let Some(limit) = op.limit {
                    top.limit = limit;
                }
                if op.offset.is_some() {
                    return Err("LIMIT/OFFSET without a SORT child is not supported".to_string());
                }
            }
        }

        Ok(child)
    }

    // -------------------------------------------------------------------
    // visit_window
    // -------------------------------------------------------------------

    fn visit_window(
        &mut self,
        op: &PhysicalWindowOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::analysis::{WindowBound, WindowFrameType};

        // Group window expressions by (partition_by, order_by) signature.
        // Different signatures need separate Sort + Analytic nodes.
        let groups = crate::sql::codegen::helpers::group_win_exprs_by_sig(&op.window_exprs);
        if groups.len() > 1 {
            return self.visit_window_multi_group(op, node, &groups);
        }

        let child = self.visit(&node.children[0])?;
        let analytic_node_id = self.alloc_node();

        let intermediate_tuple_id = self.alloc_tuple();
        let output_tuple_id = self.alloc_tuple();

        // Compile partition_by and order_by from the first window expr
        let first_win = op.window_exprs.first().ok_or("empty window_exprs")?;

        let mut partition_exprs = Vec::new();
        for expr in &first_win.partition_by {
            let mut compiler = ExprCompiler::new(&child.scope);
            partition_exprs.push(compiler.compile_typed(expr)?);
        }

        let mut order_by_exprs = Vec::new();
        for item in &first_win.order_by {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            order_by_exprs.push(texpr);
        }

        // Compile analytic functions
        let mut analytic_functions = Vec::new();
        for win_expr in &op.window_exprs {
            let mut compiler = ExprCompiler::new(&child.scope);
            let agg_call = AggregateCall {
                name: win_expr.name.clone(),
                args: win_expr.args.clone(),
                distinct: win_expr.distinct,
                result_type: win_expr.result_type.clone(),
                order_by: vec![],
            };
            let mut texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
            apply_ignore_nulls_to_root_fn(&mut texpr, win_expr.ignore_nulls);
            analytic_functions.push(texpr);
        }

        // Register intermediate slots
        for (idx, win_expr) in op.window_exprs.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                intermediate_tuple_id,
                &format!("__win_intermediate_{idx}"),
                &win_expr.result_type,
                true,
                idx as i32,
            );
        }
        self.desc_builder.add_tuple(intermediate_tuple_id, None);

        // Register output slots. Forward the child's full scope (both
        // qualified and unqualified) so post-window references like
        // `<table>.col` (produced by `SELECT *` expansion under FROM <table>)
        // still resolve.
        let mut output_scope = ExprScope::new();
        output_scope.merge(&child.scope);
        for (idx, win_expr) in op.window_exprs.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &win_expr.output_name,
                &win_expr.result_type,
                true,
                idx as i32,
            );
            output_scope.add_column(
                None,
                win_expr.output_name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: win_expr.result_type.clone(),
                    type_desc: None,
                    nullable: true,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        // Window frame
        let window = first_win.window_frame.as_ref().map(|frame| {
            let window_type = match frame.frame_type {
                WindowFrameType::Rows => plan_nodes::TAnalyticWindowType::ROWS,
                WindowFrameType::Range => plan_nodes::TAnalyticWindowType::RANGE,
            };
            let window_start = match &frame.start {
                WindowBound::UnboundedPreceding => None,
                WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                    range_offset_predicate: None,
                    rows_offset_value: None,
                }),
                WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::UnboundedFollowing => None,
            };
            let window_end = match &frame.end {
                WindowBound::UnboundedFollowing => None,
                WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                    range_offset_predicate: None,
                    rows_offset_value: None,
                }),
                WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::UnboundedPreceding => None,
            };
            plan_nodes::TAnalyticWindow {
                type_: window_type,
                window_start,
                window_end,
            }
        });

        // Build TAnalyticNode
        let analytic_tnode = plan_nodes::TAnalyticNode {
            partition_exprs,
            order_by_exprs,
            analytic_functions,
            window,
            intermediate_tuple_id,
            output_tuple_id,
            buffered_tuple_id: None,
            partition_by_eq: None,
            order_by_eq: None,
            sql_partition_keys: None,
            sql_aggregate_functions: None,
            has_outer_join_child: None,
            use_hash_based_partition: None,
            is_skewed: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = analytic_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        let mut row_tuples = child.tuple_ids.clone();
        row_tuples.push(output_tuple_id);
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.analytic_node = Some(analytic_tnode);

        // Pre-order: analytic node first, then child
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_window_multi_group
    // -------------------------------------------------------------------

    /// Handle window functions with multiple different partition/order signatures.
    /// Each group gets its own Sort + Analytic node, chained sequentially within
    /// the same fragment (no cross-group exchanges).
    fn visit_window_multi_group(
        &mut self,
        op: &PhysicalWindowOp,
        node: &PhysicalPlanNode,
        groups: &[Vec<usize>],
    ) -> Result<VisitResult, String> {
        use crate::sql::analysis::{WindowBound, WindowFrameType};

        let mut current = self.visit(&node.children[0])?;

        for group_indices in groups {
            let group_exprs: Vec<_> = group_indices
                .iter()
                .map(|&i| op.window_exprs[i].clone())
                .collect();
            let first_win = &group_exprs[0];

            // Build Sort node for this group's partition+order
            let mut sort_ordering = Vec::new();
            let mut sort_is_asc = Vec::new();
            let mut sort_nulls_first_list = Vec::new();
            for expr in &first_win.partition_by {
                let mut compiler = ExprCompiler::new(&current.scope);
                sort_ordering.push(compiler.compile_typed(expr)?);
                sort_is_asc.push(true);
                sort_nulls_first_list.push(true);
            }
            for item in &first_win.order_by {
                let mut compiler = ExprCompiler::new(&current.scope);
                sort_ordering.push(compiler.compile_typed(&item.expr)?);
                sort_is_asc.push(item.asc);
                sort_nulls_first_list.push(item.nulls_first);
            }

            if !sort_ordering.is_empty() {
                let sort_node_id = self.alloc_node();
                let sort_plan = nodes::build_sort_node_raw(
                    sort_node_id,
                    current.tuple_ids.clone(),
                    sort_ordering,
                    sort_is_asc,
                    sort_nulls_first_list,
                    -1,
                    None,
                );
                let mut pnodes = vec![sort_plan];
                pnodes.extend(current.plan_nodes);
                current.plan_nodes = pnodes;
            }

            // Build Analytic node for this group
            let analytic_node_id = self.alloc_node();
            let intermediate_tuple_id = self.alloc_tuple();
            let output_tuple_id = self.alloc_tuple();

            let mut partition_exprs = Vec::new();
            for expr in &first_win.partition_by {
                let mut compiler = ExprCompiler::new(&current.scope);
                partition_exprs.push(compiler.compile_typed(expr)?);
            }
            let mut order_by_exprs = Vec::new();
            for item in &first_win.order_by {
                let mut compiler = ExprCompiler::new(&current.scope);
                order_by_exprs.push(compiler.compile_typed(&item.expr)?);
            }

            let mut analytic_functions = Vec::new();
            for win_expr in &group_exprs {
                let mut compiler = ExprCompiler::new(&current.scope);
                let agg_call = AggregateCall {
                    name: win_expr.name.clone(),
                    args: win_expr.args.clone(),
                    distinct: win_expr.distinct,
                    result_type: win_expr.result_type.clone(),
                    order_by: vec![],
                };
                let mut texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
                apply_ignore_nulls_to_root_fn(&mut texpr, win_expr.ignore_nulls);
                analytic_functions.push(texpr);
            }

            for (idx, win_expr) in group_exprs.iter().enumerate() {
                let slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    slot_id,
                    intermediate_tuple_id,
                    &format!("__win_intermediate_{idx}"),
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
            }
            self.desc_builder.add_tuple(intermediate_tuple_id, None);

            let mut output_scope = ExprScope::new();
            output_scope.merge(&current.scope);
            for (idx, win_expr) in group_exprs.iter().enumerate() {
                let slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    slot_id,
                    output_tuple_id,
                    &win_expr.output_name,
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
                output_scope.add_column(
                    None,
                    win_expr.output_name.clone(),
                    ColumnBinding {
                        tuple_id: output_tuple_id,
                        slot_id,
                        data_type: win_expr.result_type.clone(),
                        type_desc: None,
                        nullable: true,
                    },
                );
            }
            self.desc_builder.add_tuple(output_tuple_id, None);

            let window = first_win.window_frame.as_ref().map(|frame| {
                let window_type = match frame.frame_type {
                    WindowFrameType::Rows => plan_nodes::TAnalyticWindowType::ROWS,
                    WindowFrameType::Range => plan_nodes::TAnalyticWindowType::RANGE,
                };
                let window_start = match &frame.start {
                    WindowBound::UnboundedPreceding => None,
                    WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                        range_offset_predicate: None,
                        rows_offset_value: None,
                    }),
                    WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::UnboundedFollowing => None,
                };
                let window_end = match &frame.end {
                    WindowBound::UnboundedFollowing => None,
                    WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                        range_offset_predicate: None,
                        rows_offset_value: None,
                    }),
                    WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::UnboundedPreceding => None,
                };
                plan_nodes::TAnalyticWindow {
                    type_: window_type,
                    window_start,
                    window_end,
                }
            });

            let analytic_tnode = plan_nodes::TAnalyticNode {
                partition_exprs,
                order_by_exprs,
                analytic_functions,
                window,
                intermediate_tuple_id,
                output_tuple_id,
                buffered_tuple_id: None,
                partition_by_eq: None,
                order_by_eq: None,
                sql_partition_keys: None,
                sql_aggregate_functions: None,
                has_outer_join_child: None,
                use_hash_based_partition: None,
                is_skewed: None,
            };

            let mut plan_node = nodes::default_plan_node();
            plan_node.node_id = analytic_node_id;
            plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
            plan_node.num_children = 1;
            plan_node.limit = -1;
            let mut new_tuple_ids = current.tuple_ids.clone();
            new_tuple_ids.push(output_tuple_id);
            plan_node.row_tuples = new_tuple_ids.clone();
            plan_node.nullable_tuples = vec![];
            plan_node.analytic_node = Some(analytic_tnode);

            let mut pnodes = vec![plan_node];
            pnodes.extend(current.plan_nodes);
            let cte_exchange_nodes = current.cte_exchange_nodes.clone();
            current = VisitResult {
                plan_nodes: pnodes,
                scope: output_scope,
                tuple_ids: new_tuple_ids,
                cte_exchange_nodes,
            };
        }

        Ok(current)
    }

    // -------------------------------------------------------------------
    // visit_values
    // -------------------------------------------------------------------

    fn visit_values(
        &mut self,
        op: &PhysicalValuesOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let output_tuple_id = self.alloc_tuple();
        let values_node_id = self.alloc_node();

        let mut scope = ExprScope::new();
        for (idx, col) in op.columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            scope.add_column(
                None,
                col.name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: col.data_type.clone(),
                    type_desc: None,
                    nullable: col.nullable,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        let empty_scope = ExprScope::new();
        let mut const_expr_lists = Vec::with_capacity(op.rows.len());
        for row in &op.rows {
            if row.len() != op.columns.len() {
                return Err(format!(
                    "VALUES row column count mismatch: expected {}, got {}",
                    op.columns.len(),
                    row.len()
                ));
            }
            let mut exprs = Vec::with_capacity(row.len());
            for expr in row {
                let mut compiler = ExprCompiler::new(&empty_scope);
                exprs.push(compiler.compile_typed(expr)?);
            }
            const_expr_lists.push(exprs);
        }

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = values_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::UNION_NODE;
        plan_node.num_children = 0;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];
        plan_node.union_node = Some(plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists: vec![],
            const_expr_lists,
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        });

        Ok(VisitResult {
            plan_nodes: vec![plan_node],
            scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes: Vec::new(),
        })
    }

    // -------------------------------------------------------------------
    // visit_generate_series
    // -------------------------------------------------------------------

    fn visit_generate_series(
        &mut self,
        op: &PhysicalGenerateSeriesOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Generate the series values
        let mut values = Vec::new();
        let mut v = op.start;
        if op.step > 0 {
            while v <= op.end {
                values.push(v);
                v += op.step;
            }
        } else {
            while v >= op.end {
                values.push(v);
                v += op.step;
            }
        }

        // Build a temporary parquet file
        let col_name = &op.column_name;
        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            ArrowDataType::Int64,
            false,
        )]));
        let col_array = Arc::new(Int64Array::from(values));
        let batch = RecordBatch::try_new(schema, vec![col_array])
            .map_err(|e| format!("build generate_series batch failed: {e}"))?;

        let dir = std::env::temp_dir().join("novarocks_generate_series");
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create generate_series dir failed: {e}"))?;
        let path = dir.join(format!(
            "gs_{}_{}_{}_{}.parquet",
            op.start, op.end, op.step, self.next_node_id
        ));
        crate::sql::codegen::write_parquet_to_path(&path, &batch)?;

        // Build a TableDef and emit as a scan
        let table_def = crate::sql::catalog::TableDef {
            name: op.alias.as_deref().unwrap_or("generate_series").to_string(),
            columns: vec![crate::sql::catalog::ColumnDef {
                name: col_name.clone(),
                data_type: ArrowDataType::Int64,
                nullable: false,
                write_default: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: None,
            storage: crate::sql::catalog::TableStorage::LocalParquetFile { path },
        };

        // Create a PhysicalScanOp and delegate to visit_scan
        let scan_op = PhysicalScanOp {
            database: self.current_database.to_string(),
            table: table_def,
            alias: op.alias.clone(),
            columns: vec![crate::sql::analysis::OutputColumn {
                name: col_name.clone(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        };

        // Use a dummy node (visit_scan only reads the op, not the children)
        self.visit_scan(&scan_op, _node)
    }

    // -------------------------------------------------------------------
    // visit_table_function
    // -------------------------------------------------------------------

    fn visit_table_function(
        &mut self,
        op: &PhysicalTableFunctionOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if !op.function_name.eq_ignore_ascii_case("unnest") {
            return Err(format!(
                "unsupported standalone table function: {}",
                op.function_name
            ));
        }
        if node.children.len() != 1 {
            return Err(format!(
                "table function expects one child, got {}",
                node.children.len()
            ));
        }
        if op.args.len() != op.output_columns.len() {
            return Err(format!(
                "table function output column count mismatch: args={} outputs={}",
                op.args.len(),
                op.output_columns.len()
            ));
        }

        let child = self.visit(&node.children[0])?;

        let project_tuple_id = self.alloc_tuple();
        let project_node_id = self.alloc_node();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();
        let mut remapped_child_bindings = HashMap::new();
        let mut outer_columns = Vec::new();
        let mut outer_slots = Vec::new();

        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();
        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let slot_id = self.alloc_slot();
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                name,
                type_desc.clone(),
                binding.nullable,
                idx as i32,
            );
            slot_map.insert(
                slot_id,
                expr_compiler::build_slot_ref_texpr(
                    binding.slot_id,
                    binding.tuple_id,
                    type_desc.clone(),
                ),
            );
            let new_binding = ColumnBinding {
                tuple_id: project_tuple_id,
                slot_id,
                data_type: binding.data_type.clone(),
                type_desc: Some(type_desc),
                nullable: binding.nullable,
            };
            project_scope.add_column(None, name.clone(), new_binding.clone());
            remapped_child_bindings
                .insert((binding.tuple_id, binding.slot_id), new_binding.clone());
            outer_slots.push(slot_id);
            outer_columns.push((name.clone(), new_binding));
        }
        for (qualifier, name, binding) in child.scope.iter_qualified() {
            if let Some(new_binding) =
                remapped_child_bindings.get(&(binding.tuple_id, binding.slot_id))
            {
                project_scope.add_qualified_alias(
                    qualifier.clone(),
                    name.clone(),
                    new_binding.clone(),
                );
            }
        }

        let mut param_slots = Vec::with_capacity(op.args.len());
        let mut param_type_descs = Vec::with_capacity(op.args.len());
        for (idx, arg) in op.args.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(arg)?;
            let type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("table function arg {idx} compiled to empty TExpr"))?;
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                &format!("__tf_arg_{idx}"),
                type_desc.clone(),
                arg.nullable,
                (child_cols.len() + idx) as i32,
            );
            slot_map.insert(slot_id, texpr);
            param_slots.push(slot_id);
            param_type_descs.push(type_desc);
        }
        self.desc_builder.add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        let output_tuple_id = self.alloc_tuple();
        let table_fn_node_id = self.alloc_node();
        let mut output_scope = ExprScope::new();
        let mut output_outer_by_project_slot = HashMap::new();

        for (idx, (name, binding)) in outer_columns.iter().enumerate() {
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            self.desc_builder.add_slot_with_type_desc(
                binding.slot_id,
                output_tuple_id,
                name,
                type_desc.clone(),
                binding.nullable,
                idx as i32,
            );
            let output_binding = ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id: binding.slot_id,
                data_type: binding.data_type.clone(),
                type_desc: Some(type_desc),
                nullable: binding.nullable,
            };
            output_scope.add_column(None, name.clone(), output_binding.clone());
            output_outer_by_project_slot.insert(binding.slot_id, output_binding);
        }
        for (qualifier, name, binding) in project_scope.iter_qualified() {
            if let Some(output_binding) = output_outer_by_project_slot.get(&binding.slot_id) {
                output_scope.add_qualified_alias(
                    qualifier.clone(),
                    name.clone(),
                    output_binding.clone(),
                );
            }
        }

        let mut fn_result_slots = Vec::with_capacity(op.output_columns.len());
        let mut ret_type_descs = Vec::with_capacity(op.output_columns.len());
        let result_qualifier = op.alias.clone().or_else(|| Some(op.function_name.clone()));
        for (idx, col) in op.output_columns.iter().enumerate() {
            let DataType::List(item_field) = &op.args[idx].data_type else {
                return Err(format!(
                    "UNNEST argument {} must be ARRAY, got {:?}",
                    idx + 1,
                    op.args[idx].data_type
                ));
            };
            if item_field.data_type() != &col.data_type {
                return Err(format!(
                    "UNNEST result type mismatch for column {}: arg item={:?} output={:?}",
                    col.name,
                    item_field.data_type(),
                    col.data_type
                ));
            }
            let slot_id = self.alloc_slot();
            let type_desc = type_infer::arrow_type_to_type_desc(&col.data_type)?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                output_tuple_id,
                &col.name,
                type_desc.clone(),
                true,
                (outer_columns.len() + idx) as i32,
            );
            let binding = ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: Some(type_desc.clone()),
                nullable: true,
            };
            output_scope.add_column(result_qualifier.clone(), col.name.clone(), binding);
            fn_result_slots.push(slot_id);
            ret_type_descs.push(type_desc);
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        let ret_type = ret_type_descs
            .first()
            .cloned()
            .ok_or_else(|| "table function requires at least one return type".to_string())?;
        let table_function_expr = exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: ret_type.clone(),
            num_children: 0,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: op.function_name.clone(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: param_type_descs,
                ret_type,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: Some(types::TTableFunction::new(
                    ret_type_descs,
                    None::<String>,
                    Some(op.is_left_join),
                )),
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..expr_compiler::default_expr_node()
        }]);

        let mut table_fn_plan_node = nodes::default_plan_node();
        table_fn_plan_node.node_id = table_fn_node_id;
        table_fn_plan_node.node_type = plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE;
        table_fn_plan_node.num_children = 1;
        table_fn_plan_node.limit = -1;
        table_fn_plan_node.row_tuples = vec![output_tuple_id];
        table_fn_plan_node.nullable_tuples = vec![];
        table_fn_plan_node.compact_data = true;
        table_fn_plan_node.table_function_node = Some(plan_nodes::TTableFunctionNode::new(
            Some(table_function_expr),
            Some(param_slots),
            Some(outer_slots),
            Some(fn_result_slots),
            Some(true),
        ));

        let mut plan_nodes = vec![table_fn_plan_node, project_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_subquery_alias
    // -------------------------------------------------------------------

    fn visit_subquery_alias(
        &mut self,
        op: &PhysicalSubqueryAliasOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let mut child = self.visit(&node.children[0])?;
        let child_output_bindings: Vec<_> = child
            .scope
            .iter_columns()
            .map(|(_, binding)| binding.clone())
            .collect();

        // Register all output columns with the alias as qualifier
        for (idx, col) in op.output_columns.iter().enumerate() {
            let col_name_lower = col.name.to_lowercase();
            let binding = child
                .scope
                .resolve_column(None, &col_name_lower)
                .cloned()
                .or_else(|_| {
                    child_output_bindings.get(idx).cloned().ok_or_else(|| {
                        format!(
                            "subquery alias '{}' exposes column '{}' at position {} but child has only {} columns",
                            op.alias,
                            col.name,
                            idx,
                            child_output_bindings.len()
                        )
                    })
                })?;
            child
                .scope
                .add_column(Some(op.alias.clone()), col.name.clone(), binding);
        }

        Ok(child)
    }

    // -------------------------------------------------------------------
    // visit_repeat
    // -------------------------------------------------------------------

    fn visit_repeat(
        &mut self,
        op: &PhysicalRepeatOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let repeat_node_id = self.alloc_node();

        let has_grouping_fns = !op.grouping_fn_args.is_empty();
        let virtual_tuple_id = self.alloc_tuple();

        // Collect child columns for rollup slot mapping
        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        // Start with the child's full scope
        let mut output_scope = child.scope;

        for (original_name, alias_name) in &op.grouping_key_aliases {
            if let Ok(binding) = output_scope.resolve_column(None, alias_name) {
                output_scope.add_qualified_alias(
                    "__repeat_group".to_string(),
                    original_name.clone(),
                    binding.clone(),
                );
            }
        }

        // Add virtual slots
        let num_virtual = 1 + op.grouping_fn_args.len();
        let mut virtual_slot_ids = Vec::with_capacity(num_virtual);

        let grouping_id_slot = self.alloc_slot();
        self.desc_builder.add_slot(
            grouping_id_slot,
            virtual_tuple_id,
            "__grouping_id",
            &DataType::Int64,
            false,
            0,
        );
        if !op.grouping_fn_args.is_empty() {
            output_scope.add_column(
                None,
                "__grouping_id".to_string(),
                ColumnBinding {
                    tuple_id: virtual_tuple_id,
                    slot_id: grouping_id_slot,
                    data_type: DataType::Int64,
                    type_desc: None,
                    nullable: false,
                },
            );
        }
        virtual_slot_ids.push(grouping_id_slot);

        for (fn_idx, (fn_name, _)) in op.grouping_fn_args.iter().enumerate() {
            let slot = self.alloc_slot();
            self.desc_builder.add_slot(
                slot,
                virtual_tuple_id,
                fn_name,
                &DataType::Int64,
                false,
                1 + fn_idx as i32,
            );
            output_scope.add_column(
                None,
                fn_name.clone(),
                ColumnBinding {
                    tuple_id: virtual_tuple_id,
                    slot_id: slot,
                    data_type: DataType::Int64,
                    type_desc: None,
                    nullable: false,
                },
            );
            virtual_slot_ids.push(slot);
        }

        self.desc_builder.add_tuple(virtual_tuple_id, None);

        // Build slot_id_set_list and all_rollup_slot_ids
        let all_rollup_slot_ids: BTreeSet<i32> = op
            .all_rollup_columns
            .iter()
            .filter_map(|col| {
                child_cols.iter().find_map(|(name, binding)| {
                    if name.to_lowercase() == col.to_lowercase() {
                        Some(binding.slot_id)
                    } else {
                        None
                    }
                })
            })
            .collect();

        let slot_id_set_list: Vec<BTreeSet<i32>> = op
            .repeat_column_ref_list
            .iter()
            .map(|non_null_cols| {
                non_null_cols
                    .iter()
                    .filter_map(|col| {
                        child_cols.iter().find_map(|(name, binding)| {
                            if name.to_lowercase() == col.to_lowercase() {
                                Some(binding.slot_id)
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            })
            .collect();

        // Build grouping_list
        let repeat_times = op.grouping_ids.len();
        let mut grouping_list: Vec<Vec<i64>> = Vec::with_capacity(num_virtual);

        grouping_list.push(op.grouping_ids.iter().map(|g| *g as i64).collect());

        for (_fn_name, fn_args) in &op.grouping_fn_args {
            let mut values = Vec::with_capacity(repeat_times);
            for non_null_cols in &op.repeat_column_ref_list {
                let mut bits: u64 = 0;
                for (bit_pos, arg_col) in fn_args.iter().enumerate() {
                    let is_null = !non_null_cols
                        .iter()
                        .any(|c| c.to_lowercase() == arg_col.to_lowercase());
                    if is_null {
                        let reverse_bit_pos = fn_args.len() - 1 - bit_pos;
                        bits |= 1 << reverse_bit_pos;
                    }
                }
                values.push(bits as i64);
            }
            grouping_list.push(values);
        }

        let repeat_id_list: Vec<i64> = op.grouping_ids.iter().map(|g| *g as i64).collect();

        // Build TPlanNode
        let mut row_tuples = child.tuple_ids.clone();
        if has_grouping_fns {
            row_tuples.push(virtual_tuple_id);
        }

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = repeat_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::REPEAT_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.repeat_node = Some(plan_nodes::TRepeatNode {
            output_tuple_id: virtual_tuple_id,
            slot_id_set_list,
            repeat_id_list,
            grouping_list,
            all_slot_ids: all_rollup_slot_ids,
        });

        // Pre-order: repeat node first, then child nodes
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        // Output tuple_ids
        let mut output_tuple_ids = child.tuple_ids;
        if has_grouping_fns {
            output_tuple_ids.push(virtual_tuple_id);
        }

        Ok(VisitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: output_tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_distribution
    // -------------------------------------------------------------------

    fn build_output_partition(
        &self,
        spec: &crate::sql::optimizer::property::DistributionSpec,
        child_scope: &ExprScope,
    ) -> Result<partitions::TDataPartition, String> {
        match spec {
            crate::sql::optimizer::property::DistributionSpec::Gather => {
                Ok(unpartitioned_stream_partition())
            }
            crate::sql::optimizer::property::DistributionSpec::HashPartitioned(cols) => {
                // For shuffle joins, cols contains ALL eq key columns from both
                // sides. Pick the ones that resolve in this child's scope.
                let mut partition_exprs = Vec::new();
                let mut used = std::collections::HashSet::new();
                for col in cols.iter() {
                    if used.contains(&col.column.to_lowercase()) {
                        continue; // skip duplicate column names
                    }
                    if let Ok(binding) =
                        child_scope.resolve_column(col.qualifier.as_deref(), &col.column)
                    {
                        let binding = binding.clone();
                        let type_desc = expr_compiler::binding_type_desc(&binding)?;
                        partition_exprs.push(expr_compiler::build_slot_ref_texpr(
                            binding.slot_id,
                            binding.tuple_id,
                            type_desc,
                        ));
                        used.insert(col.column.to_lowercase());
                    }
                }
                if partition_exprs.is_empty() {
                    return Err(format!(
                        "no hash partition columns resolved in child scope from {:?}",
                        cols.iter().map(|c| &c.column).collect::<Vec<_>>()
                    ));
                }
                Ok(partitions::TDataPartition::new(
                    partitions::TPartitionType::HASH_PARTITIONED,
                    Some(partition_exprs),
                    None::<Vec<partitions::TRangePartition>>,
                    None::<Vec<partitions::TBucketProperty>>,
                ))
            }
            crate::sql::optimizer::property::DistributionSpec::Any => {
                Err("PhysicalDistribution(Any) is not supported in fragment builder".to_string())
            }
        }
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

        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let VisitResult {
            plan_nodes,
            scope,
            tuple_ids,
            cte_exchange_nodes,
        } = child;

        let output_partition = self.build_output_partition(&op.spec, &scope)?;
        let exchange_partition_type = output_partition.type_;

        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(plan_nodes),
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
            cte_exchange_nodes,
        });

        let exchange_node_id = self.alloc_node();
        let exchange_node = nodes::build_exchange_node(
            exchange_node_id,
            tuple_ids.clone(),
            exchange_partition_type,
        );

        self.completed_edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            edge_kind: FragmentEdgeKind::Stream,
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope,
            tuple_ids,
            cte_exchange_nodes: Vec::new(),
        })
    }

    // -------------------------------------------------------------------
    // visit_union / visit_intersect / visit_except
    // -------------------------------------------------------------------

    fn visit_union(
        &mut self,
        op: &PhysicalUnionOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let result = self.visit_set_op_common(
            node,
            plan_nodes::TPlanNodeType::UNION_NODE,
            |plan_node, tnode| {
                plan_node.union_node = Some(tnode);
            },
        )?;
        if op.all {
            Ok(result)
        } else {
            self.emit_distinct_on_top(result)
        }
    }

    fn visit_intersect(
        &mut self,
        _op: &PhysicalIntersectOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        self.visit_set_op_common(
            node,
            plan_nodes::TPlanNodeType::INTERSECT_NODE,
            |plan_node, tnode| {
                plan_node.intersect_node = Some(plan_nodes::TIntersectNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    has_outer_join_child: None,
                    local_partition_by_exprs: None,
                });
            },
        )
    }

    fn visit_except(
        &mut self,
        _op: &PhysicalExceptOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        self.visit_set_op_common(
            node,
            plan_nodes::TPlanNodeType::EXCEPT_NODE,
            |plan_node, tnode| {
                plan_node.except_node = Some(plan_nodes::TExceptNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    local_partition_by_exprs: None,
                });
            },
        )
    }

    fn visit_set_op_common(
        &mut self,
        node: &PhysicalPlanNode,
        node_type: plan_nodes::TPlanNodeType,
        apply_payload: impl FnOnce(&mut plan_nodes::TPlanNode, plan_nodes::TUnionNode),
    ) -> Result<VisitResult, String> {
        if node.children.is_empty() {
            return Err("set operation node has no inputs".into());
        }

        let mut child_results = Vec::with_capacity(node.children.len());
        for child in &node.children {
            child_results.push(self.visit(child)?);
        }

        let output_tuple_id = self.alloc_tuple();
        let set_op_node_id = self.alloc_node();

        let output_columns: Vec<crate::sql::analysis::OutputColumn> =
            if node.output_columns.is_empty() {
                child_results[0]
                    .scope
                    .iter_columns()
                    .map(|(name, binding)| crate::sql::analysis::OutputColumn {
                        name: name.clone(),
                        data_type: binding.data_type.clone(),
                        nullable: binding.nullable,
                    })
                    .collect::<Vec<_>>()
            } else {
                node.output_columns.clone()
            };

        let mut output_scope = ExprScope::new();
        let first_child_cols: Vec<(String, ColumnBinding)> = child_results[0]
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();

        if first_child_cols.len() != output_columns.len() {
            return Err(format!(
                "set operation column count mismatch during codegen: child has {}, output has {}",
                first_child_cols.len(),
                output_columns.len()
            ));
        }

        for (idx, output_col) in output_columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &output_col.name,
                &output_col.data_type,
                output_col.nullable,
                idx as i32,
            );
            output_scope.add_column(
                None,
                output_col.name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: output_col.data_type.clone(),
                    type_desc: None,
                    nullable: output_col.nullable,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        let mut result_expr_lists = Vec::with_capacity(child_results.len());
        for child_result in &child_results {
            let mut expr_list = Vec::new();
            for (col_idx, (_, child_binding)) in child_result.scope.iter_columns().enumerate() {
                let output_col = output_columns.get(col_idx).ok_or_else(|| {
                    format!("missing output column {} for set operation", col_idx)
                })?;
                let needs_cast = child_binding.data_type != output_col.data_type;
                if needs_cast {
                    let target_desc = type_infer::arrow_type_to_type_desc(&output_col.data_type)?;
                    let child_desc = expr_compiler::binding_type_desc(child_binding)?;
                    let slot_ref = expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        child_desc,
                    );
                    expr_list.push(expr_compiler::build_cast_texpr(slot_ref, target_desc));
                } else {
                    let type_desc = expr_compiler::binding_type_desc(child_binding)?;
                    expr_list.push(expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        type_desc,
                    ));
                }
            }
            result_expr_lists.push(expr_list);
        }

        let tnode = plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists,
            const_expr_lists: vec![],
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = set_op_node_id;
        plan_node.node_type = node_type;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];

        apply_payload(&mut plan_node, tnode);

        plan_node.num_children = child_results.len() as i32;
        let mut plan_nodes_out = vec![plan_node];
        let mut cte_exchange_nodes = Vec::new();
        for child_result in child_results {
            plan_nodes_out.extend(child_result.plan_nodes);
            cte_exchange_nodes.extend(child_result.cte_exchange_nodes);
        }

        Ok(VisitResult {
            plan_nodes: plan_nodes_out,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes,
        })
    }

    fn emit_distinct_on_top(&mut self, child: VisitResult) -> Result<VisitResult, String> {
        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            let texpr =
                expr_compiler::build_slot_ref_texpr(binding.slot_id, binding.tuple_id, type_desc);
            grouping_exprs.push(texpr);

            let slot_id = self.alloc_slot();
            if let Some(slot_type_desc) = binding.type_desc.clone() {
                self.desc_builder.add_slot_with_type_desc(
                    slot_id,
                    agg_tuple_id,
                    name,
                    slot_type_desc,
                    binding.nullable,
                    idx as i32,
                );
            } else {
                self.desc_builder.add_slot(
                    slot_id,
                    agg_tuple_id,
                    name,
                    &binding.data_type,
                    binding.nullable,
                    idx as i32,
                );
            }
            agg_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type: binding.data_type.clone(),
                    type_desc: binding.type_desc.clone(),
                    nullable: binding.nullable,
                },
            );
        }

        self.desc_builder.add_tuple(agg_tuple_id, None);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            vec![],
            true,
        );

        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_cte_anchor
    // -------------------------------------------------------------------

    fn visit_cte_anchor(
        &mut self,
        _op: &PhysicalCTEAnchorOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Visit the produce subtree first — this creates a completed CTE
        // fragment (stored in self.completed_fragments / self.cte_fragments)
        // as a side effect. The returned VisitResult is intentionally discarded
        // because the anchor's output comes entirely from the consumer subtree.
        let _ = self.visit(&node.children[0])?;
        self.visit(&node.children[1])
    }

    // -------------------------------------------------------------------
    // visit_cte_produce
    // -------------------------------------------------------------------

    fn visit_cte_produce(
        &mut self,
        op: &PhysicalCTEProduceOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Allocate the CTE fragment ID before visiting the child so that
        // any Distribution nodes inside the child correctly target this
        // CTE fragment as their parent in the fragment_stack.
        let cte_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(cte_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let cte_fragment = FragmentBuildResult {
            fragment_id: cte_fragment_id,
            plan: plan_nodes::TPlan::new(child.plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(&[])?,
            output_sink: build_noop_sink(),
            output_columns: op
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: Some(op.cte_id),
            cte_exchange_nodes: child.cte_exchange_nodes,
        };
        let idx = self.completed_fragments.len();
        self.completed_fragments.push(cte_fragment);
        self.cte_fragments.insert(op.cte_id, idx);

        Ok(VisitResult {
            plan_nodes: Vec::new(),
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: Vec::new(),
        })
    }

    // -------------------------------------------------------------------
    // visit_cte_consume
    // -------------------------------------------------------------------

    fn visit_cte_consume(&mut self, op: &PhysicalCTEConsumeOp) -> Result<VisitResult, String> {
        // Verify the CTE produce fragment was already visited.
        let cte_frag_idx = self
            .cte_fragments
            .get(&op.cte_id)
            .copied()
            .ok_or_else(|| format!("CTE consume references unknown cte_id={}", op.cte_id))?;
        let cte_fragment_id = self.completed_fragments[cte_frag_idx].fragment_id;

        // Allocate an exchange node that will receive data from the CTE
        // produce fragment's multicast sink.
        let exchange_node_id = self.alloc_node();

        // Build the scope from the CTE consume's declared output columns
        // so that parent operators can resolve column references.
        let exchange_tuple_id = self.alloc_tuple();
        let mut scope = ExprScope::new();

        for (idx, col) in op.output_columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                exchange_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: exchange_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            scope.add_column(None, col.name.clone(), binding.clone());
            // Also register with the CTE alias as qualifier
            scope.add_column(Some(op.alias.clone()), col.name.clone(), binding);
        }
        self.desc_builder.add_tuple(exchange_tuple_id, None);

        let exchange_node = nodes::build_exchange_node(
            exchange_node_id,
            vec![exchange_tuple_id],
            partitions::TPartitionType::UNPARTITIONED,
        );

        // Record the CTE multicast edge so the coordinator can wire sinks.
        let target_fragment_id = self.current_fragment_id()?;
        self.completed_edges.push(FragmentEdge {
            source_fragment_id: cte_fragment_id,
            target_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: unpartitioned_stream_partition(),
            edge_kind: FragmentEdgeKind::CteMulticast { cte_id: op.cte_id },
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope,
            tuple_ids: vec![exchange_tuple_id],
            cte_exchange_nodes: vec![(op.cte_id, exchange_node_id)],
        })
    }
}

fn synthetic_iceberg_table_id(scan_node_id: i32) -> i64 {
    -(scan_node_id as i64)
}

/// Set `TFunction.ignore_nulls` on the root function node of an analytic-call
/// `TExpr` so the BE-side `lower_window_function` picks up the modifier when
/// constructing `WindowFunctionKind::FirstValue/LastValue/Lead/Lag`.
fn apply_ignore_nulls_to_root_fn(texpr: &mut exprs::TExpr, ignore_nulls: bool) {
    if !ignore_nulls {
        return;
    }
    if let Some(root) = texpr.nodes.first_mut()
        && let Some(fn_) = root.fn_.as_mut()
    {
        fn_.ignore_nulls = Some(true);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unpartitioned_stream_partition() -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partitions::TPartitionType::UNPARTITIONED,
        None::<Vec<crate::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

fn build_result_sink() -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::RESULT_SINK,
        None::<data_sinks::TDataStreamSink>,
        Some(data_sinks::TResultSink::default()),
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
    )
}

/// Placeholder sink for child / CTE fragments.  The coordinator replaces
/// this with the real DataStreamSink or MultiCastDataStreamSink after
/// fragment instance IDs are assigned.
fn build_noop_sink() -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::NOOP_SINK,
        None::<data_sinks::TDataStreamSink>,
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
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;

    use arrow::datatypes::DataType;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::plan_nodes;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergColumnStats, IcebergDeleteFileContent,
        IcebergDeleteFileFormat, IcebergDeleteFileInfo, IcebergPartitionFieldValue,
        IcebergPartitionValue, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
        ManagedTabletRef, PhysicalTableLayout, S3FileInfo, TableDef, TableStorage,
    };
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalDistributionOp, PhysicalHashJoinEqCondition,
        PhysicalHashJoinOp, PhysicalScanOp, PhysicalSortOp,
    };
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::statistics::Statistics;

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in scan-only builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            Ok(None)
        }
    }

    struct ManagedCatalog {
        layout: PhysicalTableLayout,
    }

    impl CatalogProvider for ManagedCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in managed scan builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            Ok(Some(self.layout.clone()))
        }
    }

    struct MixedCatalog {
        managed_layout: PhysicalTableLayout,
    }

    impl CatalogProvider for MixedCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in mixed scan builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            if table == "managed_t" {
                Ok(Some(self.managed_layout.clone()))
            } else {
                Ok(None)
            }
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

    fn id_eq_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(id_expr()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(value)),
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn iceberg_i32_file(path: &str, min: i32, max: i32) -> S3FileInfo {
        S3FileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: Some(HashMap::from([(
                "id".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    column_size: None,
                    lower_bound: Some(min.to_le_bytes().to_vec()),
                    upper_bound: Some(max.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn iceberg_i32_partition_file(path: &str, id: i32) -> S3FileInfo {
        S3FileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some(format!("Struct([{id}])")),
            first_row_id: None,
            data_sequence_number: Some(1),
            delete_files: vec![],
            manifest_path: Some(format!("manifest-{id}.avro")),
            partition_values: vec![IcebergPartitionFieldValue {
                source_column: "id".to_string(),
                field_name: "id".to_string(),
                transform: "identity".to_string(),
                value: Some(IcebergPartitionValue::Int32(id)),
            }],
        }
    }

    fn iceberg_delete_file(path: &str, length: i64) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(length),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: vec![],
            equality_field_ids: vec![],
        }
    }

    fn stats() -> Statistics {
        Statistics {
            output_row_count: 3.0,
            column_statistics: HashMap::new(),
        }
    }

    fn table_with_delete_files(
        delete_files: Vec<IcebergDeleteFileInfo>,
        iceberg_schema_fields: Vec<IcebergSchemaFieldDef>,
    ) -> TableDef {
        TableDef {
            name: "ice_t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                },
                ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: Some(IcebergTableInfo {
                location: "file:///warehouse/ice_t".to_string(),
                schema: IcebergSchemaDef {
                    fields: iceberg_schema_fields,
                },
                serialized_metadata: None,
            }),
            storage: TableStorage::S3ParquetFiles {
                files: vec![crate::sql::catalog::S3FileInfo {
                    path: "s3://bucket/data.parquet".to_string(),
                    size: 1,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: Some(0),
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: Some(1),
                    delete_files,
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: BTreeMap::new(),
            },
        }
    }

    fn equality_delete_file(
        equality_column_names: Vec<String>,
        equality_field_ids: Vec<i32>,
    ) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: "s3://bucket/eq-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            equality_column_names,
            equality_field_ids,
        }
    }

    #[test]
    fn equality_delete_field_ids_are_resolved_to_required_scan_columns() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(Vec::new(), vec![3])],
            vec![
                IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    children: vec![],
                },
                IcebergSchemaFieldDef {
                    field_id: 3,
                    name: "category".to_string(),
                    initial_default: None,
                    write_default: None,
                    children: vec![],
                },
            ],
        );

        add_iceberg_equality_delete_required_columns(&mut required, &table).expect("resolve ids");

        assert!(required.contains("id"));
        assert!(required.contains("category"));
    }

    #[test]
    fn equality_delete_column_names_are_legacy_fallback_for_required_scan_columns() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(
                vec!["category".to_string()],
                Vec::new(),
            )],
            vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                children: vec![],
            }],
        );

        add_iceberg_equality_delete_required_columns(&mut required, &table).expect("legacy names");

        assert!(required.contains("id"));
        assert!(required.contains("category"));
    }

    #[test]
    fn equality_delete_unknown_field_id_is_planning_error() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(Vec::new(), vec![99])],
            vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                children: vec![],
            }],
        );

        let err = add_iceberg_equality_delete_required_columns(&mut required, &table)
            .expect_err("unknown field id");

        assert!(err.contains("unknown field id 99"), "{err}");
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
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
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

    fn managed_scan_plan() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "managed_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![],
                        cloud_properties: BTreeMap::new(),
                    },
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

    fn iceberg_scan_plan() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: Some(IcebergTableInfo {
                        location: "file:///warehouse/ice_t".to_string(),
                        schema: IcebergSchemaDef {
                            fields: vec![IcebergSchemaFieldDef {
                                field_id: 1,
                                name: "id".to_string(),
                                initial_default: None,
                                write_default: None,
                                children: vec![],
                            }],
                        },
                        serialized_metadata: None,
                    }),
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![],
                        cloud_properties: BTreeMap::new(),
                    },
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

    fn iceberg_scan_plan_with_file_stats() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: Some(IcebergTableInfo {
                        location: "s3://bucket/warehouse/ice_t".to_string(),
                        schema: IcebergSchemaDef {
                            fields: vec![IcebergSchemaFieldDef {
                                field_id: 1,
                                name: "id".to_string(),
                                initial_default: None,
                                write_default: None,
                                children: vec![],
                            }],
                        },
                        serialized_metadata: None,
                    }),
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![
                            iceberg_i32_file("s3://bucket/file-1-5.parquet", 1, 5),
                            iceberg_i32_file("s3://bucket/file-10-20.parquet", 10, 20),
                        ],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![id_eq_literal(12)],
                required_columns: None,
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
        }
    }

    fn iceberg_scan_plan_with_partition_values() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: Some(IcebergTableInfo {
                        location: "s3://bucket/warehouse/ice_t".to_string(),
                        schema: IcebergSchemaDef {
                            fields: vec![IcebergSchemaFieldDef {
                                field_id: 1,
                                name: "id".to_string(),
                                initial_default: None,
                                write_default: None,
                                children: vec![],
                            }],
                        },
                        serialized_metadata: None,
                    }),
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![
                            iceberg_i32_partition_file("s3://bucket/id-1.parquet", 1),
                            iceberg_i32_partition_file("s3://bucket/id-12.parquet", 12),
                        ],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![id_eq_literal(12)],
                required_columns: None,
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
        }
    }

    fn iceberg_scan_plan_with_large_file(size: i64) -> PhysicalPlanNode {
        let mut file = iceberg_i32_file("s3://bucket/large.parquet", 1, 100);
        file.size = size;
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: Some(IcebergTableInfo {
                        location: "s3://bucket/warehouse/ice_t".to_string(),
                        schema: IcebergSchemaDef {
                            fields: vec![IcebergSchemaFieldDef {
                                field_id: 1,
                                name: "id".to_string(),
                                initial_default: None,
                                write_default: None,
                                children: vec![],
                            }],
                        },
                        serialized_metadata: None,
                    }),
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![file],
                        cloud_properties: BTreeMap::new(),
                    },
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

    fn iceberg_scan_plan_with_many_delete_files(delete_count: usize) -> PhysicalPlanNode {
        let mut file = iceberg_i32_file("s3://bucket/delete-heavy.parquet", 1, 100);
        file.delete_files = (0..delete_count)
            .map(|idx| iceberg_delete_file(&format!("s3://bucket/delete-{idx}.parquet"), 1))
            .collect();
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: Some(IcebergTableInfo {
                        location: "s3://bucket/warehouse/ice_t".to_string(),
                        schema: IcebergSchemaDef {
                            fields: vec![IcebergSchemaFieldDef {
                                field_id: 1,
                                name: "id".to_string(),
                                initial_default: None,
                                write_default: None,
                                children: vec![],
                            }],
                        },
                        serialized_metadata: None,
                    }),
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![file],
                        cloud_properties: BTreeMap::new(),
                    },
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
    fn iceberg_scan_predicates_feed_min_max_and_file_stats_pruning() {
        let plan = iceberg_scan_plan_with_file_stats();

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let hdfs = scan.hdfs_scan_node.as_ref().expect("hdfs scan payload");

        assert_eq!(
            hdfs.min_max_conjuncts.as_ref().map(Vec::len),
            Some(1),
            "standalone scan predicates should be available to HDFS min/max pruning"
        );
        assert_eq!(hdfs.min_max_tuple_id, hdfs.tuple_id);

        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");
        assert_eq!(
            ranges.len(),
            1,
            "file-level Iceberg stats should prune the file whose id range cannot contain 12"
        );
        let kept_path = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.full_path.as_deref());
        assert_eq!(kept_path, Some("s3://bucket/file-10-20.parquet"));
    }

    #[test]
    fn iceberg_identity_partition_values_prune_scan_ranges() {
        let plan = iceberg_scan_plan_with_partition_values();

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");

        assert_eq!(
            ranges.len(),
            1,
            "identity partition values should prune files before scan range planning"
        );
        let kept_path = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.full_path.as_deref());
        assert_eq!(kept_path, Some("s3://bucket/id-12.parquet"));
    }

    #[test]
    fn iceberg_large_plain_files_are_split_into_parallel_scan_ranges() {
        let plan = iceberg_scan_plan_with_large_file(300 * 1024 * 1024);

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");

        assert_eq!(ranges.len(), 3);
        let first = ranges[0].scan_range.hdfs_scan_range.as_ref().unwrap();
        let second = ranges[1].scan_range.hdfs_scan_range.as_ref().unwrap();
        let third = ranges[2].scan_range.hdfs_scan_range.as_ref().unwrap();
        assert_eq!(first.offset, Some(0));
        assert_eq!(first.length, Some(128 * 1024 * 1024));
        assert_eq!(first.file_length, Some(300 * 1024 * 1024));
        assert_eq!(second.offset, Some(128 * 1024 * 1024));
        assert_eq!(second.length, Some(128 * 1024 * 1024));
        assert_eq!(third.offset, Some(256 * 1024 * 1024));
        assert_eq!(third.length, Some(44 * 1024 * 1024));
    }

    #[test]
    fn iceberg_delete_apply_cost_rejects_too_many_delete_files() {
        let plan = iceberg_scan_plan_with_many_delete_files(1025);

        let err = match PlanFragmentBuilder::build(&plan, &DummyCatalog, "default") {
            Ok(_) => panic!("delete-heavy scan should fail fast"),
            Err(err) => err,
        };

        assert!(
            err.contains("too many Iceberg delete files"),
            "unexpected error: {err}"
        );
    }

    fn mixed_managed_iceberg_join_plan() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: Some("ice_t".to_string()),
                            column: "id".to_string(),
                        },
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    right: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: Some("managed_t".to_string()),
                            column: "id".to_string(),
                        },
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Colocate,
            }),
            children: vec![iceberg_scan_plan(), managed_scan_plan()],
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
            crate::sql::codegen::FragmentEdgeKind::Stream
        ));

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert!(
            root.plan
                .nodes
                .iter()
                .any(|node| { node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE })
        );
    }

    #[test]
    fn build_nested_gather_distribution_targets_immediate_parent_fragment() {
        // Wrap the nested gathers inside a Sort so the root is NOT a Gather
        // (root-level Gather is elided).
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
            }],
            stats: stats(),
            output_columns: output_columns(),
        };

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
        assert_eq!(build.fragment_results.len(), 3);
        assert_eq!(build.edges.len(), 2);

        // The inner gather targets its immediate parent (the outer gather fragment),
        // not the root fragment directly.
        let outer_gather_frag_id = build
            .edges
            .iter()
            .find(|e| e.target_fragment_id == build.root_fragment_id)
            .expect("edge to root")
            .source_fragment_id;
        assert!(build.edges.iter().any(|e| {
            e.target_fragment_id == outer_gather_frag_id
                && e.source_fragment_id != outer_gather_frag_id
                && matches!(e.edge_kind, crate::sql::codegen::FragmentEdgeKind::Stream)
        }));
    }

    #[test]
    fn build_maps_hash_distribution_to_hash_partitioned_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned(vec![
                    crate::sql::optimizer::property::ColumnRef {
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
            crate::partitions::TPartitionType::HASH_PARTITIONED
        );
        assert_eq!(
            edge.output_partition
                .partition_exprs
                .as_ref()
                .map(|v| v.len()),
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

        let result = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default");
        let err = result.err().expect("distribution any must fail");
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

    #[test]
    fn build_managed_scan_emits_lake_scan_with_internal_ranges() {
        let layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![ManagedTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let plan = managed_scan_plan();
        let catalog = ManagedCatalog { layout };

        let build = PlanFragmentBuilder::build(&plan, &catalog, "default").expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        let root = build.fragment_results.first().expect("root fragment");
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let schema_key = lake.schema_key.as_ref().expect("schema_key");
        assert_eq!(schema_key.db_id, Some(11));
        assert_eq!(schema_key.table_id, Some(22));
        assert_eq!(schema_key.schema_id, Some(33));

        let tuple_desc = root
            .desc_tbl
            .tuple_descriptors
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .expect("managed scan tuple descriptor");
        assert_eq!(tuple_desc.table_id, Some(22));

        let table_descs = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors");
        let table_desc = table_descs
            .iter()
            .find(|table| table.id == 22)
            .expect("managed table descriptor");
        assert_eq!(table_desc.db_name, "default");
        assert_eq!(table_desc.table_name, "managed_t");

        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&1)
            .expect("scan ranges");
        assert_eq!(ranges.len(), 1);
        let internal = ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");
        assert_eq!(internal.tablet_id, 101);
        assert_eq!(internal.partition_id, Some(201));
        assert_eq!(internal.version, "7");
        assert_eq!(internal.db_name, "default");
        assert_eq!(internal.table_name.as_deref(), Some("managed_t"));
    }

    #[test]
    fn non_managed_iceberg_scan_uses_synthetic_descriptor_table_id() {
        let build = PlanFragmentBuilder::build(&iceberg_scan_plan(), &DummyCatalog, "default")
            .expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        let root = build.fragment_results.first().expect("root fragment");
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let synthetic_table_id = synthetic_iceberg_table_id(scan_node.node_id);
        let tuple_desc = root
            .desc_tbl
            .tuple_descriptors
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .expect("scan tuple descriptor");
        assert_eq!(tuple_desc.table_id, Some(synthetic_table_id));

        let table_desc = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|table| table.id == synthetic_table_id)
            .expect("synthetic iceberg table descriptor");
        assert_eq!(
            table_desc.table_type,
            crate::types::TTableType::ICEBERG_TABLE
        );
        assert_eq!(
            table_desc
                .iceberg_table
                .as_ref()
                .and_then(|table| table.iceberg_schema.as_ref())
                .and_then(|schema| schema.fields.as_ref())
                .and_then(|fields| fields.first())
                .and_then(|field| field.field_id),
            Some(1)
        );
    }

    #[test]
    fn mixed_managed_and_iceberg_scan_table_ids_do_not_collide() {
        let catalog = MixedCatalog {
            managed_layout: PhysicalTableLayout {
                db_id: 11,
                table_id: 1,
                schema_id: 33,
                tablets: vec![ManagedTabletRef {
                    tablet_id: 101,
                    partition_id: 201,
                    version: 7,
                }],
            },
        };

        let build =
            PlanFragmentBuilder::build(&mixed_managed_iceberg_join_plan(), &catalog, "default")
                .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        let tuple_descs = &root.desc_tbl.tuple_descriptors;
        let iceberg_table_id = tuple_descs
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .and_then(|tuple| tuple.table_id)
            .expect("iceberg tuple table id");
        let managed_table_id = tuple_descs
            .iter()
            .find(|tuple| tuple.id == Some(2))
            .and_then(|tuple| tuple.table_id)
            .expect("managed tuple table id");
        assert_ne!(iceberg_table_id, managed_table_id);
        assert_eq!(managed_table_id, 1);

        let table_descs = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors");
        let iceberg_desc = table_descs
            .iter()
            .find(|table| table.id == iceberg_table_id)
            .expect("iceberg table descriptor");
        assert_eq!(
            iceberg_desc.table_type,
            crate::types::TTableType::ICEBERG_TABLE
        );
        let managed_desc = table_descs
            .iter()
            .find(|table| table.id == managed_table_id)
            .expect("managed table descriptor");
        assert_eq!(
            managed_desc.table_type,
            crate::types::TTableType::OLAP_TABLE
        );
    }
}
