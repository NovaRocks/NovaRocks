//! LowCardinalityDictionaryRewrite — the rule wrapper.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};

pub(crate) struct LowCardinalityDictionaryRewriteRule;

impl LogicalRewriteRule for LowCardinalityDictionaryRewriteRule {
    fn name(&self) -> &'static str {
        "LowCardinalityDictionaryRewrite"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    // Keep the default Leaf pattern: this TopDown rule is whole-tree and
    // context-dependent. The guard combines a dictionary provider check with a
    // recursive `contains_scan` walk instead of matching one node shape.
    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        ctx.dictionary_provider().is_some() && contains_scan(expr)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let mut dict_ctx = super::collector::collect(&expr, ctx)?;
        // Fast-path: if the collector found no eligible columns (provider
        // returned None for every scan column), the rewriter would be a
        // no-op. Return Unchanged to avoid the plan cloning cost.
        if !dict_ctx.has_any_scan_column() {
            return Ok(RewriteResult::Unchanged);
        }
        let arena_rc = ctx.scalar_arena();
        let rewritten_expr =
            super::rewriter::rewrite(expr, &mut dict_ctx, &mut arena_rc.borrow_mut())?;
        Ok(RewriteResult::Changed(rewritten_expr))
    }
}

fn contains_scan(expr: &OptExpr) -> bool {
    match &expr.op {
        Operator::LogicalScan(scan) => {
            // The rewriter is idempotent: a scan whose dict_columns is
            // already populated was already rewritten by a prior pass.
            // Return false for such scans so the TopDown driver does not
            // fire the rule again on the already-rewritten subtree.
            scan.dict_columns.is_empty()
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => expr.children.iter().any(contains_scan),
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::engine::dictionary::model::{
        DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    };
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, SortOp, TopNOp, TopNPhase};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{QueryDictionaryProvider, RewriteContext};
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::optimizer_bridge::plan::{
        logical_plan_to_opt_expr, opt_expr_to_logical_plan,
    };
    use crate::sql::planner::optimizer_bridge::scalar::{intern_sort_items, materialize_sort_keys};
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalPlanNode, LogicalScanNode, LogicalSortNode,
        PlanNodeKind,
    };

    struct StaticProvider {
        snapshot: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for StaticProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            if column_name.eq_ignore_ascii_case(&self.snapshot.column_name) {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn sample_snapshot(order_preserving: bool) -> DictionarySnapshot {
        DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::StarRocksTable {
                database: "db".to_string(),
                table: "t".to_string(),
                db_id: 1,
                table_id: 2,
            },
            column_id: Some(10),
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: None,
                schema_id: 0,
            },
            values: vec![DictionaryValue {
                id: 1,
                bytes: b"a".to_vec(),
            }],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving,
        }
    }

    fn make_table() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn s_output_column() -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: "s".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        }
    }

    fn s_column_ref() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: "s".to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn install_provider(ctx: &mut RewriteContext, order_preserving: bool) {
        let provider = StaticProvider {
            snapshot: sample_snapshot(order_preserving),
        };
        ctx.set_dictionary_provider(Arc::new(provider));
    }

    fn run_pipeline_rewrite(
        mut plan: LogicalPlanNode,
        ctx: &mut RewriteContext,
    ) -> LogicalPlanNode {
        assign_test_column_ids(&mut plan);
        let mut scalars = ScalarArena::new();
        let input = logical_plan_to_opt_expr(&plan, &mut scalars);
        let arena = Rc::new(RefCell::new(scalars));
        ctx.set_scalar_arena(Rc::clone(&arena));
        ctx.set_query_stats_input(
            crate::sql::optimizer::stats_input::OptimizerStatsInput::from_legacy_table_stats_for_migration(
                &HashMap::new(),
            ),
        );
        let pipeline = query_rewrite_pipeline();
        let rewritten = pipeline.rewrite(input, ctx).unwrap();
        opt_expr_to_logical_plan(rewritten, &arena.borrow())
    }

    fn run_rule_rewrite_expr(
        input: OptExpr,
        scalars: ScalarArena,
        ctx: &mut RewriteContext,
    ) -> (OptExpr, Rc<RefCell<ScalarArena>>) {
        let arena = Rc::new(RefCell::new(scalars));
        ctx.set_scalar_arena(Rc::clone(&arena));
        let rewritten = match super::LowCardinalityDictionaryRewriteRule
            .apply(input, ctx)
            .unwrap()
        {
            RewriteResult::Changed(expr) => expr,
            RewriteResult::Unchanged => panic!("expected dictionary rule to rewrite TopN"),
            RewriteResult::Rejected(diag) => panic!("dictionary rule rejected TopN: {diag:?}"),
        };
        (rewritten, arena)
    }

    fn assign_test_column_ids(plan: &mut LogicalPlanNode) {
        let mut next = 1u32;
        let mut ids = HashMap::<String, ColumnId>::new();
        assign_plan_column_ids(plan, &mut ids, &mut next);
    }

    #[derive(Clone, Debug)]
    struct TestOutputBinding {
        name: String,
        qualifiers: Vec<String>,
        column_id: ColumnId,
    }

    type TestScope = Vec<TestOutputBinding>;

    fn test_column_id(
        ids: &mut HashMap<String, ColumnId>,
        next: &mut u32,
        key: impl Into<String>,
    ) -> ColumnId {
        let key = key.into();
        *ids.entry(key).or_insert_with(|| {
            let id = ColumnId::new_for_test(*next);
            *next += 1;
            id
        })
    }

    fn assign_output_column_id(
        output: &mut OutputColumn,
        ids: &mut HashMap<String, ColumnId>,
        next: &mut u32,
        key: String,
    ) {
        if output.column_id == ColumnId::UNSET {
            output.column_id = test_column_id(ids, next, key);
        }
    }

    fn assign_fresh_output_column_id(output: &mut OutputColumn, next: &mut u32) {
        if output.column_id == ColumnId::UNSET {
            output.column_id = ColumnId::new_for_test(*next);
            *next += 1;
        }
    }

    fn output_scope_for_columns(columns: &[OutputColumn], qualifiers: Vec<String>) -> TestScope {
        columns
            .iter()
            .map(|column| TestOutputBinding {
                name: column.name.clone(),
                qualifiers: qualifiers.clone(),
                column_id: column.column_id,
            })
            .collect()
    }

    fn scan_qualifiers(scan: &LogicalScanNode) -> Vec<String> {
        match &scan.alias {
            Some(alias) => vec![alias.clone()],
            None => vec![
                scan.table.name.clone(),
                format!("{}.{}", scan.database, scan.table.name),
            ],
        }
    }

    fn qualifier_matches(actual: &str, expected: &str) -> bool {
        actual.eq_ignore_ascii_case(expected)
            || actual
                .rsplit('.')
                .next()
                .is_some_and(|last| last.eq_ignore_ascii_case(expected))
    }

    fn resolve_test_column_id(
        scope: &TestScope,
        qualifier: Option<&str>,
        column: &str,
    ) -> ColumnId {
        let mut matches = Vec::new();
        for binding in scope {
            if !binding.name.eq_ignore_ascii_case(column) {
                continue;
            }
            if let Some(expected) = qualifier {
                if !binding
                    .qualifiers
                    .iter()
                    .any(|actual| qualifier_matches(actual, expected))
                {
                    continue;
                }
            }
            if !matches.contains(&binding.column_id) {
                matches.push(binding.column_id);
            }
        }
        match matches.as_slice() {
            [id] => *id,
            [] => panic!("unresolved test ColumnRef {qualifier:?}.{column}"),
            _ => panic!("ambiguous test ColumnRef {qualifier:?}.{column}: {matches:?}"),
        }
    }

    fn assign_expr_column_ids(
        expr: &mut TypedExpr,
        scope: &TestScope,
        ids: &mut HashMap<String, ColumnId>,
        next: &mut u32,
    ) {
        match &mut expr.kind {
            ExprKind::ColumnRef {
                column_id,
                qualifier,
                column,
            } => {
                if *column_id == ColumnId::UNSET {
                    *column_id =
                        resolve_test_column_id(scope, qualifier.as_deref(), column.as_str());
                }
            }
            ExprKind::Literal(_)
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::SubqueryPlaceholder { .. } => {}
            ExprKind::BinaryOp { left, right, .. } => {
                assign_expr_column_ids(left, scope, ids, next);
                assign_expr_column_ids(right, scope, ids, next);
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::Lambda { body: expr, .. } => assign_expr_column_ids(expr, scope, ids, next),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                for arg in args {
                    assign_expr_column_ids(arg, scope, ids, next);
                }
            }
            ExprKind::InList { expr, list, .. } => {
                assign_expr_column_ids(expr, scope, ids, next);
                for item in list {
                    assign_expr_column_ids(item, scope, ids, next);
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                assign_expr_column_ids(expr, scope, ids, next);
                assign_expr_column_ids(low, scope, ids, next);
                assign_expr_column_ids(high, scope, ids, next);
            }
            ExprKind::Like { expr, pattern, .. } => {
                assign_expr_column_ids(expr, scope, ids, next);
                assign_expr_column_ids(pattern, scope, ids, next);
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand.as_deref_mut() {
                    assign_expr_column_ids(operand, scope, ids, next);
                }
                for (when, then) in when_then {
                    assign_expr_column_ids(when, scope, ids, next);
                    assign_expr_column_ids(then, scope, ids, next);
                }
                if let Some(else_expr) = else_expr.as_deref_mut() {
                    assign_expr_column_ids(else_expr, scope, ids, next);
                }
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    assign_expr_column_ids(arg, scope, ids, next);
                }
                for expr in partition_by {
                    assign_expr_column_ids(expr, scope, ids, next);
                }
                for item in order_by {
                    assign_expr_column_ids(&mut item.expr, scope, ids, next);
                }
            }
        }
    }

    fn assign_aggregate_call_ids(
        agg: &mut AggregateCall,
        scope: &TestScope,
        ids: &mut HashMap<String, ColumnId>,
        next: &mut u32,
        index: usize,
    ) {
        for arg in &mut agg.args {
            assign_expr_column_ids(arg, scope, ids, next);
        }
        for item in &mut agg.order_by {
            assign_expr_column_ids(&mut item.expr, scope, ids, next);
        }
        if agg.output_column_id == ColumnId::UNSET {
            agg.output_column_id = test_column_id(ids, next, format!("agg:{}:{index}", agg.name));
        }
    }

    fn assign_plan_column_ids(
        plan: &mut LogicalPlanNode,
        ids: &mut HashMap<String, ColumnId>,
        next: &mut u32,
    ) -> TestScope {
        match &mut plan.kind {
            PlanNodeKind::Scan(scan) => {
                for output in &mut scan.columns {
                    assign_fresh_output_column_id(output, next);
                }
                let scope = output_scope_for_columns(&scan.columns, scan_qualifiers(scan));
                for predicate in &mut scan.predicates {
                    assign_expr_column_ids(predicate, &scope, ids, next);
                }
                return scope;
            }
            PlanNodeKind::Values(values) => {
                for output in &mut values.columns {
                    assign_output_column_id(output, ids, next, format!("values:{}", output.name));
                }
                return output_scope_for_columns(&values.columns, Vec::new());
            }
            PlanNodeKind::CTEConsume(consume) => {
                for output in &mut consume.output_columns {
                    assign_output_column_id(
                        output,
                        ids,
                        next,
                        format!("cte_consume:{}:{}", consume.cte_id, output.name),
                    );
                }
                return output_scope_for_columns(
                    &consume.output_columns,
                    vec![consume.alias.clone()],
                );
            }
            _ => {}
        }

        let child_scopes: Vec<TestScope> = plan
            .children
            .iter_mut()
            .map(|child| assign_plan_column_ids(child, ids, next))
            .collect();

        match &mut plan.kind {
            PlanNodeKind::Filter(filter) => {
                let input_scope = child_scopes.first().expect("filter input");
                assign_expr_column_ids(&mut filter.predicate, input_scope, ids, next);
                input_scope.clone()
            }
            PlanNodeKind::Project(project) => {
                let input_scope = child_scopes.first().expect("project input");
                for item in &mut project.items {
                    assign_expr_column_ids(&mut item.expr, input_scope, ids, next);
                    if item.output_column_id == ColumnId::UNSET {
                        item.output_column_id = match &item.expr.kind {
                            ExprKind::ColumnRef { column_id, .. } => *column_id,
                            _ => test_column_id(ids, next, format!("project:{}", item.output_name)),
                        };
                    }
                }
                output_scope_for_columns(
                    &project
                        .items
                        .iter()
                        .map(|item| OutputColumn {
                            column_id: item.output_column_id,
                            name: item.output_name.clone(),
                            data_type: item.expr.data_type.clone(),
                            nullable: item.expr.nullable,
                            is_internal: false,
                        })
                        .collect::<Vec<_>>(),
                    project.output_qualifier.clone().into_iter().collect(),
                )
            }
            PlanNodeKind::Aggregate(aggregate) => {
                let input_scope = child_scopes.first().expect("aggregate input");
                for expr in &mut aggregate.group_by {
                    assign_expr_column_ids(expr, input_scope, ids, next);
                }
                for (index, agg) in aggregate.aggregates.iter_mut().enumerate() {
                    assign_aggregate_call_ids(agg, input_scope, ids, next, index);
                }
                let group_count = aggregate.group_by.len();
                for output_index in 0..aggregate.output_columns.len() {
                    let output = &mut aggregate.output_columns[output_index];
                    if output.column_id == ColumnId::UNSET {
                        output.column_id = if output_index < group_count {
                            match &aggregate.group_by[output_index].kind {
                                ExprKind::ColumnRef { column_id, .. } => *column_id,
                                _ => {
                                    test_column_id(ids, next, format!("aggregate:{}", output.name))
                                }
                            }
                        } else if let Some(agg) = aggregate
                            .aggregates
                            .get(output_index.saturating_sub(group_count))
                        {
                            agg.output_column_id
                        } else {
                            test_column_id(ids, next, format!("aggregate:{}", output.name))
                        };
                    }
                }
                output_scope_for_columns(&aggregate.output_columns, Vec::new())
            }
            PlanNodeKind::Sort(sort) => {
                let input_scope = child_scopes.first().expect("sort input");
                for item in &mut sort.items {
                    assign_expr_column_ids(&mut item.expr, input_scope, ids, next);
                }
                for expr in &mut sort.analytic_partition_by {
                    assign_expr_column_ids(expr, input_scope, ids, next);
                }
                input_scope.clone()
            }
            PlanNodeKind::Join(join) => {
                let mut scope = TestScope::new();
                for child_scope in &child_scopes {
                    scope.extend(child_scope.iter().cloned());
                }
                if let Some(condition) = join.condition.as_mut() {
                    assign_expr_column_ids(condition, &scope, ids, next);
                }
                scope
            }
            PlanNodeKind::Union(union) => {
                for output in &mut union.output_columns {
                    assign_output_column_id(output, ids, next, format!("union:{}", output.name));
                }
                if union.output_columns.is_empty() {
                    child_scopes.first().cloned().unwrap_or_default()
                } else {
                    output_scope_for_columns(&union.output_columns, Vec::new())
                }
            }
            PlanNodeKind::CTEProduce(produce) => {
                let input_scope = child_scopes.first().expect("cte produce input");
                for output in &mut produce.output_columns {
                    if output.column_id == ColumnId::UNSET {
                        output.column_id = resolve_test_column_id(input_scope, None, &output.name);
                    }
                }
                output_scope_for_columns(&produce.output_columns, Vec::new())
            }
            PlanNodeKind::CTEAnchor(_) => child_scopes.get(1).cloned().unwrap_or_default(),
            _ => child_scopes.first().cloned().unwrap_or_default(),
        }
    }

    #[test]
    fn test_helper_binds_column_refs_to_child_output_ids() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let mut aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![s_column_ref()],
                aggregates: vec![],
                output_columns: vec![s_output_column()],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );

        assign_test_column_ids(&mut aggregate);

        let PlanNodeKind::Aggregate(agg) = &aggregate.kind else {
            panic!("expected aggregate root");
        };
        let ExprKind::ColumnRef {
            column_id: group_id,
            ..
        } = &agg.group_by[0].kind
        else {
            panic!("expected group-by ColumnRef");
        };
        let PlanNodeKind::Scan(scan) = &aggregate.unary_input().kind else {
            panic!("expected scan child");
        };
        let scan_id = scan.columns[0].column_id;
        assert_eq!(
            *group_id, scan_id,
            "test fixtures must bind ColumnRef ids to child output ids"
        );
        assert_eq!(
            agg.output_columns[0].column_id, scan_id,
            "group-by output should reuse the grouped column id"
        );
    }

    #[test]
    fn group_by_string_rewrites_to_dict_column_and_decode() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![s_column_ref()],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![
                    s_output_column(),
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "cnt".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        let PlanNodeKind::Decode(decode) = &rewritten.kind else {
            panic!("expected decode root, got {rewritten:?}");
        };
        assert_eq!(decode.mappings.len(), 1);
        assert_eq!(decode.mappings[0].dict_column, "__nr_dict_t_s");
        assert_eq!(decode.mappings[0].string_column, "s");
        let aggregate_plan = rewritten.unary_input();
        let PlanNodeKind::Aggregate(agg) = &aggregate_plan.kind else {
            panic!("expected aggregate under decode");
        };
        // Group-by must reference the dict column now.
        let key = agg.group_by.first().expect("group by present");
        let ExprKind::ColumnRef {
            column,
            column_id: key_id,
            ..
        } = &key.kind
        else {
            panic!("group-by must be a column ref");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(key.data_type, DataType::Int32);
        // Scan must carry the dict_columns hint and a hidden Int32
        // OutputColumn.
        let PlanNodeKind::Scan(scan) = &aggregate_plan.unary_input().kind else {
            panic!("expected scan under aggregate");
        };
        assert_eq!(scan.dict_columns.len(), 1);
        assert_eq!(scan.dict_columns[0].dict_column, "__nr_dict_t_s");
        assert_eq!(scan.dict_columns[0].source_column, "s");
        let dict_output = scan
            .columns
            .iter()
            .find(|c| c.name == "__nr_dict_t_s" && matches!(c.data_type, DataType::Int32))
            .expect("scan exposes dict output");
        assert_eq!(*key_id, dict_output.column_id);
        assert_eq!(decode.mappings[0].source_column_id, dict_output.column_id);
        assert_eq!(decode.mappings[0].output_column_id, dict_output.column_id);
    }

    #[test]
    fn sort_non_order_preserving_decodes_before_sort() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let sort = LogicalPlanNode::new(
            PlanNodeKind::Sort(LogicalSortNode {
                items: vec![SortItem {
                    expr: s_column_ref(),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_by: vec![],
                output_columns: vec![],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        // Non-order-preserving snapshot — sort must decode first.
        install_provider(&mut ctx, false);
        let rewritten = run_pipeline_rewrite(sort, &mut ctx);
        let PlanNodeKind::Sort(_) = &rewritten.kind else {
            panic!("expected sort root, got {rewritten:?}");
        };
        // Sort's input is a Decode now.
        let PlanNodeKind::Decode(decode) = &rewritten.unary_input().kind else {
            panic!("expected decode under sort");
        };
        assert_eq!(decode.mappings.len(), 1);
        assert_eq!(decode.mappings[0].dict_column, "__nr_dict_t_s");
    }

    struct PerColumnProvider {
        snapshots: HashMap<String, DictionarySnapshot>,
    }

    impl QueryDictionaryProvider for PerColumnProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(self
                .snapshots
                .get(&column_name.to_ascii_lowercase())
                .cloned())
        }
    }

    fn two_string_table() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "s".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn string_output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref_with_id(name: &str, column_id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    #[test]
    fn topn_mixed_order_preserving_key_decodes_all_keys() {
        let mut scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: two_string_table(),
                alias: None,
                columns: vec![string_output_column("s"), string_output_column("k")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        assign_test_column_ids(&mut scan);
        let PlanNodeKind::Scan(scan_node) = &scan.kind else {
            panic!("expected scan");
        };
        let s_id = scan_node
            .columns
            .iter()
            .find(|c| c.name == "s")
            .expect("s output")
            .column_id;
        let k_id = scan_node
            .columns
            .iter()
            .find(|c| c.name == "k")
            .expect("k output")
            .column_id;

        let mut scalars = ScalarArena::new();
        let scan_expr = logical_plan_to_opt_expr(&scan, &mut scalars);
        let items = vec![
            SortItem {
                expr: column_ref_with_id("s", s_id),
                asc: true,
                nulls_first: false,
            },
            SortItem {
                expr: column_ref_with_id("k", k_id),
                asc: true,
                nulls_first: false,
            },
        ];
        let topn = OptExpr::new(
            Operator::LogicalTopN(TopNOp {
                items: intern_sort_items(&mut scalars, &items),
                limit: Some(10),
                offset: Some(0),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            vec![scan_expr],
        );
        let mut snapshots = HashMap::new();
        snapshots.insert("s".to_string(), named_snapshot("s", true));
        snapshots.insert("k".to_string(), named_snapshot("k", false));
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerColumnProvider { snapshots }));

        let (rewritten, arena) = run_rule_rewrite_expr(topn, scalars, &mut ctx);
        let Operator::LogicalTopN(topn) = &rewritten.op else {
            panic!("expected LogicalTopN root, got {:?}", rewritten.op);
        };
        let rewritten_items = materialize_sort_keys(&arena.borrow(), &topn.items);
        let key_names: Vec<&str> = rewritten_items
            .iter()
            .map(|item| match &item.expr.kind {
                ExprKind::ColumnRef { column, .. } => column.as_str(),
                other => panic!("expected ColumnRef sort item, got {other:?}"),
            })
            .collect();
        assert_eq!(
            key_names,
            vec!["s", "k"],
            "when any TopN key needs decode, all keys must stay on string refs"
        );
        let Operator::LogicalDecode(decode) = &rewritten.unary_input().op else {
            panic!(
                "expected Decode below TopN for mixed keys, got {:?}",
                rewritten.unary_input().op
            );
        };
        assert_eq!(decode.mappings.len(), 2);
        assert!(
            decode
                .mappings
                .iter()
                .any(|m| m.dict_column == "__nr_dict_t_s" && m.output_column_id == s_id)
        );
        assert!(
            decode
                .mappings
                .iter()
                .any(|m| m.dict_column == "__nr_dict_t_k" && m.output_column_id == k_id)
        );
    }

    #[test]
    fn sort_mixed_order_preserving_key_decodes_all_keys() {
        let mut scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: two_string_table(),
                alias: None,
                columns: vec![string_output_column("s"), string_output_column("k")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        assign_test_column_ids(&mut scan);
        let PlanNodeKind::Scan(scan_node) = &scan.kind else {
            panic!("expected scan");
        };
        let s_id = scan_node
            .columns
            .iter()
            .find(|c| c.name == "s")
            .expect("s output")
            .column_id;
        let k_id = scan_node
            .columns
            .iter()
            .find(|c| c.name == "k")
            .expect("k output")
            .column_id;

        let mut scalars = ScalarArena::new();
        let scan_expr = logical_plan_to_opt_expr(&scan, &mut scalars);
        let items = vec![
            SortItem {
                expr: column_ref_with_id("s", s_id),
                asc: true,
                nulls_first: false,
            },
            SortItem {
                expr: column_ref_with_id("k", k_id),
                asc: true,
                nulls_first: false,
            },
        ];
        let sort = OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: intern_sort_items(&mut scalars, &items),
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_expr],
        );
        let mut snapshots = HashMap::new();
        snapshots.insert("s".to_string(), named_snapshot("s", true));
        snapshots.insert("k".to_string(), named_snapshot("k", false));
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerColumnProvider { snapshots }));

        let (rewritten, arena) = run_rule_rewrite_expr(sort, scalars, &mut ctx);
        let Operator::LogicalSort(sort) = &rewritten.op else {
            panic!("expected LogicalSort root, got {:?}", rewritten.op);
        };
        let rewritten_items = materialize_sort_keys(&arena.borrow(), &sort.items);
        let key_names: Vec<&str> = rewritten_items
            .iter()
            .map(|item| match &item.expr.kind {
                ExprKind::ColumnRef { column, .. } => column.as_str(),
                other => panic!("expected ColumnRef sort item, got {other:?}"),
            })
            .collect();
        assert_eq!(
            key_names,
            vec!["s", "k"],
            "when any Sort key needs decode, all keys must stay on string refs"
        );
        let Operator::LogicalDecode(decode) = &rewritten.unary_input().op else {
            panic!(
                "expected Decode below Sort for mixed keys, got {:?}",
                rewritten.unary_input().op
            );
        };
        assert_eq!(decode.mappings.len(), 2);
        assert!(
            decode
                .mappings
                .iter()
                .any(|m| m.dict_column == "__nr_dict_t_s" && m.output_column_id == s_id)
        );
        assert!(
            decode
                .mappings
                .iter()
                .any(|m| m.dict_column == "__nr_dict_t_k" && m.output_column_id == k_id)
        );
    }

    #[test]
    fn disable_rule_skips_dictionary_rewrite() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![s_column_ref()],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![
                    s_output_column(),
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "cnt".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx =
            RewriteContext::for_query(vec!["LowCardinalityDictionaryRewrite".to_string()]);
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        // With the rule disabled the plan must not contain a Decode
        // boundary or any dict-encoded scan output.
        assert!(
            !matches!(&rewritten.kind, PlanNodeKind::Decode(_)),
            "expected rule disabled to suppress Decode insertion"
        );
        let PlanNodeKind::Aggregate(_) = &rewritten.kind else {
            panic!("expected aggregate root");
        };
        let PlanNodeKind::Scan(scan) = &rewritten.unary_input().kind else {
            panic!("expected scan child");
        };
        assert!(scan.dict_columns.is_empty());
        assert!(scan.columns.iter().all(|c| c.name == "s"));
    }

    // --- Item 1 (Critical) regression: bare column name collision ---

    /// Provider that exposes a dictionary only when the scan's table
    /// AND column match. Lets a test register dict for `t1.name` but
    /// not `t2.name`.
    struct PerTableProvider {
        snapshot: DictionarySnapshot,
        table: String,
    }

    impl QueryDictionaryProvider for PerTableProvider {
        fn load_active_snapshot(
            &self,
            table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            if table.name.eq_ignore_ascii_case(&self.table)
                && column_name.eq_ignore_ascii_case(&self.snapshot.column_name)
            {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn make_named_table(name: &str, column: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: column.to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn named_output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        }
    }

    fn named_snapshot(column: &str, order_preserving: bool) -> DictionarySnapshot {
        let mut snap = sample_snapshot(order_preserving);
        snap.column_name = column.to_string();
        snap
    }

    #[test]
    fn join_with_same_column_name_only_decodes_dict_side() {
        // Two scans, each producing an output column called `name`.
        // Only `t1` has an active dictionary; `t2` has none. After the
        // rewrite, ONLY the `t1` branch must wear a Decode boundary,
        // and the `t2` branch must be untouched.
        let scan_t1 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t1", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_t2 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t2", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Cross,
                condition: None,
            }),
            vec![scan_t1, scan_t2],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerTableProvider {
            snapshot: named_snapshot("name", true),
            table: "t1".to_string(),
        }));
        let rewritten = run_pipeline_rewrite(join, &mut ctx);
        let PlanNodeKind::Join(_) = &rewritten.kind else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Left side: must be Decode(Scan with dict_columns).
        let left_plan = rewritten.left();
        let PlanNodeKind::Decode(left_decode) = &left_plan.kind else {
            panic!("expected left side to be Decode, got {:?}", left_plan);
        };
        assert_eq!(left_decode.mappings.len(), 1);
        assert_eq!(left_decode.mappings[0].dict_column, "__nr_dict_t1_name");
        let PlanNodeKind::Scan(left_scan) = &left_plan.unary_input().kind else {
            panic!("expected scan under left decode");
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        // Right side: must be a plain Scan, no Decode, no dict_columns.
        let right_plan = rewritten.right();
        let PlanNodeKind::Scan(right_scan) = &right_plan.kind else {
            panic!("expected right side to be plain Scan, got {:?}", right_plan);
        };
        assert!(
            right_scan.dict_columns.is_empty(),
            "t2.name has no dict snapshot — must not be dict-encoded"
        );
        assert!(
            right_scan.columns.iter().all(|c| c.name == "name"),
            "t2 scan output must contain only the original `name` column"
        );
    }

    // --- Item 3 (Important) regression: project rename propagates dict mapping ---

    #[test]
    fn project_alias_propagates_dict_through_join_boundary() {
        // SELECT s AS t FROM dict_table feeding a cross join into a
        // no-dict scan. After rewrite, the alias-side branch must wrap
        // the Project with a Decode driven by the dict slot.
        let scan_left = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        // Project: SELECT s AS t.
        let project = LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: s_column_ref(),
                    output_name: "t".to_string(),
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                }],
                output_qualifier: None,
            }),
            vec![scan_left],
            None,
        );
        // Right side: a no-dict scan over a different table.
        let scan_right = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("other", "x"),
                alias: None,
                columns: vec![named_output_column("x")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Cross,
                condition: None,
            }),
            vec![project, scan_right],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(join, &mut ctx);
        let PlanNodeKind::Join(_) = &rewritten.kind else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Left side: Decode wrapping a Project wrapping the dict-enabled Scan.
        let left_plan = rewritten.left();
        let PlanNodeKind::Decode(left_decode) = &left_plan.kind else {
            panic!(
                "expected left to be Decode(Project(Scan)), got {:?}",
                left_plan
            );
        };
        assert_eq!(left_decode.mappings.len(), 1);
        assert_eq!(left_decode.mappings[0].dict_column, "__nr_dict_t_s");
        // The decode's string_column must reference the alias name `t`,
        // not the underlying `s` — otherwise downstream consumers
        // looking up by alias would not match.
        assert_eq!(left_decode.mappings[0].string_column, "t");
    }

    #[test]
    fn project_propagates_dict_slot_to_aggregate() {
        // Bug A regression: ColumnPruning leaves a residual Project
        // between Scan and Aggregate. The rewriter must extend that
        // Project with a sibling pass-through item for the hidden dict
        // slot so the codegen ExprScope above the Project carries the
        // `__nr_dict_t_s` slot. Without it the Aggregate's rewritten
        // group-by ColumnRef on `__nr_dict_t_s` fails to resolve
        // (`Column '__nr_dict_t_s' cannot be resolved`).
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let project = LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: s_column_ref(),
                    output_name: "s".to_string(),
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![s_column_ref()],
                aggregates: vec![],
                output_columns: vec![s_output_column()],
                already_pushed: false,
            }),
            vec![project],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        // Outer shape: Decode → Aggregate(grp=__nr_dict_t_s) → Project →
        // Scan. The critical assertion is that the Project items contain
        // BOTH the original `s` item AND a pass-through `__nr_dict_t_s`
        // item — that's what makes the Aggregate's dict-slot group-by
        // resolvable at codegen time.
        let PlanNodeKind::Decode(decode) = &rewritten.kind else {
            panic!("expected decode root, got {rewritten:?}");
        };
        let aggregate_plan = rewritten.unary_input();
        let PlanNodeKind::Aggregate(agg) = &aggregate_plan.kind else {
            panic!("expected aggregate under decode");
        };
        let key = agg.group_by.first().expect("group by present");
        let ExprKind::ColumnRef { column, .. } = &key.kind else {
            panic!("group-by must be a column ref");
        };
        assert_eq!(column, "__nr_dict_t_s");
        let PlanNodeKind::Project(proj) = &aggregate_plan.unary_input().kind else {
            panic!("expected project under aggregate");
        };
        let item_names: Vec<&str> = proj.items.iter().map(|i| i.output_name.as_str()).collect();
        assert!(
            item_names.contains(&"s"),
            "project must still emit the original `s` item; got {item_names:?}"
        );
        assert!(
            item_names.contains(&"__nr_dict_t_s"),
            "project must propagate the hidden dict slot; got {item_names:?}"
        );
        let source_item_id = proj
            .items
            .iter()
            .find(|i| i.output_name == "s")
            .expect("source item present")
            .output_column_id;
        assert_eq!(decode.mappings[0].source_column_id, source_item_id);
        assert_eq!(decode.mappings[0].output_column_id, source_item_id);
        // The pass-through dict item must be a plain ColumnRef on the
        // dict slot with Int32 data type.
        let dict_item = proj
            .items
            .iter()
            .find(|i| i.output_name == "__nr_dict_t_s")
            .expect("dict item present");
        assert_eq!(dict_item.expr.data_type, DataType::Int32);
        assert_eq!(dict_item.output_column_id, source_item_id);
        let ExprKind::ColumnRef {
            column: c,
            column_id,
            ..
        } = &dict_item.expr.kind
        else {
            panic!("dict pass-through must be a ColumnRef");
        };
        assert_eq!(c, "__nr_dict_t_s");
        assert_eq!(*column_id, source_item_id);
    }

    // -------------------------------------------------------------------
    // Task 8 — completion tests
    // -------------------------------------------------------------------

    use crate::sql::analysis::{BinOp, ExprKind as Ek};
    use crate::sql::planner::plan::{LogicalJoinNode, LogicalUnionNode};

    /// Provider that exposes the same snapshot for ANY (table, column)
    /// query — used to model a logical column whose dict mapping is
    /// shared across multiple physical scans (e.g. two scans of the
    /// same StarRocks table on different branches of a join).
    struct SharedSnapshotProvider {
        snapshot: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for SharedSnapshotProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(Some(self.snapshot.clone()))
        }
    }

    /// Provider that hands out different snapshots per scan table —
    /// `version` is keyed off the table name so two scans of "t1" and
    /// "t2" disagree on the snapshot version. Used to model "two scans,
    /// two distinct dict mappings, can't compare dict ids directly".
    struct PerTableVersionProvider {
        base: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for PerTableVersionProvider {
        fn load_active_snapshot(
            &self,
            table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            let mut snap = self.base.clone();
            snap.version = match table.name.as_str() {
                "t1" => 1,
                _ => 2,
            };
            Ok(Some(snap))
        }
    }

    fn col_ref(qualifier: Option<&str>, name: &str) -> TypedExpr {
        TypedExpr {
            kind: Ek::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: qualifier.map(|q| q.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: Ek::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn test_helper_allocates_distinct_ids_for_self_join_aliases() {
        let scan_left = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: Some("l".to_string()),
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_right = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: Some("r".to_string()),
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let mut join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Inner,
                condition: Some(eq(col_ref(Some("l"), "s"), col_ref(Some("r"), "s"))),
            }),
            vec![scan_left, scan_right],
            None,
        );

        assign_test_column_ids(&mut join);

        let PlanNodeKind::Scan(left_scan) = &join.left().kind else {
            panic!("expected left scan");
        };
        let PlanNodeKind::Scan(right_scan) = &join.right().kind else {
            panic!("expected right scan");
        };
        let left_id = left_scan.columns[0].column_id;
        let right_id = right_scan.columns[0].column_id;
        assert_ne!(
            left_id, right_id,
            "self-join scan outputs must receive globally distinct ColumnIds"
        );

        let PlanNodeKind::Join(join_node) = &join.kind else {
            panic!("expected join root");
        };
        let condition = join_node.condition.as_ref().expect("join condition");
        let Ek::BinaryOp { left, right, .. } = &condition.kind else {
            panic!("expected BinaryOp condition");
        };
        let Ek::ColumnRef {
            column_id: left_ref_id,
            ..
        } = &left.kind
        else {
            panic!("expected left ColumnRef");
        };
        let Ek::ColumnRef {
            column_id: right_ref_id,
            ..
        } = &right.kind
        else {
            panic!("expected right ColumnRef");
        };
        assert_eq!(
            *left_ref_id, left_id,
            "qualified l.s must bind to the left scan output"
        );
        assert_eq!(
            *right_ref_id, right_id,
            "qualified r.s must bind to the right scan output"
        );
    }

    #[test]
    fn same_dictionary_join_uses_dict_keys() {
        // Two scans, both with the same `name` column and a matching
        // (shared) dictionary snapshot. The equi-join predicate must be
        // rewritten to compare the dict id slots — and NO Decode must
        // appear between the join and either scan.
        let scan_t1 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t1", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_t2 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t2", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Inner,
                condition: Some(eq(col_ref(Some("t1"), "name"), col_ref(Some("t2"), "name"))),
            }),
            vec![scan_t1, scan_t2],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let rewritten = run_pipeline_rewrite(join, &mut ctx);
        let PlanNodeKind::Join(join) = &rewritten.kind else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Both sides must be plain Scans (no Decode) since the dict
        // columns are kept through the equi-join.
        let PlanNodeKind::Scan(left_scan) = &rewritten.left().kind else {
            panic!(
                "expected left scan kept dict-encoded, got {:?}",
                rewritten.left()
            );
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        let left_dict_id = left_scan
            .columns
            .iter()
            .find(|c| c.name == "__nr_dict_t1_name")
            .expect("left dict output")
            .column_id;
        let PlanNodeKind::Scan(right_scan) = &rewritten.right().kind else {
            panic!(
                "expected right scan kept dict-encoded, got {:?}",
                rewritten.right()
            );
        };
        assert_eq!(right_scan.dict_columns.len(), 1);
        let right_dict_id = right_scan
            .columns
            .iter()
            .find(|c| c.name == "__nr_dict_t2_name")
            .expect("right dict output")
            .column_id;
        // The condition must now compare dict columns directly.
        let cond = join
            .condition
            .as_ref()
            .expect("equi-join keeps a condition");
        let Ek::BinaryOp {
            left, op, right, ..
        } = &cond.kind
        else {
            panic!("expected BinaryOp condition");
        };
        assert!(matches!(op, BinOp::Eq));
        let Ek::ColumnRef {
            column: l_col,
            column_id: l_id,
            ..
        } = &left.kind
        else {
            panic!("expected left ColumnRef in condition");
        };
        let Ek::ColumnRef {
            column: r_col,
            column_id: r_id,
            ..
        } = &right.kind
        else {
            panic!("expected right ColumnRef in condition");
        };
        assert_eq!(l_col, "__nr_dict_t1_name");
        assert_eq!(r_col, "__nr_dict_t2_name");
        assert_eq!(*l_id, left_dict_id);
        assert_eq!(*r_id, right_dict_id);
    }

    /// Helper: build the same two-scan equi-join fixture used by
    /// `same_dictionary_join_uses_dict_keys`, parameterized on
    /// `JoinKind`. Returns the rewritten plan for the caller to assert
    /// on.
    fn run_same_dict_join_with_kind(kind: crate::sql::analysis::JoinKind) -> LogicalPlanNode {
        let scan_t1 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t1", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_t2 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t2", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: kind,
                condition: Some(eq(col_ref(Some("t1"), "name"), col_ref(Some("t2"), "name"))),
            }),
            vec![scan_t1, scan_t2],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        run_pipeline_rewrite(join, &mut ctx)
    }

    /// Assert that the rewritten plan is a `Join` whose left/right
    /// inputs are plain `Scan`s (no decode wrappers) and whose equi
    /// condition compares the dict id slots on each side.
    fn assert_dict_id_equi_join(rewritten: LogicalPlanNode) {
        let PlanNodeKind::Join(join) = &rewritten.kind else {
            panic!("expected join root, got {rewritten:?}");
        };
        let PlanNodeKind::Scan(left_scan) = &rewritten.left().kind else {
            panic!(
                "expected left scan kept dict-encoded, got {:?}",
                rewritten.left()
            );
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        let left_dict_id = left_scan
            .columns
            .iter()
            .find(|c| c.name == "__nr_dict_t1_name")
            .expect("left dict output")
            .column_id;
        let PlanNodeKind::Scan(right_scan) = &rewritten.right().kind else {
            panic!(
                "expected right scan kept dict-encoded, got {:?}",
                rewritten.right()
            );
        };
        assert_eq!(right_scan.dict_columns.len(), 1);
        let right_dict_id = right_scan
            .columns
            .iter()
            .find(|c| c.name == "__nr_dict_t2_name")
            .expect("right dict output")
            .column_id;
        let cond = join
            .condition
            .as_ref()
            .expect("equi-join keeps a condition");
        let Ek::BinaryOp {
            left, op, right, ..
        } = &cond.kind
        else {
            panic!("expected BinaryOp condition");
        };
        assert!(matches!(op, BinOp::Eq));
        let Ek::ColumnRef {
            column: l_col,
            column_id: l_id,
            ..
        } = &left.kind
        else {
            panic!("expected left ColumnRef in condition");
        };
        let Ek::ColumnRef {
            column: r_col,
            column_id: r_id,
            ..
        } = &right.kind
        else {
            panic!("expected right ColumnRef in condition");
        };
        assert_eq!(l_col, "__nr_dict_t1_name");
        assert_eq!(r_col, "__nr_dict_t2_name");
        assert_eq!(*l_id, left_dict_id);
        assert_eq!(*r_id, right_dict_id);
    }

    #[test]
    fn left_outer_same_dictionary_join_uses_dict_keys() {
        // LEFT OUTER JOIN over two dict-encoded sides with compatible
        // snapshots: the equi-key comparison is unchanged (the join
        // itself generates the NULL on unmatched right rows — the
        // equi-comparison happens before NULL padding). The rewrite
        // must keep dict columns on both sides and compare on dict ids.
        let rewritten = run_same_dict_join_with_kind(crate::sql::analysis::JoinKind::LeftOuter);
        assert_dict_id_equi_join(rewritten);
    }

    #[test]
    fn semi_same_dictionary_join_uses_dict_keys() {
        // LEFT SEMI JOIN over two dict-encoded sides with compatible
        // snapshots: same reasoning as the outer case — the semi-join
        // existence test reduces to the equi-key comparison, which is
        // safe to perform on dict ids.
        let rewritten = run_same_dict_join_with_kind(crate::sql::analysis::JoinKind::LeftSemi);
        assert_dict_id_equi_join(rewritten);
    }

    #[test]
    fn different_dictionary_join_decodes_keys() {
        // Two scans with disagreeing dict versions per table: the
        // rewriter must decode each side before the join (matching
        // Task 7's conservative boundary).
        let scan_t1 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t1", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_t2 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t2", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Inner,
                condition: Some(eq(col_ref(Some("t1"), "name"), col_ref(Some("t2"), "name"))),
            }),
            vec![scan_t1, scan_t2],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerTableVersionProvider {
            base: named_snapshot("name", true),
        }));
        let rewritten = run_pipeline_rewrite(join, &mut ctx);
        let PlanNodeKind::Join(_) = &rewritten.kind else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Both sides must be wrapped in Decode because the snapshots
        // differ on `version`.
        let PlanNodeKind::Decode(_) = &rewritten.left().kind else {
            panic!(
                "expected left Decode for version-mismatched dicts, got {:?}",
                rewritten.left()
            );
        };
        let PlanNodeKind::Decode(_) = &rewritten.right().kind else {
            panic!(
                "expected right Decode for version-mismatched dicts, got {:?}",
                rewritten.right()
            );
        };
    }

    #[test]
    fn union_all_same_dictionary_preserves_dict() {
        // Two UNION ALL inputs over different physical tables (t1, t2)
        // that share a single dict snapshot for their `name` column.
        // The union output must carry the dict binding upward (no
        // Decode immediately below either input).
        let scan_t1 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t1", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_t2 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t2", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let union = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![],
            }),
            vec![scan_t1, scan_t2],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let rewritten = run_pipeline_rewrite(union, &mut ctx);
        let PlanNodeKind::Union(union) = &rewritten.kind else {
            panic!("expected union root, got {rewritten:?}");
        };
        assert!(union.all, "must preserve UNION ALL semantics");
        for (i, input) in rewritten.children.iter().enumerate() {
            let PlanNodeKind::Scan(scan) = &input.kind else {
                panic!("expected union input {i} to remain a Scan, got {:?}", input);
            };
            assert_eq!(scan.dict_columns.len(), 1);
            assert!(
                scan.columns
                    .iter()
                    .any(|c| c.name == format!("__nr_dict_t{}_name", i + 1)),
                "scan {i} must expose its dict slot"
            );
        }
    }

    #[test]
    fn union_distinct_always_decodes() {
        // UNION DISTINCT must decode every input regardless of snapshot
        // compatibility, because set-distinct semantics hash on the
        // string value.
        let scan_t1 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t1", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let scan_t2 = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_named_table("t2", "name"),
                alias: None,
                columns: vec![named_output_column("name")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let union = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: false,
                output_columns: vec![],
            }),
            vec![scan_t1, scan_t2],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let rewritten = run_pipeline_rewrite(union, &mut ctx);
        let PlanNodeKind::Union(union) = &rewritten.kind else {
            panic!("expected union root, got {rewritten:?}");
        };
        assert!(!union.all);
        for input in &rewritten.children {
            assert!(
                matches!(&input.kind, PlanNodeKind::Decode(_)),
                "UNION DISTINCT input must be wrapped in Decode, got {:?}",
                input
            );
        }
    }

    #[test]
    fn count_col_consumes_dict_id() {
        // COUNT(s) on a dict-encoded column: the aggregate's arg must
        // be rewritten to the dict slot, without inserting a Decode
        // below the aggregate for that argument path.
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![s_column_ref()],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        // No string group-by → no top-level Decode wrapper.
        let PlanNodeKind::Aggregate(agg) = &rewritten.kind else {
            panic!(
                "expected aggregate root (no group-by string keys), got {:?}",
                rewritten
            );
        };
        assert_eq!(agg.aggregates.len(), 1);
        let arg = agg.aggregates[0].args.first().expect("count(s) has 1 arg");
        let Ek::ColumnRef {
            column, column_id, ..
        } = &arg.kind
        else {
            panic!("count arg must be a ColumnRef");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(arg.data_type, DataType::Int32);
        // The scan itself must still be intact under the aggregate.
        let PlanNodeKind::Scan(scan) = &rewritten.unary_input().kind else {
            panic!("expected scan under aggregate");
        };
        assert_eq!(scan.dict_columns.len(), 1);
        let dict_output_id = scan
            .columns
            .iter()
            .find(|c| c.name == "__nr_dict_t_s")
            .expect("scan dict output")
            .column_id;
        assert_eq!(*column_id, dict_output_id);
    }

    #[test]
    fn min_non_order_preserving_decodes() {
        // MIN(s) on a non-order-preserving snapshot. `min` is NOT on the
        // dict-id allowlist (`DICT_AGG_FUNCTIONS`), and `s`'s only use is
        // this `min`, so the collector BLOCKLISTS `s` and the scan never
        // dict-encodes it. The plan is therefore unchanged: a plain
        // `Aggregate(min(s))` over the string column, with NO dict slot on
        // the scan and NO Decode. (Encoding `s` would have made `min`
        // operate on Int32 dict ids — a latent wrong-result bug; not
        // encoding is the fix.)
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "min".to_string(),
                    args: vec![s_column_ref()],
                    distinct: false,
                    result_type: DataType::Utf8,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "m".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, false); // non-order-preserving
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        // No Decode anywhere: the scan keeps emitting the plain string.
        let PlanNodeKind::Aggregate(agg) = &rewritten.kind else {
            panic!("expected aggregate root, got {rewritten:?}");
        };
        let arg = agg.aggregates[0].args.first().expect("min(s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("min arg must remain a ColumnRef");
        };
        // CRITICAL: must NOT be the dict slot.
        assert_eq!(
            column, "s",
            "non-order-preserving min must keep the string column"
        );
        assert_eq!(arg.data_type, DataType::Utf8);
        // NEW correct shape: `s` was blocklisted (only consumed by the
        // non-allowlisted `min`), so the scan has NO dict encoding.
        let PlanNodeKind::Scan(scan) = &rewritten.unary_input().kind else {
            panic!("expected scan directly under aggregate (no Decode)");
        };
        assert!(
            scan.dict_columns.is_empty(),
            "s must not be dict-encoded — its only use is min(s); got {:?}",
            scan.dict_columns
        );
        assert!(
            scan.columns.iter().all(|c| c.name == "s"),
            "scan output must contain only the original `s` column"
        );
    }

    #[test]
    fn min_order_preserving_decodes_before_aggregate() {
        // MIN(s) on an order-preserving snapshot. Even though the
        // dictionary is order-preserving, `min` is NOT on the dict-id
        // allowlist (`DICT_AGG_FUNCTIONS`) — rewriting the arg to the
        // Int32 dict slot would make the aggregate emit Int32 dict ids
        // under a still-Utf8 result column. Because `s`'s only use is this
        // `min`, the collector BLOCKLISTS `s`, so the scan never
        // dict-encodes it and the plan is unchanged: a plain
        // `Aggregate(min(s))` over the string column, with NO dict slot on
        // the scan and NO Decode. See `DICT_AGG_FUNCTIONS`.
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "min".to_string(),
                    args: vec![s_column_ref()],
                    distinct: false,
                    result_type: DataType::Utf8,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "m".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true); // order-preserving
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        let PlanNodeKind::Aggregate(agg) = &rewritten.kind else {
            panic!("expected aggregate root, got {rewritten:?}");
        };
        let arg = agg.aggregates[0].args.first().expect("min(s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("min arg must remain a ColumnRef");
        };
        // CRITICAL: the arg must still reference the string column,
        // NOT the Int32 dict slot — otherwise the aggregate's declared
        // Utf8 result column would carry Int32 dict ids.
        assert_eq!(
            column, "s",
            "min(s) must keep the string column even when the snapshot is order-preserving"
        );
        assert_eq!(arg.data_type, DataType::Utf8);
        // NEW correct shape: `s` was blocklisted (only consumed by the
        // non-allowlisted `min`), so the scan has NO dict encoding.
        let PlanNodeKind::Scan(scan) = &rewritten.unary_input().kind else {
            panic!("expected scan directly under aggregate (no Decode)");
        };
        assert!(
            scan.dict_columns.is_empty(),
            "s must not be dict-encoded — its only use is min(s); got {:?}",
            scan.dict_columns
        );
        assert!(
            scan.columns.iter().all(|c| c.name == "s"),
            "scan output must contain only the original `s` column"
        );
    }

    #[test]
    fn aggregate_unsupported_function_arg_not_dict_encoded() {
        // SUM(murmur_hash3_32(s)) GROUP BY k. `sum` is not on
        // `DICT_AGG_FUNCTIONS`, and its argument is a function call over
        // `s`, so the collector blocklists `s` (it is consumed in an
        // unsafe position — the murmur hash must hash the STRING, not the
        // Int32 dict id). The scan must therefore NOT dict-encode `s`,
        // and the plan is otherwise unchanged. This is the regression for
        // the grf_broadcast wrong-fingerprint bug: hashing the dict id
        // produced a different fingerprint than hashing the string.
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "s".to_string(),
                            data_type: DataType::Utf8,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "k".to_string(),
                            data_type: DataType::Int32,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![
                    s_output_column(),
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "k".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        // murmur_hash3_32(s)
        let murmur = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "murmur_hash3_32".to_string(),
                args: vec![s_column_ref()],
                distinct: false,
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        let k_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: "k".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![k_ref],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![murmur],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "k".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "h".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        // Plan must be unchanged: Aggregate over a plain Scan, no Decode.
        let PlanNodeKind::Aggregate(agg) = &rewritten.kind else {
            panic!("expected aggregate root (no Decode), got {rewritten:?}");
        };
        // The aggregate arg must still hash the STRING column `s`.
        let arg = agg.aggregates[0].args.first().expect("sum has 1 arg");
        let ExprKind::FunctionCall { name, args, .. } = &arg.kind else {
            panic!("sum arg must remain a murmur function call");
        };
        assert_eq!(name, "murmur_hash3_32");
        let Ek::ColumnRef { column, .. } = &args[0].kind else {
            panic!("murmur arg must be a ColumnRef");
        };
        assert_eq!(
            column, "s",
            "murmur must hash the string column, not the dict id"
        );
        // CRITICAL: the scan must not dict-encode `s`.
        let PlanNodeKind::Scan(scan) = &rewritten.unary_input().kind else {
            panic!("expected scan directly under aggregate (no Decode)");
        };
        assert!(
            scan.dict_columns.is_empty(),
            "s must not be dict-encoded — its only use is the unsupported murmur_hash3_32; got {:?}",
            scan.dict_columns
        );
        assert!(
            scan.columns.iter().any(|c| c.name == "s"),
            "scan must still expose the original `s` column"
        );
        assert!(
            scan.columns
                .iter()
                .all(|c| !c.name.starts_with("__nr_dict_")),
            "scan must not expose any dict slot; got {:?}",
            scan.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn count_distinct_col_consumes_dict_id() {
        // COUNT(DISTINCT s): dict ids are 1:1 with source strings, so
        // distinct-on-dict-id has the same cardinality as
        // distinct-on-string. The arg must be rewritten to the Int32
        // dict slot, and the aggregate's BIGINT result is independent
        // of the input encoding — no output-type mismatch.
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![s_column_ref()],
                    distinct: true,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(aggregate, &mut ctx);
        let PlanNodeKind::Aggregate(agg) = &rewritten.kind else {
            panic!("expected aggregate root, got {rewritten:?}");
        };
        assert_eq!(agg.aggregates.len(), 1);
        assert!(
            agg.aggregates[0].distinct,
            "DISTINCT flag must be preserved through the rewrite"
        );
        let arg = agg.aggregates[0]
            .args
            .first()
            .expect("count(DISTINCT s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("count(DISTINCT) arg must be a ColumnRef");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(arg.data_type, DataType::Int32);
    }

    #[test]
    fn cte_anchor_always_decodes_at_boundary() {
        // TODO(task-8-cte): multi-consumer CTEs with matching dict
        // snapshots across all consumers could keep the dict column
        // through the producer/consumer boundary. Task 8 keeps the
        // conservative behaviour from Task 7 (decode at the boundary)
        // because:
        //
        // 1. Single-use CTEs are inlined before this rule runs (see
        //    `cte_rewrite::inline_single_use_ctes`), so the observable
        //    surface for the unrelaxed path is multi-consumer CTEs
        //    only.
        // 2. Implementing consumer-side divergence detection cleanly
        //    requires a fix-up pass over the rewrite (the current
        //    top-down rewrite cannot see all consumers of a CTE while
        //    rewriting its producer).
        // 3. Task 9's SQL regressions do not exercise multi-consumer
        //    CTEs with dict columns, so the gain is small.
        //
        // This test pins the current behaviour so a future relaxation
        // is a deliberate change rather than an accidental one.
        use crate::sql::planner::plan::{
            LogicalCTEAnchorNode, LogicalCTEConsumeNode, LogicalCTEProduceNode, PlanNodeKind,
        };
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: make_table(),
                alias: None,
                columns: vec![s_output_column()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let cte_id: crate::sql::analysis::cte::CteId = 7;
        let produce = LogicalPlanNode::new(
            PlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: cte_id,
                output_columns: vec![s_output_column()],
            }),
            vec![scan],
            None,
        );
        let consumer = LogicalPlanNode::new(
            PlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: "c".to_string(),
                output_columns: vec![s_output_column()],
                producer_column_ids: vec![s_output_column().column_id],
            }),
            vec![],
            None,
        );
        let anchor = LogicalPlanNode::new(
            PlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: cte_id }),
            vec![produce, consumer],
            None,
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let rewritten = run_pipeline_rewrite(anchor, &mut ctx);
        // Conservative: a Decode boundary must sit inside the CTE
        // producer subtree (between the producer and its scan), so the
        // producer output is all strings. Task 8 keeps this Task 7
        // behaviour — no dict columns leak past the producer.
        let PlanNodeKind::CTEAnchor(_) = &rewritten.kind else {
            panic!("expected CTEAnchor root, got {rewritten:?}");
        };
        let produce_plan = rewritten.child(0);
        let PlanNodeKind::CTEProduce(_) = &produce_plan.kind else {
            panic!("expected CTEProduce under anchor");
        };
        assert!(
            matches!(&produce_plan.unary_input().kind, PlanNodeKind::Decode(_)),
            "Task 8 keeps the conservative CTE producer-side Decode; got {:?}",
            produce_plan.unary_input()
        );
    }
}
