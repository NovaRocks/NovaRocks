//! PushDownPredicateProject — `Filter(Project)` rewrite.
//!
//! Pushes conjuncts that reference only pass-through (i.e. bare
//! `ColumnRef`) projection items below the Project, leaving conjuncts
//! that touch computed expressions as a residual Filter above. One step
//! only — the rewrite pipeline's bottom-up walker will push further at the
//! next round.
//!
//! Migrated to `OptExpr` / `LogicalRewriteRule`.

use std::collections::HashMap;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{FilterOp, Operator, ProjectOp, ScalarProjectItem};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::wrap_remaining_filter_opt_scalar;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode, SortKey};
use crate::sql::optimizer::scalar_expr;

pub(crate) struct PushDownPredicateProject;

impl LogicalRewriteRule for PushDownPredicateProject {
    fn name(&self) -> &'static str {
        "PushDownPredicateProject"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalFilter(_))
            && expr
                .children
                .first()
                .map(|c| matches!(&c.op, Operator::LogicalProject(_)))
                .unwrap_or(false)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            mut children,
            required_output_columns: _,
        } = expr;
        let Operator::LogicalFilter(filter) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let project_expr = children.remove(0);
        let OptExpr {
            op: project_op,
            children: mut project_children,
            required_output_columns,
        } = project_expr;
        let Operator::LogicalProject(proj) = project_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if project_children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let project_input = project_children.remove(0);

        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();

        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(&arena, filter.predicate, &mut conjuncts);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            match remap_predicate_through_project(&mut arena, conj, &proj.items) {
                Some(rewritten) => pushable.push(rewritten),
                None => remaining.push(conj),
            }
        }

        if pushable.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        let Some(pushed_id) = scalar_expr::combine_conjuncts(&mut arena, pushable) else {
            return Ok(RewriteResult::Unchanged);
        };
        let new_child = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![project_input],
        );
        let new_project = OptExpr {
            op: Operator::LogicalProject(ProjectOp {
                items: proj.items,
                output_qualifier: proj.output_qualifier,
            }),
            children: vec![new_child],
            required_output_columns,
        };
        let result = wrap_remaining_filter_opt_scalar(new_project, remaining, &mut arena);
        Ok(RewriteResult::Changed(result))
    }
}

fn remap_predicate_through_project(
    arena: &mut ScalarArena,
    predicate: ScalarId,
    project_items: &[ScalarProjectItem],
) -> Option<ScalarId> {
    let bindings = project_bindings(arena, project_items);
    remap_scalar(arena, predicate, &bindings)
}

fn project_bindings(
    arena: &ScalarArena,
    project_items: &[ScalarProjectItem],
) -> HashMap<ColumnId, Option<ScalarId>> {
    let mut bindings = HashMap::new();
    for item in project_items {
        if item.output_column_id == ColumnId::UNSET {
            continue;
        }
        let input = match arena.node(item.expr) {
            ScalarNode::ColumnRef(input_id) if *input_id != ColumnId::UNSET => Some(item.expr),
            _ => None,
        };
        bindings.insert(item.output_column_id, input);
    }
    bindings
}

fn remap_scalar(
    arena: &mut ScalarArena,
    expr: ScalarId,
    bindings: &HashMap<ColumnId, Option<ScalarId>>,
) -> Option<ScalarId> {
    let node = arena.node(expr).clone();
    let data_type = arena.data_type(expr).clone();
    let nullable = arena.nullable(expr);
    match node {
        ScalarNode::ColumnRef(column_id) => bindings.get(&column_id).copied().flatten(),
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => Some(expr),
        ScalarNode::BinaryOp { op, left, right } => {
            let left = remap_scalar(arena, left, bindings)?;
            let right = remap_scalar(arena, right, bindings)?;
            Some(arena.intern(
                ScalarNode::BinaryOp { op, left, right },
                data_type,
                nullable,
            ))
        }
        ScalarNode::UnaryOp { op, child } => {
            let child = remap_scalar(arena, child, bindings)?;
            Some(arena.intern(ScalarNode::UnaryOp { op, child }, data_type, nullable))
        }
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => {
            let args = remap_scalar_vec(arena, args, bindings)?;
            Some(arena.intern(
                ScalarNode::FunctionCall {
                    name,
                    args,
                    distinct,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::LambdaFunction { params, body } => {
            let body = remap_scalar(arena, body, bindings)?;
            Some(arena.intern(
                ScalarNode::LambdaFunction { params, body },
                data_type,
                nullable,
            ))
        }
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => {
            let args = remap_scalar_vec(arena, args, bindings)?;
            let order_by = remap_sort_keys(arena, order_by, bindings)?;
            Some(arena.intern(
                ScalarNode::AggregateCall {
                    name,
                    args,
                    distinct,
                    order_by,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::Cast { child, target } => {
            let child = remap_scalar(arena, child, bindings)?;
            Some(arena.intern(ScalarNode::Cast { child, target }, data_type, nullable))
        }
        ScalarNode::IsNull { child, negated } => {
            let child = remap_scalar(arena, child, bindings)?;
            Some(arena.intern(ScalarNode::IsNull { child, negated }, data_type, nullable))
        }
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            let child = remap_scalar(arena, child, bindings)?;
            let list = remap_scalar_vec(arena, list, bindings)?;
            Some(arena.intern(
                ScalarNode::InList {
                    child,
                    list,
                    negated,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => {
            let child = remap_scalar(arena, child, bindings)?;
            let low = remap_scalar(arena, low, bindings)?;
            let high = remap_scalar(arena, high, bindings)?;
            Some(arena.intern(
                ScalarNode::Between {
                    child,
                    low,
                    high,
                    negated,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => {
            let child = remap_scalar(arena, child, bindings)?;
            let pattern = remap_scalar(arena, pattern, bindings)?;
            Some(arena.intern(
                ScalarNode::Like {
                    child,
                    pattern,
                    negated,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let operand = remap_optional_scalar(arena, operand, bindings)?;
            let when_then = when_then
                .into_iter()
                .map(|(when, then)| {
                    Some((
                        remap_scalar(arena, when, bindings)?,
                        remap_scalar(arena, then, bindings)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            let else_expr = remap_optional_scalar(arena, else_expr, bindings)?;
            Some(arena.intern(
                ScalarNode::Case {
                    operand,
                    when_then,
                    else_expr,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => {
            let child = remap_scalar(arena, child, bindings)?;
            Some(arena.intern(
                ScalarNode::IsTruthValue {
                    child,
                    value,
                    negated,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::Nested(child) => {
            let child = remap_scalar(arena, child, bindings)?;
            Some(arena.intern(ScalarNode::Nested(child), data_type, nullable))
        }
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => {
            let args = remap_scalar_vec(arena, args, bindings)?;
            let partition_by = remap_scalar_vec(arena, partition_by, bindings)?;
            let order_by = remap_sort_keys(arena, order_by, bindings)?;
            Some(arena.intern(
                ScalarNode::WindowCall {
                    name,
                    args,
                    distinct,
                    partition_by,
                    order_by,
                    window_frame,
                    ignore_nulls,
                },
                data_type,
                nullable,
            ))
        }
        ScalarNode::Lambda { params, body } => {
            let body = remap_scalar(arena, body, bindings)?;
            Some(arena.intern(ScalarNode::Lambda { params, body }, data_type, nullable))
        }
    }
}

fn remap_scalar_vec(
    arena: &mut ScalarArena,
    exprs: Vec<ScalarId>,
    bindings: &HashMap<ColumnId, Option<ScalarId>>,
) -> Option<Vec<ScalarId>> {
    exprs
        .into_iter()
        .map(|expr| remap_scalar(arena, expr, bindings))
        .collect()
}

fn remap_optional_scalar(
    arena: &mut ScalarArena,
    expr: Option<ScalarId>,
    bindings: &HashMap<ColumnId, Option<ScalarId>>,
) -> Option<Option<ScalarId>> {
    match expr {
        Some(expr) => Some(Some(remap_scalar(arena, expr, bindings)?)),
        None => Some(None),
    }
}

fn remap_sort_keys(
    arena: &mut ScalarArena,
    keys: Vec<SortKey>,
    bindings: &HashMap<ColumnId, Option<ScalarId>>,
) -> Option<Vec<SortKey>> {
    keys.into_iter()
        .map(|key| {
            Some(SortKey {
                expr: remap_scalar(arena, key.expr, bindings)?,
                asc: key.asc,
                nulls_first: key.nulls_first,
                display: key.display,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        FilterOp, Operator, ProjectOp, ScalarProjectItem, ScanOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::{self, ScalarArena};
    use arrow::datatypes::DataType;

    // Helper: intern a TypedExpr that uses ColumnId::UNSET (used for
    // passthrough project tests where column ids are not resolved).
    // NOTE: intern_typed panics on UNSET column refs — so we build items
    // using real ColumnIds here. Use ColumnId::new_for_test to keep tests
    // simple with stable ids.

    fn col_id(n: u32) -> ColumnId {
        ColumnId::new_for_test(n)
    }

    fn col_ref(name: &str, id: ColumnId) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn qualified_col_ref(qualifier: &str, name: &str, id: ColumnId) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: Some(qualifier.into()),
                column: name.into(),
            },
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn is_not_null(expr: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(expr),
                negated: true,
            },
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }

    fn make_table_def(cols: &[(&str, ColumnId)]) -> TableDef {
        TableDef {
            name: "t".into(),
            columns: cols
                .iter()
                .map(|(n, _)| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn scan_opt(arena: &mut ScalarArena, cols: &[(&str, ColumnId)]) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: make_table_def(cols),
            alias: None,
            columns: cols
                .iter()
                .map(|(n, id)| OutputColumn {
                    column_id: *id,
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    /// Build a passthrough project: each column in `cols` is a bare ColumnRef.
    fn passthrough_project_opt(
        arena: &mut ScalarArena,
        cols: &[(&str, ColumnId)],
        output_qualifier: Option<String>,
        input: OptExpr,
    ) -> OptExpr {
        let items: Vec<ScalarProjectItem> = cols
            .iter()
            .map(|(name, id)| {
                let expr_id = crate::sql::planner::optimizer_bridge::scalar::intern_typed(
                    arena,
                    &col_ref(name, *id),
                );
                ScalarProjectItem {
                    expr: expr_id,
                    output_name: (*name).into(),
                    output_column_id: *id,
                    expr_display: None,
                }
            })
            .collect();
        OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier,
            }),
            vec![input],
        )
    }

    fn filter_opt(arena: &mut ScalarArena, predicate: TypedExpr, child: OptExpr) -> OptExpr {
        let pred_id =
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &predicate);
        OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![child],
        )
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    // Test 1: SELECT a, b FROM (SELECT a, b FROM t) WHERE a = 1
    // Expected: Project(Filter(Scan)) — the predicate is pushed below the project.
    #[test]
    fn pushes_through_passthrough_project() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let b_id = col_id(2);
        let scan = scan_opt(&mut arena, &[("a", a_id), ("b", b_id)]);
        let project = passthrough_project_opt(&mut arena, &[("a", a_id), ("b", b_id)], None, scan);
        let filter = filter_opt(&mut arena, eq(col_ref("a", a_id), int_lit(1)), project);

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();

        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalProject(_) => match &out.children[0].op {
                    Operator::LogicalFilter(_) => match &out.children[0].children[0].op {
                        Operator::LogicalScan(_) => {}
                        other => panic!("expected Scan under Filter, got {:?}", other),
                    },
                    other => panic!("expected Filter under Project, got {:?}", other),
                },
                other => panic!("expected Project at top, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    #[test]
    fn rewrites_qualified_alias_predicate_before_pushdown() {
        let mut arena = ScalarArena::new();
        let item_sk_id = col_id(10);
        let scan = scan_opt(&mut arena, &[("item_sk", item_sk_id)]);
        let project = passthrough_project_opt(
            &mut arena,
            &[("item_sk", item_sk_id)],
            Some("asceding".into()),
            scan,
        );
        let filter = filter_opt(
            &mut arena,
            is_not_null(qualified_col_ref("asceding", "item_sk", item_sk_id)),
            project,
        );

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();

        match result {
            RewriteResult::Changed(out) => {
                assert!(matches!(out.op, Operator::LogicalProject(_)));
                let inner = &out.children[0];
                let Operator::LogicalFilter(inner_filter) = &inner.op else {
                    panic!("expected pushed Filter below Project");
                };
                let arena_ref = ctx.scalar_arena();
                let arena = arena_ref.borrow();
                let pred_expr = crate::sql::planner::optimizer_bridge::scalar::materialize(
                    &arena,
                    inner_filter.predicate,
                );
                let ExprKind::IsNull { expr, negated } = &pred_expr.kind else {
                    panic!("expected pushed IS NOT NULL predicate");
                };
                assert!(*negated);
                let ExprKind::ColumnRef {
                    column_id: pushed_col_id,
                    column,
                    ..
                } = &expr.kind
                else {
                    panic!("expected pushed predicate to reference the Project input column");
                };
                // The pushed predicate must reference the underlying input column
                // (by column_id, which is the semantic identity in the arena model).
                // The qualifier is display-only and may reflect the intern_typed order,
                // so we do not assert on it here.
                assert_eq!(*pushed_col_id, item_sk_id);
                assert_eq!(column, "item_sk");
            }
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    // Test 2: SELECT a+1 AS x FROM t WHERE x = 5
    // No conjuncts are pushable; rule must return Unchanged.
    #[test]
    fn does_not_push_through_computed_projection() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let x_id = col_id(2);
        let scan = scan_opt(&mut arena, &[("a", a_id)]);
        // Build: Project(Scan) with computed item x = a + 1.
        let computed_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", a_id)),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let computed_id =
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(&mut arena, &computed_expr);
        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: computed_id,
                    output_name: "x".into(),
                    output_column_id: x_id,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![scan],
        );
        let filter = filter_opt(&mut arena, eq(col_ref("x", x_id), int_lit(5)), project);

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "should not push through a computed projection"
        );
    }

    // Test 4: WHERE 1=1 (constant predicate, no column refs).
    // Expected shape: Project(Filter(Scan))
    #[test]
    fn pushes_constant_predicate_through_project() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let scan = scan_opt(&mut arena, &[("a", a_id)]);
        let project = passthrough_project_opt(&mut arena, &[("a", a_id)], None, scan);
        let one_eq_one = eq(int_lit(1), int_lit(1));
        let filter = filter_opt(&mut arena, one_eq_one, project);
        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();
        match result {
            RewriteResult::Changed(out) => {
                assert!(matches!(out.op, Operator::LogicalProject(_)));
                assert!(matches!(out.children[0].op, Operator::LogicalFilter(_)));
            }
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    // Test 3: AND of a pass-through ref (a = 1) and a computed-expr ref (x = 5)
    // Expected shape: Filter(Project(Filter(Scan)))
    #[test]
    fn partial_pushdown_through_project() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let x_id = col_id(2);
        let scan = scan_opt(&mut arena, &[("a", a_id)]);
        let computed_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", a_id)),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let passthrough_id = crate::sql::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &col_ref("a", a_id),
        );
        let computed_id =
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(&mut arena, &computed_expr);
        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![
                    ScalarProjectItem {
                        expr: passthrough_id,
                        output_name: "a".into(),
                        output_column_id: a_id,
                        expr_display: None,
                    },
                    ScalarProjectItem {
                        expr: computed_id,
                        output_name: "x".into(),
                        output_column_id: x_id,
                        expr_display: None,
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan],
        );
        let pred = and(
            eq(col_ref("a", a_id), int_lit(1)),
            eq(col_ref("x", x_id), int_lit(5)),
        );
        let filter = filter_opt(&mut arena, pred, project);

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();

        // Expected: Filter(Project(Filter(Scan)))
        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalFilter(_) => match &out.children[0].op {
                    Operator::LogicalProject(_) => match &out.children[0].children[0].op {
                        Operator::LogicalFilter(_) => {
                            match &out.children[0].children[0].children[0].op {
                                Operator::LogicalScan(_) => {}
                                other => panic!("expected Scan at bottom, got {:?}", other),
                            }
                        }
                        other => panic!("expected Filter under Project, got {:?}", other),
                    },
                    other => panic!("expected Project under outer Filter, got {:?}", other),
                },
                other => panic!("expected outer Filter at top, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }
}
