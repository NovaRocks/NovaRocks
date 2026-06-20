//! Expression normalization (ColumnId-independent comparable form) and
//! query-expression rewriting onto MV output columns.
//! StarRocks counterpart: EquationRewriter / ColumnRewriter (single-table cut).

use std::collections::HashMap;

use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, OutputColumn, UnOp};
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};

/// Canonical, ColumnId-independent expression form. Two exprs over the same
/// base table (through different ColumnId spaces) compare equal iff they are
/// structurally identical after base-name resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NormExpr {
    Column(String),
    Literal(String),
    Call {
        name: String,
        distinct: bool,
        args: Vec<NormExpr>,
    },
}

/// Returns None for unsupported expression kinds (window calls, subqueries,
/// lambdas, IS TRUE/FALSE) — callers must treat None as "cannot match"
/// (fail closed).
pub(crate) fn normalize(
    arena: &ScalarArena,
    expr: ScalarId,
    base_names: &HashMap<ColumnId, String>,
) -> Option<NormExpr> {
    let call = |name: &str, args: Vec<NormExpr>| NormExpr::Call {
        name: name.to_string(),
        distinct: false,
        args,
    };
    Some(match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => NormExpr::Column(base_names.get(column_id)?.clone()),
        // No constant folding (MVP): literals compare by their Debug
        // representation, so cross-width / cross-encoding constants such as
        // Int(5) vs LargeInt(5) or Decimal("100.0") vs Decimal("100.00") do
        // NOT match. This is fail-closed — it can only miss a rewrite, never
        // produce a wrong one.
        ScalarNode::Literal(HashableLiteral(value)) => NormExpr::Literal(format!("{value:?}")),
        ScalarNode::BinaryOp { left, op, right } => {
            let mut l = normalize(arena, *left, base_names)?;
            let mut r = normalize(arena, *right, base_names)?;
            // Canonicalize comparisons: Gt/Ge become flipped Lt/Le.
            let (name, commutative) = match op {
                BinOp::Add => ("add", true),
                BinOp::Mul => ("mul", true),
                BinOp::Sub => ("sub", false),
                BinOp::Div => ("div", false),
                BinOp::Mod => ("mod", false),
                BinOp::Eq => ("eq", true),
                BinOp::Ne => ("ne", true),
                BinOp::EqForNull => ("eq_for_null", true),
                BinOp::And => ("and", true),
                BinOp::Or => ("or", true),
                BinOp::Lt => ("lt", false),
                BinOp::Le => ("le", false),
                BinOp::Gt => {
                    std::mem::swap(&mut l, &mut r);
                    ("lt", false)
                }
                BinOp::Ge => {
                    std::mem::swap(&mut l, &mut r);
                    ("le", false)
                }
            };
            let mut args = vec![l, r];
            if commutative {
                args.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            }
            call(name, args)
        }
        ScalarNode::UnaryOp { op, child } => {
            let name = match op {
                UnOp::Not => "not",
                UnOp::Negate => "neg",
                UnOp::BitwiseNot => "bitnot",
            };
            call(name, vec![normalize(arena, *child, base_names)?])
        }
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => NormExpr::Call {
            name: format!("fn:{}", name.to_ascii_lowercase()),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| normalize(arena, *arg, base_names))
                .collect::<Option<Vec<_>>>()?,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            ..
        } => NormExpr::Call {
            name: format!("agg:{}", name.to_ascii_lowercase()),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| normalize(arena, *arg, base_names))
                .collect::<Option<Vec<_>>>()?,
        },
        ScalarNode::Cast { child, target } => call(
            &format!("cast:{target:?}"),
            vec![normalize(arena, *child, base_names)?],
        ),
        ScalarNode::IsNull { child, negated } => call(
            if *negated { "is_not_null" } else { "is_null" },
            vec![normalize(arena, *child, base_names)?],
        ),
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            let mut args = vec![normalize(arena, *child, base_names)?];
            let mut items = list
                .iter()
                .map(|item| normalize(arena, *item, base_names))
                .collect::<Option<Vec<_>>>()?;
            items.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            args.extend(items);
            call(if *negated { "not_in" } else { "in" }, args)
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => call(
            if *negated { "not_between" } else { "between" },
            vec![
                normalize(arena, *child, base_names)?,
                normalize(arena, *low, base_names)?,
                normalize(arena, *high, base_names)?,
            ],
        ),
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => call(
            if *negated { "not_like" } else { "like" },
            vec![
                normalize(arena, *child, base_names)?,
                normalize(arena, *pattern, base_names)?,
            ],
        ),
        ScalarNode::Nested(inner) => return normalize(arena, *inner, base_names),
        // CASE [operand] WHEN .. THEN .. [ELSE ..] END. WHEN/THEN pair order
        // is semantically significant (first match wins), so args are NOT
        // sorted. Absent operand/else are encoded with distinct zero-arg
        // marker calls so `CASE WHEN c THEN a END` can never collide with
        // `CASE WHEN c THEN a ELSE b END`.
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut args = Vec::with_capacity(when_then.len() * 2 + 2);
            args.push(match operand {
                Some(op) => call("case_operand", vec![normalize(arena, *op, base_names)?]),
                None => call("case_no_operand", vec![]),
            });
            for (when, then) in when_then {
                args.push(normalize(arena, *when, base_names)?);
                args.push(normalize(arena, *then, base_names)?);
            }
            args.push(match else_expr {
                Some(else_expr) => {
                    call("case_else", vec![normalize(arena, *else_expr, base_names)?])
                }
                None => call("case_no_else", vec![]),
            });
            call("case", args)
        }
        // IsTruthValue / WindowCall / Lambda* / LambdaParamRef /
        // SubqueryPlaceholder: not normalizable here -> fail closed.
        _ => return None,
    })
}

/// Rewrite table: normalized MV dimension expr -> MV-scan column.
pub(crate) struct MvColumnMap {
    by_norm: HashMap<NormExpr, OutputColumn>,
}

impl MvColumnMap {
    /// `dims`: (normalized MV dimension expr, the MV-scan output column that
    /// materializes it). Built by the rule from candidate outputs + the new
    /// MV-scan column ids.
    pub(crate) fn new(dims: Vec<(NormExpr, OutputColumn)>) -> Self {
        Self {
            by_norm: dims.into_iter().collect(),
        }
    }

    /// Rewrite a query-side expression so that every subtree matching an MV
    /// dimension becomes a ColumnRef to the MV scan column. Returns None if
    /// any base-table leaf remains unmapped.
    pub(crate) fn rewrite(
        &self,
        arena: &mut ScalarArena,
        expr: ScalarId,
        query_base_names: &HashMap<ColumnId, String>,
    ) -> Option<ScalarId> {
        if let Some(n) = normalize(arena, expr, query_base_names)
            && let Some(col) = self.by_norm.get(&n)
        {
            arena.remember_project_output_display(col.column_id, None, col.name.clone());
            return Some(arena.intern(
                ScalarNode::ColumnRef(col.column_id),
                col.data_type.clone(),
                col.nullable,
            ));
        }
        // Not a whole-tree match: recurse; a remaining bare base ColumnRef
        // means the MV does not materialize this column -> fail.
        match arena.node(expr).clone() {
            ScalarNode::ColumnRef(_) => None,
            ScalarNode::Literal(_) => Some(expr),
            node => rewrite_children(arena, expr, node, |arena, child| {
                self.rewrite(arena, child, query_base_names)
            }),
        }
    }
}

fn rewrite_children(
    arena: &mut ScalarArena,
    original: ScalarId,
    node: ScalarNode,
    mut rewrite: impl FnMut(&mut ScalarArena, ScalarId) -> Option<ScalarId>,
) -> Option<ScalarId> {
    let rewritten = match node {
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: rewrite(arena, left)?,
            right: rewrite(arena, right)?,
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: rewrite(arena, child)?,
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => ScalarNode::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|arg| rewrite(arena, arg))
                .collect::<Option<Vec<_>>>()?,
            distinct,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ScalarNode::AggregateCall {
            name,
            args: args
                .into_iter()
                .map(|arg| rewrite(arena, arg))
                .collect::<Option<Vec<_>>>()?,
            distinct,
            order_by,
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast {
            child: rewrite(arena, child)?,
            target,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: rewrite(arena, child)?,
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: rewrite(arena, child)?,
            list: list
                .into_iter()
                .map(|item| rewrite(arena, item))
                .collect::<Option<Vec<_>>>()?,
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: rewrite(arena, child)?,
            low: rewrite(arena, low)?,
            high: rewrite(arena, high)?,
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: rewrite(arena, child)?,
            pattern: rewrite(arena, pattern)?,
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let operand = match operand {
                Some(operand) => Some(rewrite(arena, operand)?),
                None => None,
            };
            let mut mapped_when_then = Vec::with_capacity(when_then.len());
            for (when, then) in when_then {
                mapped_when_then.push((rewrite(arena, when)?, rewrite(arena, then)?));
            }
            let else_expr = match else_expr {
                Some(else_expr) => Some(rewrite(arena, else_expr)?),
                None => None,
            };
            ScalarNode::Case {
                operand,
                when_then: mapped_when_then,
                else_expr,
            }
        }
        ScalarNode::Nested(inner) => ScalarNode::Nested(rewrite(arena, inner)?),
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::WindowCall { .. }
        | ScalarNode::LambdaFunction { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::IsTruthValue { .. } => return None,
    };
    Some(arena.intern(
        rewritten,
        arena.data_type(original).clone(),
        arena.nullable(original),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    // --- expression-construction helpers (file-local, mirror Task 3 tests) ---

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref(c: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: c.column_id,
                qualifier: None,
                column: c.name.clone(),
            },
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bin(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        let data_type = match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => DataType::Int64,
            _ => DataType::Boolean,
        };
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type,
            nullable: true,
        }
    }

    fn names(pairs: &[(u32, &str)]) -> HashMap<ColumnId, String> {
        pairs
            .iter()
            .map(|(id, n)| (ColumnId(*id), n.to_string()))
            .collect()
    }

    fn normalize(e: &TypedExpr, base_names: &HashMap<ColumnId, String>) -> Option<NormExpr> {
        let mut arena = ScalarArena::new();
        let expr = intern_typed(&mut arena, e);
        super::normalize(&arena, expr, base_names)
    }

    fn rewrite_typed(
        map: &MvColumnMap,
        e: &TypedExpr,
        base_names: &HashMap<ColumnId, String>,
    ) -> Option<TypedExpr> {
        let mut arena = ScalarArena::new();
        let expr = intern_typed(&mut arena, e);
        let rewritten = map.rewrite(&mut arena, expr, base_names)?;
        Some(materialize(&arena, rewritten))
    }

    #[test]
    fn normalize_is_column_id_independent() {
        // a(id=1) + 1 on side A vs a(id=9) + 1 on side B -> equal NormExpr,
        // because both resolve their ColumnRef through their own base-name map.
        let a1 = col(1, "a");
        let a9 = col(9, "a");
        let side_a = bin(col_ref(&a1), BinOp::Add, int_lit(1));
        let side_b = bin(col_ref(&a9), BinOp::Add, int_lit(1));

        let n_a = normalize(&side_a, &names(&[(1, "a")])).expect("normalize a");
        let n_b = normalize(&side_b, &names(&[(9, "a")])).expect("normalize b");
        assert_eq!(n_a, n_b);
    }

    #[test]
    fn normalize_sorts_commutative_args() {
        let a = col(1, "a");
        let b = col(2, "b");
        let nm = names(&[(1, "a"), (2, "b")]);

        // a + b == b + a (commutative arg sort).
        let ab = bin(col_ref(&a), BinOp::Add, col_ref(&b));
        let ba = bin(col_ref(&b), BinOp::Add, col_ref(&a));
        assert_eq!(
            normalize(&ab, &nm).expect("a+b"),
            normalize(&ba, &nm).expect("b+a")
        );

        // a < 5 == 5 > a (comparison canonicalization: Gt flips to lt + swap).
        let a_lt_5 = bin(col_ref(&a), BinOp::Lt, int_lit(5));
        let five_gt_a = bin(int_lit(5), BinOp::Gt, col_ref(&a));
        assert_eq!(
            normalize(&a_lt_5, &nm).expect("a<5"),
            normalize(&five_gt_a, &nm).expect("5>a")
        );
    }

    #[test]
    fn normalize_does_not_collide_opposite_comparisons() {
        // The Gt->lt flip must NOT make `lt` commutative: `a < 5` and `a > 5`
        // describe disjoint ranges and must produce different NormExprs,
        // while `a > 5` and `5 < a` are the same predicate and must match.
        let a = col(1, "a");
        let nm = names(&[(1, "a")]);
        let a_lt_5 = bin(col_ref(&a), BinOp::Lt, int_lit(5));
        let a_gt_5 = bin(col_ref(&a), BinOp::Gt, int_lit(5));
        let five_lt_a = bin(int_lit(5), BinOp::Lt, col_ref(&a));
        assert_ne!(
            normalize(&a_lt_5, &nm).expect("a<5"),
            normalize(&a_gt_5, &nm).expect("a>5")
        );
        assert_eq!(
            normalize(&a_gt_5, &nm).expect("a>5"),
            normalize(&five_lt_a, &nm).expect("5<a")
        );
    }

    #[test]
    fn normalize_discriminates_distinct() {
        // `count(a)` and `count(distinct a)` must not normalize equal: the
        // distinct flag participates in NormExpr identity.
        let a = col(1, "a");
        let nm = names(&[(1, "a")]);
        let agg = |distinct: bool| TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "count".to_string(),
                args: vec![col_ref(&a)],
                distinct,
                order_by: vec![],
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        assert_ne!(
            normalize(&agg(false), &nm).expect("count(a)"),
            normalize(&agg(true), &nm).expect("count(distinct a)")
        );
    }

    #[test]
    fn rewrite_replaces_matched_subtrees() {
        // MV side: scan(a,b,date_col); outputs d := date_col, s := a + b.
        let mv_a = col(1, "a");
        let mv_b = col(2, "b");
        let mv_date = col(3, "date_col");
        let mv_names = names(&[(1, "a"), (2, "b"), (3, "date_col")]);

        // The MV-scan output columns that materialize each dimension.
        let mv_d_out = col(101, "mv_d");
        let mv_s_out = col(102, "mv_s");

        let dim_d_expr = col_ref(&mv_date);
        let dim_s_expr = bin(col_ref(&mv_a), BinOp::Add, col_ref(&mv_b));

        let map = MvColumnMap::new(vec![
            (normalize(&dim_d_expr, &mv_names).expect("d"), mv_d_out),
            (
                normalize(&dim_s_expr, &mv_names).expect("s"),
                mv_s_out.clone(),
            ),
        ]);

        // Query side: scan(a,b) through different ColumnIds; expr (a + b) * 2.
        let q_a = col(7, "a");
        let q_b = col(8, "b");
        let q_names = names(&[(7, "a"), (8, "b")]);
        let query_expr = bin(
            bin(col_ref(&q_a), BinOp::Add, col_ref(&q_b)),
            BinOp::Mul,
            int_lit(2),
        );

        let rewritten = rewrite_typed(&map, &query_expr, &q_names).expect("rewrite ok");
        // Expect mv_s * 2: top is a Mul whose left is a ColumnRef to mv_s.
        let ExprKind::BinaryOp { left, op, right } = &rewritten.kind else {
            panic!("expected BinaryOp, got {:?}", rewritten.kind);
        };
        assert_eq!(*op, BinOp::Mul);
        match &left.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } => {
                assert_eq!(*column_id, mv_s_out.column_id);
                assert_eq!(column, "mv_s");
            }
            other => panic!("expected ColumnRef(mv_s) on left, got {other:?}"),
        }
        assert!(
            matches!(&right.kind, ExprKind::Literal(LiteralValue::Int(2))),
            "expected literal 2 on right, got {:?}",
            right.kind
        );
    }

    #[test]
    fn rewrite_fails_on_unmapped_leaf() {
        // MV materializes only date_col and a + b.
        let mv_a = col(1, "a");
        let mv_b = col(2, "b");
        let mv_date = col(3, "date_col");
        let mv_names = names(&[(1, "a"), (2, "b"), (3, "date_col")]);
        let mv_d_out = col(101, "mv_d");
        let mv_s_out = col(102, "mv_s");
        let dim_d_expr = col_ref(&mv_date);
        let dim_s_expr = bin(col_ref(&mv_a), BinOp::Add, col_ref(&mv_b));
        let map = MvColumnMap::new(vec![
            (normalize(&dim_d_expr, &mv_names).expect("d"), mv_d_out),
            (normalize(&dim_s_expr, &mv_names).expect("s"), mv_s_out),
        ]);

        // Query expr references base column c, which the MV does not output.
        let q_c = col(9, "c");
        let q_names = names(&[(9, "c")]);
        let query_expr = bin(col_ref(&q_c), BinOp::Add, int_lit(1));

        assert!(rewrite_typed(&map, &query_expr, &q_names).is_none());
    }

    fn case_when(when: TypedExpr, then: TypedExpr, else_expr: Option<TypedExpr>) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(when, then)],
                else_expr: else_expr.map(Box::new),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    #[test]
    fn case_when_normalizes_structurally() {
        // CASE WHEN a > 1 THEN b ELSE 0 END must compare equal across
        // ColumnId spaces and unequal when the ELSE differs or is absent.
        let a1 = col(1, "a");
        let b1 = col(2, "b");
        let n1 = names(&[(1, "a"), (2, "b")]);
        let a9 = col(9, "a");
        let b9 = col(8, "b");
        let n9 = names(&[(9, "a"), (8, "b")]);

        let mk = |a: &OutputColumn, b: &OutputColumn, else_expr: Option<TypedExpr>| {
            case_when(
                bin(col_ref(a), BinOp::Gt, int_lit(1)),
                col_ref(b),
                else_expr,
            )
        };

        let lhs = normalize(&mk(&a1, &b1, Some(int_lit(0))), &n1).expect("lhs");
        let rhs = normalize(&mk(&a9, &b9, Some(int_lit(0))), &n9).expect("rhs");
        assert_eq!(lhs, rhs);

        let other_else = normalize(&mk(&a1, &b1, Some(int_lit(7))), &n1).expect("else 7");
        assert_ne!(lhs, other_else);
        let no_else = normalize(&mk(&a1, &b1, None), &n1).expect("no else");
        assert_ne!(lhs, no_else);
    }
}
