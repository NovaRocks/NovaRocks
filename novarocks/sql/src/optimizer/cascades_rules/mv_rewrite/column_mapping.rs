// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Expression normalization (ColumnId-independent comparable form) and
//! query-expression rewriting onto MV output columns.
//! StarRocks counterpart: EquationRewriter / ColumnRewriter (single-table cut).

use std::collections::HashMap;

use crate::column_id::ColumnId;
use crate::common::OutputColumn;
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

pub(crate) use super::semantic_norm::{
    NormArgumentOrder, NormExpr, NormIndex, NormSortKey, norm_contains, normalize,
};
/// Rewrite table: normalized MV dimension expr -> MV-scan column.
pub(crate) struct MvColumnMap {
    by_norm: NormIndex<OutputColumn>,
}

macro_rules! mapped {
    ($value:expr) => {
        match $value? {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

impl MvColumnMap {
    /// `dims`: (normalized MV dimension expr, the MV-scan output column that
    /// materializes it). Built by the rule from candidate outputs + the new
    /// MV-scan column ids.
    pub(crate) fn try_new(
        dims: Vec<(NormExpr, OutputColumn)>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<Self>, crate::compiler::SqlCompileError> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::LowerProgram,
        )?;
        let mut by_norm = NormIndex::new();
        for (expr, col) in dims {
            work.step()?;
            if !expr
                .value_type()
                .exactly_equals_observed::<novarocks_functions::ConstantError>(
                    &col.value_type,
                    || work.step().map_err(Into::into),
                )?
            {
                work.finish()?;
                return Ok(None);
            }
            work.flush()?;
            by_norm.insert(expr, col, control)?;
        }
        work.finish()?;
        Ok(Some(Self { by_norm }))
    }

    /// Rewrite a query-side expression so that every subtree matching an MV
    /// dimension becomes a ColumnRef to the MV scan column. Returns None if
    /// any base-table leaf remains unmapped.
    pub(crate) fn rewrite(
        &self,
        arena: &mut ScalarArena,
        expr: ScalarId,
        query_base_names: &HashMap<ColumnId, String>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
        if let Some(n) = normalize(arena, expr, query_base_names, control)?
            && let Some(col) = self.by_norm.get(&n, control)?
        {
            let mapped = arena.intern_observed(
                ScalarNode::ColumnRef(col.column_id),
                col.value_type.clone(),
                control,
            )?;
            arena.remember_project_output_display(col.column_id, None, col.name.clone());
            return Ok(Some(mapped));
        }
        // Not a whole-tree match: recurse; a remaining bare base ColumnRef
        // means the MV does not materialize this column -> fail.
        match arena.node(expr).clone() {
            ScalarNode::ColumnRef(_) => Ok(None),
            ScalarNode::Literal(_) | ScalarNode::Constant(_) => Ok(Some(expr)),
            node => rewrite_children(
                arena,
                expr,
                node,
                |arena, child| self.rewrite(arena, child, query_base_names, control),
                control,
            ),
        }
    }
}

fn rewrite_children(
    arena: &mut ScalarArena,
    original: ScalarId,
    node: ScalarNode,
    mut rewrite: impl FnMut(
        &mut ScalarArena,
        ScalarId,
    ) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    let rewritten = match node {
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy,
        } => ScalarNode::BinaryOp {
            op,
            left: mapped!(rewrite(arena, left)),
            right: mapped!(rewrite(arena, right)),
            decimal_overflow_policy,
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: mapped!(rewrite(arena, child)),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            binding,
            volatility,
        } => ScalarNode::FunctionCall {
            name,
            args: mapped!(
                args.into_iter()
                    .map(|arg| rewrite(arena, arg))
                    .map(Result::transpose)
                    .collect::<Option<Result<Vec<_>, crate::compiler::SqlCompileError>>>()
                    .transpose()
            ),
            distinct,
            binding,
            volatility,
        },
        ScalarNode::AggregateCall {
            occurrence,
            filter,
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => {
            if filter.is_some() {
                return Ok(None);
            }
            let args = mapped!(
                args.into_iter()
                    .map(|arg| rewrite(arena, arg))
                    .map(Result::transpose)
                    .collect::<Option<Result<Vec<_>, crate::compiler::SqlCompileError>>>()
                    .transpose()
            );
            let mut mapped_order_by = Vec::with_capacity(order_by.len());
            for mut key in order_by {
                key.expr = mapped!(rewrite(arena, key.expr));
                key.display = match arena.node(key.expr) {
                    ScalarNode::ColumnRef(column) => arena.column_display(*column).cloned(),
                    _ => None,
                };
                mapped_order_by.push(key);
            }
            ScalarNode::AggregateCall {
                occurrence,
                filter,
                name,
                args,
                distinct,
                order_by: mapped_order_by,
                resolved,
            }
        }
        ScalarNode::Cast {
            cast_mode,
            child,
            target,
            decimal_overflow_policy,
        } => ScalarNode::Cast {
            cast_mode,
            child: mapped!(rewrite(arena, child)),
            target,
            decimal_overflow_policy,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: mapped!(rewrite(arena, child)),
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: mapped!(rewrite(arena, child)),
            list: mapped!(
                list.into_iter()
                    .map(|item| rewrite(arena, item))
                    .map(Result::transpose)
                    .collect::<Option<Result<Vec<_>, crate::compiler::SqlCompileError>>>()
                    .transpose()
            ),
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: mapped!(rewrite(arena, child)),
            low: mapped!(rewrite(arena, low)),
            high: mapped!(rewrite(arena, high)),
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: mapped!(rewrite(arena, child)),
            pattern: mapped!(rewrite(arena, pattern)),
            negated,
        },
        ScalarNode::Case {
            ieee_float_equality,
            operand,
            when_then,
            else_expr,
        } => {
            let operand = match operand {
                Some(operand) => Some(mapped!(rewrite(arena, operand))),
                None => None,
            };
            let mut mapped_when_then = Vec::with_capacity(when_then.len());
            for (when, then) in when_then {
                mapped_when_then
                    .push((mapped!(rewrite(arena, when)), mapped!(rewrite(arena, then))));
            }
            let else_expr = match else_expr {
                Some(else_expr) => Some(mapped!(rewrite(arena, else_expr))),
                None => None,
            };
            ScalarNode::Case {
                ieee_float_equality,
                operand,
                when_then: mapped_when_then,
                else_expr,
            }
        }
        ScalarNode::Nested(inner) => ScalarNode::Nested(mapped!(rewrite(arena, inner))),
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_)
        | ScalarNode::WindowCall { .. }
        | ScalarNode::LambdaFunction { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::IsTruthValue { .. } => return Ok(None),
    };
    Ok(Some(arena.intern_observed(
        rewritten,
        arena.value_type(original).clone(),
        control,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::scalar::ScalarArena;

    use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    // --- expression-construction helpers (file-local, mirror Task 3 tests) ---

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

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
            value_type: c.value_type.clone(),
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn bin(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        let data_type = match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => DataType::Int64,
            _ => DataType::Boolean,
        };
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(left),
                op,
                right: novarocks_parser::ast::AstBox::new(right),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, true),
        }
    }

    fn names(pairs: &[(u32, &str)]) -> HashMap<ColumnId, String> {
        pairs
            .iter()
            .map(|(id, n)| (ColumnId(*id), n.to_string()))
            .collect()
    }

    fn test_map(dims: Vec<(NormExpr, OutputColumn)>) -> MvColumnMap {
        MvColumnMap::try_new(dims, &crate::compiler::SqlCompileControl::unbounded())
            .unwrap()
            .expect("exact dimension output type")
    }

    fn normalize(e: &TypedExpr, base_names: &HashMap<ColumnId, String>) -> Option<NormExpr> {
        let mut arena = ScalarArena::new();
        let expr = intern_typed(
            &mut arena,
            e,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        super::normalize(
            &arena,
            expr,
            base_names,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
    }

    fn rewrite_typed(
        map: &MvColumnMap,
        e: &TypedExpr,
        base_names: &HashMap<ColumnId, String>,
    ) -> Option<TypedExpr> {
        let mut arena = ScalarArena::new();
        let expr = intern_typed(
            &mut arena,
            e,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let rewritten = map
            .rewrite(
                &mut arena,
                expr,
                base_names,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()?;
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
                occurrence: crate::analysis::AggregateOccurrence::new(),
                filter: None,
                name: "count".to_string(),
                args: (vec![col_ref(&a)]).into(),
                distinct,
                order_by: (vec![]).into(),
                resolved: crate::functions::test_resolved_aggregate(
                    "count",
                    &[DataType::Int64],
                    distinct,
                ),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
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

        let map = test_map(vec![
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
        let ExprKind::BinaryOp {
            left, op, right, ..
        } = &rewritten.kind
        else {
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
        let map = test_map(vec![
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
                ieee_float_equality: false,
                operand: None,
                when_then: (vec![(when, then)]).into(),
                else_expr: else_expr.map(novarocks_parser::ast::AstBox::new),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
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

    #[test]
    fn actual_mv_mapping_preserves_interner_control_categories() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::Mutex;
        struct Stop {
            reason: CompileControlError,
            positive_only: bool,
            calls: Mutex<Vec<u32>>,
        }
        impl PureCompileControl for Stop {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert!(matches!(
                    phase,
                    CompilePhase::Validate | CompilePhase::LowerProgram
                ));
                self.calls.lock().unwrap().push(units);
                // Child mappings legitimately flush a short tail before the
                // wide parent interner starts. Refuse that parent's first
                // full work quantum, rather than a completed leaf's tail.
                if !self.positive_only || units == 256 {
                    Err(self.reason.clone())
                } else {
                    Ok(())
                }
            }
        }
        for reason in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for positive_only in [false, true] {
                let mut arena = ScalarArena::new();
                let input = col(1, "a");
                let leaf = arena.intern(
                    ScalarNode::ColumnRef(input.column_id),
                    input.value_type.clone(),
                );
                let source = if positive_only {
                    arena.intern(
                        ScalarNode::InList {
                            child: leaf,
                            list: vec![leaf; 320],
                            negated: false,
                        },
                        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
                    )
                } else {
                    leaf
                };
                let map = test_map(vec![(
                    NormExpr::Column {
                        name: "a".into(),
                        value_type: input.value_type.clone(),
                    },
                    col(101, "mv_a"),
                )]);
                let control = Stop {
                    reason: reason.clone(),
                    positive_only,
                    calls: Mutex::new(Vec::new()),
                };
                assert_eq!(
                    map.rewrite(&mut arena, source, &names(&[(1, "a")]), &control),
                    Err(crate::compiler::SqlCompileError::from(reason.clone()))
                );
                let calls = control.calls.lock().unwrap();
                if positive_only {
                    assert!(calls.iter().all(|&units| units <= 256));
                    assert_eq!(calls.iter().find(|&&units| units == 256), Some(&256));
                    assert_eq!(
                        calls.last(),
                        Some(&256),
                        "the refusal occurs at the first full quantum with no replacement exit"
                    );
                } else {
                    assert_eq!(calls.as_slice(), &[0]);
                }
                // The source definition remains the actual input, rather than
                // a partially published successful mapping result.
                assert!(
                    matches!(arena.node(leaf), ScalarNode::ColumnRef(actual) if *actual == input.column_id)
                );
            }
        }
    }

    #[test]
    fn actual_mv_mapping_first_missing_child_skips_later_interner_work() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Stop(AtomicUsize);
        impl PureCompileControl for Stop {
            fn checkpoint(&self, phase: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if phase == CompilePhase::Validate {
                    self.0.fetch_add(1, Ordering::Relaxed);
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        let mut arena = ScalarArena::new();
        let missing = col(1, "missing");
        let present = col(2, "present");
        let left = arena.intern(
            ScalarNode::ColumnRef(missing.column_id),
            missing.value_type.clone(),
        );
        let right = arena.intern(
            ScalarNode::ColumnRef(present.column_id),
            present.value_type.clone(),
        );
        let source = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Sub,
                left,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        );
        let map = test_map(vec![(
            NormExpr::Column {
                name: "present".into(),
                value_type: present.value_type.clone(),
            },
            col(101, "mv_present"),
        )]);
        let control = Stop(AtomicUsize::new(0));
        assert_eq!(
            map.rewrite(
                &mut arena,
                source,
                &names(&[(1, "missing"), (2, "present")]),
                &control
            ),
            Ok(None)
        );
        assert_eq!(
            control.0.load(Ordering::Relaxed),
            0,
            "a miss must not evaluate the mapped suffix"
        );
    }

    fn resolved_scalar_call(
        source: &OutputColumn,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> TypedExpr {
        let args = vec![col_ref(source)];
        let binding = crate::binding::SqlFunctionBinding::resolve_for_plan(
            crate::optimizer::BINDING_AUTHORITY,
            crate::functions::builtin_sql_function_catalog(),
            "abs",
            &args,
            policy,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let novarocks_functions::FunctionResultType::Scalar(value_type) =
            &binding.selected.result_type
        else {
            panic!("scalar result");
        };
        TypedExpr {
            value_type: value_type.clone(),
            kind: ExprKind::FunctionCall {
                name: "abs".into(),
                args: (args).into(),
                distinct: false,
                volatility: binding.semantics.volatility,
                binding,
            },
        }
    }

    fn resolved_ordered_aggregate(
        source: &OutputColumn,
        order_by: Vec<crate::analysis::SortItem>,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> TypedExpr {
        let args = vec![col_ref(source)];
        let resolved = crate::functions::resolve_sql_aggregate_binding(
            crate::functions::builtin_sql_function_catalog(),
            "array_agg",
            &args,
            &order_by,
            false,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let novarocks_functions::FunctionResultType::Scalar(value_type) =
            &resolved.selected.result_type
        else {
            panic!("scalar aggregate result");
        };
        TypedExpr {
            value_type: value_type.clone(),
            kind: ExprKind::AggregateCall {
                occurrence: crate::analysis::AggregateOccurrence::new(),
                filter: None,
                name: "array_agg".into(),
                args: (args).into(),
                distinct: false,
                order_by: (order_by).into(),
                resolved: crate::binding::SqlFunctionBinding::from_plan(
                    crate::binding::PlanBindingAuthority::Optimizer(
                        crate::optimizer::BindingAuthority(()),
                    ),
                    resolved,
                    policy,
                ),
            },
        }
    }

    fn assert_rewritten_column(expression: TypedExpr, column: &OutputColumn) {
        let ExprKind::ColumnRef { column_id, .. } = expression.kind else {
            panic!("expected materialized MV column");
        };
        assert_eq!(column_id, column.column_id);
        assert_eq!(expression.value_type, column.value_type);
    }

    #[test]
    fn mv_mapping_preserves_actual_scalar_selection_and_call_policy() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let mv_source = col(1, "a");
        let query_source = col(9, "a");
        let mv_names = names(&[(1, "a")]);
        let query_names = names(&[(9, "a")]);
        for policy in [OutputNull, ReportError] {
            let mv = resolved_scalar_call(&mv_source, policy);
            let output = OutputColumn {
                value_type: mv.value_type.clone(),
                ..col(101, "mv_abs")
            };
            let map = test_map(vec![(normalize(&mv, &mv_names).unwrap(), output.clone())]);
            assert_rewritten_column(
                rewrite_typed(
                    &map,
                    &resolved_scalar_call(&query_source, policy),
                    &query_names,
                )
                .unwrap(),
                &output,
            );
            let foreign_policy = if policy == OutputNull {
                ReportError
            } else {
                OutputNull
            };
            assert!(
                rewrite_typed(
                    &map,
                    &resolved_scalar_call(&query_source, foreign_policy),
                    &query_names
                )
                .is_none()
            );
            let floating_source = OutputColumn {
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Float64,
                    true,
                ),
                ..query_source.clone()
            };
            let floating = resolved_scalar_call(&floating_source, policy);
            let ExprKind::FunctionCall {
                binding: mv_binding,
                ..
            } = &mv.kind
            else {
                panic!("ABS");
            };
            let ExprKind::FunctionCall {
                binding: float_binding,
                ..
            } = &floating.kind
            else {
                panic!("ABS");
            };
            assert_eq!(mv_binding.function_id, float_binding.function_id);
            assert_ne!(mv_binding.selected, float_binding.selected);
            assert!(
                rewrite_typed(&map, &floating, &query_names).is_none(),
                "same display name cannot replace a different actual overload"
            );
        }
    }

    #[test]
    fn mv_mapping_binary_and_cast_policy_are_exact_and_commutation_is_preserved() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let mv_a = col(1, "a");
        let mv_b = col(2, "b");
        let query_a = col(9, "a");
        let query_b = col(8, "b");
        let mv_names = names(&[(1, "a"), (2, "b")]);
        let query_names = names(&[(9, "a"), (8, "b")]);
        for policy in [OutputNull, ReportError] {
            let binary = |a: &OutputColumn, b: &OutputColumn, policy| {
                let mut expression = bin(col_ref(a), BinOp::Add, col_ref(b));
                let ExprKind::BinaryOp {
                    decimal_overflow_policy,
                    ..
                } = &mut expression.kind
                else {
                    panic!("binary");
                };
                *decimal_overflow_policy = policy;
                expression
            };
            let cast = |a: &OutputColumn, policy| TypedExpr {
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Decimal128(3, 0),
                    true,
                ),
                kind: ExprKind::Cast {
                    cast_mode: None,
                    expr: novarocks_parser::ast::AstBox::new(col_ref(a)),
                    target: DataType::Decimal128(3, 0),
                    decimal_overflow_policy: policy,
                },
            };
            let mv_binary = binary(&mv_a, &mv_b, policy);
            let output = col(101, "mv_sum");
            let map = test_map(vec![(
                normalize(&mv_binary, &mv_names).unwrap(),
                output.clone(),
            )]);
            assert_rewritten_column(
                rewrite_typed(&map, &binary(&query_b, &query_a, policy), &query_names).unwrap(),
                &output,
            );
            let foreign_policy = if policy == OutputNull {
                ReportError
            } else {
                OutputNull
            };
            assert!(
                rewrite_typed(
                    &map,
                    &binary(&query_b, &query_a, foreign_policy),
                    &query_names
                )
                .is_none()
            );
            let mv_cast = cast(&mv_a, policy);
            let output = OutputColumn {
                value_type: mv_cast.value_type.clone(),
                ..col(102, "mv_cast")
            };
            let map = test_map(vec![(
                normalize(&mv_cast, &mv_names).unwrap(),
                output.clone(),
            )]);
            assert_rewritten_column(
                rewrite_typed(&map, &cast(&query_a, policy), &query_names).unwrap(),
                &output,
            );
            assert!(rewrite_typed(&map, &cast(&query_a, foreign_policy), &query_names).is_none());
        }
    }

    #[test]
    fn mv_mapping_ordered_aggregate_preserves_actual_binding_policy_and_all_sort_facts() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let mv_a = col(1, "a");
        let mv_b = col(2, "b");
        let mv_c = col(3, "c");
        let query_a = col(9, "a");
        let query_b = col(8, "b");
        let query_c = col(7, "c");
        let mv_names = names(&[(1, "a"), (2, "b"), (3, "c")]);
        let query_names = names(&[(9, "a"), (8, "b"), (7, "c")]);
        let sort = |column: &OutputColumn, asc, nulls_first| crate::analysis::SortItem {
            expr: col_ref(column),
            asc,
            nulls_first,
        };
        for policy in [OutputNull, ReportError] {
            let mv = resolved_ordered_aggregate(
                &mv_a,
                vec![sort(&mv_b, true, false), sort(&mv_c, false, true)],
                policy,
            );
            let output = OutputColumn {
                value_type: mv.value_type.clone(),
                ..col(101, "mv_array")
            };
            let map = test_map(vec![(normalize(&mv, &mv_names).unwrap(), output.clone())]);
            let same = resolved_ordered_aggregate(
                &query_a,
                vec![sort(&query_b, true, false), sort(&query_c, false, true)],
                policy,
            );
            assert_rewritten_column(rewrite_typed(&map, &same, &query_names).unwrap(), &output);
            let foreign_policy = if policy == OutputNull {
                ReportError
            } else {
                OutputNull
            };
            for query in [
                resolved_ordered_aggregate(
                    &query_a,
                    vec![sort(&query_b, true, false), sort(&query_c, false, true)],
                    foreign_policy,
                ),
                resolved_ordered_aggregate(
                    &query_a,
                    vec![sort(&query_c, false, true), sort(&query_b, true, false)],
                    policy,
                ),
                resolved_ordered_aggregate(
                    &query_a,
                    vec![sort(&query_b, false, false), sort(&query_c, false, true)],
                    policy,
                ),
                resolved_ordered_aggregate(
                    &query_a,
                    vec![sort(&query_b, true, true), sort(&query_c, false, true)],
                    policy,
                ),
            ] {
                assert!(rewrite_typed(&map, &query, &query_names).is_none());
            }
        }
    }

    #[test]
    fn mv_mapping_rewrites_actual_aggregate_order_channels_and_rejects_missing_sort_source() {
        use novarocks_type_contract::DecimalOverflowPolicy::ReportError;
        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let base_names = names(&[(1, "a"), (2, "b"), (3, "c")]);
        let expression = resolved_ordered_aggregate(
            &a,
            vec![
                crate::analysis::SortItem {
                    expr: col_ref(&b),
                    asc: false,
                    nulls_first: true,
                },
                crate::analysis::SortItem {
                    expr: col_ref(&c),
                    asc: true,
                    nulls_first: false,
                },
            ],
            ReportError,
        );
        let outputs = [col(101, "mv_a"), col(102, "mv_b"), col(103, "mv_c")];
        let dims =
            [(&a, &outputs[0]), (&b, &outputs[1]), (&c, &outputs[2])].map(|(source, output)| {
                (
                    normalize(&col_ref(source), &base_names).unwrap(),
                    output.clone(),
                )
            });
        let map = test_map(dims.to_vec());
        let rewritten = rewrite_typed(&map, &expression, &base_names).unwrap();
        let ExprKind::AggregateCall {
            args,
            order_by,
            resolved,
            ..
        } = &rewritten.kind
        else {
            panic!("aggregate");
        };
        let ExprKind::AggregateCall {
            resolved: original_binding,
            ..
        } = &expression.kind
        else {
            panic!("original aggregate");
        };
        assert!(std::ptr::eq(
            resolved.resolved(),
            original_binding.resolved()
        ));
        assert_rewritten_column(args[0].clone(), &outputs[0]);
        for (key, output, asc, nulls_first) in [
            (&order_by[0], &outputs[1], false, true),
            (&order_by[1], &outputs[2], true, false),
        ] {
            assert_rewritten_column(key.expr.clone(), output);
            assert_eq!(key.asc, asc);
            assert_eq!(key.nulls_first, nulls_first);
        }
        let map = test_map(dims[..2].to_vec());
        assert!(
            rewrite_typed(&map, &expression, &base_names).is_none(),
            "an unmapped ordering expression cannot retain the base ScalarId"
        );
    }
}
