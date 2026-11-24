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

//! Predicate classification (equality/range vs residual), per-column
//! interval containment, and compensation computation.
//! StarRocks counterpart: PredicateSplit / RangePredicate.

use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::{BinOp, LiteralValue};
use crate::compiler::SqlCompileError;
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use novarocks_type_contract::{
    CharacterKind, CompileCheckpoints, CompilePhase, PureCompileControl, ValueKind,
};

use super::column_mapping::{NormExpr, norm_contains, normalize};

/// Inclusive/exclusive bound on one column.
#[derive(Clone, Debug, PartialEq)]
struct Bound {
    value: LiteralValue,
    inclusive: bool,
}

/// Conjunct-merged interval for one base column.
#[derive(Clone, Debug, Default, PartialEq)]
struct ColumnRange {
    low: Option<Bound>,
    high: Option<Bound>,
}

#[derive(Debug)]
pub(crate) struct ContainmentResult {
    /// Query conjuncts the MV does not already guarantee; to be re-applied
    /// as a Filter above the MV scan. These ids belong to the query arena;
    /// the caller rewrites them to MV output columns before injecting a Filter.
    pub compensation: Vec<ScalarId>,
}

struct Classified {
    /// base column name -> (merged range, original conjuncts on the column)
    ranges: HashMap<String, (ColumnRange, Vec<ScalarId>)>,
    /// normalized residual -> original conjunct
    residuals: Vec<NormExpr>,
    residual_ids: Vec<ScalarId>,
}

/// Classify conjuncts. Returns None when any conjunct cannot be classified
/// safely (e.g. un-normalizable residual) — fail closed.
fn classify(
    arena: &ScalarArena,
    conjuncts: &[ScalarId],
    base_names: &HashMap<ColumnId, String>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Classified>, SqlCompileError> {
    let mut ranges: HashMap<String, (ColumnRange, Vec<ScalarId>)> = HashMap::new();
    let mut residuals = Vec::new();
    let mut residual_ids = Vec::new();
    for c in conjuncts {
        // Observe the range model's opaque string and map operations at the
        // same scope boundary as residual normalization.
        work.flush()?;
        let range = as_range_conjunct(arena, *c, base_names, work)?;
        work.step()?;
        work.flush()?;
        match range {
            Some((col, low, high)) => {
                let entry = ranges.entry(col).or_default();
                let low_ok = low.is_none_or(|b| tighten_low(&mut entry.0, b).is_some());
                let high_ok =
                    low_ok && high.is_none_or(|b| tighten_high(&mut entry.0, b).is_some());
                if high_ok {
                    entry.1.push(*c);
                }
                work.step()?;
                work.flush()?;
                if !high_ok {
                    return Ok(None);
                }
            }
            None => {
                let Some(n) = normalize(arena, *c, base_names, work.control())? else {
                    return Ok(None);
                };
                residuals.push(n);
                residual_ids.push(*c);
                work.step()?;
            }
        }
    }
    Ok(Some(Classified {
        ranges,
        residuals,
        residual_ids,
    }))
}

/// Read the selected VARCHAR value across a declaration-only cast to
/// unbounded VARCHAR. Keep the expression and its casts for compensation.
fn varchar_bound(
    arena: &ScalarArena,
    mut expr: ScalarId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<LiteralValue>, SqlCompileError> {
    loop {
        work.step()?;
        let ty = arena.value_type(expr);
        let kind = ty.value_type().kind();
        if ty.data_type() != &DataType::Utf8
            || !matches!(kind, ValueKind::String)
                && !matches!(kind, ValueKind::BoundedCharacter(c) if c.kind() == CharacterKind::Varchar)
        {
            return Ok(None);
        }
        match arena.node(expr) {
            ScalarNode::Literal(HashableLiteral(value @ LiteralValue::String(_))) => {
                return Ok(Some(value.clone()));
            }
            ScalarNode::Constant(value) if value.value_type() == ty => {
                work.flush()?;
                return Ok(value
                    .utf8_observed(CompilePhase::LowerProgram, work.control())?
                    .map(|text| LiteralValue::String(text.to_owned())));
            }
            ScalarNode::Cast {
                child,
                target: DataType::Utf8,
                ..
            } if matches!(kind, ValueKind::String) => {
                expr = *child;
            }
            // CHAR, narrowing VARCHAR and other value-changing casts are
            // outside this lexical range observation.
            _ => return Ok(None),
        }
    }
}

fn range_literal(
    arena: &ScalarArena,
    expr: ScalarId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<LiteralValue>, SqlCompileError> {
    if let ScalarNode::Literal(HashableLiteral(value)) = arena.node(expr)
        && !matches!(value, LiteralValue::String(_))
    {
        return Ok(Some(value.clone()));
    }
    if let Some(value) = crate::optimizer::scalar_expr::signed_integer_literal(arena, expr) {
        return Ok(Some(LiteralValue::Int(value)));
    }
    varchar_bound(arena, expr, work)
}

type RangeConjunct = (String, Option<Bound>, Option<Bound>);

/// `col op literal` / `literal op col` / BETWEEN -> (column, low?, high?).
/// op ∈ {<, <=, >, >=, =}. `!=`, IS NULL, IN, LIKE etc. are residuals.
fn as_range_conjunct(
    arena: &ScalarArena,
    expr: ScalarId,
    base_names: &HashMap<ColumnId, String>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<RangeConjunct>, SqlCompileError> {
    let col_of = |x: ScalarId| -> Option<String> {
        let ScalarNode::ColumnRef(column_id) = arena.node(x) else {
            return None;
        };
        base_names.get(column_id).cloned()
    };
    if let Some((op, [left, right])) =
        crate::common::binary_operator::scalar_comparison(arena, expr)
        && [left, right].into_iter().all(|operand| {
            crate::common::binary_operator::permits_equality_substitution(arena.data_type(operand))
        })
    {
        let (col, operand, op) = match (col_of(left), col_of(right)) {
            (Some(col), None) => (col, right, op),
            (None, Some(col)) => {
                let flipped = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::Le => BinOp::Ge,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::Ge => BinOp::Le,
                    BinOp::Eq => BinOp::Eq,
                    _ => return Ok(None),
                };
                (col, left, flipped)
            }
            _ => return Ok(None),
        };
        let Some(value) = range_literal(arena, operand, work)? else {
            return Ok(None);
        };
        let bound = Bound {
            value,
            inclusive: matches!(op, BinOp::Eq | BinOp::Ge | BinOp::Le),
        };
        return Ok(match op {
            BinOp::Eq => Some((col, Some(bound.clone()), Some(bound))),
            BinOp::Ge | BinOp::Gt => Some((col, Some(bound), None)),
            BinOp::Le | BinOp::Lt => Some((col, None, Some(bound))),
            _ => None,
        });
    }
    let ScalarNode::Between {
        child,
        low,
        high,
        negated: false,
    } = arena.node(expr)
    else {
        return Ok(None);
    };
    let Some(col) = col_of(*child) else {
        return Ok(None);
    };
    let (Some(lo), Some(hi)) = (
        range_literal(arena, *low, work)?,
        range_literal(arena, *high, work)?,
    ) else {
        return Ok(None);
    };
    Ok(Some((
        col,
        Some(Bound {
            value: lo,
            inclusive: true,
        }),
        Some(Bound {
            value: hi,
            inclusive: true,
        }),
    )))
}

/// Compare two literals of compatible kinds. None = incomparable (fail closed).
fn cmp_literal(a: &LiteralValue, b: &LiteralValue) -> Option<std::cmp::Ordering> {
    use LiteralValue::*;
    match (a, b) {
        (Int(x), Int(y)) => Some(x.cmp(y)),
        (LargeInt(x), LargeInt(y)) => Some(x.cmp(y)),
        (Int(x), LargeInt(y)) => Some(i128::from(*x).cmp(y)),
        (LargeInt(x), Int(y)) => Some(x.cmp(&i128::from(*y))),
        (Float(x), Float(y)) => x.partial_cmp(y),
        (String(x), String(y)) => Some(x.cmp(y)),
        (Bool(x), Bool(y)) => Some(x.cmp(y)),
        // Decimal / Null / mixed kinds: refuse to compare (fail closed).
        // Note Int<->Float is deliberately NOT bridged: `x as f64` loses
        // precision above 2^53 and could flip an ordering, so a mixed
        // Int/Float bound pair on one column fails closed rather than risk a
        // wrong containment decision. In practice the analyzer types literals
        // to the column, so both sides share a kind and this never triggers.
        _ => None,
    }
}

fn tighten_low(r: &mut ColumnRange, b: Bound) -> Option<()> {
    match &r.low {
        None => r.low = Some(b),
        Some(cur) => match cmp_literal(&b.value, &cur.value)? {
            std::cmp::Ordering::Greater => r.low = Some(b),
            std::cmp::Ordering::Equal if !b.inclusive => r.low = Some(b),
            _ => {}
        },
    }
    Some(())
}

fn tighten_high(r: &mut ColumnRange, b: Bound) -> Option<()> {
    match &r.high {
        None => r.high = Some(b),
        Some(cur) => match cmp_literal(&b.value, &cur.value)? {
            std::cmp::Ordering::Less => r.high = Some(b),
            std::cmp::Ordering::Equal if !b.inclusive => r.high = Some(b),
            _ => {}
        },
    }
    Some(())
}

/// query_low >= mv_low (with inclusivity)?  i.e. query interval starts inside MV's.
fn low_contained(query: &Option<Bound>, mv: &Option<Bound>) -> Option<bool> {
    match (query, mv) {
        (_, None) => Some(true),
        (None, Some(_)) => Some(false),
        (Some(q), Some(m)) => Some(match cmp_literal(&q.value, &m.value)? {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => m.inclusive || !q.inclusive,
        }),
    }
}

fn high_contained(query: &Option<Bound>, mv: &Option<Bound>) -> Option<bool> {
    match (query, mv) {
        (_, None) => Some(true),
        (None, Some(_)) => Some(false),
        (Some(q), Some(m)) => Some(match cmp_literal(&q.value, &m.value)? {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => m.inclusive || !q.inclusive,
        }),
    }
}

/// Core check: MV data ⊇ query data. Returns None when not contained (or
/// not provably contained). On success returns the compensation conjuncts.
pub(crate) fn check_containment(
    query_conjuncts: &[ScalarId],
    query_arena: &ScalarArena,
    mv_conjuncts: &[ScalarId],
    mv_arena: &ScalarArena,
    query_base_names: &HashMap<ColumnId, String>,
    mv_base_names: &HashMap<ColumnId, String>,
    control: &dyn PureCompileControl,
) -> Result<Option<ContainmentResult>, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let Some(q) = classify(query_arena, query_conjuncts, query_base_names, &mut work)? else {
            return Ok(None);
        };
        let Some(m) = classify(mv_arena, mv_conjuncts, mv_base_names, &mut work)? else {
            return Ok(None);
        };
        let mut compensation = Vec::new();
        for (col, (mv_range, _)) in &m.ranges {
            work.flush()?;
            let contained = q.ranges.get(col).and_then(|(q_range, _)| {
                Some(
                    low_contained(&q_range.low, &mv_range.low)?
                        && high_contained(&q_range.high, &mv_range.high)?,
                )
            });
            work.step()?;
            work.flush()?;
            if contained != Some(true) {
                return Ok(None);
            }
        }
        for (col, (q_range, originals)) in &q.ranges {
            work.flush()?;
            let equal = m
                .ranges
                .get(col)
                .is_some_and(|(mv_range, _)| mv_range == q_range);
            work.step()?;
            work.flush()?;
            if !equal {
                for original in originals {
                    compensation.push(*original);
                    work.step()?;
                }
            }
        }
        // Residuals keep exact selected-value/type identity. Use the
        // original expressions when a proven range requires compensation.
        for mn in &m.residuals {
            work.flush()?;
            if !norm_contains(&q.residuals, mn, control)? {
                return Ok(None);
            }
            work.step()?;
        }
        for (qn, original) in q.residuals.iter().zip(&q.residual_ids) {
            work.flush()?;
            if !norm_contains(&m.residuals, qn, control)? {
                compensation.push(*original);
            }
            work.step()?;
        }
        Ok(Some(ContainmentResult { compensation }))
    })();
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::scalar::{ScalarArena, ScalarId};

    use crate::planner::optimizer_bridge::scalar::intern_typed;
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

    fn string_lit(v: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(v.to_string())),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        }
    }

    fn bin(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(left),
                op,
                right: novarocks_parser::ast::AstBox::new(right),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        }
    }

    /// The single base column `a` used by every single-column case below.
    fn a() -> OutputColumn {
        col(1, "a")
    }

    /// `a >= v`
    fn ge_a(v: i64) -> TypedExpr {
        bin(col_ref(&a()), BinOp::Ge, int_lit(v))
    }

    /// `a > v`
    fn gt_a(v: i64) -> TypedExpr {
        bin(col_ref(&a()), BinOp::Gt, int_lit(v))
    }

    /// `a != v`
    fn ne_a(v: i64) -> TypedExpr {
        bin(col_ref(&a()), BinOp::Ne, int_lit(v))
    }

    /// `a >= 'v'` (String literal, to exercise incomparable-type fail-closed).
    fn ge_a_str(v: &str) -> TypedExpr {
        bin(col_ref(&a()), BinOp::Ge, string_lit(v))
    }

    /// `a LIKE 'pat'`
    fn like_a(pat: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Like {
                expr: novarocks_parser::ast::AstBox::new(col_ref(&a())),
                pattern: novarocks_parser::ast::AstBox::new(string_lit(pat)),
                negated: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        }
    }

    /// `a BETWEEN lo AND hi`
    fn between_a(lo: i64, hi: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Between {
                expr: novarocks_parser::ast::AstBox::new(col_ref(&a())),
                low: novarocks_parser::ast::AstBox::new(int_lit(lo)),
                high: novarocks_parser::ast::AstBox::new(int_lit(hi)),
                negated: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        }
    }

    /// Base-name map `{ c1 -> "a" }`, used for BOTH sides in single-column cases.
    fn names() -> HashMap<ColumnId, String> {
        let mut m = HashMap::new();
        m.insert(ColumnId(1), "a".to_string());
        m
    }

    fn check_containment(
        query_conjuncts: &[TypedExpr],
        mv_conjuncts: &[TypedExpr],
        query_base_names: &HashMap<ColumnId, String>,
        mv_base_names: &HashMap<ColumnId, String>,
    ) -> Option<ContainmentResult> {
        let mut query_arena = ScalarArena::new();
        let query_ids: Vec<ScalarId> = query_conjuncts
            .iter()
            .map(|expr| {
                intern_typed(
                    &mut query_arena,
                    expr,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap()
            })
            .collect();
        let mut mv_arena = ScalarArena::new();
        let mv_ids: Vec<ScalarId> = mv_conjuncts
            .iter()
            .map(|expr| {
                intern_typed(
                    &mut mv_arena,
                    expr,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap()
            })
            .collect();
        super::check_containment(
            &query_ids,
            &query_arena,
            &mv_ids,
            &mv_arena,
            query_base_names,
            mv_base_names,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap()
    }

    #[test]
    fn equal_ranges_need_no_compensation() {
        // MV: a >= 5      query: a >= 5
        let n = names();
        let r = check_containment(&[ge_a(5)], &[ge_a(5)], &n, &n).expect("contained");
        assert!(r.compensation.is_empty());
    }

    #[test]
    fn tighter_query_range_compensates() {
        // MV: a >= 5      query: a >= 10  -> contained, compensation [a >= 10]
        let n = names();
        let r = check_containment(&[ge_a(10)], &[ge_a(5)], &n, &n).expect("contained");
        assert_eq!(r.compensation.len(), 1);
    }

    #[test]
    fn wider_query_range_fails() {
        // MV: a >= 10     query: a >= 5  -> NOT contained
        let n = names();
        assert!(check_containment(&[ge_a(5)], &[ge_a(10)], &n, &n).is_none());
    }

    #[test]
    fn open_closed_boundary_inclusivity() {
        // Exercises the inclusivity arm of interval containment at a shared
        // endpoint (the subtlest part of low_contained/high_contained).
        let n = names();
        // MV `a > 5` excludes 5; query `a >= 5` includes it -> NOT contained.
        assert!(check_containment(&[ge_a(5)], &[gt_a(5)], &n, &n).is_none());
        // MV `a >= 5` includes 5; query `a > 5` excludes it -> contained, and
        // the tighter (exclusive) query bound is re-applied as compensation.
        let r = check_containment(&[gt_a(5)], &[ge_a(5)], &n, &n).expect("contained");
        assert_eq!(r.compensation.len(), 1);
    }

    #[test]
    fn mv_residual_must_appear_in_query() {
        let n = names();
        // MV: a LIKE 'x%'   query: (no like) -> fail
        assert!(check_containment(&[], &[like_a("x%")], &n, &n).is_none());
        // MV: a LIKE 'x%'   query: a LIKE 'x%' AND a >= 5 -> ok, comp [a >= 5]
        let r = check_containment(&[like_a("x%"), ge_a(5)], &[like_a("x%")], &n, &n)
            .expect("contained");
        assert_eq!(r.compensation.len(), 1);
    }

    #[test]
    fn ne_is_residual_not_range() {
        let n = names();
        // MV: a != 5    query: a > 5 -> must FAIL (no punctured-interval logic)
        assert!(check_containment(&[gt_a(5)], &[ne_a(5)], &n, &n).is_none());
        // exact match passes
        assert!(check_containment(&[ne_a(5)], &[ne_a(5)], &n, &n).is_some());
    }

    #[test]
    fn between_expands_to_range() {
        let n = names();
        // MV: a BETWEEN 0 AND 100   query: a BETWEEN 10 AND 20 -> contained
        let r = check_containment(&[between_a(10, 20)], &[between_a(0, 100)], &n, &n)
            .expect("contained");
        assert_eq!(r.compensation.len(), 1); // the tighter BETWEEN re-applied
    }

    #[test]
    fn varchar_ranges_match_folded_constants_and_declaration_only_casts() {
        use novarocks_type_contract::{
            CharacterType, FunctionValueType, TrinoCastMode, ValueLayout, ValueTypeRef,
            ValueTypeStorageLifetime,
        };

        let unbounded = FunctionValueType::new(DataType::Utf8, false);
        let mut column = a();
        column.value_type = FunctionValueType::new(DataType::Utf8, true);
        let storage = ValueTypeStorageLifetime::pure();
        let bounded = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::BoundedCharacter(
                    CharacterType::new(CharacterKind::Varchar, 10).unwrap(),
                ),
                false,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&DataType::Utf8, storage).unwrap(),
        )
        .unwrap();
        let cast = |text: &str| TypedExpr {
            kind: ExprKind::Cast {
                cast_mode: Some(TrinoCastMode::Implicit),
                expr: novarocks_parser::ast::AstBox::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(text.into())),
                    value_type: bounded.clone(),
                }),
                target: DataType::Utf8,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: unbounded.clone(),
        };
        let folded = |text: &str| TypedExpr {
            kind: ExprKind::Constant(
                crate::constant::admit_syntax_constant(
                    &LiteralValue::String(text.into()),
                    &unbounded,
                    crate::constant::test_constant_policy(),
                    crate::optimizer::test_optimizer_control(),
                )
                .unwrap(),
            ),
            value_type: unbounded.clone(),
        };
        let mv = [
            bin(col_ref(&column), BinOp::Ge, cast("2021-08-01")),
            bin(col_ref(&column), BinOp::Lt, cast("2021-09-01")),
        ];
        let query = [
            bin(col_ref(&column), BinOp::Ge, folded("2021-08-01")),
            bin(col_ref(&column), BinOp::Lt, folded("2021-09-01")),
        ];
        let n = names();
        assert!(
            check_containment(&query, &mv, &n, &n)
                .expect("equal VARCHAR ranges")
                .compensation
                .is_empty()
        );
        let between = |upper: &str| TypedExpr {
            kind: ExprKind::Between {
                expr: novarocks_parser::ast::AstBox::new(col_ref(&column)),
                low: novarocks_parser::ast::AstBox::new(folded("2021-08-05")),
                high: novarocks_parser::ast::AstBox::new(folded(upper)),
                negated: false,
            },
            value_type: FunctionValueType::new(DataType::Boolean, true),
        };
        assert_eq!(
            check_containment(&[between("2021-08-20")], &mv, &n, &n)
                .expect("contained VARCHAR range")
                .compensation
                .len(),
            1
        );
        assert!(check_containment(&[between("2021-09-01")], &mv, &n, &n).is_none());
    }

    #[test]
    fn incomparable_literals_fail_closed() {
        let n = names();
        // MV: a >= 5 (Int)   query: a >= 'x' (String) -> cannot compare -> fail
        assert!(check_containment(&[ge_a_str("x")], &[ge_a(5)], &n, &n).is_none());
    }
}
