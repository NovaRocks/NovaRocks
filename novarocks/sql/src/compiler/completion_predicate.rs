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

//! Pure lowering of exact scan predicate occurrences into provider needs.
//!
//! A conjunct is offered only when its complete SQL meaning has an exact typed
//! tuple-domain representation. Every other conjunct remains an engine scan
//! residual. Column identity arrives as an exact `ColumnId -> projection
//! ordinal` map built while the provider projection is frozen; this module
//! never resolves a column by a display name.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Constraint, Domain, MAX_CONNECTOR_VALUE_BYTES,
    Range, TupleDomain, ValueSet,
};
use novarocks_type_contract::{CompileCheckpoints, TemporalPrecision, TemporalType};

use super::completion::{ProviderPredicateOccurrenceId, ProviderReadPredicateNeed};
use crate::analysis::scalar_effects::ScalarEffectQuery;
use crate::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::column_id::ColumnId;
use crate::planner::payload::PlanScanNode;

#[derive(Clone, Copy)]
pub(super) struct ProviderPredicateColumn {
    pub(super) ordinal: u32,
    pub(super) value_type: ConnectorValueType,
}

pub(super) fn lower_provider_predicates(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    row_mutation_match: bool,
    effects: &ScalarEffectQuery<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[ProviderReadPredicateNeed]>, super::SqlCompileError> {
    lower_provider_conjunction(scan, columns, row_mutation_match, false, effects, work)
}

pub(super) fn lower_delete_provider_predicates(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    effects: &ScalarEffectQuery<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[ProviderReadPredicateNeed]>, super::SqlCompileError> {
    lower_provider_conjunction(scan, columns, false, true, effects, work)
}

fn lower_provider_conjunction(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    row_mutation_match: bool,
    delete_probe: bool,
    effects: &ScalarEffectQuery<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[ProviderReadPredicateNeed]>, super::SqlCompileError> {
    let mut predicates = Vec::new();
    for (ordinal, predicate) in scan.predicates.iter().enumerate() {
        work.step()?;
        // A provider may reduce input before any engine residual, even with
        // a non-exact offer. Stop where this would skip original demanded work.
        if !effects.typed(predicate)?.can_skip_for_earlier_filter() {
            break;
        }
        let Ok(occurrence) = u32::try_from(ordinal) else {
            continue;
        };
        let lowered = if delete_probe {
            lower_delete_predicate(scan, columns, predicate, work)?
        } else {
            lower_provider_atom(scan, columns, predicate, row_mutation_match, work)?
        };
        let Some((column, domain, exact_eligible)) = lowered else {
            continue;
        };
        if let Ok(summary) = TupleDomain::with_column_domains(BTreeMap::from([(column, domain)])) {
            predicates.push(
                ProviderReadPredicateNeed::new(
                    ProviderPredicateOccurrenceId::new(occurrence),
                    Constraint::of_summary(summary),
                )
                .with_exact_eligibility(exact_eligible),
            );
        }
    }
    Ok(predicates.into_boxed_slice())
}

/// A tuple domain can express a disjunction on one field as a union of
/// complete domains. Retain other disjunctions in the original DELETE.
#[recursive::recursive]
fn lower_delete_predicate(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    predicate: &TypedExpr,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<(u32, Domain, bool)>, super::SqlCompileError> {
    work.step()?;
    let ExprKind::BinaryOp {
        left,
        op: BinOp::Or,
        right,
        ..
    } = &unnest(predicate).kind
    else {
        return lower_provider_atom(scan, columns, predicate, false, work);
    };
    let Some((column, left, left_exact)) = lower_delete_predicate(scan, columns, left, work)?
    else {
        return Ok(None);
    };
    let Some((right_column, right, right_exact)) =
        lower_delete_predicate(scan, columns, right, work)?
    else {
        return Ok(None);
    };
    if column != right_column {
        return Ok(None);
    }
    Ok(left
        .union(&right)
        .ok()
        .filter(|domain| !domain.is_all())
        .map(|domain| (column, domain, left_exact && right_exact)))
}

fn lower_provider_atom(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    predicate: &TypedExpr,
    row_mutation_match: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<(u32, Domain, bool)>, super::SqlCompileError> {
    if !can_offer_atom(predicate, scan, columns) {
        return Ok(None);
    }
    let mut failure = None;
    let atom = lower_atom_with_literal(
        predicate,
        |expression| lower_column(scan, columns, expression),
        |expression, expected| {
            if let ConnectorValueType::Temporal(declaration) = expected {
                let expression = unnest(expression);
                let ExprKind::Constant(value) = &expression.kind else {
                    return None;
                };
                if expression.value_type.value_type().temporal() != Some(declaration)
                    || value.value_type() != &expression.value_type
                {
                    return None;
                }
                let decoded = work
                    .flush()
                    .map_err(novarocks_functions::ConstantError::from)
                    .and_then(|()| {
                        value.temporal_observed(
                            novarocks_type_contract::CompilePhase::Validate,
                            work.control(),
                        )
                    });
                match decoded {
                    Ok(value) => value.map(|value| ConnectorValue::Temporal {
                        data_type: declaration,
                        value,
                    }),
                    Err(cause) => {
                        failure = Some(match cause {
                            novarocks_functions::ConstantError::Control(cause) => {
                                super::SqlCompileError::from(cause)
                            }
                            other => super::SqlCompileError::Compilation(other.to_string()),
                        });
                        None
                    }
                }
            } else if row_mutation_match {
                lower_row_mutation_literal(expression, expected)
            } else {
                lower_literal(expression, expected)
            }
        },
    );
    if let Some(cause) = failure {
        return Err(cause);
    }
    let Some((column, domain)) = atom else {
        return Ok(None);
    };
    Ok(Some((
        column,
        complete_datetime_offer(domain, work)?,
        exact_atom(predicate),
    )))
}

/// Every accepted comparison was already checked in its actual SQL type.
/// Carry its unchanged bounds at full precision so occurrences with different
/// lossless coercions of the same column can share the exact conjunction.
fn complete_datetime_offer(
    domain: Domain,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Domain, super::SqlCompileError> {
    let ConnectorValueType::Temporal(source) = domain.value_type() else {
        return Ok(domain);
    };
    let picos = TemporalPrecision::PICOSECONDS;
    let target = match source {
        TemporalType::Time(_) => TemporalType::Time(picos),
        TemporalType::TimeWithTimeZone(_) => TemporalType::TimeWithTimeZone(picos),
        TemporalType::Timestamp(_) => TemporalType::Timestamp(picos),
        TemporalType::TimestampWithTimeZone(_) => TemporalType::TimestampWithTimeZone(picos),
        TemporalType::Interval(_) => return Ok(domain),
    };
    if source == target {
        return Ok(domain);
    }

    let promote = |value: &ConnectorValue| match value {
        ConnectorValue::Temporal { data_type, value } if *data_type == source => {
            Ok(ConnectorValue::Temporal {
                data_type: target,
                value: *value,
            })
        }
        _ => Err(super::SqlCompileError::Compilation(
            "provider temporal offer bound differs from its exact SQL declaration".into(),
        )),
    };
    let bound = |bound: &Bound| -> Result<Bound, super::SqlCompileError> {
        Ok(match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Inclusive(value) => Bound::Inclusive(promote(value)?),
            Bound::Exclusive(value) => Bound::Exclusive(promote(value)?),
        })
    };
    let value_type = ConnectorValueType::Temporal(target);
    let invalid = |error: novarocks_spi::connector::ConnectorError| {
        super::SqlCompileError::Compilation(error.to_string())
    };
    let mut ranges = Vec::with_capacity(domain.values().ranges().len());
    for range in domain.values().ranges() {
        work.step()?;
        ranges.push(
            Range::try_new(value_type, bound(range.low())?, bound(range.high())?)
                .map_err(invalid)?,
        );
    }
    Ok(Domain::new(
        ValueSet::of_ranges(value_type, ranges).map_err(invalid)?,
        domain.null_allowed(),
    ))
}

/// Prove that the original expression, rather than its transported domain,
/// has the closed comparison or intrinsic shape accepted by physical plans.
fn exact_atom(predicate: &TypedExpr) -> bool {
    let predicate = unnest(predicate);
    let is_column = |expr: &TypedExpr| matches!(unnest(expr).kind, ExprKind::ColumnRef { .. });
    let is_literal = |expr: &TypedExpr| {
        !expr.value_type.nullable()
            && matches!(
                unnest(expr).kind,
                ExprKind::Literal(_) | ExprKind::Constant(_)
            )
    };
    if let Some((_, [left, right])) = crate::common::binary_operator::typed_comparison(predicate) {
        return left.value_type.data_type() == right.value_type.data_type()
            && left.value_type.value_type().temporal() == right.value_type.value_type().temporal()
            && ((is_column(left) && is_literal(right)) || (is_literal(left) && is_column(right)));
    }
    match &predicate.kind {
        ExprKind::IsNull { expr, .. } => is_column(expr),
        ExprKind::InList { expr, list, .. } => is_column(expr) && list.iter().all(is_literal),
        _ => false,
    }
}

fn can_offer_atom(
    predicate: &TypedExpr,
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
) -> bool {
    let predicate = unnest(predicate);
    let ExprKind::FunctionCall { binding, .. } = &predicate.kind else {
        return true;
    };
    let Some((operator, operands)) = crate::common::binary_operator::typed_operands(predicate)
    else {
        return false;
    };
    if operator.comparison().is_none()
        || operator.comparison_overload_text() != Some(binding.selected.overload.as_str())
    {
        return false;
    }
    let [left, right] = operands.map(unnest);
    if !novarocks_physical_plan::supports_provider_scan_comparison(&left.value_type)
        || !novarocks_physical_plan::supports_provider_scan_comparison(&right.value_type)
    {
        return false;
    }
    let is_literal = |expression: &TypedExpr| {
        !expression.value_type.nullable()
            && matches!(
                expression.kind,
                ExprKind::Literal(_) | ExprKind::Constant(_)
            )
    };
    (lower_column(scan, columns, left).is_some() && is_literal(right))
        || (is_literal(left) && lower_column(scan, columns, right).is_some())
}

/// Shared exact atom author. Each caller supplies its original column identity
/// and complete type checks; this function never resolves names or scans.
pub(super) fn lower_atom_with_literal(
    predicate: &TypedExpr,
    resolve_column: impl Fn(&TypedExpr) -> Option<ProviderPredicateColumn>,
    mut lower_literal: impl FnMut(&TypedExpr, ConnectorValueType) -> Option<ConnectorValue>,
) -> Option<(u32, Domain)> {
    let predicate = unnest(predicate);
    if let Some((op, [left, right])) = crate::common::binary_operator::typed_comparison(predicate) {
        // A standard floating predicate can exclude NaN or distinguish the
        // value relation from Provider metadata ordering. Keep its own evaluation.
        if crate::common::binary_operator::typed_operands(predicate).is_some()
            && [left, right].iter().any(|operand| {
                !crate::common::binary_operator::permits_equality_substitution(
                    operand.value_type.data_type(),
                )
            })
        {
            return None;
        }
        let (column, op, literal) = if let Some(column) = resolve_column(left) {
            (column, op, right)
        } else {
            (resolve_column(right)?, reverse_comparison(op), left)
        };
        let value = lower_literal(literal, column.value_type)?;
        return Some((
            column.ordinal,
            comparison_domain(column.value_type, op, value)?,
        ));
    }
    match &predicate.kind {
        ExprKind::IsNull { expr, negated } => {
            let column = resolve_column(expr)?;
            Some((
                column.ordinal,
                if *negated {
                    Domain::not_null(column.value_type)
                } else {
                    Domain::only_null(column.value_type)
                },
            ))
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } if !negated && !list.is_empty() => {
            let column = resolve_column(expr)?;
            let values = list
                .iter()
                .map(|literal| lower_literal(literal, column.value_type))
                .collect::<Option<Vec<_>>>()?;
            let values = ValueSet::of_values(column.value_type, values).ok()?;
            Some((column.ordinal, Domain::new(values, false)))
        }
        _ => None,
    }
}

fn lower_column(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    expression: &TypedExpr,
) -> Option<ProviderPredicateColumn> {
    let expression = unnest(expression);
    let (source, widened) = if let ExprKind::FunctionCall { binding, args, .. } = &expression.kind {
        let [source] = &args[..] else { return None };
        let original = source.value_type.value_type().temporal()?;
        let widened = expression.value_type.value_type().temporal()?;
        use novarocks_functions::builtin::value_conversion as owner;
        if !original.is_lossless_widening_to(widened)
            || binding.function_id.as_str() != owner::VALUE_CONVERSION_FUNCTION_ID
            || owner::temporal_cast_overload(&source.value_type, &expression.value_type, false)
                != Some(binding.selected.overload.as_str())
        {
            return None;
        }
        (unnest(source), Some((original, widened)))
    } else {
        (expression, None)
    };
    let ExprKind::ColumnRef { column_id, .. } = &source.kind else {
        return None;
    };
    if scan
        .variant_columns
        .iter()
        .any(|column| column.synthetic_column_id() == *column_id)
    {
        return None;
    }
    let output = scan
        .columns
        .iter()
        .find(|output| output.column_id == *column_id)?;
    if output.value_type != source.value_type {
        return None;
    }
    // Composite assignments can name a scan column, but the provider domain
    // vocabulary cannot represent even an IS NULL domain over that type.
    // Keep the exact SQL predicate as an engine residual instead of emitting
    // a constraint that the native decoder must reject.
    let mut column = *columns.get(column_id)?;
    if let Some((original, widened)) = widened {
        if column.value_type != ConnectorValueType::Temporal(original) {
            return None;
        }
        column.value_type = ConnectorValueType::Temporal(widened);
    }
    column.value_type.is_comparable().then_some(column)
}

fn comparison_domain(
    value_type: ConnectorValueType,
    op: BinOp,
    value: ConnectorValue,
) -> Option<Domain> {
    let values = match op {
        BinOp::Eq => ValueSet::of_values(value_type, vec![value]).ok()?,
        BinOp::Ne => ValueSet::of_ranges(
            value_type,
            vec![
                Range::try_new(
                    value_type,
                    Bound::Unbounded,
                    Bound::Exclusive(value.clone()),
                )
                .ok()?,
                Range::try_new(value_type, Bound::Exclusive(value), Bound::Unbounded).ok()?,
            ],
        )
        .ok()?,
        BinOp::Lt => single_range(value_type, Bound::Unbounded, Bound::Exclusive(value))?,
        BinOp::Le => single_range(value_type, Bound::Unbounded, Bound::Inclusive(value))?,
        BinOp::Gt => single_range(value_type, Bound::Exclusive(value), Bound::Unbounded)?,
        BinOp::Ge => single_range(value_type, Bound::Inclusive(value), Bound::Unbounded)?,
        BinOp::EqForNull
        | BinOp::Add
        | BinOp::Sub
        | BinOp::Mul
        | BinOp::Div
        | BinOp::Mod
        | BinOp::And
        | BinOp::Or => return None,
    };
    Some(Domain::new(values, false))
}

fn single_range(value_type: ConnectorValueType, low: Bound, high: Bound) -> Option<ValueSet> {
    ValueSet::of_ranges(
        value_type,
        vec![Range::try_new(value_type, low, high).ok()?],
    )
    .ok()
}

fn lower_row_mutation_literal(
    expression: &TypedExpr,
    expected: ConnectorValueType,
) -> Option<ConnectorValue> {
    let expression = unnest(expression);
    let ExprKind::Constant(value) = &expression.kind else {
        return lower_literal(expression, expected);
    };
    // Folded values carry their original exact SQL declaration. Read only the
    // selected physical signed value, preserving NULL and carrier checks.
    if expression.value_type.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || value.value_type() != &expression.value_type
        || exact_predicate_value_type(expression.value_type.data_type())? != expected
    {
        return None;
    }
    let integer = value.try_i64().ok().flatten()?;
    signed_integer_value(integer, expected)
}

fn signed_integer_value(value: i64, expected: ConnectorValueType) -> Option<ConnectorValue> {
    match expected {
        ConnectorValueType::TinyInt => Some(ConnectorValue::TinyInt(i8::try_from(value).ok()?)),
        ConnectorValueType::SmallInt => Some(ConnectorValue::SmallInt(i16::try_from(value).ok()?)),
        ConnectorValueType::Integer => Some(ConnectorValue::Integer(i32::try_from(value).ok()?)),
        ConnectorValueType::BigInt => Some(ConnectorValue::BigInt(value)),
        _ => None,
    }
}

pub(super) fn lower_literal(
    expression: &TypedExpr,
    expected: ConnectorValueType,
) -> Option<ConnectorValue> {
    use novarocks_type_contract::{CharacterKind, ValueKind, trino_can_coerce_scalar};

    let varchar = |kind: &ValueKind| match kind {
        ValueKind::String => true,
        ValueKind::BoundedCharacter(character) => character.kind() == CharacterKind::Varchar,
        _ => false,
    };
    let expression = unnest(expression);
    if (expression.value_type.nullable() && !matches!(expression.kind, ExprKind::Cast { .. }))
        || exact_predicate_value_type(expression.value_type.data_type())? != expected
        || (expected == ConnectorValueType::Varchar
            && !varchar(expression.value_type.value_type().kind()))
    {
        return None;
    }
    // The original analyzer retains the coercion. A non-null signed integer
    // or VARCHAR widening preserves the value in the declared provider domain
    // without evaluating a scalar function.
    let mut source = expression;
    while let ExprKind::Cast { expr, target, .. } = &source.kind {
        if target != source.value_type.data_type() {
            return None;
        }
        let preserving = if expected == ConnectorValueType::Varchar {
            let from = expr.value_type.value_type().kind();
            let to = source.value_type.value_type().kind();
            !expr.value_type.nullable()
                && varchar(from)
                && varchar(to)
                && trino_can_coerce_scalar(from, to)
        } else {
            crate::optimizer::scalar_expr::nonnull_signed_literal_widening(
                &expr.value_type,
                &source.value_type,
            )
        };
        if !preserving {
            return None;
        }
        source = unnest(expr);
    }
    if !std::ptr::eq(source, expression) && expected != ConnectorValueType::Varchar {
        let ExprKind::Literal(LiteralValue::Int(value)) = &source.kind else {
            return None;
        };
        if !crate::optimizer::scalar_expr::signed_integer_fits(
            *value,
            source.value_type.data_type(),
        ) {
            return None;
        }
        return signed_integer_value(*value, expected);
    }
    if let ExprKind::Constant(constant) = &source.kind
        && expected == ConnectorValueType::Varchar
        && constant.value_type() == &source.value_type
        && source.value_type.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
    {
        let text = constant.try_utf8().ok().flatten()?;
        return (text.len() <= MAX_CONNECTOR_VALUE_BYTES)
            .then(|| ConnectorValue::Varchar(Arc::from(text)));
    }
    let ExprKind::Literal(literal) = &source.kind else {
        return None;
    };
    let value = match (expected, literal) {
        (ConnectorValueType::Boolean, LiteralValue::Bool(value)) => ConnectorValue::Boolean(*value),
        (ConnectorValueType::TinyInt, LiteralValue::Int(value)) => {
            ConnectorValue::TinyInt(i8::try_from(*value).ok()?)
        }
        (ConnectorValueType::SmallInt, LiteralValue::Int(value)) => {
            ConnectorValue::SmallInt(i16::try_from(*value).ok()?)
        }
        (ConnectorValueType::Integer, LiteralValue::Int(value)) => {
            ConnectorValue::Integer(i32::try_from(*value).ok()?)
        }
        (ConnectorValueType::BigInt, LiteralValue::Int(value)) => ConnectorValue::BigInt(*value),
        (ConnectorValueType::Double, LiteralValue::Float(value)) if !value.is_nan() => {
            ConnectorValue::Double(*value)
        }
        (ConnectorValueType::Date, LiteralValue::Int(value)) => {
            ConnectorValue::Date(i32::try_from(*value).ok()?)
        }
        (ConnectorValueType::TimeMicros, LiteralValue::Int(value)) => {
            ConnectorValue::TimeMicros(*value)
        }
        (ConnectorValueType::TimestampMillis, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampMillis(*value)
        }
        (ConnectorValueType::TimestampMicros, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampMicros(*value)
        }
        (ConnectorValueType::TimestampNanos, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampNanos(*value)
        }
        (ConnectorValueType::TimestampTzMicros, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampTzMicros(*value)
        }
        (ConnectorValueType::TimestampTzNanos, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampTzNanos(*value)
        }
        (ConnectorValueType::Varchar, LiteralValue::String(value)) => {
            ConnectorValue::Varchar(Arc::from(value.as_str()))
        }
        (ConnectorValueType::Varbinary, LiteralValue::Binary(value)) => {
            ConnectorValue::Varbinary(Arc::from(value.as_slice()))
        }
        _ => return None,
    };
    (value.payload_bytes() <= MAX_CONNECTOR_VALUE_BYTES).then_some(value)
}

fn exact_predicate_value_type(data_type: &DataType) -> Option<ConnectorValueType> {
    match data_type {
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, Some(zone))
            if !zone.eq_ignore_ascii_case("UTC") =>
        {
            None
        }
        DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => None,
        _ => super::completion::provider_connector_type_for_engine(data_type),
    }
}
const fn reverse_comparison(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        BinOp::Eq
        | BinOp::Ne
        | BinOp::EqForNull
        | BinOp::Add
        | BinOp::Sub
        | BinOp::Mul
        | BinOp::Div
        | BinOp::Mod
        | BinOp::And
        | BinOp::Or => op,
    }
}

fn unnest(mut expression: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expression.kind {
        expression = inner;
    }
    expression
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    fn offered(
        scan: &PlanScanNode,
        columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    ) -> Box<[ProviderReadPredicateNeed]> {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &control,
            novarocks_type_contract::CompilePhase::Validate,
        )
        .unwrap();
        let effects = ScalarEffectQuery {
            functions: Some(crate::functions::builtin_sql_function_catalog()),
            allow_throw_exception: true,
            control: &control,
        };
        let result = lower_provider_predicates(scan, columns, false, &effects, &mut work).unwrap();
        work.finish().unwrap();
        result
    }

    use crate::analysis::OutputColumn;
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };

    #[test]
    fn provider_signed_literal_widening_keeps_exact_value_and_rejects_narrowing() {
        let catalog = crate::catalog::memory::PlannerMemoryCatalog::default();
        for (sql, expected) in [
            (
                "SELECT CAST(2147483647 AS BIGINT)",
                Some(ConnectorValue::BigInt(2147483647)),
            ),
            ("SELECT CAST(2147483648 AS INTEGER)", None),
            ("SELECT CAST(1.0 AS BIGINT)", None),
            ("SELECT CAST(NULL AS BIGINT)", None),
        ] {
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            let (resolved, _, _) = crate::analyzer::analyze(query, &catalog, "default").unwrap();
            let crate::analysis::QueryBody::Select(select) = resolved.body else {
                panic!("select");
            };
            let expression = &select.projection[0].expr;
            assert_eq!(
                lower_literal(expression, ConnectorValueType::BigInt),
                expected,
                "{sql}"
            );
        }
    }

    fn column() -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(1),
            name: "k".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        }
    }

    fn column_ref() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(1),
                qualifier: None,
                column: "k".to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        }
    }

    fn integer(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        }
    }

    fn comparison(op: BinOp, left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(left),
                op,
                right: novarocks_parser::ast::AstBox::new(right),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        }
    }

    fn scan(predicates: Vec<TypedExpr>) -> PlanScanNode {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    binding,
                    SqlTableIdentity {
                        catalog: "iceberg".to_string(),
                        namespace: "db".to_string(),
                        table: "t".to_string(),
                    },
                    SqlScanKind::Data {
                        version: crate::planner::table::SqlTableVersionSelector::Current,
                    },
                )),
            },
            alias: None,
            columns: vec![column()],
            predicates,
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn columns() -> BTreeMap<ColumnId, ProviderPredicateColumn> {
        BTreeMap::from([(
            ColumnId(1),
            ProviderPredicateColumn {
                ordinal: 0,
                value_type: ConnectorValueType::Integer,
            },
        )])
    }

    #[test]
    fn provider_offers_stop_before_the_original_required_division() {
        let mut division = comparison(BinOp::Div, integer(12), column_ref());
        division.value_type = column_ref().value_type.with_nullable(true).unwrap();
        let fallible = comparison(BinOp::Gt, division, integer(3));
        let guard = comparison(BinOp::Eq, column_ref(), integer(1));
        assert!(offered(&scan(vec![fallible.clone(), guard.clone()]), &columns()).is_empty());
        assert_eq!(offered(&scan(vec![guard, fallible]), &columns()).len(), 1);
    }

    #[test]
    fn exact_occurrences_keep_original_predicate_ordinals() {
        let unsupported = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };
        let scan = scan(vec![
            comparison(BinOp::Ge, column_ref(), integer(7)),
            unsupported,
            comparison(BinOp::Lt, integer(10), column_ref()),
        ]);

        let offered = offered(&scan, &columns());

        assert_eq!(offered.len(), 2);
        assert_eq!(offered[0].occurrence().get(), 0);
        assert_eq!(offered[1].occurrence().get(), 2);
        assert_eq!(offered[0].constraint().summary().columns().count(), 1);
        assert!(offered[0].constraint().expression().is_constant_true());
    }

    #[test]
    fn datetime_predicate_need_proves_plain_micro_and_nanosecond_comparisons() {
        use arrow::datatypes::Field;
        use novarocks_type_contract::{
            CompilePhase, FunctionValueType, TemporalMicros, TemporalValue, ValueKind, ValueLayout,
            ValueTypeRef, ValueTypeStorageLifetime,
        };
        let control = crate::compiler::SqlCompileControl::unbounded();
        for precision in [
            TemporalPrecision::MICROSECONDS,
            TemporalPrecision::NANOSECONDS,
        ] {
            let declared = TemporalType::Timestamp(precision);
            let ty = FunctionValueType::try_from_value(
                ValueTypeRef::try_new(
                    ValueKind::Temporal(declared),
                    false,
                    ValueTypeStorageLifetime::pure(),
                )
                .unwrap(),
                ValueLayout::try_new(&declared.carrier(), ValueTypeStorageLifetime::pure())
                    .unwrap(),
            )
            .unwrap();
            let constant = novarocks_functions::ConstantValue::from_temporal(
                Arc::new(Field::new("literal", ty.data_type().clone(), false)),
                ty.clone(),
                TemporalValue::Timestamp(TemporalMicros::from_picos(0).unwrap()),
                crate::constant::test_constant_policy(),
                CompilePhase::Validate,
                &control,
            )
            .unwrap();
            let literal = TypedExpr {
                kind: ExprKind::Constant(constant),
                value_type: ty.clone(),
            };
            let column = TypedExpr {
                value_type: ty.clone(),
                ..column_ref()
            };
            let mut scan = scan(vec![comparison(BinOp::Ge, column, literal)]);
            scan.columns[0].value_type = ty;
            let columns = BTreeMap::from([(
                ColumnId(1),
                ProviderPredicateColumn {
                    ordinal: 0,
                    value_type: ConnectorValueType::Temporal(declared),
                },
            )]);
            let needs = offered(&scan, &columns);
            let [need] = needs.as_ref() else {
                panic!("expected one complete timestamp comparison")
            };
            assert!(need.exact_eligible());
            let domain = need.constraint().summary().domain_for(&0).unwrap();
            assert_eq!(
                domain.value_type(),
                ConnectorValueType::Temporal(TemporalType::Timestamp(
                    TemporalPrecision::PICOSECONDS
                ))
            );
        }
    }

    #[test]
    fn datetime_comparison_offers_of_mixed_precisions_intersect_exactly() {
        use novarocks_type_contract::{CompilePhase, TemporalMicros, TemporalValue};

        let control = crate::compiler::SqlCompileControl::unbounded();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let timestamp =
            |precision| TemporalType::Timestamp(TemporalPrecision::new(precision).unwrap());
        let point = |data_type, picos| ConnectorValue::Temporal {
            data_type,
            value: TemporalValue::Timestamp(TemporalMicros::from_picos(picos).unwrap()),
        };
        let greater = comparison_domain(
            ConnectorValueType::Temporal(timestamp(9)),
            BinOp::Gt,
            point(timestamp(9), -1_001_000),
        )
        .unwrap();
        let less = comparison_domain(
            ConnectorValueType::Temporal(timestamp(7)),
            BinOp::Lt,
            point(timestamp(7), 400_000),
        )
        .unwrap();
        let non_null = Domain::not_null(ConnectorValueType::Temporal(timestamp(6)));
        let combined = complete_datetime_offer(greater, &mut work)
            .unwrap()
            .intersect(&complete_datetime_offer(less, &mut work).unwrap())
            .unwrap()
            .intersect(&complete_datetime_offer(non_null, &mut work).unwrap())
            .unwrap();

        assert_eq!(
            combined.value_type(),
            ConnectorValueType::Temporal(timestamp(12))
        );
        assert!(!combined.null_allowed());
        let [range] = combined.values().ranges() else {
            panic!("the combined temporal filter must be one exact bounded range");
        };
        assert_eq!(
            range.low(),
            &Bound::Exclusive(point(timestamp(12), -1_001_000))
        );
        assert_eq!(
            range.high(),
            &Bound::Exclusive(point(timestamp(12), 400_000))
        );
        work.finish().unwrap();
    }

    #[test]
    fn column_identity_is_not_recovered_from_expression_name() {
        let expression = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(9),
                qualifier: None,
                column: "k".to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        };
        let scan = scan(vec![comparison(BinOp::Eq, expression, integer(1))]);

        assert!(offered(&scan, &columns()).is_empty());
    }

    #[test]
    fn composite_null_checks_remain_engine_residuals() {
        use arrow::datatypes::Field;

        let composite_types = [
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::Struct(vec![Field::new("k", DataType::Int32, true)].into()),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int32, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];
        for data_type in composite_types {
            for negated in [false, true] {
                let mut expression = column_ref();
                expression.value_type =
                    novarocks_type_contract::FunctionValueType::try_with_logical_type(
                        data_type.clone(),
                        expression.value_type.nullable(),
                        expression.value_type.logical_type(),
                    )
                    .unwrap();
                expression.value_type = expression.value_type.with_nullable(true).unwrap();
                let predicate = TypedExpr {
                    kind: ExprKind::IsNull {
                        expr: novarocks_parser::ast::AstBox::new(expression),
                        negated,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
                };
                let mut scan = scan(vec![predicate]);
                scan.columns[0].value_type =
                    novarocks_type_contract::FunctionValueType::try_with_logical_type(
                        data_type.clone(),
                        scan.columns[0].value_type.nullable(),
                        scan.columns[0].value_type.logical_type(),
                    )
                    .unwrap();
                scan.columns[0].value_type =
                    scan.columns[0].value_type.with_nullable(true).unwrap();
                let columns = BTreeMap::from([(
                    ColumnId(1),
                    ProviderPredicateColumn {
                        ordinal: 0,
                        value_type: ConnectorValueType::NonComparable,
                    },
                )]);
                assert!(offered(&scan, &columns).is_empty());
                assert_eq!(scan.predicates.len(), 1, "SQL residual is retained");
            }
        }
    }

    #[test]
    fn negated_in_list_remains_an_engine_residual() {
        let predicate = TypedExpr {
            kind: ExprKind::InList {
                expr: novarocks_parser::ast::AstBox::new(column_ref()),
                list: (vec![integer(1), integer(2)]).into(),
                negated: true,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        };

        assert!(offered(&scan(vec![predicate]), &columns()).is_empty());
    }
}
