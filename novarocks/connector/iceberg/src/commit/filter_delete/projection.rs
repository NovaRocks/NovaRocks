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

use crate::iceberg::expr::{
    BinaryExpression, BoundPredicate, Predicate, PredicateOperator, Reference, SetExpression,
};
use crate::iceberg::spec::{
    Datum, FieldSummary, Literal, PartitionField, PrimitiveLiteral, PrimitiveType, Transform, Type,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::partition_types::resolved_partition_primitive;

use super::super::conflict_scope::{
    IN_PREDICATE_LIMIT, binary_bounds_may_match, checked_projection_arithmetic,
    projected_partition_matches, projection_boundary_adjustment,
};
use super::FileVerdict;
use super::metrics::compare_literals;

pub(super) struct PartitionProjection {
    ordinal: usize,
    primitive: PrimitiveType,
    wide: Option<Predicate>,
    strict: Option<Predicate>,
    null_verdict: FileVerdict,
}

impl PartitionProjection {
    pub(super) fn new(
        ordinal: usize,
        partition: &PartitionField,
        source: &PrimitiveType,
        leaf: &BoundPredicate,
    ) -> Result<Option<Self>> {
        let transform = partition.transform;
        if matches!(transform, Transform::Void | Transform::Unknown) {
            return Ok(None);
        }
        if matches!(transform, Transform::Bucket(0) | Transform::Truncate(0))
            || matches!(transform, Transform::Bucket(width) if i32::try_from(width).is_err())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid delete partition transform {transform}"),
            ));
        }
        let Type::Primitive(primitive) = transform.result_type(&Type::Primitive(source.clone()))?
        else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Non-primitive delete partition result",
            ));
        };
        let null_verdict = match leaf {
            BoundPredicate::Unary(expression) if expression.op() == PredicateOperator::IsNull => {
                FileVerdict::All
            }
            BoundPredicate::Unary(expression) if expression.op() == PredicateOperator::NotNull => {
                FileVerdict::None
            }
            BoundPredicate::Binary(_) | BoundPredicate::Set(_) => FileVerdict::None,
            _ => FileVerdict::Partial,
        };
        Ok(Some(Self {
            ordinal,
            primitive,
            wide: project(transform, &partition.name, source, leaf, false),
            strict: project(transform, &partition.name, source, leaf, true),
            null_verdict,
        }))
    }

    pub(super) fn summary_may_match(&self, summaries: &[FieldSummary]) -> bool {
        let (Some(summary), Some(predicate)) = (summaries.get(self.ordinal), &self.wide) else {
            return true;
        };
        let decode = |bytes: &[u8]| {
            if self.primitive == PrimitiveType::Boolean && !matches!(bytes, [0] | [1]) {
                return None;
            }
            Datum::try_from_bytes(bytes, self.primitive.clone()).ok()
        };
        let lower = summary
            .lower_bound
            .as_ref()
            .and_then(|bytes| decode(bytes.as_ref()));
        let upper = summary
            .upper_bound
            .as_ref()
            .and_then(|bytes| decode(bytes.as_ref()));
        if let Some((lower, upper)) = lower.as_ref().zip(upper.as_ref())
            && compare_literals(lower.literal(), upper.literal())
                .is_some_and(std::cmp::Ordering::is_gt)
        {
            return true;
        }
        let bounds_may_match = |op, value: &Datum| {
            binary_bounds_may_match(
                op,
                lower
                    .as_ref()
                    .and_then(|bound| compare_literals(bound.literal(), value.literal())),
                upper
                    .as_ref()
                    .and_then(|bound| compare_literals(bound.literal(), value.literal())),
            )
        };
        // Non-null bounds also exclude NaNs. A possible NaN preserves the
        // summary when it could satisfy the source SQL comparison.
        let comparable = !matches!(self.primitive, PrimitiveType::Float | PrimitiveType::Double)
            || summary.contains_nan == Some(false);
        match predicate {
            Predicate::Unary(expression) if expression.op() == PredicateOperator::IsNull => {
                summary.contains_null
            }
            Predicate::Binary(expression) if comparable => {
                bounds_may_match(expression.op(), expression.literal())
            }
            Predicate::Set(expression)
                if comparable
                    && expression.op() == PredicateOperator::In
                    && expression.literals().len() <= IN_PREDICATE_LIMIT =>
            {
                expression
                    .literals()
                    .iter()
                    .any(|literal| bounds_may_match(PredicateOperator::Eq, literal))
            }
            _ => true,
        }
    }

    pub(super) fn file_verdict(&self, values: &[Option<Literal>]) -> Result<FileVerdict> {
        let invalid = || {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid delete partition value at index {}", self.ordinal),
            )
        };
        let Some(value) = values.get(self.ordinal).ok_or_else(invalid)? else {
            return Ok(self.null_verdict);
        };
        let Literal::Primitive(stored) = value else {
            return Err(invalid());
        };
        let resolved = resolved_partition_primitive(stored, &self.primitive);
        if !self.primitive.compatible(&resolved) {
            return Err(invalid());
        }
        let widened;
        let value = if let std::borrow::Cow::Owned(value) = resolved {
            widened = Literal::Primitive(value);
            &widened
        } else {
            // The original value already has the expected representation.
            value
        };
        let matches = |predicate, unknown| {
            projected_partition_matches(predicate, Some(value), unknown, &compare_literals)
        };
        if self
            .wide
            .as_ref()
            .is_some_and(|predicate| !matches(predicate, true))
        {
            return Ok(FileVerdict::None);
        }
        if self
            .strict
            .as_ref()
            .is_some_and(|predicate| matches(predicate, false))
        {
            return Ok(FileVerdict::All);
        }
        Ok(FileVerdict::Partial)
    }
}

fn project(
    transform: Transform,
    name: &str,
    source: &PrimitiveType,
    leaf: &BoundPredicate,
    strict: bool,
) -> Option<Predicate> {
    let is_identity = transform == Transform::Day && *source == PrimitiveType::Date
        || transform == Transform::Truncate(1)
            && matches!(
                source,
                PrimitiveType::Int | PrimitiveType::Long | PrimitiveType::Decimal { .. }
            );
    if is_identity || exact_truncate_string_comparison(transform, leaf) {
        return Transform::Identity.project(name, leaf).ok().flatten();
    }
    if !checked_projection_arithmetic(&transform, leaf, strict)
        || !hour_micros_projection_fits(transform, source, leaf, strict)
        || matches!(transform, Transform::Truncate(_))
            && matches!(source, PrimitiveType::Decimal { .. })
            && !matches!(leaf, BoundPredicate::Unary(_))
    {
        return None;
    }
    let projected = if strict {
        transform.strict_project(name, leaf)
    } else {
        transform.project(name, leaf)
    }
    .ok()
    .flatten()?;
    Some(apply_legacy_time_fix(
        transform, name, source, projected, strict,
    ))
}

fn exact_truncate_string_comparison(transform: Transform, leaf: &BoundPredicate) -> bool {
    let (Transform::Truncate(width), BoundPredicate::Binary(expression)) = (transform, leaf) else {
        return false;
    };
    let PrimitiveLiteral::String(point) = expression.literal().literal() else {
        return false;
    };
    // A short partition value is the complete string. A longer value cannot
    // truncate to it, and the relative ordering across that boundary is exact.
    point.chars().count() < width as usize
}

fn hour_micros_projection_fits(
    transform: Transform,
    source: &PrimitiveType,
    leaf: &BoundPredicate,
    strict: bool,
) -> bool {
    if transform != Transform::Hour
        || !matches!(
            source,
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz
        )
    {
        return true;
    }
    let check = |datum: &Datum, adjustment: i64| {
        if let PrimitiveLiteral::Long(micros) = datum.literal() {
            micros
                .checked_add(adjustment)
                .is_some_and(|value| i32::try_from(value.div_euclid(3_600_000_000)).is_ok())
        } else {
            false
        }
    };
    match leaf {
        BoundPredicate::Binary(expression) => check(
            expression.literal(),
            projection_boundary_adjustment(expression.op(), strict),
        ),
        BoundPredicate::Set(expression) => {
            expression.literals().iter().all(|datum| check(datum, 0))
        }
        _ => true,
    }
}

fn apply_legacy_time_fix(
    transform: Transform,
    name: &str,
    source: &PrimitiveType,
    predicate: Predicate,
    strict: bool,
) -> Predicate {
    // Iceberg Java 1.12.0 ProjectionUtil fixes both projections for old
    // time transforms that could encode negative partition values one larger.
    // iceberg-rust covers inclusive year/month and day(timestamp micros).
    let needs_fix = match transform {
        Transform::Year | Transform::Month => strict,
        Transform::Hour => true,
        Transform::Day => {
            *source != PrimitiveType::Date && (strict || *source != PrimitiveType::Timestamp)
        }
        _ => false,
    };
    if !needs_fix {
        return predicate;
    }
    match &predicate {
        Predicate::Binary(expression) => {
            let literal = expression.literal();
            let op = expression.op();
            if !strict && op == PredicateOperator::Eq {
                if let Some(shifted) = shift_legacy_bound(literal, false) {
                    return Predicate::Set(SetExpression::new(
                        PredicateOperator::In,
                        Reference::new(name),
                        [literal.clone(), shifted].into_iter().collect(),
                    ));
                }
            } else {
                let shift = if strict
                    && matches!(
                        op,
                        PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEq
                    ) {
                    shift_legacy_bound(literal, true)
                } else if !strict
                    && matches!(
                        op,
                        PredicateOperator::LessThan | PredicateOperator::LessThanOrEq
                    )
                {
                    shift_legacy_bound(literal, false)
                } else {
                    None
                };
                if let Some(shifted) = shift {
                    return Predicate::Binary(BinaryExpression::new(
                        op,
                        Reference::new(name),
                        shifted,
                    ));
                }
            }
        }
        Predicate::Set(expression) if !strict && expression.op() == PredicateOperator::In => {
            let mut fixed = expression.literals().clone();
            fixed.extend(
                expression
                    .literals()
                    .iter()
                    .filter_map(|literal| shift_legacy_bound(literal, false)),
            );
            return Predicate::Set(SetExpression::new(
                PredicateOperator::In,
                Reference::new(name),
                fixed,
            ));
        }
        _ => {}
    }
    predicate
}

fn shift_legacy_bound(literal: &Datum, include_zero: bool) -> Option<Datum> {
    let PrimitiveLiteral::Int(value) = *literal.literal() else {
        return None;
    };
    if value > 0 || (value == 0 && !include_zero) {
        return None;
    }
    Some(match literal.data_type() {
        PrimitiveType::Date => Datum::date(value + 1),
        PrimitiveType::Int => Datum::int(value + 1),
        _ => return None,
    })
}
