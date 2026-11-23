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

use std::cmp::Ordering;

use crate::iceberg::expr::{BoundPredicate, PredicateOperator};
use crate::iceberg::spec::{DataFile, Datum, FormatVersion, PrimitiveLiteral, PrimitiveType};

use super::super::conflict_scope::{
    IN_PREDICATE_LIMIT, MetricBounds, binary_bounds_may_match, column_metric_bounds,
};
use super::FileVerdict;
use super::condition::is_order_preserving_promotion;

pub(super) struct MetricEvidence<'a> {
    bounds: MetricBounds<'a>,
    all_null: bool,
    no_null: bool,
    no_nan: bool,
}

impl<'a> MetricEvidence<'a> {
    pub(super) fn new(
        file: &'a DataFile,
        id: i32,
        primitive: &PrimitiveType,
        version: FormatVersion,
    ) -> Self {
        let bounds = column_metric_bounds(file, id, primitive, |source| {
            is_order_preserving_promotion(version, source, primitive)
        });
        let nulls = file.null_value_counts().get(&id).copied();
        Self {
            bounds,
            all_null: nulls == Some(file.record_count()),
            no_null: nulls == Some(0),
            no_nan: !matches!(primitive, PrimitiveType::Float | PrimitiveType::Double)
                || file.nan_value_counts().get(&id) == Some(&0),
        }
    }

    pub(super) fn verdict(&self, leaf: &BoundPredicate) -> FileVerdict {
        if let BoundPredicate::Unary(expression) = leaf {
            return match expression.op() {
                PredicateOperator::IsNull if self.no_null => FileVerdict::None,
                PredicateOperator::IsNull if self.all_null => FileVerdict::All,
                PredicateOperator::NotNull if self.no_null => FileVerdict::All,
                PredicateOperator::NotNull if self.all_null => FileVerdict::None,
                _ => FileVerdict::Partial,
            };
        }
        if self.all_null {
            return FileVerdict::None;
        }
        if !self.no_nan {
            // Bounds describe non-NaN values. They cannot settle possible
            // matches for a comparison over an unaccounted floating value.
            return FileVerdict::Partial;
        }
        let lower = self.bounds.0.as_deref();
        let upper = self.bounds.1.as_deref();
        let compare = |bound: Option<&Datum>, literal: &Datum| {
            bound.and_then(|bound| compare_literals(bound.literal(), literal.literal()))
        };
        if let (Some(lower), Some(upper)) = (lower, upper)
            && compare(Some(lower), upper).is_some_and(Ordering::is_gt)
        {
            return FileVerdict::Partial;
        }
        match leaf {
            BoundPredicate::Binary(expression) => {
                let operand = expression.literal();
                let lower = compare(lower, operand);
                let upper = compare(upper, operand);
                let op = expression.op();
                if !binary_bounds_may_match(op, lower, upper) {
                    return FileVerdict::None;
                }
                let all = match op {
                    PredicateOperator::Eq => lower == Some(Ordering::Equal) && upper == lower,
                    PredicateOperator::LessThan => upper.is_some_and(Ordering::is_lt),
                    PredicateOperator::LessThanOrEq => upper.is_some_and(Ordering::is_le),
                    PredicateOperator::GreaterThan => lower.is_some_and(Ordering::is_gt),
                    PredicateOperator::GreaterThanOrEq => lower.is_some_and(Ordering::is_ge),
                    _ => false,
                };
                if self.no_null && all {
                    return FileVerdict::All;
                }
            }
            BoundPredicate::Set(expression) if expression.op() == PredicateOperator::In => {
                let literals = expression.literals();
                if literals.len() <= IN_PREDICATE_LIMIT
                    && !literals.iter().any(|literal| {
                        binary_bounds_may_match(
                            PredicateOperator::Eq,
                            compare(lower, literal),
                            compare(upper, literal),
                        )
                    })
                {
                    return FileVerdict::None;
                }
                if self.no_null
                    && lower.zip(upper).is_some_and(|(lower, upper)| {
                        compare(Some(lower), upper) == Some(Ordering::Equal)
                            && literals.contains(lower)
                    })
                {
                    return FileVerdict::All;
                }
            }
            _ => {}
        }
        FileVerdict::Partial
    }
}

pub(super) fn compare_literals(
    left: &PrimitiveLiteral,
    right: &PrimitiveLiteral,
) -> Option<Ordering> {
    match (left, right) {
        // SQL compares signed zeros as equal. NaN is outside the ordered
        // connector domain and always requires separate evidence.
        (PrimitiveLiteral::Float(left), PrimitiveLiteral::Float(right)) => {
            left.0.partial_cmp(&right.0)
        }
        (PrimitiveLiteral::Double(left), PrimitiveLiteral::Double(right)) => {
            left.0.partial_cmp(&right.0)
        }
        _ if std::mem::discriminant(left) == std::mem::discriminant(right) => {
            left.partial_cmp(right)
        }
        _ => None,
    }
}
