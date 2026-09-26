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

//! Trino-aligned predicate algebra for the connector read stack.
//!
//! `TupleDomain` / `Domain` / `ValueSet` / `Range` are the only predicate
//! contract crossing the SPI. They are transport-neutral and generic over the
//! provider's own column handle type, so no provider ordinal, wire tag, or
//! opaque payload appears in a predicate.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{ConnectorError, ConnectorErrorKind};

use super::value::{ConnectorValue, ConnectorValueType};

pub const MAX_TUPLE_DOMAIN_COLUMNS: usize = 4096;
pub const MAX_VALUE_SET_RANGES: usize = 4096;
pub const MAX_VALUE_SET_DISCRETE_VALUES: usize = 4096;
pub const MAX_CONNECTOR_EXPRESSION_NODES: usize = 16_384;
pub const MAX_CONNECTOR_EXPRESSION_DEPTH: usize = 64;
pub const MAX_CONNECTOR_VALUE_BYTES: usize = 64 * 1024;

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

/// One end of a [`Range`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Bound {
    /// The range extends without limit on this side.
    Unbounded,
    /// The bounding value is part of the range.
    Inclusive(ConnectorValue),
    /// The bounding value is excluded from the range.
    Exclusive(ConnectorValue),
}

impl Bound {
    pub const fn value(&self) -> Option<&ConnectorValue> {
        match self {
            Self::Unbounded => None,
            Self::Inclusive(value) | Self::Exclusive(value) => Some(value),
        }
    }

    pub const fn is_unbounded(&self) -> bool {
        matches!(self, Self::Unbounded)
    }

    const fn is_inclusive(&self) -> bool {
        matches!(self, Self::Inclusive(_))
    }
}

/// A contiguous, exactly typed value interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range {
    value_type: ConnectorValueType,
    low: Bound,
    high: Bound,
}

impl Range {
    /// The whole domain of a type.
    pub const fn all(value_type: ConnectorValueType) -> Self {
        Self {
            value_type,
            low: Bound::Unbounded,
            high: Bound::Unbounded,
        }
    }

    /// A single-value range.
    pub fn equal(value: ConnectorValue) -> Self {
        let value_type = value.value_type();
        Self {
            value_type,
            low: Bound::Inclusive(value.clone()),
            high: Bound::Inclusive(value),
        }
    }

    pub fn try_new(
        value_type: ConnectorValueType,
        low: Bound,
        high: Bound,
    ) -> Result<Self, ConnectorError> {
        let is_single_equal_value = low.is_inclusive() && high.is_inclusive() && low == high;
        let is_whole_domain = low.is_unbounded() && high.is_unbounded();
        if !value_type.is_orderable() && !is_whole_domain && !is_single_equal_value {
            return Err(invalid(
                "connector range requires an orderable type or a single equal value",
            ));
        }
        for bound in [&low, &high] {
            if let Some(value) = bound.value() {
                if value.value_type() != value_type {
                    return Err(invalid(
                        "connector range bound type differs from its column type",
                    ));
                }
                if value.payload_bytes() > MAX_CONNECTOR_VALUE_BYTES {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "connector range bound exceeds the value size limit",
                    ));
                }
            }
        }
        let range = Self {
            value_type,
            low,
            high,
        };
        if range.is_provably_empty()? {
            return Err(invalid("connector range low bound is above its high bound"));
        }
        Ok(range)
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.value_type
    }

    pub const fn low(&self) -> &Bound {
        &self.low
    }

    pub const fn high(&self) -> &Bound {
        &self.high
    }

    pub const fn is_all(&self) -> bool {
        self.low.is_unbounded() && self.high.is_unbounded()
    }

    /// The single value of this range when it holds exactly one.
    pub fn single_value(&self) -> Option<&ConnectorValue> {
        match (&self.low, &self.high) {
            (Bound::Inclusive(low), Bound::Inclusive(high)) if low == high => Some(low),
            _ => None,
        }
    }

    fn is_provably_empty(&self) -> Result<bool, ConnectorError> {
        let (Some(low), Some(high)) = (self.low.value(), self.high.value()) else {
            return Ok(false);
        };
        let ordering = low
            .try_compare_same_type(high)
            .ok_or_else(|| invalid("connector range bounds are not comparable"))?;
        Ok(match ordering {
            Ordering::Greater => true,
            Ordering::Equal => !(self.low.is_inclusive() && self.high.is_inclusive()),
            Ordering::Less => false,
        })
    }

    pub fn contains_value(&self, value: &ConnectorValue) -> Result<bool, ConnectorError> {
        if value.value_type() != self.value_type {
            return Ok(false);
        }
        let above_low = match &self.low {
            Bound::Unbounded => true,
            Bound::Inclusive(low) => matches!(
                value
                    .try_compare_same_type(low)
                    .ok_or_else(|| invalid("connector range bound is not comparable"))?,
                Ordering::Greater | Ordering::Equal
            ),
            Bound::Exclusive(low) => matches!(
                value
                    .try_compare_same_type(low)
                    .ok_or_else(|| invalid("connector range bound is not comparable"))?,
                Ordering::Greater
            ),
        };
        if !above_low {
            return Ok(false);
        }
        Ok(match &self.high {
            Bound::Unbounded => true,
            Bound::Inclusive(high) => matches!(
                value
                    .try_compare_same_type(high)
                    .ok_or_else(|| invalid("connector range bound is not comparable"))?,
                Ordering::Less | Ordering::Equal
            ),
            Bound::Exclusive(high) => matches!(
                value
                    .try_compare_same_type(high)
                    .ok_or_else(|| invalid("connector range bound is not comparable"))?,
                Ordering::Less
            ),
        })
    }
}

/// Order two low bounds; an unbounded low sorts first.
fn compare_low(left: &Bound, right: &Bound) -> Result<Ordering, ConnectorError> {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ok(Ordering::Equal),
        (Bound::Unbounded, _) => Ok(Ordering::Less),
        (_, Bound::Unbounded) => Ok(Ordering::Greater),
        (left_bound, right_bound) => {
            let left_value = left_bound.value().expect("bounded");
            let right_value = right_bound.value().expect("bounded");
            let ordering = left_value
                .try_compare_same_type(right_value)
                .ok_or_else(|| invalid("connector range bounds are not comparable"))?;
            Ok(match ordering {
                Ordering::Equal => match (left_bound.is_inclusive(), right_bound.is_inclusive()) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => Ordering::Equal,
                },
                other => other,
            })
        }
    }
}

/// Order two high bounds; an unbounded high sorts last.
fn compare_high(left: &Bound, right: &Bound) -> Result<Ordering, ConnectorError> {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ok(Ordering::Equal),
        (Bound::Unbounded, _) => Ok(Ordering::Greater),
        (_, Bound::Unbounded) => Ok(Ordering::Less),
        (left_bound, right_bound) => {
            let left_value = left_bound.value().expect("bounded");
            let right_value = right_bound.value().expect("bounded");
            let ordering = left_value
                .try_compare_same_type(right_value)
                .ok_or_else(|| invalid("connector range bounds are not comparable"))?;
            Ok(match ordering {
                Ordering::Equal => match (left_bound.is_inclusive(), right_bound.is_inclusive()) {
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    _ => Ordering::Equal,
                },
                other => other,
            })
        }
    }
}

/// Whether `left`'s high bound reaches `right`'s low bound, so the two ranges
/// can be merged. Adjacency of discrete values is deliberately not inferred.
fn bounds_meet(high: &Bound, low: &Bound) -> Result<bool, ConnectorError> {
    let (Some(high_value), Some(low_value)) = (high.value(), low.value()) else {
        return Ok(true);
    };
    let ordering = high_value
        .try_compare_same_type(low_value)
        .ok_or_else(|| invalid("connector range bounds are not comparable"))?;
    Ok(match ordering {
        Ordering::Less => false,
        Ordering::Greater => true,
        Ordering::Equal => high.is_inclusive() || low.is_inclusive(),
    })
}

/// A normalized, exactly typed set of values: sorted, non-overlapping ranges.
///
/// An empty range list is the empty set; a single unbounded range is the whole
/// set. Discrete values are unit ranges, so no second representation exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValueSet {
    value_type: ConnectorValueType,
    ranges: Vec<Range>,
}

impl ValueSet {
    pub const fn none(value_type: ConnectorValueType) -> Self {
        Self {
            value_type,
            ranges: Vec::new(),
        }
    }

    pub fn all(value_type: ConnectorValueType) -> Self {
        Self {
            value_type,
            ranges: vec![Range::all(value_type)],
        }
    }

    pub fn of_ranges(
        value_type: ConnectorValueType,
        ranges: Vec<Range>,
    ) -> Result<Self, ConnectorError> {
        if ranges.len() > MAX_VALUE_SET_RANGES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector value set range count exceeds the hard limit",
            ));
        }
        for range in &ranges {
            if range.value_type != value_type {
                return Err(invalid(
                    "connector value set range type differs from its column type",
                ));
            }
        }
        Ok(Self {
            value_type,
            ranges: normalize(ranges)?,
        })
    }

    pub fn of_values(
        value_type: ConnectorValueType,
        values: Vec<ConnectorValue>,
    ) -> Result<Self, ConnectorError> {
        if values.len() > MAX_VALUE_SET_DISCRETE_VALUES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector value set discrete value count exceeds the hard limit",
            ));
        }
        let ranges = values.into_iter().map(Range::equal).collect();
        Self::of_ranges(value_type, ranges)
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.value_type
    }

    pub fn ranges(&self) -> &[Range] {
        &self.ranges
    }

    pub fn is_none(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn is_all(&self) -> bool {
        self.ranges.len() == 1 && self.ranges[0].is_all()
    }

    /// The exact discrete values when every range holds exactly one value.
    pub fn discrete_values(&self) -> Option<Vec<&ConnectorValue>> {
        self.ranges.iter().map(Range::single_value).collect()
    }

    pub fn contains_value(&self, value: &ConnectorValue) -> Result<bool, ConnectorError> {
        for range in &self.ranges {
            if range.contains_value(value)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn intersect(&self, other: &Self) -> Result<Self, ConnectorError> {
        if self.value_type != other.value_type {
            return Err(invalid("connector value sets have different exact types"));
        }
        let mut ranges = Vec::new();
        for left in &self.ranges {
            for right in &other.ranges {
                let low = if compare_low(&left.low, &right.low)? == Ordering::Less {
                    right.low.clone()
                } else {
                    left.low.clone()
                };
                let high = if compare_high(&left.high, &right.high)? == Ordering::Greater {
                    right.high.clone()
                } else {
                    left.high.clone()
                };
                let candidate = Range {
                    value_type: self.value_type,
                    low,
                    high,
                };
                if !candidate.is_provably_empty()? {
                    ranges.push(candidate);
                }
            }
        }
        Ok(Self {
            value_type: self.value_type,
            ranges: normalize(ranges)?,
        })
    }

    pub fn union(&self, other: &Self) -> Result<Self, ConnectorError> {
        if self.value_type != other.value_type {
            return Err(invalid("connector value sets have different exact types"));
        }
        let mut ranges = self.ranges.clone();
        ranges.extend(other.ranges.iter().cloned());
        Ok(Self {
            value_type: self.value_type,
            ranges: normalize(ranges)?,
        })
    }
}

fn normalize(mut ranges: Vec<Range>) -> Result<Vec<Range>, ConnectorError> {
    if ranges.is_empty() {
        return Ok(ranges);
    }
    let mut comparison_error = None;
    ranges.sort_by(|left, right| match compare_low(&left.low, &right.low) {
        Ok(Ordering::Equal) => compare_high(&left.high, &right.high).unwrap_or_else(|error| {
            comparison_error.get_or_insert(error);
            Ordering::Equal
        }),
        Ok(other) => other,
        Err(error) => {
            comparison_error.get_or_insert(error);
            Ordering::Equal
        }
    });
    if let Some(error) = comparison_error {
        return Err(error);
    }

    let mut merged: Vec<Range> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(previous) if bounds_meet(&previous.high, &range.low)? => {
                if compare_high(&range.high, &previous.high)? == Ordering::Greater {
                    previous.high = range.high;
                }
            }
            _ => merged.push(range),
        }
    }
    if merged.len() > MAX_VALUE_SET_RANGES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector value set range count exceeds the hard limit",
        ));
    }
    Ok(merged)
}

/// The set of values one column may take, plus whether `NULL` is allowed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Domain {
    values: ValueSet,
    null_allowed: bool,
}

impl Domain {
    pub fn new(values: ValueSet, null_allowed: bool) -> Self {
        Self {
            values,
            null_allowed,
        }
    }

    pub fn all(value_type: ConnectorValueType) -> Self {
        Self::new(ValueSet::all(value_type), true)
    }

    pub const fn none(value_type: ConnectorValueType) -> Self {
        Self {
            values: ValueSet::none(value_type),
            null_allowed: false,
        }
    }

    pub const fn only_null(value_type: ConnectorValueType) -> Self {
        Self {
            values: ValueSet::none(value_type),
            null_allowed: true,
        }
    }

    pub fn not_null(value_type: ConnectorValueType) -> Self {
        Self::new(ValueSet::all(value_type), false)
    }

    pub fn single_value(value: ConnectorValue) -> Result<Self, ConnectorError> {
        let value_type = value.value_type();
        Ok(Self::new(
            ValueSet::of_values(value_type, vec![value])?,
            false,
        ))
    }

    pub const fn values(&self) -> &ValueSet {
        &self.values
    }

    pub const fn null_allowed(&self) -> bool {
        self.null_allowed
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.values.value_type
    }

    pub fn is_none(&self) -> bool {
        self.values.is_none() && !self.null_allowed
    }

    pub fn is_all(&self) -> bool {
        self.values.is_all() && self.null_allowed
    }

    pub fn intersect(&self, other: &Self) -> Result<Self, ConnectorError> {
        Ok(Self::new(
            self.values.intersect(&other.values)?,
            self.null_allowed && other.null_allowed,
        ))
    }

    pub fn union(&self, other: &Self) -> Result<Self, ConnectorError> {
        Ok(Self::new(
            self.values.union(&other.values)?,
            self.null_allowed || other.null_allowed,
        ))
    }

    /// Whether this domain can be satisfied by any value or null.
    pub fn overlaps(&self, other: &Self) -> Result<bool, ConnectorError> {
        Ok(!self.intersect(other)?.is_none())
    }
}

/// A per-column conjunction of [`Domain`]s.
///
/// `None` means the predicate is unsatisfiable. A column absent from the map
/// is unconstrained, so an empty map is the unconstrained predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TupleDomain<C: Ord + Clone + Debug> {
    domains: Option<BTreeMap<C, Domain>>,
}

impl<C: Ord + Clone + Debug> TupleDomain<C> {
    pub const fn all() -> Self {
        Self {
            domains: Some(BTreeMap::new()),
        }
    }

    pub const fn none() -> Self {
        Self { domains: None }
    }

    pub fn with_column_domains(domains: BTreeMap<C, Domain>) -> Result<Self, ConnectorError> {
        if domains.len() > MAX_TUPLE_DOMAIN_COLUMNS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector tuple domain column count exceeds the hard limit",
            ));
        }
        if domains.values().any(Domain::is_none) {
            return Ok(Self::none());
        }
        Ok(Self {
            domains: Some(
                domains
                    .into_iter()
                    .filter(|(_, domain)| !domain.is_all())
                    .collect(),
            ),
        })
    }

    pub const fn is_none(&self) -> bool {
        self.domains.is_none()
    }

    pub fn is_all(&self) -> bool {
        self.domains.as_ref().is_some_and(BTreeMap::is_empty)
    }

    pub fn domains(&self) -> Option<&BTreeMap<C, Domain>> {
        self.domains.as_ref()
    }

    pub fn domain_for(&self, column: &C) -> Option<&Domain> {
        self.domains.as_ref()?.get(column)
    }

    pub fn columns(&self) -> impl Iterator<Item = &C> {
        self.domains.iter().flat_map(BTreeMap::keys)
    }

    pub fn intersect(&self, other: &Self) -> Result<Self, ConnectorError> {
        let (Some(left), Some(right)) = (&self.domains, &other.domains) else {
            return Ok(Self::none());
        };
        let mut merged = left.clone();
        for (column, domain) in right {
            match merged.get(column) {
                Some(existing) => {
                    let intersected = existing.intersect(domain)?;
                    if intersected.is_none() {
                        return Ok(Self::none());
                    }
                    merged.insert(column.clone(), intersected);
                }
                None => {
                    merged.insert(column.clone(), domain.clone());
                }
            }
        }
        Self::with_column_domains(merged)
    }

    /// Keep only the columns a caller can evaluate, widening the rest to all.
    pub fn filter_columns(&self, keep: impl Fn(&C) -> bool) -> Self {
        match &self.domains {
            None => Self::none(),
            Some(domains) => Self {
                domains: Some(
                    domains
                        .iter()
                        .filter(|(column, _)| keep(column))
                        .map(|(column, domain)| (column.clone(), domain.clone()))
                        .collect(),
                ),
            },
        }
    }

    pub fn transform_keys<D: Ord + Clone + Debug>(
        &self,
        mut map: impl FnMut(&C) -> Option<D>,
    ) -> TupleDomain<D> {
        match &self.domains {
            None => TupleDomain::none(),
            Some(domains) => TupleDomain {
                domains: Some(
                    domains
                        .iter()
                        .filter_map(|(column, domain)| {
                            map(column).map(|mapped| (mapped, domain.clone()))
                        })
                        .collect(),
                ),
            },
        }
    }
}

/// A named function reference inside a [`ConnectorExpression`].
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectorFunctionName(Arc<str>);

impl ConnectorFunctionName {
    pub fn try_new(name: impl AsRef<str>) -> Result<Self, ConnectorError> {
        let name = name.as_ref();
        if name.is_empty() || name.len() > 256 || !name.is_ascii() {
            return Err(invalid("connector function name must be bounded ASCII"));
        }
        Ok(Self(Arc::from(name)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The residual expression a connector may accept beyond a [`TupleDomain`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorExpression {
    /// `None` is the typed `NULL` literal.
    Constant {
        value: Option<ConnectorValue>,
        value_type: ConnectorValueType,
    },
    Variable {
        name: Arc<str>,
        value_type: ConnectorValueType,
    },
    FieldDereference {
        target: Box<ConnectorExpression>,
        field_index: u32,
        value_type: ConnectorValueType,
    },
    Call {
        function: ConnectorFunctionName,
        value_type: ConnectorValueType,
        arguments: Vec<ConnectorExpression>,
    },
}

impl ConnectorExpression {
    pub const fn value_type(&self) -> ConnectorValueType {
        match self {
            Self::Constant { value_type, .. }
            | Self::Variable { value_type, .. }
            | Self::FieldDereference { value_type, .. }
            | Self::Call { value_type, .. } => *value_type,
        }
    }

    /// Validate node count and depth before an expression crosses the SPI.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let mut nodes = 0_usize;
        self.walk(1, &mut nodes)
    }

    fn walk(&self, depth: usize, nodes: &mut usize) -> Result<(), ConnectorError> {
        if depth > MAX_CONNECTOR_EXPRESSION_DEPTH {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector expression depth exceeds the hard limit",
            ));
        }
        *nodes += 1;
        if *nodes > MAX_CONNECTOR_EXPRESSION_NODES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector expression node count exceeds the hard limit",
            ));
        }
        match self {
            Self::Constant { value, value_type } => {
                if let Some(value) = value
                    && value.value_type() != *value_type
                {
                    return Err(invalid("connector constant type differs from its value"));
                }
                Ok(())
            }
            Self::Variable { name, .. } => {
                if name.is_empty() {
                    return Err(invalid(
                        "connector expression variable name must not be empty",
                    ));
                }
                Ok(())
            }
            Self::FieldDereference { target, .. } => target.walk(depth + 1, nodes),
            Self::Call { arguments, .. } => {
                for argument in arguments {
                    argument.walk(depth + 1, nodes)?;
                }
                Ok(())
            }
        }
    }

    /// Every free variable name referenced by this expression.
    pub fn variable_names(&self, out: &mut Vec<Arc<str>>) {
        match self {
            Self::Constant { .. } => {}
            Self::Variable { name, .. } => out.push(name.clone()),
            Self::FieldDereference { target, .. } => target.variable_names(out),
            Self::Call { arguments, .. } => {
                for argument in arguments {
                    argument.variable_names(out);
                }
            }
        }
    }

    /// The always-true expression.
    pub fn constant_true() -> Self {
        Self::Constant {
            value: Some(ConnectorValue::Boolean(true)),
            value_type: ConnectorValueType::Boolean,
        }
    }

    pub fn is_constant_true(&self) -> bool {
        matches!(
            self,
            Self::Constant {
                value: Some(ConnectorValue::Boolean(true)),
                ..
            }
        )
    }
}

/// The full filter offered to a connector: a summary domain plus a residual
/// expression whose variables are all bound to column handles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Constraint<C: Ord + Clone + Debug> {
    summary: TupleDomain<C>,
    expression: ConnectorExpression,
    assignments: BTreeMap<Arc<str>, C>,
}

impl<C: Ord + Clone + Debug> Constraint<C> {
    pub fn try_new(
        summary: TupleDomain<C>,
        expression: ConnectorExpression,
        assignments: BTreeMap<Arc<str>, C>,
    ) -> Result<Self, ConnectorError> {
        expression.validate()?;
        let mut variables = Vec::new();
        expression.variable_names(&mut variables);
        for variable in &variables {
            if !assignments.contains_key(variable) {
                return Err(invalid(
                    "connector constraint expression variable has no column assignment",
                ));
            }
        }
        Ok(Self {
            summary,
            expression,
            assignments,
        })
    }

    /// A constraint that carries only a summary domain.
    pub fn of_summary(summary: TupleDomain<C>) -> Self {
        Self {
            summary,
            expression: ConnectorExpression::constant_true(),
            assignments: BTreeMap::new(),
        }
    }

    pub const fn summary(&self) -> &TupleDomain<C> {
        &self.summary
    }

    pub const fn expression(&self) -> &ConnectorExpression {
        &self.expression
    }

    pub const fn assignments(&self) -> &BTreeMap<Arc<str>, C> {
        &self.assignments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn big_int(value: i64) -> ConnectorValue {
        ConnectorValue::BigInt(value)
    }

    fn range(low: i64, low_inclusive: bool, high: i64, high_inclusive: bool) -> Range {
        Range::try_new(
            ConnectorValueType::BigInt,
            if low_inclusive {
                Bound::Inclusive(big_int(low))
            } else {
                Bound::Exclusive(big_int(low))
            },
            if high_inclusive {
                Bound::Inclusive(big_int(high))
            } else {
                Bound::Exclusive(big_int(high))
            },
        )
        .expect("valid range")
    }

    #[test]
    fn all_and_none_are_distinct_and_absorbing() {
        let all = ValueSet::all(ConnectorValueType::BigInt);
        let none = ValueSet::none(ConnectorValueType::BigInt);
        assert!(all.is_all() && !all.is_none());
        assert!(none.is_none() && !none.is_all());
        assert!(all.intersect(&none).expect("typed").is_none());
        assert!(all.union(&none).expect("typed").is_all());
    }

    #[test]
    fn overlapping_and_touching_ranges_merge_but_gaps_do_not() {
        let touching = ValueSet::of_ranges(
            ConnectorValueType::BigInt,
            vec![range(1, true, 2, true), range(2, false, 3, true)],
        )
        .expect("typed");
        assert_eq!(touching.ranges().len(), 1);

        let gapped = ValueSet::of_ranges(
            ConnectorValueType::BigInt,
            vec![range(1, true, 2, false), range(2, false, 3, true)],
        )
        .expect("typed");
        assert_eq!(gapped.ranges().len(), 2);
    }

    #[test]
    fn discrete_values_round_trip_as_unit_ranges() {
        let values = ValueSet::of_values(
            ConnectorValueType::BigInt,
            vec![big_int(7), big_int(3), big_int(7)],
        )
        .expect("typed");
        let discrete = values.discrete_values().expect("all unit ranges");
        assert_eq!(discrete.len(), 2);
        assert!(values.contains_value(&big_int(3)).expect("typed"));
        assert!(!values.contains_value(&big_int(4)).expect("typed"));
    }

    #[test]
    fn intersect_of_ranges_keeps_only_the_overlap() {
        let left = ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range(1, true, 10, true)])
            .expect("typed");
        let right =
            ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range(5, false, 20, true)])
                .expect("typed");
        let intersected = left.intersect(&right).expect("typed");
        assert_eq!(intersected.ranges().len(), 1);
        assert!(!intersected.contains_value(&big_int(5)).expect("typed"));
        assert!(intersected.contains_value(&big_int(6)).expect("typed"));
        assert!(intersected.contains_value(&big_int(10)).expect("typed"));
        assert!(!intersected.contains_value(&big_int(11)).expect("typed"));
    }

    #[test]
    fn nullability_is_tracked_separately_from_values() {
        let only_null = Domain::only_null(ConnectorValueType::BigInt);
        assert!(!only_null.is_none());
        assert!(only_null.null_allowed());
        let not_null = Domain::not_null(ConnectorValueType::BigInt);
        assert!(!not_null.null_allowed());
        assert!(only_null.intersect(&not_null).expect("typed").is_none());
    }

    #[test]
    fn value_sets_of_different_types_do_not_combine() {
        let left = ValueSet::all(ConnectorValueType::BigInt);
        let right = ValueSet::all(ConnectorValueType::Integer);
        assert!(left.intersect(&right).is_err());
        assert!(left.union(&right).is_err());
    }

    #[test]
    fn empty_ranges_are_rejected_at_construction() {
        assert!(
            Range::try_new(
                ConnectorValueType::BigInt,
                Bound::Inclusive(big_int(5)),
                Bound::Inclusive(big_int(4)),
            )
            .is_err()
        );
        assert!(
            Range::try_new(
                ConnectorValueType::BigInt,
                Bound::Exclusive(big_int(5)),
                Bound::Exclusive(big_int(5)),
            )
            .is_err()
        );
    }

    #[test]
    fn tuple_domain_none_absorbs_and_all_is_empty() {
        let mut domains = BTreeMap::new();
        domains.insert(1_u32, Domain::none(ConnectorValueType::BigInt));
        let tuple = TupleDomain::with_column_domains(domains).expect("bounded");
        assert!(tuple.is_none());
        assert!(TupleDomain::<u32>::all().is_all());
        assert!(
            TupleDomain::<u32>::all()
                .intersect(&TupleDomain::none())
                .expect("typed")
                .is_none()
        );
    }

    #[test]
    fn tuple_domain_intersect_conjoins_per_column() {
        let mut left_domains = BTreeMap::new();
        left_domains.insert(
            1_u32,
            Domain::new(
                ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range(1, true, 10, true)])
                    .expect("typed"),
                false,
            ),
        );
        let mut right_domains = BTreeMap::new();
        right_domains.insert(
            1_u32,
            Domain::new(
                ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range(20, true, 30, true)])
                    .expect("typed"),
                false,
            ),
        );
        let left = TupleDomain::with_column_domains(left_domains).expect("bounded");
        let right = TupleDomain::with_column_domains(right_domains).expect("bounded");
        assert!(left.intersect(&right).expect("typed").is_none());
    }

    #[test]
    fn constraints_require_every_expression_variable_to_be_assigned() {
        let expression = ConnectorExpression::Call {
            function: ConnectorFunctionName::try_new("$like").expect("name"),
            value_type: ConnectorValueType::Boolean,
            arguments: vec![ConnectorExpression::Variable {
                name: Arc::from("v0"),
                value_type: ConnectorValueType::Varchar,
            }],
        };
        assert!(
            Constraint::<u32>::try_new(TupleDomain::all(), expression.clone(), BTreeMap::new())
                .is_err()
        );
        let mut assignments = BTreeMap::new();
        assignments.insert(Arc::from("v0"), 7_u32);
        assert!(Constraint::try_new(TupleDomain::all(), expression, assignments).is_ok());
    }

    #[test]
    fn expression_depth_is_bounded() {
        let mut expression = ConnectorExpression::Constant {
            value: Some(ConnectorValue::BigInt(1)),
            value_type: ConnectorValueType::BigInt,
        };
        for _ in 0..MAX_CONNECTOR_EXPRESSION_DEPTH {
            expression = ConnectorExpression::Call {
                function: ConnectorFunctionName::try_new("$negate").expect("name"),
                value_type: ConnectorValueType::BigInt,
                arguments: vec![expression],
            };
        }
        assert!(expression.validate().is_err());
    }
}
