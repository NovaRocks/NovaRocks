// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Typed, layout-independent relations for the reachable values of a batch.
use std::ops::Range;

use allocator_api2::alloc::{Allocator, Global};
use allocator_api2::vec::Vec as AllocatedVec;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeListArray, ListArray, MapArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{
    CarrierNullabilityPolicy, FunctionValueType, KeyComparison, KeyLeaf, KeyRelation,
    UnsupportedKeyType, ValueKind, ValueTypeRef, canonical_f32_bits, canonical_f64_bits,
    key_character_bytes_observed, key_domains_match_observed, key_leaf,
    value_layout_matches_carrier_with_nullability_observed,
};

use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    arrow_raw::byte_span,
    control::{Growth, Work},
    decode::PreparedTemporalArray,
    reachability::SelectionSet,
};

mod retained;
pub use retained::{RetainedKeyChild, RetainedKeyContents, RetainedKeyValue};

#[derive(Debug)]
pub enum KeyPreparationFailure<E> {
    Unsupported(UnsupportedKeyType),
    Caller(E),
}

/// Caller-selected coordinates. Nothing outside coverage is read.
#[derive(Clone)]
pub enum KeyRows<'a> {
    All,
    Range(Range<usize>),
    Selected(&'a SelectionSet),
}

#[derive(Clone, Debug)]
pub struct PreparedKeyOperations {
    declaration: FunctionValueType,
    relation: KeyRelation,
}

pub struct BoundKeyArray<'a> {
    operations: &'a PreparedKeyOperations,
    array: &'a ArrayRef,
    rows: KeyRows<'a>,
}

/// The search result keeps absence separate from evaluated uncertainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyLookup {
    Found(usize),
    Missing,
    Indeterminate,
}

impl PreparedKeyOperations {
    pub fn prepare_observed<E>(
        declaration: &FunctionValueType,
        relation: KeyRelation,
        work: &mut Work<'_, E>,
    ) -> Result<Self, KeyPreparationFailure<E>> {
        relation
            .validate_type_observed(declaration.value_type(), &mut *work)
            .map_err(KeyPreparationFailure::Caller)?
            .map_err(KeyPreparationFailure::Unsupported)?;
        Ok(Self {
            declaration: declaration.clone(),
            relation,
        })
    }

    pub fn bind_observed<'a, E>(
        &'a self,
        array: &'a ArrayRef,
        rows: KeyRows<'a>,
        work: &mut Work<'_, E>,
    ) -> Result<BoundKeyArray<'a>, ValueFailure<E>> {
        let layout = self.declaration.typed_binding().layout();
        let same = value_layout_matches_carrier_with_nullability_observed(
            layout,
            array.data_type(),
            CarrierNullabilityPolicy::Permissive,
            |count| work(count).map_err(BindFailure::Caller),
        )
        .map_err(bind_failure)?;
        if !same {
            return Err(invalid(ValueViolation::Layout).into());
        }
        match &rows {
            KeyRows::All => {}
            KeyRows::Range(rows) if rows.start <= rows.end && rows.end <= array.len() => {}
            KeyRows::Selected(rows) if rows.len() == array.len() => {}
            _ => return Err(invalid(ValueViolation::Bounds).into()),
        }
        Ok(BoundKeyArray {
            operations: self,
            array,
            rows,
        })
    }

    pub fn relation(&self) -> KeyRelation {
        self.relation
    }

    pub fn compare_observed<E>(
        &self,
        left: &BoundKeyArray<'_>,
        left_row: usize,
        right: &BoundKeyArray<'_>,
        right_row: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        self.check_pair(left, right, work)?;
        let left = left.position(left_row)?;
        let right = right.position(right_row)?;
        ComparisonWork {
            relation: self.relation,
            growth,
            work,
            allocator: &Global,
        }
        .compare(left, right)
    }

    /// Compare two selected Arrow values with the caller's actual allocator.
    /// Recursive MAP scratch is released at the end of each original index.
    pub fn compare_in_observed<A: Allocator + Clone, E>(
        &self,
        left: &BoundKeyArray<'_>,
        right: &BoundKeyArray<'_>,
        rows: (usize, usize),
        allocator: &A,
        work: &mut Work<'_, E>,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        self.check_pair(left, right, work)?;
        ComparisonWork {
            relation: self.relation,
            growth: &mut |_| Ok(()),
            work,
            allocator,
        }
        .compare(left.position(rows.0)?, right.position(rows.1)?)
    }

    /// Results stay in the supplied pair order, including repeated coordinates.
    pub fn compare_pairs_observed<E>(
        &self,
        left: &BoundKeyArray<'_>,
        right: &BoundKeyArray<'_>,
        pairs: &[(usize, usize)],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<Vec<KeyComparison>, ValueFailure<E>> {
        self.check_pair(left, right, work)?;
        let mut output =
            super::initialized_result(pairs.len(), KeyComparison::Unknown, growth, work)?;
        let mut comparison = ComparisonWork {
            relation: self.relation,
            growth,
            work,
            allocator: &Global,
        };
        for (&(left_row, right_row), output) in pairs.iter().zip(&mut output) {
            *output = comparison.compare(left.position(left_row)?, right.position(right_row)?)?;
        }
        Ok(output)
    }

    /// Scan the supplied candidates in original order, with a sure match
    /// taking precedence over any earlier uncertain comparison.
    pub fn lookup_observed<E>(
        &self,
        candidates: &BoundKeyArray<'_>,
        probe: &BoundKeyArray<'_>,
        probe_row: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<KeyLookup, ValueFailure<E>> {
        self.check_pair(candidates, probe, work)?;
        let probe = probe.position(probe_row)?;
        let mut operation = ComparisonWork {
            relation: self.relation,
            growth,
            work,
            allocator: &Global,
        };
        let mut uncertain = false;
        let mut scan = |operation: &mut ComparisonWork<'_, '_, '_, E, Global>,
                        row|
         -> Result<Option<KeyLookup>, ValueFailure<E>> {
            let candidate = candidates.position(row)?;
            match operation.compare(candidate, probe)? {
                KeyComparison::True => Ok(Some(KeyLookup::Found(row))),
                KeyComparison::False => Ok(None),
                KeyComparison::Unknown | KeyComparison::Indeterminate => {
                    uncertain = true;
                    Ok(None)
                }
            }
        };
        match &candidates.rows {
            KeyRows::All => {
                for row in 0..candidates.array.len() {
                    if let Some(found) = scan(&mut operation, row)? {
                        return Ok(found);
                    }
                }
            }
            KeyRows::Range(rows) => {
                for row in rows.clone() {
                    if let Some(found) = scan(&mut operation, row)? {
                        return Ok(found);
                    }
                }
            }
            KeyRows::Selected(selection) => {
                let mut cursor = selection.cursor();
                while let Some(row) = cursor.next_observed(&mut |n| (operation.work)(n))? {
                    if let Some(found) = scan(&mut operation, row)? {
                        return Ok(found);
                    }
                }
            }
        }
        Ok(if uncertain {
            KeyLookup::Indeterminate
        } else {
            KeyLookup::Missing
        })
    }

    /// Hash is a candidate filter. A matching hash is never evidence of equality.
    /// Both relations may share the identity hash since EQUAL TRUE implies
    /// IDENTICAL TRUE. This hash has no wire, bucket, or persisted identity.
    pub fn hash_observed<E>(
        &self,
        values: &BoundKeyArray<'_>,
        row: usize,
        work: &mut Work<'_, E>,
    ) -> Result<u64, ValueFailure<E>> {
        self.check_input(values, work)?;
        hash(values.position(row)?, work)
    }

    /// Proves absence of reachable NULL. A false result does not imply that a
    /// particular comparison will be UNKNOWN.
    pub fn determinate_observed<E>(
        &self,
        values: &BoundKeyArray<'_>,
        row: usize,
        work: &mut Work<'_, E>,
    ) -> Result<bool, ValueFailure<E>> {
        self.check_input(values, work)?;
        determinate(values.position(row)?, work)
    }

    pub fn hash_retained_observed<E>(
        &self,
        value: &dyn RetainedKeyValue,
        work: &mut Work<'_, E>,
    ) -> Result<u64, ValueFailure<E>> {
        hash(
            Position::retained(self.declaration.value_type(), Some(value)),
            work,
        )
    }

    /// Compare actual retained values without repackaging them as Arrow.
    /// Transient candidate storage uses the supplied state allocator.
    pub fn compare_retained_in_observed<A: Allocator + Clone, E>(
        &self,
        left: &dyn RetainedKeyValue,
        right: &dyn RetainedKeyValue,
        allocator: &A,
        work: &mut Work<'_, E>,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        let declaration = self.declaration.value_type();
        ComparisonWork {
            relation: self.relation,
            growth: &mut |_| Ok(()),
            work,
            allocator,
        }
        .compare(
            Position::retained(declaration, Some(left)),
            Position::retained(declaration, Some(right)),
        )
    }

    fn check_input<E>(
        &self,
        values: &BoundKeyArray<'_>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        if self.relation != values.operations.relation
            || (!std::ptr::eq(self, values.operations)
                && !key_domains_match_observed(
                    self.declaration.value_type(),
                    values.operations.declaration.value_type(),
                    |count| work(count).map_err(BindFailure::Caller),
                )
                .map_err(bind_failure)?)
        {
            return Err(invalid(ValueViolation::Layout).into());
        }
        Ok(())
    }

    fn check_pair<E>(
        &self,
        left: &BoundKeyArray<'_>,
        right: &BoundKeyArray<'_>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        self.check_input(left, work)?;
        self.check_input(right, work)
    }
}

enum BindFailure<E> {
    Caller(E),
    Invalid,
}

impl<E> From<novarocks_type_contract::ValueTypeError> for BindFailure<E> {
    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Invalid
    }
}

fn bind_failure<E>(failure: BindFailure<E>) -> ValueFailure<E> {
    match failure {
        BindFailure::Caller(error) => ValueFailure::Caller(error),
        BindFailure::Invalid => invalid(ValueViolation::Layout).into(),
    }
}

impl BoundKeyArray<'_> {
    fn position(&self, row: usize) -> Result<Position<'_>, ValueError> {
        let readable = match &self.rows {
            KeyRows::All => row < self.array.len(),
            KeyRows::Range(rows) => rows.contains(&row),
            KeyRows::Selected(rows) => rows.contains(row),
        };
        if !readable {
            return Err(invalid(ValueViolation::UndefinedRow));
        }
        Ok(Position::arrow(
            self.operations.declaration.value_type(),
            self.array,
            row,
        ))
    }
}

const fn invalid(violation: ValueViolation) -> ValueError {
    ValueError::invariant(violation)
}

#[derive(Clone, Copy)]
struct Position<'a> {
    ty: &'a ValueTypeRef,
    source: KeySource<'a>,
}

#[derive(Clone, Copy)]
enum KeySource<'a> {
    Arrow(&'a ArrayRef, usize),
    Retained(Option<&'a dyn RetainedKeyValue>),
}

impl<'a> Position<'a> {
    fn arrow(ty: &'a ValueTypeRef, array: &'a ArrayRef, row: usize) -> Self {
        Self {
            ty,
            source: KeySource::Arrow(array, row),
        }
    }

    fn retained(ty: &'a ValueTypeRef, value: Option<&'a dyn RetainedKeyValue>) -> Self {
        Self {
            ty,
            source: KeySource::Retained(value),
        }
    }

    fn child(self, index: usize, array: &'a ArrayRef, row: usize) -> Self {
        Self::arrow(
            self.ty.child_type_at(index).expect("bound shape"),
            array,
            row,
        )
    }

    fn retained_child(
        self,
        index: usize,
        value: &'a dyn RetainedKeyValue,
        kind: RetainedKeyChild,
        row: usize,
    ) -> Self {
        Self::retained(
            self.ty.child_type_at(index).expect("bound shape"),
            value.child(kind, row),
        )
    }

    fn read<E>(self, work: &mut Work<'_, E>) -> Result<Value<'a>, ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        let ty = self.ty;
        let (array, row) = match self.source {
            KeySource::Arrow(array, row) => (array, row),
            KeySource::Retained(Some(value)) => return retained::read(self, value, work),
            KeySource::Retained(None) => return self.null().map_err(Into::into),
        };
        if row >= array.len() {
            return Err(invalid(ValueViolation::Bounds).into());
        }
        if array.is_null(row) {
            return self.null().map_err(Into::into);
        }
        macro_rules! primitive {
            ($array:ty) => {
                downcast::<$array>(array)?.value(row)
            };
        }
        let leaf = key_leaf(ty).map_err(|_| invalid(ValueViolation::Carrier))?;
        Ok(match leaf {
            Some(KeyLeaf::Null) => return self.null().map_err(Into::into),
            Some(KeyLeaf::Float32) => Value::Float32(primitive!(Float32Array)),
            Some(KeyLeaf::Float64) => Value::Float64(primitive!(Float64Array)),
            Some(KeyLeaf::Boolean) => Value::fixed([u8::from(primitive!(BooleanArray))]),
            Some(KeyLeaf::Signed) => Value::fixed(signed_value(array, row)?.to_le_bytes()),
            Some(KeyLeaf::Decimal128) => Value::fixed(primitive!(Decimal128Array).to_le_bytes()),
            Some(KeyLeaf::Decimal256) => Value::fixed(primitive!(Decimal256Array).to_le_bytes()),
            Some(KeyLeaf::FixedBytes) => {
                Value::Bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row))
            }
            Some(KeyLeaf::Temporal(declaration)) => {
                let input =
                    PreparedTemporalArray::bind(declaration, array.as_ref(), ty.nullable())?;
                let key = input
                    .relation_key(row)?
                    .ok_or(invalid(ValueViolation::Carrier))?;
                Value::fixed(key.order_bytes())
            }
            Some(KeyLeaf::Bytes | KeyLeaf::Character(_)) => {
                let bytes = if let DataType::Dictionary(_, values) = array.data_type() {
                    let layout = ByteLayout::from_data_type(values)
                        .ok_or(invalid(ValueViolation::Carrier))?;
                    ByteSource::parse(array, layout, Some(layout))?.value(row)?
                } else {
                    byte_span(array.as_ref(), row)?.map(|span| span.bytes())
                };
                match bytes {
                    Some(bytes) => Value::Bytes(match leaf {
                        Some(KeyLeaf::Character(character)) => {
                            key_character_bytes_observed(character, bytes, &mut *work)
                                .map_err(ValueFailure::Caller)?
                        }
                        _ => bytes,
                    }),
                    None => return self.null().map_err(Into::into),
                }
            }
            None => match ty.kind() {
                ValueKind::Struct(_) => Value::Sequence(Sequence::Struct {
                    position: self,
                    array: downcast(array)?,
                    row,
                }),
                ValueKind::Map { .. } => {
                    let value: &MapArray = downcast(array)?;
                    Value::Map(MapValue {
                        position: self,
                        source: MapSource::Arrow(value),
                        rows: child_range(value.value_offsets(), row, value.keys().len())?,
                    })
                }
                ValueKind::List(_) | ValueKind::FixedSizeList { .. } => {
                    let (values, range) = list_values(array, row)?;
                    Value::Sequence(Sequence::List {
                        position: self,
                        array: values,
                        rows: range,
                    })
                }
                _ => return Err(invalid(ValueViolation::Carrier).into()),
            },
        })
    }

    fn null(self) -> Result<Value<'a>, ValueError> {
        if self.ty.nullable() {
            Ok(Value::Null)
        } else {
            Err(invalid(ValueViolation::RequiredNull))
        }
    }
}

fn downcast<T: Array + 'static>(array: &ArrayRef) -> Result<&T, ValueError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or(invalid(ValueViolation::Carrier))
}

fn signed_value(array: &ArrayRef, row: usize) -> Result<i64, ValueError> {
    macro_rules! signed {
        ($array:ty) => {
            i64::from(downcast::<$array>(array)?.value(row))
        };
    }
    Ok(match array.data_type() {
        DataType::Int8 => signed!(Int8Array),
        DataType::Int16 => signed!(Int16Array),
        DataType::Int32 => signed!(Int32Array),
        DataType::Int64 => downcast::<Int64Array>(array)?.value(row),
        DataType::Date32 => signed!(Date32Array),
        DataType::Timestamp(TimeUnit::Second, _) => {
            downcast::<TimestampSecondArray>(array)?.value(row)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            downcast::<TimestampMillisecondArray>(array)?.value(row)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            downcast::<TimestampMicrosecondArray>(array)?.value(row)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            downcast::<TimestampNanosecondArray>(array)?.value(row)
        }
        _ => return Err(invalid(ValueViolation::Carrier)),
    })
}

fn child_range<T: Copy + TryInto<usize>>(
    offsets: &[T],
    row: usize,
    len: usize,
) -> Result<Range<usize>, ValueError> {
    let start = offsets.get(row).and_then(|value| (*value).try_into().ok());
    let end = offsets
        .get(row + 1)
        .and_then(|value| (*value).try_into().ok());
    match (start, end) {
        (Some(start), Some(end)) if start <= end && end <= len => Ok(start..end),
        _ => Err(invalid(ValueViolation::Bounds)),
    }
}

fn list_values(array: &ArrayRef, row: usize) -> Result<(&ArrayRef, Range<usize>), ValueError> {
    Ok(match array.data_type() {
        DataType::List(_) => {
            let value: &ListArray = downcast(array)?;
            let values = value.values();
            (
                values,
                child_range(value.value_offsets(), row, values.len())?,
            )
        }
        DataType::LargeList(_) => {
            let value: &LargeListArray = downcast(array)?;
            let values = value.values();
            (
                values,
                child_range(value.value_offsets(), row, values.len())?,
            )
        }
        DataType::FixedSizeList(_, length) => {
            let value: &FixedSizeListArray = downcast(array)?;
            let values = value.values();
            let length = usize::try_from(*length).map_err(|_| invalid(ValueViolation::Bounds))?;
            let end = row
                .checked_add(1)
                .and_then(|row| row.checked_mul(length))
                .filter(|&end| end <= values.len())
                .ok_or(invalid(ValueViolation::Bounds))?;
            (values, end - length..end)
        }
        _ => return Err(invalid(ValueViolation::Carrier)),
    })
}

enum Value<'a> {
    Null,
    Float32(f32),
    Float64(f64),
    Inline([u8; 32], usize),
    Bytes(&'a [u8]),
    Sequence(Sequence<'a>),
    Map(MapValue<'a>),
}

impl Value<'_> {
    fn fixed<const N: usize>(bytes: [u8; N]) -> Self {
        let mut value = [0; 32];
        value[..N].copy_from_slice(&bytes);
        Self::Inline(value, N)
    }
}

enum Sequence<'a> {
    List {
        position: Position<'a>,
        array: &'a ArrayRef,
        rows: Range<usize>,
    },
    Struct {
        position: Position<'a>,
        array: &'a StructArray,
        row: usize,
    },
    Retained {
        position: Position<'a>,
        value: &'a dyn RetainedKeyValue,
        len: usize,
        kind: RetainedKeyChild,
    },
}

impl<'a> Sequence<'a> {
    fn len(&self) -> usize {
        match self {
            Self::List { rows, .. } => rows.len(),
            Self::Struct { array, .. } => array.num_columns(),
            Self::Retained { len, .. } => *len,
        }
    }

    fn at(&self, coordinate: usize) -> Position<'a> {
        match self {
            Self::List {
                position,
                array,
                rows,
            } => position.child(0, array, rows.start + coordinate),
            Self::Struct {
                position,
                array,
                row,
            } => position.child(coordinate, array.column(coordinate), *row),
            Self::Retained {
                position,
                value,
                kind,
                ..
            } => position.retained_child(
                if matches!(kind, RetainedKeyChild::Field) {
                    coordinate
                } else {
                    0
                },
                *value,
                *kind,
                coordinate,
            ),
        }
    }
}

const BYTE_BLOCK: usize = 64 * 1024;

fn equal_bytes<E>(
    left: &[u8],
    right: &[u8],
    work: &mut Work<'_, E>,
) -> Result<bool, ValueFailure<E>> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.chunks(BYTE_BLOCK).zip(right.chunks(BYTE_BLOCK)) {
        work(left.len()).map_err(ValueFailure::Caller)?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

fn determinate<E>(position: Position<'_>, work: &mut Work<'_, E>) -> Result<bool, ValueFailure<E>> {
    match position.read(work)? {
        Value::Null => return Ok(false),
        Value::Sequence(sequence) => {
            for index in 0..sequence.len() {
                if !determinate(sequence.at(index), work)? {
                    return Ok(false);
                }
            }
        }
        Value::Map(map) => {
            for row in map.rows.clone() {
                if !determinate(map.key(row), work)? || !determinate(map.value(row), work)? {
                    return Ok(false);
                }
            }
        }
        Value::Float32(_) | Value::Float64(_) | Value::Inline(..) | Value::Bytes(_) => {}
    }
    Ok(true)
}

const HASH_SEED: u64 = 0xcbf2_9ce4_8422_2325;

fn mix(state: u64, value: u64) -> u64 {
    (state ^ value.rotate_left(17)).wrapping_mul(0x9e37_79b1_85eb_ca87)
}

fn hash_bytes<E>(bytes: &[u8], work: &mut Work<'_, E>) -> Result<u64, ValueFailure<E>> {
    let mut seed = HASH_SEED;
    for block in bytes.chunks(BYTE_BLOCK) {
        work(block.len()).map_err(ValueFailure::Caller)?;
        seed = super::ByteHashAlgorithm::Fnv1a64 { seed }.hash_bytes(block);
    }
    Ok(seed)
}

fn hash<E>(position: Position<'_>, work: &mut Work<'_, E>) -> Result<u64, ValueFailure<E>> {
    Ok(match position.read(work)? {
        Value::Null => 0x7d8a_f0e6_1988_75fa,
        Value::Float32(value) => hash_bytes(&canonical_f32_bits(value).to_le_bytes(), work)?,
        Value::Float64(value) => hash_bytes(&canonical_f64_bits(value).to_le_bytes(), work)?,
        Value::Inline(bytes, len) => hash_bytes(&bytes[..len], work)?,
        Value::Bytes(bytes) => hash_bytes(bytes, work)?,
        Value::Sequence(sequence) => {
            let mut state = mix(HASH_SEED, sequence.len() as u64);
            for index in 0..sequence.len() {
                state = mix(state, hash(sequence.at(index), work)?);
            }
            state
        }
        Value::Map(map) => {
            let mut state = mix(HASH_SEED, map.rows.len() as u64);
            for row in map.rows.clone() {
                let key = hash(map.key(row), work)?;
                let value = hash(map.value(row), work)?;
                // Order-independent accumulation over directional key-value pairs.
                state = state.wrapping_add(mix(key, value));
            }
            state
        }
    })
}

struct ComparisonWork<'a, 'g, 'w, E, A> {
    relation: KeyRelation,
    growth: &'a mut Growth<'g, E>,
    work: &'a mut Work<'w, E>,
    allocator: &'a A,
}

fn merge(left: KeyComparison, right: KeyComparison) -> KeyComparison {
    use KeyComparison::{False, Indeterminate, True, Unknown};
    match (left, right) {
        (False, _) | (_, False) => False,
        (Indeterminate, _) | (_, Indeterminate) => Indeterminate,
        (Unknown, _) | (_, Unknown) => Unknown,
        (True, True) => True,
    }
}

impl<E, A: Allocator + Clone> ComparisonWork<'_, '_, '_, E, A> {
    fn compare(
        &mut self,
        left: Position<'_>,
        right: Position<'_>,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        use KeyComparison::{False, True, Unknown};
        Ok(match (left.read(self.work)?, right.read(self.work)?) {
            (Value::Null, Value::Null) if self.relation == KeyRelation::Identical => True,
            (Value::Null, _) | (_, Value::Null) => {
                if self.relation == KeyRelation::Equal {
                    Unknown
                } else {
                    False
                }
            }
            (Value::Float32(a), Value::Float32(b)) => match self.relation {
                KeyRelation::Equal => (a == b).into(),
                KeyRelation::Identical => (canonical_f32_bits(a) == canonical_f32_bits(b)).into(),
            },
            (Value::Float64(a), Value::Float64(b)) => match self.relation {
                KeyRelation::Equal => (a == b).into(),
                KeyRelation::Identical => (canonical_f64_bits(a) == canonical_f64_bits(b)).into(),
            },
            (Value::Inline(a, a_len), Value::Inline(b, b_len)) => (a[..a_len] == b[..b_len]).into(),
            (Value::Bytes(a), Value::Bytes(b)) => equal_bytes(a, b, self.work)?.into(),
            (Value::Sequence(a), Value::Sequence(b)) => {
                if a.len() != b.len() {
                    return Ok(False);
                }
                self.compare_sequence((0..a.len()).map(|index| (a.at(index), b.at(index))))?
            }
            (Value::Map(a), Value::Map(b)) => self.compare_maps(a, b)?,
            _ => return Err(invalid(ValueViolation::Layout).into()),
        })
    }

    fn compare_sequence<'a, 'b>(
        &mut self,
        pairs: impl Iterator<Item = (Position<'a>, Position<'b>)>,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        let mut result = KeyComparison::True;
        for (left, right) in pairs {
            result = merge(result, self.compare(left, right)?);
            if result == KeyComparison::False {
                break;
            }
        }
        Ok(result)
    }

    fn compare_maps(
        &mut self,
        left: MapValue<'_>,
        right: MapValue<'_>,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        if left.rows.len() != right.rows.len() {
            return Ok(KeyComparison::False);
        }
        if left.rows.is_empty() {
            return Ok(KeyComparison::True);
        }
        let result = self.match_map_entries(&left, &right, true)?;
        if result == KeyComparison::False || self.relation == KeyRelation::Identical {
            return Ok(result);
        }
        // A left key could have UNKNOWN against all right keys even though
        // another right key is deterministically missing on the left.
        Ok(merge(result, self.match_map_entries(&right, &left, false)?))
    }

    fn match_map_entries(
        &mut self,
        left: &MapValue<'_>,
        right: &MapValue<'_>,
        compare_values: bool,
    ) -> Result<KeyComparison, ValueFailure<E>> {
        let index = MatchIndex::new(right, self.relation, self.growth, self.work, self.allocator)?;
        let mut result = KeyComparison::True;
        for row in left.rows.clone() {
            let part = match self.lookup(left.key(row), right, &index)? {
                KeyLookup::Found(other) if compare_values => {
                    self.compare(left.value(row), right.value(other))?
                }
                KeyLookup::Found(_) => KeyComparison::True,
                KeyLookup::Missing => return Ok(KeyComparison::False),
                KeyLookup::Indeterminate => KeyComparison::Indeterminate,
            };
            result = merge(result, part);
            if result == KeyComparison::False {
                break;
            }
        }
        Ok(result)
    }

    fn lookup(
        &mut self,
        key: Position<'_>,
        map: &MapValue<'_>,
        index: &MatchIndex<A>,
    ) -> Result<KeyLookup, ValueFailure<E>> {
        let hash = hash(key, self.work)?;
        let mut slot = index.slot(hash);
        while let Some(candidate) = index.slots[slot] {
            (self.work)(1).map_err(ValueFailure::Caller)?;
            if candidate.hash == hash {
                match self.compare(key, map.key(candidate.row))? {
                    KeyComparison::True => return Ok(KeyLookup::Found(candidate.row)),
                    KeyComparison::False
                    | KeyComparison::Unknown
                    | KeyComparison::Indeterminate => {}
                }
            }
            slot = index.next(slot);
        }
        if self.relation == KeyRelation::Identical
            || (index.determinate && determinate(key, self.work)?)
        {
            return Ok(KeyLookup::Missing);
        }
        // Hashing may screen true matches, but cannot screen UNKNOWN. Visit
        // actual pairs after all determinate candidates have been evaluated.
        for row in map.rows.clone() {
            match self.compare(key, map.key(row))? {
                KeyComparison::True => return Ok(KeyLookup::Found(row)),
                KeyComparison::False => {}
                KeyComparison::Unknown | KeyComparison::Indeterminate => {
                    return Ok(KeyLookup::Indeterminate);
                }
            }
        }
        Ok(KeyLookup::Missing)
    }
}

enum MapSource<'a> {
    Arrow(&'a MapArray),
    Retained(&'a dyn RetainedKeyValue),
}

struct MapValue<'a> {
    position: Position<'a>,
    source: MapSource<'a>,
    rows: Range<usize>,
}

impl<'a> MapValue<'a> {
    fn key(&self, row: usize) -> Position<'a> {
        match self.source {
            MapSource::Arrow(array) => self.position.child(0, array.keys(), row),
            MapSource::Retained(value) => {
                self.position
                    .retained_child(0, value, RetainedKeyChild::MapKey, row)
            }
        }
    }

    fn value(&self, row: usize) -> Position<'a> {
        match self.source {
            MapSource::Arrow(array) => self.position.child(1, array.values(), row),
            MapSource::Retained(value) => {
                self.position
                    .retained_child(1, value, RetainedKeyChild::MapValue, row)
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Candidate {
    hash: u64,
    row: usize,
}

struct MatchIndex<A: Allocator> {
    slots: AllocatedVec<Option<Candidate>, A>,
    determinate: bool,
}

impl<A: Allocator + Clone> MatchIndex<A> {
    fn new<E>(
        keys: &MapValue<'_>,
        relation: KeyRelation,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        allocator: &A,
    ) -> Result<Self, ValueFailure<E>> {
        let capacity = keys
            .rows
            .len()
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(invalid(ValueViolation::Bounds))?;
        let layout = std::alloc::Layout::array::<Option<Candidate>>(capacity)
            .map_err(|_| invalid(ValueViolation::Bounds))?;
        growth(layout).map_err(ValueFailure::Caller)?;
        let mut slots = AllocatedVec::new_in(allocator.clone());
        slots
            .try_reserve_exact(capacity)
            .map_err(|source| ValueError::Allocator {
                requested: layout.size(),
                source,
            })?;
        let block = (BYTE_BLOCK / std::mem::size_of::<Option<Candidate>>()).max(1);
        while slots.len() < capacity {
            let end = (slots.len() + block).min(capacity);
            work((end - slots.len()) * std::mem::size_of::<Option<Candidate>>())
                .map_err(ValueFailure::Caller)?;
            slots.resize(end, None);
        }
        let mut index = Self {
            slots,
            determinate: true,
        };
        for row in keys.rows.clone() {
            let key = keys.key(row);
            if relation == KeyRelation::Equal && index.determinate {
                index.determinate = determinate(key, work)?;
            }
            let hash = hash(key, work)?;
            let mut slot = index.slot(hash);
            while index.slots[slot].is_some() {
                work(1).map_err(ValueFailure::Caller)?;
                slot = index.next(slot);
            }
            index.slots[slot] = Some(Candidate { hash, row });
        }
        Ok(index)
    }

    fn slot(&self, hash: u64) -> usize {
        // Mix high bits down before the power-of-two reduction.
        novarocks_type_contract::avalanche_hash_u64(hash) as usize & (self.slots.len() - 1)
    }

    fn next(&self, slot: usize) -> usize {
        (slot + 1) & (self.slots.len() - 1)
    }
}

#[cfg(test)]
mod tests;
