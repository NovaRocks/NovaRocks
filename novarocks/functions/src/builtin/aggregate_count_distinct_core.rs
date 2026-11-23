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
//! The COUNT DISTINCT family codec and allocation shape from the original v1.
//! Numeric DISTINCT sum/avg use a different, fixed-width codec.
use crate::aggregate_scalar::{
    self as scalar, AggScalarValue, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
};
use allocator_api2::vec::Vec as StateVec;
use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use hashbrown::{Equivalent, HashSet, hash_map::DefaultHashBuilder};
use std::hash::{Hash, Hasher};
#[path = "aggregate_count_distinct_source_failure.rs"]
mod source_failure;

// allocator-api2 Vec hashes its complete slice but does not implement
// Borrow<[u8]>. This query uses that same hash without allocating a stored key.
struct BorrowedCountKey<'a>(&'a [u8]);
impl Hash for BorrowedCountKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl<A: ScalarStateAllocator> Equivalent<StateVec<u8, A>> for BorrowedCountKey<'_> {
    fn equivalent(&self, key: &StateVec<u8, A>) -> bool {
        self.0 == key.as_slice()
    }
}

#[derive(Debug)]
pub struct CountDistinctState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub values: HashSet<StateVec<u8, A>, DefaultHashBuilder, A>,
}
impl<A: ScalarStateAllocator> CountDistinctState<A> {
    pub fn new(allocator: impl Into<A>) -> Self {
        let allocator = allocator.into();
        Self {
            values: HashSet::with_hasher_in(DefaultHashBuilder::default(), allocator.clone()),
            allocator,
        }
    }
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub fn iter(&self) -> impl Iterator<Item = &StateVec<u8, A>> {
        self.values.iter()
    }
    pub fn retained_bytes(&self) -> usize {
        0
    }
    pub fn insert_with_work(
        &mut self,
        value: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        // Observe the library lookup at its boundaries; this does not observe
        // hashing or collision comparisons inside the hash table.
        work.flush()?;
        let exists = self.values.contains(&BorrowedCountKey(value));
        work.flush()?;
        if exists {
            return Ok(());
        }
        // Preserve the original reservation order, including table capacity
        // left behind if the subsequent key allocation is refused.
        self.values.try_reserve(1).map_err(|source| {
            self.values
                .allocator()
                .scalar_allocation_error("reserve distinct hash set", source)
        })?;
        let key = scalar::scalar_bytes(self.allocator.clone(), value, work)?;
        self.values.insert(key);
        work.flush()?;
        Ok(())
    }
    pub fn insert(&mut self, value: Vec<u8>) -> Result<(), crate::KernelFailure> {
        self.insert_with_work(&value, &mut ScalarWork::new(None))
    }
    pub fn clear(&mut self) {
        // Dropping the table releases its capacity, unlike HashSet::clear.
        self.values =
            HashSet::with_hasher_in(DefaultHashBuilder::default(), self.allocator.clone());
    }
    #[cfg(test)]
    pub fn table_bytes(&self) -> usize {
        self.values.raw_table().allocation_info().1.size()
    }
}
/// Legacy scratch keeps Vec's original infallible growth and panic behavior;
/// selected scratch uses the explicitly supplied host allocator.
pub trait CountBuffer<'array> {
    fn append(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure>;
    fn bytes(&self) -> &[u8];
    fn raw_borrowed(
        self,
        bytes: &'array [u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, crate::KernelFailure>
    where
        Self: Sized,
    {
        self.raw_inline(bytes, work)
    }
    fn raw_inline(
        mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, crate::KernelFailure>
    where
        Self: Sized,
    {
        self.append(bytes, work)?;
        Ok(self)
    }
}
impl<'array> CountBuffer<'array> for Vec<u8> {
    fn append(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        self.extend_from_slice(bytes);
        work.flush()
    }
    fn bytes(&self) -> &[u8] {
        self.as_slice()
    }
    fn raw_inline(
        self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, crate::KernelFailure> {
        let key = bytes.to_vec();
        work.flush()?;
        Ok(key)
    }
}
impl<'array, A: ScalarStateAllocator> CountBuffer<'array> for StateVec<u8, A> {
    fn append(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        work.flush()?;
        self.try_reserve(bytes.len()).map_err(|source| {
            self.allocator()
                .scalar_allocation_error("reserve distinct key scratch", source)
        })?;
        for byte in bytes {
            self.push(*byte);
            work.step()?;
        }
        work.flush()
    }
    fn bytes(&self) -> &[u8] {
        self.as_slice()
    }
}
pub trait CountScalarValue: Sized {
    fn tag(&self) -> u8;
    fn fixed_bytes(&self) -> Option<([u8; 16], usize)>;
    fn variable_bytes(&self) -> Option<&[u8]>;
    fn decimal256(&self) -> Option<i256>;
    fn children(&self) -> usize;
    fn child(&self, index: usize, second: bool) -> Option<&Self>;
}
const TEMPORAL_TAG: u8 = 13;
macro_rules! value_access {
    ($enum:ident, $utf8:ident) => {
        fn tag(&self) -> u8 {
            match self {
                $enum::Bool(_) => 1,
                $enum::Int64(_) => 2,
                $enum::Float64(_) => 3,
                $enum::Utf8(_) => 4,
                $enum::Date32(_) => 5,
                $enum::Timestamp(_) => 6,
                $enum::Decimal128(_) => 7,
                $enum::Struct(_) => 8,
                $enum::Map(_) => 9,
                $enum::List(_) => 10,
                $enum::Decimal256(_) => 11,
                $enum::Binary(_) => 12,
                $enum::Temporal(..) => TEMPORAL_TAG,
            }
        }
        fn fixed_bytes(&self) -> Option<([u8; 16], usize)> {
            let mut bytes = [0; 16];
            macro_rules! fixed {
                ($v:expr) => {{
                    let raw = $v;
                    bytes[..raw.len()].copy_from_slice(&raw);
                    return Some((bytes, raw.len()));
                }};
            }
            match self {
                $enum::Bool(v) => fixed!([*v as u8]),
                $enum::Int64(v) | $enum::Timestamp(v) => fixed!(v.to_le_bytes()),
                $enum::Float64(v) => fixed!(v.to_bits().to_le_bytes()),
                $enum::Date32(v) => fixed!(v.to_le_bytes()),
                $enum::Decimal128(v) => fixed!(v.to_le_bytes()),
                _ => None,
            }
        }
        fn variable_bytes(&self) -> Option<&[u8]> {
            match self {
                $enum::Utf8(v) => Some(v.$utf8()),
                $enum::Binary(v) => Some(v.as_slice()),
                _ => None,
            }
        }
        fn decimal256(&self) -> Option<i256> {
            match self {
                $enum::Decimal256(v) => Some(*v),
                _ => None,
            }
        }
        fn children(&self) -> usize {
            match self {
                $enum::Struct(v) | $enum::List(v) => v.len(),
                $enum::Map(v) => v.len(),
                _ => 0,
            }
        }
        fn child(&self, index: usize, second: bool) -> Option<&Self> {
            match self {
                $enum::Struct(v) | $enum::List(v) => v[index].as_ref(),
                $enum::Map(v) => {
                    if second {
                        v[index].1.as_ref()
                    } else {
                        v[index].0.as_ref()
                    }
                }
                _ => None,
            }
        }
    };
}
impl CountScalarValue for AggScalarValue {
    value_access!(AggScalarValue, as_bytes);
}
impl<A: ScalarStateAllocator> CountScalarValue for TrackedAggScalarValue<A> {
    value_access!(TrackedAggScalarValue, as_slice);
}
fn append_scalar_body<'buffer, V: CountScalarValue, B: CountBuffer<'buffer>>(
    value: &V,
    out: &mut B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    let tag = value.tag();
    if tag == TEMPORAL_TAG {
        return Err(scalar::temporal_scalar_contract());
    }
    out.append(&[tag], work)?;
    if let Some((bytes, len)) = value.fixed_bytes() {
        return out.append(&bytes[..len], work);
    }
    if let Some(bytes) = value.variable_bytes() {
        out.append(
            &source_failure::encoded_len(source_failure::Length::Bytes, bytes.len())?.to_le_bytes(),
            work,
        )?;
        return out.append(bytes, work);
    }
    if let Some(v) = value.decimal256() {
        // The recursive v1 tag uses decimal text, unlike direct Decimal256.
        let text = v.to_string();
        out.append(
            &source_failure::encoded_len(source_failure::Length::DecimalText, text.len())?
                .to_le_bytes(),
            work,
        )?;
        return out.append(text.as_bytes(), work);
    }
    out.append(
        &source_failure::encoded_len(source_failure::Length::Children, value.children())?
            .to_le_bytes(),
        work,
    )?;
    for idx in 0..value.children() {
        append_optional(value.child(idx, false), out, work)?;
        if tag == 9 {
            append_optional(value.child(idx, true), out, work)?;
        }
    }
    Ok(())
}
pub fn append_optional<'buffer, V: CountScalarValue, B: CountBuffer<'buffer>>(
    value: Option<&V>,
    out: &mut B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    match value {
        Some(value) => {
            out.append(&[1], work)?;
            append_scalar_body(value, out, work)
        }
        None => out.append(&[0], work),
    }
}
pub trait CountScalarReader {
    type Value: CountScalarValue;
    fn read(
        &self,
        array: &ArrayRef,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<Self::Value>, crate::KernelFailure>;
}
pub struct OwnedCountReader;
impl CountScalarReader for OwnedCountReader {
    type Value = AggScalarValue;
    fn read(
        &self,
        array: &ArrayRef,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<Self::Value>, crate::KernelFailure> {
        work.flush()?;
        scalar::scalar_from_array(array, row, work)
    }
}
pub struct TrackedCountReader<'a, A: ScalarStateAllocator>(pub &'a A);
impl<A: ScalarStateAllocator> CountScalarReader for TrackedCountReader<'_, A> {
    type Value = TrackedAggScalarValue<A>;
    fn read(
        &self,
        array: &ArrayRef,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<Self::Value>, crate::KernelFailure> {
        scalar::tracked_scalar_from_array(array, row, self.0, work)
    }
}
/// Exact scalar or packed-Struct v1 key. Primitive raw keys use native endian.
pub fn encode_row<'array, R: CountScalarReader, B: CountBuffer<'array>>(
    array: &'array ArrayRef,
    row: usize,
    reader: &R,
    mut out: B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<B>, crate::KernelFailure> {
    macro_rules! primitive {
        ($array:ty,$bytes:expr,$error:literal) => {{
            let arr = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| source_failure::contract($error))?;
            if arr.is_null(row) {
                return Ok(None);
            }
            return out.raw_inline(&($bytes)(arr.value(row)), work).map(Some);
        }};
    }
    match array.data_type() {
        DataType::Null => Ok(None),
        DataType::Int8 => primitive!(
            Int8Array,
            |v: i8| v.to_ne_bytes(),
            "failed to downcast to Int8Array"
        ),
        DataType::Int16 => primitive!(
            Int16Array,
            |v: i16| v.to_ne_bytes(),
            "failed to downcast to Int16Array"
        ),
        DataType::Int32 => primitive!(
            Int32Array,
            |v: i32| v.to_ne_bytes(),
            "failed to downcast to Int32Array"
        ),
        DataType::Int64 => primitive!(
            Int64Array,
            |v: i64| v.to_ne_bytes(),
            "failed to downcast to Int64Array"
        ),
        DataType::Float32 => primitive!(
            Float32Array,
            |v: f32| v.to_bits().to_ne_bytes(),
            "failed to downcast to Float32Array"
        ),
        DataType::Float64 => primitive!(
            Float64Array,
            |v: f64| v.to_bits().to_ne_bytes(),
            "failed to downcast to Float64Array"
        ),
        DataType::Boolean => primitive!(
            BooleanArray,
            |v: bool| [v as u8],
            "failed to downcast to BooleanArray"
        ),
        DataType::Date32 => primitive!(
            Date32Array,
            |v: i32| v.to_ne_bytes(),
            "failed to downcast to Date32Array"
        ),
        DataType::Decimal128(..) => primitive!(
            Decimal128Array,
            |v: i128| v.to_le_bytes(),
            "failed to downcast to Decimal128Array"
        ),
        DataType::Decimal256(..) => primitive!(
            Decimal256Array,
            |v: i256| v.to_le_bytes(),
            "failed to downcast to Decimal256Array"
        ),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(
            TimestampSecondArray,
            |v: i64| v.to_ne_bytes(),
            "failed to downcast to TimestampSecondArray"
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(
            TimestampMillisecondArray,
            |v: i64| v.to_ne_bytes(),
            "failed to downcast to TimestampMillisecondArray"
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(
            TimestampMicrosecondArray,
            |v: i64| v.to_ne_bytes(),
            "failed to downcast to TimestampMicrosecondArray"
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(
            TimestampNanosecondArray,
            |v: i64| v.to_ne_bytes(),
            "failed to downcast to TimestampNanosecondArray"
        ),
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| source_failure::contract("failed to downcast to StringArray"))?;
            if arr.is_null(row) {
                return Ok(None);
            }
            out.raw_borrowed(arr.value(row).as_bytes(), work).map(Some)
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| source_failure::contract("failed to downcast to BinaryArray"))?;
            if arr.is_null(row) {
                return Ok(None);
            }
            out.raw_borrowed(arr.value(row), work).map(Some)
        }
        DataType::List(_) | DataType::Struct(_) => {
            if array.is_null(row) {
                return Ok(None);
            }
            let value = reader.read(array, row, work)?;
            let Some(value) = value else {
                return Ok(None);
            };
            if value.tag() == 8 {
                for idx in 0..value.children() {
                    work.step()?;
                    if value.child(idx, false).is_none() {
                        return Ok(None);
                    }
                }
            }
            append_optional(Some(&value), &mut out, work)?;
            Ok(Some(out))
        }
        other => Err(source_failure::contract(format_args!(
            "unsupported count_distinct input type: {:?}",
            other
        ))),
    }
}
/// Variadic logical channels are the original packed Struct fields. All fields
/// are read before the immediate-NULL predicate, preserving the old row order.
pub fn encode_tuple<'buffer, A: ScalarStateAllocator, B: CountBuffer<'buffer>>(
    arguments: &[(&ArrayRef, usize)],
    allocator: &A,
    mut out: B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<B>, crate::KernelFailure> {
    let mut values = scalar::aggregate_vec_with_capacity(
        allocator,
        arguments.len(),
        "reserve distinct tuple scratch",
        work,
    )?;
    for (array, row) in arguments {
        values.push(scalar::tracked_scalar_from_array(
            array, *row, allocator, work,
        )?);
        work.step()?;
    }
    for value in &values {
        work.step()?;
        if value.is_none() {
            return Ok(None);
        }
    }
    let value = TrackedAggScalarValue::Struct(values);
    append_optional(Some(&value), &mut out, work)?;
    Ok(Some(out))
}
pub fn serialize_set<'buffer, A: ScalarStateAllocator, B: CountBuffer<'buffer>>(
    set: &CountDistinctState<A>,
    mut out: B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<B, crate::KernelFailure> {
    // Preserve v1 iteration order and length words; reject a nonrepresentable
    // extent before emitting its fixed-width header.
    out.append(
        &source_failure::encoded_len(source_failure::Length::SetCount, set.len())?.to_le_bytes(),
        work,
    )?;
    for value in set.iter() {
        out.append(
            &source_failure::encoded_len(source_failure::Length::KeyBytes, value.len())?
                .to_le_bytes(),
            work,
        )?;
        out.append(value, work)?;
    }
    Ok(out)
}
pub trait CountDecodedKeys {
    fn begin(
        &mut self,
        count: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure>;
    fn push_key(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure>;
}
impl CountDecodedKeys for Vec<Vec<u8>> {
    fn begin(
        &mut self,
        count: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        *self = Vec::with_capacity(count);
        work.flush()
    }
    fn push_key(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        self.push(bytes.to_vec());
        work.flush()
    }
}
impl<A: ScalarStateAllocator> CountDecodedKeys for StateVec<StateVec<u8, A>, A> {
    fn begin(
        &mut self,
        count: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        work.flush()?;
        self.try_reserve_exact(count).map_err(|source| {
            self.allocator()
                .scalar_allocation_error("reserve distinct decoded keys", source)
        })?;
        work.flush()
    }
    fn push_key(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        let value = scalar::scalar_bytes(self.allocator().clone(), bytes, work)?;
        self.push(value);
        work.step()
    }
}
pub fn deserialize_set<C: CountDecodedKeys>(
    bytes: &[u8],
    mut output: C,
    work: &mut ScalarWork<'_, '_>,
) -> Result<C, crate::KernelFailure> {
    let invalid = || source_failure::malformed();
    if bytes.len() < 4 {
        return Err(invalid());
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let mut pos = 4usize;
    if count > bytes.len().saturating_sub(4) / 4 {
        return Err(invalid());
    }
    output.begin(count, work)?;
    for _ in 0..count {
        if pos.checked_add(4).is_none_or(|end| end > bytes.len()) {
            return Err(invalid());
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos.checked_add(len).is_none_or(|end| end > bytes.len()) {
            return Err(invalid());
        }
        output.push_key(&bytes[pos..pos + len], work)?;
        pos += len;
    }
    // v1 deliberately ignores all trailing bytes and accepts every key width.
    Ok(output)
}
pub fn merge_decoded<A: ScalarStateAllocator>(
    state: &mut CountDistinctState<A>,
    values: impl IntoIterator<Item = impl AsRef<[u8]>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    for value in values {
        state.insert_with_work(value.as_ref(), work)?;
    }
    Ok(())
}
pub fn finalize_count<A: ScalarStateAllocator>(
    set: &CountDistinctState<A>,
) -> Result<i64, crate::KernelFailure> {
    source_failure::cardinality(set.len())
}

pub fn validate_source_type(ty: &DataType) -> Result<(), crate::KernelFailure> {
    match ty {
        DataType::Null
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Boolean
        | DataType::Utf8
        | DataType::Binary
        | DataType::Date32
        | DataType::Timestamp(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::List(_)
        | DataType::Struct(_) => Ok(()),
        other => Err(source_failure::contract(format_args!(
            "unsupported count_distinct input type: {:?}",
            other
        ))),
    }
}

/// Original top-level dispatch checks concrete primitive carriers before rows.
/// Nested readers only downcast when a nonnull row is actually read.
pub fn validate_source_array(array: &ArrayRef) -> Result<(), crate::KernelFailure> {
    macro_rules! carrier {
        ($ty:ty,$message:literal) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .map(|_| ())
                .ok_or_else(|| source_failure::contract($message))
        };
    }
    match array.data_type() {
        DataType::Int8 => carrier!(Int8Array, "failed to downcast to Int8Array"),
        DataType::Int16 => carrier!(Int16Array, "failed to downcast to Int16Array"),
        DataType::Int32 => carrier!(Int32Array, "failed to downcast to Int32Array"),
        DataType::Int64 => carrier!(Int64Array, "failed to downcast to Int64Array"),
        DataType::Float32 => carrier!(Float32Array, "failed to downcast to Float32Array"),
        DataType::Float64 => carrier!(Float64Array, "failed to downcast to Float64Array"),
        DataType::Boolean => carrier!(BooleanArray, "failed to downcast to BooleanArray"),
        DataType::Utf8 => carrier!(StringArray, "failed to downcast to StringArray"),
        DataType::Binary => carrier!(BinaryArray, "failed to downcast to BinaryArray"),
        DataType::Date32 => carrier!(Date32Array, "failed to downcast to Date32Array"),
        DataType::Decimal128(..) => {
            carrier!(Decimal128Array, "failed to downcast to Decimal128Array")
        }
        DataType::Decimal256(..) => {
            carrier!(Decimal256Array, "failed to downcast to Decimal256Array")
        }
        DataType::Timestamp(TimeUnit::Second, _) => carrier!(
            TimestampSecondArray,
            "failed to downcast to TimestampSecondArray"
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => carrier!(
            TimestampMillisecondArray,
            "failed to downcast to TimestampMillisecondArray"
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => carrier!(
            TimestampMicrosecondArray,
            "failed to downcast to TimestampMicrosecondArray"
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => carrier!(
            TimestampNanosecondArray,
            "failed to downcast to TimestampNanosecondArray"
        ),
        ty => validate_source_type(ty),
    }
}

#[cfg(test)]
#[path = "aggregate_count_distinct_core_tests.rs"]
mod tests;

/// Direct scalar keys require no temporary allocation. Nested encoded keys use
/// the same tracked buffer as tuple encoding and serialization.
pub enum SelectedCountKey<'array, A: ScalarStateAllocator> {
    Inline { bytes: [u8; 32], len: usize },
    Borrowed(&'array [u8]),
    Owned(StateVec<u8, A>),
}
impl<A: ScalarStateAllocator> SelectedCountKey<'_, A> {
    pub fn new(allocator: A) -> Self {
        Self::Owned(StateVec::new_in(allocator))
    }
}
impl<'array, A: ScalarStateAllocator> CountBuffer<'array> for SelectedCountKey<'array, A> {
    fn append(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        match self {
            Self::Owned(out) => CountBuffer::append(out, bytes, work),
            _ => Err(source_failure::contract(
                "COUNT DISTINCT raw key cannot be extended",
            )),
        }
    }
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, len } => &bytes[..*len],
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes.as_slice(),
        }
    }
    fn raw_inline(
        self,
        source: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, crate::KernelFailure> {
        if source.len() > 32 {
            return Err(source_failure::contract(
                "native COUNT DISTINCT numeric key is at most 32 bytes",
            ));
        }
        let mut bytes = [0; 32];
        bytes[..source.len()].copy_from_slice(source);
        work.step()?;
        Ok(Self::Inline {
            bytes,
            len: source.len(),
        })
    }
    fn raw_borrowed(
        self,
        bytes: &'array [u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, crate::KernelFailure> {
        work.step()?;
        Ok(Self::Borrowed(bytes))
    }
}
