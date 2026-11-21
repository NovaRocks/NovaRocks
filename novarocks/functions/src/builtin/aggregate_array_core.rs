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
//! Original ARRAY aggregate computation, scalar output author and row-state codec.
use super::aggregate_array_source_failure as failure;
use crate::KernelFailure;
use crate::aggregate_scalar::{
    checked_list_output_offset, checked_map_output_offset, checked_utf8_output_offset,
};
use crate::{
    aggregate_scalar::{
        AggScalarValue, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
        aggregate_vec_with_capacity, tracked_scalar_from_array, tracked_scalar_heap_capacity,
        tracked_scalar_to_output,
    },
    aggregate_scalar_fingerprint::{
        compare_scalar_values, key_fingerprint, tracked_optional_key_fingerprint,
    },
    largeint,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray, NullArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use hashbrown::{HashSet as ScalarSet, hash_map::DefaultHashBuilder};
use std::{cmp::Ordering, collections::HashSet, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub enum ArrayAggKind<'a> {
    Array {
        distinct: bool,
        ascending: &'a [bool],
        nulls_first: &'a [bool],
    },
    Unique,
}
#[derive(Clone, Copy, Debug)]
pub struct ArrayAggConfig<'a> {
    pub kind: ArrayAggKind<'a>,
    pub output_type: &'a DataType,
    pub intermediate_type: &'a DataType,
    pub input_arg_type: Option<&'a DataType>,
}
#[derive(Debug)]
pub struct ArrayAggState<A: ScalarStateAllocator> {
    pub allocator: A,
    rows: ScalarVec<ScalarVec<Option<TrackedAggScalarValue<A>>, A>, A>,
    distinct_seen: ScalarSet<ScalarVec<u8, A>, DefaultHashBuilder, A>,
    row_backing_bytes: usize,
    key_backing_bytes: usize,
}
impl<A: ScalarStateAllocator> ArrayAggState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            rows: ScalarVec::new_in(allocator.clone()),
            distinct_seen: ScalarSet::with_hasher_in(
                DefaultHashBuilder::default(),
                allocator.clone(),
            ),
            allocator,
            row_backing_bytes: 0,
            key_backing_bytes: 0,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.distinct_seen.is_empty()
    }
    pub fn retained_bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<ScalarVec<Option<TrackedAggScalarValue<A>>, A>>()
            + self.row_backing_bytes
            + self.distinct_seen.raw_table().allocation_info().1.size()
            + self.key_backing_bytes
    }
    fn note_row_backing(
        &mut self,
        row: &ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        let mut retained = row.capacity() * std::mem::size_of::<Option<TrackedAggScalarValue<A>>>();
        for value in row.iter().flatten() {
            work.step()?;
            retained = retained
                .checked_add(tracked_scalar_heap_capacity(value, work)?)
                .ok_or(crate::KernelFailure::ResourceExhausted)?;
        }
        self.row_backing_bytes = self
            .row_backing_bytes
            .checked_add(retained)
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        Ok(())
    }
    pub fn clear(&mut self) {
        self.row_backing_bytes = 0;
        self.key_backing_bytes = 0;
        self.rows = ScalarVec::new_in(self.allocator.clone());
        self.distinct_seen =
            ScalarSet::with_hasher_in(DefaultHashBuilder::default(), self.allocator.clone());
    }
    fn push_row(
        &mut self,
        row: ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        work.flush()?;
        self.rows.try_reserve(1).map_err(|source| {
            self.allocator
                .scalar_allocation_error("reserve array_agg row", source)
        })?;
        work.flush()?;
        self.note_row_backing(&row, work)?;
        self.rows.push(row);
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub enum ArrayAggValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
    Struct(Vec<Option<ArrayAggValue>>),
    List(Vec<Option<ArrayAggValue>>),
}

fn is_distinct_kind(kind: &ArrayAggKind<'_>) -> bool {
    match kind {
        ArrayAggKind::Array { distinct, .. } => *distinct,
        ArrayAggKind::Unique => true,
    }
}
fn order_by_kind<'a>(kind: &ArrayAggKind<'a>) -> (&'a [bool], &'a [bool]) {
    match kind {
        ArrayAggKind::Array {
            ascending,
            nulls_first,
            ..
        } => (*ascending, *nulls_first),
        ArrayAggKind::Unique => (&[], &[]),
    }
}
pub fn to_common_scalar(
    value: &ArrayAggValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<AggScalarValue, KernelFailure> {
    work.step()?;
    Ok(match value {
        ArrayAggValue::Bool(v) => AggScalarValue::Bool(*v),
        ArrayAggValue::Int64(v) => AggScalarValue::Int64(*v),
        ArrayAggValue::Float64(v) => AggScalarValue::Float64(*v),
        ArrayAggValue::Utf8(v) => {
            work.flush()?;
            let value = v.clone();
            work.flush()?;
            AggScalarValue::Utf8(value)
        }
        ArrayAggValue::Date32(v) => AggScalarValue::Date32(*v),
        ArrayAggValue::Timestamp(v) => AggScalarValue::Timestamp(*v),
        ArrayAggValue::Decimal128(v) => AggScalarValue::Decimal128(*v),
        ArrayAggValue::Struct(items) => AggScalarValue::Struct(
            items
                .iter()
                .map(|item| {
                    work.step()?;
                    item.as_ref().map(|v| to_common_scalar(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ArrayAggValue::List(items) => AggScalarValue::List(
            items
                .iter()
                .map(|item| {
                    work.step()?;
                    item.as_ref().map(|v| to_common_scalar(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}
pub fn distinct_key(
    value: &Option<ArrayAggValue>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, KernelFailure> {
    work.step()?;
    Ok(match value {
        None => vec![0],
        Some(v) => {
            let mut out = vec![1];
            out.extend(key_fingerprint(&to_common_scalar(v, work)?, work)?);
            out
        }
    })
}
fn compare_optional_values(
    left: &Option<ArrayAggValue>,
    right: &Option<ArrayAggValue>,
    asc: bool,
    nulls_first: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, KernelFailure> {
    work.step()?;
    let ord = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let left = to_common_scalar(left, work)?;
            let right = to_common_scalar(right, work)?;
            let ord = compare_scalar_values(&left, &right, work)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn array_value_from_tracked<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayAggValue, KernelFailure> {
    array_value_from_common(tracked_scalar_to_output(value, work)?, work)
}

fn array_value_from_common(
    value: AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayAggValue, KernelFailure> {
    work.step()?;
    Ok(match value {
        AggScalarValue::Bool(value) => ArrayAggValue::Bool(value),
        AggScalarValue::Int64(value) => ArrayAggValue::Int64(value),
        AggScalarValue::Float64(value) => ArrayAggValue::Float64(value),
        AggScalarValue::Utf8(value) => ArrayAggValue::Utf8(value),
        AggScalarValue::Date32(value) => ArrayAggValue::Date32(value),
        AggScalarValue::Timestamp(value) => ArrayAggValue::Timestamp(value),
        AggScalarValue::Decimal128(value) => ArrayAggValue::Decimal128(value),
        AggScalarValue::Struct(values) => ArrayAggValue::Struct(
            values
                .into_iter()
                .map(|value| {
                    work.step()?;
                    value.map(|v| array_value_from_common(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AggScalarValue::List(values) => ArrayAggValue::List(
            values
                .into_iter()
                .map(|value| {
                    work.step()?;
                    value.map(|v| array_value_from_common(v, work)).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AggScalarValue::Map(entries) => ArrayAggValue::List(
            entries
                .into_iter()
                .map(|(key, value)| {
                    work.step()?;
                    Ok(Some(ArrayAggValue::Struct(vec![
                        key.map(|v| array_value_from_common(v, work)).transpose()?,
                        value
                            .map(|v| array_value_from_common(v, work))
                            .transpose()?,
                    ])))
                })
                .collect::<Result<Vec<_>, KernelFailure>>()?,
        ),
        AggScalarValue::Decimal256(_)
        | AggScalarValue::Binary(_)
        | AggScalarValue::Temporal(..) => {
            return Err(failure::contract(
                "array_agg tracked scalar type is not supported by its output ABI",
            ));
        }
    })
}

fn sort_rows(
    rows: &mut [Vec<Option<ArrayAggValue>>],
    is_asc_order: &[bool],
    nulls_first: &[bool],
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    let order_by_num = is_asc_order.len();
    if order_by_num == 0 {
        return Ok(());
    }
    work.flush()?;
    let mut error: Option<KernelFailure> = None;
    rows.sort_by(|left, right| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key in 0..order_by_num {
            let col = 1 + key;
            let l = left.get(col).cloned().unwrap_or(None);
            let r = right.get(col).cloned().unwrap_or(None);
            match compare_optional_values(&l, &r, is_asc_order[key], nulls_first[key], work) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(err) => {
                    error = Some(err);
                    return Ordering::Equal;
                }
            }
        }
        Ordering::Equal
    });
    if let Some(err) = error {
        return Err(err);
    }
    work.flush()?;
    Ok(())
}

fn extract_final_values<A: ScalarStateAllocator>(
    spec: &ArrayAggConfig<'_>,
    state: &ArrayAggState<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<Option<ArrayAggValue>>, KernelFailure> {
    let mut rows = state
        .rows
        .iter()
        .map(|row| {
            work.step()?;
            row.iter()
                .map(|value| {
                    work.step()?;
                    value
                        .as_ref()
                        .map(|v| array_value_from_tracked(v, work))
                        .transpose()
                })
                .collect::<Result<Vec<_>, KernelFailure>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (is_asc_order, nulls_first) = order_by_kind(&spec.kind);
    sort_rows(&mut rows, is_asc_order, nulls_first, work)?;

    let distinct = is_distinct_kind(&spec.kind);
    let mut out = Vec::with_capacity(rows.len());
    let mut seen = HashSet::new();
    for row in rows {
        work.step()?;
        let value = row.first().cloned().unwrap_or(None);
        if distinct {
            let key = distinct_key(&value, work)?;
            if seen.insert(key) {
                out.push(value);
            }
        } else {
            out.push(value);
        }
    }
    Ok(out)
}

fn reconcile_field_to_field(
    expected: &Arc<Field>,
    actual: &Arc<Field>,
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Arc<Field>, KernelFailure> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        let data_type =
            reconcile_data_type(expected.data_type(), actual.data_type(), context, work)?;
        let nullable = if expected.is_nullable() == actual.is_nullable() {
            expected.is_nullable()
        } else {
            actual.is_nullable()
        };
        if &data_type == expected.data_type() && nullable == expected.is_nullable() {
            Ok(expected.clone())
        } else {
            Ok(Arc::new(Field::new(expected.name(), data_type, nullable)))
        }
    })();
    work.flush()?;
    result
}

fn reconcile_field_to_data_type(
    expected: &Arc<Field>,
    actual: &DataType,
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Arc<Field>, KernelFailure> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        let data_type = reconcile_data_type(expected.data_type(), actual, context, work)?;
        if &data_type == expected.data_type() {
            Ok(expected.clone())
        } else {
            Ok(Arc::new(Field::new(
                expected.name(),
                data_type,
                expected.is_nullable(),
            )))
        }
    })();
    work.flush()?;
    result
}

fn reconcile_data_type(
    expected: &DataType,
    actual: &DataType,
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<DataType, KernelFailure> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        if expected == actual {
            return Ok(expected.clone());
        }
        match (expected, actual) {
            (DataType::Map(_, expected_ordered), DataType::Map(_, actual_ordered))
                if expected_ordered == actual_ordered =>
            {
                Ok(actual.clone())
            }
            (DataType::List(expected_item), DataType::List(actual_item)) => Ok(DataType::List(
                reconcile_field_to_field(expected_item, actual_item, context, work)?,
            )),
            (DataType::Struct(expected_fields), DataType::Struct(actual_fields))
                if expected_fields.len() == actual_fields.len() =>
            {
                let fields = expected_fields
                    .iter()
                    .zip(actual_fields.iter())
                    .map(|(expected_field, actual_field)| {
                        work.step()?;
                        reconcile_field_to_field(expected_field, actual_field, context, work)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            _ => Err(failure::shape_mismatch(format_args!(
                "{context} type mismatch: expected {:?}, got {:?}",
                expected, actual
            ))),
        }
    })();
    work.flush()?;
    result
}

fn reconcile_fields_for_columns(
    expected_fields: &Fields,
    columns: &[ArrayRef],
    context: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Fields, KernelFailure> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        if expected_fields.len() != columns.len() {
            return Err(failure::contract(format_args!(
                "{context} field count mismatch: expected {}, got {}",
                expected_fields.len(),
                columns.len()
            )));
        }
        let fields = expected_fields
            .iter()
            .zip(columns.iter())
            .map(|(expected_field, column)| {
                work.step()?;
                reconcile_field_to_data_type(expected_field, column.data_type(), context, work)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Fields::from(fields))
    })();
    work.flush()?;
    result
}

fn append_value<A: ScalarStateAllocator>(
    state: &mut ArrayAggState<A>,
    value: Option<TrackedAggScalarValue<A>>,
    distinct: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    if distinct {
        let key = tracked_optional_key_fingerprint(&value, &state.allocator, work)?;
        if state.distinct_seen.contains(&key) {
            return Ok(());
        }
        let mut row = aggregate_vec_with_capacity(
            &state.allocator,
            1,
            "reserve array_agg distinct row",
            work,
        )?;
        row.push(value);
        state.rows.try_reserve(1).map_err(|source| {
            state
                .allocator
                .scalar_allocation_error("reserve array_agg row", source)
        })?;
        state.distinct_seen.try_reserve(1).map_err(|source| {
            state
                .allocator
                .scalar_allocation_error("reserve array_agg distinct key", source)
        })?;
        state.note_row_backing(&row, work)?;
        state.key_backing_bytes = state
            .key_backing_bytes
            .checked_add(key.capacity())
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        state.distinct_seen.insert(key);
        state.rows.push(row);
    } else {
        let mut row =
            aggregate_vec_with_capacity(&state.allocator, 1, "reserve array_agg row", work)?;
        row.push(value);
        state.push_row(row, work)?;
    }
    Ok(())
}

pub fn unwrap_update_value_array<'a>(
    spec: &ArrayAggConfig<'_>,
    array: &'a ArrayRef,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(&'a ArrayRef, Option<&'a StructArray>), KernelFailure> {
    work.step()?;
    work.flush()?;
    let result = (|| {
        let Some(wrapper) = array.as_any().downcast_ref::<StructArray>() else {
            return Ok((array, None));
        };
        let Some(expected_item_type) = spec.input_arg_type else {
            return Ok((array, None));
        };
        let first = wrapper.columns().first().ok_or_else(|| {
            failure::contract(
                "array_agg struct input must contain at least 1 field for value extraction",
            )
        })?;
        let first_matches_expected = if first.data_type() == expected_item_type {
            true
        } else {
            match reconcile_data_type(
                expected_item_type,
                first.data_type(),
                "array_agg update input",
                work,
            ) {
                Ok(_) => true,
                Err(error) if failure::is_shape_mismatch(&error) => false,
                Err(error) => return Err(error),
            }
        };
        if first_matches_expected {
            Ok((first, Some(wrapper)))
        } else {
            Ok((array, None))
        }
    })();
    work.flush()?;
    result
}

fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<ArrayAggValue>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure> {
    match output_type {
        DataType::Null => {
            let mut any = false;
            for value in &values {
                work.step()?;
                any |= value.is_some();
            }
            if any {
                work.step()?;
                return Err(failure::contract("scalar output type mismatch for Null"));
            }
            Ok(Arc::new(NullArray::new(values.len())) as ArrayRef)
        }
        DataType::Boolean => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Bool(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Boolean")),
                }
            }
            Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
        }
        DataType::Int8 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v =
                            i8::try_from(v).map_err(|source| failure::narrow("int8", source))?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Int8")),
                }
            }
            Ok(Arc::new(Int8Array::from(out)) as ArrayRef)
        }
        DataType::Int16 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v =
                            i16::try_from(v).map_err(|source| failure::narrow("int16", source))?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Int16")),
                }
            }
            Ok(Arc::new(Int16Array::from(out)) as ArrayRef)
        }
        DataType::Int32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v =
                            i32::try_from(v).map_err(|source| failure::narrow("int32", source))?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Int32")),
                }
            }
            Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
        }
        DataType::Int64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Int64(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Int64")),
                }
            }
            Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
        }
        DataType::Float32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Float64(v)) => out.push(Some(v as f32)),
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Float32")),
                }
            }
            Ok(Arc::new(Float32Array::from(out)) as ArrayRef)
        }
        DataType::Float64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Float64(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Float64")),
                }
            }
            Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut out = Vec::with_capacity(values.len());
            let mut bytes = 0_i32;
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Utf8(v)) => {
                        bytes = checked_utf8_output_offset(bytes, v.len())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Utf8")),
                }
            }
            Ok(Arc::new(StringArray::from(out)) as ArrayRef)
        }
        DataType::Date32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Date32(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err(failure::contract("scalar output type mismatch for Date32")),
                }
            }
            Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
        }
        DataType::Timestamp(unit, tz) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Timestamp(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err(failure::contract(
                            "scalar output type mismatch for Timestamp",
                        ));
                    }
                }
            }
            let tz = tz.as_deref().map(|s| s.to_string());
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    let arr = TimestampSecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Millisecond => {
                    let arr = TimestampMillisecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Microsecond => {
                    let arr = TimestampMicrosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Nanosecond => {
                    let arr = TimestampNanosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
            };
            Ok(array)
        }
        DataType::Decimal128(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err(failure::contract(
                            "scalar output type mismatch for Decimal128",
                        ));
                    }
                }
            }
            let arr = Decimal128Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(failure::constructor)?;
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                work.step()?;
                match value {
                    Some(ArrayAggValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => {
                        return Err(failure::contract(
                            "scalar output type mismatch for LargeInt",
                        ));
                    }
                }
            }
            largeint::array_from_i128_observed(&out, &mut |event| match event {
                largeint::LargeIntObservation::Step => work.step(),
                largeint::LargeIntObservation::OpaqueBoundary => work.flush(),
            })
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(failure::contract(
                    "array_agg cannot build Struct output with no fields",
                ));
            }
            let mut null_builder = NullBufferBuilder::new(values.len());
            let mut by_field: Vec<Vec<Option<ArrayAggValue>>> = (0..fields.len())
                .map(|_| Vec::with_capacity(values.len()))
                .collect();
            for value in values {
                work.step()?;
                match value {
                    None => {
                        null_builder.append_null();
                        for field_values in &mut by_field {
                            work.step()?;
                            field_values.push(None);
                        }
                    }
                    Some(ArrayAggValue::Struct(items)) => {
                        if items.len() != fields.len() {
                            return Err(failure::contract(format_args!(
                                "scalar output type mismatch for Struct: expected {} fields, got {}",
                                fields.len(),
                                items.len()
                            )));
                        }
                        null_builder.append_non_null();
                        for (idx, item) in items.into_iter().enumerate() {
                            work.step()?;
                            by_field[idx].push(item);
                        }
                    }
                    Some(other) => {
                        return Err(failure::contract(format_args!(
                            "scalar output type mismatch for Struct: got {:?}",
                            other
                        )));
                    }
                }
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (idx, field) in fields.iter().enumerate() {
                work.step()?;
                columns.push(build_scalar_array(
                    field.data_type(),
                    std::mem::take(&mut by_field[idx]),
                    work,
                )?);
            }
            let output_fields =
                reconcile_fields_for_columns(fields, &columns, "array_agg struct output", work)?;
            work.flush()?;
            let out = StructArray::try_new(output_fields, columns, null_builder.finish())
                .map_err(failure::constructor)?;
            work.flush()?;
            Ok(Arc::new(out) as ArrayRef)
        }
        DataType::Map(entries_field, ordered) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(failure::contract(format_args!(
                    "array_agg map entries field must be Struct, got {:?}",
                    entries_field.data_type()
                )));
            };
            if entry_fields.len() != 2 {
                return Err(failure::contract(format_args!(
                    "array_agg map entries field must contain key/value, got {} fields",
                    entry_fields.len()
                )));
            }

            let mut key_values = Vec::new();
            let mut value_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i32 = 0;
            let mut null_builder = NullBufferBuilder::new(values.len());

            for value in values {
                work.step()?;
                match value {
                    None => {
                        null_builder.append_null();
                        offsets.push(current);
                    }
                    Some(ArrayAggValue::List(items)) => {
                        let next = checked_map_output_offset(current, items.len())?;
                        null_builder.append_non_null();
                        for item in items {
                            work.step()?;
                            let Some(ArrayAggValue::Struct(entry)) = item else {
                                return Err(failure::contract(
                                    "scalar output type mismatch for Map entry",
                                ));
                            };
                            if entry.len() != 2 {
                                return Err(failure::contract(format_args!(
                                    "scalar output type mismatch for Map entry field count: expected 2, got {}",
                                    entry.len()
                                )));
                            }
                            key_values.push(entry[0].clone());
                            value_values.push(entry[1].clone());
                        }
                        current = next;
                        offsets.push(current);
                    }
                    Some(other) => {
                        return Err(failure::contract(format_args!(
                            "scalar output type mismatch for Map: got {:?}",
                            other
                        )));
                    }
                }
            }

            let key_array = build_scalar_array(entry_fields[0].data_type(), key_values, work)?;
            let value_array = build_scalar_array(entry_fields[1].data_type(), value_values, work)?;
            let entries_fields = if key_array.null_count() > 0 && !entry_fields[0].is_nullable() {
                let mut adjusted = entry_fields.iter().cloned().collect::<Vec<_>>();
                adjusted[0] = Arc::new(Field::new(
                    entry_fields[0].name(),
                    entry_fields[0].data_type().clone(),
                    true,
                ));
                Fields::from(adjusted)
            } else {
                entry_fields.clone()
            };
            let entries =
                StructArray::try_new(entries_fields.clone(), vec![key_array, value_array], None)
                    .map_err(failure::constructor)?;
            let entries_field = Arc::new(Field::new(
                entries_field.name(),
                DataType::Struct(entries_fields),
                entries_field.is_nullable(),
            ));
            let out = MapArray::try_new(
                entries_field,
                OffsetBuffer::new(offsets.into()),
                entries,
                null_builder.finish(),
                *ordered,
            )
            .map_err(failure::constructor)?;
            Ok(Arc::new(out) as ArrayRef)
        }
        DataType::List(field) => {
            let mut flattened = Vec::<Option<ArrayAggValue>>::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i32 = 0;
            let mut null_builder = NullBufferBuilder::new(values.len());
            for value in values {
                work.step()?;
                match value {
                    None => {
                        null_builder.append_null();
                        offsets.push(current);
                    }
                    Some(ArrayAggValue::List(items)) => {
                        current = checked_list_output_offset(current, items.len())?;
                        flattened.extend(items);
                        null_builder.append_non_null();
                        offsets.push(current);
                    }
                    Some(other) => {
                        return Err(failure::contract(format_args!(
                            "scalar output type mismatch for List: got {:?}",
                            other
                        )));
                    }
                }
            }
            let child_values = build_scalar_array(field.data_type(), flattened, work)?;
            let list_field = reconcile_field_to_data_type(
                field,
                child_values.data_type(),
                "array_agg list item",
                work,
            )?;
            let out = ListArray::try_new(
                list_field,
                OffsetBuffer::new(offsets.into()),
                child_values,
                null_builder.finish(),
            )
            .map_err(failure::constructor)?;
            Ok(Arc::new(out) as ArrayRef)
        }
        other => Err(failure::contract(format_args!(
            "unsupported scalar output type: {:?}",
            other
        ))),
    }
}

/// Frozen intermediate-carrier rejection. The error retains shape facts only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayAggMergeInputError {
    EmptyStruct { fields: usize },
    FirstFieldNotList,
    NotListOrStruct,
}
impl std::fmt::Display for ArrayAggMergeInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyStruct { fields } => write!(
                f,
                "array_agg merge struct input expects at least 1 field, got {fields}"
            ),
            Self::FirstFieldNotList => {
                f.write_str("array_agg merge struct field[0] must be ListArray")
            }
            Self::NotListOrStruct => {
                f.write_str("array_agg merge input must be ListArray or StructArray")
            }
        }
    }
}
impl std::error::Error for ArrayAggMergeInputError {}
impl novarocks_failure_contract::FailureProjection for ArrayAggMergeInputError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(154).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ArrayAggMergeInputError {}
impl From<ArrayAggMergeInputError> for KernelFailure {
    fn from(source: ArrayAggMergeInputError) -> Self {
        crate::KernelFailure::from_domain(source)
    }
}
pub fn merge_input_list_array(
    array: &ArrayRef,
) -> Result<(&ListArray, Option<&StructArray>), ArrayAggMergeInputError> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok((list, None));
    }
    if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
        if struct_arr.num_columns() < 1 {
            return Err(ArrayAggMergeInputError::EmptyStruct {
                fields: struct_arr.num_columns(),
            });
        }
        let list = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(ArrayAggMergeInputError::FirstFieldNotList)?;
        return Ok((list, Some(struct_arr)));
    }
    Err(ArrayAggMergeInputError::NotListOrStruct)
}
/// Exact arena-free row computation. Each address came from an evaluated carrier.
pub fn update_row<
    'a,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = (&'a ArrayRef, usize)>,
>(
    state: &mut ArrayAggState<A>,
    kind: ArrayAggKind<'_>,
    mut columns: I,
    wrapped: bool,
    parent_null: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    work.step()?;
    if !wrapped {
        let (array, row) = columns
            .next()
            .ok_or_else(|| failure::contract("array_agg input missing"))?;
        if matches!(kind, ArrayAggKind::Unique)
            && let Some(list) = array.as_any().downcast_ref::<ListArray>()
        {
            if list.is_null(row) {
                return Ok(());
            }
            let values = list.values();
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            for idx in start..end {
                work.step()?;
                let value = tracked_scalar_from_array(values, idx, &state.allocator, work)?;
                append_value(state, value, true, work)?;
            }
            return Ok(());
        }
        let value = tracked_scalar_from_array(array, row, &state.allocator, work)?;
        return append_value(state, value, false, work);
    }
    let mut row_values = aggregate_vec_with_capacity(
        &state.allocator,
        columns.len(),
        "reserve array_agg input row",
        work,
    )?;
    for (col, row) in columns {
        work.step()?;
        if parent_null {
            row_values.push(None)
        } else {
            row_values.push(tracked_scalar_from_array(col, row, &state.allocator, work)?)
        }
    }
    state.push_row(row_values, work)
}
pub fn merge_row<A: ScalarStateAllocator>(
    state: &mut ArrayAggState<A>,
    kind: ArrayAggKind<'_>,
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    let (first_list, struct_arr) = merge_input_list_array(array)?;
    let first_values = first_list.values();
    let first_offsets = first_list.value_offsets();
    work.step()?;
    if struct_arr.map(|s| s.is_null(row)).unwrap_or(false) || first_list.is_null(row) {
        return Ok(());
    }
    let start = first_offsets[row] as usize;
    let end = first_offsets[row + 1] as usize;
    for idx in start..end {
        work.step()?;
        if matches!(kind, ArrayAggKind::Unique) {
            let value = tracked_scalar_from_array(first_values, idx, &state.allocator, work)?;
            append_value(state, value, true, work)?;
            continue;
        }
        if let Some(struct_arr) = struct_arr {
            let mut row_values = aggregate_vec_with_capacity(
                &state.allocator,
                struct_arr.num_columns(),
                "reserve array_agg merge row",
                work,
            )?;
            for (col_idx, col) in struct_arr.columns().iter().enumerate() {
                work.step()?;
                let list = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    failure::contract(format_args!(
                        "array_agg merge struct field[{}] must be ListArray",
                        col_idx
                    ))
                })?;
                if list.is_null(row) {
                    row_values.push(None);
                    continue;
                }
                let offsets = list.value_offsets();
                if offsets[row] != first_offsets[row] || offsets[row + 1] != first_offsets[row + 1]
                {
                    return Err(failure::contract(format_args!(
                        "array_agg merge struct field[{}] offsets mismatch",
                        col_idx
                    )));
                }
                row_values.push(tracked_scalar_from_array(
                    list.values(),
                    idx,
                    &state.allocator,
                    work,
                )?);
            }
            state.push_row(row_values, work)?;
        } else {
            let value = tracked_scalar_from_array(first_values, idx, &state.allocator, work)?;
            append_value(state, value, false, work)?;
        }
    }
    Ok(())
}

pub fn build_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s ArrayAggState<A>>,
>(
    spec: &ArrayAggConfig<'_>,
    states: I,
    output_intermediate: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure> {
    let target_type = if output_intermediate {
        &spec.intermediate_type
    } else {
        &spec.output_type
    };
    match target_type {
        DataType::List(list_field) => {
            let mut flattened = Vec::<Option<ArrayAggValue>>::new();
            let mut out_offsets = Vec::with_capacity(states.len() + 1);
            out_offsets.push(0_i32);
            let mut current: i32 = 0;
            for state in states {
                work.step()?;
                let values = if output_intermediate {
                    state
                        .rows
                        .iter()
                        .map(|row| {
                            work.step()?;
                            row.first()
                                .and_then(Option::as_ref)
                                .map(|v| array_value_from_tracked(v, work))
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    extract_final_values(spec, state, work)?
                };
                current = checked_list_output_offset(current, values.len())?;
                out_offsets.push(current);
                flattened.extend(values);
            }
            let values = build_scalar_array(list_field.data_type(), flattened, work)?;
            let output_list_field = reconcile_field_to_data_type(
                list_field,
                values.data_type(),
                "array_agg output list item",
                work,
            )?;
            let list_out = ListArray::try_new(
                output_list_field,
                OffsetBuffer::new(out_offsets.into()),
                values,
                None,
            )
            .map_err(failure::constructor)?;
            Ok(Arc::new(list_out) as ArrayRef)
        }
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                failure::contract("array_agg struct target type must contain at least 1 field")
            })?;
            let DataType::List(first_list_field) = first.data_type() else {
                return Err(failure::contract(format_args!(
                    "array_agg struct field[0] must be List, got {:?}",
                    first.data_type()
                )));
            };
            let mut flattened_by_col: Vec<Vec<Option<ArrayAggValue>>> =
                (0..fields.len()).map(|_| Vec::new()).collect();
            let mut out_offsets = Vec::with_capacity(states.len() + 1);
            out_offsets.push(0_i32);
            let mut current: i32 = 0;

            for state in states {
                work.step()?;
                let rows = if output_intermediate {
                    state
                        .rows
                        .iter()
                        .map(|row| {
                            work.step()?;
                            row.iter()
                                .map(|value| {
                                    work.step()?;
                                    value
                                        .as_ref()
                                        .map(|v| array_value_from_tracked(v, work))
                                        .transpose()
                                })
                                .collect::<Result<Vec<_>, KernelFailure>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    extract_final_values(spec, state, work)?
                        .into_iter()
                        .map(|value| vec![value])
                        .collect()
                };
                current = checked_list_output_offset(current, rows.len())?;
                out_offsets.push(current);
                for row in rows {
                    work.step()?;
                    for (col_idx, values) in flattened_by_col.iter_mut().enumerate() {
                        work.step()?;
                        values.push(row.get(col_idx).cloned().unwrap_or(None));
                    }
                }
            }

            let mut columns = Vec::with_capacity(fields.len());
            for (idx, field) in fields.iter().enumerate() {
                work.step()?;
                let DataType::List(list_field) = field.data_type() else {
                    return Err(failure::contract(format_args!(
                        "array_agg struct field[{}] must be List, got {:?}",
                        idx,
                        field.data_type()
                    )));
                };
                let values = if idx == 0 {
                    build_scalar_array(
                        first_list_field.data_type(),
                        flattened_by_col[idx].clone(),
                        work,
                    )?
                } else {
                    build_scalar_array(list_field.data_type(), flattened_by_col[idx].clone(), work)?
                };
                let output_list_field = reconcile_field_to_data_type(
                    list_field,
                    values.data_type(),
                    "array_agg intermediate list item",
                    work,
                )?;
                let list = ListArray::try_new(
                    output_list_field,
                    OffsetBuffer::new(out_offsets.clone().into()),
                    values,
                    None,
                )
                .map_err(failure::constructor)?;
                columns.push(Arc::new(list) as ArrayRef);
            }

            let output_fields = reconcile_fields_for_columns(
                fields,
                &columns,
                "array_agg intermediate struct output",
                work,
            )?;
            Ok(Arc::new(
                StructArray::try_new(output_fields, columns, None).map_err(failure::constructor)?,
            ) as ArrayRef)
        }
        other => Err(failure::contract(format_args!(
            "array_agg target type must be List or Struct(List...), got {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod merge_input_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn merge_carrier_refusal_retains_its_author_and_drops_schema_owners() {
        let field = Arc::new(Field::new(
            "ResourceExhausted numeric overflow",
            DataType::Int32,
            true,
        ));
        let weak = Arc::downgrade(&field);
        let input: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![field.clone()].into(),
                vec![Arc::new(Int32Array::from(vec![1]))],
                None,
            )
            .unwrap(),
        );
        let source = merge_input_list_array(&input).unwrap_err();
        assert_eq!(source, ArrayAggMergeInputError::FirstFieldNotList);
        let original = source.failure_fact();
        let failure = KernelFailure::from(source);
        assert_eq!(failure.failure_fact(), original);
        assert_eq!(original.class(), FailureClass::Invariant);
        assert!(
            matches!(failure, crate::KernelFailure::Source(ref cause) if std::error::Error::source(cause).unwrap().downcast_ref::<ArrayAggMergeInputError>().is_some())
        );
        drop(input);
        drop(field);
        assert!(weak.upgrade().is_none());
    }
}

#[cfg(test)]
mod typed_array_source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;

    #[test]
    fn actual_whole_array_numeric_refusal_keeps_the_integer_source() {
        for (ty, value) in [
            (DataType::Int8, 128),
            (DataType::Int16, 32768),
            (DataType::Int32, i64::from(i32::MAX) + 1),
        ] {
            let failure = build_scalar_array(
                &ty,
                vec![Some(ArrayAggValue::Int64(value))],
                &mut ScalarWork::new(None),
            )
            .unwrap_err();
            assert_eq!(
                failure.failure_fact(),
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            );
            let KernelFailure::Source(source) = failure else {
                panic!("actual numeric source required")
            };
            assert!(
                source
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<std::num::TryFromIntError>()
            );
        }
    }

    #[test]
    fn actual_list_constructor_refusal_releases_its_input_field() {
        let field = Arc::new(Field::new("ResourceExhausted", DataType::Int64, false));
        let weak = Arc::downgrade(&field);
        let ty = DataType::List(field.clone());
        let failure = build_scalar_array(
            &ty,
            vec![Some(ArrayAggValue::List(vec![None]))],
            &mut ScalarWork::new(None),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        let KernelFailure::Source(source) = failure else {
            panic!("actual Arrow source required")
        };
        assert!(
            source
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<arrow_schema::ArrowError>()
        );
        drop(ty);
        drop(field);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn real_unordered_sort_failure_precedes_optional_footer() {
        use crate::{KernelEvaluationControl, kernel_input::EvaluationCheckpoints};
        use std::{
            sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
            time::Duration,
        };
        struct NoFooter(AtomicUsize);
        impl KernelEvaluationControl for NoFooter {
            fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
                assert_eq!(
                    self.0.fetch_add(1, AtomicOrdering::SeqCst),
                    0,
                    "no footer after actual NaN refusal"
                );
                Ok(())
            }
            fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
                panic!("sort does not wait")
            }
        }
        let control = NoFooter(AtomicUsize::new(0));
        let mut checkpoints = EvaluationCheckpoints::new(&control);
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        let mut rows = vec![
            vec![None, Some(ArrayAggValue::Float64(f64::NAN))],
            vec![None, Some(ArrayAggValue::Float64(1.0))],
        ];
        let failure = sort_rows(&mut rows, &[true], &[false], &mut work).unwrap_err();
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::InvalidValue.failure_fact()
        );
        assert_eq!(control.0.load(AtomicOrdering::SeqCst), 1);
    }
}
