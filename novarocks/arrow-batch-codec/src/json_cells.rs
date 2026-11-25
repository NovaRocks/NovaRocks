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

//! Read JSON cells by their admitted logical declaration and physical rows.
//! Carrier geometry has already been admitted by the caller.

use arrow_array::{
    Array, FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, MapArray, StructArray, UnionArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{ValueKind, ValueLogicalType, ValueTypeRef};
use std::ops::Range;

use crate::CodecError;

/// The role owns both the checkpoint and the trust-boundary policy for each
/// borrowed value. Traversal never allocates or lends authority to the bytes.
pub trait JsonCellObserver {
    type Error;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn value(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;
}

/// Only visit values reachable from actual rows, preserving parent NULLs and
/// the original collection ranges. Physical bytes never supply a declaration.
pub fn visit_json_cells<O: JsonCellObserver>(
    array: &dyn Array,
    value: &ValueTypeRef,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    visit_range(array, value, 0..array.len(), observer)
}

fn contains_json<O: JsonCellObserver>(
    value: &ValueTypeRef,
    observer: &mut O,
) -> Result<bool, CodecError<O::Error>> {
    observer.step().map_err(CodecError::Observer)?;
    if value.domain() == Some(ValueLogicalType::Json) {
        return Ok(true);
    }
    for index in 0..value.child_count() {
        if contains_json(
            value.child_type_at(index).expect("admitted child"),
            observer,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn visit_range<O: JsonCellObserver>(
    array: &dyn Array,
    value: &ValueTypeRef,
    rows: Range<usize>,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    if rows.start > rows.end || rows.end > array.len() {
        return Err(CodecError::Invalid(
            "JSON collection range exceeds the child carrier",
        ));
    }
    if rows.is_empty() || !contains_json(value, observer)? {
        return Ok(());
    }
    for row in rows {
        visit_row(array, value, row, observer)?;
    }
    Ok(())
}

fn visit_row<O: JsonCellObserver>(
    array: &dyn Array,
    value: &ValueTypeRef,
    row: usize,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    observer.step().map_err(CodecError::Observer)?;
    if row >= array.len() {
        return Err(CodecError::Invalid("JSON ordinal exceeds its carrier"));
    }
    if array.is_null(row) {
        return Ok(());
    }
    if matches!(array.data_type(), DataType::Dictionary(..)) {
        return arrow_array::downcast_dictionary_array!(array => {
            match array.key(row) {
                Some(index) => visit_row(array.values().as_ref(), value, index, observer),
                None => Ok(()),
            }
        } _ => wrong_carrier());
    }
    if matches!(array.data_type(), DataType::RunEndEncoded(..)) {
        return arrow_array::downcast_run_array!(array => {
            visit_row(array.values().as_ref(), value, array.get_physical_index(row), observer)
        } _ => wrong_carrier());
    }
    match value.kind() {
        ValueKind::Nominal(ValueLogicalType::Json) => {
            let array = downcast::<LargeBinaryArray, _>(array)?;
            observer
                .value(array.value(row))
                .map_err(CodecError::Observer)
        }
        ValueKind::Struct(fields) => {
            let array = downcast::<StructArray, _>(array)?;
            for (child, field) in array.columns().iter().zip(fields) {
                visit_range(child.as_ref(), field.value_type(), row..row + 1, observer)?;
            }
            Ok(())
        }
        ValueKind::Union(fields) => {
            let array = downcast::<UnionArray, _>(array)?;
            let tag = array.type_id(row);
            for field in fields {
                observer.step().map_err(CodecError::Observer)?;
                if tag == field.type_id {
                    let index = array.value_offset(row);
                    return visit_range(
                        array.child(tag).as_ref(),
                        field.field.value_type(),
                        index..index + 1,
                        observer,
                    );
                }
            }
            wrong_carrier()
        }
        ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => {
            let list = list_row(array, row)?;
            visit_range(list.child, item, list.rows, observer)
        }
        ValueKind::Map { key, value } => {
            let array = downcast::<MapArray, _>(array)?;
            let offsets = array.value_offsets();
            let rows = offsets[row] as usize..offsets[row + 1] as usize;
            visit_range(array.keys().as_ref(), key, rows.clone(), observer)?;
            visit_range(array.values().as_ref(), value, rows, observer)
        }
        _ => Ok(()),
    }
}

struct ListRow<'a> {
    child: &'a dyn Array,
    rows: Range<usize>,
}

fn list_row<E>(array: &dyn Array, row: usize) -> Result<ListRow<'_>, CodecError<E>> {
    macro_rules! list {
        ($ty:ty) => {{
            let array = downcast::<$ty, E>(array)?;
            let offsets = array.value_offsets();
            ListRow {
                child: array.values().as_ref(),
                rows: offsets[row] as usize..offsets[row + 1] as usize,
            }
        }};
    }
    macro_rules! view {
        ($ty:ty) => {{
            let array = downcast::<$ty, E>(array)?;
            let start = array.value_offsets()[row] as usize;
            let end = start
                .checked_add(array.value_sizes()[row] as usize)
                .ok_or(CodecError::Invalid("JSON list-view extent overflow"))?;
            ListRow {
                child: array.values().as_ref(),
                rows: start..end,
            }
        }};
    }
    Ok(match array.data_type() {
        DataType::List(_) => list!(ListArray),
        DataType::LargeList(_) => list!(LargeListArray),
        DataType::ListView(_) => view!(ListViewArray),
        DataType::LargeListView(_) => view!(LargeListViewArray),
        DataType::FixedSizeList(_, len) => {
            let array = downcast::<FixedSizeListArray, E>(array)?;
            let start = (array.offset() + row)
                .checked_mul(*len as usize)
                .ok_or(CodecError::Invalid("JSON fixed list extent overflow"))?;
            let end = start
                .checked_add(*len as usize)
                .ok_or(CodecError::Invalid("JSON fixed list extent overflow"))?;
            ListRow {
                child: array.values().as_ref(),
                rows: start..end,
            }
        }
        _ => return wrong_carrier(),
    })
}

fn downcast<A: Array + 'static, E>(array: &dyn Array) -> Result<&A, CodecError<E>> {
    array.as_any().downcast_ref().ok_or_else(carrier_error)
}

fn wrong_carrier<T, E>() -> Result<T, CodecError<E>> {
    Err(carrier_error())
}

fn carrier_error<E>() -> CodecError<E> {
    CodecError::Invalid("JSON declaration differs from the admitted Arrow carrier")
}
