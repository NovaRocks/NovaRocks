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

//! Arrow construction inside the original statement CPU creator. Every payload
//! and retained Arrow node is funded before allocation, including List children.
use super::construction::{array_layout, control_error, layout_error, validate_value};
use super::source::{SystemTableRow, SystemValue};
use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, ListArray, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_functions::selected_copy::{
    CopyConstructionError, preflight_arrow_node_structure_construction,
};
use novarocks_query_application::system_tables::SystemTableError;
use novarocks_sql::system_table::SystemTableDeclaration;
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueSchemaCreation,
};
use std::{alloc::Layout, sync::Arc};
fn exhausted() -> SystemTableError {
    control_error(CompileControlError::ResourceExhausted)
}
fn request(
    creation: &mut ValueSchemaCreation<'_, CompileControlError>,
    layout: Layout,
) -> Result<(), SystemTableError> {
    if layout.size() != 0 {
        creation.request_layout(layout).map_err(control_error)?;
    }
    Ok(())
}
fn request_arc<T>(
    creation: &mut ValueSchemaCreation<'_, CompileControlError>,
) -> Result<(), SystemTableError> {
    creation
        .request_layout(arc_layout(Layout::new::<T>()).map_err(layout_error)?)
        .map_err(control_error)
}
fn exact_vec<T>(count: usize) -> Result<Vec<T>, SystemTableError> {
    let mut result = Vec::new();
    result.try_reserve_exact(count).map_err(|_| exhausted())?;
    Ok(result)
}
fn bitmap(rows: &[SystemTableRow], column: usize) -> Result<NullBuffer, SystemTableError> {
    let count = rows.len().checked_add(7).ok_or_else(exhausted)? / 8;
    let mut bits = exact_vec::<u8>(count)?;
    bits.resize(count, 0);
    for (row, values) in rows.iter().enumerate() {
        if !values[column].is_null() {
            bits[row / 8] |= 1 << (row % 8);
        }
    }
    Ok(NullBuffer::new(BooleanBuffer::new(
        Buffer::from_vec(bits),
        0,
        rows.len(),
    )))
}
fn node(
    ty: &DataType,
    nulls: bool,
    creation: &mut ValueSchemaCreation<'_, CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SystemTableError> {
    preflight_arrow_node_structure_construction(ty, nulls, creation, &mut |_| work.step()).map_err(
        |error| match error {
            CopyConstructionError::Creator(error) => control_error(error),
            CopyConstructionError::Resource(error) => layout_error(error),
            CopyConstructionError::Copy(error) => {
                SystemTableError::Internal(format!("system table structure model refused: {error}"))
            }
        },
    )
}
fn strings<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    count: usize,
    bytes: usize,
    nulls: Option<NullBuffer>,
    creation: &mut ValueSchemaCreation<'_, CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<StringArray, SystemTableError> {
    i32::try_from(bytes).map_err(|_| {
        SystemTableError::InvalidRequest(
            "system table batch exceeds Arrow UTF8 offset width".into(),
        )
    })?;
    request(creation, array_layout::<u8>(bytes)?)?;
    request(
        creation,
        array_layout::<i32>(count.checked_add(1).ok_or_else(exhausted)?)?,
    )?;
    let mut payload = exact_vec::<u8>(bytes)?;
    let mut offsets = exact_vec::<i32>(count + 1)?;
    offsets.push(0);
    for value in values {
        work.step().map_err(control_error)?;
        if let Some(value) = value {
            payload.extend_from_slice(value.as_bytes());
        }
        offsets.push(i32::try_from(payload.len()).expect("preflight UTF8 extent"));
    }
    StringArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(payload),
        nulls,
    )
    .map_err(|error| SystemTableError::Internal(format!("build system-table UTF8: {error}")))
}
pub(crate) fn build_system_table_batch(
    declaration: &SystemTableDeclaration,
    required_columns: &[usize],
    rows: &[SystemTableRow],
    creation: &mut ValueSchemaCreation<'_, CompileControlError>,
    control: &dyn PureCompileControl,
) -> Result<RecordBatch, SystemTableError> {
    if rows.is_empty() || rows.iter().any(|row| row.len() != required_columns.len()) {
        return Err(SystemTableError::Internal(
            "system-table batch has empty rows or inconsistent width".into(),
        ));
    }
    let mut work =
        CompileCheckpoints::try_new(control, CompilePhase::LowerProgram).map_err(control_error)?;
    request(creation, array_layout::<FieldRef>(required_columns.len())?)?;
    creation
        .request_layout(
            arc_layout(array_layout::<FieldRef>(required_columns.len())?).map_err(layout_error)?,
        )
        .map_err(control_error)?;
    request(creation, array_layout::<ArrayRef>(required_columns.len())?)?;
    request_arc::<Schema>(creation)?;
    let mut fields = exact_vec::<FieldRef>(required_columns.len())?;
    let mut arrays = exact_vec::<ArrayRef>(required_columns.len())?;
    for (column, &ordinal) in required_columns.iter().enumerate() {
        work.step().map_err(control_error)?;
        let declared = declaration.columns.get(ordinal).ok_or_else(|| {
            SystemTableError::InvalidRequest("invalid system-table projection ordinal".into())
        })?;
        let ty = declared.value_type.data_type();
        let mut bytes = 0usize;
        let mut items = 0usize;
        let mut has_nulls = false;
        for row in rows {
            work.step().map_err(control_error)?;
            validate_value(row[column].as_ref(), ty, declared.value_type.nullable())?;
            match &row[column] {
                SystemValue::Null => has_nulls = true,
                SystemValue::Utf8(value) => {
                    bytes = bytes.checked_add(value.len()).ok_or_else(exhausted)?
                }
                SystemValue::Utf8List(values) => {
                    items = items.checked_add(values.len()).ok_or_else(exhausted)?;
                    for value in values {
                        work.step().map_err(control_error)?;
                        bytes = bytes.checked_add(value.len()).ok_or_else(exhausted)?;
                    }
                }
                _ => {}
            }
        }
        node(ty, has_nulls, creation, &mut work)?;
        request_arc::<Field>(creation)?;
        request(creation, array_layout::<u8>(declared.name.len())?)?;
        // FVT materialization constructs a new nested Field; fund that actual
        // child before calling the same author used for every declared type.
        if let DataType::List(child) = ty {
            request_arc::<Field>(creation)?;
            request(creation, array_layout::<u8>(child.name().len())?)?;
        }
        let field = Arc::new(
            declared
                .value_type
                .try_to_field(declared.name.clone())
                .map_err(|error| SystemTableError::Internal(error.to_string()))?,
        );
        let nulls = if has_nulls {
            request(
                creation,
                array_layout::<u8>(rows.len().checked_add(7).ok_or_else(exhausted)? / 8)?,
            )?;
            Some(bitmap(rows, column)?)
        } else {
            None
        };
        let array: ArrayRef = match ty {
            DataType::Utf8 => Arc::new(strings(
                rows.iter().map(|r| match &r[column] {
                    SystemValue::Utf8(v) => Some(v.as_str()),
                    _ => None,
                }),
                rows.len(),
                bytes,
                nulls,
                creation,
                &mut work,
            )?),
            DataType::Int64
            | DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Nanosecond, _) => {
                request(creation, array_layout::<i64>(rows.len())?)?;
                let mut values = exact_vec::<i64>(rows.len())?;
                for row in rows {
                    work.step().map_err(control_error)?;
                    values.push(match row[column] {
                        SystemValue::Int64(v) | SystemValue::TimestampMillis(v) => v,
                        _ => 0,
                    });
                }
                match ty {
                    DataType::Int64 => Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls)),
                    DataType::Timestamp(TimeUnit::Millisecond, zone) => Arc::new(
                        TimestampMillisecondArray::new(ScalarBuffer::from(values), nulls)
                            .with_timezone_opt(zone.clone()),
                    ),
                    DataType::Timestamp(TimeUnit::Nanosecond, zone) => Arc::new(
                        TimestampNanosecondArray::new(ScalarBuffer::from(values), nulls)
                            .with_timezone_opt(zone.clone()),
                    ),
                    _ => unreachable!(),
                }
            }
            DataType::Boolean => {
                let count = rows.len().checked_add(7).ok_or_else(exhausted)? / 8;
                request(creation, array_layout::<u8>(count)?)?;
                let mut values = exact_vec::<u8>(count)?;
                values.resize(count, 0);
                for (i, row) in rows.iter().enumerate() {
                    work.step().map_err(control_error)?;
                    if matches!(row[column], SystemValue::Boolean(true)) {
                        values[i / 8] |= 1 << (i % 8);
                    }
                }
                Arc::new(BooleanArray::new(
                    BooleanBuffer::new(Buffer::from_vec(values), 0, rows.len()),
                    nulls,
                ))
            }
            DataType::List(_) => {
                i32::try_from(items).map_err(|_| {
                    SystemTableError::InvalidRequest(
                        "system table batch exceeds Arrow List offset width".into(),
                    )
                })?;
                request(
                    creation,
                    array_layout::<i32>(rows.len().checked_add(1).ok_or_else(exhausted)?)?,
                )?;
                node(&DataType::Utf8, false, creation, &mut work)?;
                let mut offsets = exact_vec::<i32>(rows.len() + 1)?;
                offsets.push(0);
                let mut count = 0;
                for row in rows {
                    work.step().map_err(control_error)?;
                    if let SystemValue::Utf8List(values) = &row[column] {
                        count += values.len();
                    }
                    offsets.push(i32::try_from(count).expect("preflight List extent"));
                }
                let values = strings(
                    rows.iter()
                        .flat_map(|row| match &row[column] {
                            SystemValue::Utf8List(values) => values.as_slice(),
                            _ => &[],
                        })
                        .map(|s| Some(s.as_str())),
                    items,
                    bytes,
                    None,
                    creation,
                    &mut work,
                )?;
                let DataType::List(child) = field.data_type() else {
                    unreachable!()
                };
                Arc::new(
                    ListArray::try_new(
                        child.clone(),
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        Arc::new(values),
                        nulls,
                    )
                    .map_err(|error| {
                        SystemTableError::Internal(format!("build system-table List: {error}"))
                    })?,
                )
            }
            _ => {
                return Err(SystemTableError::Internal(
                    "unsupported system-table column construction".into(),
                ));
            }
        };
        fields.push(field);
        arrays.push(array);
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .map_err(|error| SystemTableError::Internal(format!("build system-table batch: {error}")))?;
    work.finish().map_err(control_error)?;
    Ok(batch)
}
