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

//! Arrow construction inside the original statement CPU creator. Payload Vecs
//! are quoted before allocation and adopted by Arrow without another copy.
use super::construction::{array_layout, control_error, layout_error};
use super::source::SystemTableRow;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_functions::selected_copy::{
    CopyConstructionError, preflight_flat_arrow_structure_construction,
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
        if values[column].is_some() {
            bits[row / 8] |= 1 << (row % 8);
        }
    }
    Ok(NullBuffer::new(BooleanBuffer::new(
        Buffer::from_vec(bits),
        0,
        rows.len(),
    )))
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
        let data_type = declared.value_type.data_type();
        let mut payload_bytes = 0usize;
        let mut has_nulls = false;
        for row in rows {
            work.step().map_err(control_error)?;
            if let Some(value) = &row[column] {
                payload_bytes = payload_bytes
                    .checked_add(value.len())
                    .ok_or_else(exhausted)?;
            } else {
                has_nulls = true;
            }
        }
        if has_nulls && !declared.value_type.nullable() {
            return Err(SystemTableError::Internal(
                "system-table row violates declared nullability".into(),
            ));
        }
        if !matches!(
            data_type,
            DataType::Utf8 | DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _)
        ) {
            return Err(SystemTableError::Internal(
                "unsupported system-table column construction".into(),
            ));
        }
        preflight_flat_arrow_structure_construction(data_type, has_nulls, creation, &mut |_| {
            work.step()
        })
        .map_err(|error| match error {
            CopyConstructionError::Creator(error) => control_error(error),
            CopyConstructionError::Resource(error) => layout_error(error),
            CopyConstructionError::Copy(error) => {
                SystemTableError::Internal(format!("system table structure model refused: {error}"))
            }
        })?;
        request_arc::<Field>(creation)?;
        request(creation, array_layout::<u8>(declared.name.len())?)?;
        // These registered flat declarations have no nested type allocation;
        // a timestamp timezone keeps its original Arc when the type is cloned.
        fields.push(Arc::new(
            declared
                .value_type
                .try_to_field(declared.name.clone())
                .map_err(|error| SystemTableError::Internal(error.to_string()))?,
        ));
        if has_nulls {
            request(
                creation,
                array_layout::<u8>(rows.len().checked_add(7).ok_or_else(exhausted)? / 8)?,
            )?;
        }
        let array: ArrayRef = match data_type {
            DataType::Utf8 => {
                i32::try_from(payload_bytes).map_err(|_| {
                    SystemTableError::InvalidRequest(
                        "system table batch exceeds Arrow UTF8 offset width".into(),
                    )
                })?;
                request(creation, array_layout::<u8>(payload_bytes)?)?;
                request(
                    creation,
                    array_layout::<i32>(rows.len().checked_add(1).ok_or_else(exhausted)?)?,
                )?;
                let mut values = exact_vec::<u8>(payload_bytes)?;
                let mut offsets = exact_vec::<i32>(rows.len() + 1)?;
                offsets.push(0);
                for row in rows {
                    work.step().map_err(control_error)?;
                    if let Some(value) = &row[column] {
                        values.extend_from_slice(value.as_bytes());
                    }
                    offsets.push(i32::try_from(values.len()).expect("preflight UTF8 extent"));
                }
                let nulls = if has_nulls {
                    Some(bitmap(rows, column)?)
                } else {
                    None
                };
                Arc::new(
                    StringArray::try_new(
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        Buffer::from_vec(values),
                        nulls,
                    )
                    .map_err(|error| {
                        SystemTableError::Internal(format!("build system-table UTF8: {error}"))
                    })?,
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, zone) => {
                if rows.iter().any(|row| row[column].is_some()) {
                    return Err(SystemTableError::Internal(
                        "system-table fresh time has no concrete value author".into(),
                    ));
                }
                request(creation, array_layout::<i64>(rows.len())?)?;
                let mut values = exact_vec::<i64>(rows.len())?;
                values.resize(rows.len(), 0);
                Arc::new(
                    TimestampNanosecondArray::new(
                        ScalarBuffer::from(values),
                        Some(bitmap(rows, column)?),
                    )
                    .with_timezone_opt(zone.clone()),
                )
            }
            _ => unreachable!("registered flat carrier preflight"),
        };
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
