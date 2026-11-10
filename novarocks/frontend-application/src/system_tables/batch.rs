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

//! Arrow construction called only inside the original statement CPU creator.
use super::source::SystemTableRow;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_query_application::system_tables::{SystemTableError, SystemTableSnapshot};
use novarocks_sql::system_table::SystemTableDeclaration;
use std::sync::Arc;

pub(crate) fn build_system_table_batch(
    declaration: &SystemTableDeclaration,
    required_columns: &[usize],
    rows: Vec<SystemTableRow>,
) -> Result<RecordBatch, SystemTableError> {
    if rows.is_empty() {
        return Err(SystemTableError::Internal(
            "empty system-table batches are not snapshot data".into(),
        ));
    }
    if rows.iter().any(|row| row.len() != required_columns.len()) {
        return Err(SystemTableError::Internal(
            "system-table row width differs from requested projection".into(),
        ));
    }
    if required_columns.is_empty() {
        return SystemTableSnapshot::empty_projection_batch(rows.len());
    }
    let mut fields = Vec::with_capacity(required_columns.len());
    let mut arrays = Vec::<ArrayRef>::with_capacity(required_columns.len());
    for (column, &ordinal) in required_columns.iter().enumerate() {
        let declared = declaration.columns.get(ordinal).ok_or_else(|| {
            SystemTableError::InvalidRequest("invalid system-table projection ordinal".into())
        })?;
        let data_type = declared.value_type.data_type();
        fields.push(Field::new(
            &declared.name,
            data_type.clone(),
            declared.value_type.nullable(),
        ));
        let array: ArrayRef = match data_type {
            DataType::Utf8 => Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row[column].as_deref()),
            )),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, zone)
                if rows.iter().all(|row| row[column].is_none()) =>
            {
                Arc::new(
                    TimestampNanosecondArray::from(vec![None::<i64>; rows.len()])
                        .with_timezone_opt(zone.clone()),
                )
            }
            _ => {
                return Err(SystemTableError::Internal(
                    "unsupported system-table column construction".into(),
                ));
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| SystemTableError::Internal(format!("build system-table batch: {error}")))
}
