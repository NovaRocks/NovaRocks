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
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SchemaValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampMicrosecond(i64),
}

pub(crate) type SchemaRow = HashMap<String, SchemaValue>;

pub(crate) fn normalize_column_key(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}

pub(crate) fn build_chunk(
    chunk_schema: ChunkSchemaRef,
    rows: &[SchemaRow],
) -> Result<Chunk, String> {
    let schema = chunk_schema.arrow_schema_ref();
    if rows.is_empty() {
        return Chunk::try_new_with_chunk_schema(RecordBatch::new_empty(schema), chunk_schema);
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let key = normalize_column_key(field.name());
        let nullable = field.is_nullable();
        let data_type = field.data_type();
        let array = match data_type {
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    match row.get(&key) {
                        Some(SchemaValue::Boolean(value)) => builder.append_value(*value),
                        None if nullable => builder.append_null(),
                        None => {
                            return Err(format!(
                                "schema scan column {} is non-nullable but row value is missing",
                                field.name()
                            ));
                        }
                        Some(other) => {
                            return Err(format!(
                                "schema scan value type mismatch for BOOLEAN column {}: {:?}",
                                field.name(),
                                other
                            ));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get(&key) {
                        Some(SchemaValue::Int32(value)) => builder.append_value(*value),
                        Some(SchemaValue::Int64(value)) => {
                            let converted = i32::try_from(*value).map_err(|_| {
                                format!(
                                    "schema scan value overflow for INT column {}: {}",
                                    field.name(),
                                    value
                                )
                            })?;
                            builder.append_value(converted);
                        }
                        None if nullable => builder.append_null(),
                        None => {
                            return Err(format!(
                                "schema scan column {} is non-nullable but row value is missing",
                                field.name()
                            ));
                        }
                        Some(other) => {
                            return Err(format!(
                                "schema scan value type mismatch for INT column {}: {:?}",
                                field.name(),
                                other
                            ));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get(&key) {
                        Some(SchemaValue::Int32(value)) => builder.append_value(*value as i64),
                        Some(SchemaValue::Int64(value)) => builder.append_value(*value),
                        None if nullable => builder.append_null(),
                        None => {
                            return Err(format!(
                                "schema scan column {} is non-nullable but row value is missing",
                                field.name()
                            ));
                        }
                        Some(other) => {
                            return Err(format!(
                                "schema scan value type mismatch for BIGINT column {}: {:?}",
                                field.name(),
                                other
                            ));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get(&key) {
                        Some(SchemaValue::Float64(value)) => builder.append_value(*value),
                        None if nullable => builder.append_null(),
                        None => {
                            return Err(format!(
                                "schema scan column {} is non-nullable but row value is missing",
                                field.name()
                            ));
                        }
                        Some(other) => {
                            return Err(format!(
                                "schema scan value type mismatch for DOUBLE column {}: {:?}",
                                field.name(),
                                other
                            ));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    match row.get(&key) {
                        Some(SchemaValue::TimestampMicrosecond(value)) => {
                            builder.append_value(*value)
                        }
                        None if nullable => builder.append_null(),
                        None => {
                            return Err(format!(
                                "schema scan column {} is non-nullable but row value is missing",
                                field.name()
                            ));
                        }
                        Some(other) => {
                            return Err(format!(
                                "schema scan value type mismatch for DATETIME column {}: {:?}",
                                field.name(),
                                other
                            ));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match row.get(&key) {
                        Some(SchemaValue::Utf8(value)) => builder.append_value(value),
                        None if nullable => builder.append_null(),
                        None => {
                            return Err(format!(
                                "schema scan column {} is non-nullable but row value is missing",
                                field.name()
                            ));
                        }
                        Some(other) => {
                            return Err(format!(
                                "schema scan value type mismatch for VARCHAR column {}: {:?}",
                                field.name(),
                                other
                            ));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            other => {
                return Err(format!(
                    "unsupported schema scan output data type for column {}: {:?}",
                    field.name(),
                    other
                ));
            }
        };
        columns.push(array);
    }

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|err| format!("build schema scan record batch failed: {}", err))?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}
