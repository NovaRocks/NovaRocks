// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Arrow-only merge semantics for frozen StarRocks key-model batches.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use super::wire::{StorageColumn, StorageModel, StorageSchema};

pub(crate) fn merge_key_model_batches(
    model: StorageModel,
    schema: &StorageSchema,
    output_schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<RecordBatch, ConnectorError> {
    if model == StorageModel::Duplicate || model == StorageModel::Primary {
        return Err(unsupported(
            "StarRocks direct key-model merge requested for a non-merge model",
        ));
    }
    let specs = output_specs(schema, &output_schema, model)?;
    let keys = schema
        .columns
        .iter()
        .filter(|column| column.is_key)
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(corrupt(
            "StarRocks direct key-model schema has no key columns",
        ));
    }
    let mut groups = Vec::<Vec<Cell>>::new();
    let mut index_by_key = BTreeMap::<Vec<u8>, usize>::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let key = encode_key(batch, &keys, row)?;
            if let Some(index) = index_by_key.get(&key).copied() {
                merge_row(batch, row, &specs, &mut groups[index])?;
            } else {
                let values = specs
                    .iter()
                    .map(|spec| cell_from_batch(batch, &spec.name, row))
                    .collect::<Result<Vec<_>, _>>()?;
                index_by_key.insert(key, groups.len());
                groups.push(values);
            }
        }
    }
    let arrays = specs
        .iter()
        .enumerate()
        .map(|(index, spec)| build_array(spec, groups.iter().map(|row| &row[index])))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(output_schema, arrays)
        .map_err(|_| corrupt("StarRocks direct key-model output batch is invalid"))
}

#[derive(Clone)]
struct Spec {
    name: String,
    data_type: DataType,
    is_key: bool,
    op: Option<AggOp>,
}

fn output_specs(
    storage: &StorageSchema,
    output: &SchemaRef,
    model: StorageModel,
) -> Result<Vec<Spec>, ConnectorError> {
    output
        .fields()
        .iter()
        .map(|field| {
            let column = storage
                .columns
                .iter()
                .find(|column| column.name == field.name().as_str())
                .ok_or_else(|| {
                    corrupt(
                        "StarRocks direct key-model output column is absent from storage schema",
                    )
                })?;
            let op = if column.is_key {
                None
            } else if model == StorageModel::Unique {
                Some(AggOp::Replace)
            } else {
                Some(AggOp::parse(column.aggregation.as_deref())?)
            };
            Ok(Spec {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                is_key: column.is_key,
                op,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum AggOp {
    Sum,
    Min,
    Max,
    Replace,
    ReplaceIfNotNull,
}

impl AggOp {
    fn parse(value: Option<&str>) -> Result<Self, ConnectorError> {
        match value.map(str::trim).map(str::to_ascii_uppercase).as_deref() {
            Some("SUM") => Ok(Self::Sum),
            Some("MIN") => Ok(Self::Min),
            Some("MAX") => Ok(Self::Max),
            Some("REPLACE") | Some("NONE") => Ok(Self::Replace),
            Some("REPLACE_IF_NOT_NULL") => Ok(Self::ReplaceIfNotNull),
            _ => Err(unsupported(
                "StarRocks direct aggregate operation is unsupported",
            )),
        }
    }
}

fn merge_row(
    batch: &RecordBatch,
    row: usize,
    specs: &[Spec],
    state: &mut [Cell],
) -> Result<(), ConnectorError> {
    for (index, spec) in specs.iter().enumerate() {
        if spec.is_key {
            continue;
        }
        let incoming = cell_from_batch(batch, &spec.name, row)?;
        let operation = spec.op.ok_or_else(|| {
            corrupt("StarRocks direct key-model non-key column is missing its merge operation")
        })?;
        merge_cell(&mut state[index], incoming, operation)?;
    }
    Ok(())
}

fn merge_cell(state: &mut Cell, incoming: Cell, op: AggOp) -> Result<(), ConnectorError> {
    match op {
        AggOp::Replace => *state = incoming,
        AggOp::ReplaceIfNotNull if !matches!(incoming, Cell::Null) => *state = incoming,
        AggOp::ReplaceIfNotNull => {}
        AggOp::Sum => *state = add_cells(state, &incoming)?,
        AggOp::Min => *state = ordered_cells(state, &incoming, true)?,
        AggOp::Max => *state = ordered_cells(state, &incoming, false)?,
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum Cell {
    Null,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Date32(i32),
    TimestampMicros(i64),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Text(String),
    Binary(Vec<u8>),
}

fn cell_from_batch(batch: &RecordBatch, name: &str, row: usize) -> Result<Cell, ConnectorError> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| corrupt("StarRocks direct key-model column is absent from decoded batch"))?;
    if array.is_null(row) {
        return Ok(Cell::Null);
    }
    let mismatch = || corrupt("StarRocks direct key-model Arrow type mismatch");
    match array.data_type() {
        DataType::Int8 => Ok(Cell::I8(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Int16 => Ok(Cell::I16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Int32 => Ok(Cell::I32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Int64 => Ok(Cell::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Float32 => Ok(Cell::F32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Float64 => Ok(Cell::F64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Boolean => Ok(Cell::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Date32 => Ok(Cell::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Cell::TimestampMicros(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(mismatch)?
                .value(row),
        )),
        DataType::Decimal128(precision, scale) => Ok(Cell::Decimal128 {
            value: array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(mismatch)?
                .value(row),
            precision: *precision,
            scale: *scale,
        }),
        DataType::Utf8 => Ok(Cell::Text(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mismatch)?
                .value(row)
                .to_string(),
        )),
        DataType::Binary => Ok(Cell::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mismatch)?
                .value(row)
                .to_vec(),
        )),
        _ => Err(unsupported(
            "StarRocks direct key-model scalar type is unsupported",
        )),
    }
}

fn encode_key(
    batch: &RecordBatch,
    columns: &[&StorageColumn],
    row: usize,
) -> Result<Vec<u8>, ConnectorError> {
    let mut encoded = Vec::new();
    for column in columns {
        let value = cell_from_batch(batch, &column.name, row)?;
        encode_cell(&value, &mut encoded);
    }
    Ok(encoded)
}

fn encode_cell(cell: &Cell, output: &mut Vec<u8>) {
    match cell {
        Cell::Null => output.push(0),
        Cell::I8(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::I16(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::I32(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::I64(value) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::F32(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Cell::F64(value) => {
            output.push(6);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Cell::Bool(value) => {
            output.push(7);
            output.push(u8::from(*value));
        }
        Cell::Date32(value) => {
            output.push(8);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::TimestampMicros(value) => {
            output.push(9);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::Decimal128 {
            value,
            precision,
            scale,
        } => {
            output.push(10);
            output.push(*precision);
            output.extend_from_slice(&scale.to_be_bytes());
            output.extend_from_slice(&value.to_be_bytes());
        }
        Cell::Text(value) => encode_bytes(11, value.as_bytes(), output),
        Cell::Binary(value) => encode_bytes(12, value, output),
    }
}

fn encode_bytes(tag: u8, bytes: &[u8], output: &mut Vec<u8>) {
    output.push(tag);
    output.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    output.extend_from_slice(bytes);
}

fn add_cells(left: &Cell, right: &Cell) -> Result<Cell, ConnectorError> {
    match (left, right) {
        (Cell::Null, value) | (value, Cell::Null) => Ok(value.clone()),
        (Cell::I8(a), Cell::I8(b)) => a
            .checked_add(*b)
            .map(Cell::I8)
            .ok_or_else(|| corrupt("StarRocks direct aggregate SUM overflows")),
        (Cell::I16(a), Cell::I16(b)) => a
            .checked_add(*b)
            .map(Cell::I16)
            .ok_or_else(|| corrupt("StarRocks direct aggregate SUM overflows")),
        (Cell::I32(a), Cell::I32(b)) => a
            .checked_add(*b)
            .map(Cell::I32)
            .ok_or_else(|| corrupt("StarRocks direct aggregate SUM overflows")),
        (Cell::I64(a), Cell::I64(b)) => a
            .checked_add(*b)
            .map(Cell::I64)
            .ok_or_else(|| corrupt("StarRocks direct aggregate SUM overflows")),
        (Cell::F32(a), Cell::F32(b)) => Ok(Cell::F32(*a + *b)),
        (Cell::F64(a), Cell::F64(b)) => Ok(Cell::F64(*a + *b)),
        (
            Cell::Decimal128 {
                value: a,
                precision,
                scale,
            },
            Cell::Decimal128 {
                value: b,
                precision: right_precision,
                scale: right_scale,
            },
        ) if precision == right_precision && scale == right_scale => a
            .checked_add(*b)
            .map(|value| Cell::Decimal128 {
                value,
                precision: *precision,
                scale: *scale,
            })
            .ok_or_else(|| corrupt("StarRocks direct aggregate DECIMAL SUM overflows")),
        _ => Err(corrupt(
            "StarRocks direct aggregate SUM has incompatible column types",
        )),
    }
}

fn ordered_cells(left: &Cell, right: &Cell, min: bool) -> Result<Cell, ConnectorError> {
    if matches!(left, Cell::Null) {
        return Ok(right.clone());
    }
    if matches!(right, Cell::Null) {
        return Ok(left.clone());
    }
    let order = compare_cells(left, right)?;
    Ok(if (min && order.is_gt()) || (!min && order.is_lt()) {
        right.clone()
    } else {
        left.clone()
    })
}

fn compare_cells(left: &Cell, right: &Cell) -> Result<std::cmp::Ordering, ConnectorError> {
    macro_rules! cmp {
        ($a:ident, $b:ident) => {
            Ok($a.cmp($b))
        };
    }
    match (left, right) {
        (Cell::I8(a), Cell::I8(b)) => cmp!(a, b),
        (Cell::I16(a), Cell::I16(b)) => cmp!(a, b),
        (Cell::I32(a), Cell::I32(b)) => cmp!(a, b),
        (Cell::I64(a), Cell::I64(b)) => cmp!(a, b),
        (Cell::F32(a), Cell::F32(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| corrupt("StarRocks direct aggregate ordering does not support NaN")),
        (Cell::F64(a), Cell::F64(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| corrupt("StarRocks direct aggregate ordering does not support NaN")),
        (Cell::Bool(a), Cell::Bool(b)) => cmp!(a, b),
        (Cell::Date32(a), Cell::Date32(b)) => cmp!(a, b),
        (Cell::TimestampMicros(a), Cell::TimestampMicros(b)) => cmp!(a, b),
        (
            Cell::Decimal128 {
                value: a,
                precision,
                scale,
            },
            Cell::Decimal128 {
                value: b,
                precision: right_precision,
                scale: right_scale,
            },
        ) if precision == right_precision && scale == right_scale => cmp!(a, b),
        (Cell::Text(a), Cell::Text(b)) => cmp!(a, b),
        (Cell::Binary(a), Cell::Binary(b)) => cmp!(a, b),
        _ => Err(corrupt(
            "StarRocks direct aggregate ordering has incompatible column types",
        )),
    }
}

fn build_array<'a>(
    spec: &Spec,
    values: impl Iterator<Item = &'a Cell>,
) -> Result<ArrayRef, ConnectorError> {
    let values = values.collect::<Vec<_>>();
    macro_rules! primitive {
        ($cell:ident, $array:ident, $ty:ty) => {{
            let mut out = Vec::<Option<$ty>>::with_capacity(values.len());
            for cell in &values {
                match cell {
                    Cell::Null => out.push(None),
                    Cell::$cell(value) => out.push(Some(*value)),
                    _ => {
                        return Err(corrupt(
                            "StarRocks direct key-model output cell type differs from Arrow schema",
                        ));
                    }
                }
            }
            Ok(Arc::new($array::from(out)) as ArrayRef)
        }};
    }
    match spec.data_type {
        DataType::Int8 => primitive!(I8, Int8Array, i8),
        DataType::Int16 => primitive!(I16, Int16Array, i16),
        DataType::Int32 => primitive!(I32, Int32Array, i32),
        DataType::Int64 => primitive!(I64, Int64Array, i64),
        DataType::Float32 => primitive!(F32, Float32Array, f32),
        DataType::Float64 => primitive!(F64, Float64Array, f64),
        DataType::Boolean => primitive!(Bool, BooleanArray, bool),
        DataType::Date32 => primitive!(Date32, Date32Array, i32),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            primitive!(TimestampMicros, TimestampMicrosecondArray, i64)
        }
        DataType::Decimal128(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for cell in values {
                match cell {
                    Cell::Null => out.push(None),
                    Cell::Decimal128 {
                        value,
                        precision: actual_precision,
                        scale: actual_scale,
                    } if *actual_precision == precision && *actual_scale == scale => {
                        out.push(Some(*value))
                    }
                    _ => {
                        return Err(corrupt(
                            "StarRocks direct key-model output cell type differs from Arrow schema",
                        ));
                    }
                }
            }
            let array = Decimal128Array::from(out)
                .with_precision_and_scale(precision, scale)
                .map_err(|_| corrupt("StarRocks direct key-model DECIMAL exceeds precision"))?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => {
            let mut out = Vec::new();
            for cell in values {
                match cell {
                    Cell::Null => out.push(None),
                    Cell::Text(value) => out.push(Some(value.as_str())),
                    _ => {
                        return Err(corrupt(
                            "StarRocks direct key-model output cell type differs from Arrow schema",
                        ));
                    }
                }
            }
            Ok(Arc::new(StringArray::from(out)))
        }
        DataType::Binary => {
            let mut out = BinaryBuilder::new();
            for cell in values {
                match cell {
                    Cell::Null => out.append_null(),
                    Cell::Binary(value) => out.append_value(value),
                    _ => {
                        return Err(corrupt(
                            "StarRocks direct key-model output cell type differs from Arrow schema",
                        ));
                    }
                }
            }
            Ok(Arc::new(out.finish()))
        }
        _ => Err(unsupported(
            "StarRocks direct key-model output type is unsupported",
        )),
    }
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};

    fn column(name: &str, key: bool, aggregation: Option<&str>) -> StorageColumn {
        StorageColumn {
            unique_id: if key { 1 } else { 2 },
            name: name.to_string(),
            physical_type: "BIGINT".to_string(),
            is_key: key,
            aggregation: aggregation.map(str::to_string),
            nullable: false,
            default_value: None,
            precision: None,
            scale: None,
            length: None,
            children: Vec::new(),
        }
    }
    fn schema(model: StorageModel, aggregation: Option<&str>) -> StorageSchema {
        StorageSchema {
            id: None,
            model,
            columns: vec![
                column("id", true, None),
                column("value", false, aggregation),
            ],
        }
    }
    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }
    fn output() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    #[test]
    fn unique_keys_replace_later_value_across_batches() {
        let batch = merge_key_model_batches(
            StorageModel::Unique,
            &schema(StorageModel::Unique, None),
            output(),
            &[batch(&[(1, 10), (2, 20)]), batch(&[(1, 30)])],
        )
        .unwrap();
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[30, 20]
        );
    }
    #[test]
    fn aggregate_keys_merge_sum_across_batches() {
        let batch = merge_key_model_batches(
            StorageModel::Aggregate,
            &schema(StorageModel::Aggregate, Some("SUM")),
            output(),
            &[batch(&[(1, 10), (2, 20)]), batch(&[(1, 30)])],
        )
        .unwrap();
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[40, 20]
        );
    }

    #[test]
    fn aggregate_keys_merge_decimal_values_with_frozen_scale() {
        let output = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Decimal128(10, 2), false),
        ]));
        let batch = |values: &[i128]| {
            RecordBatch::try_new(
                Arc::clone(&output),
                vec![
                    Arc::new(Int64Array::from(vec![1_i64; values.len()])),
                    Arc::new(
                        Decimal128Array::from(values.to_vec())
                            .with_precision_and_scale(10, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap()
        };
        let merged = merge_key_model_batches(
            StorageModel::Aggregate,
            &schema(StorageModel::Aggregate, Some("SUM")),
            Arc::clone(&output),
            &[batch(&[125]), batch(&[75])],
        )
        .unwrap();
        assert_eq!(
            merged
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            200
        );
    }

    #[test]
    fn aggregate_keys_preserve_each_frozen_value_merge_operation() {
        let schema = StorageSchema {
            id: None,
            model: StorageModel::Aggregate,
            columns: vec![
                column("id", true, None),
                column("sum_value", false, Some("SUM")),
                column("min_value", false, Some("MIN")),
                column("max_value", false, Some("MAX")),
                column("replace_value", false, Some("REPLACE")),
                column("replace_non_null", false, Some("REPLACE_IF_NOT_NULL")),
            ],
        };
        let output = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sum_value", DataType::Int64, false),
            Field::new("min_value", DataType::Int64, false),
            Field::new("max_value", DataType::Int64, false),
            Field::new("replace_value", DataType::Int64, false),
            Field::new("replace_non_null", DataType::Int64, true),
        ]));
        let input = |values: Vec<Vec<Option<i64>>>| {
            RecordBatch::try_new(
                output.clone(),
                values
                    .into_iter()
                    .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
                    .collect(),
            )
            .unwrap()
        };
        let merged = merge_key_model_batches(
            StorageModel::Aggregate,
            &schema,
            output.clone(),
            &[
                input(vec![
                    vec![Some(1)],
                    vec![Some(10)],
                    vec![Some(5)],
                    vec![Some(5)],
                    vec![Some(1)],
                    vec![Some(8)],
                ]),
                input(vec![
                    vec![Some(1)],
                    vec![Some(20)],
                    vec![Some(3)],
                    vec![Some(7)],
                    vec![Some(2)],
                    vec![None],
                ]),
            ],
        )
        .unwrap();
        let values = |index| {
            merged
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(
            (values(1), values(2), values(3), values(4), values(5)),
            (30, 3, 7, 2, 8)
        );
    }
}
