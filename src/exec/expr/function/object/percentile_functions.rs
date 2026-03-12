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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Float64Builder, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow::datatypes::DataType;

use crate::common::{largeint, percentile};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

pub fn eval_percentile_hash(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BinaryBuilder::new();
    for row in 0..input.len() {
        match numeric_value_at(&input, row, "percentile_hash")? {
            Some(value) => builder.append_value(percentile::encode_single_value(value)),
            None => builder.append_value(percentile::encode_empty_state()),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_percentile_empty(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for _ in 0..chunk.len() {
        builder.append_value(percentile::encode_empty_state());
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_percentile_approx_raw(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let payloads = arena.eval(args[0], chunk)?;
    let quantiles = arena.eval(args[1], chunk)?;
    let mut builder = Float64Builder::with_capacity(payloads.len());

    for row in 0..payloads.len() {
        let payload = payload_bytes_at(&payloads, row, "percentile_approx_raw")?;
        let quantile = numeric_value_at(&quantiles, row, "percentile_approx_raw")?;
        match (payload, quantile) {
            (Some(payload), Some(quantile)) => {
                let state = percentile::decode_state(payload)?;
                if let Some(value) = percentile::quantile_from_state(&state, Some(quantile)) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub(crate) fn numeric_value_at(
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<f64>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int8Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int16Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int32Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Float32Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Float64Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
            let divisor = 10_f64.powi(*scale as i32);
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64 / divisor))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast FixedSizeBinaryArray"))?;
            Ok((!arr.is_null(row)).then_some(largeint::value_at(arr, row)? as f64))
        }
        other => Err(format!(
            "{context}: unsupported numeric input type {:?}",
            other
        )),
    }
}

pub(crate) fn payload_bytes_at<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<&'a [u8]>, String> {
    match array.data_type() {
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast BinaryArray"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast StringArray"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row).as_bytes()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast LargeBinaryArray"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast LargeStringArray"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row).as_bytes()))
        }
        other => Err(format!(
            "{context}: unsupported percentile payload type {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{ArrayRef, BinaryArray, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn one_col_chunk(data_type: DataType, array: ArrayRef) -> Chunk {
        let field = Field::new("c1", data_type, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    #[test]
    fn percentile_hash_encodes_value_and_empty_state() {
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Int32);
        let input = Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef;
        let chunk = one_col_chunk(DataType::Int32, input);

        let out = eval_percentile_hash(&arena, arg, &[arg], &chunk).expect("eval");
        let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

        let decoded = percentile::decode_state(out.value(0)).expect("decode");
        assert_eq!(decoded.digest.count(), 1.0);
        assert_eq!(percentile::quantile_value(&decoded, 0.5), Some(7.0));
        let empty = percentile::decode_state(out.value(1)).expect("decode empty");
        assert_eq!(empty.digest.count(), 0.0);
        assert!(empty.quantiles.is_none());
    }
}
