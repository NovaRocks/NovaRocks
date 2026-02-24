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
    Array, ArrayRef, BinaryBuilder, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryBuilder, LargeListArray, LargeStringBuilder, ListArray, StringBuilder, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

fn decode_codes_to_output(
    codes: &[Option<i32>],
    dict: &HashMap<i32, Vec<u8>>,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let code_to_bytes = |code: Option<i32>| -> Result<Option<&Vec<u8>>, String> {
        if let Some(code) = code {
            if let Some(bytes) = dict.get(&code) {
                return Ok(Some(bytes));
            }
            // Keep legacy semantics for NULL sentinel code 0 when no explicit mapping exists.
            if code == 0 {
                return Ok(None);
            }
            return Err(format!("global dict missing code {}", code));
        }
        // Null row: allow derived dicts to provide explicit null-output mapping on code 0.
        if let Some(bytes) = dict.get(&0) {
            return Ok(Some(bytes));
        }
        Ok(None)
    };

    match output_type {
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    builder.append_value(bytes);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    builder.append_value(bytes);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    let code_label = code.map_or_else(|| "NULL".to_string(), |c| c.to_string());
                    let value = std::str::from_utf8(bytes).map_err(|e| {
                        format!(
                            "global dict value for code {} is not utf8: {}",
                            code_label, e
                        )
                    })?;
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            let mut builder = StringBuilder::with_capacity(codes.len(), 0);
            for code in codes {
                if let Some(bytes) = code_to_bytes(*code)? {
                    let code_label = code.map_or_else(|| "NULL".to_string(), |c| c.to_string());
                    let value = std::str::from_utf8(bytes).map_err(|e| {
                        format!(
                            "global dict value for code {} is not utf8: {}",
                            code_label, e
                        )
                    })?;
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn collect_codes_i8(values: &Int8Array) -> Vec<Option<i32>> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    out
}

fn collect_codes_i16(values: &Int16Array) -> Vec<Option<i32>> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    out
}

fn collect_codes_i32(values: &Int32Array) -> Vec<Option<i32>> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(values.value(i)));
        }
    }
    out
}

fn collect_codes_i64(values: &Int64Array) -> Result<Vec<Option<i32>>, String> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            let value = values.value(i);
            out.push(Some(
                i32::try_from(value)
                    .map_err(|_| format!("dict code out of i32 range: {}", value))?,
            ));
        }
    }
    Ok(out)
}

fn collect_codes_u8(values: &UInt8Array) -> Vec<Option<i32>> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    out
}

fn collect_codes_u16(values: &UInt16Array) -> Vec<Option<i32>> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(i32::from(values.value(i))));
        }
    }
    out
}

fn collect_codes_u32(values: &UInt32Array) -> Result<Vec<Option<i32>>, String> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            let value = values.value(i);
            out.push(Some(
                i32::try_from(value)
                    .map_err(|_| format!("dict code out of i32 range: {}", value))?,
            ));
        }
    }
    Ok(out)
}

fn collect_codes_u64(values: &UInt64Array) -> Result<Vec<Option<i32>>, String> {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            out.push(None);
        } else {
            let value = values.value(i);
            out.push(Some(
                i32::try_from(value)
                    .map_err(|_| format!("dict code out of i32 range: {}", value))?,
            ));
        }
    }
    Ok(out)
}

fn collect_codes_from_array(input: &ArrayRef) -> Result<Vec<Option<i32>>, String> {
    match input.data_type() {
        DataType::Int8 => {
            let values = input
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "dict decode failed to downcast Int8 array".to_string())?;
            Ok(collect_codes_i8(values))
        }
        DataType::Int16 => {
            let values = input
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "dict decode failed to downcast Int16 array".to_string())?;
            Ok(collect_codes_i16(values))
        }
        DataType::Int32 => {
            let values = input
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "dict decode failed to downcast Int32 array".to_string())?;
            Ok(collect_codes_i32(values))
        }
        DataType::Int64 => {
            let values = input
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "dict decode failed to downcast Int64 array".to_string())?;
            collect_codes_i64(values)
        }
        DataType::UInt8 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "dict decode failed to downcast UInt8 array".to_string())?;
            Ok(collect_codes_u8(values))
        }
        DataType::UInt16 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "dict decode failed to downcast UInt16 array".to_string())?;
            Ok(collect_codes_u16(values))
        }
        DataType::UInt32 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "dict decode failed to downcast UInt32 array".to_string())?;
            collect_codes_u32(values)
        }
        DataType::UInt64 => {
            let values = input
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "dict decode failed to downcast UInt64 array".to_string())?;
            collect_codes_u64(values)
        }
        DataType::Null => Ok(vec![None; input.len()]),
        other => Err(format!(
            "dict decode expects integer input, got {:?}",
            other
        )),
    }
}

fn decode_array_with_dict(
    input: &ArrayRef,
    dict: &HashMap<i32, Vec<u8>>,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    match (input.data_type(), output_type) {
        (DataType::List(_), DataType::List(output_item)) => {
            let list = input
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "dict decode failed to downcast List array".to_string())?;
            let decoded_values =
                decode_array_with_dict(&list.values(), dict, output_item.data_type())?;
            Ok(Arc::new(ListArray::new(
                output_item.clone(),
                list.offsets().clone(),
                decoded_values,
                list.nulls().cloned(),
            )))
        }
        (DataType::LargeList(_), DataType::LargeList(output_item)) => {
            let list = input
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "dict decode failed to downcast LargeList array".to_string())?;
            let decoded_values =
                decode_array_with_dict(&list.values(), dict, output_item.data_type())?;
            Ok(Arc::new(LargeListArray::new(
                output_item.clone(),
                list.offsets().clone(),
                decoded_values,
                list.nulls().cloned(),
            )))
        }
        (DataType::Null, _) => Ok(new_null_array(output_type, input.len())),
        _ => {
            let codes = collect_codes_from_array(input)?;
            decode_codes_to_output(&codes, dict, output_type)
        }
    }
}

fn default_dict_decode_output_type(input_type: &DataType) -> DataType {
    match input_type {
        DataType::List(item) => DataType::List(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(default_dict_decode_output_type(item.data_type())),
        )),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(default_dict_decode_output_type(item.data_type())),
        )),
        _ => DataType::Utf8,
    }
}

fn normalize_dict_decode_output_type(
    input_type: &DataType,
    requested_output_type: &DataType,
) -> DataType {
    match input_type {
        DataType::List(input_item) => {
            let item_output = match requested_output_type {
                DataType::List(output_item) => normalize_dict_decode_output_type(
                    input_item.data_type(),
                    output_item.data_type(),
                ),
                _ => default_dict_decode_output_type(input_item.data_type()),
            };
            DataType::List(Arc::new(
                input_item.as_ref().clone().with_data_type(item_output),
            ))
        }
        DataType::LargeList(input_item) => {
            let item_output = match requested_output_type {
                DataType::LargeList(output_item) => normalize_dict_decode_output_type(
                    input_item.data_type(),
                    output_item.data_type(),
                ),
                _ => default_dict_decode_output_type(input_item.data_type()),
            };
            DataType::LargeList(Arc::new(
                input_item.as_ref().clone().with_data_type(item_output),
            ))
        }
        _ => requested_output_type.clone(),
    }
}

pub(crate) fn eval_dict_decode(
    arena: &ExprArena,
    id: ExprId,
    child: ExprId,
    dict: &Arc<HashMap<i32, Vec<u8>>>,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(child, chunk)?;
    let output_type = arena.data_type(id).unwrap_or(&DataType::Utf8);
    let normalized_output_type = normalize_dict_decode_output_type(input.data_type(), output_type);
    decode_array_with_dict(&input, dict, &normalized_output_type)
}
