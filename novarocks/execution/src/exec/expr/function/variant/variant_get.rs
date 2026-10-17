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

//! Spark-aligned `variant_get` / `try_variant_get`, evaluated through the
//! upstream parquet-variant-compute kernel so expression-layer results are
//! row-identical with the scan-layer shredded fast path (IV3-6 decision B).

use super::common::binary_value_at_any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBufferBuilder, LargeBinaryArray, LargeBinaryBuilder,
    StringArray, StructArray, new_empty_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field};
use parquet::variant::{
    GetOptions, VariantArray, json_to_variant, unshred_variant, variant_get as kernel_variant_get,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use novarocks_types::value::variant::{
    VariantMetadata, VariantPathSegment, VariantValue, parse_variant_path, split_serialized,
    variant_get_target_type,
};

/// Read the original required string literal or its checked frozen value.
/// SQL admission remains literal-only; this never evaluates a foldable tree.
fn literal_utf8_arg<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    id: ExprId,
    what: &str,
    fn_name: &str,
) -> Result<String, __Eval::Error> {
    match arena.node(id) {
        Some(ExprNode::Literal(LiteralValue::Utf8(s))) => Ok(s.clone()),
        Some(ExprNode::Constant(value))
            if value.value_type().logical_type()
                == novarocks_type_contract::ValueLogicalType::Physical
                && value.value_type().data_type() == &DataType::Utf8 =>
        {
            Ok((value
                .try_utf8()
                .map_err(|error| format!("{fn_name} has an invalid frozen {what} string: {error}"))?
                .map(str::to_owned)
                .ok_or_else(|| {
                    format!("{fn_name} requires a constant string literal for the {what} argument")
                }))?)
        }
        _ => Err(
            (format!("{fn_name} requires a constant string literal for the {what} argument"))
                .into(),
        ),
    }
}

fn kernel_path(path_str: &str) -> Result<parquet::variant::VariantPath<'static>, String> {
    let parsed = parse_variant_path(path_str)?;
    let elems: Vec<parquet::variant::VariantPathElement<'static>> = parsed
        .segments
        .iter()
        .map(|seg| match seg {
            VariantPathSegment::ObjectKey(k) => {
                parquet::variant::VariantPathElement::field(k.clone())
            }
            VariantPathSegment::ArrayIndex(i) => {
                parquet::variant::VariantPathElement::index(*i as usize)
            }
        })
        .collect();
    Ok(parquet::variant::VariantPath::from(elems))
}

/// Engine LargeBinary `[size|metadata|value]` rows -> upstream VariantArray.
fn large_binary_to_variant_array(arr: &LargeBinaryArray) -> Result<VariantArray, String> {
    let mut metadata_builder = BinaryBuilder::new();
    let mut value_builder = BinaryBuilder::new();
    let mut validity = BooleanBufferBuilder::new(arr.len());
    // Compute once; the struct-level validity bit makes null-row child bytes
    // unread, but use a valid empty variant metadata rather than empty bytes
    // so we never depend on the kernel ignoring null-row content.
    let empty_meta = VariantMetadata::empty();
    let empty_meta_raw = empty_meta.raw();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            metadata_builder.append_value(empty_meta_raw);
            value_builder.append_value([]);
            validity.append(false);
            continue;
        }
        let (m, v) = split_serialized(arr.value(row))
            .map_err(|e| format!("variant_get: invalid variant input at row {row}: {e}"))?;
        metadata_builder.append_value(m);
        value_builder.append_value(v);
        validity.append(true);
    }
    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let struct_arr = StructArray::new(
        fields,
        vec![
            Arc::new(metadata_builder.finish()) as ArrayRef,
            Arc::new(value_builder.finish()) as ArrayRef,
        ],
        Some(NullBuffer::new(validity.finish())),
    );
    VariantArray::try_new(&struct_arr)
        .map_err(|e| format!("variant_get: failed to assemble variant input: {e}"))
}

/// Upstream VariantArray -> engine LargeBinary rows (2-arg return form).
fn variant_array_to_large_binary(va: VariantArray) -> Result<ArrayRef, String> {
    let va = if va.typed_value_field().is_some() {
        unshred_variant(&va).map_err(|e| format!("variant_get: unshred failed: {e}"))?
    } else {
        va
    };
    let metadata = va.metadata_field().clone();
    let value = va
        .value_field()
        .cloned()
        .ok_or_else(|| "variant_get: kernel result missing value column".to_string())?;
    let len = metadata.len();
    let mut builder = LargeBinaryBuilder::new();
    for row in 0..len {
        if va.is_null(row) {
            builder.append_null();
            continue;
        }
        let (m, v) = match (
            binary_value_at_any(&metadata, row, "variant_get")?,
            binary_value_at_any(&value, row, "variant_get")?,
        ) {
            (Some(m), Some(v)) => (m, v),
            _ => {
                builder.append_null();
                continue;
            }
        };
        let serialized = VariantValue::create(m, v)
            .map_err(|e| format!("variant_get: kernel result row {row}: {e}"))?
            .serialize();
        builder.append_value(serialized.as_slice());
    }
    Ok(Arc::new(builder.finish()))
}

fn eval_variant_get_impl<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    strict: bool,
    fn_name: &str,
) -> Result<ArrayRef, __Eval::Error> {
    if !(2..=3).contains(&args.len()) {
        return Err((format!("{fn_name} expects 2 or 3 arguments, got {}", args.len())).into());
    }
    let path_str = literal_utf8_arg(arena, args[1], "path", fn_name)?;
    let target_type = if args.len() == 3 {
        Some(variant_get_target_type(&literal_utf8_arg(
            arena, args[2], "type", fn_name,
        )?)?)
    } else {
        None
    };
    let result_type = target_type.clone().unwrap_or(DataType::LargeBinary);
    if chunk.is_empty() {
        return Ok(new_empty_array(&result_type));
    }

    let input = arena.eval(args[0], chunk)?;
    let variant_array = if let Some(bin) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        large_binary_to_variant_array(bin)?
    } else if let Some(json) = input.as_any().downcast_ref::<StringArray>() {
        // JSON-string input mode (e.g. variant_get(parse_json(s), ...)).
        // Malformed JSON is a query error in both strict and try modes —
        // try_ only relaxes the *cast*, mirroring Spark.
        let json_ref: ArrayRef = Arc::new(json.clone());
        json_to_variant(&json_ref).map_err(|e| format!("{fn_name}: invalid JSON input: {e}"))?
    } else {
        return Err((format!(
            "{fn_name} expects VARIANT or JSON/VARCHAR as first argument, got {:?}",
            input.data_type()
        ))
        .into());
    };

    let mut opts =
        GetOptions::new_with_path(kernel_path(&path_str)?).with_cast_options(CastOptions {
            safe: !strict,
            format_options: Default::default(),
        });
    if let Some(dt) = &target_type {
        opts = opts.with_as_type(Some(Arc::new(Field::new("", dt.clone(), true))));
    }

    let input_ref: ArrayRef = Arc::new(variant_array.into_inner());
    let result = kernel_variant_get(&input_ref, opts).map_err(|e| format!("{fn_name}: {e}"))?;

    match target_type {
        Some(_) => Ok(result),
        None => {
            let va = VariantArray::try_new(result.as_ref())
                .map_err(|e| format!("{fn_name}: unexpected kernel result: {e}"))?;
            Ok((variant_array_to_large_binary(va))?)
        }
    }
}

pub fn eval_variant_get<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_variant_get_impl(arena, args, chunk, true, "variant_get"))?)
}

pub fn eval_try_variant_get<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_variant_get_impl(arena, args, chunk, false, "try_variant_get"))?)
}

#[cfg(test)]
#[path = "variant_get_constant_tests.rs"]
mod constant_tests;
