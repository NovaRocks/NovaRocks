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

//! Read-side variant column handling: collapse parquet variant structs
//! (unshredded `{metadata,value}` or shredded `{metadata,value,typed_value}`)
//! into the engine-internal LargeBinary form `[size:u32 LE | metadata | value]`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBufferBuilder, LargeBinaryArray, LargeBinaryBuilder,
    RecordBatch, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field};
use parquet::variant::{
    GetOptions, VariantArray, unshred_variant, variant_get as kernel_variant_get,
};

use crate::common::ids::SlotId;
use crate::exec::variant::{
    VariantMetadata, VariantPathSegment, VariantValue, parse_variant_path, split_serialized,
};
use crate::formats::parquet::VariantPathSpec;
use crate::types;

fn is_binary_like(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

/// True when `data_type` is a parquet variant struct layout we can collapse:
/// a `metadata` binary child plus at least one of `value` (binary) /
/// `typed_value` (shredded subtree). Any other child name disqualifies.
pub(crate) fn is_variant_struct_data_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.is_empty() {
        return false;
    }
    let mut has_metadata = false;
    let mut has_value = false;
    let mut has_typed_value = false;
    for f in fields {
        match f.name().as_str() {
            "metadata" if is_binary_like(f.data_type()) => has_metadata = true,
            "value" if is_binary_like(f.data_type()) => has_value = true,
            "typed_value" => has_typed_value = true,
            _ => return false,
        }
    }
    has_metadata && (has_value || has_typed_value)
}

fn binary_value_at_any(arr: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    use arrow::array::{BinaryArray, BinaryViewArray, LargeBinaryArray};
    if arr.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(a.value(row)));
    }
    Err(format!(
        "expected a binary array for variant metadata/value, got {:?}",
        arr.data_type()
    ))
}

/// Collapse a parquet variant struct column (unshredded or shredded) into
/// the engine-internal LargeBinary `[size|metadata|value]` form.
///
/// Shredded inputs (a `typed_value` child present) are first folded back to
/// plain metadata+value via the upstream `unshred_variant` kernel, so both
/// layouts produce identical engine-visible rows. Corrupt rows (missing
/// metadata or value bytes on a non-null row) are a hard error — never a
/// silent variant-null.
pub(crate) fn collapse_variant_struct_to_largebinary(
    source_array: &ArrayRef,
    column_name: &str,
) -> Result<ArrayRef, String> {
    let struct_arr = source_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| format!("expected StructArray for variant column `{column_name}`"))?;

    let has_typed_value = struct_arr
        .fields()
        .iter()
        .any(|f| f.name() == "typed_value");

    // Holds the unshredded form alive for the borrow below.
    let unshredded_holder;
    let (metadata_col, value_col): (ArrayRef, ArrayRef) = if has_typed_value {
        let variant = VariantArray::try_new(source_array.as_ref())
            .map_err(|e| format!("variant column `{column_name}`: invalid shredded layout: {e}"))?;
        unshredded_holder = unshred_variant(&variant)
            .map_err(|e| format!("variant column `{column_name}`: unshred failed: {e}"))?;
        let value = unshredded_holder.value_field().ok_or_else(|| {
            format!("variant column `{column_name}`: unshred produced no value column")
        })?;
        (unshredded_holder.metadata_field().clone(), value.clone())
    } else {
        let mut metadata_idx = None;
        let mut value_idx = None;
        for (i, f) in struct_arr.fields().iter().enumerate() {
            match f.name().as_str() {
                "metadata" => metadata_idx = Some(i),
                "value" => value_idx = Some(i),
                _ => {}
            }
        }
        let metadata_idx = metadata_idx.ok_or_else(|| {
            format!("variant column `{column_name}`: struct missing metadata field")
        })?;
        let value_idx = value_idx
            .ok_or_else(|| format!("variant column `{column_name}`: struct missing value field"))?;
        (
            struct_arr.column(metadata_idx).clone(),
            struct_arr.column(value_idx).clone(),
        )
    };

    let mut builder = LargeBinaryBuilder::new();
    for row in 0..struct_arr.len() {
        if struct_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let metadata = binary_value_at_any(&metadata_col, row)?;
        let value = binary_value_at_any(&value_col, row)?;
        match (metadata, value) {
            (Some(m), Some(v)) => {
                let serialized = VariantValue::create(m, v)
                    .map_err(|e| format!("variant column `{column_name}` row {row}: {e}"))?
                    .serialize();
                builder.append_value(serialized.as_slice());
            }
            _ => {
                return Err(format!(
                    "variant column `{column_name}` row {row}: missing metadata/value bytes \
                     (corrupt file or unsupported variant encoding)"
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Replace every VARIANT-typed slot whose column arrived as a parquet
/// variant struct with the engine-internal LargeBinary form. Non-variant
/// slots and already-LargeBinary variant slots pass through untouched.
pub(crate) fn convert_variant_columns(
    slot_types: &[types::TPrimitiveType],
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if slot_types.is_empty() {
        return Ok(batch);
    }
    if !slot_types.contains(&types::TPrimitiveType::VARIANT) {
        return Ok(batch);
    }

    if batch.num_columns() != slot_types.len() {
        return Err(format!(
            "parquet scan slot_types mismatch: columns={} slot_types={}",
            batch.num_columns(),
            slot_types.len()
        ));
    }

    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(idx);
        if slot_types[idx] != types::TPrimitiveType::VARIANT {
            new_fields.push(field.clone());
            new_columns.push(col.clone());
            continue;
        }

        match col.data_type() {
            DataType::LargeBinary => {
                new_fields.push(field.clone());
                new_columns.push(col.clone());
            }
            DataType::Struct(_) => {
                if !is_variant_struct_data_type(col.data_type()) {
                    return Err(format!(
                        "VARIANT column `{}` has unsupported struct layout: {:?}",
                        field.name(),
                        col.data_type()
                    ));
                }
                let collapsed = collapse_variant_struct_to_largebinary(col, field.name())?;
                let meta = field.metadata().clone();
                let new_field = Arc::new(
                    arrow::datatypes::Field::new(
                        field.name(),
                        DataType::LargeBinary,
                        field.is_nullable(),
                    )
                    .with_metadata(meta),
                );
                new_fields.push(new_field);
                new_columns.push(collapsed);
            }
            other => {
                return Err(format!("VARIANT column has unsupported type: {:?}", other));
            }
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e: arrow::error::ArrowError| e.to_string())
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

fn large_binary_to_variant_array(arr: &LargeBinaryArray) -> Result<VariantArray, String> {
    let mut metadata_builder = BinaryBuilder::new();
    let mut value_builder = BinaryBuilder::new();
    let mut validity = BooleanBufferBuilder::new(arr.len());
    let empty_meta = VariantMetadata::empty();
    let empty_meta_raw = empty_meta.raw();

    for row in 0..arr.len() {
        if arr.is_null(row) {
            metadata_builder.append_value(empty_meta_raw);
            value_builder.append_value([]);
            validity.append(false);
            continue;
        }
        let (metadata, value) = split_serialized(arr.value(row))
            .map_err(|e| format!("variant path source row {row}: {e}"))?;
        metadata_builder.append_value(metadata);
        value_builder.append_value(value);
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
        .map_err(|e| format!("variant path source: failed to assemble variant input: {e}"))
}

fn source_to_engine_variant_array(
    source_array: &ArrayRef,
    column_name: &str,
) -> Result<VariantArray, String> {
    let source = match source_array.data_type() {
        DataType::LargeBinary => source_array.clone(),
        DataType::Struct(_) => {
            if !is_variant_struct_data_type(source_array.data_type()) {
                return Err(format!(
                    "variant path source column `{column_name}` has unsupported struct layout: {:?}",
                    source_array.data_type()
                ));
            }
            collapse_variant_struct_to_largebinary(source_array, column_name)?
        }
        other => {
            return Err(format!(
                "variant path source column `{column_name}` has unsupported type: {:?}",
                other
            ));
        }
    };
    let source = source
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            format!("variant path source column `{column_name}` did not collapse to LargeBinary")
        })?;
    large_binary_to_variant_array(source)
}

fn object_key_path(path_str: &str) -> Option<Vec<String>> {
    let parsed = parse_variant_path(path_str).ok()?;
    if parsed.segments.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(parsed.segments.len());
    for segment in parsed.segments {
        let VariantPathSegment::ObjectKey(key) = segment else {
            return None;
        };
        keys.push(key);
    }
    Some(keys)
}

fn struct_child_by_name(struct_arr: &StructArray, name: &str) -> Option<ArrayRef> {
    let idx = struct_arr
        .fields()
        .iter()
        .position(|field| field.name() == name)?;
    Some(struct_arr.column(idx).clone())
}

fn array_has_non_null_value(array: &ArrayRef) -> bool {
    array.null_count() < array.len()
}

fn child_non_null_where_parent_null(parent: &StructArray, child: &ArrayRef) -> bool {
    if parent.null_count() == 0 {
        return false;
    }
    (0..parent.len()).any(|row| parent.is_null(row) && !child.is_null(row))
}

fn try_materialize_shredded_typed_child(
    source_array: &ArrayRef,
    spec: &VariantPathSpec,
) -> Option<ArrayRef> {
    if !is_variant_struct_data_type(source_array.data_type()) {
        return None;
    }
    let root = source_array.as_any().downcast_ref::<StructArray>()?;
    let path = object_key_path(&spec.canonical_path)?;
    let mut current = struct_child_by_name(root, "typed_value")?;

    for (idx, key) in path.iter().enumerate() {
        let current_struct = current.as_any().downcast_ref::<StructArray>()?;
        let path_node = struct_child_by_name(current_struct, key)?;
        let path_node_struct = path_node.as_any().downcast_ref::<StructArray>()?;
        let typed_child = struct_child_by_name(path_node_struct, "typed_value")?;

        if child_non_null_where_parent_null(path_node_struct, &typed_child) {
            return None;
        }

        if idx + 1 == path.len() {
            if typed_child.data_type() != &spec.requested_type {
                return None;
            }
            if typed_child.len() != source_array.len() {
                return None;
            }
            if child_non_null_where_parent_null(root, &typed_child) {
                return None;
            }
            if let Some(value_child) = struct_child_by_name(path_node_struct, "value")
                && array_has_non_null_value(&value_child)
            {
                return None;
            }
            return Some(typed_child);
        }

        current = typed_child;
    }

    None
}

fn materialize_single_variant_path(
    source_array: &ArrayRef,
    spec: &VariantPathSpec,
) -> Result<ArrayRef, String> {
    if let Some(array) = try_materialize_shredded_typed_child(source_array, spec) {
        return Ok(array);
    }

    let fn_name = if spec.strict {
        "variant_get"
    } else {
        "try_variant_get"
    };
    let variant_array = source_to_engine_variant_array(source_array, &spec.source_name)?;
    let opts = GetOptions::new_with_path(kernel_path(&spec.canonical_path)?)
        .with_cast_options(CastOptions {
            safe: !spec.strict,
            format_options: Default::default(),
        })
        .with_as_type(Some(Arc::new(Field::new(
            "",
            spec.requested_type.clone(),
            true,
        ))));

    let input_ref: ArrayRef = Arc::new(variant_array.into_inner());
    kernel_variant_get(&input_ref, opts).map_err(|e| format!("{fn_name}: {e}"))
}

/// Materialize synthetic scan slots for pushed-down variant paths.
///
/// `batch` is the parquet read batch after read-column reordering. `read_slot_ids`
/// describes that read layout, including hidden source slots. `output_slot_ids`
/// is the reader-visible materialized layout: physical slots that remain visible
/// plus synthetic `VariantPathSpec::output_slot_id` slots. Hidden source read
/// slots are omitted unless the caller includes them explicitly in
/// `output_slot_ids`.
pub(crate) fn materialize_variant_path_columns(
    batch: RecordBatch,
    read_slot_ids: &[SlotId],
    output_slot_ids: &[SlotId],
    specs: &[VariantPathSpec],
) -> Result<RecordBatch, String> {
    if specs.is_empty() {
        return Ok(batch);
    }
    if batch.num_columns() != read_slot_ids.len() {
        return Err(format!(
            "variant path read slot mismatch: columns={} read_slot_ids={}",
            batch.num_columns(),
            read_slot_ids.len()
        ));
    }

    let batch_schema = batch.schema();
    let mut existing = HashMap::with_capacity(read_slot_ids.len());
    for (idx, slot_id) in read_slot_ids.iter().copied().enumerate() {
        existing.insert(
            slot_id,
            (
                batch_schema.field(idx).as_ref().clone(),
                batch.column(idx).clone(),
            ),
        );
    }

    let mut synthetic = HashMap::with_capacity(specs.len());
    for spec in specs {
        let source_idx = read_slot_ids
            .iter()
            .position(|slot_id| *slot_id == spec.source_read_slot_id)
            .ok_or_else(|| {
                format!(
                    "variant path source_read_slot_id={} for output_slot_id={} is missing from read slots {:?}",
                    spec.source_read_slot_id, spec.output_slot_id, read_slot_ids
                )
            })?;
        let array = materialize_single_variant_path(batch.column(source_idx), spec)?;
        let mut field = spec.output_field.clone();
        if array.null_count() > 0 && !field.is_nullable() {
            field = field.with_nullable(true);
        }
        if synthetic
            .insert(spec.output_slot_id, (field, array))
            .is_some()
        {
            return Err(format!(
                "duplicate variant path output_slot_id={}",
                spec.output_slot_id
            ));
        }
    }

    let mut fields = Vec::with_capacity(output_slot_ids.len());
    let mut columns = Vec::with_capacity(output_slot_ids.len());
    for slot_id in output_slot_ids {
        if let Some((field, array)) = synthetic.get(slot_id) {
            fields.push(field.clone());
            columns.push(array.clone());
            continue;
        }
        if let Some((field, array)) = existing.get(slot_id) {
            fields.push(field.clone());
            columns.push(array.clone());
            continue;
        }
        return Err(format!(
            "variant path output slot_id={} is neither a read column nor a synthetic column",
            slot_id
        ));
    }

    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
        .map_err(|e| format!("build variant path materialized batch failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields, Schema};

    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    #[test]
    fn variant_struct_gate_accepts_unshredded() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_shredded_three_child() {
        let typed_value = struct_of(vec![Field::new(
            "a",
            struct_of(vec![
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", DataType::Int64, true),
            ]),
            false,
        )]);
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("typed_value", typed_value, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_metadata_plus_typed_value_only() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("typed_value", DataType::Int64, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_binary_view_children() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_rejects_non_variant_shapes() {
        // missing metadata
        assert!(!is_variant_struct_data_type(&struct_of(vec![Field::new(
            "value",
            DataType::Binary,
            true
        )])));
        // unknown extra field
        assert!(!is_variant_struct_data_type(&struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("extra", DataType::Int32, true),
        ])));
        // metadata only
        assert!(!is_variant_struct_data_type(&struct_of(vec![Field::new(
            "metadata",
            DataType::Binary,
            false
        )])));
        // metadata wrong type
        assert!(!is_variant_struct_data_type(&struct_of(vec![
            Field::new("metadata", DataType::Int32, false),
            Field::new("value", DataType::Binary, true),
        ])));
        // not a struct
        assert!(!is_variant_struct_data_type(&DataType::LargeBinary));
    }

    use arrow::array::{Float64Array, Int64Array, LargeBinaryArray, StringArray};
    use parquet::variant::{ShreddedSchemaBuilder, json_to_variant, shred_variant};

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::function::variant::{eval_try_variant_get, eval_variant_get};
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use crate::exec::variant::{
        parse_variant_path, variant_query, variant_to_i64, variant_to_string,
    };
    use crate::formats::parquet::VariantPathSpec;

    fn variant_struct_from_json(shredded: bool, rows: Vec<Option<&str>>) -> ArrayRef {
        let json: ArrayRef = Arc::new(StringArray::from(rows));
        let unshredded = json_to_variant(&json).expect("json_to_variant");
        let variant = if shredded {
            let shred_type = ShreddedSchemaBuilder::new()
                .with_path("a", &DataType::Int64)
                .expect("with_path")
                .build();
            shred_variant(&unshredded, &shred_type).expect("shred_variant")
        } else {
            unshredded
        };
        Arc::new(variant.into_inner()) as ArrayRef
    }

    /// Build the canonical 5-row test column from JSON, optionally shredded
    /// on path `a` as Int64. Rows: shredded int, shredded int, missing `a`,
    /// wrong-typed `a`, SQL NULL.
    fn test_variant_struct(shredded: bool) -> ArrayRef {
        variant_struct_from_json(
            shredded,
            vec![
                Some(r#"{"a": 1, "b": "x"}"#),
                Some(r#"{"a": 99, "b": "y"}"#),
                Some(r#"{"b": "no-a"}"#),
                Some(r#"{"a": "not-an-int"}"#),
                None,
            ],
        )
    }

    fn get_a_int(serialized: &[u8]) -> Option<i64> {
        let v = VariantValue::from_serialized(serialized).ok()?;
        let path = parse_variant_path("$.a").ok()?;
        let sub = variant_query(&v, &path).ok()?;
        variant_to_i64(&sub).ok()
    }

    /// Extract the string content of `$.a` when the sub-value is a variant string.
    /// Returns `None` when `$.a` is absent or is not a string (e.g. an int).
    fn get_a_str(serialized: &[u8]) -> Option<String> {
        let v = VariantValue::from_serialized(serialized).ok()?;
        let path = parse_variant_path("$.a").ok()?;
        let sub = variant_query(&v, &path).ok()?;
        variant_to_string(&sub).ok()
    }

    #[test]
    fn collapse_unshredded_struct_round_trips() {
        let col = test_variant_struct(false);
        let out = collapse_variant_struct_to_largebinary(&col, "v").expect("collapse");
        let out = out
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinary");
        assert_eq!(out.len(), 5);
        assert_eq!(get_a_int(out.value(0)), Some(1));
        assert_eq!(get_a_int(out.value(1)), Some(99));
        assert_eq!(get_a_int(out.value(2)), None); // `a` missing
        assert!(
            !out.is_null(3),
            "row 3 (wrong-typed a) round-trips without error"
        );
        assert!(out.is_null(4)); // SQL NULL preserved
    }

    #[test]
    fn collapse_shredded_struct_matches_unshredded() {
        let shredded = collapse_variant_struct_to_largebinary(&test_variant_struct(true), "v")
            .expect("collapse shredded");
        let plain = collapse_variant_struct_to_largebinary(&test_variant_struct(false), "v")
            .expect("collapse plain");
        let shredded = shredded
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let plain = plain.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        for row in 0..5 {
            assert_eq!(
                shredded.is_null(row),
                plain.is_null(row),
                "row {row} nullness"
            );
            if shredded.is_null(row) {
                continue;
            }
            // Byte-equality is NOT required (field order inside the rebuilt
            // object may differ); semantic equality on every field is.
            assert_eq!(
                get_a_int(shredded.value(row)),
                get_a_int(plain.value(row)),
                "row {row} $.a"
            );
        }
        // Row 3: `$.a` is a wrong-typed string ("not-an-int").  The int extractor
        // returns None for both paths (not a regression signal on its own).  These
        // three guards verify the *string* value actually survives the shredded
        // round-trip identically, catching any drop or corruption of that field.
        assert!(
            !shredded.is_null(3) && !plain.is_null(3),
            "row 3 present in both"
        );
        assert_eq!(
            get_a_str(shredded.value(3)),
            get_a_str(plain.value(3)),
            "row 3 $.a string preserved across shredded round-trip"
        );
        assert!(
            get_a_str(plain.value(3)).is_some(),
            "row 3 $.a should be a non-null string"
        );
    }

    #[test]
    fn collapse_rejects_missing_bytes_instead_of_silent_null() {
        use arrow::array::BinaryArray;
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field, Fields};
        // metadata present, value NULL, no typed_value: previously this row
        // silently became variant-null; it must now be a hard error.
        let metadata: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![Some(&[0x01u8][..])]));
        let value: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![None]));
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]);
        let struct_arr: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![metadata, value],
            None::<NullBuffer>,
        ));
        let err =
            collapse_variant_struct_to_largebinary(&struct_arr, "v").expect_err("must fail fast");
        assert!(err.contains("v"), "error names the column: {err}");
        assert!(
            err.contains("missing metadata/value"),
            "error says what is wrong: {err}"
        );
    }

    fn batch_with_variant_struct(shredded: bool) -> RecordBatch {
        let col = test_variant_struct(shredded);
        let field = Field::new("v", col.data_type().clone(), true);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).expect("batch")
    }

    fn variant_path_spec_for(
        canonical_path: &str,
        requested_type: DataType,
        strict: bool,
    ) -> VariantPathSpec {
        VariantPathSpec {
            source_slot_id: SlotId::new(1),
            source_read_slot_id: SlotId::new(90),
            output_slot_id: SlotId::new(2),
            source_field_id: None,
            source_name: "v".to_string(),
            output_name: "v_a".to_string(),
            source_field: Field::new("v", DataType::LargeBinary, true),
            output_field: Field::new("v_a", requested_type.clone(), true),
            canonical_path: canonical_path.to_string(),
            requested_type,
            strict,
        }
    }

    fn variant_path_spec(strict: bool) -> VariantPathSpec {
        variant_path_spec_for("$.a", DataType::Int64, strict)
    }

    fn direct_shredded_a_typed_child(source: &ArrayRef) -> ArrayRef {
        let root = source.as_any().downcast_ref::<StructArray>().unwrap();
        let root_typed_value_idx = root
            .fields()
            .iter()
            .position(|field| field.name() == "typed_value")
            .expect("root typed_value");
        let root_typed_value = root
            .column(root_typed_value_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("root typed_value struct");
        let a_idx = root_typed_value
            .fields()
            .iter()
            .position(|field| field.name() == "a")
            .expect("typed field a");
        let a_node = root_typed_value
            .column(a_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("a shredded node");
        let a_typed_idx = a_node
            .fields()
            .iter()
            .position(|field| field.name() == "typed_value")
            .expect("a typed_value");
        a_node.column(a_typed_idx).clone()
    }

    fn expression_variant_get_result_for_source(
        source: ArrayRef,
        strict: bool,
    ) -> Result<ArrayRef, String> {
        let collapsed = collapse_variant_struct_to_largebinary(&source, "v")?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::LargeBinary,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![collapsed]).expect("expr batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("expr chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("expr chunk");

        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::LargeBinary);
        let path = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("$.a".to_string())),
            DataType::Utf8,
        );
        let ty = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("bigint".to_string())),
            DataType::Utf8,
        );
        let args = [source, path, ty];
        if strict {
            eval_variant_get(&arena, ExprId(usize::MAX), &args, &chunk)
        } else {
            eval_try_variant_get(&arena, ExprId(usize::MAX), &args, &chunk)
        }
    }

    fn expression_variant_get_result(shredded: bool, strict: bool) -> Result<ArrayRef, String> {
        expression_variant_get_result_for_source(test_variant_struct(shredded), strict)
    }

    fn materialized_variant_path(shredded: bool, strict: bool) -> Result<RecordBatch, String> {
        materialize_variant_path_columns(
            batch_with_variant_struct(shredded),
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec(strict)],
        )
    }

    fn assert_int64_array_eq(actual: &ArrayRef, expected: &ArrayRef) {
        let actual = actual.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = expected.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(actual.len(), expected.len());
        for row in 0..actual.len() {
            assert_eq!(actual.is_null(row), expected.is_null(row), "row {row}");
            if !actual.is_null(row) {
                assert_eq!(actual.value(row), expected.value(row), "row {row}");
            }
        }
    }

    #[test]
    fn variant_path_materialization_matches_variant_get_on_unshredded_fallback() {
        let source = variant_struct_from_json(
            false,
            vec![
                Some(r#"{"a": 1, "b": "x"}"#),
                Some(r#"{"a": 99, "b": "y"}"#),
                Some(r#"{"b": "no-a"}"#),
                None,
            ],
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                source.data_type().clone(),
                true,
            )])),
            vec![source.clone()],
        )
        .expect("batch");
        let batch = materialize_variant_path_columns(
            batch,
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec(true)],
        )
        .expect("materialize");
        let expected = expression_variant_get_result_for_source(source, true).expect("expr");
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "v_a");
        assert_int64_array_eq(batch.column(0), &expected);
    }

    #[test]
    fn variant_path_shredded_exact_match_uses_direct_typed_child() {
        let source = variant_struct_from_json(
            true,
            vec![
                Some(r#"{"a": 1, "b": "x"}"#),
                Some(r#"{"a": 99, "b": "y"}"#),
                Some(r#"{"b": "no-a"}"#),
                None,
            ],
        );
        let direct_child = direct_shredded_a_typed_child(&source);
        assert_eq!(direct_child.data_type(), &DataType::Int64);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                source.data_type().clone(),
                true,
            )])),
            vec![source.clone()],
        )
        .expect("batch");

        let batch = materialize_variant_path_columns(
            batch,
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec(true)],
        )
        .expect("materialize");

        assert!(
            Arc::ptr_eq(batch.column(0), &direct_child),
            "exact shredded typed-value path should return the direct typed child array"
        );
        let expected = expression_variant_get_result_for_source(source, true).expect("expr");
        assert_int64_array_eq(batch.column(0), &expected);
    }

    #[test]
    fn variant_path_shredded_type_mismatch_uses_fallback() {
        let source = variant_struct_from_json(
            true,
            vec![
                Some(r#"{"a": 1, "b": "x"}"#),
                Some(r#"{"a": 99, "b": "y"}"#),
                Some(r#"{"b": "no-a"}"#),
                None,
            ],
        );
        let direct_child = direct_shredded_a_typed_child(&source);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                source.data_type().clone(),
                true,
            )])),
            vec![source],
        )
        .expect("batch");

        let batch = materialize_variant_path_columns(
            batch,
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec_for("$.a", DataType::Float64, false)],
        )
        .expect("materialize");

        assert!(
            !Arc::ptr_eq(batch.column(0), &direct_child),
            "requested-type mismatch must use fallback instead of direct typed child"
        );
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(1), 99.0);
        assert!(values.is_null(2));
        assert!(values.is_null(3));
    }

    #[test]
    fn variant_path_strict_errors_but_try_returns_null_on_cast_failure() {
        let error = materialized_variant_path(false, true).expect_err("strict cast must fail");
        assert!(
            error.contains("variant_get"),
            "error names strict function: {error}"
        );

        let batch = materialized_variant_path(false, false).expect("try materialize");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            values.is_null(3),
            "try_variant_get cast failure row should be NULL"
        );
    }

    #[test]
    fn variant_path_missing_path_returns_null() {
        let batch = materialized_variant_path(false, false).expect("materialize");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values.is_null(2), "missing $.a should be NULL");
    }

    #[test]
    fn variant_path_hidden_source_read_slot_does_not_leak() {
        let batch = materialized_variant_path(false, false).expect("materialize");
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "v_a");
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn variant_path_shredded_fallback_matches_variant_get() {
        let batch = materialized_variant_path(true, false).expect("materialize shredded");
        let expected = expression_variant_get_result(true, false).expect("expr shredded");
        assert_int64_array_eq(batch.column(0), &expected);
    }

    #[test]
    fn convert_variant_columns_handles_shredded_struct() {
        let batch = batch_with_variant_struct(true);
        let out =
            convert_variant_columns(&[types::TPrimitiveType::VARIANT], batch).expect("convert");
        assert_eq!(out.column(0).data_type(), &DataType::LargeBinary);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(get_a_int(col.value(0)), Some(1));
        assert_eq!(get_a_int(col.value(1)), Some(99));
        assert!(col.is_null(4));
    }

    #[test]
    fn convert_variant_columns_passes_through_non_variant() {
        use arrow::array::Int64Array;
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
        let field = Field::new("x", DataType::Int64, false);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).expect("batch");
        let out = convert_variant_columns(&[types::TPrimitiveType::BIGINT], batch.clone())
            .expect("convert");
        assert_eq!(out.column(0).as_ref(), batch.column(0).as_ref());
    }

    #[test]
    fn shredded_parquet_file_round_trips_to_engine_form() {
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
        use std::collections::HashMap;
        use std::fs::File;

        let inner = test_variant_struct(true);
        let mut md = HashMap::new();
        md.insert(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        );
        md.insert("ARROW:extension:metadata".to_string(), String::new());
        md.insert("PARQUET:field_id".to_string(), "2".to_string());
        let field = Field::new("v", inner.data_type().clone(), true).with_metadata(md);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![inner]).expect("batch");

        let dir = std::env::temp_dir().join(format!(
            "nova_variant_read_test_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("t").len()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("shredded.parquet");
        {
            let f = File::create(&path).expect("create");
            let mut w = ArrowWriter::try_new(f, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        }

        let opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            File::open(&path).expect("open"),
            opts,
        )
        .expect("builder");
        let mut reader = builder.build().expect("build");
        let read_batch = reader.next().expect("one batch").expect("batch ok");

        // The file's variant column comes back as the shredded struct; the
        // engine conversion must reconstruct full variant rows from it.
        let out = convert_variant_columns(&[types::TPrimitiveType::VARIANT], read_batch)
            .expect("convert from file");
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(col.len(), 5);
        assert_eq!(get_a_int(col.value(0)), Some(1));
        assert_eq!(get_a_int(col.value(1)), Some(99));
        assert_eq!(get_a_int(col.value(2)), None);
        assert!(col.is_null(4));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
