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

//! Write-side glue for Iceberg v3 variant columns.
//!
//! NovaRocks carries variants in execution as a single `LargeBinary`
//! per column whose bytes are `[size:u32 LE | metadata | value]` (the
//! `VariantValue::serialize` form). Iceberg parquet writers expect the
//! parent column to be a `StructArray { metadata: BinaryArray (req),
//! value: BinaryArray (req) }`; this module bridges the two right
//! before `iceberg::ParquetWriter::write`.

use std::collections::HashMap;

use arrow::datatypes::DataType;
use iceberg::spec::SchemaRef;

pub(crate) const VARIANT_SHREDDING_PROPERTY_PREFIX: &str = "write.parquet.variant-shredding.";

#[derive(Clone, Debug, Default)]
pub(crate) struct VariantShreddingConfig {
    specs_by_index: HashMap<usize, VariantShreddingSpec>,
}

impl VariantShreddingConfig {
    pub(crate) fn is_empty(&self) -> bool {
        self.specs_by_index.is_empty()
    }

    pub(crate) fn spec_for_index(&self, index: usize) -> Option<&VariantShreddingSpec> {
        self.specs_by_index.get(&index)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantShreddingSpec {
    as_type: DataType,
}

impl VariantShreddingSpec {
    pub(crate) fn as_type(&self) -> &DataType {
        &self.as_type
    }
}

pub(crate) fn parse_variant_shredding_properties(
    properties: &HashMap<String, String>,
    iceberg_schema: &SchemaRef,
) -> Result<VariantShreddingConfig, String> {
    use iceberg::spec::{PrimitiveType, Type};

    let mut columns_by_name = HashMap::new();
    for (idx, field) in iceberg_schema.as_struct().fields().iter().enumerate() {
        let key = novarocks_catalog::identifier::normalize_identifier(&field.name)?;
        columns_by_name.insert(key, (idx, field));
    }

    let mut specs_by_index = HashMap::new();
    for (key, value) in properties {
        let Some(column_name) = key.strip_prefix(VARIANT_SHREDDING_PROPERTY_PREFIX) else {
            continue;
        };
        let normalized_column = novarocks_catalog::identifier::normalize_identifier(column_name)
            .map_err(|e| format!("invalid variant shredding property `{key}`: {e}"))?;
        let Some((idx, field)) = columns_by_name.get(&normalized_column).copied() else {
            return Err(format!(
                "invalid variant shredding property `{key}`: column `{column_name}` does not exist"
            ));
        };
        if !matches!(
            field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Variant)
        ) {
            return Err(format!(
                "invalid variant shredding property `{key}`: column `{}` is not VARIANT",
                field.name
            ));
        }
        let spec = parse_variant_shredding_spec(key, value)?;
        if specs_by_index.insert(idx, spec).is_some() {
            return Err(format!(
                "duplicate variant shredding property for column `{}`",
                field.name
            ));
        }
    }

    Ok(VariantShreddingConfig { specs_by_index })
}

pub(crate) fn apply_variant_shredding_to_arrow_schema(
    annotated_schema: &arrow::datatypes::SchemaRef,
    shredding_config: &VariantShreddingConfig,
) -> Result<arrow::datatypes::SchemaRef, String> {
    use std::sync::Arc;

    if shredding_config.is_empty() {
        return Ok(Arc::clone(annotated_schema));
    }

    let fields = annotated_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let Some(spec) = shredding_config.spec_for_index(idx) else {
                return Ok(field.clone());
            };
            let data_type = shredded_variant_data_type(spec.as_type()).map_err(|e| {
                format!(
                    "derive shredded Arrow type for column `{}`: {e}",
                    field.name()
                )
            })?;
            Ok(field.as_ref().clone().with_data_type(data_type).into())
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        annotated_schema.metadata().clone(),
    )))
}

fn shredded_variant_data_type(as_type: &DataType) -> Result<DataType, String> {
    use arrow::array::{Array, ArrayRef, BinaryArray, StructArray};
    use arrow::datatypes::{Field, Fields};
    use std::sync::Arc;

    let metadata = BinaryArray::from_iter_values(std::iter::empty::<&[u8]>());
    let value = BinaryArray::from_iter_values(std::iter::empty::<&[u8]>());
    let unshredded = StructArray::new(
        Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]),
        vec![Arc::new(metadata) as ArrayRef, Arc::new(value) as ArrayRef],
        None,
    );
    let variant = parquet::variant::VariantArray::try_new(&unshredded)
        .map_err(|e| format!("build empty VariantArray: {e}"))?;
    let shredded = parquet::variant::shred_variant(&variant, as_type)
        .map_err(|e| format!("shred empty VariantArray: {e}"))?;
    Ok(shredded.into_inner().data_type().clone())
}

fn parse_variant_shredding_spec(
    property_key: &str,
    raw: &str,
) -> Result<VariantShreddingSpec, String> {
    use parquet::variant::ShreddedSchemaBuilder;

    let mut builder = ShreddedSchemaBuilder::new();
    let entries = split_property_entries(raw).map_err(|e| format!("{property_key}: {e}"))?;
    if entries.is_empty() {
        return Err(format!(
            "{property_key}: expected at least one `<path> <type>` entry"
        ));
    }

    for entry in entries {
        let (path, ty) = parse_path_type_entry(property_key, entry)?;
        let data_type = parse_shredding_sql_type(property_key, ty)?;
        builder = builder
            .with_path(path, &data_type)
            .map_err(|e| format!("{property_key}: invalid variant path `{path}`: {e}"))?;
    }

    Ok(VariantShreddingSpec {
        as_type: builder.build(),
    })
}

fn split_property_entries(raw: &str) -> Result<Vec<&str>, String> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    for (idx, ch) in raw.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err("unbalanced ')' in variant shredding property".to_string());
                }
            }
            ',' if depth == 0 => {
                let item = raw[start..idx].trim();
                if !item.is_empty() {
                    entries.push(item);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("unbalanced '(' in variant shredding property".to_string());
    }
    let tail = raw[start..].trim();
    if !tail.is_empty() {
        entries.push(tail);
    }
    Ok(entries)
}

fn parse_path_type_entry<'a>(
    property_key: &str,
    entry: &'a str,
) -> Result<(&'a str, &'a str), String> {
    let Some(split_at) = entry.find(char::is_whitespace) else {
        return Err(format!(
            "{property_key}: expected `<path> <type>` entry, got `{entry}`"
        ));
    };
    let path = entry[..split_at].trim();
    let ty = entry[split_at..].trim();
    if path.is_empty() || ty.is_empty() {
        return Err(format!(
            "{property_key}: expected `<path> <type>` entry, got `{entry}`"
        ));
    }
    if path.contains('[') || path.contains(']') {
        return Err(format!(
            "{property_key}: array-index variant shredding path `{path}` is not supported"
        ));
    }
    let path = path.strip_prefix("$.").unwrap_or(path);
    if path.is_empty() || path.starts_with('$') {
        return Err(format!(
            "{property_key}: invalid variant shredding path `{path}`"
        ));
    }
    Ok((path, ty))
}

fn parse_shredding_sql_type(property_key: &str, raw: &str) -> Result<DataType, String> {
    use novarocks_catalog::schema::SqlType;

    let sql_type = crate::sql::parser::dialect::create_table::parse_sql_type_string(raw)
        .map_err(|e| format!("{property_key}: invalid shredding type `{raw}`: {e}"))?;
    match sql_type {
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::BigInt => Ok(DataType::Int64),
        SqlType::Double => Ok(DataType::Float64),
        SqlType::String => Ok(DataType::Utf8),
        SqlType::Date => Ok(DataType::Date32),
        other => Err(format!(
            "{property_key}: unsupported variant shredding type `{raw}` ({other:?}); supported types are boolean, bigint, double, string, date"
        )),
    }
}

/// Returns the offsets within a `[metadata|value]` payload at which the
/// metadata segment ends. Mirrors the parsing in
/// `src/exec/variant.rs::load_metadata` but only computes the length —
/// it deliberately does not validate the value segment.
///
/// `payload` must be the bytes AFTER the leading `u32` size header.
pub(crate) fn metadata_byte_len(payload: &[u8]) -> Result<usize, String> {
    // Mirror src/exec/variant.rs::load_metadata, but stop at the metadata
    // segment instead of returning the full slice.
    const HEADER: usize = 1;
    const VERSION_MASK: u8 = 0b0000_1111;
    const OFFSET_SIZE_MASK: u8 = 0b1100_0000;
    const OFFSET_SIZE_SHIFT: u8 = 6;

    if payload.len() < HEADER + 1 {
        return Err(format!(
            "variant metadata too short: {} bytes",
            payload.len()
        ));
    }
    let header = payload[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(format!("unsupported variant metadata version: {version}"));
    }
    let offset_size = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    if !(1..=4).contains(&offset_size) {
        return Err(format!(
            "invalid variant metadata offset size: {offset_size}"
        ));
    }
    if payload.len() < HEADER + offset_size as usize {
        return Err("variant metadata too short to contain dict_size".to_string());
    }
    let dict_size = read_le_u32(&payload[HEADER..], offset_size)? as usize;
    let offset_list_offset = HEADER + offset_size as usize;
    let last_offset_pos = offset_list_offset + dict_size * offset_size as usize;
    if last_offset_pos + offset_size as usize > payload.len() {
        return Err("variant metadata too short to contain offset list".to_string());
    }
    let last_data_size = read_le_u32(&payload[last_offset_pos..], offset_size)? as usize;
    let data_offset = offset_list_offset + (1 + dict_size) * offset_size as usize;
    let end = data_offset + last_data_size;
    if end > payload.len() {
        return Err(format!(
            "variant metadata end {end} exceeds payload {}",
            payload.len()
        ));
    }
    Ok(end)
}

fn read_le_u32(data: &[u8], size: u8) -> Result<u32, String> {
    if size == 0 || size > 4 {
        return Err("invalid little-endian size".to_string());
    }
    if data.len() < size as usize {
        return Err("variant metadata: not enough bytes for u32 read".to_string());
    }
    let mut out: u32 = 0;
    for (i, byte) in data.iter().copied().enumerate().take(size as usize) {
        out |= (byte as u32) << (8 * i);
    }
    Ok(out)
}

/// Returns the *top-level* arrow indices in the iceberg current
/// schema that correspond to `PrimitiveType::Variant` fields. Order
/// matches `iceberg_schema.as_struct().fields()`.
pub(crate) fn variant_field_indices(iceberg_schema: &SchemaRef) -> Vec<usize> {
    use iceberg::spec::{PrimitiveType, Type};
    iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, f)| match f.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Variant) => Some(idx),
            _ => None,
        })
        .collect()
}

use arrow::record_batch::RecordBatch;

pub(crate) fn transform_variant_columns_for_write(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
    variant_indices: &[usize],
    shredding_config: &VariantShreddingConfig,
) -> Result<RecordBatch, String> {
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, LargeBinaryArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use std::collections::HashSet;
    use std::sync::Arc;

    if batch.num_columns() != annotated_schema.fields().len() {
        return Err(format!(
            "variant_write: column count mismatch: batch={} annotated={}",
            batch.num_columns(),
            annotated_schema.fields().len()
        ));
    }

    let variant_set: HashSet<usize> = variant_indices.iter().copied().collect();
    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (idx, col) in batch.columns().iter().enumerate() {
        if !variant_set.contains(&idx) {
            out_columns.push(col.clone());
            continue;
        }
        let lb = col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                format!(
                    "variant_write: column {idx} expected LargeBinary, got {:?}",
                    col.data_type()
                )
            })?;

        let n = lb.len();
        let mut meta_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();
        let mut nulls = vec![true; n];

        #[allow(clippy::needless_range_loop)]
        for row in 0..n {
            if lb.is_null(row) {
                nulls[row] = false;
                meta_builder.append_value(&[] as &[u8]);
                value_builder.append_value(&[] as &[u8]);
                continue;
            }
            let raw = lb.value(row);
            if raw.len() < 4 {
                return Err(format!(
                    "variant_write: row {row} payload too short ({} bytes)",
                    raw.len()
                ));
            }
            let total = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
            if 4 + total > raw.len() {
                return Err(format!(
                    "variant_write: row {row} declared total {total} exceeds payload {}",
                    raw.len() - 4
                ));
            }
            let payload = &raw[4..4 + total];
            let m_len = metadata_byte_len(payload)?;
            meta_builder.append_value(&payload[..m_len]);
            value_builder.append_value(&payload[m_len..]);
        }

        let meta_arr: BinaryArray = meta_builder.finish();
        let value_arr: BinaryArray = value_builder.finish();
        let null_buffer = NullBuffer::from(nulls);

        // Use the annotated schema's variant field as the StructArray
        // type — this carries the `arrow.parquet.variant` extension metadata
        // that PATCH 6 attaches.
        let struct_field = annotated_schema.field(idx);
        let arrow::datatypes::DataType::Struct(child_fields) = struct_field.data_type() else {
            return Err(format!(
                "variant_write: annotated schema for variant index {idx} is not Struct"
            ));
        };
        let struct_arr = StructArray::new(
            child_fields.clone(),
            vec![
                Arc::new(meta_arr) as ArrayRef,
                Arc::new(value_arr) as ArrayRef,
            ],
            Some(null_buffer),
        );
        let out_arr = if let Some(spec) = shredding_config.spec_for_index(idx) {
            let variant = parquet::variant::VariantArray::try_new(&struct_arr)
                .map_err(|e| format!("variant_write: build VariantArray for column {idx}: {e}"))?;
            let shredded =
                parquet::variant::shred_variant(&variant, spec.as_type()).map_err(|e| {
                    format!("variant_write: shred variant column {idx} using table property: {e}")
                })?;
            Arc::new(shredded.into_inner()) as ArrayRef
        } else {
            Arc::new(struct_arr) as ArrayRef
        };
        out_columns.push(out_arr);
    }

    // Build the output schema by keeping the input batch's fields for all
    // non-variant columns and swapping in the annotated field only for
    // variant positions. This is necessary because this function runs before
    // annotate_batch: non-variant columns may still carry widened engine
    // types (e.g. INSERT SELECT id + 1 produces Int64 for an Int32 column).
    // Rebuilding against the full annotated_schema would make
    // RecordBatch::try_new reject such batches. annotate_batch reconciles
    // non-variant types afterward.
    let out_fields: Vec<arrow::datatypes::FieldRef> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, input_field)| {
            if variant_set.contains(&idx) {
                // Use the annotated field which carries the extension metadata
                // required by the parquet writer (ARROW:extension:name etc.).
                let field = annotated_schema.field(idx).as_ref().clone();
                if shredding_config.spec_for_index(idx).is_some() {
                    field
                        .with_data_type(out_columns[idx].data_type().clone())
                        .into()
                } else {
                    field.into()
                }
            } else {
                input_field.clone()
            }
        })
        .collect();
    let out_schema =
        arrow::datatypes::Schema::new_with_metadata(out_fields, batch.schema().metadata().clone());
    RecordBatch::try_new(std::sync::Arc::new(out_schema), out_columns)
        .map_err(|e| format!("variant_write: rebuild RecordBatch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_metadata_empty() -> Vec<u8> {
        // Version 1, dict_size = 0, single offset entry of 0.
        vec![0x01, 0x00, 0x00]
    }

    #[test]
    fn metadata_byte_len_empty_dict() {
        let m = build_metadata_empty();
        let payload: Vec<u8> = m
            .iter()
            .copied()
            .chain([/* value */ 0x00].iter().copied())
            .collect();
        assert_eq!(metadata_byte_len(&payload).expect("ok"), m.len());
    }

    #[test]
    fn metadata_byte_len_rejects_short_input() {
        let err = metadata_byte_len(&[0x01]).expect_err("must reject");
        assert!(
            err.to_lowercase().contains("metadata") || err.to_lowercase().contains("short"),
            "{err}"
        );
    }

    #[test]
    fn variant_field_indices_finds_variant_columns() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                    NestedField::optional(3, "s", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "v2", Type::Primitive(PrimitiveType::Variant)).into(),
                ])
                .build()
                .expect("schema"),
        );
        assert_eq!(variant_field_indices(&schema), vec![1, 3]);
    }

    #[test]
    fn variant_field_indices_returns_empty_when_no_variants() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        assert!(variant_field_indices(&schema).is_empty());
    }

    fn build_variant_payload_with_string(s: &str) -> Vec<u8> {
        // Replicates VariantValue with a short-string value but no metadata
        // dict (dict_size = 0). Sufficient for round-trip tests.
        let metadata = build_metadata_empty();
        // Short-string value: basic_type = 1 (ShortString), header byte =
        // (len << 2) | 0b01.
        let mut value = Vec::with_capacity(1 + s.len());
        let len = s.len();
        assert!(len < 64, "test helper limit");
        value.push(((len as u8) << 2) | 0b01);
        value.extend_from_slice(s.as_bytes());

        let total = (metadata.len() + value.len()) as u32;
        let mut out = Vec::with_capacity(4 + metadata.len() + value.len());
        out.extend_from_slice(&total.to_le_bytes());
        out.extend_from_slice(&metadata);
        out.extend_from_slice(&value);
        out
    }

    fn make_iceberg_schema(fields: Vec<iceberg::spec::NestedFieldRef>) -> iceberg::spec::SchemaRef {
        use std::sync::Arc;
        Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(fields)
                .build()
                .expect("schema"),
        )
    }

    fn make_annotated_arrow_schema(
        iceberg_schema: &iceberg::spec::SchemaRef,
    ) -> arrow::datatypes::SchemaRef {
        use std::sync::Arc;
        Arc::new(iceberg::arrow::schema_to_arrow_schema(iceberg_schema).expect("convert"))
    }

    #[test]
    fn transform_single_variant_column_one_row() {
        use arrow::array::{LargeBinaryArray, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let annotated = make_annotated_arrow_schema(&iceberg_schema);
        let raw = build_variant_payload_with_string("hi");
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::LargeBinary,
            true,
        )]));
        let arr = LargeBinaryArray::from_iter_values([raw.as_slice()]);
        let batch = RecordBatch::try_new(input_schema, vec![Arc::new(arr)]).expect("batch");

        let out =
            transform_variant_columns_for_write(&batch, &annotated, &[0], &Default::default())
                .expect("ok");
        assert_eq!(out.num_columns(), 1);
        let col = out.column(0);
        let s = col
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct");
        assert_eq!(s.fields().len(), 2);
        assert_eq!(s.fields()[0].name(), "metadata");
        assert_eq!(s.fields()[1].name(), "value");
        let meta_arr = s
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("binary");
        let val_arr = s
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("binary");
        // metadata = empty dict (3 bytes), value = short-string "hi" (1 + 2 bytes).
        assert_eq!(meta_arr.value(0), &[0x01, 0x00, 0x00]);
        assert_eq!(val_arr.value(0).len(), 3);
        assert_eq!(val_arr.value(0)[1..], *b"hi");
    }

    #[test]
    fn transform_handles_null_row_with_zero_length_children() {
        use arrow::array::{Array, LargeBinaryArray, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let annotated = make_annotated_arrow_schema(&iceberg_schema);
        let raw = build_variant_payload_with_string("a");
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::LargeBinary,
            true,
        )]));
        let arr = LargeBinaryArray::from(vec![Some(raw.as_slice()), None, Some(raw.as_slice())]);
        let batch = RecordBatch::try_new(input_schema, vec![Arc::new(arr)]).expect("batch");

        let out =
            transform_variant_columns_for_write(&batch, &annotated, &[0], &Default::default())
                .expect("ok");
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct");
        assert_eq!(s.len(), 3);
        assert!(s.is_valid(0));
        assert!(!s.is_valid(1)); // parent null
        assert!(s.is_valid(2));
        let meta = s
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("b");
        let val = s
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("b");
        // Children must NOT be marked null at the leaf level (Required) —
        // null parent rows carry zero-length placeholders.
        assert!(meta.is_valid(1));
        assert!(val.is_valid(1));
        assert_eq!(meta.value(1), &[] as &[u8]);
        assert_eq!(val.value(1), &[] as &[u8]);
    }

    #[test]
    fn transform_passes_through_non_variant_columns_unchanged() {
        use arrow::array::{Int32Array, LargeBinaryArray, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let annotated = make_annotated_arrow_schema(&iceberg_schema);
        let raw = build_variant_payload_with_string("x");
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::LargeBinary, true),
        ]));
        let id_arr = Int32Array::from(vec![Some(7)]);
        let v_arr = LargeBinaryArray::from_iter_values([raw.as_slice()]);
        let batch = RecordBatch::try_new(input_schema, vec![Arc::new(id_arr), Arc::new(v_arr)])
            .expect("batch");
        let out =
            transform_variant_columns_for_write(&batch, &annotated, &[1], &Default::default())
                .expect("ok");
        assert_eq!(out.num_columns(), 2);
        let id = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");
        assert_eq!(id.value(0), 7);
        let v = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct");
        assert_eq!(v.fields().len(), 2);
    }

    /// Regression: INSERT SELECT that widens a non-variant column (e.g. id + 1
    /// produces Int64 for an Int32 table column) must not fail. The function
    /// runs before annotate_batch, so non-variant columns may still carry
    /// widened engine types. The output schema for those columns must come from
    /// the INPUT batch, not from annotated_schema.
    #[test]
    fn transform_preserves_widened_non_variant_column_type() {
        use arrow::array::{Int64Array, LargeBinaryArray, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        // Annotated schema says id is Int32 (the table column type).
        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let annotated = make_annotated_arrow_schema(&iceberg_schema);

        // The engine batch has id as Int64 (widened by id + 1).
        let raw = build_variant_payload_with_string("x");
        let input_id_field = Field::new("id", DataType::Int64, true);
        let input_schema = Arc::new(Schema::new(vec![
            input_id_field.clone(),
            Field::new("v", DataType::LargeBinary, true),
        ]));
        let id_arr = Int64Array::from(vec![Some(42i64)]);
        let v_arr = LargeBinaryArray::from_iter_values([raw.as_slice()]);
        let batch = RecordBatch::try_new(input_schema, vec![Arc::new(id_arr), Arc::new(v_arr)])
            .expect("batch");

        // Must succeed despite id being Int64 vs annotated Int32.
        let out =
            transform_variant_columns_for_write(&batch, &annotated, &[1], &Default::default())
                .expect("transform must succeed for widened non-variant column");

        // (a) Succeeds (already asserted above).
        assert_eq!(out.num_columns(), 2);

        // (b) Output column 0 is still Int64 (untouched by this function).
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0 must be Int64");

        // (c) Output column 1 is a Struct (variant was transformed).
        let v = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("column 1 must be StructArray");
        assert_eq!(v.fields().len(), 2);

        // (d) Output schema: id field equals the INPUT batch field (Int64),
        //     v field carries the extension metadata from annotated_schema.
        assert_eq!(out.schema().field(0), &input_id_field);
        let out_schema = out.schema();
        let v_schema_field = out_schema.field(1);
        // The annotated variant field carries the extension metadata.
        assert_eq!(
            v_schema_field,
            annotated.field(1),
            "variant schema field must come from annotated_schema"
        );
    }

    #[test]
    fn transform_handles_two_adjacent_variant_columns() {
        use arrow::array::{Array, LargeBinaryArray, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "v1", Type::Primitive(PrimitiveType::Variant)).into(),
            NestedField::optional(2, "v2", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let annotated = make_annotated_arrow_schema(&iceberg_schema);
        let raw1 = build_variant_payload_with_string("a");
        let raw2 = build_variant_payload_with_string("bcd");
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::LargeBinary, true),
            Field::new("v2", DataType::LargeBinary, true),
        ]));
        let v1 = LargeBinaryArray::from_iter_values([raw1.as_slice()]);
        let v2 = LargeBinaryArray::from_iter_values([raw2.as_slice()]);
        let batch =
            RecordBatch::try_new(input_schema, vec![Arc::new(v1), Arc::new(v2)]).expect("batch");
        let out =
            transform_variant_columns_for_write(&batch, &annotated, &[0, 1], &Default::default())
                .expect("ok");
        let s1 = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let s2 = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        assert_eq!(s1.len(), 1);
        assert_eq!(s2.len(), 1);
    }

    #[test]
    fn parse_variant_shredding_properties_accepts_whitelisted_paths() {
        use arrow::datatypes::{DataType, Field, Fields};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::collections::HashMap;

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let props = HashMap::from([(
            "write.parquet.variant-shredding.v".to_string(),
            "a bigint, b.c string".to_string(),
        )]);

        let config = parse_variant_shredding_properties(&props, &iceberg_schema).expect("config");
        let spec = config.spec_for_index(1).expect("variant column spec");

        assert_eq!(
            spec.as_type(),
            &DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new(
                    "b",
                    DataType::Struct(Fields::from(vec![Field::new("c", DataType::Utf8, true)])),
                    true,
                ),
            ]))
        );
    }

    #[test]
    fn parse_variant_shredding_properties_rejects_non_variant_column() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::collections::HashMap;

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let props = HashMap::from([(
            "write.parquet.variant-shredding.id".to_string(),
            "a bigint".to_string(),
        )]);

        let err = parse_variant_shredding_properties(&props, &iceberg_schema)
            .expect_err("id is not variant");
        assert!(err.contains("variant-shredding.id"), "{err}");
        assert!(err.contains("VARIANT"), "{err}");
    }

    #[test]
    fn transform_variant_columns_for_write_shreds_configured_column() {
        use arrow::array::{
            Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, RecordBatch,
        };
        use arrow::datatypes::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use parquet::variant::json_to_variant;
        use std::collections::HashMap;
        use std::sync::Arc;

        fn binary_value(array: &ArrayRef, row: usize) -> Vec<u8> {
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                return arr.value(row).to_vec();
            }
            if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                return arr.value(row).to_vec();
            }
            if let Some(arr) = array.as_any().downcast_ref::<BinaryViewArray>() {
                return arr.value(row).to_vec();
            }
            panic!("unexpected binary array type: {:?}", array.data_type());
        }

        fn engine_variant_payload_from_json(json: &str) -> Vec<u8> {
            let json_array: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![json]));
            let variant = json_to_variant(&json_array).expect("json_to_variant");
            let inner = variant.into_inner();
            let metadata = binary_value(inner.column_by_name("metadata").unwrap(), 0);
            let value = binary_value(inner.column_by_name("value").unwrap(), 0);
            let total = (metadata.len() + value.len()) as u32;
            let mut out = Vec::with_capacity(4 + metadata.len() + value.len());
            out.extend_from_slice(&total.to_le_bytes());
            out.extend_from_slice(&metadata);
            out.extend_from_slice(&value);
            out
        }

        let iceberg_schema = make_iceberg_schema(vec![
            NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ]);
        let annotated = make_annotated_arrow_schema(&iceberg_schema);
        let props = HashMap::from([(
            "write.parquet.variant-shredding.v".to_string(),
            "a bigint".to_string(),
        )]);
        let config = parse_variant_shredding_properties(&props, &iceberg_schema).expect("config");

        let raw = engine_variant_payload_from_json(r#"{"a": 42, "b": "x"}"#);
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::LargeBinary,
            true,
        )]));
        let arr = LargeBinaryArray::from_iter_values([raw.as_slice()]);
        let batch = RecordBatch::try_new(input_schema, vec![Arc::new(arr)]).expect("batch");

        let out = transform_variant_columns_for_write(&batch, &annotated, &[0], &config)
            .expect("shredded transform");
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct");
        assert_eq!(s.fields().len(), 3);
        assert!(s.column_by_name("typed_value").is_some());
        assert_eq!(
            s.column_by_name("value").unwrap().data_type(),
            &DataType::BinaryView
        );
        assert_eq!(
            out.schema().field(0).metadata(),
            annotated.field(0).metadata(),
            "variant field-id and extension metadata must stay on the top-level field"
        );
    }
}
