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
//! Recursive decoder for StarRocks complex columns.
//!
//! This module decodes one projected column from one segment into an Arrow array.
//! It supports scalar leaves plus container assembly for:
//! - ARRAY: element child + optional null child + length child
//! - MAP: key child + value child + optional null child + length child
//! - STRUCT: field children + optional null child
//!
//! Notes:
//! - Length children store per-row element counts (not cumulative offsets).
//! - Parent null flags are stored as dedicated tinyint child columns.
//! - JSON/VARIANT remain unsupported.

use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, MapArray, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

use crate::formats::starrocks::plan::StarRocksNativeSchemaColumnPlan;
use crate::formats::starrocks::segment::StarRocksSegmentColumnMeta;

use super::column_decode::decode_column_values_by_total_rows;
use super::column_state::{OutputColumnData, OutputColumnKind};
use super::constants::{LOGICAL_TYPE_INT, LOGICAL_TYPE_TINYINT};
use super::page::DecodedPageValuePayload;
use super::schema_map::{
    decimal_output_meta_from_arrow_type, expected_logical_type_from_schema_type,
    is_char_schema_type,
};

/// Decode one projected column from one segment into an Arrow array.
pub(super) fn decode_column_array_for_segment(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    schema: &StarRocksNativeSchemaColumnPlan,
    output_data_type: &DataType,
    output_name: &str,
    expected_rows: usize,
) -> Result<ArrayRef, String> {
    decode_column_array_inner(
        segment_path,
        segment_bytes,
        column_meta,
        schema,
        output_data_type,
        output_name,
        expected_rows,
    )
}

fn decode_column_array_inner(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    schema: &StarRocksNativeSchemaColumnPlan,
    output_data_type: &DataType,
    output_name: &str,
    expected_rows: usize,
) -> Result<ArrayRef, String> {
    if let Some(kind) = OutputColumnKind::from_arrow_type(output_data_type) {
        return decode_scalar_array(
            segment_path,
            segment_bytes,
            column_meta,
            schema,
            kind,
            output_data_type,
            output_name,
            expected_rows,
        );
    }

    let schema_type = schema.schema_type.trim().to_ascii_uppercase();
    match output_data_type {
        DataType::List(item_field) => {
            if schema_type != "ARRAY" {
                return Err(format!(
                    "ARRAY schema type mismatch in complex decoder: segment={}, output_column={}, schema_type={}, output_type={:?}",
                    segment_path, output_name, schema.schema_type, output_data_type
                ));
            }
            if schema.children.len() != 1 {
                return Err(format!(
                    "ARRAY schema child mismatch in complex decoder: segment={}, output_column={}, schema_children={}, expected=1",
                    segment_path,
                    output_name,
                    schema.children.len()
                ));
            }
            let is_nullable = column_meta.is_nullable.ok_or_else(|| {
                format!(
                    "missing nullable flag in ARRAY segment meta: segment={}, output_column={}",
                    segment_path, output_name
                )
            })?;
            let (element_meta, null_meta, length_meta) = if is_nullable {
                if column_meta.children.len() != 3 {
                    return Err(format!(
                        "nullable ARRAY child count mismatch in segment meta: segment={}, output_column={}, children={}, expected=3",
                        segment_path,
                        output_name,
                        column_meta.children.len()
                    ));
                }
                (
                    &column_meta.children[0],
                    Some(&column_meta.children[1]),
                    &column_meta.children[2],
                )
            } else {
                if column_meta.children.len() != 2 {
                    return Err(format!(
                        "non-nullable ARRAY child count mismatch in segment meta: segment={}, output_column={}, children={}, expected=2",
                        segment_path,
                        output_name,
                        column_meta.children.len()
                    ));
                }
                (&column_meta.children[0], None, &column_meta.children[1])
            };

            let lengths = decode_length_values(
                segment_path,
                segment_bytes,
                length_meta,
                expected_rows,
                &format!("{output_name}.__array_lengths"),
            )?;
            let offsets = lengths_to_offsets(&lengths, segment_path, output_name)?;
            let element_rows = offsets.last().copied().ok_or_else(|| {
                format!(
                    "ARRAY offsets are empty in complex decoder: segment={}, output_column={}",
                    segment_path, output_name
                )
            })? as usize;
            let element_array = decode_column_array_inner(
                segment_path,
                segment_bytes,
                element_meta,
                &schema.children[0],
                item_field.data_type(),
                &format!("{output_name}.item"),
                element_rows,
            )?;
            let null_buffer = if let Some(meta) = null_meta {
                let flags = decode_parent_null_flags(
                    segment_path,
                    segment_bytes,
                    meta,
                    expected_rows,
                    &format!("{output_name}.__array_nulls"),
                )?;
                null_buffer_from_flags(&flags, segment_path, output_name)?
            } else {
                None
            };
            let list = ListArray::new(
                item_field.clone(),
                OffsetBuffer::new(offsets.into()),
                element_array,
                null_buffer,
            );
            Ok(Arc::new(list))
        }
        DataType::Map(map_field, ordered) => {
            if schema_type != "MAP" {
                return Err(format!(
                    "MAP schema type mismatch in complex decoder: segment={}, output_column={}, schema_type={}, output_type={:?}",
                    segment_path, output_name, schema.schema_type, output_data_type
                ));
            }
            if schema.children.len() != 2 {
                return Err(format!(
                    "MAP schema child mismatch in complex decoder: segment={}, output_column={}, schema_children={}, expected=2",
                    segment_path,
                    output_name,
                    schema.children.len()
                ));
            }
            let DataType::Struct(entry_fields) = map_field.data_type() else {
                return Err(format!(
                    "MAP entries type mismatch in complex decoder: segment={}, output_column={}, entries_type={:?}, expected=Struct(key,value)",
                    segment_path,
                    output_name,
                    map_field.data_type()
                ));
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "MAP entries field count mismatch in complex decoder: segment={}, output_column={}, entries_fields={}, expected=2",
                    segment_path,
                    output_name,
                    entry_fields.len()
                ));
            }
            let is_nullable = column_meta.is_nullable.ok_or_else(|| {
                format!(
                    "missing nullable flag in MAP segment meta: segment={}, output_column={}",
                    segment_path, output_name
                )
            })?;
            let (key_meta, value_meta, null_meta, length_meta) = if is_nullable {
                if column_meta.children.len() != 4 {
                    return Err(format!(
                        "nullable MAP child count mismatch in segment meta: segment={}, output_column={}, children={}, expected=4",
                        segment_path,
                        output_name,
                        column_meta.children.len()
                    ));
                }
                (
                    &column_meta.children[0],
                    &column_meta.children[1],
                    Some(&column_meta.children[2]),
                    &column_meta.children[3],
                )
            } else {
                if column_meta.children.len() != 3 {
                    return Err(format!(
                        "non-nullable MAP child count mismatch in segment meta: segment={}, output_column={}, children={}, expected=3",
                        segment_path,
                        output_name,
                        column_meta.children.len()
                    ));
                }
                (
                    &column_meta.children[0],
                    &column_meta.children[1],
                    None,
                    &column_meta.children[2],
                )
            };

            let lengths = decode_length_values(
                segment_path,
                segment_bytes,
                length_meta,
                expected_rows,
                &format!("{output_name}.__map_lengths"),
            )?;
            let offsets = lengths_to_offsets(&lengths, segment_path, output_name)?;
            let entry_rows = offsets.last().copied().ok_or_else(|| {
                format!(
                    "MAP offsets are empty in complex decoder: segment={}, output_column={}",
                    segment_path, output_name
                )
            })? as usize;
            let keys = decode_column_array_inner(
                segment_path,
                segment_bytes,
                key_meta,
                &schema.children[0],
                entry_fields[0].data_type(),
                &format!("{output_name}.key"),
                entry_rows,
            )?;
            let values = decode_column_array_inner(
                segment_path,
                segment_bytes,
                value_meta,
                &schema.children[1],
                entry_fields[1].data_type(),
                &format!("{output_name}.value"),
                entry_rows,
            )?;
            let mut entry_fields_for_values = entry_fields.clone();
            let mut map_field_for_values = map_field.clone();
            if keys.null_count() > 0 && !entry_fields[0].is_nullable() {
                let mut adjusted = entry_fields.iter().cloned().collect::<Vec<_>>();
                adjusted[0] = Arc::new(Field::new(
                    entry_fields[0].name(),
                    entry_fields[0].data_type().clone(),
                    true,
                ));
                entry_fields_for_values = arrow::datatypes::Fields::from(adjusted);
                map_field_for_values = Arc::new(Field::new(
                    map_field.name(),
                    DataType::Struct(entry_fields_for_values.clone()),
                    map_field.is_nullable(),
                ));
            }
            let entries =
                StructArray::new(entry_fields_for_values.clone(), vec![keys, values], None);
            let null_buffer = if let Some(meta) = null_meta {
                let flags = decode_parent_null_flags(
                    segment_path,
                    segment_bytes,
                    meta,
                    expected_rows,
                    &format!("{output_name}.__map_nulls"),
                )?;
                null_buffer_from_flags(&flags, segment_path, output_name)?
            } else {
                None
            };
            let map = MapArray::try_new(
                map_field_for_values,
                OffsetBuffer::new(offsets.into()),
                entries,
                null_buffer,
                *ordered,
            )
            .map_err(|e| {
                format!(
                    "build MAP array failed in complex decoder: segment={}, output_column={}, error={}",
                    segment_path, output_name, e
                )
            })?;
            Ok(Arc::new(map))
        }
        DataType::Struct(struct_fields) => {
            if schema_type != "STRUCT" {
                return Err(format!(
                    "STRUCT schema type mismatch in complex decoder: segment={}, output_column={}, schema_type={}, output_type={:?}",
                    segment_path, output_name, schema.schema_type, output_data_type
                ));
            }
            if schema.children.len() != struct_fields.len() {
                return Err(format!(
                    "STRUCT schema child mismatch in complex decoder: segment={}, output_column={}, schema_children={}, output_fields={}",
                    segment_path,
                    output_name,
                    schema.children.len(),
                    struct_fields.len()
                ));
            }
            let is_nullable = column_meta.is_nullable.ok_or_else(|| {
                format!(
                    "missing nullable flag in STRUCT segment meta: segment={}, output_column={}",
                    segment_path, output_name
                )
            })?;
            let expected_children = struct_fields.len() + usize::from(is_nullable);
            if column_meta.children.len() != expected_children {
                return Err(format!(
                    "STRUCT child count mismatch in segment meta: segment={}, output_column={}, children={}, expected={}",
                    segment_path,
                    output_name,
                    column_meta.children.len(),
                    expected_children
                ));
            }

            let mut field_arrays = Vec::with_capacity(struct_fields.len());
            for (idx, field) in struct_fields.iter().enumerate() {
                let child_array = decode_column_array_inner(
                    segment_path,
                    segment_bytes,
                    &column_meta.children[idx],
                    &schema.children[idx],
                    field.data_type(),
                    &format!("{output_name}.{}", field.name()),
                    expected_rows,
                )?;
                field_arrays.push(child_array);
            }
            let null_buffer = if is_nullable {
                let flags = decode_parent_null_flags(
                    segment_path,
                    segment_bytes,
                    &column_meta.children[struct_fields.len()],
                    expected_rows,
                    &format!("{output_name}.__struct_nulls"),
                )?;
                null_buffer_from_flags(&flags, segment_path, output_name)?
            } else {
                None
            };
            let struct_array = StructArray::new(struct_fields.clone(), field_arrays, null_buffer);
            Ok(Arc::new(struct_array))
        }
        other => Err(format!(
            "unsupported complex output type in native starrocks reader: segment={}, output_column={}, schema_type={}, output_type={:?}",
            segment_path, output_name, schema.schema_type, other
        )),
    }
}

fn decode_scalar_array(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    schema: &StarRocksNativeSchemaColumnPlan,
    output_kind: OutputColumnKind,
    output_data_type: &DataType,
    output_name: &str,
    expected_rows: usize,
) -> Result<ArrayRef, String> {
    if expected_rows == 0 {
        let data = OutputColumnData::with_capacity(output_kind, 0);
        return data.into_array(
            &[],
            false,
            output_name,
            decimal_output_meta_from_arrow_type(output_data_type),
        );
    }

    let expected_logical_type =
        expected_logical_type_from_schema_type(&schema.schema_type).ok_or_else(|| {
            format!(
                "unsupported scalar schema type in native starrocks reader: segment={}, output_column={}, schema_type={}",
                segment_path, output_name, schema.schema_type
            )
        })?;
    let decoded_page = decode_column_values_by_total_rows(
        segment_path,
        segment_bytes,
        column_meta,
        expected_logical_type,
        output_kind.arrow_type_name(),
        output_kind,
        output_name,
        expected_rows,
    )?;
    if decoded_page.num_values != expected_rows {
        return Err(format!(
            "segment row count mismatch in data page: segment={}, output_column={}, expected_rows={}, actual_rows={}",
            segment_path, output_name, expected_rows, decoded_page.num_values
        ));
    }

    let mut data = OutputColumnData::with_capacity(output_kind, expected_rows);
    match decoded_page.payload {
        DecodedPageValuePayload::Fixed {
            value_bytes,
            elem_size,
        } => {
            data.append_from_bytes(&value_bytes, elem_size, segment_path, output_name)?;
        }
        DecodedPageValuePayload::Variable { values } => {
            data.append_variable_values(
                values,
                segment_path,
                output_name,
                is_char_schema_type(&schema.schema_type),
            )?;
        }
    }

    let (null_flags, has_null) = if let Some(page_null_flags) = decoded_page.null_flags {
        if page_null_flags.len() != expected_rows {
            return Err(format!(
                "decoded null flag length mismatch in data page: segment={}, output_column={}, expected_rows={}, actual_null_flags={}",
                segment_path,
                output_name,
                expected_rows,
                page_null_flags.len()
            ));
        }
        (page_null_flags, true)
    } else {
        (vec![0; expected_rows], false)
    };
    data.into_array(
        &null_flags,
        has_null,
        output_name,
        decimal_output_meta_from_arrow_type(output_data_type),
    )
}

fn decode_length_values(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    expected_rows: usize,
    output_name: &str,
) -> Result<Vec<i32>, String> {
    if expected_rows == 0 {
        return Ok(Vec::new());
    }
    let decoded_page = decode_column_values_by_total_rows(
        segment_path,
        segment_bytes,
        column_meta,
        LOGICAL_TYPE_INT,
        "Int32",
        OutputColumnKind::Int32,
        output_name,
        expected_rows,
    )?;
    if decoded_page.num_values != expected_rows {
        return Err(format!(
            "complex length row count mismatch in data page: segment={}, output_column={}, expected_rows={}, actual_rows={}",
            segment_path, output_name, expected_rows, decoded_page.num_values
        ));
    }
    if decoded_page.null_flags.is_some() {
        return Err(format!(
            "unexpected null map in complex length column: segment={}, output_column={}",
            segment_path, output_name
        ));
    }
    let DecodedPageValuePayload::Fixed {
        value_bytes,
        elem_size,
    } = decoded_page.payload
    else {
        return Err(format!(
            "unexpected variable payload in complex length column: segment={}, output_column={}",
            segment_path, output_name
        ));
    };
    if elem_size != 4 {
        return Err(format!(
            "invalid element size in complex length column: segment={}, output_column={}, elem_size={}, expected=4",
            segment_path, output_name, elem_size
        ));
    }
    if value_bytes.len() % 4 != 0 {
        return Err(format!(
            "invalid length payload size in complex decoder: segment={}, output_column={}, bytes={}",
            segment_path,
            output_name,
            value_bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(expected_rows);
    for (idx, chunk) in value_bytes.chunks_exact(4).enumerate() {
        let length = i32::from_le_bytes(
            chunk
                .try_into()
                .map_err(|_| "convert complex length bytes failed".to_string())?,
        );
        if length < 0 {
            return Err(format!(
                "negative length in complex decoder: segment={}, output_column={}, row_index={}, length={}",
                segment_path, output_name, idx, length
            ));
        }
        out.push(length);
    }
    Ok(out)
}

fn decode_parent_null_flags(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    expected_rows: usize,
    output_name: &str,
) -> Result<Vec<u8>, String> {
    if expected_rows == 0 {
        return Ok(Vec::new());
    }
    let decoded_page = decode_column_values_by_total_rows(
        segment_path,
        segment_bytes,
        column_meta,
        LOGICAL_TYPE_TINYINT,
        "Int8",
        OutputColumnKind::Int8,
        output_name,
        expected_rows,
    )?;
    if decoded_page.num_values != expected_rows {
        return Err(format!(
            "complex null flag row count mismatch in data page: segment={}, output_column={}, expected_rows={}, actual_rows={}",
            segment_path, output_name, expected_rows, decoded_page.num_values
        ));
    }
    if decoded_page.null_flags.is_some() {
        return Err(format!(
            "unexpected nested null map in complex null flag column: segment={}, output_column={}",
            segment_path, output_name
        ));
    }
    let DecodedPageValuePayload::Fixed {
        value_bytes,
        elem_size,
    } = decoded_page.payload
    else {
        return Err(format!(
            "unexpected variable payload in complex null flag column: segment={}, output_column={}",
            segment_path, output_name
        ));
    };
    if elem_size != 1 {
        return Err(format!(
            "invalid element size in complex null flag column: segment={}, output_column={}, elem_size={}, expected=1",
            segment_path, output_name, elem_size
        ));
    }
    if value_bytes.len() != expected_rows {
        return Err(format!(
            "invalid null flag payload size in complex decoder: segment={}, output_column={}, expected_rows={}, bytes={}",
            segment_path,
            output_name,
            expected_rows,
            value_bytes.len()
        ));
    }
    for (idx, flag) in value_bytes.iter().enumerate() {
        if *flag != 0 && *flag != 1 {
            return Err(format!(
                "invalid null flag value in complex decoder: segment={}, output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                segment_path, output_name, idx, flag
            ));
        }
    }
    Ok(value_bytes)
}

fn lengths_to_offsets(
    lengths: &[i32],
    segment_path: &str,
    output_name: &str,
) -> Result<Vec<i32>, String> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0_i32);
    let mut current = 0_i64;
    for (idx, length) in lengths.iter().enumerate() {
        if *length < 0 {
            return Err(format!(
                "negative length in complex decoder: segment={}, output_column={}, row_index={}, length={}",
                segment_path, output_name, idx, length
            ));
        }
        current += i64::from(*length);
        if current > i32::MAX as i64 {
            return Err(format!(
                "complex offset overflow: segment={}, output_column={}, row_index={}, offset={}",
                segment_path, output_name, idx, current
            ));
        }
        offsets.push(current as i32);
    }
    Ok(offsets)
}

fn null_buffer_from_flags(
    flags: &[u8],
    segment_path: &str,
    output_name: &str,
) -> Result<Option<arrow_buffer::NullBuffer>, String> {
    let mut builder = NullBufferBuilder::new(flags.len());
    let mut has_null = false;
    for (idx, flag) in flags.iter().enumerate() {
        match *flag {
            0 => builder.append_non_null(),
            1 => {
                builder.append_null();
                has_null = true;
            }
            other => {
                return Err(format!(
                    "invalid null flag value in complex decoder: segment={}, output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                    segment_path, output_name, idx, other
                ));
            }
        }
    }
    if has_null {
        Ok(builder.finish())
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lengths_to_offsets_builds_prefix_sum() {
        let offsets = lengths_to_offsets(&[0, 2, 3, 1], "s.dat", "c_arr").expect("offsets");
        assert_eq!(offsets, vec![0, 0, 2, 5, 6]);
    }

    #[test]
    fn lengths_to_offsets_rejects_negative_length() {
        let err = lengths_to_offsets(&[1, -1], "s.dat", "c_arr").expect_err("negative length");
        assert!(err.contains("negative length"), "err={}", err);
    }

    #[test]
    fn null_buffer_from_flags_rejects_invalid_value() {
        let err = null_buffer_from_flags(&[0, 2, 1], "s.dat", "c_struct").expect_err("bad flag");
        assert!(err.contains("invalid null flag value"), "err={}", err);
    }
}
