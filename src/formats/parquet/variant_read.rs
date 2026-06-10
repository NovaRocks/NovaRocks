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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeBinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use parquet::variant::{VariantArray, unshred_variant};

use crate::exec::variant::VariantValue;

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
        let variant = VariantArray::try_new(source_array.as_ref()).map_err(|e| {
            format!("variant column `{column_name}`: invalid shredded layout: {e}")
        })?;
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
        let value_idx = value_idx.ok_or_else(|| {
            format!("variant column `{column_name}`: struct missing value field")
        })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields};

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

    use arrow::array::{LargeBinaryArray, StringArray};
    use parquet::variant::{ShreddedSchemaBuilder, json_to_variant, shred_variant};

    use crate::exec::variant::{parse_variant_path, variant_query, variant_to_i64, variant_to_string};

    /// Build the canonical 5-row test column from JSON, optionally shredded
    /// on path `a` as Int64. Rows: shredded int, shredded int, missing `a`,
    /// wrong-typed `a`, SQL NULL.
    fn test_variant_struct(shredded: bool) -> ArrayRef {
        let json: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"a": 1, "b": "x"}"#),
            Some(r#"{"a": 99, "b": "y"}"#),
            Some(r#"{"b": "no-a"}"#),
            Some(r#"{"a": "not-an-int"}"#),
            None,
        ]));
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
        assert!(!out.is_null(3), "row 3 (wrong-typed a) round-trips without error");
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
            assert_eq!(shredded.is_null(row), plain.is_null(row), "row {row} nullness");
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
        let err = collapse_variant_struct_to_largebinary(&struct_arr, "v")
            .expect_err("must fail fast");
        assert!(err.contains("v"), "error names the column: {err}");
        assert!(
            err.contains("missing metadata/value"),
            "error says what is wrong: {err}"
        );
    }
}
