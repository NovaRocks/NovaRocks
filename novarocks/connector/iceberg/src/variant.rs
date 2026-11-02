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

//! Iceberg/Parquet VARIANT physical wire conversion.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeBinaryBuilder,
    StructArray,
};
use novarocks_types::value::variant::VariantValue;
use parquet::variant::{VariantArray, unshred_variant};

fn binary_value_at_any(array: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(array.value(row)));
    }
    Err(format!(
        "expected a binary array for variant metadata/value, got {:?}",
        array.data_type()
    ))
}

/// Collapse an unshredded or shredded Parquet VARIANT struct into NovaRocks'
/// canonical `[size:u32 LE | metadata | value]` LargeBinary representation.
pub fn collapse_variant_struct_to_largebinary(
    source_array: &ArrayRef,
    column_name: &str,
) -> Result<ArrayRef, String> {
    let struct_array = source_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| format!("expected StructArray for variant column `{column_name}`"))?;
    let has_typed_value = struct_array
        .fields()
        .iter()
        .any(|field| field.name() == "typed_value");

    let unshredded_holder;
    let (metadata_column, value_column): (ArrayRef, ArrayRef) = if has_typed_value {
        let variant = VariantArray::try_new(source_array.as_ref()).map_err(|error| {
            format!("variant column `{column_name}`: invalid shredded layout: {error}")
        })?;
        unshredded_holder = unshred_variant(&variant)
            .map_err(|error| format!("variant column `{column_name}`: unshred failed: {error}"))?;
        (
            unshredded_holder.metadata_column().clone(),
            unshredded_holder.value_column().clone(),
        )
    } else {
        let mut metadata_index = None;
        let mut value_index = None;
        for (index, field) in struct_array.fields().iter().enumerate() {
            match field.name().as_str() {
                "metadata" => metadata_index = Some(index),
                "value" => value_index = Some(index),
                _ => {}
            }
        }
        let metadata_index = metadata_index.ok_or_else(|| {
            format!("variant column `{column_name}`: struct missing metadata field")
        })?;
        let value_index = value_index
            .ok_or_else(|| format!("variant column `{column_name}`: struct missing value field"))?;
        (
            struct_array.column(metadata_index).clone(),
            struct_array.column(value_index).clone(),
        )
    };

    let mut builder = LargeBinaryBuilder::new();
    for row in 0..struct_array.len() {
        if struct_array.is_null(row) {
            builder.append_null();
            continue;
        }
        match (
            binary_value_at_any(&metadata_column, row)?,
            binary_value_at_any(&value_column, row)?,
        ) {
            (Some(metadata), Some(value)) => {
                let serialized = VariantValue::create(metadata, value)
                    .map_err(|error| format!("variant column `{column_name}` row {row}: {error}"))?
                    .serialize();
                builder.append_value(serialized.as_slice());
            }
            _ => {
                return Err(format!(
                    "variant column `{column_name}` row {row}: missing metadata/value bytes (corrupt file or unsupported variant encoding)"
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_types::value::variant::{
        parse_variant_path, variant_query, variant_to_i64, variant_to_string,
    };
    use parquet::variant::{ShreddedSchemaBuilder, json_to_variant, shred_variant};
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
        let out = collapse_variant_struct_to_largebinary(read_batch.column(0), "v")
            .expect("convert from file");
        let col = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq!(col.len(), 5);
        assert_eq!(get_a_int(col.value(0)), Some(1));
        assert_eq!(get_a_int(col.value(1)), Some(99));
        assert_eq!(get_a_int(col.value(2)), None);
        assert!(col.is_null(4));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
