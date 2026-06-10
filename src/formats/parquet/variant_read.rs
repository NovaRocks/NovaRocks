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

use arrow::datatypes::DataType;

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
}
