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

use arrow_schema::{DataType, Field};
use novarocks_types::logical::{
    LogicalType, NR_LOGICAL_TYPE_KEY, field_with_logical_type, logical_type_of_field,
};
use novarocks_types::{undecorated_nested_type, wider_type};

fn logical(name: &str, ty: DataType, nullable: bool, domain: LogicalType) -> Field {
    field_with_logical_type(Field::new(name, ty, nullable), domain)
}

fn list(field: Field) -> DataType {
    DataType::List(Arc::new(field))
}

fn item(ty: &DataType) -> &Field {
    let DataType::List(field) = ty else {
        panic!("expected List");
    };
    field
}

fn fields(ty: &DataType) -> &[Arc<Field>] {
    let DataType::Struct(fields) = ty else {
        panic!("expected Struct");
    };
    fields.as_ref()
}

fn map(key: Field, value: Field, nullable: bool, sorted: bool) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "source_entries",
            DataType::Struct(vec![Arc::new(key), Arc::new(value)].into()),
            nullable,
        )),
        sorted,
    )
}

fn map_fields(ty: &DataType) -> &[Arc<Field>] {
    let DataType::Map(entries, sorted) = ty else {
        panic!("expected Map");
    };
    assert_eq!(entries.name(), "entries");
    assert!(!entries.is_nullable());
    assert!(!sorted);
    assert!(entries.metadata().is_empty());
    fields(entries.data_type())
}

fn assert_domain(field: &Field, expected: Option<LogicalType>) {
    assert_eq!(logical_type_of_field(field), expected);
    match expected {
        Some(domain) => {
            assert_eq!(field.metadata().len(), 1);
            assert_eq!(
                field.metadata()[NR_LOGICAL_TYPE_KEY],
                domain.metadata_value()
            );
        }
        None => assert!(field.metadata().is_empty()),
    }
}

#[test]
fn list_same_json_domain_survives_nullability_merge() {
    let left = list(logical("item", DataType::Utf8, false, LogicalType::Json));
    let right = list(logical("item", DataType::Utf8, true, LogicalType::Json));
    for merged in [wider_type(&left, &right), wider_type(&right, &left)] {
        assert_eq!(item(&merged).data_type(), &DataType::Utf8);
        assert!(item(&merged).is_nullable());
        assert_domain(item(&merged), Some(LogicalType::Json));
    }
}

#[test]
fn json_domain_survives_supported_string_storage_coercion() {
    let wide = list(logical(
        "item",
        DataType::LargeUtf8,
        false,
        LogicalType::Json,
    ));
    let narrow = list(logical("item", DataType::Utf8, true, LogicalType::Json));
    for merged in [wider_type(&wide, &narrow), wider_type(&narrow, &wide)] {
        assert_eq!(item(&merged).data_type(), &DataType::Utf8);
        assert!(item(&merged).is_nullable());
        assert_domain(item(&merged), Some(LogicalType::Json));
    }
}

#[test]
fn all_opaque_identities_survive_same_domain_rebuild() {
    for domain in [
        LogicalType::Hll,
        LogicalType::Bitmap,
        LogicalType::Object,
        LogicalType::Percentile,
    ] {
        for storage in [DataType::Binary, DataType::LargeBinary] {
            let left = list(logical("item", storage.clone(), false, domain));
            let right = list(logical("item", storage.clone(), true, domain));
            let merged = wider_type(&left, &right);
            assert_eq!(item(&merged).data_type(), &storage);
            assert!(item(&merged).is_nullable());
            assert_domain(item(&merged), Some(domain));
        }
    }
}

#[test]
fn physical_null_is_neutral_in_either_operand_order() {
    let null = list(Field::new("item", DataType::Null, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            NR_LOGICAL_TYPE_KEY.to_owned(),
            "unknown".to_owned(),
        )]),
    ));
    for (storage, domain) in [
        (DataType::Utf8, LogicalType::Json),
        (DataType::Binary, LogicalType::Bitmap),
    ] {
        let value = list(logical("item", storage.clone(), false, domain));
        for merged in [wider_type(&null, &value), wider_type(&value, &null)] {
            assert_eq!(item(&merged).data_type(), &storage);
            assert!(item(&merged).is_nullable());
            assert_domain(item(&merged), Some(domain));
        }
    }
}

#[test]
fn json_and_plain_string_do_not_manufacture_a_json_domain() {
    let json = list(logical("item", DataType::Utf8, false, LogicalType::Json));
    let plain = list(Field::new("item", DataType::Utf8, true));
    for merged in [wider_type(&json, &plain), wider_type(&plain, &json)] {
        assert_eq!(item(&merged).data_type(), &DataType::Utf8);
        assert!(item(&merged).is_nullable());
        assert_domain(item(&merged), None);
    }
    let hll = list(logical("item", DataType::Binary, false, LogicalType::Hll));
    let bitmap = list(logical("item", DataType::Binary, true, LogicalType::Bitmap));
    assert_domain(item(&wider_type(&hll, &bitmap)), None);
    assert_domain(item(&wider_type(&bitmap, &hll)), None);
}

#[test]
fn struct_name_alignment_preserves_reordered_domains_and_left_order() {
    let left = DataType::Struct(
        vec![
            Arc::new(logical(
                "document",
                DataType::Utf8,
                false,
                LogicalType::Json,
            )),
            Arc::new(logical("sketch", DataType::Binary, false, LogicalType::Hll)),
        ]
        .into(),
    );
    let right = DataType::Struct(
        vec![
            Arc::new(logical("sketch", DataType::Binary, true, LogicalType::Hll)),
            Arc::new(Field::new("document", DataType::Null, true)),
        ]
        .into(),
    );
    let merged = wider_type(&left, &right);
    let actual = fields(&merged);
    assert_eq!(actual[0].name(), "document");
    assert_eq!(actual[1].name(), "sketch");
    assert_domain(&actual[0], Some(LogicalType::Json));
    assert_domain(&actual[1], Some(LogicalType::Hll));
    assert!(actual.iter().all(|field| field.is_nullable()));
    let reversed = wider_type(&right, &left);
    assert_eq!(fields(&reversed)[0].name(), "sketch");
    assert_domain(&fields(&reversed)[0], Some(LogicalType::Hll));
    assert_domain(&fields(&reversed)[1], Some(LogicalType::Json));
}

#[test]
fn struct_positional_merge_preserves_left_names_and_exact_domains() {
    let left = DataType::Struct(
        vec![
            Arc::new(logical(
                "left_json",
                DataType::Utf8,
                false,
                LogicalType::Json,
            )),
            Arc::new(logical(
                "left_sketch",
                DataType::Binary,
                false,
                LogicalType::Percentile,
            )),
        ]
        .into(),
    );
    let right = DataType::Struct(
        vec![
            Arc::new(logical(
                "right_json",
                DataType::Utf8,
                true,
                LogicalType::Json,
            )),
            Arc::new(Field::new("right_sketch", DataType::Binary, true)),
        ]
        .into(),
    );
    let merged = wider_type(&left, &right);
    let actual = fields(&merged);
    assert_eq!(actual[0].name(), "left_json");
    assert_eq!(actual[1].name(), "left_sketch");
    assert_domain(&actual[0], Some(LogicalType::Json));
    assert_domain(&actual[1], None);
    assert!(actual.iter().all(|field| field.is_nullable()));
}

#[test]
fn map_key_and_value_merge_domains_independently_without_changing_flags() {
    let left = map(
        Field::new("old_key", DataType::Null, false),
        logical("old_value", DataType::Binary, false, LogicalType::Object),
        true,
        true,
    );
    let right = map(
        logical("new_key", DataType::Utf8, false, LogicalType::Json),
        logical("new_value", DataType::Binary, true, LogicalType::Object),
        false,
        false,
    );
    let merged = wider_type(&left, &right);
    let actual = map_fields(&merged);
    assert_eq!(actual[0].name(), "key");
    assert_eq!(actual[1].name(), "value");
    assert_eq!(actual[0].data_type(), &DataType::Utf8);
    assert!(!actual[0].is_nullable());
    assert!(actual[1].is_nullable());
    assert_domain(&actual[0], Some(LogicalType::Json));
    assert_domain(&actual[1], Some(LogicalType::Object));
}

#[test]
fn map_conflicting_value_does_not_clear_the_unrelated_key_domain() {
    let left = map(
        logical("key", DataType::Utf8, false, LogicalType::Json),
        logical("value", DataType::Binary, false, LogicalType::Hll),
        false,
        false,
    );
    let right = map(
        logical("key", DataType::Utf8, true, LogicalType::Json),
        logical("value", DataType::Binary, true, LogicalType::Bitmap),
        false,
        true,
    );
    let merged = wider_type(&left, &right);
    let actual = map_fields(&merged);
    assert_domain(&actual[0], Some(LogicalType::Json));
    assert_domain(&actual[1], None);
}

#[test]
fn recursive_list_struct_map_keeps_only_the_surviving_leaf_domain() {
    let make = |nullable| {
        list(Field::new(
            "item",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "nested",
                    map(
                        Field::new("key", DataType::Int32, false),
                        logical("value", DataType::Utf8, nullable, LogicalType::Json),
                        false,
                        false,
                    ),
                    nullable,
                ))]
                .into(),
            ),
            nullable,
        ))
    };
    let merged = wider_type(&make(false), &make(true));
    assert!(item(&merged).is_nullable());
    assert_domain(item(&merged), None);
    let nested = &fields(item(&merged).data_type())[0];
    assert!(nested.is_nullable());
    assert_domain(nested, None);
    let entries = map_fields(nested.data_type());
    assert_eq!(entries[0].data_type(), &DataType::Int32);
    assert_domain(&entries[0], None);
    assert_domain(&entries[1], Some(LogicalType::Json));
}

#[test]
fn rebuilt_fields_drop_provider_decoration_but_equal_type_fast_path_is_unchanged() {
    let decorated = logical("item", DataType::Utf8, false, LogicalType::Json).with_metadata(
        std::collections::HashMap::<String, String>::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("PARQUET:field_id".to_owned(), "71".to_owned()),
        ]),
    );
    let left = list(decorated);
    assert_eq!(wider_type(&left, &left), left);
    assert_eq!(
        item(&wider_type(&left, &left)).metadata()["PARQUET:field_id"],
        "71"
    );
    let right = list(logical("item", DataType::Utf8, true, LogicalType::Json));
    assert_domain(item(&wider_type(&left, &right)), Some(LogicalType::Json));
    assert_domain(
        item(&undecorated_nested_type(&left)),
        Some(LogicalType::Json),
    );
    let plain = list(Field::new("item", DataType::Int32, false).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "PARQUET:field_id".to_owned(),
            "72".to_owned(),
        )]),
    ));
    let widened = wider_type(&plain, &list(Field::new("item", DataType::Int64, true)));
    assert_eq!(item(&widened).data_type(), &DataType::Int64);
    assert_domain(item(&widened), None);
}

#[test]
fn recognized_markers_use_existing_normalization_but_invalid_storage_is_not_propagated() {
    let canonical = list(logical("item", DataType::Utf8, false, LogicalType::Json));
    let normalized = list(Field::new("item", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            NR_LOGICAL_TYPE_KEY.to_owned(),
            " JSON ".to_owned(),
        )]),
    ));
    assert_domain(
        item(&wider_type(&canonical, &normalized)),
        Some(LogicalType::Json),
    );
    for marker in ["unknown", "hll"] {
        let invalid = list(Field::new("item", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.to_owned(),
                marker.to_owned(),
            )]),
        ));
        let widened = wider_type(&canonical, &invalid);
        assert_eq!(item(&widened).metadata()[NR_LOGICAL_TYPE_KEY], "invalid");
    }
    let invalid_left = list(logical("item", DataType::Int32, false, LogicalType::Json));
    let invalid_right = list(logical("item", DataType::Int64, true, LogicalType::Json));
    // A compatible output alone cannot legitimize an incompatible input tag.
    assert_eq!(
        item(&wider_type(&canonical, &invalid_right)).metadata()[NR_LOGICAL_TYPE_KEY],
        "invalid"
    );
    assert_eq!(
        item(&wider_type(&invalid_right, &canonical)).metadata()[NR_LOGICAL_TYPE_KEY],
        "invalid"
    );
    let merged = wider_type(&invalid_left, &invalid_right);
    assert_eq!(item(&merged).data_type(), &DataType::Int64);
    assert_eq!(item(&merged).metadata()[NR_LOGICAL_TYPE_KEY], "invalid");
}
