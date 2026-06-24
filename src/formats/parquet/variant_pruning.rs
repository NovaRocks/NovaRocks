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

use arrow::datatypes::DataType;
use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::schema::types::{ColumnDescriptor, Type};

use crate::common::ids::SlotId;

use super::variant_read::object_key_path;
use super::{MinMaxPredicate, VariantPathSpec};

#[derive(Clone, Debug)]
pub(crate) struct BoundVariantPathPruningPredicate {
    pub(crate) leaf_column_index: usize,
    pub(crate) leaf_column_path: String,
    pub(crate) residual_value_column_index: Option<usize>,
    pub(crate) residual_value_column_path: Option<String>,
    pub(crate) requested_type: DataType,
    pub(crate) predicate: MinMaxPredicate,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariantPathPruningPredicate {
    pub output_slot_id: SlotId,
    pub source_slot_id: SlotId,
    pub source_field_id: Option<i32>,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub predicate: MinMaxPredicate,
}

pub(crate) fn bind_variant_path_pruning_predicates(
    metadata: &ParquetMetaData,
    specs: &[VariantPathSpec],
    predicates: &[VariantPathPruningPredicate],
) -> Vec<BoundVariantPathPruningPredicate> {
    let schema_descr = metadata.file_metadata().schema_descr();
    let mut bound = Vec::new();

    for predicate in predicates {
        let Some(spec) = specs
            .iter()
            .find(|spec| spec.output_slot_id == predicate.output_slot_id)
        else {
            continue;
        };
        if predicate.source_slot_id != spec.source_slot_id
            || predicate.canonical_path != spec.canonical_path
            || predicate.requested_type != spec.requested_type
        {
            continue;
        }
        let Some(source_field_id) = predicate.source_field_id else {
            continue;
        };
        if spec.source_field_id != Some(source_field_id) {
            continue;
        }
        let Some(path) = object_key_path(&predicate.canonical_path) else {
            continue;
        };

        for (leaf_column_index, column) in schema_descr.columns().iter().enumerate() {
            let root = schema_descr.get_column_root(leaf_column_index);
            if !root_matches_field_id(root, source_field_id) {
                continue;
            }
            let expected_path = expected_typed_leaf_path(root.name(), &path);
            if column.path().parts() != expected_path.as_slice() {
                continue;
            }
            if !parquet_leaf_matches_requested_type(column, &predicate.requested_type) {
                continue;
            }
            let expected_residual_path = expected_residual_value_leaf_path(root.name(), &path);
            let residual_value_column = schema_descr.columns().iter().enumerate().find(
                |(residual_column_index, residual_column)| {
                    let residual_root = schema_descr.get_column_root(*residual_column_index);
                    root_matches_field_id(residual_root, source_field_id)
                        && residual_column.path().parts() == expected_residual_path.as_slice()
                },
            );
            bound.push(BoundVariantPathPruningPredicate {
                leaf_column_index,
                leaf_column_path: column.path().string(),
                residual_value_column_index: residual_value_column
                    .map(|(residual_column_index, _)| residual_column_index),
                residual_value_column_path: residual_value_column
                    .map(|(_, residual_column)| residual_column.path().string()),
                requested_type: predicate.requested_type.clone(),
                predicate: predicate.predicate.clone(),
            });
            break;
        }
    }

    bound
}

pub(crate) fn variant_residual_value_all_null_for_row_group(
    row_group: &RowGroupMetaData,
    pred: &BoundVariantPathPruningPredicate,
) -> bool {
    let Some(residual_column_index) = pred.residual_value_column_index else {
        return true;
    };
    let Some(column) = row_group.columns().get(residual_column_index) else {
        return false;
    };
    let Some(stats) = column.statistics() else {
        return false;
    };
    let row_count = row_group.num_rows();
    if row_count < 0 {
        return false;
    }
    stats.null_count_opt() == Some(row_count as u64)
}

fn root_matches_field_id(root: &Type, source_field_id: i32) -> bool {
    let info = root.get_basic_info();
    info.has_id() && info.id() == source_field_id
}

fn expected_typed_leaf_path(root_name: &str, path: &[String]) -> Vec<String> {
    let mut parts = Vec::with_capacity(1 + path.len() * 2);
    parts.push(root_name.to_string());
    for key in path {
        parts.push("typed_value".to_string());
        parts.push(key.clone());
    }
    parts.push("typed_value".to_string());
    parts
}

fn expected_residual_value_leaf_path(root_name: &str, path: &[String]) -> Vec<String> {
    let mut parts = Vec::with_capacity(1 + path.len() * 2);
    parts.push(root_name.to_string());
    for key in path {
        parts.push("typed_value".to_string());
        parts.push(key.clone());
    }
    parts.push("value".to_string());
    parts
}

fn parquet_leaf_matches_requested_type(column: &ColumnDescriptor, requested: &DataType) -> bool {
    let leaf_type = column.self_type();
    let physical_type = leaf_type.get_physical_type();
    let info = leaf_type.get_basic_info();
    match requested {
        DataType::Boolean => physical_type == PhysicalType::BOOLEAN,
        DataType::Int64 => {
            physical_type == PhysicalType::INT64
                && info.logical_type_ref().is_none()
                && info.converted_type() == ConvertedType::NONE
        }
        DataType::Float64 => physical_type == PhysicalType::DOUBLE,
        DataType::Utf8 => {
            physical_type == PhysicalType::BYTE_ARRAY
                && (matches!(info.logical_type_ref(), Some(LogicalType::String))
                    || info.converted_type() == ConvertedType::UTF8)
        }
        DataType::Date32 => {
            physical_type == PhysicalType::INT32
                && (matches!(info.logical_type_ref(), Some(LogicalType::Date))
                    || info.converted_type() == ConvertedType::DATE)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
        StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::common::ids::SlotId;
    use crate::formats::parquet::{MinMaxPredicate, MinMaxPredicateValue};

    use super::*;

    fn field_id_meta(field_id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())])
    }

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(field_id_meta(field_id))
    }

    fn variant_path_spec_for(
        source_field_id: Option<i32>,
        canonical_path: &str,
        requested_type: DataType,
    ) -> VariantPathSpec {
        let output_field_name = format!(
            "__nr_var_payload_{}",
            canonical_path.replace(['$', '.'], "")
        );
        VariantPathSpec {
            source_slot_id: SlotId::new(1),
            source_read_slot_id: SlotId::new(1),
            output_slot_id: SlotId::new(2),
            source_field_id,
            source_name: "payload_logical".to_string(),
            output_name: output_field_name.clone(),
            source_field: Field::new("payload_logical", DataType::LargeBinary, true),
            output_field: Field::new(output_field_name, requested_type.clone(), true),
            canonical_path: canonical_path.to_string(),
            requested_type,
            strict: true,
        }
    }

    fn variant_path_spec(source_field_id: Option<i32>) -> VariantPathSpec {
        variant_path_spec_for(source_field_id, "$.a", DataType::Int64)
    }

    fn variant_path_predicate(
        source_field_id: Option<i32>,
        canonical_path: &str,
        requested_type: DataType,
    ) -> VariantPathPruningPredicate {
        VariantPathPruningPredicate {
            output_slot_id: SlotId::new(2),
            source_slot_id: SlotId::new(1),
            source_field_id,
            canonical_path: canonical_path.to_string(),
            requested_type,
            predicate: MinMaxPredicate::Ge {
                column: "__nr_var_payload_a".to_string(),
                value: MinMaxPredicateValue::Int64(7),
            },
        }
    }

    fn struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> StructArray {
        StructArray::try_new(Fields::from(fields), columns, None).expect("struct array")
    }

    fn variant_metadata_with_leaf_path(
        path: &[&str],
        leaf_field: Field,
        leaf_array: ArrayRef,
        root_field_id: i32,
    ) -> ParquetMetaData {
        assert!(!path.is_empty());

        let mut typed_value_array = leaf_array;
        let mut typed_value_field = leaf_field;
        for (index, key) in path.iter().rev().enumerate() {
            let key_node = Arc::new(struct_array(
                vec![typed_value_field.clone()],
                vec![typed_value_array],
            )) as ArrayRef;
            let key_field = Field::new(*key, key_node.data_type().clone(), true);
            typed_value_array = Arc::new(struct_array(vec![key_field], vec![key_node])) as ArrayRef;
            if index + 1 < path.len() {
                typed_value_field =
                    Field::new("typed_value", typed_value_array.data_type().clone(), true);
            }
        }

        let root_typed_field =
            Field::new("typed_value", typed_value_array.data_type().clone(), true);
        let payload = Arc::new(struct_array(
            vec![root_typed_field],
            vec![typed_value_array],
        )) as ArrayRef;
        let payload_field = field_with_id(
            "payload_physical",
            payload.data_type().clone(),
            true,
            root_field_id,
        );
        let schema = Arc::new(Schema::new(vec![payload_field]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![payload]).expect("batch");

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).expect("writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        let reader =
            SerializedFileReader::new(bytes::Bytes::from(buffer)).expect("metadata reader");
        reader.metadata().clone()
    }

    fn variant_metadata_with_residual_value_leaf_path(
        path: &[&str],
        leaf_field: Field,
        leaf_array: ArrayRef,
        residual_value_array: ArrayRef,
        root_field_id: i32,
    ) -> ParquetMetaData {
        assert!(!path.is_empty());

        let final_key_node = Arc::new(struct_array(
            vec![
                leaf_field,
                Field::new("value", residual_value_array.data_type().clone(), true),
            ],
            vec![leaf_array, residual_value_array],
        )) as ArrayRef;
        let final_key_field = Field::new(
            *path.last().expect("final key"),
            final_key_node.data_type().clone(),
            true,
        );
        let mut typed_value_array =
            Arc::new(struct_array(vec![final_key_field], vec![final_key_node])) as ArrayRef;

        for key in path.iter().rev().skip(1) {
            let typed_value_field =
                Field::new("typed_value", typed_value_array.data_type().clone(), true);
            let key_node = Arc::new(struct_array(
                vec![typed_value_field],
                vec![typed_value_array],
            )) as ArrayRef;
            let key_field = Field::new(*key, key_node.data_type().clone(), true);
            typed_value_array = Arc::new(struct_array(vec![key_field], vec![key_node])) as ArrayRef;
        }

        let root_typed_field =
            Field::new("typed_value", typed_value_array.data_type().clone(), true);
        let payload = Arc::new(struct_array(
            vec![root_typed_field],
            vec![typed_value_array],
        )) as ArrayRef;
        let payload_field = field_with_id(
            "payload_physical",
            payload.data_type().clone(),
            true,
            root_field_id,
        );
        let schema = Arc::new(Schema::new(vec![payload_field]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![payload]).expect("batch");

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).expect("writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        let reader =
            SerializedFileReader::new(bytes::Bytes::from(buffer)).expect("metadata reader");
        reader.metadata().clone()
    }

    fn variant_metadata_with_leaf(leaf_field: Field, leaf_array: ArrayRef) -> ParquetMetaData {
        variant_metadata_with_leaf_path(&["a"], leaf_field, leaf_array, 10)
    }

    fn variant_metadata() -> ParquetMetaData {
        variant_metadata_with_leaf(
            Field::new("typed_value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(7)])),
        )
    }

    #[test]
    fn variant_pruning_binds_typed_leaf_by_source_field_id_and_path() {
        let metadata = variant_metadata();
        let specs = vec![variant_path_spec(Some(10))];
        let predicate = variant_path_predicate(Some(10), "$.a", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate.clone()]);

        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].leaf_column_index, 0);
        assert_eq!(
            bound[0].leaf_column_path,
            "payload_physical.typed_value.a.typed_value"
        );
        assert_eq!(bound[0].requested_type, DataType::Int64);
        assert_eq!(bound[0].predicate, predicate.predicate);
    }

    #[test]
    fn variant_pruning_binds_all_pr5_whitelisted_leaf_types() {
        let cases: Vec<(&str, DataType, Field, ArrayRef)> = vec![
            (
                "boolean",
                DataType::Boolean,
                Field::new("typed_value", DataType::Boolean, true),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ),
            (
                "int64",
                DataType::Int64,
                Field::new("typed_value", DataType::Int64, true),
                Arc::new(Int64Array::from(vec![Some(7)])),
            ),
            (
                "float64",
                DataType::Float64,
                Field::new("typed_value", DataType::Float64, true),
                Arc::new(Float64Array::from(vec![Some(7.5)])),
            ),
            (
                "utf8",
                DataType::Utf8,
                Field::new("typed_value", DataType::Utf8, true),
                Arc::new(StringArray::from(vec![Some("value")])),
            ),
            (
                "date32",
                DataType::Date32,
                Field::new("typed_value", DataType::Date32, true),
                Arc::new(Date32Array::from(vec![Some(7)])),
            ),
        ];

        for (case_name, requested_type, leaf_field, leaf_array) in cases {
            let metadata = variant_metadata_with_leaf(leaf_field, leaf_array);
            let specs = vec![variant_path_spec_for(
                Some(10),
                "$.a",
                requested_type.clone(),
            )];
            let predicate = variant_path_predicate(Some(10), "$.a", requested_type.clone());
            let bound =
                bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate.clone()]);

            assert_eq!(bound.len(), 1, "{case_name}");
            assert_eq!(
                bound[0].leaf_column_path, "payload_physical.typed_value.a.typed_value",
                "{case_name}"
            );
            assert_eq!(bound[0].requested_type, requested_type, "{case_name}");
            assert_eq!(bound[0].predicate, predicate.predicate, "{case_name}");
        }
    }

    #[test]
    fn variant_pruning_binds_multi_key_typed_leaf_by_exact_path() {
        let metadata = variant_metadata_with_leaf_path(
            &["a", "b"],
            Field::new("typed_value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(7)])),
            10,
        );
        let specs = vec![variant_path_spec_for(Some(10), "$.a.b", DataType::Int64)];
        let predicate = variant_path_predicate(Some(10), "$.a.b", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate.clone()]);

        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].leaf_column_index, 0);
        assert_eq!(
            bound[0].leaf_column_path,
            "payload_physical.typed_value.a.typed_value.b.typed_value"
        );
        assert_eq!(bound[0].requested_type, DataType::Int64);
        assert_eq!(bound[0].predicate, predicate.predicate);
    }

    #[test]
    fn variant_pruning_binds_final_path_residual_value_sibling() {
        let metadata = variant_metadata_with_residual_value_leaf_path(
            &["a", "b"],
            Field::new("typed_value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(7)])),
            Arc::new(BinaryArray::from_opt_vec(vec![Some(&[1u8, 2u8][..])])),
            10,
        );
        let specs = vec![variant_path_spec_for(Some(10), "$.a.b", DataType::Int64)];
        let predicate = variant_path_predicate(Some(10), "$.a.b", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate]);

        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].leaf_column_index, 0);
        assert_eq!(
            bound[0].leaf_column_path,
            "payload_physical.typed_value.a.typed_value.b.typed_value"
        );
        assert_eq!(bound[0].residual_value_column_index, Some(1));
        assert_eq!(
            bound[0].residual_value_column_path.as_deref(),
            Some("payload_physical.typed_value.a.typed_value.b.value")
        );
    }

    #[test]
    fn variant_pruning_does_not_bind_without_source_field_id_match() {
        let metadata = variant_metadata();
        let specs = vec![variant_path_spec(Some(10))];
        let predicate = variant_path_predicate(Some(11), "$.a", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate]);

        assert!(bound.is_empty());
    }

    #[test]
    fn variant_pruning_does_not_bind_unsupported_path_or_type() {
        let metadata = variant_metadata_with_leaf(
            Field::new(
                "typed_value",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1234)])),
        );
        let specs = vec![variant_path_spec(Some(10))];
        let array_path = variant_path_predicate(Some(10), "$[0]", DataType::Int64);
        let timestamp_type = variant_path_predicate(
            Some(10),
            "$.a",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let bound =
            bind_variant_path_pruning_predicates(&metadata, &specs, &[array_path, timestamp_type]);

        assert!(bound.is_empty());
    }

    #[test]
    fn variant_pruning_does_not_bind_inconsistent_predicate_metadata() {
        let metadata = variant_metadata();

        let mut source_slot_mismatch = variant_path_predicate(Some(10), "$.a", DataType::Int64);
        source_slot_mismatch.source_slot_id = SlotId::new(99);
        let bound = bind_variant_path_pruning_predicates(
            &metadata,
            &[variant_path_spec(Some(10))],
            &[source_slot_mismatch],
        );
        assert!(bound.is_empty(), "source slot mismatch");

        let mut path_mismatch_spec = variant_path_spec(Some(10));
        path_mismatch_spec.canonical_path = "$.b".to_string();
        let path_mismatch = variant_path_predicate(Some(10), "$.a", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(
            &metadata,
            &[path_mismatch_spec],
            &[path_mismatch],
        );
        assert!(bound.is_empty(), "canonical path mismatch");

        let float_metadata = variant_metadata_with_leaf(
            Field::new("typed_value", DataType::Float64, true),
            Arc::new(Float64Array::from(vec![Some(7.5)])),
        );
        let mut type_mismatch_spec = variant_path_spec(Some(10));
        type_mismatch_spec.output_field = Field::new("__nr_var_payload_a", DataType::Int64, true);
        type_mismatch_spec.requested_type = DataType::Int64;
        let type_mismatch = variant_path_predicate(Some(10), "$.a", DataType::Float64);
        let bound = bind_variant_path_pruning_predicates(
            &float_metadata,
            &[type_mismatch_spec],
            &[type_mismatch],
        );
        assert!(bound.is_empty(), "requested type mismatch");
    }

    #[test]
    fn variant_pruning_does_not_bind_when_root_field_id_differs_even_if_name_and_path_match() {
        let metadata = variant_metadata_with_leaf_path(
            &["a"],
            Field::new("typed_value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(7)])),
            11,
        );
        let specs = vec![variant_path_spec(Some(10))];
        let predicate = variant_path_predicate(Some(10), "$.a", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate]);

        assert!(bound.is_empty());
    }

    #[test]
    fn variant_pruning_does_not_bind_annotated_int64_as_plain_int64() {
        let metadata = variant_metadata_with_leaf(
            Field::new("typed_value", DataType::Time64(TimeUnit::Microsecond), true),
            Arc::new(Time64MicrosecondArray::from(vec![Some(1234)])),
        );
        let specs = vec![variant_path_spec(Some(10))];
        let predicate = variant_path_predicate(Some(10), "$.a", DataType::Int64);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &[predicate]);

        assert!(bound.is_empty());
    }
}
