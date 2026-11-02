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

//! Current Arrow IPC dictionary behavior, independently of a new plan codec.
//! Explicit policies are test admission only; these tests make no host grant.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int16Array, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Int8Type, Int16Type, Schema};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;
use novarocks_physical_plan::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, arrow_fields_exact,
};

struct TestControl;
impl PureCompileControl for TestControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 64,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 64 * 1024,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 100_000,
        max_library_validation_bytes: 100_000,
    }
}
fn pool(field: Field, array: ArrayRef) -> ConstantPool {
    let value_type = FunctionValueType::new(field.data_type().clone(), field.is_nullable());
    ConstantPool::try_new(
        Arc::new(field),
        value_type,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &TestControl,
    )
    .unwrap()
}
#[allow(deprecated)]
fn dict_field(name: &str, data_type: DataType, id: i64) -> Field {
    Field::new_dict(name, data_type, false, id, true)
        .with_metadata(HashMap::from([("source".to_owned(), name.to_owned())]))
}
fn dict(keys: Vec<i8>, values: Vec<&str>) -> ArrayRef {
    Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(keys),
            Arc::new(StringArray::from(values)),
        )
        .unwrap(),
    )
}
fn batch(pool: &ConstantPool) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![pool.field().clone()])),
        vec![pool.array().clone()],
    )
    .unwrap()
}
fn stream(pool: &ConstantPool) -> Vec<u8> {
    let batch = batch(pool);
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    output
}
fn decode(raw: &[u8]) -> RecordBatch {
    let mut reader = StreamReader::try_new(raw, None).unwrap();
    let value = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    value
}
fn utf8_at(array: &ArrayRef, row: usize) -> &str {
    let dictionary = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    values.value(dictionary.keys().value(row) as usize)
}

#[test]
#[allow(deprecated)]
fn actual_arrow_ipc_dictionary_root_reassigns_id_but_keeps_order_metadata_and_values() {
    let source = dict(vec![1, 0], vec!["alpha", "beta"]);
    let source = pool(
        dict_field("root_dictionary", source.data_type().clone(), -99),
        source,
    );
    assert_eq!(source.field().dict_id(), Some(-99));
    assert_eq!(
        source
            .value(0)
            .unwrap()
            .utf8_observed(CompilePhase::Validate, &TestControl)
            .unwrap(),
        Some("beta")
    );
    let output = decode(&stream(&source));
    let field = output.schema().field(0).clone();
    assert_eq!(field.dict_id(), Some(0));
    assert_eq!(field.dict_is_ordered(), Some(true));
    assert_eq!(field.name(), source.field().name());
    assert_eq!(field.metadata(), source.field().metadata());
    assert_eq!(field.data_type(), source.field().data_type());
    assert!(!arrow_fields_exact(source.field(), &field));
    assert_eq!(utf8_at(output.column(0), 0), "beta");
    assert_eq!(utf8_at(output.column(0), 1), "alpha");
    let dictionary = output
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(dictionary.keys().values().as_ref(), &[1, 0]);
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), "alpha");
    assert_eq!(values.value(1), "beta");
    let decoded = pool(field, output.column(0).clone());
    assert_eq!(source.field().dict_id(), Some(-99));
    assert_eq!(source.field().dict_is_ordered(), Some(true));
    assert_eq!(decoded.field().dict_id(), Some(0));
    assert_eq!(source.value_type(), decoded.value_type());
    assert_eq!(source.value_type().data_type(), &DataType::Utf8);
    for row in 0..2 {
        assert!(
            source
                .value(row)
                .unwrap()
                .equals_observed(
                    &decoded.value(row).unwrap(),
                    CompilePhase::Validate,
                    &TestControl
                )
                .unwrap()
        );
    }
    assert!(
        !source
            .value(0)
            .unwrap()
            .equals_observed(
                &decoded.value(1).unwrap(),
                CompilePhase::Validate,
                &TestControl
            )
            .unwrap()
    );
}

#[test]
#[allow(deprecated)]
fn actual_arrow_ipc_dictionary_shared_authored_id_keeps_independent_struct_values() {
    let left = dict(vec![0, 1], vec!["left-zero", "left-one"]);
    let right = dict(vec![1, 0], vec!["right-zero", "right-one"]);
    let fields: Fields = vec![
        dict_field("left", left.data_type().clone(), 9),
        dict_field("right", right.data_type().clone(), 9),
    ]
    .into();
    let structure: ArrayRef = Arc::new(StructArray::new(fields.clone(), vec![left, right], None));
    let source = pool(
        Field::new("root", DataType::Struct(fields), false),
        structure,
    );
    let output = decode(&stream(&source));
    let structure = output
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let fields = structure.fields();
    assert_eq!(fields[0].dict_id(), Some(0));
    assert_eq!(fields[1].dict_id(), Some(1));
    assert_eq!(fields[0].dict_is_ordered(), Some(true));
    assert_eq!(fields[1].dict_is_ordered(), Some(true));
    assert_eq!(
        fields[0].metadata().get("source").map(String::as_str),
        Some("left")
    );
    assert_eq!(
        fields[1].metadata().get("source").map(String::as_str),
        Some("right")
    );
    assert_eq!(utf8_at(structure.column(0), 0), "left-zero");
    assert_eq!(utf8_at(structure.column(0), 1), "left-one");
    assert_eq!(utf8_at(structure.column(1), 0), "right-one");
    assert_eq!(utf8_at(structure.column(1), 1), "right-zero");
    let DataType::Struct(authored) = source.field().data_type() else {
        panic!("actual source is Struct")
    };
    assert_eq!(authored[0].dict_id(), Some(9));
    assert_eq!(authored[1].dict_id(), Some(9));
    assert_eq!(authored[0].dict_is_ordered(), Some(true));
    assert_eq!(authored[1].dict_is_ordered(), Some(true));
    assert_eq!(fields[0].metadata(), authored[0].metadata());
    assert_eq!(fields[1].metadata(), authored[1].metadata());
    for (column, keys, expected_values) in [
        (0, [0, 1], ["left-zero", "left-one"]),
        (1, [1, 0], ["right-zero", "right-one"]),
    ] {
        let dictionary = structure
            .column(column)
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(dictionary.keys().values().as_ref(), &keys);
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.len(), expected_values.len());
        for (row, expected) in expected_values.into_iter().enumerate() {
            assert_eq!(values.value(row), expected);
        }
    }
    let DataType::Struct(normalized) = source.value_type().data_type() else {
        panic!("complete value layout is Struct")
    };
    for field in normalized {
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.dict_id(), None);
        assert_eq!(field.dict_is_ordered(), None);
        assert!(field.metadata().as_arc().is_none());
    }
    assert!(!arrow_fields_exact(
        source.field(),
        output.schema().field(0)
    ));
    let decoded = pool(output.schema().field(0).clone(), output.column(0).clone());
    assert_eq!(source.value_type(), decoded.value_type());
    for row in 0..2 {
        assert!(
            source
                .value(row)
                .unwrap()
                .equals_observed(
                    &decoded.value(row).unwrap(),
                    CompilePhase::Validate,
                    &TestControl
                )
                .unwrap()
        );
    }
}

#[test]
fn actual_arrow_ipc_bare_nested_dictionary_is_a_legal_constant_but_both_writers_refuse() {
    let inner: ArrayRef = Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![1, 0]),
            Arc::new(StringArray::from(vec!["inner-zero", "inner-one"])),
        )
        .unwrap(),
    );
    let outer: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), inner).unwrap());
    let original_type = DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::Utf8),
        )),
    );
    assert_eq!(outer.data_type(), &original_type);
    let source = pool(dict_field("nested_bare", original_type.clone(), -99), outer);
    assert_eq!(
        source
            .value(0)
            .unwrap()
            .utf8_observed(CompilePhase::Validate, &TestControl)
            .unwrap(),
        Some("inner-one")
    );
    assert_eq!(source.field().data_type(), &original_type);
    assert_eq!(
        source
            .value(1)
            .unwrap()
            .utf8_observed(CompilePhase::Validate, &TestControl)
            .unwrap(),
        Some("inner-zero")
    );
    // Arrow 60.1 rejects the schema before writing, rather than producing
    // the older flattened schema whose record batch could not be decoded.
    let batch = batch(&source);
    let mut output = Vec::new();
    let error = match StreamWriter::try_new(&mut output, batch.schema().as_ref()) {
        Err(source) => source,
        Ok(_) => panic!("direct dictionary-of-dictionary must be refused before writing"),
    };
    let arrow::error::ArrowError::InvalidArgumentError(message) = error else {
        panic!("actual SDK schema rejection must remain InvalidArgumentError: {error}")
    };
    assert_eq!(
        message,
        "Arrow IPC field metadata cannot encode direct dictionary-of-dictionary values for field \"nested_bare\""
    );
    assert!(output.is_empty());

    // This was already a first-party IPC profile refusal. ConstantPool
    // admission above grants neither schema flattening nor a codec capability.
    let error = novarocks_plan_codec::ipc_schema_v2::encode_single_field_schema(
        source.field(),
        novarocks_plan_codec::ipc_schema_v2::IpcSchemaProjectionLimits {
            max_field_occurrences: 64,
            max_type_occurrences: 128,
            max_string_bytes: 4096,
            max_flatbuffer_bytes: 64 * 1024,
        },
        &TestControl,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        novarocks_plan_codec::physical_type_v2::TypeCodecError::InvalidShape(
            "direct nested dictionary has no inner IPC Field identity"
        )
    ));
}

#[test]
#[allow(deprecated)]
fn actual_arrow_ipc_dictionary_field_nested_dictionary_preserves_both_encoding_layers() {
    let inner: ArrayRef = Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![1, 0]),
            Arc::new(StringArray::from(vec!["inner-zero", "inner-one"])),
        )
        .unwrap(),
    );
    let fields: Fields = vec![dict_field("inner", inner.data_type().clone(), 9)].into();
    let values: ArrayRef = Arc::new(StructArray::new(fields, vec![inner], None));
    let outer: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), values).unwrap(),
    );
    let source = pool(
        dict_field("nested_with_field", outer.data_type().clone(), -99),
        outer,
    );
    let output = decode(&stream(&source));
    assert_eq!(output.schema().field(0).dict_id(), Some(1));
    let outer = output
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(outer.keys().values().as_ref(), &[0, 1]);
    let values = outer
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(values.fields()[0].dict_id(), Some(0));
    assert_eq!(values.fields()[0].dict_is_ordered(), Some(true));
    assert_eq!(
        values.fields()[0]
            .metadata()
            .get("source")
            .map(String::as_str),
        Some("inner")
    );
    let inner = values
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int16Type>>()
        .unwrap();
    assert_eq!(inner.keys().values().as_ref(), &[1, 0]);
    let leaf = inner
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(leaf.value(0), "inner-zero");
    assert_eq!(leaf.value(1), "inner-one");
    assert!(!arrow_fields_exact(
        source.field(),
        output.schema().field(0)
    ));
}
