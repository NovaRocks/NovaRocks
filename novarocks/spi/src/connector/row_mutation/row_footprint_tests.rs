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

use super::*;
use arrow::array::{
    ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int64Array, ListArray, ListViewArray,
    NullArray, StringArray, StructArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{Int8Type, Int64Type};
use std::sync::Arc;

fn old_digest(schema: &SchemaRef, batches: &[RecordBatch]) -> [u8; 32] {
    // The pre-change converter path is intentionally independent of footprint
    // calculation. This seals the v2 logical Arrow row bytes, not the estimate.
    let mut h = Sha256::new();
    h.update(b"novarocks.connector-row-mutation-selection.v2\0");
    h.update((batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64).to_be_bytes());
    h.update(canonical_schema_digest(schema).unwrap());
    let c = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect(),
    )
    .unwrap();
    for b in batches {
        let rows = c.convert_columns(b.columns()).unwrap();
        for row in rows.iter() {
            digest_bytes(&mut h, row.as_ref());
        }
    }
    h.finalize().into()
}
fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
fn reset_calls() {
    ROW_CONVERSION_CALLS.with(|n| n.set(0));
}
fn calls() -> usize {
    ROW_CONVERSION_CALLS.with(|n| n.get())
}
fn refused_before_arrow(schema: SchemaRef, batches: Vec<RecordBatch>) {
    reset_calls();
    let err = ConnectorRowMutationSelection::try_new(schema, batches, 1_048_576, 64 * 1024 * 1024)
        .unwrap_err();
    assert_eq!(err.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(
        calls(),
        0,
        "preflight must precede even Arrow's constructor"
    );
}

#[test]
fn row_footprint_preserves_legacy_digest_and_exact_encoded_bytes() {
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), None]),
        None,
        Some(vec![]),
    ]);
    let dict = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), Some(0), None]),
        Arc::new(StringArray::from(vec!["long value", "v", "unused"])),
    )
    .unwrap();
    let b = batch(vec![
        Arc::new(lists),
        Arc::new(dict),
        Arc::new(StringArray::from(vec![Some(""), Some("é"), None])),
    ]);
    let footprint = ConnectorRowConversionFootprint::for_schema(b.schema_ref()).unwrap();
    let proof = footprint.for_columns(b.columns(), 0).unwrap();
    let converter = RowConverter::new(
        b.schema()
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect(),
    )
    .unwrap();
    let encoded = converter.convert_columns(b.columns()).unwrap();
    assert_eq!(
        proof.encoded_rows_bytes,
        encoded.iter().map(|r| r.as_ref().len()).sum::<usize>()
    );
    assert!(proof.peak_bytes >= encoded.size() + b.get_array_memory_size());
    let expected = old_digest(b.schema_ref(), std::slice::from_ref(&b));
    let selection = ConnectorRowMutationSelection::try_new(b.schema(), vec![b], 3, 65536).unwrap();
    assert_eq!(selection.digest(), expected);
}

#[test]
fn row_footprint_wide_null_expansion_refused_before_converter() {
    let columns = (0..128)
        .map(|_| Arc::new(NullArray::new(1_048_576)) as ArrayRef)
        .collect();
    let b = batch(columns);
    assert!(b.get_array_memory_size() < 64 * 1024);
    refused_before_arrow(b.schema(), vec![b]);
}

#[test]
fn row_footprint_fixed_list_hidden_null_children_are_bounded() {
    let field = Arc::new(Field::new("item", DataType::Null, true));
    let list = FixedSizeListArray::new_null(field, 100_000_000, 1);
    let b = batch(vec![Arc::new(list)]);
    assert!(b.get_array_memory_size() < 4096);
    refused_before_arrow(b.schema(), vec![b]);
}

#[test]
fn row_footprint_constructor_null_expansion_refused_without_arrays() {
    let item = Arc::new(Field::new("item", DataType::Null, true));
    let child = Arc::new(Field::new(
        "hidden",
        DataType::FixedSizeList(item, 100_000_000),
        true,
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(vec![child].into()),
        true,
    )]));
    refused_before_arrow(schema, vec![]);
}

#[test]
fn row_footprint_repeated_list_view_references_expand_encoded_bytes() {
    let child = Arc::new(Field::new("item", DataType::Null, true));
    let list = ListViewArray::try_new(
        child,
        ScalarBuffer::from(vec![0; 64]),
        ScalarBuffer::from(vec![1000; 64]),
        Arc::new(NullArray::new(1000)),
        None,
    )
    .unwrap();
    let b = batch(vec![Arc::new(list)]);
    let f = ConnectorRowConversionFootprint::for_schema(b.schema_ref()).unwrap();
    let p = f.for_columns(b.columns(), 0).unwrap();
    assert_eq!(p.encoded_rows_bytes, 64 * (1000 * 10 + 1));
    assert!(p.encoded_rows_bytes > 100 * b.get_array_memory_size());
}

#[test]
fn row_footprint_slices_charge_backing_capacity_and_keep_digest_bytes() {
    let large = Arc::new(StringArray::from(vec![
        "x".repeat(32768),
        "small".to_owned(),
        "last".to_owned(),
    ])) as ArrayRef;
    let sliced = large.slice(1, 1);
    let b = batch(vec![sliced]);
    let footprint = ConnectorRowConversionFootprint::for_schema(b.schema_ref()).unwrap();
    let p = footprint.for_columns(b.columns(), 0).unwrap();
    assert!(p.input_bytes >= 32768);
    assert_eq!(p.encoded_rows_bytes, 10);
    let expected = old_digest(b.schema_ref(), std::slice::from_ref(&b));
    // Retained backing can exceed the nominal string length because Arrow's
    // builder owns capacity and the slice retains that whole allocation.
    let retained_budget =
        (ConnectorRowConversionFootprint::retained_schema_bytes(b.schema_ref()).unwrap()
            + ConnectorRowConversionFootprint::retained_batch_bytes(&b).unwrap()) as u64;
    let selection =
        ConnectorRowMutationSelection::try_new(b.schema(), vec![b], 1, retained_budget).unwrap();
    assert_eq!(selection.digest(), expected);
}

#[test]
fn row_footprint_metadata_schema_and_rows_refuse_before_converter() {
    // Repeated constructor subtrees amplify a small retained metadata value;
    // refusal does not require a large test allocation.
    let mut metadata = HashMap::new();
    metadata.insert("k".into(), "v".repeat(512 * 1024));
    let mut field = Arc::new(Field::new("v", DataType::Null, true).with_metadata(metadata));
    for _ in 0..40 {
        field = Arc::new(Field::new("nested", DataType::List(field), true));
    }
    let schema = Arc::new(Schema::new(vec![field]));
    refused_before_arrow(schema, vec![]);
    let schema = Arc::new(Schema::new(
        (0..4097)
            .map(|i| Field::new(format!("c{i}"), DataType::Null, true))
            .collect::<Vec<_>>(),
    ));
    refused_before_arrow(schema, vec![]);
    let b = batch(vec![Arc::new(NullArray::new(1_048_577))]);
    refused_before_arrow(b.schema(), vec![b]);
}

#[test]
fn row_footprint_nested_nulls_and_metadata_keep_legacy_hash() {
    let fields = vec![Arc::new(Field::new("v", DataType::Int64, true))].into();
    let values = StructArray::new(
        fields,
        vec![Arc::new(Int64Array::from(vec![Some(7), None]))],
        None,
    );
    let b = batch(vec![Arc::new(values)]);
    let mut metadata = HashMap::new();
    metadata.insert("z".into(), "last".into());
    metadata.insert("a".into(), "first".into());
    let schema = Arc::new(Schema::new_with_metadata(
        b.schema().fields().clone(),
        metadata,
    ));
    let b = RecordBatch::try_new(schema.clone(), b.columns().to_vec()).unwrap();
    let expected = old_digest(&schema, std::slice::from_ref(&b));
    let selection = ConnectorRowMutationSelection::try_new(schema, vec![b], 2, 65536).unwrap();
    assert_eq!(selection.digest(), expected);
}

#[test]
fn row_footprint_dictionary_duplicates_refuse_before_converter() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0; 20000]),
        Arc::new(StringArray::from(vec!["v".repeat(32768)])),
    )
    .unwrap();
    let b = batch(vec![Arc::new(dictionary)]);
    assert!(b.get_array_memory_size() < 65536);
    refused_before_arrow(b.schema(), vec![b]);
}

#[test]
fn row_footprint_checked_composition_cannot_increase_the_limit() {
    let b = batch(vec![Arc::new(NullArray::new(1))]);
    let f = ConnectorRowConversionFootprint::for_schema(b.schema_ref()).unwrap();
    let p = f.for_columns(b.columns(), 0).unwrap();
    assert!(p.checked_peak_with(10, 20).unwrap() == p.peak_bytes + 30);
    assert_eq!(
        p.checked_peak_with(MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES, 1)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(
        f.checked_constructor_peak_with(usize::MAX, 1)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

#[test]
fn row_footprint_oversized_valid_row_is_refused_without_large_buffers() {
    let field = Arc::new(Field::new("item", DataType::Null, true));
    let array = FixedSizeListArray::try_new(
        field,
        100_000_000,
        Arc::new(NullArray::new(100_000_000)),
        None,
    )
    .unwrap();
    let b = batch(vec![Arc::new(array)]);
    refused_before_arrow(b.schema(), vec![b]);
}

#[test]
fn row_footprint_schema_depth_is_refused_before_arrow_recursion() {
    let mut field = Arc::new(Field::new("leaf", DataType::Null, true));
    for _ in 0..64 {
        field = Arc::new(Field::new("nested", DataType::List(field), true));
    }
    refused_before_arrow(Arc::new(Schema::new(vec![field])), vec![]);
}

#[test]
fn row_footprint_independent_equal_batch_schemas_refuse_before_converter() {
    // Sixteen independent 64-KiB metadata values are enough to reproduce the
    // accounting gap using a small caller budget, without a 256-MiB fixture.
    let mut metadata = HashMap::new();
    metadata.insert("independent".to_owned(), "m".repeat(64 * 1024));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("v", DataType::Null, true)],
        metadata,
    ));
    let batches = (0..16)
        .map(|_| {
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    schema.fields().clone(),
                    schema
                        .metadata()
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<HashMap<_, _>>(),
                )),
                vec![Arc::new(NullArray::new(1))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let one = ConnectorRowConversionFootprint::retained_batch_bytes(&batches[0]).unwrap();
    let root = ConnectorRowConversionFootprint::retained_schema_bytes(&schema).unwrap();
    let budget = root + 4 * one;
    assert!(
        batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>()
            < budget
    );
    assert!(!Arc::ptr_eq(
        batches[0].schema_ref(),
        batches[1].schema_ref()
    ));
    assert_ne!(
        batches[0].schema_ref().metadata()["independent"].as_ptr(),
        batches[1].schema_ref().metadata()["independent"].as_ptr()
    );
    reset_calls();
    let error =
        ConnectorRowMutationSelection::try_new(schema, batches, 16, budget as u64).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(calls(), 0);
}

#[test]
fn row_footprint_equal_schema_instances_keep_digest_and_count_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("independent".to_owned(), "m".repeat(1024));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("v", DataType::Null, true)],
        metadata,
    ));
    let batches = (0..2)
        .map(|_| {
            RecordBatch::try_new(
                Arc::new(schema.as_ref().clone()),
                vec![Arc::new(NullArray::new(1))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let expected = old_digest(&schema, &batches);
    let selection = ConnectorRowMutationSelection::try_new(schema, batches, 2, 65536).unwrap();
    assert!(
        selection.byte_count() >= 3 * 1024,
        "explicit and per-batch schemas coexist"
    );
    assert_eq!(selection.digest(), expected);
    selection.validate().unwrap();
}

#[test]
fn row_footprint_actual_child_array_types_charge_independent_field_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("independent".to_owned(), "m".repeat(65536));
    let field = Field::new("leaf", DataType::Null, true).with_metadata(metadata);
    let logical_fields = vec![Arc::new(field.clone())].into();
    let child_fields = vec![Arc::new(field)].into();
    let child = StructArray::new(child_fields, vec![Arc::new(NullArray::new(1))], None);
    let outer = StructArray::new(
        vec![Arc::new(Field::new(
            "child",
            DataType::Struct(logical_fields),
            true,
        ))]
        .into(),
        vec![Arc::new(child)],
        None,
    );
    let columns = vec![Arc::new(outer) as ArrayRef];
    let source = ConnectorRowConversionFootprint::retained_columns_bytes(&columns).unwrap();
    assert!(
        source >= 2 * 65536,
        "parent and actual child types have independent equal metadata"
    );
    assert!(columns[0].get_array_memory_size() < 4096);
}

#[test]
fn row_footprint_outer_batch_spare_capacity_is_refused_before_converter() {
    let b = batch(vec![Arc::new(NullArray::new(1))]);
    let mut batches = Vec::with_capacity(65536);
    batches.push(b.clone());
    reset_calls();
    let error = ConnectorRowMutationSelection::try_new(b.schema(), batches, 1, 64 * 1024 * 1024)
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(calls(), 0);
}
