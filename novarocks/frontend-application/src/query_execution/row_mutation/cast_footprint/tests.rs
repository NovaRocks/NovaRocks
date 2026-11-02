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
use arrow::{
    array::{DictionaryArray, Int32Array, Int64Array, StringArray},
    datatypes::Int32Type,
};
use std::sync::Arc;
fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap()
}
#[test]
fn layout_preflight_counts_independent_owner_copies_of_shared_backing() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7; 128]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let input = RecordBatch::try_new(schema.clone(), vec![array.clone(), array]).unwrap();
    let p = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
    assert_eq!(p.output_bytes, 0);
    assert!(p.copy_peak_bytes >= 2 * 128 * 8);
    assert!(p.peak_bytes >= p.input_bytes + p.copy_peak_bytes + p.temporary_bytes);
}
#[test]
fn layout_preflight_dictionary_matches_actual_expansion_and_copy() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 0, 1]),
            Arc::new(StringArray::from(vec!["abc", "longer"])),
        )
        .unwrap(),
    );
    let input = batch(dictionary);
    let schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
    let p = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
    let out = super::super::signed_layout::normalize(&Arc::new(schema), &input).unwrap();
    assert!(p.output_bytes >= out.column(0).get_buffer_memory_size());
    assert!(p.copy_peak_bytes >= 18 + 5 * 4);
    assert!(p.output_bytes > 0);
}
#[test]
fn layout_preflight_rejects_value_conversion_before_normalization() {
    let input = batch(Arc::new(Int64Array::from(vec![7])));
    let schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
    let e = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap_err();
    assert_eq!(e.kind(), ConnectorErrorKind::InvalidRequest);
}
#[test]
fn layout_preflight_fixed_peak_is_enforced_with_retained_and_other_owners() {
    let input = batch(Arc::new(Int64Array::from(vec![7])));
    let schema = input.schema();
    let p = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
    assert!(
        CowSignedLayoutFootprint::with_limit(&schema, &input, 0, 0, 0, p.peak_bytes - 1).is_err()
    );
    assert!(
        CowSignedLayoutFootprint::for_batch(&schema, &input, SELECTION_BYTES + 1, 0, 0).is_err()
    );
    assert!(CowSignedLayoutFootprint::for_batch(&schema, &input, 0, INPUT_BYTES, 0).is_err());
    assert!(CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, TRANSFORM_BYTES).is_err());
}

#[test]
fn layout_preflight_rejects_large_dictionary_expansion_without_allocating_it() {
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; MAX_ROWS]),
            Arc::new(StringArray::from(vec!["x".repeat(512)])),
        )
        .unwrap(),
    );
    let input = batch(array);
    let schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
    let error = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
}

#[test]
fn layout_preflight_preserves_utf8_extent_from_small_legal_dictionary() {
    use novarocks_failure_contract::{
        FailureClass, FailureProjection, RepresentationBoundary, SharedFailureCause,
    };
    use std::error::Error;

    const ROWS: usize = 524_288;
    const VALUE_BYTES: usize = 4096;
    const _: () = assert!(ROWS <= MAX_ROWS);
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; ROWS]),
            Arc::new(StringArray::from(vec!["x".repeat(VALUE_BYTES)])),
        )
        .unwrap(),
    );
    let input = batch(Arc::clone(&dictionary));
    let schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
    assert!(ConnectorRowConversionFootprint::retained_batch_bytes(&input).unwrap() < INPUT_BYTES);
    assert!(dictionary.get_buffer_memory_size() < 3 * 1024 * 1024);

    // Only the footprint runs. No expanded output array is materialized.
    let error = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap_err();
    let measured_error =
        super::super::signed_layout::normalize(&Arc::new(schema.clone()), &input).unwrap_err();
    let fact = error.failure_fact();
    assert_eq!(fact, measured_error.failure_fact());
    assert_eq!(fact.class(), FailureClass::Representation);
    assert!(fact.resource().is_none());
    let representation = fact.representation().unwrap();
    assert_eq!(
        representation.boundary,
        RepresentationBoundary::SingleOutput
    );
    assert_eq!(
        representation.required,
        Some(ROWS as u64 * VALUE_BYTES as u64)
    );
    assert_eq!(representation.extent_limit, Some(i32::MAX as u64));
    assert_eq!(representation.representable_prefix, Some((ROWS - 1) as u64));
    // A blanket InvalidRequest/Resource remap would discard this typed source.
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    assert_eq!(shared.failure_fact(), fact);
    assert!(shared.source().is_some());
    assert!(Arc::ptr_eq(input.column(0), &dictionary));
    assert_eq!(dictionary.len(), ROWS);
}

#[test]
fn metadata_resource_source_model_and_arithmetic_keep_their_original_facts_and_causes() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_type_contract::owned_resources::btree::BTreeResourceError;
    use std::error::Error;
    for (source, class, kind) in [
        (
            BTreeResourceError::Arithmetic("count overflow"),
            FailureClass::Resource,
            ConnectorErrorKind::ResourceExhausted,
        ),
        (
            BTreeResourceError::SourceModel("source mismatch"),
            FailureClass::Invariant,
            ConnectorErrorKind::InvalidRequest,
        ),
    ] {
        let fact = source.failure_fact();
        let error = metadata_resource_error(source);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.failure_fact(), fact);
        assert_eq!(error.failure_fact().class(), class);
        let mut cause = error.source();
        let mut actual = false;
        while let Some(source) = cause {
            actual |= source.is::<BTreeResourceError>();
            assert!(!source.is::<arrow::error::ArrowError>());
            cause = source.source();
        }
        assert!(actual);
    }
}
