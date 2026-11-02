// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::exec::operators::hashjoin::{
    build_capture::InputRunWriter,
    build_pages::{
        CapturesForGraphs,
        tests::{collect, work},
    },
    build_state::ProjectionDomains,
    join_completion::tests::{Control, fixture},
};
use arrow::{
    array::{
        Array, Decimal128Array, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray,
        StringArray, StringViewArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Int32Type, Schema, TimeUnit},
};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::SlotId;

fn schema(
    types: &[FunctionValueType],
    ids: &[u32],
    owner: &dyn InputConstructionOwner,
) -> ChunkSchemaRef {
    let fields = types
        .iter()
        .zip(ids)
        .map(|(ty, id)| Field::new(format!("c{id}"), ty.data_type().clone(), ty.nullable()))
        .collect::<Vec<_>>();
    let ids = ids.iter().map(|&id| SlotId::new(id)).collect::<Vec<_>>();
    let layout =
        crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::new(fields)), &ids, types);
    ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
        &layout,
        owner.storage_lifetime(),
        |layout| owner.request(layout),
        |units| {
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
            )
        },
    )
    .unwrap()
}
fn chunk(schema: ChunkSchemaRef, array: ArrayRef, owner: &dyn InputConstructionOwner) -> Chunk {
    let rows = array.len();
    // Only fixture payload is plain Arrow; headers use the actual owner.
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        vec![array],
        rows,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}
fn pages(
    source: &Chunk,
    work: &crate::exec::hash_table::key_index::BulkWork<'_>,
) -> Arc<BuildRows> {
    let domains = ProjectionDomains::PayloadOnly;
    let mut receipts = Vec::new();
    for slot in 0..2 {
        let mut writer = InputRunWriter::new(
            &domains,
            B3RunId::from_input_slot(slot, 2).unwrap(),
            true,
            work,
        )
        .unwrap();
        if slot == 0 {
            for (start, len) in [(0, 2), (2, 0), (2, 1)] {
                let page =
                    Chunk::try_new_like(source.record_batch().slice(start, len), source).unwrap();
                writer.push_payload(&page, work).unwrap();
            }
        }
        receipts.push(writer.seal(work).unwrap());
    }
    let (inputs, _lease) = collect(receipts, work);
    let (rows, captures) = BuildRows::from_inputs(inputs, &domains, work).unwrap();
    assert!(matches!(captures, CapturesForGraphs::PayloadOnly));
    rows
}
fn pairs(rows: &BuildRows) -> Vec<JoinPairRow> {
    vec![
        JoinPairRow {
            probe: Some(0),
            build: Some(rows.row(2).unwrap()),
        },
        JoinPairRow {
            probe: Some(1),
            build: Some(rows.row(0).unwrap()),
        },
        JoinPairRow {
            probe: Some(2),
            build: None,
        },
        JoinPairRow {
            probe: None,
            build: Some(rows.row(1).unwrap()),
        },
        JoinPairRow {
            probe: Some(0),
            build: Some(rows.row(0).unwrap()),
        },
    ]
}

#[test]
fn paged_gather_preserves_selected_values_types_null_extensions_and_duplicate_rows() {
    let cases: Vec<(FunctionValueType, ArrayRef, ArrayRef)> = vec![
        (
            FunctionValueType::new(DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(5), None, Some(9)])),
            Arc::new(Int64Array::from(vec![
                Some(9),
                Some(5),
                None,
                None,
                Some(5),
            ])),
        ),
        (
            FunctionValueType::new(DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("five"), None, Some("nine")])),
            Arc::new(StringArray::from(vec![
                Some("nine"),
                Some("five"),
                None,
                None,
                Some("five"),
            ])),
        ),
        (
            FunctionValueType::new(DataType::Utf8View, true),
            Arc::new(StringViewArray::from(vec![
                Some("a string longer than inline"),
                None,
                Some("short"),
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("short"),
                Some("a string longer than inline"),
                None,
                None,
                Some("a string longer than inline"),
            ])),
        ),
        (
            FunctionValueType::new(DataType::Decimal128(12, 2), true),
            Arc::new(
                Decimal128Array::from(vec![Some(501), None, Some(-902)])
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(-902), Some(501), None, None, Some(501)])
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ),
        ),
        (
            FunctionValueType::new(DataType::FixedSizeBinary(3), true),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some([0, 1, 2]), None, Some([253, 254, 255])].into_iter(),
                    3,
                )
                .unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [
                        Some([253, 254, 255]),
                        Some([0, 1, 2]),
                        None,
                        None,
                        Some([0, 1, 2]),
                    ]
                    .into_iter(),
                    3,
                )
                .unwrap(),
            ),
        ),
        (
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(5), None, Some(-9)]).with_timezone("UTC"),
            ),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(-9), Some(5), None, None, Some(5)])
                    .with_timezone("UTC"),
            ),
        ),
        (
            FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(5), None]),
                None,
                Some(vec![Some(9)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(9)]),
                Some(vec![Some(5), None]),
                None,
                None,
                Some(vec![Some(5), None]),
            ])),
        ),
    ];
    for (ty, values, expected) in cases {
        let (owner, tracker) = fixture();
        let work = work(owner.as_ref(), &tracker, &Control);
        let probe_ty = FunctionValueType::new(DataType::Int32, true);
        let probe_schema = schema(std::slice::from_ref(&probe_ty), &[1], owner.as_ref());
        let build_schema = schema(std::slice::from_ref(&ty), &[2], owner.as_ref());
        let output_schema = schema(&[probe_ty, ty], &[1, 2], owner.as_ref());
        let probe = chunk(
            probe_schema.clone(),
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
            owner.as_ref(),
        );
        let source = chunk(build_schema.clone(), values, owner.as_ref());
        let rows = pages(&source, &work);
        let gather =
            PagedJoinGather::new(output_schema, &probe_schema, &build_schema, owner.as_ref())
                .unwrap();
        let output = gather
            .gather(
                Some(&probe),
                &rows,
                &pairs(&rows),
                owner.as_ref(),
                &work.host,
                &Control,
            )
            .unwrap();
        assert_eq!(
            output.record_batch().column(0).to_data(),
            Int32Array::from(vec![Some(10), None, Some(30), None, Some(10)]).to_data()
        );
        assert_eq!(
            output.record_batch().column(1).to_data(),
            expected.to_data()
        );
        let output = output.into_chunk(owner.as_ref()).unwrap();
        assert_eq!(output.columns()[1].to_data(), expected.to_data());
        let empty = gather
            .gather(
                Some(&probe),
                &rows,
                &[],
                owner.as_ref(),
                &work.host,
                &Control,
            )
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.record_batch().schema(), output.schema());
    }
}

#[test]
fn paged_gather_last_raw_buffer_alias_keeps_only_its_actual_creator() {
    let (owner, tracker) = fixture();
    let array = {
        let work = work(owner.as_ref(), &tracker, &Control);
        let ty = FunctionValueType::new(DataType::Int64, true);
        let probe_schema = schema(std::slice::from_ref(&ty), &[1], owner.as_ref());
        let build_schema = schema(std::slice::from_ref(&ty), &[2], owner.as_ref());
        let output_schema = schema(std::slice::from_ref(&ty), &[2], owner.as_ref());
        let source = chunk(
            build_schema.clone(),
            Arc::new(Int64Array::from(vec![Some(5), None, Some(9)])),
            owner.as_ref(),
        );
        let rows = pages(&source, &work);
        let gather =
            PagedJoinGather::new(output_schema, &probe_schema, &build_schema, owner.as_ref())
                .unwrap();
        let output = gather
            .gather(
                None,
                &rows,
                &[JoinPairRow {
                    probe: None,
                    build: Some(rows.row(2).unwrap()),
                }],
                owner.as_ref(),
                &work.host,
                &Control,
            )
            .unwrap();
        Arc::clone(output.record_batch().column(0))
    };
    drop(owner);
    assert!(tracker.current() > 0);
    let buffer = array.to_data().buffers()[0].clone();
    drop(array);
    assert!(tracker.current() > 0);
    drop(buffer);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn paged_gather_uses_dictionary_values_across_different_codes_and_underlying_nulls() {
    use crate::exec::pipeline::operator::{
        InputEncodingPolicy, hydrate_for_downstream_with_factory,
    };
    use arrow::array::DictionaryArray;
    // This is the same encoding declaration as the actual join E receiver.
    struct JoinInput;
    impl InputEncodingPolicy for JoinInput {}
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let carrier = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let build_schema = schema(std::slice::from_ref(&ty), &[2], owner.as_ref());
    let mut slots = funded_vec(owner.as_ref(), 1).unwrap();
    slots.push(
        ChunkSlotSchema::try_new_typed_observed::<ConstructionFailure, _>(
            SlotId::new(2),
            Arc::new(Field::new("c2", carrier.clone(), true)),
            ty.typed_binding().clone(),
            None,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap(),
    );
    let encoded_schema = funded_shared(
        owner.as_ref(),
        ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
            slots,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )
        .unwrap(),
    )
    .unwrap();
    let hydration = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), Arc::clone(&tracker),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let probe_schema = schema(
        &[FunctionValueType::new(DataType::Int32, true)],
        &[1],
        owner.as_ref(),
    );
    let probe = chunk(
        probe_schema.clone(),
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        owner.as_ref(),
    );
    let first: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        )
        .unwrap(),
    );
    let second: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![1, 0]),
            Arc::new(StringArray::from(vec!["b", "a"])),
        )
        .unwrap(),
    );
    let domains = ProjectionDomains::PayloadOnly;
    let mut receipts = Vec::new();
    for slot in 0..2 {
        let mut writer = InputRunWriter::new(
            &domains,
            B3RunId::from_input_slot(slot, 2).unwrap(),
            true,
            &work,
        )
        .unwrap();
        if slot == 0 {
            for array in [&first, &second] {
                let raw = chunk(encoded_schema.clone(), Arc::clone(array), owner.as_ref());
                assert_eq!(raw.columns()[0].data_type(), &carrier);
                let source = hydrate_for_downstream_with_factory(
                    &raw,
                    &JoinInput,
                    Some(hydration.as_ref()),
                    &mut conversions,
                )
                .unwrap();
                assert_eq!(source.columns()[0].data_type(), &DataType::Utf8);
                writer.push_payload(&source, &work).unwrap();
            }
        }
        receipts.push(writer.seal(&work).unwrap());
    }
    let (inputs, _lease) = collect(receipts, &work);
    let (rows, _) = BuildRows::from_inputs(inputs, &domains, &work).unwrap();
    let pairs = [
        JoinPairRow {
            probe: Some(0),
            build: Some(rows.row(3).unwrap()),
        },
        JoinPairRow {
            probe: Some(1),
            build: Some(rows.row(0).unwrap()),
        },
        JoinPairRow {
            probe: Some(2),
            build: Some(rows.row(1).unwrap()),
        },
        JoinPairRow {
            probe: Some(0),
            build: Some(rows.row(2).unwrap()),
        },
        JoinPairRow {
            probe: Some(1),
            build: None,
        },
    ];
    let gather = PagedJoinGather::new(
        build_schema.clone(),
        &probe_schema,
        &build_schema,
        owner.as_ref(),
    )
    .unwrap();
    let output = gather
        .gather(
            Some(&probe),
            &rows,
            &pairs,
            owner.as_ref(),
            &work.host,
            &Control,
        )
        .unwrap();
    assert_eq!(output.record_batch().column(0).data_type(), &DataType::Utf8);
    let values = output.record_batch().column(0);
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("b"), Some("a"), None, Some("a"), None]
    );
    let output = output.into_chunk(owner.as_ref()).unwrap();
    assert_eq!(output.columns()[0].logical_nulls().unwrap().null_count(), 2);
}
