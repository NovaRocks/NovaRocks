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

use arrow::array::builder::{Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder};
use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use novarocks_execution::exec::node::table_write_relation::RootWriteResultRelationSchema;

use super::*;

mod record_handoff;

struct Row {
    kind: i8,
    target: Option<i32>,
    row_count: Option<i64>,
    fragment: Option<Vec<u8>>,
    input_fields: Option<Vec<Option<i32>>>,
    blob_type: Option<String>,
    body: Option<Vec<u8>>,
    properties: Option<Vec<(Option<String>, Option<String>)>>,
}

fn summary(row_count: i64) -> Row {
    Row {
        kind: RootRowKind::ROW_COUNT,
        target: None,
        row_count: Some(row_count),
        fragment: None,
        input_fields: None,
        blob_type: None,
        body: None,
        properties: None,
    }
}

fn fragment(target: i32, bytes: usize) -> Row {
    Row {
        kind: RootRowKind::PREPARED_FRAGMENT,
        target: Some(target),
        row_count: None,
        fragment: Some(vec![7_u8; bytes]),
        input_fields: None,
        blob_type: None,
        body: None,
        properties: None,
    }
}

fn artifact(target: i32, field_id: i32, blob_type: &str, body: &[u8]) -> Row {
    Row {
        kind: RootRowKind::ARTIFACT_DRAFT,
        target: Some(target),
        row_count: None,
        fragment: None,
        input_fields: Some(vec![Some(field_id)]),
        blob_type: Some(blob_type.to_string()),
        body: Some(body.to_vec()),
        properties: Some(vec![(Some("ndv".into()), Some("1".into()))]),
    }
}

fn chunk(rows: Vec<Row>) -> Chunk {
    let kinds: ArrayRef = Arc::new(Int8Array::from(
        rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
    ));
    let targets: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|row| row.target).collect::<Vec<_>>(),
    ));
    let counts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
    ));
    let fragments: ArrayRef = Arc::new(BinaryArray::from(
        rows.iter()
            .map(|row| row.fragment.as_deref())
            .collect::<Vec<_>>(),
    ));
    let mut fields = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    let mut properties = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    for row in &rows {
        match row.input_fields.as_ref() {
            Some(values) => {
                for value in values {
                    match value {
                        Some(value) => fields.values().append_value(*value),
                        None => fields.values().append_null(),
                    }
                }
                fields.append(true);
            }
            None => fields.append(false),
        }
        match row.properties.as_ref() {
            Some(values) => {
                for (key, value) in values {
                    match key {
                        Some(key) => properties.keys().append_value(key),
                        None => properties.keys().append_null(),
                    }
                    match value {
                        Some(value) => properties.values().append_value(value),
                        None => properties.values().append_null(),
                    }
                }
                properties.append(true).expect("map row");
            }
            None => properties.append(false).expect("null map row"),
        }
    }
    let blob_types: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.blob_type.as_deref())
            .collect::<Vec<_>>(),
    ));
    let bodies: ArrayRef = Arc::new(BinaryArray::from(
        rows.iter()
            .map(|row| row.body.as_deref())
            .collect::<Vec<_>>(),
    ));
    let relation = RootWriteResultRelationSchema::fixed();
    Chunk::try_new_with_columns(
        Arc::clone(relation.chunk_schema()),
        vec![
            kinds,
            targets,
            counts,
            fragments,
            Arc::new(fields.finish()),
            blob_types,
            bodies,
            Arc::new(properties.finish()),
            arrow::array::new_null_array(&arrow::datatypes::DataType::Int64, rows.len()),
            arrow::array::new_null_array(&arrow::datatypes::DataType::Int64, rows.len()),
            arrow::array::new_null_array(&arrow::datatypes::DataType::Int64, rows.len()),
            arrow::array::new_null_array(
                &arrow::datatypes::DataType::FixedSizeBinary(16),
                rows.len(),
            ),
        ],
    )
    .expect("root relation chunk")
}

/// Backend record encoder with the original session's real outer envelope.
#[cfg(novarocks_dev_controls)]
pub(crate) fn component_write_body(fragment: Vec<u8>) -> Vec<u8> {
    [fragment_record(fragment), row_count_record(3)].concat()
}

pub(crate) fn row_count_record(rows: i64) -> Vec<u8> {
    records(vec![summary(rows)]).pop().unwrap()
}

pub(crate) fn fragment_record(fragment: Vec<u8>) -> Vec<u8> {
    records(vec![Row {
        kind: RootRowKind::PREPARED_FRAGMENT,
        target: Some(0),
        row_count: None,
        fragment: Some(fragment),
        input_fields: None,
        blob_type: None,
        body: None,
        properties: None,
    }])
    .pop()
    .unwrap()
}

fn targets(count: u32) -> Vec<WriteTargetOrdinal> {
    (0..count)
        .map(|ordinal| WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"))
        .collect()
}

fn new_decoder(count: u32) -> RootWriteResultDecoder {
    RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
        targets(count),
        std::iter::empty(),
    ))
}

fn finish(mut decoder: RootWriteResultDecoder) -> Result<DecodedPreparedWriteSet, String> {
    decoder
        .observe_root_eof()
        .map_err(|error| error.to_string())?;
    decoder.finish().map_err(|error| error.to_string())
}

fn exact_decoder() -> RootWriteResultDecoder {
    let target = WriteTargetOrdinal::try_new(0).expect("target");
    let identity = StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").expect("identity");
    RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
        [target],
        [(target, identity)],
    ))
}

/// Encode `rows` exactly as the Backend root producer does.
fn records(rows: Vec<Row>) -> Vec<Vec<u8>> {
    use novarocks_native_adapter::root_record_assembly::{RootRecordAssembly, RootRecordDomain};
    use novarocks_native_adapter::root_write_commit_codec::{
        WriteCommitEncoder, WriteCommitTotals,
    };
    let mut encoder = WriteCommitEncoder::try_new(
        chunk(rows).record_batch().clone(),
        WriteCommitTotals::default(),
    )
    .expect("fixed relation");
    let mut stream = Vec::new();
    let mut output = vec![0_u8; 7];
    loop {
        let turn = encoder.step(&mut output).expect("encodable rows");
        stream.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == novarocks_result_render::RenderTurnStatus::InputComplete {
            break;
        }
    }
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::WriteCommit, 1 << 20);
    let mut out = Vec::new();
    for body in stream.chunks(5) {
        assembly
            .push(body, |record| {
                out.push(record.to_vec());
                Ok::<(), std::convert::Infallible>(())
            })
            .unwrap();
    }
    assembly.finish().unwrap();
    out
}

#[test]
fn relayed_record_sink_keeps_original_resource_fact_through_domain_projection() {
    use novarocks_failure_contract::FailureProjection;
    let original = novarocks_spi::connector::ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
        "original test resource refused a write record",
    );
    let expected = original.failure_fact();
    let body = records(vec![summary(1)]).remove(0);
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::WriteCommit, 1 << 20);
    let rejected = assembly
        .push(&body, |_| {
            Err::<(), RootWriteDecodeFailure>(RootWriteDecodeFailure::source(original.clone()))
        })
        .unwrap_err();
    let failure = RootWriteDecodeFailure::from(rejected);
    assert_eq!(failure.source.as_ref().unwrap().failure_fact(), expected);
    let domain =
        crate::query_execution::internal_consumer::domain_state::InternalDomainFailure::from(
            failure,
        );
    assert_eq!(domain.execution_input().unwrap().fact(), expected);
    let crate::query_execution::internal_consumer::domain_state::InternalDomainFailure::Source(
        source,
    ) = domain
    else {
        unreachable!()
    };
    assert_eq!(
        source.downcast_ref::<novarocks_spi::connector::ConnectorError>(),
        Some(&original)
    );
}

#[test]
fn production_write_decoder_without_author_is_refused_even_in_tests() {
    let mut decoder = RootWriteResultDecoder::new(RootWriteDecodeContract::for_test(
        targets(1),
        std::iter::empty(),
    ));
    let record = records(vec![fragment(0, 16)]).remove(0);
    let error = decoder.apply_relay_body(&record).unwrap_err();
    assert_eq!(
        error
            .source
            .unwrap()
            .downcast_ref::<novarocks_spi::connector::ConnectorError>()
            .unwrap()
            .kind(),
        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(decoder.relay_assembly.retained_bytes(), 0);
    assert!(decoder.fragments.is_empty());
}

fn original_assembly_budget(
    bytes: usize,
) -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
    novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) {
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    let mut result = ResultCapacityConfig::V1;
    result.all_objects_bytes[2] = bytes as u64;
    control.configure_result_capacity(result).unwrap();
    control.mark_ready().unwrap();
    let (root, window) = control
        .root_admission()
        .try_begin_root_with_result(
            WorkRequest::new(WorkClass::Management),
            ResultWindowClass::Internal,
        )
        .unwrap();
    let binding =
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &root.owner.scope(),
            window.retain_alias(),
        )
        .unwrap();
    (control, root, binding)
}

#[test]
fn original_result_budget_refuses_partial_header_before_backing_and_preserves_source() {
    let record = records(vec![fragment(0, 16)]).remove(0);
    let header = novarocks_native_adapter::root_write_commit_codec::WRITE_COMMIT_HEADER_BYTES;
    assert!(record.len() > header);
    let (_control, root, binding) = original_assembly_budget(header - 1);
    let window = binding.window_alias();
    let mut decoder = RootWriteResultDecoder::new_with_capacity(
        RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
        &binding,
    )
    .unwrap();
    let error = decoder.apply_relay_body(&record[..header]).unwrap_err();
    let original = error.source.unwrap();
    assert_eq!(
        original
            .downcast_ref::<novarocks_spi::connector::ConnectorError>()
            .unwrap()
            .kind(),
        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(decoder.relay_assembly.retained_bytes(), 0);
    assert_eq!(window.retained_payload_bytes(), 0);
    assert!(decoder.fragments.is_empty());
    drop((decoder, binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn partial_replacement_needs_old_and_new_bytes_and_last_partial_exit_refunds() {
    let record = records(vec![fragment(0, 16)]).remove(0);
    let header = novarocks_native_adapter::root_write_commit_codec::WRITE_COMMIT_HEADER_BYTES;
    let (_control, root, binding) = original_assembly_budget(record.len());
    let window = binding.window_alias();
    let mut decoder = RootWriteResultDecoder::new_with_capacity(
        RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
        &binding,
    )
    .unwrap();
    decoder.apply_relay_body(&record[..1]).unwrap();
    assert_eq!(decoder.relay_assembly.retained_bytes(), header);
    assert_eq!(window.retained_payload_bytes(), header as u64);
    // New full record alone fits; old header plus new record does not.
    let error = decoder.apply_relay_body(&record[1..header]).unwrap_err();
    assert_eq!(
        error
            .source
            .unwrap()
            .downcast_ref::<novarocks_spi::connector::ConnectorError>()
            .unwrap()
            .kind(),
        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(decoder.relay_assembly.retained_bytes(), header);
    assert_eq!(window.retained_payload_bytes(), header as u64);
    assert!(decoder.fragments.is_empty());
    drop(decoder);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn replaced_partial_keeps_exact_receipt_through_original_sink_failure_and_drop() {
    let record = records(vec![fragment(0, 16)]).remove(0);
    let header = novarocks_native_adapter::root_write_commit_codec::WRITE_COMMIT_HEADER_BYTES;
    let (_control, root, binding) = original_assembly_budget(header + record.len());
    let window = binding.window_alias();
    let mut assembly = RootRecordAssembly::new(RootRecordDomain::WriteCommit, record.len());
    let mut observations = Vec::new();
    let reserve = |bytes| {
        observations.push((bytes, window.retained_payload_bytes()));
        window
            .reserve_payload_bytes(bytes as u64)
            .map(novarocks_spi::connector::ConnectorPayloadRetentionGuard::new)
            .map_err(RootWriteDecodeFailure::source)
    };
    assembly
        .push_budgeted(&record[..header], reserve, |_| Ok(()))
        .unwrap();
    assert_eq!(observations, [(header, 0), (record.len(), header as u64)]);
    assert_eq!(assembly.retained_bytes(), record.len());
    assert_eq!(window.retained_payload_bytes(), record.len() as u64);
    let original = novarocks_spi::connector::ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
        "original test resource refused a write record",
    );
    let error = assembly
        .push_budgeted(
            &record[header..],
            |bytes| {
                window
                    .reserve_payload_bytes(bytes as u64)
                    .map(novarocks_spi::connector::ConnectorPayloadRetentionGuard::new)
                    .map_err(RootWriteDecodeFailure::source)
            },
            |received| {
                assert_eq!(received, record.as_slice());
                Err(RootWriteDecodeFailure::source(original.clone()))
            },
        )
        .unwrap_err();
    let error = RootWriteDecodeFailure::from(error);
    assert_eq!(
        error
            .source
            .unwrap()
            .downcast_ref::<novarocks_spi::connector::ConnectorError>(),
        Some(&original)
    );
    assert_eq!(window.retained_payload_bytes(), record.len() as u64);
    drop(assembly);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn complete_borrowed_record_charges_only_its_retained_fragment_and_hands_receipt_off() {
    let record = records(vec![fragment(0, 16)]).remove(0);
    let (_control, root, binding) = original_assembly_budget(16);
    let window = binding.window_alias();
    let mut decoder = RootWriteResultDecoder::new_with_capacity(
        RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
        &binding,
    )
    .unwrap();
    decoder.apply_relay_body(&record).unwrap();
    decoder
        .apply_relay_body(&records(vec![summary(1)]).remove(0))
        .unwrap();
    assert_eq!(decoder.relay_assembly.retained_bytes(), 0);
    assert_eq!(window.retained_payload_bytes(), 16);
    let prepared = finish(decoder).unwrap();
    assert_eq!(window.retained_payload_bytes(), 16);
    drop(prepared);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn prepared_payload_and_last_statistics_body_clone_retain_actual_capacity() {
    let (_control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let contract = RootWriteDecodeContract::for_test(
        [target],
        [(
            target,
            StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").unwrap(),
        )],
    );
    let mut decoder = RootWriteResultDecoder::new_with_capacity(contract, &binding).unwrap();
    for record in records(vec![
        fragment(0, 5),
        artifact(0, 11, "theta-v1", b"sketch"),
        summary(9),
    ]) {
        decoder.apply_relay_body(&record).unwrap();
    }
    let prepared = finish(decoder).unwrap();
    let body = prepared.statistics[0].draft().body().clone();
    drop(binding);
    root.owner.complete();
    root.business.release();
    assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
    drop(prepared);
    assert_eq!(body.as_ref(), b"sketch");
    assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
    drop(body);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
}

#[test]
fn fragment_only_prepared_set_retains_capacity_until_its_payload_exits() {
    let (_control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let mut decoder = RootWriteResultDecoder::new_with_capacity(
        RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
        &binding,
    )
    .unwrap();
    for record in records(vec![fragment(0, 5), summary(9)]) {
        decoder.apply_relay_body(&record).unwrap();
    }
    let prepared = finish(decoder).unwrap();
    drop(binding);
    root.owner.complete();
    root.business.release();
    assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
    drop(prepared);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
}

#[test]
fn relayed_records_decode_to_the_same_prepared_set_as_the_arrow_relation() {
    let rows = || {
        vec![
            fragment(0, 5),
            artifact(0, 11, "theta-v1", b"sketch"),
            fragment(0, 3),
            summary(9),
        ]
    };
    let mut from_chunk = exact_decoder();
    from_chunk.apply_chunk(&chunk(rows())).unwrap();
    let from_chunk = finish(from_chunk).unwrap();
    let mut from_records = exact_decoder();
    for record in records(rows()) {
        for body in record.chunks(3) {
            from_records.apply_relay_body(body).unwrap();
        }
    }
    let from_records = finish(from_records).unwrap();
    assert_eq!(from_records.row_count, from_chunk.row_count);
    assert_eq!(from_records.fragments, from_chunk.fragments);
    assert_eq!(from_records.statistics, from_chunk.statistics);
}

#[test]
fn relayed_body_end_refuses_partial_record_without_publishing_eof() {
    let record = records(vec![fragment(0, 5)]).remove(0);
    let mut decoder = new_decoder(1);
    decoder.apply_relay_body(&record[..7]).unwrap();
    assert!(
        decoder
            .observe_root_eof()
            .unwrap_err()
            .contains("unfinished")
    );
    assert!(!decoder.root_eof);
    decoder.apply_relay_body(&record[7..]).unwrap();
    let tail = records(vec![summary(5)]).remove(0);
    decoder.apply_relay_body(&tail).unwrap();
    assert!(decoder.check_relay_end(1).is_err());
    assert!(!decoder.root_eof);
    decoder.check_relay_end(2).unwrap();
    let prepared = finish(decoder).unwrap();
    assert_eq!(prepared.row_count(), 5);
}

#[test]
fn relayed_records_obey_membership_targets_and_artifact_sets() {
    // A target outside the frozen set is refused.
    let mut decoder = new_decoder(1);
    let foreign = records(vec![fragment(3, 1), summary(0)]);
    assert!(decoder.apply_record(&foreign[0]).is_err());
    // An unexpected artifact identity is refused.
    let mut decoder = exact_decoder();
    let unexpected = records(vec![artifact(0, 12, "theta-v1", b"x")]);
    assert!(decoder.apply_record(&unexpected[0]).is_err());
    // A missing ROW_COUNT fails at finish; records after EOF are refused.
    let mut decoder = new_decoder(1);
    let only_fragment = records(vec![fragment(0, 1)]);
    decoder.apply_record(&only_fragment[0]).unwrap();
    decoder.observe_root_eof().unwrap();
    assert!(decoder.apply_record(&only_fragment[0]).is_err());
    assert!(decoder.finish().is_err());
    // A truncated record is refused before any fact is applied.
    let mut decoder = new_decoder(1);
    let record = &records(vec![fragment(0, 4), summary(1)])[0];
    assert!(decoder.apply_record(&record[..record.len() - 1]).is_err());
}

#[test]
fn a_complete_set_spans_several_batches_and_keeps_its_fragments() {
    let mut decoder = new_decoder(2);
    decoder
        .apply_chunk(&chunk(vec![summary(42), fragment(0, 8)]))
        .expect("first batch");
    decoder
        .apply_chunk(&chunk(vec![fragment(1, 16), fragment(0, 4)]))
        .expect("second batch");
    let set = finish(decoder).expect("complete set");
    assert_eq!(set.row_count(), 42);
    assert_eq!(set.fragments().len(), 3);
    assert_eq!(set.fragments()[0].0.get(), 0);
    assert_eq!(set.fragments()[1].0.get(), 1);
    assert_eq!(set.fragments()[0].1.len(), 8);
}

#[test]
fn a_write_that_staged_nothing_is_still_a_complete_set() {
    let mut decoder = new_decoder(1);
    decoder
        .apply_chunk(&chunk(vec![summary(0)]))
        .expect("batch");
    let set = finish(decoder).expect("complete set");
    assert_eq!(set.row_count(), 0);
    assert!(set.fragments().is_empty());
}

#[test]
fn a_prefix_without_a_summary_is_not_a_partial_write_but_no_write() {
    let mut decoder = new_decoder(1);
    decoder
        .apply_chunk(&chunk(vec![fragment(0, 8)]))
        .expect("batch");
    let error = finish(decoder).expect_err("no summary");
    assert!(
        error.contains("missing its ROW_COUNT row"),
        "unexpected error: {error}"
    );
}

#[test]
fn two_summary_rows_would_mean_two_roots_and_are_refused() {
    let mut decoder = new_decoder(1);
    let error = decoder
        .apply_chunk(&chunk(vec![summary(1), summary(2)]))
        .expect_err("two summaries");
    assert!(
        error.contains("more than one ROW_COUNT"),
        "unexpected error: {error}"
    );
}

#[test]
fn a_row_whose_payload_contradicts_its_kind_is_refused() {
    // A summary that also claims a target ordinal.
    let mut decoder = new_decoder(1);
    assert!(
        decoder
            .apply_chunk(&chunk(vec![Row {
                kind: RootRowKind::ROW_COUNT,
                target: Some(0),
                row_count: Some(1),
                fragment: None,
                input_fields: None,
                blob_type: None,
                body: None,
                properties: None,
            }]))
            .is_err()
    );

    // A fragment row with no fragment.
    let mut decoder = new_decoder(1);
    assert!(
        decoder
            .apply_chunk(&chunk(vec![Row {
                kind: RootRowKind::PREPARED_FRAGMENT,
                target: Some(0),
                row_count: None,
                fragment: None,
                input_fields: None,
                blob_type: None,
                body: None,
                properties: None,
            }]))
            .is_err()
    );

    // An unknown kind.
    let mut decoder = new_decoder(1);
    assert!(
        decoder
            .apply_chunk(&chunk(vec![Row {
                kind: 9,
                target: None,
                row_count: Some(1),
                fragment: None,
                input_fields: None,
                blob_type: None,
                body: None,
                properties: None,
            }]))
            .is_err()
    );
}

#[test]
fn a_fragment_naming_a_target_outside_the_sealed_set_is_refused() {
    let mut decoder = new_decoder(1);
    let error = decoder
        .apply_chunk(&chunk(vec![fragment(1, 4)]))
        .expect_err("foreign target");
    assert!(
        error.contains("outside this query's frozen set"),
        "unexpected error: {error}"
    );
}

#[test]
fn a_negative_row_count_is_corrupt_rather_than_a_huge_unsigned_one() {
    let mut decoder = new_decoder(1);
    let error = decoder
        .apply_chunk(&chunk(vec![summary(-1)]))
        .expect_err("negative row count");
    assert!(error.contains("row shape"), "unexpected error: {error}");
}

#[test]
fn the_frontend_receives_a_fragment_larger_than_one_mib() {
    let mut decoder = new_decoder(1);
    let bytes = 1024 * 1024 + 1;
    decoder
        .apply_chunk(&chunk(vec![fragment(0, bytes), summary(0)]))
        .expect("large fragment");
    let prepared = finish(decoder).expect("complete write");
    assert_eq!(prepared.fragments()[0].1.len(), bytes);
}

#[test]
fn query_local_membership_is_exact_not_max_ordinal_based() {
    let target_five = WriteTargetOrdinal::try_new(5).expect("target");
    let mut decoder = RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
        [target_five],
        std::iter::empty(),
    ));
    let error = decoder
        .apply_chunk(&chunk(vec![fragment(4, 4)]))
        .expect_err("target below the maximum is still foreign");
    assert!(error.contains("outside this query's frozen set"));
}

#[test]
fn artifact_identity_includes_the_query_local_target() {
    let target_zero = WriteTargetOrdinal::try_new(0).expect("target");
    let target_one = WriteTargetOrdinal::try_new(1).expect("target");
    let identity =
        StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").expect("artifact identity");
    let mut decoder = RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
        [target_zero, target_one],
        [
            (target_zero, identity.clone()),
            (target_one, identity.clone()),
        ],
    ));
    decoder
        .apply_chunk(&chunk(vec![
            summary(2),
            artifact(0, 11, "theta-v1", b"a"),
            artifact(1, 11, "theta-v1", b"b"),
        ]))
        .expect("two target-qualified artifacts");
    let complete = finish(decoder).expect("complete Root result");
    assert_eq!(complete.statistics.len(), 2);
    assert_eq!(complete.statistics[0].target(), target_zero);
    assert_eq!(complete.statistics[1].target(), target_one);
}

#[test]
fn the_decoder_requires_eof_and_does_not_own_task_success() {
    let mut no_eof = new_decoder(1);
    no_eof.apply_chunk(&chunk(vec![summary(0)])).expect("row");
    assert!(
        no_eof
            .finish()
            .unwrap_err()
            .contains("EOF was not observed")
    );

    let mut complete_root = new_decoder(1);
    complete_root
        .apply_chunk(&chunk(vec![summary(0)]))
        .expect("row");
    complete_root.observe_root_eof().expect("EOF");
    assert_eq!(
        complete_root
            .finish()
            .expect("complete Root relation")
            .row_count(),
        0
    );
}

#[test]
fn exact_root_rows_and_eof_preserve_the_original_complete_payload() {
    let mut decoder = exact_decoder();
    decoder
        .apply_chunk(&chunk(vec![
            summary(7),
            fragment(0, 3),
            artifact(0, 11, "theta-v1", b"sketch"),
        ]))
        .unwrap();
    decoder.observe_root_eof().unwrap();
    let prepared = decoder.finish().expect("complete Root relation");
    assert_eq!(prepared.row_count(), 7);
    assert_eq!(prepared.fragments().len(), 1);
    assert_eq!(prepared.fragments()[0].1, vec![7_u8; 3]);
    assert_eq!(prepared.statistics.len(), 1);
    assert_eq!(prepared.statistics[0].target().get(), 0);
    assert_eq!(
        prepared.statistics[0].draft().identity().input_fields(),
        &[11]
    );
    assert_eq!(prepared.statistics[0].draft().body().as_ref(), b"sketch");
}

#[test]
fn root_eof_before_rows_rejects_the_original_trailing_batch() {
    let mut decoder = exact_decoder();
    decoder.observe_root_eof().unwrap();
    let error = decoder
        .apply_chunk(&chunk(vec![
            summary(7),
            fragment(0, 3),
            artifact(0, 11, "theta-v1", b"sketch"),
        ]))
        .expect_err("EOF closes the Root stream");
    assert!(error.to_string().contains("trailing batch after EOF"));
}

#[test]
fn undeclared_deletion_verification_is_refused_in_arrow_and_record_forms() {
    use novarocks_native_adapter::root_write_commit_codec::{
        WriteCommitEncoder, WriteCommitTotals,
    };
    let template = chunk(vec![summary(0)]);
    let mut columns = template.columns().to_vec();
    columns[0] = Arc::new(Int8Array::from(vec![RootRowKind::DELETE_VERIFICATION]));
    columns[2] = arrow::array::new_null_array(&arrow::datatypes::DataType::Int64, 1);
    for (index, value) in [(8, 7), (9, 5), (10, 1)] {
        columns[index] = Arc::new(Int64Array::from(vec![Some(value)]));
    }
    let mut fingerprint = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
    fingerprint.append_value([3; 16]).unwrap();
    columns[11] = Arc::new(fingerprint.finish());
    let verification = Chunk::try_new_with_columns(template.chunk_schema_ref(), columns).unwrap();
    let mut decoder = exact_decoder();
    assert!(
        decoder
            .apply_chunk(&verification)
            .unwrap_err()
            .to_string()
            .contains("undeclared summary kind DeleteVerification")
    );
    let mut encoder = WriteCommitEncoder::try_new(
        verification.record_batch().clone(),
        WriteCommitTotals::default(),
    )
    .unwrap();
    let mut bytes = [0; 128];
    let turn = encoder.step(&mut bytes).unwrap();
    assert_eq!(turn.emitted_bytes, 88);
    let mut decoder = exact_decoder();
    assert!(
        decoder
            .apply_record(&bytes[..turn.emitted_bytes])
            .unwrap_err()
            .to_string()
            .contains("undeclared summary kind DeleteVerification")
    );
}

fn verification_chunk(value: DeleteVerificationCounts) -> Chunk {
    let template = chunk(vec![summary(0)]);
    let mut columns = template.columns().to_vec();
    columns[0] = Arc::new(Int8Array::from(vec![RootRowKind::DELETE_VERIFICATION]));
    columns[2] = arrow::array::new_null_array(&arrow::datatypes::DataType::Int64, 1);
    let (counts, fingerprint) = value.into_wire();
    for (index, value) in [8, 9, 10].into_iter().zip(counts) {
        columns[index] = Arc::new(Int64Array::from(vec![Some(value)]));
    }
    let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
    builder.append_value(fingerprint).unwrap();
    columns[11] = Arc::new(builder.finish());
    Chunk::try_new_with_columns(template.chunk_schema_ref(), columns).unwrap()
}

fn declared_verification_decoder() -> RootWriteResultDecoder {
    let mut contract = RootWriteDecodeContract::for_test(targets(1), std::iter::empty());
    contract.declared_summary_kinds.delete_verification = Some(());
    RootWriteResultDecoder::for_test_local(contract)
}

#[test]
fn declared_verification_is_exact_once_and_preserves_values_in_both_root_forms() {
    use novarocks_native_adapter::root_write_commit_codec::{
        WriteCommitEncoder, WriteCommitTotals,
    };
    let value = DeleteVerificationCounts::try_new(9, 6, 2, u128::MAX - 3).unwrap();
    let verification = verification_chunk(value);
    for relayed in [false, true] {
        let mut decoder = declared_verification_decoder();
        if relayed {
            let row_count = records(vec![summary(17)]);
            decoder.apply_record(&row_count[0]).unwrap();
            let mut encoder = WriteCommitEncoder::try_new(
                verification.record_batch().clone(),
                WriteCommitTotals::default(),
            )
            .unwrap();
            let mut bytes = [0; 128];
            let turn = encoder.step(&mut bytes).unwrap();
            decoder.apply_record(&bytes[..turn.emitted_bytes]).unwrap();
        } else {
            decoder.apply_chunk(&chunk(vec![summary(17)])).unwrap();
            decoder.apply_chunk(&verification).unwrap();
        }
        decoder.observe_root_eof().unwrap();
        let prepared = decoder.finish().unwrap();
        assert_eq!(prepared.row_count(), 17);
        let DecodedPreparedWriteSet {
            row_count: rows,
            delete_verification: actual,
            fragments,
            statistics,
            ..
        } = prepared;
        assert_eq!((rows, actual), (17, Some(value)));
        assert!(fragments.is_empty());
        assert!(statistics.is_empty());
    }
    let mut missing = declared_verification_decoder();
    missing.apply_chunk(&chunk(vec![summary(17)])).unwrap();
    missing.observe_root_eof().unwrap();
    assert!(
        missing
            .finish()
            .unwrap_err()
            .to_string()
            .contains("missing declared summary kind DeleteVerification")
    );
    let mut duplicate = declared_verification_decoder();
    duplicate.apply_chunk(&verification).unwrap();
    assert!(duplicate.apply_chunk(&verification).is_err());
    let mut no_eof = declared_verification_decoder();
    no_eof.apply_chunk(&chunk(vec![summary(17)])).unwrap();
    no_eof.apply_chunk(&verification).unwrap();
    assert!(no_eof.finish().unwrap_err().to_string().contains("EOF"));
}

#[test]
fn write_counts_declaration_still_uses_the_ordinary_row_count_record() {
    let mut contract = RootWriteDecodeContract::for_test(targets(1), std::iter::empty());
    contract.declared_summary_kinds.write_counts = Some(());
    let mut decoder = RootWriteResultDecoder::for_test_local(contract);
    decoder.apply_chunk(&chunk(vec![summary(23)])).unwrap();
    let prepared = finish(decoder).unwrap();
    assert_eq!(prepared.row_count(), 23);
    assert_eq!(prepared.delete_verification, None);
}
