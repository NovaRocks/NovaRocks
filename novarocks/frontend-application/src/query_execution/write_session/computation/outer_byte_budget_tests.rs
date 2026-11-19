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
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorEncodedPayload, ConnectorFieldPath,
};

struct ObservedDecoder {
    calls: Arc<AtomicUsize>,
    alias: Arc<Mutex<Option<bytes::Bytes>>>,
}
impl ConnectorWriteFragmentWireDecoder for ObservedDecoder {
    fn owner(&self) -> &str {
        "outer_byte_budget_test"
    }
    fn decode_commit_fragment_payload(
        &self,
        payload: &ConnectorEncodedPayload,
        _author: &novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ) -> Result<novarocks_spi::connector::write_stack::ConnectorCommitFragment, ConnectorCodecError>
    {
        self.calls.fetch_add(1, Ordering::AcqRel);
        *self.alias.lock().unwrap() = Some(payload.payload().clone());
        Err(ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_payload"),
            ConnectorCodecErrorKind::InvalidValue,
            "original Provider rejected its fragment",
        ))
    }
}
fn observed_decoder(
    target: WriteTargetOrdinal,
) -> (
    WriteComputationDecode,
    Arc<AtomicUsize>,
    Arc<Mutex<Option<bytes::Bytes>>>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let alias = Arc::new(Mutex::new(None));
    (
        WriteComputationDecode {
            origin: Arc::new(()),
            decoder: Arc::new(ObservedDecoder {
                calls: calls.clone(),
                alias: alias.clone(),
            }),
            receiving: WriteReceivingPort {
                session: Weak::new(),
            },
            query_targets: vec![target].into(),
            local_query: None,
        },
        calls,
        alias,
    )
}

#[test]
fn full_original_budget_refuses_outer_copy_before_provider_and_keeps_canonical_input() {
    let (_control, root, binding, _) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let window = binding.window_alias();
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let bytes = super::super::tests::commit_fragment_bytes();
    let count = bytes.len() as u64;
    let prepared =
        DecodedPreparedWriteSet::for_test(1, vec![(target, bytes)]).with_test_capacity(&binding);
    assert_eq!(window.retained_payload_bytes(), count);
    let limit = novarocks_workload_control::ResultCapacityConfig::V1.all_objects_bytes[2];
    let other_reserved_backing = window.reserve_payload_bytes(limit - 2 * count + 1).unwrap();
    let before = window.retained_payload_bytes();
    let (decoder, calls, alias) = observed_decoder(target);
    let error = decoder.interpret_borrowed(&prepared).unwrap_err();
    assert!(error.to_string().contains("retained result bytes"));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert!(alias.lock().unwrap().is_none());
    assert_eq!(window.retained_payload_bytes(), before);
    assert_eq!(prepared.fragments()[0].1.len() as u64, count);
    drop(other_reserved_backing);
    assert_eq!(window.retained_payload_bytes(), count);
    drop(prepared);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn provider_bytes_alias_keeps_only_its_outer_receipt_after_borrowed_failure() {
    let (_control, root, binding, _) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let window = binding.window_alias();
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let bytes = super::super::tests::commit_fragment_bytes();
    let count = bytes.len() as u64;
    let prepared =
        DecodedPreparedWriteSet::for_test(1, vec![(target, bytes)]).with_test_capacity(&binding);
    let (decoder, calls, alias) = observed_decoder(target);
    let error = decoder.interpret_borrowed(&prepared).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("original Provider rejected its fragment")
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(window.retained_payload_bytes(), 2 * count);
    // Borrowed logical-row inspection cannot retire the canonical input.
    drop(prepared);
    assert_eq!(window.retained_payload_bytes(), count);
    let last = alias.lock().unwrap().take().unwrap();
    assert!(!last.is_empty());
    drop(last);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn large_write_set_keeps_each_decoded_objects_receipt_until_drop() {
    use crate::query_execution::write_session::tests::{fixture_with_outcome, known_committed};
    let (_control, root, binding, _) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let window = binding.window_alias();
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let count = 16_385;
    let retained_per_value = 256;
    fixture.recorded.lock().unwrap().decoded_fragment_charge = Some(retained_per_value);
    use crate::query_execution::write_result::{
        RootWriteDecodeContract,
        tests::{fragment_record, row_count_record},
    };
    let record = fragment_record(super::super::tests::commit_fragment_bytes());
    let mut root_decoder = RootWriteResultDecoder::new_with_capacity(
        RootWriteDecodeContract::try_new(
            &[target],
            fixture.session.targets(),
            novarocks_result_contract::PerKind::empty(),
        )
        .unwrap(),
        &binding,
    )
    .unwrap();
    let mut decoder = fixture.session.computation_decode(&[target]).unwrap();
    for _ in 0..count {
        decoder.apply_body(&mut root_decoder, &record).unwrap();
    }
    assert_eq!(fixture.received_fragment_count(), count);
    assert_eq!(fixture.recorded.lock().unwrap().write_confirmations, 0);
    decoder
        .apply_body(&mut root_decoder, &row_count_record(1))
        .unwrap();
    root_decoder.check_relay_end((count + 1) as u64).unwrap();
    root_decoder.observe_root_eof().unwrap();
    let segment = decoder
        .complete_root(root_decoder.finish().unwrap())
        .unwrap();
    assert_eq!(segment.fragment_count, count);
    fixture.session.receiving_port().accept(segment).unwrap();
    assert_eq!(
        window.retained_payload_bytes(),
        (count * retained_per_value) as u64
    );
    drop(fixture);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn decoded_object_refusal_returns_already_admitted_receipts() {
    use crate::query_execution::write_session::tests::{fixture_with_outcome, known_committed};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let (_control, root, binding, _) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let window = binding.window_alias();
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let charge = 256;
    fixture.recorded.lock().unwrap().decoded_fragment_charge = Some(charge);
    let bytes = super::super::tests::commit_fragment_bytes();
    let outer_bytes = bytes.len() as u64;
    let prepared =
        DecodedPreparedWriteSet::for_test(1, vec![(target, bytes); 2]).with_test_capacity(&binding);
    let limit = novarocks_workload_control::ResultCapacityConfig::V1.all_objects_bytes[2];
    let capacity_for_one_decoded_object = outer_bytes + charge as u64;
    let other = window
        .reserve_payload_bytes(limit - 2 * outer_bytes - capacity_for_one_decoded_object)
        .unwrap();
    let before = window.retained_payload_bytes();
    let decoder = fixture.session.computation_decode(&[target]).unwrap();
    let error = decoder.interpret_borrowed(&prepared).unwrap_err();
    let (_, cause) = error.into_parts();
    assert_eq!(
        cause.unwrap().failure_fact().class(),
        FailureClass::Resource
    );
    assert_eq!(window.retained_payload_bytes(), before);
    drop((other, prepared));
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((binding, window));
    root.owner.complete();
    root.business.release();
}

#[test]
fn verification_value_follows_only_its_original_received_computation_cut() {
    use crate::query_execution::write_session::tests::{fixture_with_outcome, known_committed};
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let value = DeleteVerificationCounts::try_new(7, 5, 1, u128::MAX).unwrap();
    for (rows, verification) in [(3, Some(value)), (5, None)] {
        let prepared = DecodedPreparedWriteSet::for_test(rows, Vec::new());
        let prepared = match verification {
            Some(value) => prepared.with_test_delete_verification(value),
            None => prepared,
        };
        let segment = fixture
            .session
            .computation_decode(&fixture.session.expected_targets())
            .unwrap()
            .interpret(prepared)
            .unwrap();
        let accepted = fixture.session.receiving_port().accept(segment).unwrap();
        assert_eq!(accepted.row_count(), rows);
        assert_eq!(accepted.delete_verification(), verification);
        assert!(accepted.belongs_to(&fixture.session));
    }
    assert_eq!(fixture.session.accumulated_row_count().unwrap(), 8);
}

#[test]
fn arrival_refusal_keeps_original_resource_failure_and_leaves_query_unconfirmed() {
    use crate::query_execution::write_result::{RootWriteDecodeContract, tests::fragment_record};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let (_control, root, binding, _) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let window = binding.window_alias();
    let fixture =
        super::super::tests::fixture_with_outcome(1, 1, super::super::tests::known_committed());
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let record = fragment_record(super::super::tests::commit_fragment_bytes());
    let contract = RootWriteDecodeContract::try_new(
        &[target],
        fixture.session.targets(),
        novarocks_result_contract::PerKind::empty(),
    )
    .unwrap();
    let mut root_decoder = RootWriteResultDecoder::new_with_capacity(contract, &binding).unwrap();
    let mut decode = fixture.session.computation_decode(&[target]).unwrap();
    let refusal = ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        novarocks_workload_control::WorkError::Capacity("original Provider arrival"),
    );
    let expected = refusal.failure_fact();
    {
        let mut recorded = fixture.recorded.lock().unwrap();
        recorded.arrival_refusal = Some(refusal);
        recorded.decoded_fragment_charge = Some(256);
    }
    let original = decode.apply_body(&mut root_decoder, &record).unwrap_err();
    let (_, cause) = original.into_parts();
    let actual = cause.unwrap().failure_fact();
    assert_eq!(actual.class(), FailureClass::Resource);
    assert_eq!(actual, expected);
    assert_eq!(fixture.received_fragment_count(), 0);
    assert_eq!(fixture.recorded.lock().unwrap().write_confirmations, 0);
    assert_eq!(fixture.session.accumulated_row_count().unwrap(), 0);
    assert_eq!(window.retained_payload_bytes(), 0);
    drop((decode, root_decoder, fixture, binding, window));
    root.owner.complete();
    root.business.release();
}
