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
//! Source-local functional witnesses for external codec admission and cleanup.
//! N maintains these on codec/resource changes. No fixture or historical run is
//! required. Host ledgers observe grants and typed blocks, not library bytes.
use super::*;
use crate::aggregate_scalar::{self as scalar, TrackedAggScalarValue};
use crate::builtin::aggregate_by_core::ByEncodeBuffer;
use crate::builtin::aggregate_n_core::{
    NState, decode_payload_at, decode_payload_at_with_codec_resources,
};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl};
use arrow_array::{ArrayRef, BinaryArray};
use arrow_buffer::i256;
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
pub(in crate::builtin) struct Ledger {
    pub(in crate::builtin) blocks: Vec<(usize, Layout)>,
    pub(in crate::builtin) loans: Vec<usize>,
    pub(in crate::builtin) attempts: usize,
    pub(in crate::builtin) typed_attempts: usize,
    pub(in crate::builtin) grants: usize,
    pub(in crate::builtin) releases: usize,
    pub(in crate::builtin) granted_bytes: usize,
    pub(in crate::builtin) released_bytes: usize,
}
#[derive(Default)]
pub(in crate::builtin) struct Host {
    pub(in crate::builtin) ledger: Mutex<Ledger>,
    pub(in crate::builtin) refusal: Option<(usize, KernelFailure)>,
    pub(in crate::builtin) armed_opaque: Mutex<Option<(usize, KernelFailure)>>,
    pub(in crate::builtin) armed_typed: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        {
            let mut ledger = self.ledger.lock().unwrap();
            let at = ledger.typed_attempts;
            ledger.typed_attempts += 1;
            if let Some((stop, cause)) = self.armed_typed.lock().unwrap().as_ref() {
                assert!(at <= *stop, "typed allocation after primary refusal");
                if at == *stop {
                    return Err(cause.clone());
                }
            }
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        self.ledger
            .lock()
            .unwrap()
            .blocks
            .push((pointer.as_ptr().addr(), layout));
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let index = ledger
            .blocks
            .iter()
            .position(|entry| *entry == (pointer.as_ptr().addr(), layout))
            .expect("exact typed block released once");
        ledger.blocks.swap_remove(index);
        unsafe {
            std::alloc::dealloc(pointer.as_ptr(), layout);
        }
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        let refusal = self
            .refusal
            .clone()
            .or_else(|| self.armed_opaque.lock().unwrap().clone());
        if let Some((stop, cause)) = &refusal {
            assert!(at <= *stop, "admission after primary refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        ledger.loans.push(bytes);
        ledger.grants += 1;
        ledger.granted_bytes += bytes;
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut ledger = self.ledger.lock().unwrap();
        // The opaque port grants fungible bytes. Custody may split a prior
        // operation loan into retained output and an unused remainder.
        let available: usize = ledger.loans.iter().sum();
        assert!(
            bytes <= available,
            "opaque release exceeds actual live funding"
        );
        ledger.released_bytes += bytes;
        let mut remaining = bytes;
        while remaining != 0 {
            let index = ledger
                .loans
                .iter()
                .position(|entry| *entry == remaining)
                .unwrap_or(ledger.loans.len() - 1);
            let released = remaining.min(ledger.loans[index]);
            ledger.loans[index] -= released;
            remaining -= released;
            if ledger.loans[index] == 0 {
                ledger.loans.swap_remove(index);
                ledger.releases += 1;
            }
        }
    }
}
pub(in crate::builtin) struct Control {
    host: Arc<Host>,
    pub(in crate::builtin) refusal: Option<(usize, KernelFailure)>,
    pub(in crate::builtin) checks: Mutex<Vec<(u32, usize)>>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut checks = self.checks.lock().unwrap();
        let at = checks.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after refusal");
        }
        checks.push((units, self.host.ledger.lock().unwrap().loans.len()));
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("N codec never waits")
    }
}
pub(in crate::builtin) fn control(
    host: &Arc<Host>,
    refusal: Option<(usize, KernelFailure)>,
) -> Control {
    Control {
        host: Arc::clone(host),
        refusal,
        checks: Mutex::new(Vec::new()),
    }
}
fn allocator(host: &Arc<Host>) -> HostAggregateAllocator {
    HostAggregateAllocator::try_new(Arc::clone(host) as Arc<dyn AggregateStateAllocator>).unwrap()
}
fn charge(host: &Arc<Host>) -> OpaqueRetainedCharge {
    OpaqueRetainedCharge::try_new(Arc::clone(host) as Arc<dyn AggregateStateAllocator>).unwrap()
}
pub(in crate::builtin) fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("codec original cause"),
        internal("codec original cause"),
        KernelFailure::Operational(KernelDiagnostic::new("codec original cause")),
        KernelFailure::InstanceFailed,
    ]
}
fn kernel_cause(error: ScalarStateError) -> KernelFailure {
    match error {
        ScalarStateError::Kernel(cause) => cause,
        other => panic!("expected typed cause, got {other:?}"),
    }
}
pub(in crate::builtin) fn clean(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert!(ledger.blocks.is_empty());
    assert!(ledger.loans.is_empty());
    assert_eq!(ledger.grants, ledger.releases);
    assert_eq!(ledger.granted_bytes, ledger.released_bytes);
}
fn state(
    allocator: &HostAggregateAllocator,
    values: Vec<TrackedAggScalarValue<HostAggregateAllocator>>,
) -> NState<HostAggregateAllocator> {
    let mut state = NState::new(allocator.clone());
    state.initialized = true;
    state.limit = 17;
    state.values.try_reserve_exact(values.len()).unwrap();
    state.values.extend(values);
    state
}
#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    appends: usize,
    host: Option<Arc<Host>>,
    pub(in crate::builtin) refusal: Option<(usize, KernelFailure)>,
    protected: usize,
}
impl ByEncodeBuffer for Writer {
    fn push(&mut self, byte: u8) -> Result<(), ScalarStateError> {
        self.bytes.push(byte);
        Ok(())
    }
    fn append(&mut self, bytes: &[u8], _: &mut ScalarWork<'_, '_>) -> Result<(), ScalarStateError> {
        let at = self.appends;
        self.appends += 1;
        // Header writes precede the decimal call; both length and text appends
        // happen with the distinct OWN and library reservations still live.
        if at >= 2 {
            if let Some(host) = &self.host {
                assert_eq!(host.ledger.lock().unwrap().loans.len(), 2);
                self.protected += 1;
            }
        }
        if let Some((stop, cause)) = &self.refusal {
            assert!(at <= *stop, "append after primary refusal");
            if at == *stop {
                return Err(cause.clone().into());
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}
struct Observed<'a> {
    bound: BoundNCodecResources<'a>,
    expected_text: Option<&'a str>,
    formats: usize,
    parsed: usize,
}
impl NCodecResources for Observed<'_> {
    type Lease = NCodecLease;
    fn before_decimal256_format(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<NCodecLease, KernelFailure> {
        self.bound.before_decimal256_format(work)
    }
    fn after_decimal256_format(
        &mut self,
        text: &str,
        capacity: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        if let Some(expected) = self.expected_text {
            assert_eq!(text, expected);
        }
        // Upgrade-sensitive protection of the OWN writer extent, not a private
        // num-bigint capacity or timing measurement.
        assert!(capacity <= 78 && text.len() <= 78);
        self.formats += 1;
        self.bound.after_decimal256_format(text, capacity, work)
    }
    fn before_decimal256_parse(
        &mut self,
        bytes: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<NCodecLease, KernelFailure> {
        self.bound.before_decimal256_parse(bytes, work)
    }
    fn after_decimal256_parse(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        self.parsed += 1;
        self.bound.after_decimal256_parse(work)
    }
}
fn observed<'a>(
    charge: &'a OpaqueRetainedCharge,
    allocator: &'a HostAggregateAllocator,
    expected_text: Option<&'a str>,
) -> Observed<'a> {
    Observed {
        bound: BoundNCodecResources { charge, allocator },
        expected_text,
        formats: 0,
        parsed: 0,
    }
}
const MIN_TEXT: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
const MAX_TEXT: &str =
    "57896044618658097711785492504343953926634992332820282019728792003956564819967";
fn payload(text: &[u8], limit: u32) -> ArrayRef {
    let mut bytes = Vec::from(limit.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.push(11);
    bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
    bytes.extend_from_slice(text);
    Arc::new(BinaryArray::from(vec![bytes.as_slice()]))
}

#[test]
fn decimal_extrema_hold_string_and_library_loans_through_original_appends() {
    for (value, text) in [
        (i256::MIN, MIN_TEXT),
        (i256::MAX, MAX_TEXT),
        (i256::ZERO, "0"),
        (i256::from_i128(-1), "-1"),
        (i256::from_i128(1), "1"),
    ] {
        let host = Arc::new(Host::default());
        {
            let allocator = allocator(&host);
            let charge = charge(&host);
            let state = state(&allocator, vec![TrackedAggScalarValue::Decimal256(value)]);
            let control = control(&host, None);
            let mut checkpoints = EvaluationCheckpoints::new(&control);
            let mut work = ScalarWork::new(Some(&mut checkpoints));
            let mut resources = observed(&charge, &allocator, Some(text));
            let mut writer = Writer {
                host: Some(Arc::clone(&host)),
                ..Writer::default()
            };
            state
                .serialize_with_codec_resources(&mut writer, &mut resources, &mut work)
                .unwrap();
            assert_eq!(writer.protected, 2);
            assert_eq!(writer.bytes[8], 11);
            assert_eq!(
                u32::from_le_bytes(writer.bytes[9..13].try_into().unwrap()) as usize,
                text.len()
            );
            assert_eq!(&writer.bytes[13..], text.as_bytes());
            assert_eq!(resources.formats, 1);
            assert!(host.ledger.lock().unwrap().loans.is_empty());
            assert_eq!(control.checks.lock().unwrap().as_slice(), &[(2, 0), (0, 2)]);
        }
        clean(&host);
    }
}

struct Noop;
impl NCodecResources for Noop {
    type Lease = ();
    fn before_decimal256_format(
        &mut self,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn before_decimal256_parse(
        &mut self,
        _: usize,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
}
#[test]
fn original_noop_trace_and_all_eight_codec_tags_remain_identical() {
    let host = Arc::new(Host::default());
    {
        let allocator = allocator(&host);
        let mut no_work = ScalarWork::new(None);
        let utf8 =
            scalar::scalar_bytes(allocator.clone(), "hello🪨".as_bytes(), &mut no_work).unwrap();
        let state = state(
            &allocator,
            vec![
                TrackedAggScalarValue::Bool(true),
                TrackedAggScalarValue::Int64(-9),
                TrackedAggScalarValue::Float64(3.5),
                TrackedAggScalarValue::Utf8(utf8),
                TrackedAggScalarValue::Date32(-2),
                TrackedAggScalarValue::Timestamp(123),
                TrackedAggScalarValue::Decimal128(i128::MIN),
                TrackedAggScalarValue::Decimal256(i256::MIN),
            ],
        );
        let mut expected = Vec::from(17u32.to_le_bytes());
        expected.extend_from_slice(&8u32.to_le_bytes());
        expected.extend_from_slice(&[1, 1, 2]);
        expected.extend_from_slice(&(-9i64).to_le_bytes());
        expected.push(3);
        expected.extend_from_slice(&3.5f64.to_bits().to_le_bytes());
        expected.push(4);
        expected.extend_from_slice(&9u32.to_le_bytes());
        expected.extend_from_slice("hello🪨".as_bytes());
        expected.push(5);
        expected.extend_from_slice(&(-2i32).to_le_bytes());
        expected.push(6);
        expected.extend_from_slice(&123i64.to_le_bytes());
        expected.push(7);
        expected.extend_from_slice(&i128::MIN.to_le_bytes());
        expected.push(11);
        expected.extend_from_slice(&78u32.to_le_bytes());
        expected.extend_from_slice(MIN_TEXT.as_bytes());
        let original = control(&host, None);
        let explicit = control(&host, None);
        let mut bytes = [Writer::default(), Writer::default()];
        for (which, controller) in [&original, &explicit].into_iter().enumerate() {
            let mut checkpoints = EvaluationCheckpoints::new(controller);
            {
                let mut work = ScalarWork::new(Some(&mut checkpoints));
                if which == 0 {
                    state.serialize(&mut bytes[which], &mut work).unwrap();
                } else {
                    state
                        .serialize_with_codec_resources(&mut bytes[which], &mut Noop, &mut work)
                        .unwrap();
                }
            }
            checkpoints.finish().unwrap();
            assert_eq!(bytes[which].bytes, expected);
        }
        assert_eq!(original.checks.lock().unwrap().as_slice(), &[(16, 0)]);
        assert_eq!(
            *original.checks.lock().unwrap(),
            *explicit.checks.lock().unwrap()
        );
        let charge = charge(&host);
        let mut resources = observed(&charge, &allocator, Some(MIN_TEXT));
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![expected.as_slice()]));
        let (_, values) = decode_payload_at_with_codec_resources(
            &array,
            0,
            &allocator,
            &mut resources,
            &mut no_work,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resources.parsed, 1);
        assert_eq!(values.len(), 8);
        let mut decoded = NState::new(allocator.clone());
        decoded.limit = 17;
        decoded.values = values;
        let mut rebuilt = Writer::default();
        decoded
            .serialize_with_codec_resources(&mut rebuilt, &mut resources, &mut no_work)
            .unwrap();
        assert_eq!(rebuilt.bytes, expected);
        assert_eq!(resources.formats, 1);
    }
    clean(&host);
}

#[test]
fn opaque_refusals_preserve_all_typed_causes_and_release_granted_prefixes() {
    for cause in causes() {
        for stop in [0, 1] {
            let host = Arc::new(Host {
                refusal: Some((stop, cause.clone())),
                ..Host::default()
            });
            {
                let allocator = allocator(&host);
                let charge = charge(&host);
                let state = state(
                    &allocator,
                    vec![TrackedAggScalarValue::Decimal256(i256::MIN)],
                );
                let mut resources = observed(&charge, &allocator, None);
                let mut writer = Writer::default();
                let error = state
                    .serialize_with_codec_resources(
                        &mut writer,
                        &mut resources,
                        &mut ScalarWork::new(None),
                    )
                    .unwrap_err();
                assert_eq!(kernel_cause(error), cause);
                assert_eq!(writer.bytes.len(), 9);
                assert_eq!(resources.formats, 0);
                assert_eq!(host.ledger.lock().unwrap().attempts, stop + 1);
            }
            clean(&host);
        }
        let host = Arc::new(Host {
            refusal: Some((0, cause.clone())),
            ..Host::default()
        });
        {
            let allocator = allocator(&host);
            let charge = charge(&host);
            let mut resources = observed(&charge, &allocator, None);
            let error = decode_payload_at_with_codec_resources(
                &payload(MIN_TEXT.as_bytes(), 17),
                0,
                &allocator,
                &mut resources,
                &mut ScalarWork::new(None),
            )
            .unwrap_err();
            assert_eq!(kernel_cause(error), cause);
            assert_eq!(resources.parsed, 0);
            assert_eq!(host.ledger.lock().unwrap().attempts, 1);
        }
        clean(&host);
    }
}

#[test]
fn control_refusals_at_both_call_boundaries_keep_first_cause_without_footer() {
    for cause in causes() {
        for stop in [0, 1] {
            let host = Arc::new(Host::default());
            {
                let allocator = allocator(&host);
                let charge = charge(&host);
                let state = state(
                    &allocator,
                    vec![TrackedAggScalarValue::Decimal256(i256::MIN)],
                );
                let mut resources = observed(&charge, &allocator, None);
                let control = control(&host, Some((stop, cause.clone())));
                let mut checkpoints = EvaluationCheckpoints::new(&control);
                let mut writer = Writer::default();
                let error = state
                    .serialize_with_codec_resources(
                        &mut writer,
                        &mut resources,
                        &mut ScalarWork::new(Some(&mut checkpoints)),
                    )
                    .unwrap_err();
                assert_eq!(kernel_cause(error), cause);
                assert_eq!(writer.bytes.len(), 9);
                assert_eq!(resources.formats, stop);
                assert_eq!(control.checks.lock().unwrap().len(), stop + 1);
            }
            clean(&host);
        }
        // Decoder's original allocation/UTF8 flushes remain before its newly
        // controlled parse scope; discover that prefix with the real call.
        let baseline_host = Arc::new(Host::default());
        let successful_trace = {
            let allocator = allocator(&baseline_host);
            let charge = charge(&baseline_host);
            let control = control(&baseline_host, None);
            let mut checkpoints = EvaluationCheckpoints::new(&control);
            let mut resources = observed(&charge, &allocator, None);
            decode_payload_at_with_codec_resources(
                &payload(MIN_TEXT.as_bytes(), 17),
                0,
                &allocator,
                &mut resources,
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap();
            let trace = control.checks.lock().unwrap().clone();
            trace
        };
        clean(&baseline_host);
        let after = successful_trace
            .iter()
            .position(|(_, loans)| *loans == 1)
            .unwrap();
        for stop in [after - 1, after] {
            let host = Arc::new(Host::default());
            {
                let allocator = allocator(&host);
                let charge = charge(&host);
                let control = control(&host, Some((stop, cause.clone())));
                let mut checkpoints = EvaluationCheckpoints::new(&control);
                let mut resources = observed(&charge, &allocator, None);
                let error = decode_payload_at_with_codec_resources(
                    &payload(MIN_TEXT.as_bytes(), 17),
                    0,
                    &allocator,
                    &mut resources,
                    &mut ScalarWork::new(Some(&mut checkpoints)),
                )
                .unwrap_err();
                assert_eq!(kernel_cause(error), cause);
                assert_eq!(resources.parsed, usize::from(stop == after));
                assert_eq!(
                    control.checks.lock().unwrap().as_slice(),
                    &successful_trace[..=stop]
                );
            }
            clean(&host);
        }
    }
}

#[test]
fn append_failure_destroys_text_under_its_live_loans_and_preserves_cause() {
    for cause in causes() {
        for stop in [2, 3] {
            let host = Arc::new(Host::default());
            {
                let allocator = allocator(&host);
                let charge = charge(&host);
                let state = state(
                    &allocator,
                    vec![TrackedAggScalarValue::Decimal256(i256::MAX)],
                );
                let mut resources = observed(&charge, &allocator, Some(MAX_TEXT));
                let control = control(&host, None);
                let mut checkpoints = EvaluationCheckpoints::new(&control);
                let mut writer = Writer {
                    host: Some(Arc::clone(&host)),
                    refusal: Some((stop, cause.clone())),
                    ..Writer::default()
                };
                let error = state
                    .serialize_with_codec_resources(
                        &mut writer,
                        &mut resources,
                        &mut ScalarWork::new(Some(&mut checkpoints)),
                    )
                    .unwrap_err();
                assert_eq!(kernel_cause(error), cause);
                assert_eq!(resources.formats, 1);
                assert_eq!(writer.appends, stop + 1);
                assert_eq!(writer.protected, stop - 1);
                assert_eq!(control.checks.lock().unwrap().len(), 2);
            }
            clean(&host);
        }
    }
}

#[test]
fn malformed_payload_preserves_original_error_order_and_success_only_footer() {
    for (text, expected, parse_attempt) in [
        (
            b"invalid".as_slice(),
            "min_n/max_n decimal256 parse failed",
            true,
        ),
        (b"".as_slice(), "min_n/max_n decimal256 parse failed", true),
        (
            b"\xff".as_slice(),
            "min_n/max_n decimal256 decode failed: invalid utf-8 sequence of 1 bytes from index 0",
            false,
        ),
    ] {
        let host = Arc::new(Host::default());
        {
            let allocator = allocator(&host);
            let charge = charge(&host);
            let mut resources = observed(&charge, &allocator, None);
            // Original zero-limit handling happens AFTER complete decoding.
            let array = payload(text, 0);
            let original =
                decode_payload_at(&array, 0, &allocator, &mut ScalarWork::new(None)).unwrap_err();
            let result = decode_payload_at_with_codec_resources(
                &array,
                0,
                &allocator,
                &mut resources,
                &mut ScalarWork::new(None),
            )
            .unwrap_err();
            match (original, result) {
                (ScalarStateError::Legacy(old), ScalarStateError::Legacy(new)) => {
                    assert_eq!(old, expected);
                    assert_eq!(new, old);
                }
                other => panic!("expected original Legacy text, got {other:?}"),
            }
            assert_eq!(resources.parsed, 0);
            assert_eq!(
                host.ledger.lock().unwrap().attempts,
                usize::from(parse_attempt)
            );
        }
        clean(&host);
    }
}

#[test]
fn codec_loans_reject_a_foreign_allocator_before_entering_the_formatter() {
    let host = Arc::new(Host::default());
    let foreign = Arc::new(Host::default());
    {
        let allocator = allocator(&host);
        let charge = charge(&foreign);
        let state = state(
            &allocator,
            vec![TrackedAggScalarValue::Decimal256(i256::MIN)],
        );
        let mut resources = observed(&charge, &allocator, None);
        let mut writer = Writer::default();
        let error = state
            .serialize_with_codec_resources(&mut writer, &mut resources, &mut ScalarWork::new(None))
            .unwrap_err();
        assert_eq!(
            kernel_cause(error),
            invalid("min_n/max_n codec reservation uses another host authority")
        );
        assert_eq!(resources.formats, 0);
        assert_eq!(writer.bytes.len(), 9);
        assert_eq!(foreign.ledger.lock().unwrap().attempts, 1);
    }
    clean(&host);
    clean(&foreign);
}

#[test]
fn parse_policy_checks_extent_arithmetic_before_any_host_call() {
    assert_eq!(
        NCodecBudgetPolicyV1::parse_invoice(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    let host = Arc::new(Host::default());
    {
        let allocator = allocator(&host);
        let charge = charge(&host);
        let mut resources = BoundNCodecResources {
            charge: &charge,
            allocator: &allocator,
        };
        let result = resources.before_decimal256_parse(usize::MAX, &mut ScalarWork::new(None));
        assert!(matches!(result, Err(KernelFailure::ResourceExhausted)));
        assert_eq!(host.ledger.lock().unwrap().attempts, 0);
    }
    clean(&host);
}
