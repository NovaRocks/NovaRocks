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

//! Short owner-local hard-limit/last-alias tests, not performance acceptance.
//! Oracles are independent bytes/NULL values, the original canonical decoder
//! and actual isolated production MemTracker balances after real Drop. Public
//! control verdicts exercise unpublished-prefix rollback without a product hook.

use super::*;
use crate::runtime_filter::contribution::{
    ContributionCodecError, ContributionCodecExpectation, OrderedBoundUpdate, OrderedScalar,
    OrderedTuple, RuntimeFilterContribution as CodecContribution, RuntimeOrderKey,
    decode_contribution, encode_contribution,
};
use arrow::datatypes::DataType;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct Control {
    calls: AtomicUsize,
    units: AtomicUsize,
    max_units: AtomicUsize,
    refuse_at: Option<usize>,
    cause: KernelFailure,
}
impl Control {
    fn pass() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            units: AtomicUsize::new(0),
            max_units: AtomicUsize::new(0),
            refuse_at: None,
            cause: KernelFailure::Cancelled,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.units.fetch_add(units as usize, Ordering::SeqCst);
        self.max_units.fetch_max(units as usize, Ordering::SeqCst);
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.refuse_at == Some(ordinal) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ordered storage never waits")
    }
}

#[test]
fn ordered_topn_owned_utf8_preserves_null_empty_and_last_shared_key() {
    let tracker = MemTracker::new_root("ordered-key-last-alias");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let control = Control::pass();
    let null = host.copy_utf8(None, &control).unwrap();
    assert!(matches!(null.borrowed(), OrderedTupleRef::Single(None)));
    assert_eq!(tracker.current(), 0);
    let empty = host.copy_utf8(Some(""), &control).unwrap();
    assert!(matches!(
        empty.borrowed(),
        OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8("")))
    ));
    assert!(
        tracker.current() > 0,
        "empty payload still has paid typed metadata"
    );
    drop(empty);
    assert_eq!(tracker.current(), 0);
    let text = "\u{6d77}\0\u{6e2f}".repeat(1024);
    let key = host.copy_utf8(Some(&text), &control).unwrap();
    let held = tracker.current();
    assert!(held >= text.len() as i64);
    let last = key.clone();
    assert_eq!(
        tracker.current(),
        held,
        "whole sharing requests no payload or holder"
    );
    drop(key);
    drop(host);
    assert_eq!(tracker.current(), held);
    std::thread::spawn(move || {
        let OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(value))) = last.borrowed() else {
            panic!("UTF8 key lost")
        };
        assert_eq!(value, text);
        drop(last);
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
    assert!(control.max_units.load(Ordering::SeqCst) <= MAX_UNOBSERVED_KERNEL_WORK as usize);
}

#[test]
fn ordered_topn_owned_canonical_bytes_use_original_codec_and_whole_aliases() {
    let tracker = MemTracker::new_root("ordered-canonical-last-alias");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [9; 32]);
    for value in [None, Some(""), Some("\0"), Some("\u{6d77}\u{6e2f}")] {
        let tuple = OrderedTuple::new([value.map(|s| OrderedScalar::Utf8(s.into()))]);
        let expected =
            CodecContribution::ordered_bound(OrderedBoundUpdate::new(contract.digest(), tuple));
        let original = encode_contribution(
            &expected,
            ContributionCodecExpectation::OrderedBound(&contract),
            1024,
        )
        .unwrap();
        let bytes = host
            .encode_ordered_bound(
                OrderedTupleRef::Single(value.map(OrderedScalarRef::Utf8)),
                &contract,
                1024,
                &Control::pass(),
            )
            .unwrap();
        assert_eq!(bytes.as_slice(), original.payload());
        assert_eq!(
            decode_contribution(
                &bytes,
                &contract.digest(),
                ContributionCodecExpectation::OrderedBound(&contract),
                1024
            )
            .unwrap(),
            expected
        );
        let held = tracker.current();
        let ingress = bytes.clone_for_local_ingress();
        assert_eq!(bytes.as_ptr(), ingress.as_ptr());
        assert_eq!(tracker.current(), held);
        drop(bytes);
        assert_eq!(tracker.current(), held);
        std::thread::spawn(move || drop(ingress)).join().unwrap();
        assert_eq!(tracker.current(), 0);
    }
    let legacy = CanonicalContributionBytes::from(vec![1, 2, 3]);
    let ingress = legacy.clone_for_local_ingress();
    assert_eq!(legacy, ingress);
    assert_ne!(
        legacy.as_ptr(),
        ingress.as_ptr(),
        "legacy ingress retains its original copy"
    );
}

#[test]
fn ordered_topn_owned_contribution_clone_keeps_the_original_paid_payload() {
    let tracker = MemTracker::new_root("ordered-contribution-shared-payload");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [3; 32]);
    let value = "x".repeat(4096);
    let contribution = host
        .ordered_contribution(
            OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(&value))),
            &contract,
            8192,
            &Control::pass(),
        )
        .unwrap();
    assert_eq!(
        contribution.kind(),
        super::super::RuntimeFilterContributionKind::OrderedBound
    );
    assert_eq!(contribution.contract_digest(), contract.digest());
    let held = tracker.current();
    let local = contribution.clone();
    let outbound = contribution.canonical_bytes().clone();
    assert_eq!(
        contribution.canonical_bytes().as_ptr(),
        local.canonical_bytes().as_ptr()
    );
    assert_eq!(tracker.current(), held);
    let legacy = super::super::RuntimeFilterContribution::new(
        contribution.kind(),
        contribution.contract_digest(),
        Arc::<[u8]>::from(contribution.canonical_bytes().as_slice()),
    );
    assert_eq!(contribution, legacy);
    assert_eq!(format!("{contribution:?}"), format!("{legacy:?}"));
    drop(contribution);
    drop(local);
    drop(host);
    assert_eq!(tracker.current(), held);
    std::thread::spawn(move || drop(outbound)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_real_limits_refuse_prefix_vector_and_late_holder_without_stock() {
    let text = "x".repeat(4096);
    let prefix = source_custody_prefix_facts().requests().bytes();
    for limit in [1, prefix, prefix + text.len()] {
        let tracker = MemTracker::new_root("ordered-storage-hard-refusal");
        tracker.install_limit_once(limit as i64).unwrap();
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let control = Control::pass();
        let refusal = host.copy_utf8(Some(&text), &control).err().unwrap();
        if limit == prefix {
            crate::runtime_filter::assert_real_vector_refusal(&refusal, Some(text.len() as u64));
        } else {
            crate::runtime_filter::assert_real_tracker_capacity(&refusal, limit as i64);
        }
        assert_eq!(tracker.current(), 0);
        if limit == 1 {
            assert_eq!(control.calls.load(Ordering::SeqCst), 0);
        }
        if limit == prefix + text.len() {
            assert!(
                control.units.load(Ordering::SeqCst) >= text.len(),
                "holder refusal follows the actual payload copy"
            );
        }
    }
}

#[test]
fn ordered_topn_owned_encoder_and_null_refuse_prefix_before_the_first_callback() {
    let tracker = MemTracker::new_root("ordered-prefix-first-callback");
    tracker.install_limit_once(1).unwrap();
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [5; 32]);
    let control = Control::pass();
    let refusal = host
        .encode_ordered_bound(
            OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8("value"))),
            &contract,
            1024,
            &control,
        )
        .err()
        .unwrap();
    let ControlledContributionCodecError::Kernel(refusal) = refusal else {
        panic!("the actual prefix host must refuse before the codec");
    };
    crate::runtime_filter::assert_real_tracker_capacity(&refusal, 1);
    assert_eq!(control.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tracker.current(), 0);
    let refusal = host.copy_utf8(None, &control).err().unwrap();
    crate::runtime_filter::assert_real_tracker_capacity(&refusal, 1);
    assert_eq!(control.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tracker.current(), 0);

    // A distinct real admission reaches the vector after its prefix. The
    // standard container preserves its Layout, but erases the quota cause.
    let tracker = MemTracker::new_root("ordered-encoder-real-vector-refusal");
    tracker
        .install_limit_once(source_custody_prefix_facts().requests().bytes() as i64)
        .unwrap();
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let control = Control::pass();
    let refusal = host
        .encode_ordered_bound(
            OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8("value"))),
            &contract,
            1024,
            &control,
        )
        .err()
        .unwrap();
    let ControlledContributionCodecError::Kernel(refusal) = refusal else {
        panic!("the actual encoded vector must refuse before publication");
    };
    crate::runtime_filter::assert_real_vector_refusal(&refusal, None);
    assert!(control.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_control_refusal_frees_unpublished_key_and_canonical_prefix() {
    let text = "x".repeat(4096);
    let tracker = MemTracker::new_root("ordered-storage-control-refusal");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [7; 32]);
    let tuple = OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(&text)));
    let successful = Control::pass();
    drop(
        host.encode_ordered_bound(tuple, &contract, usize::MAX, &successful)
            .unwrap(),
    );
    let last = successful.calls.load(Ordering::SeqCst) - 1;
    assert_eq!(tracker.current(), 0);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = Control {
            refuse_at: Some(last),
            cause: cause.clone(),
            ..Control::pass()
        };
        assert!(
            matches!(host.encode_ordered_bound(tuple, &contract, usize::MAX, &control), Err(ControlledContributionCodecError::Kernel(e)) if e == cause)
        );
        assert_eq!(
            tracker.current(),
            0,
            "late refusal destroys the complete unpublished owner"
        );
        let control = Control {
            refuse_at: Some(4),
            cause: cause.clone(),
            ..Control::pass()
        };
        assert!(matches!(host.copy_utf8(Some(&text), &control), Err(e) if e == cause));
        assert_eq!(tracker.current(), 0);
    }
    assert!(matches!(
        host.encode_ordered_bound(tuple, &contract, 1, &Control::pass()),
        Err(ControlledContributionCodecError::Codec(
            ContributionCodecError::EncodedSizeExceeded
        ))
    ));
    assert_eq!(tracker.current(), 0);
}

fn static_utf8_order(
    direction: novarocks_local_program::FilterSortDirection,
    null_order: novarocks_local_program::FilterNullOrder,
) -> novarocks_local_program::StaticFilterContract {
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection};
    // Frozen v1 canonical bytes: count1, Utf8 tag9, direction/null position;
    // SHA256 domain/version, inclusive1 and comparator digest. The original
    // strict fragment-contract author verifies each literal below.
    let (comparator, order) = match (direction, null_order) {
        (FilterSortDirection::Ascending, FilterNullOrder::First) => (
            "77782fddf467caee0ce29343ed593a1acb8cf89a46ef1f6765730c4bdbabc51d",
            "5137b9bc01ebb44e0f02e78917ef2e76a655d5320b360f7ec68ff2566970ca62",
        ),
        (FilterSortDirection::Ascending, FilterNullOrder::Last) => (
            "c712d2a8677a0968f10c6e39490fcd77ab08fcd3ee9de29bb1df944a16223bae",
            "a26b9e26239e0842aa3fd6016bcaf0d41d0f69d17b6a078ea57633f17e1eba26",
        ),
        (FilterSortDirection::Descending, FilterNullOrder::First) => (
            "e75b9b677a5a21c827d830a9a56dc38d389f51314de51dc18cb6ce7dd186911f",
            "63fd0c611009b809d3e952d66ce73e4a9b966b7f68b395eae784f35ca8331627",
        ),
        (FilterSortDirection::Descending, FilterNullOrder::Last) => (
            "ea4e538a3a740e5c6813c79021f0b816aacd03a060a9741902504e4034bdcaae",
            "2efde53cbc305da924d5b722ff253591fa2d9dfe6c137f319531b0108af51d6f",
        ),
    };
    let comparator_digest: [u8; 32] = hex::decode(comparator).unwrap().try_into().unwrap();
    let contract_digest: [u8; 32] = hex::decode(order).unwrap().try_into().unwrap();
    novarocks_local_program::StaticFilterContract::Ordered {
        keys: novarocks_local_program::FilterOrderKeys::from_legacy(Arc::from([
            novarocks_local_program::FilterOrderKey {
                value_type: novarocks_functions::FunctionValueType::new(DataType::Utf8, true),
                direction,
                null_order,
            },
        ])),
        comparator_digest,
        contract_digest,
    }
}

#[test]
fn ordered_topn_owned_frozen_key_slots_share_last_owner_and_preserve_legacy_content() {
    use crate::runtime_filter::contribution::{RuntimeOrderNullOrder, RuntimeOrderSortDirection};
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection};
    for (direction, expected_direction) in [
        (
            FilterSortDirection::Ascending,
            RuntimeOrderSortDirection::Ascending,
        ),
        (
            FilterSortDirection::Descending,
            RuntimeOrderSortDirection::Descending,
        ),
    ] {
        for (nulls, expected_nulls) in [
            (FilterNullOrder::First, RuntimeOrderNullOrder::First),
            (FilterNullOrder::Last, RuntimeOrderNullOrder::Last),
        ] {
            let tracker = MemTracker::new_root("ordered-frozen-keys-last-alias");
            let host = OrderedTopNAllocationHost::new(tracker.clone());
            let frozen = static_utf8_order(direction, nulls);
            let novarocks_local_program::StaticFilterContract::Ordered {
                comparator_digest,
                contract_digest,
                ..
            } = &frozen
            else {
                unreachable!()
            };
            let (comparator_digest, contract_digest) = (*comparator_digest, *contract_digest);
            let contract = host.order_contract(&frozen, &Control::pass()).unwrap();
            let expected = RuntimeOrderContract::from_frozen(
                [RuntimeOrderKey::with_value_type(
                    novarocks_functions::FunctionValueType::new(DataType::Utf8, true),
                    expected_direction,
                    expected_nulls,
                )],
                comparator_digest,
                contract_digest,
            );
            assert_eq!(contract, expected);
            assert_eq!(format!("{contract:?}"), format!("{expected:?}"));
            let held = tracker.current();
            assert!(held > std::mem::size_of::<RuntimeOrderKey>() as i64);
            let last = contract.clone();
            assert_eq!(contract.keys().as_ptr(), last.keys().as_ptr());
            assert_eq!(tracker.current(), held);
            let legacy_copy = expected.clone();
            assert_ne!(expected.keys().as_ptr(), legacy_copy.keys().as_ptr());
            let tuple = OrderedTuple::new([Some(OrderedScalar::Utf8("\u{6d77}\0".into()))]);
            let bound =
                CodecContribution::ordered_bound(OrderedBoundUpdate::new(contract_digest, tuple));
            let legacy_bytes = encode_contribution(
                &bound,
                ContributionCodecExpectation::OrderedBound(&expected),
                1024,
            )
            .unwrap();
            let owned_bytes = encode_contribution(
                &bound,
                ContributionCodecExpectation::OrderedBound(&contract),
                1024,
            )
            .unwrap();
            assert_eq!(owned_bytes.payload(), legacy_bytes.payload());
            drop(contract);
            drop(host);
            assert_eq!(tracker.current(), held);
            std::thread::spawn(move || {
                assert_eq!(last, expected);
                assert_eq!(last.comparator_digest(), comparator_digest);
                assert_eq!(last.digest(), contract_digest);
                drop(last);
            })
            .join()
            .unwrap();
            assert_eq!(tracker.current(), 0);
        }
    }
}

// Owner-local regression: an exact frozen key keeps its semantic binding,
// including nullability, while its slots share the original paid backing.
#[test]
fn ordered_topn_owned_frozen_keys_preserve_complete_nullability() {
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection, StaticFilterContract};
    for nullable in [false, true] {
        let value_type = novarocks_functions::FunctionValueType::new(DataType::Utf8, nullable);
        let mut frozen = static_utf8_order(FilterSortDirection::Descending, FilterNullOrder::Last);
        let StaticFilterContract::Ordered { keys, .. } = &mut frozen else {
            unreachable!()
        };
        let mut replacement = keys.as_slice().to_vec();
        replacement[0].value_type = value_type.clone();
        *keys = replacement.into();
        let tracker = MemTracker::new_root("ordered-frozen-complete-binding");
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let order = host.order_contract(&frozen, &Control::pass()).unwrap();
        assert_eq!(order.keys()[0].value_type(), Some(&value_type));
        assert_eq!(order.keys()[0].value_type().unwrap().nullable(), nullable);
        let shared = order.clone();
        drop(order);
        drop(host);
        assert!(tracker.current() > 0);
        assert_eq!(shared.keys()[0].value_type(), Some(&value_type));
        drop(shared);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn ordered_topn_owned_frozen_key_slots_refuse_real_prefix_vector_and_holder_limits() {
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection};
    let contract = static_utf8_order(FilterSortDirection::Ascending, FilterNullOrder::First);
    let prefix = source_custody_prefix_facts().requests().bytes();
    for limit in [1, prefix, prefix + std::mem::size_of::<RuntimeOrderKey>()] {
        let tracker = MemTracker::new_root("ordered-frozen-keys-hard-limit");
        tracker.install_limit_once(limit as i64).unwrap();
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let control = Control::pass();
        let refusal = host.order_contract(&contract, &control).unwrap_err();
        if limit == prefix {
            crate::runtime_filter::assert_real_vector_refusal(
                &refusal,
                Some(std::mem::size_of::<RuntimeOrderKey>() as u64),
            );
        } else {
            crate::runtime_filter::assert_real_tracker_capacity(&refusal, limit as i64);
        }
        assert_eq!(tracker.current(), 0);
        if limit == 1 {
            assert_eq!(control.calls.load(Ordering::SeqCst), 0);
        } else if limit > prefix {
            assert!(
                control.calls.load(Ordering::SeqCst) > 1,
                "holder refusal follows actual slot allocation"
            );
        }
    }
}

#[test]
fn ordered_topn_owned_frozen_key_slots_preserve_control_causes_and_named_uninstalled_shapes() {
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection, StaticFilterContract};
    let tracker = MemTracker::new_root("ordered-frozen-keys-control");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = static_utf8_order(FilterSortDirection::Descending, FilterNullOrder::Last);
    let control = Control::pass();
    drop(host.order_contract(&contract, &control).unwrap());
    assert_eq!(tracker.current(), 0);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for ordinal in 0..control.calls.load(Ordering::SeqCst) {
            let refusal = Control {
                refuse_at: Some(ordinal),
                cause: cause.clone(),
                ..Control::pass()
            };
            assert!(
                matches!(host.order_contract(&contract, &refusal), Err(e) if e.cause() == &crate::runtime::task::ExecutionFailureCause::Kernel(cause.clone()))
            );
            assert_eq!(
                tracker.current(),
                0,
                "unpublished slots and holder are actually freed"
            );
        }
    }
    let StaticFilterContract::Ordered { keys, .. } = contract else {
        unreachable!()
    };
    let wrong_type = StaticFilterContract::Ordered {
        keys: novarocks_local_program::FilterOrderKeys::from_legacy(Arc::from([
            novarocks_local_program::FilterOrderKey {
                value_type: novarocks_functions::FunctionValueType::new(DataType::Boolean, true),
                ..keys[0].clone()
            },
        ])),
        comparator_digest: [17; 32],
        contract_digest: [23; 32],
    };
    let wrong_arity = StaticFilterContract::Ordered {
        keys: novarocks_local_program::FilterOrderKeys::from_legacy(Arc::from([
            keys[0].clone(),
            keys[0].clone(),
        ])),
        comparator_digest: [17; 32],
        contract_digest: [23; 32],
    };
    let membership = StaticFilterContract::membership(
        &novarocks_functions::FunctionValueType::new(DataType::Utf8, true),
        novarocks_local_program::FilterNullSemantics::NeverMatches,
    )
    .unwrap();
    for (shape, detail) in [
        (
            wrong_type,
            "pure Aggregate TopN runtime filter order key type is not installed",
        ),
        (
            wrong_arity,
            "pure Aggregate TopN runtime filter requires one complete order key",
        ),
        (
            membership,
            "pure Aggregate TopN runtime filter requires an ordered contract",
        ),
    ] {
        let control = Control::pass();
        assert!(
            matches!(host.order_contract(&shape, &control), Err(e) if matches!(e.cause(), crate::runtime::task::ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(d)) if d.message() == detail))
        );
        assert_eq!(control.calls.load(Ordering::SeqCst), 0);
        assert_eq!(tracker.current(), 0);
    }
}

// Original strict constructor and literal wire contract are independent of
// the new paid-slot entry. Corrupting either digest must never publish slots.
#[test]
fn ordered_topn_owned_frozen_contract_rejects_changed_digests_with_original_typed_cause() {
    use crate::runtime::task::ExecutionFailureCause;

    use novarocks_local_program::{FilterNullOrder, FilterSortDirection, StaticFilterContract};
    for direction in [
        FilterSortDirection::Ascending,
        FilterSortDirection::Descending,
    ] {
        for nulls in [FilterNullOrder::First, FilterNullOrder::Last] {
            let frozen = static_utf8_order(direction, nulls);
            let StaticFilterContract::Ordered {
                keys,
                comparator_digest,
                contract_digest,
            } = &frozen
            else {
                unreachable!()
            };
            let original_keys = [RuntimeOrderKey::with_value_type(
                keys[0].value_type.clone(),
                match direction {
                    FilterSortDirection::Ascending => RuntimeOrderSortDirection::Ascending,
                    FilterSortDirection::Descending => RuntimeOrderSortDirection::Descending,
                },
                match nulls {
                    FilterNullOrder::First => RuntimeOrderNullOrder::First,
                    FilterNullOrder::Last => RuntimeOrderNullOrder::Last,
                },
            )];
            RuntimeOrderContract::from_fragment_contract(
                original_keys.clone(),
                *comparator_digest,
                *contract_digest,
            )
            .unwrap();
            for comparator in [true, false] {
                let mut changed = frozen.clone();
                let StaticFilterContract::Ordered {
                    comparator_digest,
                    contract_digest,
                    ..
                } = &mut changed
                else {
                    unreachable!()
                };
                if comparator {
                    comparator_digest[0] ^= 1;
                } else {
                    contract_digest[0] ^= 1;
                }
                let expected = RuntimeOrderContract::from_fragment_contract(
                    original_keys.clone(),
                    *comparator_digest,
                    *contract_digest,
                )
                .unwrap_err();
                let tracker = MemTracker::new_root("ordered-frozen-digest-refusal");
                let host = OrderedTopNAllocationHost::new(tracker.clone());
                let actual = host.order_contract(&changed, &Control::pass()).unwrap_err();
                assert_eq!(
                    actual.cause(),
                    &ExecutionFailureCause::RuntimeFilter(expected.clone())
                );
                assert_eq!(actual.to_string(), expected.to_string());
                assert_eq!(actual.site(), None);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}

// Numeric payloads have no backing allocation. The real candidate Vec pays
// their complete Layout later; a zero-byte pseudo grant would hide this fact.
#[test]
fn ordered_topn_owned_numeric_inline_copy_has_no_payload_grant_and_keeps_first_cause() {
    let tracker = MemTracker::new_root("inline-order-key");
    tracker.install_limit_once(1).unwrap();
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let key = host
        .copy_scalar(
            Some(OrderedScalarRef::LargeInt(i128::MIN)),
            &Control::pass(),
        )
        .unwrap();
    assert!(matches!(
        key.borrowed(),
        OrderedTupleRef::Single(Some(OrderedScalarRef::LargeInt(i128::MIN)))
    ));
    assert_eq!(tracker.current(), 0);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = Control {
            refuse_at: Some(0),
            cause: cause.clone(),
            ..Control::pass()
        };
        assert_eq!(
            host.copy_scalar(Some(OrderedScalarRef::Int64(7)), &control),
            Err(cause)
        );
        assert_eq!(tracker.current(), 0);
    }
    drop(key);
    drop(host);
    assert_eq!(tracker.current(), 0);
}

// The original strict constructor supplies an independent binding/codec
// oracle for the paid adapter. These literal complete declarations protect
// precision, scale, unit and nominal identity, not merely the Arrow width.
#[test]
fn ordered_topn_owned_numeric_contract_retains_complete_binding_and_last_alias() {
    use novarocks_functions::{FunctionValueType as V, ValueLogicalType};
    use novarocks_local_program::{
        FilterNullOrder, FilterOrderKey, FilterOrderKeys, FilterSortDirection, StaticFilterContract,
    };
    let mut types = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Decimal128(28, 7),
    ]
    .into_iter()
    .map(|ty| V::new(ty, true))
    .collect::<Vec<_>>();
    for unit in [
        arrow::datatypes::TimeUnit::Second,
        arrow::datatypes::TimeUnit::Millisecond,
        arrow::datatypes::TimeUnit::Microsecond,
        arrow::datatypes::TimeUnit::Nanosecond,
    ] {
        types.push(V::new(DataType::Timestamp(unit, None), false));
    }
    types.push(
        V::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    for ty in types {
        // Existing canonical author creates an input; the old strict runtime
        // author independently validates it before the paid adapter is called.
        let (comparator_digest, contract_digest) =
            novarocks_local_program::single_key_ordered_filter_digests_observed(
                FilterSortDirection::Descending,
                FilterNullOrder::First,
                |emit| novarocks_local_program::visit_filter_key_type(ty.data_type(), emit),
                || Ok::<_, novarocks_local_program::FilterMembershipSchemaError>(()),
            )
            .unwrap();
        let expected = RuntimeOrderContract::from_fragment_contract(
            [RuntimeOrderKey::with_value_type(
                ty.clone(),
                RuntimeOrderSortDirection::Descending,
                RuntimeOrderNullOrder::First,
            )],
            comparator_digest,
            contract_digest,
        )
        .unwrap();
        let frozen = StaticFilterContract::Ordered {
            keys: FilterOrderKeys::from_legacy(Arc::from([FilterOrderKey {
                value_type: ty.clone(),
                direction: FilterSortDirection::Descending,
                null_order: FilterNullOrder::First,
            }])),
            comparator_digest,
            contract_digest,
        };
        let tracker = MemTracker::new_root("numeric-order-contract-custody");
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let actual = host
            .shared_order_contract(&frozen, &Control::pass())
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
        assert_eq!(actual.keys()[0].value_type(), Some(&ty));
        let last = actual.clone();
        assert_eq!(actual.keys().as_ptr(), last.keys().as_ptr());
        let held = tracker.current();
        assert!(held > 0);
        drop(actual);
        drop(host);
        assert_eq!(tracker.current(), held);
        std::thread::spawn(move || {
            assert_eq!(last.as_ref(), &expected);
            assert_eq!(last.keys()[0].value_type(), Some(&ty));
            drop(last);
        })
        .join()
        .unwrap();
        assert_eq!(tracker.current(), 0);
    }
}
