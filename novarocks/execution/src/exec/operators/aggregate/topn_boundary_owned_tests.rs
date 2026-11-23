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

//! Owner-local ranking/lifetime protection. The independent oracle sorts ALL
//! declared new-group projections with std byte/NULL order; it does not call
//! the boundary/comparator under test. Group ids are explicit input facts here,
//! not a claim of actual KeyTable or SQL producer installation. Tests own
//! isolated trackers/controls and join the only thread before returning.

use super::*;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime_filter::contribution::{
    OrderedScalarRef, OrderedTupleRef, RuntimeOrderKey, RuntimeOrderNullOrder,
    RuntimeOrderSortDirection,
};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

struct Pass;
impl KernelEvaluationControl for Pass {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("boundary never waits")
    }
}
fn key_value(key: &OwnedOrderKey) -> Option<&str> {
    match key.borrowed() {
        OrderedTupleRef::Single(None) => None,
        OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(s))) => Some(s),
        _ => panic!("owned UTF8 key lost its exact type"),
    }
}
fn contract(
    direction: RuntimeOrderSortDirection,
    nulls: RuntimeOrderNullOrder,
) -> crate::runtime_filter::RuntimeOrderContractRef {
    Arc::new(RuntimeOrderContract::from_frozen(
        [RuntimeOrderKey::with_order(
            DataType::Utf8,
            direction,
            nulls,
        )],
        [2; 32],
        [3; 32],
    ))
    .into()
}
fn oracle_cmp(
    a: Option<&str>,
    b: Option<&str>,
    direction: RuntimeOrderSortDirection,
    nulls: RuntimeOrderNullOrder,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls == RuntimeOrderNullOrder::First {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls == RuntimeOrderNullOrder::First {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            let result = a.as_bytes().cmp(b.as_bytes());
            if direction == RuntimeOrderSortDirection::Ascending {
                result
            } else {
                result.reverse()
            }
        }
    }
}

#[test]
fn ordered_topn_owned_boundary_matches_independent_full_sort_and_legacy_projection() {
    let input = [
        Some("z"),
        None,
        Some(""),
        Some("\u{6d77}\u{6e2f}"),
        Some("a\0b"),
        Some("a"),
        Some("a"),
        Some("b"),
        Some("\u{00e9}"),
        Some("\0"),
    ];
    for direction in [
        RuntimeOrderSortDirection::Ascending,
        RuntimeOrderSortDirection::Descending,
    ] {
        for nulls in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
            let tracker = MemTracker::new_root("ordered-owned-ranking-oracle");
            let host = OrderedTopNAllocationHost::new(tracker.clone());
            let contract = contract(direction, nulls);
            let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
                NonZeroU32::new(3).unwrap(),
                contract.clone(),
                &host,
            )
            .unwrap();
            let mut legacy =
                AggregateTopNBoundaryState::try_new(NonZeroU32::new(3).unwrap(), contract.clone())
                    .unwrap();
            let mut all = Vec::new();
            let mut previous: Option<Option<&str>> = None;
            for (id, value) in input.into_iter().enumerate() {
                state
                    .observe_owned_group(id, host.copy_utf8(value, &Pass).unwrap(), &Pass)
                    .unwrap();
                legacy
                    .observe_new_group(
                        id,
                        OrderedTuple::new([value.map(|s| OrderedScalar::Utf8(s.into()))]),
                    )
                    .unwrap();
                all.push(value);
                all.sort_by(|a, b| oracle_cmp(*a, *b, direction, nulls));
                let snapshot = state.snapshot_owned(&Pass).unwrap();
                if all.len() < 3 {
                    assert!(snapshot.is_none());
                    assert_eq!(state.take_pending_controlled(&Pass).unwrap(), None);
                    assert_eq!(legacy.take_pending_tightening().unwrap(), None);
                    continue;
                }
                let expected = all[2];
                assert_eq!(key_value(snapshot.as_ref().unwrap()), expected);
                let pending = state.take_pending_controlled(&Pass).unwrap();
                let expected_emit = previous.is_none_or(|old| {
                    oracle_cmp(expected, old, direction, nulls) == Ordering::Less
                });
                assert_eq!(pending.is_some(), expected_emit);
                let legacy_pending = legacy.take_pending_tightening().unwrap();
                assert_eq!(pending.is_some(), legacy_pending.is_some());
                if let Some(pending) = pending {
                    assert_eq!(key_value(&pending), expected);
                    previous = Some(expected);
                }
            }
            assert!(state.finish_controlled(&Pass).unwrap().is_none());
            assert!(state.finish_controlled(&Pass).unwrap().is_none());
            drop(state);
            drop(host);
            assert_eq!(tracker.current(), 0);
        }
    }
}

#[test]
fn ordered_topn_owned_pending_key_outlives_eviction_and_state_on_another_thread() {
    let tracker = MemTracker::new_root("ordered-pending-key-eviction");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
        NonZeroU32::new(2).unwrap(),
        contract(
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        ),
        &host,
    )
    .unwrap();
    for (id, value) in ["z", "b"].into_iter().enumerate() {
        state
            .observe_owned_group(id, host.copy_utf8(Some(value), &Pass).unwrap(), &Pass)
            .unwrap();
    }
    let pending = state.take_pending_controlled(&Pass).unwrap().unwrap();
    assert_eq!(key_value(&pending), Some("z"));
    state
        .observe_owned_group(2, host.copy_utf8(Some("a"), &Pass).unwrap(), &Pass)
        .unwrap();
    assert_eq!(
        key_value(&state.snapshot_owned(&Pass).unwrap().unwrap()),
        Some("b")
    );
    drop(state);
    drop(host);
    assert!(
        tracker.current() > 0,
        "evicted pending key still owns its real backing"
    );
    std::thread::spawn(move || {
        assert_eq!(key_value(&pending), Some("z"));
        drop(pending);
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_candidate_slot_refusal_keeps_new_group_uncommitted() {
    let tracker = MemTracker::new_root("ordered-candidate-layout-refusal");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    // Prepare an immutable NULL value without retained payload/metadata, then
    // freeze the real limit before the first candidate container request.
    let null = host.copy_utf8(None, &Pass).unwrap();
    assert_eq!(tracker.current(), 0);
    tracker.install_limit_once(1).unwrap();
    let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
        NonZeroU32::new(1).unwrap(),
        contract(
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        ),
        &host,
    )
    .unwrap();
    let error = state.observe_owned_group(0, null, &Pass).unwrap_err();
    let OwnedTopNBoundaryError::Kernel(cause) = error else {
        panic!("actual candidate allocator refusal: {error:?}");
    };
    crate::runtime_filter::assert_real_vector_refusal(&cause, None);
    assert_eq!(state.observed_group_count, 0);
    assert!(state.snapshot_owned(&Pass).unwrap().is_none());
    drop(state);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_long_comparison_preserves_control_and_original_sequence_errors() {
    struct Refuse {
        units: AtomicUsize,
        cause: KernelFailure,
    }
    impl KernelEvaluationControl for Refuse {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            if self.units.fetch_add(units as usize, AtomicOrdering::SeqCst) + units as usize >= 512
            {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("boundary never waits")
        }
    }
    let tracker = MemTracker::new_root("ordered-boundary-control-cause");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let prefix = "x".repeat(1 << 16);
    let values = [prefix.clone() + "a", prefix + "b"];
    let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
        NonZeroU32::new(2).unwrap(),
        contract(
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        ),
        &host,
    )
    .unwrap();
    for (id, value) in values.iter().enumerate() {
        state
            .observe_owned_group(id, host.copy_utf8(Some(value), &Pass).unwrap(), &Pass)
            .unwrap();
    }
    let held = tracker.current();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = Refuse {
            units: AtomicUsize::new(0),
            cause: cause.clone(),
        };
        assert_eq!(
            state.snapshot_owned(&control),
            Err(OwnedTopNBoundaryError::Kernel(cause))
        );
        // Metadata work is observed too. Refusal may cross the threshold by
        // one public quantum, but must never consume the whole long value.
        assert!(
            (512..512 + novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize)
                .contains(&control.units.load(AtomicOrdering::SeqCst))
        );
        assert_eq!(tracker.current(), held);
    }
    assert_eq!(
        state.observe_owned_group(0, host.copy_utf8(None, &Pass).unwrap(), &Pass),
        Err(OwnedTopNBoundaryError::Boundary(
            AggregateTopNBoundaryError::GroupIdentityOutOfSequence {
                expected: 2,
                actual: 0
            }
        ))
    );
    assert_eq!(state.observed_group_count, 2);
    drop(state);
    drop(host);
    assert_eq!(tracker.current(), 0);
}

// The old owned-key oracle only covered strings. This owner-local matrix uses
// std integer order over literal inputs, across every numeric encoding. It
// also replays the unchanged legacy tuple shell; neither supplies the expected
// nth value. Each isolated tracker is empty after the real state/key drops.
#[test]
fn topn_boundary_owned_numeric_matches_independent_sort_and_legacy() {
    type Make = fn(i128) -> OrderedScalar;
    let mut cases: Vec<(DataType, i128, i128, Make)> = vec![
        (DataType::Int8, i8::MIN.into(), i8::MAX.into(), |v| {
            OrderedScalar::Int8(v as i8)
        }),
        (DataType::Int16, i16::MIN.into(), i16::MAX.into(), |v| {
            OrderedScalar::Int16(v as i16)
        }),
        (DataType::Int32, i32::MIN.into(), i32::MAX.into(), |v| {
            OrderedScalar::Int32(v as i32)
        }),
        (DataType::Int64, i64::MIN.into(), i64::MAX.into(), |v| {
            OrderedScalar::Int64(v as i64)
        }),
        (DataType::Date32, -1000, 1000, |v| {
            OrderedScalar::Date32(v as i32)
        }),
        (
            DataType::Decimal128(18, 3),
            -999999999999999999,
            999999999999999999,
            OrderedScalar::Decimal128,
        ),
        (
            DataType::FixedSizeBinary(16),
            i128::MIN,
            i128::MAX,
            OrderedScalar::LargeInt,
        ),
    ];
    cases.extend(
        [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ]
        .into_iter()
        .map(|unit| {
            (
                DataType::Timestamp(unit, None),
                i64::MIN.into(),
                i64::MAX.into(),
                (|v| OrderedScalar::Timestamp(v as i64)) as Make,
            )
        }),
    );
    for (ty, low, high, make) in cases {
        for direction in [
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderSortDirection::Descending,
        ] {
            for nulls in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
                let tracker = MemTracker::new_root("numeric-owned-boundary-oracle");
                let host = OrderedTopNAllocationHost::new(tracker.clone());
                let order: crate::runtime_filter::RuntimeOrderContractRef =
                    Arc::new(RuntimeOrderContract::from_frozen(
                        [RuntimeOrderKey::with_order(ty.clone(), direction, nulls)],
                        [2; 32],
                        [3; 32],
                    ))
                    .into();
                let mut owned = OwnedAggregateTopNBoundaryState::try_new_owned(
                    NonZeroU32::new(3).unwrap(),
                    order.clone(),
                    &host,
                )
                .unwrap();
                let mut legacy =
                    AggregateTopNBoundaryState::try_new(NonZeroU32::new(3).unwrap(), order)
                        .unwrap();
                let mut all = Vec::new();
                for (id, value) in [Some(high), None, Some(low), Some(0), Some(7), Some(low)]
                    .into_iter()
                    .enumerate()
                {
                    let scalar = value.map(make);
                    owned
                        .observe_owned_group(
                            id,
                            host.copy_scalar(
                                scalar.as_ref().map(OrderedScalar::as_borrowed),
                                &Pass,
                            )
                            .unwrap(),
                            &Pass,
                        )
                        .unwrap();
                    legacy
                        .observe_new_group(id, OrderedTuple::new([scalar]))
                        .unwrap();
                    all.push(value);
                    all.sort_by(|a, b| match (a, b) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => {
                            if nulls == RuntimeOrderNullOrder::First {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (Some(_), None) => {
                            if nulls == RuntimeOrderNullOrder::First {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        (Some(a), Some(b)) => {
                            if direction == RuntimeOrderSortDirection::Ascending {
                                a.cmp(b)
                            } else {
                                b.cmp(a)
                            }
                        }
                    });
                    let snapshot = owned.snapshot_owned(&Pass).unwrap();
                    if id < 2 {
                        assert!(snapshot.is_none());
                        continue;
                    }
                    let expected = all[2].map(make);
                    let OrderedTupleRef::Single(actual) = snapshot.as_ref().unwrap().borrowed()
                    else {
                        panic!("one order key")
                    };
                    assert_eq!(
                        actual,
                        expected.as_ref().map(OrderedScalar::as_borrowed),
                        "{ty:?}"
                    );
                    assert_eq!(
                        legacy.snapshot(),
                        AggregateTopNBoundarySnapshot::Ready {
                            nth_best_inclusive: OrderedTuple::new([expected])
                        }
                    );
                }
                drop(owned);
                drop(legacy);
                drop(host);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}

// Frozen literal Arrow values protect extraction independently of ranking and
// retain the legacy NULL-before-numeric-type-error order. The production pure
// shell and original tuple shell both consume this one borrowed leaf.
#[test]
fn topn_boundary_shared_numeric_projection_keeps_exact_carriers_and_null_order() {
    let largeint = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [
            None,
            Some(i128::MIN.to_be_bytes()),
            Some(i128::MAX.to_be_bytes()),
        ]
        .into_iter(),
        16,
    )
    .unwrap();
    let cases: Vec<(DataType, ArrayRef, Vec<Option<OrderedScalar>>)> = vec![
        (
            DataType::FixedSizeBinary(16),
            Arc::new(largeint),
            vec![
                None,
                Some(OrderedScalar::LargeInt(i128::MIN)),
                Some(OrderedScalar::LargeInt(i128::MAX)),
            ],
        ),
        (
            DataType::Int8,
            Arc::new(Int8Array::from(vec![None, Some(i8::MIN), Some(i8::MAX)])),
            vec![
                None,
                Some(OrderedScalar::Int8(i8::MIN)),
                Some(OrderedScalar::Int8(i8::MAX)),
            ],
        ),
        (
            DataType::Int16,
            Arc::new(Int16Array::from(vec![None, Some(i16::MIN), Some(i16::MAX)])),
            vec![
                None,
                Some(OrderedScalar::Int16(i16::MIN)),
                Some(OrderedScalar::Int16(i16::MAX)),
            ],
        ),
        (
            DataType::Int32,
            Arc::new(Int32Array::from(vec![None, Some(i32::MIN), Some(i32::MAX)])),
            vec![
                None,
                Some(OrderedScalar::Int32(i32::MIN)),
                Some(OrderedScalar::Int32(i32::MAX)),
            ],
        ),
        (
            DataType::Int64,
            Arc::new(Int64Array::from(vec![None, Some(i64::MIN), Some(i64::MAX)])),
            vec![
                None,
                Some(OrderedScalar::Int64(i64::MIN)),
                Some(OrderedScalar::Int64(i64::MAX)),
            ],
        ),
        (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![None, Some(-1), Some(20000)])),
            vec![
                None,
                Some(OrderedScalar::Date32(-1)),
                Some(OrderedScalar::Date32(20000)),
            ],
        ),
        (
            DataType::Decimal128(12, 3),
            Arc::new(
                Decimal128Array::from(vec![None, Some(-1001), Some(999)])
                    .with_precision_and_scale(12, 3)
                    .unwrap(),
            ),
            vec![
                None,
                Some(OrderedScalar::Decimal128(-1001)),
                Some(OrderedScalar::Decimal128(999)),
            ],
        ),
        (
            DataType::Timestamp(TimeUnit::Second, None),
            Arc::new(TimestampSecondArray::from(vec![None, Some(-1), Some(7)])),
            vec![
                None,
                Some(OrderedScalar::Timestamp(-1)),
                Some(OrderedScalar::Timestamp(7)),
            ],
        ),
        (
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::new(TimestampMillisecondArray::from(vec![
                None,
                Some(-1),
                Some(7),
            ])),
            vec![
                None,
                Some(OrderedScalar::Timestamp(-1)),
                Some(OrderedScalar::Timestamp(7)),
            ],
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![
                None,
                Some(-1),
                Some(7),
            ])),
            vec![
                None,
                Some(OrderedScalar::Timestamp(-1)),
                Some(OrderedScalar::Timestamp(7)),
            ],
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            Arc::new(TimestampNanosecondArray::from(vec![
                None,
                Some(-1),
                Some(7),
            ])),
            vec![
                None,
                Some(OrderedScalar::Timestamp(-1)),
                Some(OrderedScalar::Timestamp(7)),
            ],
        ),
    ];
    for (ty, array, expected) in cases {
        let order = RuntimeOrderContract::from_frozen([RuntimeOrderKey::new(ty)], [2; 32], [3; 32]);
        for (row, value) in expected.into_iter().enumerate() {
            assert_eq!(
                borrowed_key_from_group_array(&order, &array, row).unwrap(),
                value.as_ref().map(OrderedScalar::as_borrowed)
            );
            assert_eq!(
                tuple_from_group_array(&order, &array, row).unwrap(),
                OrderedTuple::new([value])
            );
        }
        assert_eq!(
            borrowed_key_from_group_array(&order, &array, array.len()),
            Err(AggregateTopNBoundaryError::CandidateRowOutOfBounds {
                row: array.len(),
                len: array.len()
            })
        );
    }
    let order = RuntimeOrderContract::from_frozen(
        [RuntimeOrderKey::new(DataType::Int64)],
        [2; 32],
        [3; 32],
    );
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1)]));
    assert_eq!(
        tuple_from_group_array(&order, &wrong, 0).unwrap(),
        OrderedTuple::new([None])
    );
    assert_eq!(
        tuple_from_group_array(&order, &wrong, 1),
        Err(AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
            expected: novarocks_failure_contract::BoundedDetail::truncating_display("Int64"),
            actual: novarocks_failure_contract::BoundedDetail::truncating_display("Int32")
        })
    );
}

// Inline keys still require real paid candidate storage. A positive budget
// smaller than one candidate must refuse the first insertion and keep the
// original group sequence unchanged, without a dummy payload reservation.
#[test]
fn topn_boundary_owned_numeric_candidate_refuses_actual_container_growth() {
    let tracker = MemTracker::new_root("numeric-boundary-candidate-budget");
    tracker.install_limit_once(1).unwrap();
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let order = Arc::new(RuntimeOrderContract::from_frozen(
        [RuntimeOrderKey::new(DataType::Int64)],
        [2; 32],
        [3; 32],
    ))
    .into();
    let mut state =
        OwnedAggregateTopNBoundaryState::try_new_owned(NonZeroU32::new(1).unwrap(), order, &host)
            .unwrap();
    let key = host
        .copy_scalar(Some(OrderedScalarRef::Int64(7)), &Pass)
        .unwrap();
    assert_eq!(tracker.current(), 0);
    let error = state.observe_owned_group(0, key, &Pass).unwrap_err();
    let OwnedTopNBoundaryError::Kernel(cause) = error else {
        panic!("actual candidate allocator refusal: {error:?}");
    };
    crate::runtime_filter::assert_real_vector_refusal(&cause, None);
    assert_eq!(state.observed_group_count, 0);
    assert_eq!(tracker.current(), 0);
    drop(state);
    drop(host);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn owned_topn_admits_zoned_keys_and_encodes_the_exact_instant_bound() {
    use crate::exec::hash_table::key_index::{KeyReadRow, KeyReadValues};
    use crate::runtime_filter::contribution::{
        ContributionCodecExpectation, RuntimeFilterContribution, decode_contribution,
    };
    use arrow::array::FixedSizeBinaryBuilder;
    use novarocks_type_contract::{
        FunctionValueType, TemporalCarrier, TemporalMicros, TemporalPrecision, TemporalType,
        TemporalValue, TimeZoneKey, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };

    let temporal = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let carrier = temporal.carrier();
    let storage = ValueTypeStorageLifetime::pure();
    let ty = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(temporal), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&carrier, storage).unwrap(),
    )
    .unwrap();
    let ascending = RuntimeOrderSortDirection::Ascending;
    let nulls_last = RuntimeOrderNullOrder::Last;
    let order = Arc::new(RuntimeOrderContract::from_frozen(
        [RuntimeOrderKey::with_value_type(
            ty.clone(),
            ascending,
            nulls_last,
        )],
        [2; 32],
        [3; 32],
    ));
    let tracker = MemTracker::new_root("zoned-ordered-boundary");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let limit = NonZeroU32::new(1).unwrap();

    for raw_key in [
        RuntimeOrderKey::new(carrier.clone()),
        RuntimeOrderKey::with_value_type(
            FunctionValueType::new(carrier, false),
            ascending,
            nulls_last,
        ),
    ] {
        let raw_contract = Arc::new(RuntimeOrderContract::from_frozen(
            [raw_key],
            [2; 32],
            [3; 32],
        ));
        assert!(matches!(
            OwnedAggregateTopNBoundaryState::try_new_owned(limit, raw_contract.into(), &host),
            Err(OwnedTopNBoundaryError::Boundary(
                AggregateTopNBoundaryError::UnsupportedOrderKeyType { .. }
            ))
        ));
    }

    let mut state =
        OwnedAggregateTopNBoundaryState::try_new_owned(limit, order.clone().into(), &host).unwrap();
    let late = TemporalMicros::new(1, 2).unwrap();
    let early = TemporalMicros::new(1, 1).unwrap();
    let shanghai = TimeZoneKey::parse("Asia/Shanghai").unwrap();
    let utc = TimeZoneKey::UTC;
    let mut array = FixedSizeBinaryBuilder::new(14);
    for (instant, zone) in [(late, utc), (early, shanghai), (late, shanghai)] {
        let value = TemporalValue::TimestampWithTimeZone { instant, zone };
        let TemporalCarrier::ZonedTimestamp(bytes) = value.to_carrier(temporal).unwrap() else {
            panic!("typed zoned timestamp must use its declared 14-byte carrier");
        };
        array.append_value(bytes).unwrap();
    }
    let array: ArrayRef = Arc::new(array.finish());
    let mut projected = Vec::new();
    let rows = [0, 1, 2].map(|row| KeyReadRow { ordinal: row, row });
    let page = KeyReadValues::Fixed {
        array: &array,
        binding: &ty,
    };
    visit_key_read_page(&order, &page, &rows, &Pass, &mut |group, scalar| {
        projected.push(scalar);
        state.observe_owned_group(group, host.copy_scalar(scalar, &Pass)?, &Pass)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(projected[0], projected[2]);
    assert_ne!(projected[0], projected[1]);

    let bound = state.snapshot_owned(&Pass).unwrap().unwrap();
    let expected_bytes = TemporalValue::TimestampWithTimeZone {
        instant: early,
        zone: utc,
    }
    .relation_key(temporal)
    .unwrap()
    .order_bytes();
    let expected = OrderedTuple::new([Some(OrderedScalar::Temporal(expected_bytes))]);
    let bytes = host
        .encode_ordered_bound(bound.borrowed(), &order, 1024, &Pass)
        .unwrap();
    let decoded = decode_contribution(
        bytes.as_slice(),
        &order.digest(),
        ContributionCodecExpectation::OrderedBound(&order),
        1024,
    )
    .unwrap();
    let RuntimeFilterContribution::OrderedBound(update) = decoded else {
        panic!("the original ordered contribution decoder must return a bound");
    };
    assert_eq!(update.bound(), &expected);
    drop(bytes);
    drop(bound);
    drop(state);
    assert_eq!(tracker.current(), 0);
}
