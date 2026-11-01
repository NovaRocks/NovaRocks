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

use super::super::statistics::BuildRangeStats;
use super::*;

fn prepare<'a>(
    spec: &KeySpec,
    keys: &'a KeyInput,
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> PreparedKeyBatch<'a> {
    PreparedKeyBatch::new(
        spec,
        keys,
        AggregateAllocator::new(tracker.clone()),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}
#[test]
fn exact_integer_range_statistics_preserve_occurrences_null_policy_and_widened_span_without_hashes()
{
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int64, true);
    let keys = input(
        &[Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(i64::MAX),
            None,
            Some(-7),
        ]))],
        std::slice::from_ref(&ty),
        &[3, 1, 3, 0, 2],
        owner.as_ref(),
    );
    for nulls in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let spec = KeySpec::new(std::slice::from_ref(&ty), &[nulls], owner.as_ref(), &|| {
            Ok(())
        })
        .unwrap();
        let page = prepare(&spec, &keys, owner.as_ref(), &tracker);
        let whole = BuildRangeStats::collect_integer(&page, 0..5, &|| Ok(())).unwrap();
        assert_eq!(
            (
                whole.input_rows(),
                whole.indexable_rows(),
                whole.null_rows()
            ),
            (5, if nulls == NullPolicy::Equal { 5 } else { 4 }, 1)
        );
        assert_eq!(whole.integer().unwrap().min_max, Some((i64::MIN, i64::MAX)));
        assert_eq!(whole.integer().unwrap().span(), Some(1u128 << 64));
        let mut parts = BuildRangeStats::collect_integer(&page, 0..2, &|| Ok(())).unwrap();
        parts
            .merge_completed_range(
                BuildRangeStats::collect_integer(&page, 2..5, &|| Ok(())).unwrap(),
                &|| Ok(()),
            )
            .unwrap();
        assert_eq!(
            (
                parts.input_rows(),
                parts.indexable_rows(),
                parts.null_rows(),
                parts.integer()
            ),
            (
                whole.input_rows(),
                whole.indexable_rows(),
                whole.null_rows(),
                whole.integer()
            )
        );
        let empty = BuildRangeStats::collect_integer(&page, 2..2, &|| Ok(())).unwrap();
        assert_eq!(
            (
                empty.input_rows(),
                empty.indexable_rows(),
                empty.null_rows(),
                empty.integer().unwrap().span()
            ),
            (0, 0, 0, None)
        );
        let null = BuildRangeStats::collect_integer(&page, 4..5, &|| Ok(())).unwrap();
        assert_eq!(
            (
                null.input_rows(),
                null.null_rows(),
                null.integer().unwrap().min_max
            ),
            (1, 1, None)
        );
    }
}

#[test]
fn exact_mixed_dictionary_statistics_use_logical_nulls_and_saved_zero_hash_participation() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let spec = KeySpec::new(
        &types,
        &[NullPolicy::Unmatchable, NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::new(&spec, owner.as_ref()).unwrap();
    let keys = input(
        &[
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 1, 1, 1]),
                    Arc::new(StringArray::from(vec![None, Some("alpha")])),
                )
                .unwrap(),
            ),
        ],
        &types,
        &[2, 0, 1, 0, 3],
        owner.as_ref(),
    );
    let hashes = hash(&keys, &spec, &policy, owner.as_ref()).force_hash(0);
    let before = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    let page = prepare(&spec, &keys, owner.as_ref(), &tracker);
    let result =
        BuildRangeStats::collect_hashed(&page, &hashes, &policy, 0..5, &|| Ok(())).unwrap();
    assert_eq!(
        (
            result.input_rows(),
            result.indexable_rows(),
            result.null_rows()
        ),
        (5, 4, 3)
    );
    assert!(result.integer().is_none());
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        before
    );
    let foreign = LocalHashPolicy::new(&spec, owner.as_ref()).unwrap();
    let error = BuildRangeStats::collect_hashed(&page, &hashes, &foreign, 0..5, &|| Ok(()))
        .err()
        .unwrap();
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::Layout);
    let bad = BuildRangeStats::collect_hashed(&page, &hashes, &policy, 0..6, &|| Ok(()))
        .err()
        .unwrap();
    assert_value_failure(&bad, novarocks_value_arrays::ValueViolation::Bounds);
}

#[test]
fn exact_statistics_refuse_foreign_spec_merge_and_keep_original_control_before_publishing() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int8, false);
    let keys = input(
        &[Arc::new(Int8Array::from(vec![1, 2, 3]))],
        std::slice::from_ref(&ty),
        &[2, 0, 1],
        owner.as_ref(),
    );
    let first = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let second = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let page = prepare(&first, &keys, owner.as_ref(), &tracker);
    let mut first_stats = BuildRangeStats::collect_integer(&page, 0..1, &|| Ok(())).unwrap();
    let second_page = prepare(&second, &keys, owner.as_ref(), &tracker);
    let foreign = BuildRangeStats::collect_integer(&second_page, 1..3, &|| Ok(())).unwrap();
    let error = first_stats
        .merge_completed_range(foreign, &|| Ok(()))
        .unwrap_err();
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::Layout);
    assert_eq!(first_stats.input_rows(), 1);
    let cause = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    let ticks = Cell::new(0);
    let error = BuildRangeStats::collect_integer(&page, 0..3, &|| {
        ticks.set(ticks.get() + 1);
        if ticks.get() == 2 {
            Err(cause.clone())
        } else {
            Ok(())
        }
    })
    .err()
    .unwrap();
    assert_eq!(error, cause);
    let other = BuildRangeStats::collect_integer(&page, 1..3, &|| Ok(())).unwrap();
    assert_eq!(
        first_stats
            .merge_completed_range(other, &|| Err(cause.clone()))
            .unwrap_err(),
        cause
    );
    assert_eq!(first_stats.input_rows(), 1);
}

#[test]
fn original_null_observer_can_stop_inside_one_wide_tuple_without_reading_a_later_column() {
    let (owner, tracker) = owner_with_tracker();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let arrays = vec![array; 513];
    let types = vec![FunctionValueType::new(DataType::Int32, false); 513];
    let spec = KeySpec::new(
        &types,
        &vec![NullPolicy::Equal; 513],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let keys = input(&arrays, &types, &[0], owner.as_ref());
    let page = prepare(&spec, &keys, owner.as_ref(), &tracker);
    let ticks = Cell::new(0);
    let cause = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    let stopped = page
        .tuples()
        .row_has_null_observed(0, &|| {
            ticks.set(ticks.get() + 1);
            if ticks.get() == 3 {
                Err(cause.clone())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(stopped, cause);
    assert_eq!(ticks.get(), 3);
    assert!(!page.tuples().row_has_null(0).unwrap());
}

#[test]
fn null_carrier_statistics_and_tuple_required_checks_use_logical_null_without_a_bitmap() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Null, true);
    let source: ArrayRef = Arc::new(NullArray::new(3));
    assert!(source.nulls().is_none());
    let keys = input(
        std::slice::from_ref(&source),
        std::slice::from_ref(&ty),
        &[2, 0, 2, 1],
        owner.as_ref(),
    );
    for nulls in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let spec = KeySpec::new(std::slice::from_ref(&ty), &[nulls], owner.as_ref(), &|| {
            Ok(())
        })
        .unwrap();
        let policy = LocalHashPolicy::new(&spec, owner.as_ref()).unwrap();
        let hashes = hash(&keys, &spec, &policy, owner.as_ref());
        let page = prepare(&spec, &keys, owner.as_ref(), &tracker);
        assert!(page.tuples().row_has_null(2).unwrap());
        let stats =
            BuildRangeStats::collect_hashed(&page, &hashes, &policy, 0..4, &|| Ok(())).unwrap();
        assert_eq!(
            (
                stats.input_rows(),
                stats.indexable_rows(),
                stats.null_rows()
            ),
            (4, if nulls == NullPolicy::Equal { 4 } else { 0 }, 4)
        );
        assert!(stats.integer().is_none());
        let sub =
            BuildRangeStats::collect_hashed(&page, &hashes, &policy, 1..3, &|| Ok(())).unwrap();
        assert_eq!((sub.input_rows(), sub.null_rows()), (2, 2));
        let empty =
            BuildRangeStats::collect_hashed(&page, &hashes, &policy, 2..2, &|| Ok(())).unwrap();
        assert_eq!(
            (
                empty.input_rows(),
                empty.indexable_rows(),
                empty.null_rows()
            ),
            (0, 0, 0)
        );
    }
    let error = KeyInput::new(
        &[source],
        &[FunctionValueType::new(DataType::Null, false)],
        &readable(3),
        &[0],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| Ok(()),
    )
    .err()
    .unwrap();
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::RequiredNull);
}
