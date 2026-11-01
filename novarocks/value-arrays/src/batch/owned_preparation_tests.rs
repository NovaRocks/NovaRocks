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
use crate::{
    ByteLayout,
    access::ByteSource,
    batch::selected::visit_selected,
    control::{global_growth, uncontrolled},
    decode::ValueConversionContext,
    reachability::ReachableRows,
};
use arrow_array::{ArrayRef, DictionaryArray, StringViewArray, types::Int32Type};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use novarocks_type_contract::ValueTypeStorageHold;
use std::sync::{
    Weak,
    atomic::{AtomicUsize, Ordering},
};

fn rows(len: usize, selected: &[usize]) -> Arc<SelectionSet> {
    let mut rows = SelectionSet::try_new(len).unwrap();
    for &row in selected {
        rows.insert(row).unwrap();
    }
    Arc::new(rows)
}

fn checked(decoded: DecodedBytes, rows: Arc<SelectionSet>) -> OwnedPreparedBytes {
    OwnedPreparedBytes::new_observed(
        decoded,
        rows,
        ValueTypeStorageLifetime::pure(),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap()
}

fn values(input: &PreparedBytes<'_>) -> Vec<(usize, Option<Vec<u8>>)> {
    let mut values = Vec::new();
    visit_selected(
        input,
        &mut |_, row, value| {
            values.push((row, value.map(<[u8]>::to_vec)));
            Ok::<_, Infallible>(())
        },
        &mut uncontrolled,
    )
    .unwrap();
    values
}

fn checks() -> (usize, usize) {
    PREPARATION_CHECKS.with(std::cell::Cell::get)
}

#[test]
fn owned_preparation_preserves_sliced_identity_constant_and_dictionary_null_mapping() {
    let input: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("skip"),
        Some("a retained external identity value"),
        None,
        Some("unselected"),
    ]));
    let input = input.slice(1, 3);
    let selected = rows(3, &[0, 1]);
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
            &ReachableRows::Selection(&selected),
        )
        .unwrap();
    assert!(matches!(decoded.mapping(), RowMapping::Identity { len: 3 }));
    let short = values(&PreparedBytes::new(&decoded, &selected).unwrap());
    let owned = checked(decoded, selected);
    assert_eq!(values(&owned.borrow()), short);
    assert_eq!(
        short,
        vec![
            (0, Some(b"a retained external identity value".to_vec())),
            (1, None)
        ]
    );

    for value in [Some("a retained external constant value"), None] {
        let input: ArrayRef = Arc::new(StringViewArray::from(vec![value]));
        let selected = rows(4, &[0, 2, 3]);
        let decoded = ValueConversionContext::default()
            .decode(
                &ByteSource::constant(&input, ByteLayout::Utf8View, 4).unwrap(),
                &ReachableRows::Selection(&selected),
            )
            .unwrap();
        assert!(matches!(
            decoded.mapping(),
            RowMapping::Constant { len: 4, index: 0 }
        ));
        let owned = checked(decoded, selected);
        assert_eq!(
            values(&owned.borrow()),
            [0, 2, 3].map(|row| (row, value.map(|s| s.as_bytes().to_vec())))
        );
    }

    let bottom: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("a retained external dictionary value"),
        None,
        Some("unreferenced"),
    ]));
    // The NULL key's physical integer is deliberately outside the dictionary.
    // Arrow's checked dictionary constructor accepts it because it is invalid.
    let keys = Int32Array::new(
        ScalarBuffer::from(vec![2, 0, i32::MAX, 1, 0]),
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    );
    let input: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, bottom).unwrap());
    let selected = rows(5, &[1, 2, 3, 4]);
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8, Some(ByteLayout::Utf8View)).unwrap(),
            &ReachableRows::Selection(&selected),
        )
        .unwrap();
    assert!(matches!(decoded.mapping(), RowMapping::Dictionary(_)));
    assert!(!decoded.bottom_coverage().contains(2));
    let short = values(&PreparedBytes::new(&decoded, &selected).unwrap());
    let owned = checked(decoded, selected);
    assert_eq!(values(&owned.borrow()), short);
    assert_eq!(
        short,
        vec![
            (1, Some(b"a retained external dictionary value".to_vec())),
            (2, None),
            (3, None),
            (4, Some(b"a retained external dictionary value".to_vec())),
        ]
    );
    assert!(matches!(
        owned.borrow().check_row(0),
        Err(ValueError::Invariant(ValueViolation::UndefinedRow))
    ));
}

#[test]
fn owned_preparation_checks_once_and_moved_short_loans_do_not_rebind_scan_or_request() {
    let long = "external owned preparation payload";
    let mut source_values = vec![None; 200_000];
    source_values[199_999] = Some(long);
    let input: ArrayRef = Arc::new(StringViewArray::from(source_values));
    let selected = rows(200_000, &[0, 199_999]);
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
            &ReachableRows::Selection(&selected),
        )
        .unwrap();
    let before = checks();
    let before_bind = crate::arrow_raw::concrete_bindings();
    let mut requests = Vec::new();
    let mut units = 0;
    let owned = OwnedPreparedBytes::new_observed(
        decoded,
        selected,
        ValueTypeStorageLifetime::pure(),
        &mut |layout| {
            requests.push(layout);
            Ok::<_, Infallible>(())
        },
        &mut |n| {
            units += n;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(checks(), (before.0 + 1, before.1 + 2));
    assert_eq!(crate::arrow_raw::concrete_bindings(), before_bind + 1);
    assert!(!requests.is_empty());
    assert!(units > 0);
    let admitted_requests = requests.len();
    let prepared_checks = checks();
    let prepared_bindings = crate::arrow_raw::concrete_bindings();
    let owned = vec![owned].pop().unwrap();
    for _ in 0..4_096 {
        let loan = owned.borrow();
        let copied = loan;
        assert_eq!(copied.len(), 2);
        assert_eq!(copied.view().value(199_999).unwrap(), Some(long.as_bytes()));
        assert_eq!(copied.view().value(0).unwrap(), None);
    }
    assert_eq!(checks(), prepared_checks);
    assert_eq!(crate::arrow_raw::concrete_bindings(), prepared_bindings);
    assert_eq!(requests.len(), admitted_requests);
}

#[test]
fn owned_and_short_preparation_reject_the_same_length_and_readable_domain_failures() {
    let input: ArrayRef = Arc::new(StringViewArray::from(vec!["readable", "not admitted"]));
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
            &ReachableRows::indices(2, &[0]),
        )
        .unwrap();
    for (selected, expected) in [
        (rows(3, &[0]), ValueViolation::Bounds),
        (rows(2, &[1]), ValueViolation::UndefinedRow),
    ] {
        let short = PreparedBytes::new(&decoded, &selected);
        assert!(matches!(short, Err(ValueError::Invariant(actual)) if actual == expected));
        let mut requests = 0;
        let owned = OwnedPreparedBytes::new_observed(
            decoded.clone(),
            selected,
            ValueTypeStorageLifetime::pure(),
            &mut |_| {
                requests += 1;
                Ok::<_, Infallible>(())
            },
            &mut uncontrolled,
        );
        assert!(
            matches!(owned, Err(ValueFailure::Value(ValueError::Invariant(actual))) if actual == expected)
        );
        if expected == ValueViolation::Bounds {
            assert_eq!(requests, 0);
        }
    }
}

#[test]
fn owned_preparation_original_growth_and_validator_control_refusals_are_preserved() {
    let input: ArrayRef = Arc::new(StringViewArray::from(vec![
        "an external value needs a real source table",
    ]));
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
            &ReachableRows::all(1),
        )
        .unwrap();
    let result = OwnedPreparedBytes::new_observed(
        decoded,
        rows(1, &[0]),
        ValueTypeStorageLifetime::pure(),
        &mut |_| Err("source-table-growth"),
        &mut |_| Ok(()),
    );
    assert!(matches!(
        result,
        Err(ValueFailure::Caller("source-table-growth"))
    ));

    for selected in [rows(200_000, &[]), rows(200_000, &[199_999])] {
        let input: ArrayRef = Arc::new(StringViewArray::new_null(200_000));
        let decoded = ValueConversionContext::default()
            .decode(
                &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
                &ReachableRows::all(200_000),
            )
            .unwrap();
        let before = checks();
        let result = OwnedPreparedBytes::new_observed(
            decoded,
            selected,
            ValueTypeStorageLifetime::pure(),
            &mut |_| Ok::<_, &'static str>(()),
            &mut |n| {
                if checks().0 > before.0 && n > 0 {
                    Err("selected-bitmap-control")
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            result,
            Err(ValueFailure::Caller("selected-bitmap-control"))
        ));
        assert_eq!(checks().0, before.0 + 1);
    }
}

struct Hold {
    drops: Arc<AtomicUsize>,
    selection: Option<Weak<SelectionSet>>,
}
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(selection) = &self.selection {
            assert!(
                selection.upgrade().is_none(),
                "selection backing must exit before its preparation creator"
            );
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn owned_preparation_move_and_context_clear_keep_source_and_preparation_creators_to_actual_exit() {
    let source_drops = Arc::new(AtomicUsize::new(0));
    let preparation_drops = Arc::new(AtomicUsize::new(0));
    let source_storage = ValueTypeStorageLifetime::held(Arc::new(Hold {
        drops: source_drops.clone(),
        selection: None,
    }));
    let input: ArrayRef = Arc::new(StringViewArray::from(vec![
        "retained after caller and context exit",
    ]));
    let weak_source = Arc::downgrade(&input);
    let mut context = ValueConversionContext::default();
    let decoded = context
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None)
                .unwrap()
                .with_storage(&source_storage),
            &ReachableRows::all(1),
        )
        .unwrap();
    let selected = rows(1, &[0]);
    let preparation_storage = ValueTypeStorageLifetime::held(Arc::new(Hold {
        drops: preparation_drops.clone(),
        selection: Some(Arc::downgrade(&selected)),
    }));
    let owned = OwnedPreparedBytes::new_observed(
        decoded,
        selected,
        preparation_storage,
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    drop(source_storage);
    drop(input);
    context.clear();
    drop(context);
    assert!(weak_source.upgrade().is_some());
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 0);
    let owned = std::thread::spawn(move || owned).join().unwrap();
    assert_eq!(
        values(&owned.borrow()),
        vec![(0, Some(b"retained after caller and context exit".to_vec()))]
    );
    drop(owned);
    assert!(weak_source.upgrade().is_none());
    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_preparation_failure_drops_real_domain_before_its_preparation_creator() {
    let input: ArrayRef = Arc::new(StringViewArray::from(vec![
        "an external failure-path source table",
    ]));
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
            &ReachableRows::all(1),
        )
        .unwrap();
    let selected = rows(1, &[0]);
    let drops = Arc::new(AtomicUsize::new(0));
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold {
        drops: drops.clone(),
        selection: Some(Arc::downgrade(&selected)),
    }));
    let result = OwnedPreparedBytes::new_observed(
        decoded,
        selected,
        storage,
        &mut |_| Err("failure-path-growth"),
        &mut |_| Ok(()),
    );
    assert!(matches!(
        result,
        Err(ValueFailure::Caller("failure-path-growth"))
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_preparation_selection_alias_cannot_expand_the_admitted_domain() {
    let input: ArrayRef = Arc::new(StringViewArray::from(vec!["admitted", "unreadable"]));
    let decoded = ValueConversionContext::default()
        .decode(
            &ByteSource::parse(&input, ByteLayout::Utf8View, None).unwrap(),
            &ReachableRows::indices(2, &[0]),
        )
        .unwrap();
    let mut selected = rows(2, &[0]);
    let owned = checked(decoded.clone(), selected.clone());
    assert!(Arc::get_mut(&mut selected).is_none());
    assert!(matches!(
        owned.borrow().check_row(1),
        Err(ValueError::Invariant(ValueViolation::UndefinedRow))
    ));
    let expanded = checked_attempt(decoded, rows(2, &[0, 1]));
    assert!(matches!(
        expanded,
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::UndefinedRow
        )))
    ));
    assert_eq!(
        values(&owned.borrow()),
        vec![(0, Some(b"admitted".to_vec()))]
    );
    drop(owned);
    assert!(Arc::get_mut(&mut selected).is_some());
}

fn checked_attempt(
    decoded: DecodedBytes,
    rows: Arc<SelectionSet>,
) -> Result<OwnedPreparedBytes, ValueFailure<Infallible>> {
    OwnedPreparedBytes::new_observed(
        decoded,
        rows,
        ValueTypeStorageLifetime::pure(),
        &mut global_growth,
        &mut uncontrolled,
    )
}
