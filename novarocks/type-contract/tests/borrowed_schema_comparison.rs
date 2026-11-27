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

//! Requested-allocation evidence for borrowed schema comparison only. All
//! fixtures and assertions live outside capture; this is not a MEM grant proof.

use arrow_schema::{DataType, Field, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, MAX_VALUE_TYPE_DEPTH, ValueTypeError,
    arrow_data_types_exact_borrowed_observed, arrow_fields_exact_borrowed_observed,
    arrow_fields_exact_observed,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    collections::HashMap,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Requests {
    allocations: usize,
    reallocations: usize,
    bytes: usize,
}
thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static REQUESTS: Cell<Requests> = const {
        Cell::new(Requests { allocations: 0, reallocations: 0, bytes: 0 })
    };
}
fn record(bytes: usize, realloc: bool) {
    if ENABLED.try_with(Cell::get).unwrap_or(false) {
        let _ = REQUESTS.try_with(|cell| {
            let mut current = cell.get();
            current.bytes = current.bytes.saturating_add(bytes);
            if realloc {
                current.reallocations = current.reallocations.saturating_add(1);
            } else {
                current.allocations = current.allocations.saturating_add(1);
            }
            cell.set(current);
        });
    }
}
struct ObservedAllocator;
// SAFETY: Original pointers, Layouts and sizes are forwarded to System without
// modification. Const, non-Drop TLS counters allocate nothing and never unwind.
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller's valid allocation Layout is passed unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size(), false);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: System receives the original valid allocation Layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size(), false);
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: System receives the caller's original allocation and size.
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        if !replacement.is_null() {
            record(size, true);
        }
        replacement
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The original pointer and allocation Layout are unchanged.
        unsafe { System.dealloc(pointer, layout) };
    }
}
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn capture<T>(call: impl FnOnce() -> T) -> (T, Requests) {
    REQUESTS.with(|cell| cell.set(Requests::default()));
    ENABLED.with(|cell| cell.set(true));
    let result = call();
    ENABLED.with(|cell| cell.set(false));
    (result, REQUESTS.with(Cell::get))
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Failure {
    Type(ValueTypeError),
    Control(CompileControlError),
}
impl From<ValueTypeError> for Failure {
    fn from(value: ValueTypeError) -> Self {
        Self::Type(value)
    }
}
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

fn metadata(reverse: bool) -> HashMap<String, String> {
    let entries = [
        (format!("{}A", "k".repeat(2050)), "v".repeat(3075)),
        (format!("{}B", "k".repeat(2050)), "w".repeat(2049)),
        ("provider".into(), "source".into()),
    ];
    let mut result = HashMap::new();
    if reverse {
        for (key, value) in entries.into_iter().rev() {
            result.insert(key, value);
        }
    } else {
        result.extend(entries);
    }
    result
}
fn nested(reverse: bool) -> Field {
    #[allow(deprecated)]
    let dictionary = Field::new_dict(
        "labels",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        i64::MIN,
        true,
    )
    .with_metadata(metadata(reverse));
    Field::new(
        "root",
        DataType::Struct(
            vec![
                Arc::new(dictionary),
                Arc::new(Field::new(
                    "sequence",
                    DataType::LargeList(Arc::new(
                        Field::new("item", DataType::Decimal128(38, -7), true)
                            .with_metadata(metadata(reverse)),
                    )),
                    true,
                )),
                Arc::new(Field::new(
                    "clock",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    false,
                )),
            ]
            .into(),
        ),
        true,
    )
    .with_metadata(metadata(reverse))
}

#[test]
fn borrowed_nested_comparisons_allocate_no_requested_backing() {
    let left = nested(false);
    let right = nested(true);
    let callbacks = Cell::new(0usize);
    let (field, requests) = capture(|| {
        arrow_fields_exact_borrowed_observed::<Failure>(&left, &right, || {
            callbacks.set(callbacks.get() + 1);
            Ok(())
        })
    });
    assert_eq!(field, Ok(true));
    assert_eq!(requests, Requests::default());
    assert!(callbacks.get() > 20);
    let (types, requests) = capture(|| {
        arrow_data_types_exact_borrowed_observed::<Failure>(
            left.data_type(),
            right.data_type(),
            || Ok(()),
        )
    });
    assert_eq!(types, Ok(true));
    assert_eq!(requests, Requests::default());
    let (observed, requests) =
        capture(|| arrow_fields_exact_observed::<Failure>(&left, &right, || Ok(())));
    assert_eq!(observed, Ok(true));
    assert_eq!(requests, Requests::default());
}

#[test]
fn borrowed_metadata_matches_unordered_exact_keys_and_values() {
    let left = Field::new("v", DataType::Utf8, true).with_metadata(metadata(false));
    let equal = Field::new("v", DataType::Utf8, true).with_metadata(metadata(true));
    let mut changed_key = metadata(true);
    let value = changed_key.remove("provider").unwrap();
    changed_key.insert("provideR".into(), value);
    let changed_key = Field::new("v", DataType::Utf8, true).with_metadata(changed_key);
    let mut changed_value = metadata(true);
    let key = format!("{}A", "k".repeat(2050));
    changed_value
        .get_mut(&key)
        .unwrap()
        .replace_range(3074.., "x");
    let changed_value = Field::new("v", DataType::Utf8, true).with_metadata(changed_value);
    for (right, expected) in [
        (&equal, true),
        (&changed_key, false),
        (&changed_value, false),
    ] {
        let (result, requests) =
            capture(|| arrow_fields_exact_borrowed_observed::<Failure>(&left, right, || Ok(())));
        assert_eq!(result, Ok(expected));
        assert_eq!(requests, Requests::default());
    }
}

#[test]
fn borrowed_exact_field_checks_dictionary_bits_and_complete_type_attributes() {
    #[allow(deprecated)]
    let dictionary = |name, nullable, id, ordered| {
        Field::new_dict(
            name,
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            nullable,
            id,
            ordered,
        )
    };
    let left = dictionary("v", true, 0, false);
    for right in [
        dictionary("V", true, 0, false),
        dictionary("v", false, 0, false),
        dictionary("v", true, i64::MAX, false),
        dictionary("v", true, 0, true),
    ] {
        let (result, requests) =
            capture(|| arrow_fields_exact_borrowed_observed::<Failure>(&left, &right, || Ok(())));
        assert_eq!(result, Ok(false));
        assert_eq!(requests, Requests::default());
    }
    for (left, right) in [
        (DataType::Int64, DataType::UInt64),
        (DataType::Decimal128(38, 2), DataType::Decimal128(37, 2)),
        (DataType::Decimal128(38, 2), DataType::Decimal128(38, -2)),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ] {
        let (result, requests) = capture(|| {
            arrow_data_types_exact_borrowed_observed::<Failure>(&left, &right, || Ok(()))
        });
        assert_eq!(result, Ok(false));
        assert_eq!(requests, Requests::default());
    }
}

#[test]
fn borrowed_observer_first_failure_stops_every_real_success_and_false_prefix() {
    let left = nested(false);
    let equal = nested(true);
    let changed_metadata = HashMap::from([
        (format!("{}A", "k".repeat(2050)), "v".repeat(3075)),
        (format!("{}B", "k".repeat(2050)), "w".repeat(2049)),
        ("provider".into(), "sourcE".into()),
    ]);
    let DataType::Struct(fields) = equal.data_type() else {
        unreachable!("fixture is a Struct");
    };
    let mut fields = fields.to_vec();
    fields[0] = Arc::new(fields[0].as_ref().clone().with_metadata(changed_metadata));
    let different =
        Field::new("root", DataType::Struct(fields.into()), true).with_metadata(metadata(true));
    for (right, expected) in [(&equal, true), (&different, false)] {
        for type_port in [false, true] {
            let calls = Cell::new(0usize);
            let compare = |fail: Option<(usize, CompileControlError)>| {
                let observer = || {
                    let at = calls.get();
                    calls.set(at + 1);
                    if let Some((stop, cause)) = fail.clone()
                        && at == stop
                    {
                        return Err(Failure::Control(cause));
                    }
                    Ok(())
                };
                if type_port {
                    arrow_data_types_exact_borrowed_observed(
                        left.data_type(),
                        right.data_type(),
                        observer,
                    )
                } else {
                    arrow_fields_exact_borrowed_observed(&left, right, observer)
                }
            };
            let (result, requests) = capture(|| compare(None));
            assert_eq!(result, Ok(expected));
            assert_eq!(requests, Requests::default());
            let total = calls.get();
            for cause in CAUSES {
                for stop in 0..total {
                    calls.set(0);
                    let (result, requests) = capture(|| compare(Some((stop, cause.clone()))));
                    assert_eq!(result, Err(Failure::Control(cause.clone())));
                    // Observer calls have no payload: this exact count proves
                    // the complete prefix, including no retry after refusal.
                    assert_eq!(calls.get(), stop + 1);
                    assert_eq!(requests, Requests::default());
                }
            }
        }
    }
}

#[test]
fn borrowed_type_bounds_and_wide_metadata_keep_zero_allocation() {
    let mut deep = DataType::Int64;
    for _ in 1..MAX_VALUE_TYPE_DEPTH {
        deep = DataType::List(Arc::new(Field::new("item", deep, true)));
    }
    let (result, requests) =
        capture(|| arrow_data_types_exact_borrowed_observed::<Failure>(&deep, &deep, || Ok(())));
    assert_eq!(result, Ok(true));
    assert_eq!(requests, Requests::default());
    let over = DataType::List(Arc::new(Field::new("item", deep, true)));
    let (result, requests) =
        capture(|| arrow_data_types_exact_borrowed_observed::<Failure>(&over, &over, || Ok(())));
    assert_eq!(result, Err(Failure::Type(ValueTypeError::TooDeep)));
    assert_eq!(requests, Requests::default());
    let fields = (0..320)
        .map(|i| {
            Field::new(format!("field{i}"), DataType::Int64, true)
                .with_metadata(metadata(i % 2 == 0))
        })
        .collect::<Vec<_>>();
    let wide = DataType::Struct(fields.into());
    let calls = Cell::new(0usize);
    let (result, requests) = capture(|| {
        arrow_data_types_exact_borrowed_observed::<Failure>(&wide, &wide, || {
            calls.set(calls.get() + 1);
            Ok(())
        })
    });
    assert_eq!(result, Ok(true));
    assert!(calls.get() > 256);
    assert_eq!(requests, Requests::default());

    let field_with_metadata = || {
        Field::new("metadata", DataType::Int64, true).with_metadata(
            (0..512)
                .map(|i| (format!("key-{i}"), format!("value-{i}")))
                .collect::<arrow_schema::Metadata>(),
        )
    };
    let left = field_with_metadata();
    let right = field_with_metadata();
    calls.set(0);
    let (result, requests) = capture(|| {
        arrow_fields_exact_borrowed_observed::<Failure>(&left, &right, || {
            calls.set(calls.get() + 1);
            Ok(())
        })
    });
    assert_eq!(result, Ok(true));
    assert!(calls.get() < 4 * 512);
    assert_eq!(requests, Requests::default());
}
