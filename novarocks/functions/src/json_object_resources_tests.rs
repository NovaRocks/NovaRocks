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
use arrow_array::{BooleanArray, Int64Array, UInt32Array, builder::ListBuilder};
use arrow_buffer::NullBuffer;
use arrow_schema::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Control {
    events: Mutex<usize>,
    stop: Option<usize>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert_eq!(units, 1);
        let mut events = self.events.lock().unwrap();
        let at = *events;
        *events += 1;
        if let Some(stop) = self.stop {
            assert!(at <= stop, "observation after first refusal");
        }
        if self.stop == Some(at) {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("geometry never waits")
    }
}

#[test]
#[expect(deprecated)]
fn json_object_resources_type_debug_covers_every_variant_and_hidden_field_facts() {
    use DataType::*;
    let field = Arc::new(
        Field::new_dict("\0\u{200b}µ", Utf8, true, i64::MIN, true)
            .with_metadata(HashMap::from([("\nname".into(), "\0\u{200b}value".into())])),
    );
    assert_eq!(field.dict_id(), None);
    let types = [
        Null,
        Boolean,
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float16,
        Float32,
        Float64,
        Timestamp(TimeUnit::Nanosecond, Some("\0µ".into())),
        Date32,
        Date64,
        Time32(TimeUnit::Millisecond),
        Time64(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
        Interval(IntervalUnit::MonthDayNano),
        Binary,
        FixedSizeBinary(i32::MIN),
        LargeBinary,
        BinaryView,
        Utf8,
        LargeUtf8,
        Utf8View,
        List(field.clone()),
        ListView(field.clone()),
        FixedSizeList(field.clone(), i32::MIN),
        LargeList(field.clone()),
        LargeListView(field.clone()),
        Struct(vec![field.clone(), field.clone()].into()),
        Union(
            UnionFields::new([1, 127], [field.clone(), field.clone()]),
            UnionMode::Sparse,
        ),
        Dictionary(Box::new(UInt64), Box::new(LargeListView(field.clone()))),
        Decimal32(255, -128),
        Decimal64(255, -128),
        Decimal128(255, -128),
        Decimal256(255, -128),
        Map(field.clone(), false),
        RunEndEncoded(field.clone(), field.clone()),
    ];
    for ty in &types {
        let actual = format!("{ty:?}");
        let bound = type_debug(ty, &Control::default()).unwrap();
        assert!(
            actual.len() <= bound,
            "{actual}: {} > {bound}",
            actual.len()
        );
    }
    let ty = RunEndEncoded(field.clone(), field);
    let control = Control::default();
    type_debug(&ty, &control).unwrap();
    let count = *control.events.lock().unwrap();
    for stop in 0..count {
        let control = Control {
            stop: Some(stop),
            ..Control::default()
        };
        assert_eq!(
            type_debug(&ty, &control),
            Err(KernelFailure::DeadlineExceeded)
        );
        assert_eq!(*control.events.lock().unwrap(), stop + 1);
    }
}

#[test]
fn json_object_resources_null_unsupported_is_failure_only_but_null_parent_skips_children() {
    let unsupported: ArrayRef = Arc::new(UInt32Array::from(vec![None]));
    let geometry = scalar_geometry(&unsupported, 0, &Control::default()).unwrap();
    assert!(matches!(
        geometry,
        ScalarGeometry::FailureOnly {
            copy: CopyGeometry {
                bytes: 0,
                requests: 0
            },
            ..
        }
    ));
    let fields = vec![Arc::new(Field::new("unsupported", DataType::UInt32, true))].into();
    let parent: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![unsupported],
        Some(NullBuffer::from(vec![false])),
    ));
    let control = Control::default();
    assert_eq!(
        scalar_geometry(&parent, 0, &control).unwrap(),
        ScalarGeometry::Known {
            copy: CopyGeometry::default(),
            format: FormatGeometry::Null
        }
    );
    assert_eq!(*control.events.lock().unwrap(), 1);
}

#[test]
fn json_object_resources_nested_failure_keeps_full_parent_and_known_sibling_copy_prefix() {
    let fields = vec![
        Arc::new(Field::new("a", DataType::Utf8, false)),
        Arc::new(Field::new("b", DataType::UInt32, false)),
        Arc::new(Field::new("c", DataType::Binary, false)),
    ]
    .into();
    let parent: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(UInt32Array::from(vec![1])),
            Arc::new(BinaryArray::from_vec(vec![b"12345"])),
        ],
        None,
    ));
    let geometry = scalar_geometry(&parent, 0, &Control::default()).unwrap();
    assert!(matches!(geometry, ScalarGeometry::FailureOnly { .. }));
    assert_eq!(
        geometry.copy(),
        CopyGeometry {
            bytes: 3 * size_of::<Option<AggScalarValue>>() + 8,
            requests: 3
        }
    );
    assert!(geometry.diagnostic_bytes() >= "unsupported scalar type: UInt32".len());
}

#[test]
fn json_object_resources_default_scalar_debug_bounds_match_original_shared_formatter() {
    let mut lists = ListBuilder::new(arrow_array::builder::StringBuilder::new());
    lists.values().append_value("\0\u{200b}µ");
    lists.values().append_null();
    lists.append(true);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Int64Array::from(vec![i64::MIN])),
        Arc::new(StringArray::from(vec!["\0\u{200b}µ"])),
        Arc::new(BinaryArray::from_vec(vec![b"\0\xff"])),
        Arc::new(lists.finish()),
    ];
    for array in arrays {
        let geometry = scalar_geometry(&array, 0, &Control::default()).unwrap();
        let ScalarGeometry::Known { format, .. } = geometry else {
            panic!("supported original scalar");
        };
        let mut work = crate::aggregate_scalar::ScalarWork::new(None);
        let scalar = crate::aggregate_scalar::scalar_from_array(&array, 0, &mut work)
            .unwrap()
            .unwrap();
        assert!(format!("{scalar:?}").len() <= format.scalar_debug().unwrap());
    }
}

#[test]
fn json_object_resources_checked_geometry_refuses_unrepresentable_layouts() {
    assert_eq!(
        CopyGeometry::exact(usize::MAX, Layout::new::<Option<AggScalarValue>>()),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        quoted_debug(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        sequence_syntax(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn json_object_resources_batch_envelopes_cover_actual_output_and_empty_builder() {
    use crate::Selection;
    let raws = [
        "",
        "{",
        "{\"z\":[1,null],\"a\":\"µ\"}",
        "{\"$serde_json::private::RawValue\":\"[2,1]\"}",
    ];
    for raw in raws {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("\0key"),
                None,
                Some("duplicate"),
            ])),
            Arc::new(StringArray::from(vec![raw, raw, raw])),
            Arc::new(StringArray::from(vec!["odd", "odd", "odd"])),
        ];
        for selection in [
            Selection::all(3),
            Selection::try_sparse(3, &[0, 2]).unwrap(),
            Selection::try_sparse(3, &[]).unwrap(),
        ] {
            let mut resources = JsonObjectBatchResources::default();
            for (ordinal, row) in selection.iter().enumerate() {
                resources
                    .add_row(arrays.as_slice(), ordinal, row, &Control::default())
                    .unwrap();
            }
            let output = crate::json_object_core::evaluate_original(&arrays, selection).unwrap();
            let text = output.as_any().downcast_ref::<StringArray>().unwrap();
            let actual_bytes: usize = text.iter().flatten().map(str::len).sum();
            assert!(actual_bytes <= resources.text_bytes);
            let retained = output.get_array_memory_size()
                + crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
                    &DataType::Utf8,
                    2,
                    output.nulls().is_some(),
                    0,
                )
                .unwrap();
            assert!(
                retained + 2 * crate::MAX_ROW_ERROR_MESSAGE_BYTES
                    <= resources.body_grant(selection.len()).unwrap()
            );
            assert!(resources.diagnostic_grant().unwrap() >= 2 * 178);
        }
    }
}

#[test]
fn json_object_resources_failure_only_is_not_an_early_data_verdict() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(UInt32Array::from(vec![7])),
    ];
    let mut resources = JsonObjectBatchResources::default();
    resources
        .add_row(arrays.as_slice(), 0, 0, &Control::default())
        .unwrap();
    assert!(resources.diagnostic_grant().unwrap() > 0);
    // Only the original computation decides that its NULL key suppresses the
    // unsupported value. Geometry did not fabricate an earlier Data error.
    let output =
        crate::json_object_core::evaluate_original(&arrays, crate::Selection::all(1)).unwrap();
    assert!(output.is_null(0));
    assert!(resources.row_peak > 0);
    assert!(resources.row_requests > 0);
}

#[test]
fn json_object_resources_parser_and_batch_arithmetic_refuse_without_default_budget() {
    assert!(matches!(
        parser_geometry(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(
        JsonObjectBatchResources::default().body_grant(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    let resources = JsonObjectBatchResources {
        diagnostic_bytes: usize::MAX,
        ..Default::default()
    };
    assert_eq!(
        resources.diagnostic_grant(),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn json_object_resources_cumulative_requests_and_peak_remain_distinct_and_checked() {
    let mut resources = JsonObjectBatchResources::default();
    let arguments: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["a", "b"])),
        Arc::new(StringArray::from(vec!["[1,2]", "{bad"])),
    ];
    for row in 0..2 {
        resources
            .add_row(arguments.as_slice(), row, row, &Control::default())
            .unwrap();
    }
    let geometry = resources.body_geometry(2).unwrap();
    assert_eq!(geometry.peak_bytes, resources.body_grant(2).unwrap());
    assert!(geometry.requested_bytes > geometry.peak_bytes);
    assert!(geometry.requests > resources.row_requests);
    // Cumulative growth arithmetic must fail before an opaque request even
    // when the simultaneous-stock expression would still fit in usize.
    let rows = usize::MAX / (GROWTH_REQUESTS * 4);
    assert!(matches!(
        JsonObjectBatchResources::default().body_geometry(rows),
        Err(KernelFailure::ResourceExhausted)
    ));
}
