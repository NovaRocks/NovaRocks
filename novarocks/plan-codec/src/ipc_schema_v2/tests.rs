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
use arrow::datatypes::{IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use arrow::ipc::{convert::try_fb_to_schema, writer::StreamWriter};
use novarocks_type_contract::{CompileControlError, NR_LOGICAL_TYPE_KEY, arrow_fields_exact};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl Control {
    fn recording() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            fail: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            fail: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.events.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.fail {
            assert!(at <= refusal, "a refused source was observed again");
        }
        trace.push((phase, units));
        match self.fail.clone() {
            Some((refusal, cause)) if at == refusal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> IpcSchemaProjectionLimits {
    // This explicit fixture envelope is not a host allocation grant.
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 1024 * 1024,
        max_flatbuffer_bytes: 4 * 1024 * 1024,
    }
}
fn decoded_field(metadata: &[u8]) -> Field {
    let message = arrow::ipc::root_as_message(metadata).unwrap();
    assert_eq!(message.version(), arrow::ipc::MetadataVersion::V5);
    assert_eq!(message.header_type(), arrow::ipc::MessageHeader::Schema);
    assert_eq!(message.bodyLength(), 0);
    assert!(message.custom_metadata().is_none());
    let schema = try_fb_to_schema(message.header_as_schema().unwrap()).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert!(schema.metadata().is_empty());
    schema.field(0).clone()
}
fn assert_projection(field: &Field) {
    let bytes = encode_single_field_schema(field, limits(), &Control::recording()).unwrap();
    let decoded = decoded_field(&bytes);
    assert!(
        arrow_fields_exact(field, &decoded),
        "expected {field:?}; actual {decoded:?}"
    );
    assert_eq!(decoded.name(), field.name());
    assert_eq!(decoded.is_nullable(), field.is_nullable());
    assert_eq!(decoded.metadata(), field.metadata());
}
fn source_field(name: &str, ty: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new(name, ty, nullable).with_metadata(HashMap::from([
        ("provider_id".to_owned(), "71".to_owned()),
        ("source_annotation".to_owned(), "preserved".to_owned()),
    ])))
}
#[allow(deprecated)]
fn dictionary_field(name: &str, ty: DataType, id: i64, ordered: bool) -> Arc<Field> {
    Arc::new(
        Field::new_dict(name, ty, true, id, ordered)
            .with_metadata(HashMap::from([("provider_id".to_owned(), "99".to_owned())])),
    )
}

#[test]
fn timestamp_units_preserve_none_empty_and_nonempty_timezone_presence() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [
            None,
            Some(Arc::<str>::from("")),
            Some(Arc::<str>::from("UTC")),
        ] {
            let field = Field::new("event_time", DataType::Timestamp(unit, zone.clone()), true);
            let bytes =
                encode_single_field_schema(&field, limits(), &Control::recording()).unwrap();
            let message = arrow::ipc::root_as_message(&bytes).unwrap();
            let fb_field = message.header_as_schema().unwrap().fields().unwrap().get(0);
            assert_eq!(
                fb_field.type_as_timestamp().unwrap().timezone(),
                zone.as_deref()
            );
            assert!(arrow_fields_exact(&field, &decoded_field(&bytes)));
        }
        // Independently demonstrate the locked standard writer's lossy empty
        // zone projection; the new schema author must preserve Some("").
        let schema = Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(unit, Some("".into())),
            true,
        )]);
        let mut stream = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream, &schema).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let metadata_len = u32::from_le_bytes(stream[4..8].try_into().unwrap()) as usize;
        let message = arrow::ipc::root_as_message(&stream[8..8 + metadata_len]).unwrap();
        assert_eq!(
            message
                .header_as_schema()
                .unwrap()
                .fields()
                .unwrap()
                .get(0)
                .type_as_timestamp()
                .unwrap()
                .timezone(),
            None
        );
    }
}

#[test]
fn scalar_width_units_and_decimal_parameters_are_exact_standard_arrow_schema() {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::FixedSizeBinary(0),
        DataType::FixedSizeBinary(16),
        DataType::FixedSizeBinary(31),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Decimal32(9, i8::MIN),
        DataType::Decimal64(18, -1),
        DataType::Decimal128(38, 38),
        DataType::Decimal256(76, 0),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Duration(unit));
    }
    for (ordinal, ty) in types.into_iter().enumerate() {
        assert_projection(
            source_field(&format!("source_{ordinal}"), ty, ordinal % 2 == 0).as_ref(),
        );
    }
}

#[test]
fn nested_field_boundaries_keep_order_names_metadata_nullability_and_union_ids() {
    let json = Arc::new(
        Field::new("json_value", DataType::Utf8, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("provider_id".to_owned(), "17".to_owned()),
        ])),
    );
    let key = source_field("source_key", DataType::Utf8, false);
    let entries = source_field(
        "source_entries",
        DataType::Struct(vec![key, json.clone()].into()),
        false,
    );
    let ends = source_field("source_runs", DataType::Int16, false);
    let union: UnionFields = [
        (7, json.clone()),
        (1, source_field("integer", DataType::Int64, false)),
    ]
    .into_iter()
    .collect();
    for ty in [
        DataType::List(json.clone()),
        DataType::LargeList(json.clone()),
        DataType::ListView(json.clone()),
        DataType::LargeListView(json.clone()),
        DataType::FixedSizeList(json.clone(), 0),
        DataType::FixedSizeList(json.clone(), 3),
        DataType::Struct(
            vec![
                json.clone(),
                source_field("middle", DataType::Int64, false),
                json.clone(),
            ]
            .into(),
        ),
        DataType::Map(entries.clone(), false),
        DataType::Map(entries, true),
        DataType::Union(union.clone(), UnionMode::Sparse),
        DataType::Union(union, UnionMode::Dense),
        DataType::RunEndEncoded(ends, json),
    ] {
        assert_projection(source_field("actual_root", ty, true).as_ref());
    }
}

#[test]
#[allow(deprecated)]
fn dictionary_ids_and_order_preserve_root_and_actual_nested_field_boundaries() {
    let ty = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
    for id in [0, -99, i64::MAX] {
        for ordered in [false, true] {
            let field = dictionary_field("root_dictionary", ty.clone(), id, ordered);
            let bytes =
                encode_single_field_schema(&field, limits(), &Control::recording()).unwrap();
            let decoded = decoded_field(&bytes);
            assert_eq!(decoded.dict_id(), Some(id));
            assert_eq!(decoded.dict_is_ordered(), Some(ordered));
            assert!(arrow_fields_exact(&field, &decoded));
        }
    }
    let inner = dictionary_field("inner_dictionary", ty.clone(), -73, true);
    let sibling = dictionary_field("sibling_dictionary", ty, -73, false);
    let structure = DataType::Struct(vec![inner, sibling].into());
    assert_projection(source_field("structure", structure.clone(), false).as_ref());
    let outer = dictionary_field(
        "outer_dictionary",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(structure)),
        i64::MAX,
        false,
    );
    assert_projection(&outer);
}

#[test]
fn authored_nominal_metadata_is_preserved_without_inference_from_carrier() {
    for (ty, label) in [
        (DataType::Utf8, "json"),
        (DataType::LargeBinary, "variant"),
        (DataType::FixedSizeBinary(16), "largeint"),
        (DataType::FixedSizeBinary(16), "uuid"),
        (DataType::Binary, "hll"),
        (DataType::Binary, "bitmap"),
    ] {
        let field = Field::new("declared", ty.clone(), false).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), label.to_owned()),
            ("provider_id".to_owned(), "41".to_owned()),
        ]));
        assert_projection(&field);
        let plain = Field::new("plain", ty, false);
        let bytes = encode_single_field_schema(&plain, limits(), &Control::recording()).unwrap();
        assert!(
            !decoded_field(&bytes)
                .metadata()
                .contains_key(NR_LOGICAL_TYPE_KEY)
        );
    }
}

fn assert_refusals(
    field: &Field,
    envelope: IpcSchemaProjectionLimits,
    baseline: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize>,
) {
    assert!(!baseline.is_empty());
    assert!(
        baseline
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Encode && *units <= 256)
    );
    for at in positions {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(encode_single_field_schema(field, envelope, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
}
fn assert_ordinary_error(
    field: &Field,
    envelope: IpcSchemaProjectionLimits,
    message: &'static str,
) {
    let control = Control::recording();
    assert!(
        matches!(encode_single_field_schema(field, envelope, &control), Err(TypeCodecError::InvalidShape(actual)) if actual == message)
    );
    let baseline = control.trace();
    assert_eq!(baseline.first(), Some(&(CompilePhase::Encode, 0)));
    assert!(
        baseline.len() >= 2,
        "ordinary error omitted its completion observation"
    );
    assert_refusals(field, envelope, &baseline, 0..baseline.len());
}

#[test]
fn explicit_field_type_string_and_primary_backing_envelopes_have_near_over_boundaries() {
    // Independently counted source: root "r", item "c", two Fields and two
    // types (List and Int64), exactly two string bytes and no metadata.
    let field = Field::new(
        "r",
        DataType::List(Arc::new(Field::new("c", DataType::Int64, false))),
        true,
    );
    let exact = IpcSchemaProjectionLimits {
        max_field_occurrences: 2,
        max_type_occurrences: 2,
        max_string_bytes: 2,
        max_flatbuffer_bytes: 64 * 1024,
    };
    let bytes = encode_single_field_schema(&field, exact, &Control::recording()).unwrap();
    assert!(arrow_fields_exact(&field, &decoded_field(&bytes)));
    assert_ordinary_error(
        &field,
        IpcSchemaProjectionLimits {
            max_field_occurrences: 1,
            ..exact
        },
        "IPC schema field envelope exceeded",
    );
    assert_ordinary_error(
        &field,
        IpcSchemaProjectionLimits {
            max_type_occurrences: 1,
            ..exact
        },
        "IPC schema type envelope exceeded",
    );
    assert_ordinary_error(
        &field,
        IpcSchemaProjectionLimits {
            max_string_bytes: 1,
            ..exact
        },
        "IPC schema string envelope exceeded",
    );

    // The source model covers primary builder backing, not only final length.
    // Discover the public admission boundary without reproducing its formula.
    let mut low = 0;
    let mut high = exact.max_flatbuffer_bytes;
    while low < high {
        let middle = low + (high - low) / 2;
        match encode_single_field_schema(
            &field,
            IpcSchemaProjectionLimits {
                max_flatbuffer_bytes: middle,
                ..exact
            },
            &Control::recording(),
        ) {
            Ok(_) => high = middle,
            Err(TypeCodecError::InvalidShape("IPC schema FlatBuffer envelope exceeded")) => {
                low = middle + 1
            }
            Err(error) => panic!("unexpected source refusal: {error}"),
        }
    }
    assert!(low >= bytes.len());
    assert!(
        encode_single_field_schema(
            &field,
            IpcSchemaProjectionLimits {
                max_flatbuffer_bytes: low,
                ..exact
            },
            &Control::recording()
        )
        .is_ok()
    );
    assert_ordinary_error(
        &field,
        IpcSchemaProjectionLimits {
            max_flatbuffer_bytes: low - 1,
            ..exact
        },
        "IPC schema FlatBuffer envelope exceeded",
    );
}

#[test]
fn invalid_carrier_grammar_and_bare_nested_dictionary_refuse_with_observed_tails() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    for ty in [
        DataType::FixedSizeBinary(-1),
        DataType::FixedSizeList(item.clone(), -1),
        DataType::Time32(TimeUnit::Nanosecond),
        DataType::Decimal128(0, 0),
        DataType::Dictionary(Box::new(DataType::Float64), Box::new(DataType::Utf8)),
        DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int8, false)), item),
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Int64, false)),
            false,
        ),
    ] {
        let field = Field::new("invalid_source", ty, true);
        let control = Control::recording();
        assert!(matches!(
            encode_single_field_schema(&field, limits(), &control),
            Err(TypeCodecError::Carrier(_))
        ));
        let baseline = control.trace();
        assert!(baseline.len() >= 2);
        assert_refusals(&field, limits(), &baseline, 0..baseline.len());
    }
    let field = dictionary_field(
        "bare_nested",
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::Utf8),
            )),
        ),
        0,
        false,
    );
    assert_ordinary_error(
        &field,
        limits(),
        "direct nested dictionary has no inner IPC Field identity",
    );
}

#[test]
fn actual_success_callbacks_and_wide_metadata_quantums_keep_all_primary_control_prefixes() {
    let small = Field::new(
        "observed_source",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
    .with_metadata(HashMap::from([("provider_id".to_owned(), "71".to_owned())]));
    let control = Control::recording();
    let output = encode_single_field_schema(&small, limits(), &control).unwrap();
    assert!(arrow_fields_exact(&small, &decoded_field(&output)));
    let baseline = control.trace();
    assert_refusals(&small, limits(), &baseline, 0..baseline.len());

    let common = "x".repeat(320);
    let metadata: HashMap<String, String> = (0..8)
        .map(|ordinal| (format!("{common}{ordinal:02}"), format!("value_{ordinal}")))
        .collect();
    let wide = Field::new("wide_metadata", DataType::Utf8, true).with_metadata(metadata);
    let control = Control::recording();
    let output = encode_single_field_schema(&wide, limits(), &control).unwrap();
    assert!(arrow_fields_exact(&wide, &decoded_field(&output)));
    let baseline = control.trace();
    let mut positions: Vec<usize> = baseline
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| (*units == 256).then_some(at))
        .collect();
    assert!(
        !positions.is_empty(),
        "actual long-key comparisons must reach a quantum"
    );
    positions.extend([0, baseline.len() - 2, baseline.len() - 1]);
    positions.sort_unstable();
    positions.dedup();
    // The small fixture covers every opaque zero callback. The wide fixture
    // covers every actual positive quantum, entry, output exit and final tail.
    assert_refusals(&wide, limits(), &baseline, positions);
}

#[path = "owned_tests.rs"]
mod owned_tests;
