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
use arrow::datatypes::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
use arrow::ipc;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_type_contract::{CompileControlError, arrow_fields_exact};
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
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            fail: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            fail: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.fail {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.fail.clone() {
            Some((refusal, cause)) if at == refusal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn envelope() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 1024 * 1024,
        max_flatbuffer_bytes: 4 * 1024 * 1024,
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 64,
        max_tables: 16_384,
        max_apparent_size: 4 * 1024 * 1024,
        ignore_missing_null_terminator: false,
    }
}
fn verify(bytes: &[u8], expected: &Field, control: &Control) -> Result<(), TypeCodecError> {
    verify_single_field_schema_message(bytes, expected, envelope(), &verifier(), control)
}
fn assert_rejection(bytes: &[u8], expected: &Field) {
    let control = Control::good();
    let error = verify(bytes, expected, &control).unwrap_err();
    assert!(
        !matches!(error, TypeCodecError::Control(_)),
        "ordinary malformed input is not cancellation"
    );
    let baseline = control.trace();
    assert!(
        baseline.len() >= 2,
        "ordinary refusal omitted completed tail"
    );
    assert_prefixes(
        bytes,
        expected,
        envelope(),
        &verifier(),
        &baseline,
        0..baseline.len(),
    );
}
fn assert_prefixes(
    bytes: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    options: &VerifierOptions,
    baseline: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize>,
) {
    for at in positions {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(verify_single_field_schema_message(bytes, expected, limits, options, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
}

#[derive(Clone, Copy)]
struct Profile {
    header: ipc::MessageHeader,
    version: ipc::MetadataVersion,
    body: i64,
    endian: ipc::Endianness,
    schema_metadata: bool,
    message_metadata: bool,
    features: bool,
}
impl Profile {
    fn ordinary() -> Self {
        Self {
            header: ipc::MessageHeader::Schema,
            version: ipc::MetadataVersion::V5,
            body: 0,
            endian: ipc::Endianness::Little,
            schema_metadata: false,
            message_metadata: false,
            features: false,
        }
    }
}
fn kv<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    key: Option<&str>,
    value: Option<&str>,
) -> WIPOffset<ipc::KeyValue<'a>> {
    let key = key.map(|key| builder.create_string(key));
    let value = value.map(|value| builder.create_string(value));
    ipc::KeyValue::create(builder, &ipc::KeyValueArgs { key, value })
}
fn field<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    name: &str,
    nullable: bool,
    kind: ipc::Type,
    payload: Option<WIPOffset<flatbuffers::UnionWIPOffset>>,
    children: &[WIPOffset<ipc::Field<'a>>],
    metadata: &[(Option<&str>, Option<&str>)],
    dictionary: Option<WIPOffset<ipc::DictionaryEncoding<'a>>>,
) -> WIPOffset<ipc::Field<'a>> {
    let name = builder.create_string(name);
    let children = builder.create_vector(children);
    let metadata: Vec<_> = metadata
        .iter()
        .map(|(key, value)| kv(builder, *key, *value))
        .collect();
    let metadata = (!metadata.is_empty()).then(|| builder.create_vector(&metadata));
    ipc::Field::create(
        builder,
        &ipc::FieldArgs {
            name: Some(name),
            nullable,
            type_type: kind,
            type_: payload,
            dictionary,
            children: Some(children),
            custom_metadata: metadata,
        },
    )
}
fn integer_field<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    name: &str,
    nullable: bool,
    metadata: &[(Option<&str>, Option<&str>)],
) -> WIPOffset<ipc::Field<'a>> {
    let value = ipc::Int::create(
        builder,
        &ipc::IntArgs {
            bitWidth: 64,
            is_signed: true,
        },
    );
    field(
        builder,
        name,
        nullable,
        ipc::Type::Int,
        Some(value.as_union_value()),
        &[],
        metadata,
        None,
    )
}
fn finish<'a>(
    mut builder: FlatBufferBuilder<'a>,
    fields: &[WIPOffset<ipc::Field<'a>>],
    profile: Profile,
) -> Vec<u8> {
    let fields = builder.create_vector(fields);
    let pair = kv(&mut builder, Some("unexpected"), Some("metadata"));
    let pairs = builder.create_vector(&[pair]);
    let flags = builder.create_vector(&[ipc::Feature::COMPRESSED_BODY]);
    let schema = ipc::Schema::create(
        &mut builder,
        &ipc::SchemaArgs {
            endianness: profile.endian,
            fields: Some(fields),
            custom_metadata: profile.schema_metadata.then_some(pairs),
            features: profile.features.then_some(flags),
        },
    );
    let message = ipc::Message::create(
        &mut builder,
        &ipc::MessageArgs {
            version: profile.version,
            header_type: profile.header,
            header: Some(schema.as_union_value()),
            bodyLength: profile.body,
            custom_metadata: profile.message_metadata.then_some(pairs),
        },
    );
    ipc::finish_message_buffer(&mut builder, message);
    builder.finished_data().to_vec()
}

#[test]
fn actual_encoder_messages_verify_complete_fields_and_exact_timezone_presence() {
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
            let expected = Field::new("source", DataType::Timestamp(unit, zone), true)
                .with_metadata(HashMap::from([("provider_id".to_owned(), "17".to_owned())]));
            let bytes =
                encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
            verify(&bytes, &expected, &Control::good()).unwrap();
            // Only successfully verified metadata enters Arrow schema conversion.
            let message = ipc::root_as_message(&bytes).unwrap();
            let actual =
                ipc::convert::try_fb_to_schema(message.header_as_schema().unwrap()).unwrap();
            assert!(arrow_fields_exact(&expected, actual.field(0)));
        }
    }
    let expected = Field::new(
        "source",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    );
    let different = Field::new(
        "source",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        true,
    );
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    assert_rejection(&bytes, &different);
    let bytes = encode_single_field_schema(
        &Field::new("source", DataType::Float64, true),
        envelope(),
        &Control::good(),
    )
    .unwrap();
    assert_rejection(&bytes, &Field::new("source", DataType::Int64, true));
}

#[test]
#[allow(deprecated)]
fn dictionary_and_nested_nominal_fields_verify_without_carrier_retagging() {
    let inner = Arc::new(Field::new_dict(
        "encoded",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        true,
        -99,
        true,
    ));
    let json = Arc::new(
        Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([
            (
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                "json".to_owned(),
            ),
            ("provider_id".to_owned(), "7".to_owned()),
        ])),
    );
    let expected = Field::new(
        "source",
        DataType::Struct(vec![inner.clone(), json.clone()].into()),
        false,
    );
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    verify(&bytes, &expected, &Control::good()).unwrap();
    let changed = Arc::new(Field::new_dict(
        "encoded",
        inner.data_type().clone(),
        true,
        0,
        true,
    ));
    assert_rejection(
        &bytes,
        &Field::new(
            "source",
            DataType::Struct(vec![changed, json].into()),
            false,
        ),
    );
    for id in [0, -99, i64::MAX] {
        let expected = Field::new_dict("source", inner.data_type().clone(), true, id, false);
        let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
        verify(&bytes, &expected, &Control::good()).unwrap();
        let wrong_order = Field::new_dict("source", inner.data_type().clone(), true, id, true);
        assert_rejection(&bytes, &wrong_order);
    }
}

#[test]
fn independent_schema_root_profiles_and_truncated_metadata_are_rejected() {
    let expected = Field::new("source", DataType::Int64, true);
    let normal = Profile::ordinary();
    for profile in [
        Profile {
            header: ipc::MessageHeader(255),
            ..normal
        },
        Profile {
            header: ipc::MessageHeader::RecordBatch,
            ..normal
        },
        Profile {
            version: ipc::MetadataVersion(99),
            ..normal
        },
        Profile {
            version: ipc::MetadataVersion::V4,
            ..normal
        },
        Profile { body: -1, ..normal },
        Profile { body: 1, ..normal },
        Profile {
            endian: ipc::Endianness::Big,
            ..normal
        },
        Profile {
            endian: ipc::Endianness(99),
            ..normal
        },
        Profile {
            schema_metadata: true,
            ..normal
        },
        Profile {
            message_metadata: true,
            ..normal
        },
        Profile {
            features: true,
            ..normal
        },
    ] {
        let mut builder = FlatBufferBuilder::new();
        let root = integer_field(&mut builder, "source", true, &[]);
        let bytes = finish(builder, &[root], profile);
        assert_rejection(&bytes, &expected);
    }
    for count in [0, 2] {
        let mut builder = FlatBufferBuilder::new();
        let root = integer_field(&mut builder, "source", true, &[]);
        let roots = vec![root; count];
        assert_rejection(&finish(builder, &roots, normal), &expected);
    }
    for missing_name in [true, false] {
        let mut builder = FlatBufferBuilder::new();
        let name = builder.create_string("source");
        let payload = ipc::Int::create(
            &mut builder,
            &ipc::IntArgs {
                bitWidth: 64,
                is_signed: true,
            },
        );
        let root = ipc::Field::create(
            &mut builder,
            &ipc::FieldArgs {
                name: (!missing_name).then_some(name),
                nullable: true,
                type_type: ipc::Type::Int,
                type_: Some(payload.as_union_value()),
                dictionary: None,
                children: None,
                custom_metadata: None,
            },
        );
        let fields = builder.create_vector(&[root]);
        let schema = ipc::Schema::create(
            &mut builder,
            &ipc::SchemaArgs {
                endianness: ipc::Endianness::Little,
                fields: missing_name.then_some(fields),
                custom_metadata: None,
                features: None,
            },
        );
        let message = ipc::Message::create(
            &mut builder,
            &ipc::MessageArgs {
                version: ipc::MetadataVersion::V5,
                header_type: ipc::MessageHeader::Schema,
                header: Some(schema.as_union_value()),
                bodyLength: 0,
                custom_metadata: None,
            },
        );
        ipc::finish_message_buffer(&mut builder, message);
        assert_rejection(builder.finished_data(), &expected);
    }
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    for truncated in [&bytes[..0], &bytes[..3]] {
        assert_rejection(truncated, &expected);
    }
}

#[test]
fn independent_unknown_scalar_enums_missing_payload_and_extra_children_refuse() {
    // Expected carriers match each raw family so rejection must not rely on
    // comparing every malformed type against an unrelated Int64 carrier.
    for case in 0..10 {
        let mut builder = FlatBufferBuilder::new();
        let mut children = Vec::new();
        let (kind, payload, expected) = match case {
            0 => {
                let value = ipc::Int::create(
                    &mut builder,
                    &ipc::IntArgs {
                        bitWidth: 64,
                        is_signed: true,
                    },
                );
                (
                    ipc::Type(255),
                    Some(value.as_union_value()),
                    DataType::Int64,
                )
            }
            1 => (ipc::Type::Int, None, DataType::Int64),
            2 => {
                let value = ipc::FloatingPoint::create(
                    &mut builder,
                    &ipc::FloatingPointArgs {
                        precision: ipc::Precision(99),
                    },
                );
                (
                    ipc::Type::FloatingPoint,
                    Some(value.as_union_value()),
                    DataType::Float64,
                )
            }
            3 => {
                let value = ipc::Date::create(
                    &mut builder,
                    &ipc::DateArgs {
                        unit: ipc::DateUnit(99),
                    },
                );
                (
                    ipc::Type::Date,
                    Some(value.as_union_value()),
                    DataType::Date32,
                )
            }
            4 => {
                let value = ipc::Time::create(
                    &mut builder,
                    &ipc::TimeArgs {
                        unit: ipc::TimeUnit(99),
                        bitWidth: 64,
                    },
                );
                (
                    ipc::Type::Time,
                    Some(value.as_union_value()),
                    DataType::Time64(TimeUnit::Microsecond),
                )
            }
            5 => {
                let value = ipc::Time::create(
                    &mut builder,
                    &ipc::TimeArgs {
                        unit: ipc::TimeUnit::MICROSECOND,
                        bitWidth: 17,
                    },
                );
                (
                    ipc::Type::Time,
                    Some(value.as_union_value()),
                    DataType::Time64(TimeUnit::Microsecond),
                )
            }
            6 => {
                let value = ipc::Timestamp::create(
                    &mut builder,
                    &ipc::TimestampArgs {
                        unit: ipc::TimeUnit(99),
                        timezone: None,
                    },
                );
                (
                    ipc::Type::Timestamp,
                    Some(value.as_union_value()),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                )
            }
            7 => {
                let value = ipc::Interval::create(
                    &mut builder,
                    &ipc::IntervalArgs {
                        unit: ipc::IntervalUnit(99),
                    },
                );
                (
                    ipc::Type::Interval,
                    Some(value.as_union_value()),
                    DataType::Interval(IntervalUnit::MonthDayNano),
                )
            }
            8 => {
                let value = ipc::Int::create(
                    &mut builder,
                    &ipc::IntArgs {
                        bitWidth: 7,
                        is_signed: true,
                    },
                );
                (
                    ipc::Type::Int,
                    Some(value.as_union_value()),
                    DataType::Int64,
                )
            }
            _ => {
                children.push(integer_field(&mut builder, "unexpected_child", true, &[]));
                let value = ipc::Int::create(
                    &mut builder,
                    &ipc::IntArgs {
                        bitWidth: 64,
                        is_signed: true,
                    },
                );
                (
                    ipc::Type::Int,
                    Some(value.as_union_value()),
                    DataType::Int64,
                )
            }
        };
        let root = field(
            &mut builder,
            "source",
            true,
            kind,
            payload,
            &children,
            &[],
            None,
        );
        assert_rejection(
            &finish(builder, &[root], Profile::ordinary()),
            &Field::new("source", expected, true),
        );
    }
}

#[test]
fn independent_list_and_union_children_and_tags_are_checked_before_conversion() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let mut builder = FlatBufferBuilder::new();
    let list = ipc::List::create(&mut builder, &ipc::ListArgs {});
    let root = field(
        &mut builder,
        "source",
        true,
        ipc::Type::List,
        Some(list.as_union_value()),
        &[],
        &[],
        None,
    );
    assert_rejection(
        &finish(builder, &[root], Profile::ordinary()),
        &Field::new("source", DataType::List(item), true),
    );

    let children = [
        Arc::new(Field::new("a", DataType::Int64, true)),
        Arc::new(Field::new("b", DataType::Int64, true)),
    ];
    let fields: UnionFields = [(7, children[0].clone()), (41, children[1].clone())]
        .into_iter()
        .collect();
    let expected = Field::new("source", DataType::Union(fields, UnionMode::Dense), true);
    for tags in [
        None,
        Some(vec![7]),
        Some(vec![7, 7]),
        Some(vec![41, 7]),
        Some(vec![7, 128]),
    ] {
        let mut builder = FlatBufferBuilder::new();
        let a = integer_field(&mut builder, "a", true, &[]);
        let b = integer_field(&mut builder, "b", true, &[]);
        let ids = tags.as_ref().map(|tags| builder.create_vector(tags));
        let union = ipc::Union::create(
            &mut builder,
            &ipc::UnionArgs {
                mode: ipc::UnionMode::Dense,
                typeIds: ids,
            },
        );
        let root = field(
            &mut builder,
            "source",
            true,
            ipc::Type::Union,
            Some(union.as_union_value()),
            &[a, b],
            &[],
            None,
        );
        assert_rejection(&finish(builder, &[root], Profile::ordinary()), &expected);
    }
}

#[test]
#[allow(deprecated)]
fn independent_dictionary_kind_key_id_and_order_refuse_without_defaulting() {
    let expected = Field::new_dict(
        "source",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        -99,
        true,
    );
    for case in 0..5 {
        let mut builder = FlatBufferBuilder::new();
        let index = (case != 1).then(|| {
            ipc::Int::create(
                &mut builder,
                &ipc::IntArgs {
                    bitWidth: 8,
                    is_signed: case != 2,
                },
            )
        });
        let dictionary = ipc::DictionaryEncoding::create(
            &mut builder,
            &ipc::DictionaryEncodingArgs {
                id: if case == 3 { 0 } else { -99 },
                indexType: index,
                isOrdered: case != 4,
                dictionaryKind: if case == 0 {
                    ipc::DictionaryKind(99)
                } else {
                    ipc::DictionaryKind::DenseArray
                },
            },
        );
        let value = ipc::Utf8::create(&mut builder, &ipc::Utf8Args {});
        let root = field(
            &mut builder,
            "source",
            true,
            ipc::Type::Utf8,
            Some(value.as_union_value()),
            &[],
            &[],
            Some(dictionary),
        );
        assert_rejection(&finish(builder, &[root], Profile::ordinary()), &expected);
    }
}

#[test]
fn independent_field_metadata_is_unordered_but_complete_unique_and_exact() {
    let expected = Field::new("source", DataType::Int64, true).with_metadata(HashMap::from([
        ("a".to_owned(), "1".to_owned()),
        ("b".to_owned(), "2".to_owned()),
    ]));
    let mut builder = FlatBufferBuilder::new();
    let root = integer_field(
        &mut builder,
        "source",
        true,
        &[(Some("b"), Some("2")), (Some("a"), Some("1"))],
    );
    let bytes = finish(builder, &[root], Profile::ordinary());
    verify(&bytes, &expected, &Control::good()).unwrap();
    let message = ipc::root_as_message(&bytes).unwrap();
    assert!(arrow_fields_exact(
        &expected,
        ipc::convert::try_fb_to_schema(message.header_as_schema().unwrap())
            .unwrap()
            .field(0)
    ));
    for pairs in [
        vec![(None, Some("1")), (Some("b"), Some("2"))],
        vec![(Some("a"), None), (Some("b"), Some("2"))],
        vec![(Some("a"), Some("1")), (Some("a"), Some("1"))],
        vec![(Some("a"), Some("99")), (Some("b"), Some("2"))],
        vec![(Some("a"), Some("1")), (Some("c"), Some("2"))],
    ] {
        let mut builder = FlatBufferBuilder::new();
        let root = integer_field(&mut builder, "source", true, &pairs);
        assert_rejection(&finish(builder, &[root], Profile::ordinary()), &expected);
    }
    for (name, nullable) in [("wrong_source", true), ("source", false)] {
        let mut builder = FlatBufferBuilder::new();
        let root = integer_field(
            &mut builder,
            name,
            nullable,
            &[(Some("a"), Some("1")), (Some("b"), Some("2"))],
        );
        assert_rejection(&finish(builder, &[root], Profile::ordinary()), &expected);
    }
}

#[test]
fn verifier_projection_envelopes_and_every_success_callback_preserve_primary_control() {
    let expected = Field::new("source", DataType::Int64, true);
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    let control = Control::good();
    verify(&bytes, &expected, &control).unwrap();
    let baseline = control.trace();
    assert_prefixes(
        &bytes,
        &expected,
        envelope(),
        &verifier(),
        &baseline,
        0..baseline.len(),
    );
    for options in [
        VerifierOptions {
            max_depth: 0,
            ..verifier()
        },
        VerifierOptions {
            max_tables: 0,
            ..verifier()
        },
        VerifierOptions {
            max_apparent_size: 0,
            ..verifier()
        },
    ] {
        let control = Control::good();
        let error =
            verify_single_field_schema_message(&bytes, &expected, envelope(), &options, &control)
                .unwrap_err();
        assert!(!matches!(error, TypeCodecError::Control(_)));
        let baseline = control.trace();
        assert!(baseline.len() >= 2);
        assert_prefixes(
            &bytes,
            &expected,
            envelope(),
            &options,
            &baseline,
            0..baseline.len(),
        );
    }
    for limits in [
        IpcSchemaProjectionLimits {
            max_field_occurrences: 0,
            ..envelope()
        },
        IpcSchemaProjectionLimits {
            max_type_occurrences: 0,
            ..envelope()
        },
        IpcSchemaProjectionLimits {
            max_string_bytes: 5,
            ..envelope()
        },
        IpcSchemaProjectionLimits {
            max_flatbuffer_bytes: bytes.len() - 1,
            ..envelope()
        },
    ] {
        let control = Control::good();
        assert!(
            verify_single_field_schema_message(&bytes, &expected, limits, &verifier(), &control)
                .is_err()
        );
        let baseline = control.trace();
        assert!(baseline.len() >= 2);
        assert_prefixes(
            &bytes,
            &expected,
            limits,
            &verifier(),
            &baseline,
            0..baseline.len(),
        );
    }
}

#[test]
fn wide_real_field_validation_reaches_quantum_with_no_callback_after_primary_refusal() {
    let fields: Vec<_> = (0..320)
        .map(|ordinal| {
            Arc::new(
                Field::new(
                    format!("column_{ordinal}"),
                    DataType::Int64,
                    ordinal % 2 == 0,
                )
                .with_metadata(HashMap::from([(
                    "provider_id".to_owned(),
                    ordinal.to_string(),
                )])),
            )
        })
        .collect();
    let expected = Field::new("source", DataType::Struct(fields.into()), true);
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    let control = Control::good();
    verify(&bytes, &expected, &control).unwrap();
    let baseline = control.trace();
    let mut positions: Vec<_> = baseline
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| (*units == 256).then_some(at))
        .collect();
    assert!(!positions.is_empty());
    positions.extend([0, baseline.len() - 2, baseline.len() - 1]);
    positions.sort_unstable();
    positions.dedup();
    assert_prefixes(
        &bytes,
        &expected,
        envelope(),
        &verifier(),
        &baseline,
        positions,
    );
}

#[test]
fn apparent_size_overflow_envelopes_refuse_with_every_primary_control_prefix() {
    let expected = Field::new("source", DataType::Int64, true);
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    assert!(!bytes.is_empty());
    for max_apparent_size in [usize::MAX, usize::MAX - bytes.len() + 1] {
        let options = VerifierOptions {
            max_apparent_size,
            ..verifier()
        };
        let control = Control::good();
        assert!(matches!(
            verify_single_field_schema_message(&bytes, &expected, envelope(), &options, &control),
            Err(TypeCodecError::InvalidShape(_))
        ));
        let baseline = control.trace();
        assert!(
            baseline.len() >= 2,
            "ordinary envelope refusal omitted its tail"
        );
        assert_prefixes(
            &bytes,
            &expected,
            envelope(),
            &options,
            &baseline,
            0..baseline.len(),
        );
    }
}

#[test]
fn accepted_type_depth_64_uses_actual_schema_table_depth_67() {
    // Type depth starts at one: 63 List nodes plus the Int64 leaf are 64.
    // Message, Schema, and the root Field add three table levels; the leaf
    // Int payload therefore needs verifier depth 67, not 66.
    let mut ty = DataType::Int64;
    for _ in 0..63 {
        ty = DataType::List(Arc::new(Field::new("item", ty, true)));
    }
    let expected = Field::new("source", ty, true);
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    let options = VerifierOptions {
        max_depth: 67,
        ..verifier()
    };
    verify_single_field_schema_message(&bytes, &expected, envelope(), &options, &Control::good())
        .unwrap();
    let message = ipc::root_as_message_with_opts(&options, &bytes).unwrap();
    let schema = message.header_as_schema().unwrap();
    let mut selected = schema.fields().unwrap().get(0);
    assert_eq!(selected.name(), Some("source"));
    for _ in 0..63 {
        assert_eq!(selected.type_type(), ipc::Type::List);
        assert!(selected.type_as_list().is_some());
        let children = selected.children().unwrap();
        assert_eq!(children.len(), 1);
        selected = children.get(0);
        assert_eq!(selected.name(), Some("item"));
    }
    assert_eq!(selected.type_type(), ipc::Type::Int);
    let leaf = selected.type_as_int().unwrap();
    assert_eq!(leaf.bitWidth(), 64);
    assert!(leaf.is_signed());
    let decoded = ipc::convert::try_fb_to_schema(schema).unwrap();
    assert!(arrow_fields_exact(&expected, decoded.field(0)));

    let too_shallow = VerifierOptions {
        max_depth: 66,
        ..verifier()
    };
    assert!(matches!(
        ipc::root_as_message_with_opts(&too_shallow, &bytes),
        Err(flatbuffers::InvalidFlatbuffer::DepthLimitReached)
    ));
    let control = Control::good();
    assert!(matches!(
        verify_single_field_schema_message(&bytes, &expected, envelope(), &too_shallow, &control),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let baseline = control.trace();
    assert_prefixes(
        &bytes,
        &expected,
        envelope(),
        &too_shallow,
        &baseline,
        [0, baseline.len() - 1],
    );
}

#[test]
fn excessive_verifier_depth_and_independent_deep_metadata_refuse_before_conversion() {
    let expected = Field::new("source", DataType::Int64, true);
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    let supported = VerifierOptions {
        max_depth: 67,
        ..verifier()
    };
    verify_single_field_schema_message(&bytes, &expected, envelope(), &supported, &Control::good())
        .unwrap();
    for max_depth in [68, usize::MAX] {
        let options = VerifierOptions {
            max_depth,
            ..verifier()
        };
        let control = Control::good();
        assert!(matches!(
            verify_single_field_schema_message(&bytes, &expected, envelope(), &options, &control),
            Err(TypeCodecError::InvalidShape(_))
        ));
        let baseline = control.trace();
        assert!(baseline.len() >= 2);
        assert_prefixes(
            &bytes,
            &expected,
            envelope(),
            &options,
            &baseline,
            0..baseline.len(),
        );
    }

    // Author an oversized raw schema with the generated FlatBuffer builders,
    // independently of the checked encoder. The expected source stays shallow.
    let mut builder = FlatBufferBuilder::new();
    let mut child = integer_field(&mut builder, "item", true, &[]);
    for level in 0..96 {
        let payload = ipc::List::create(&mut builder, &ipc::ListArgs {});
        child = field(
            &mut builder,
            if level == 95 { "source" } else { "item" },
            true,
            ipc::Type::List,
            Some(payload.as_union_value()),
            &[child],
            &[],
            None,
        );
    }
    let deep = finish(builder, &[child], Profile::ordinary());
    assert!(matches!(
        ipc::root_as_message_with_opts(&supported, &deep),
        Err(flatbuffers::InvalidFlatbuffer::DepthLimitReached)
    ));
    let control = Control::good();
    assert!(matches!(
        verify_single_field_schema_message(&deep, &expected, envelope(), &supported, &control),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let baseline = control.trace();
    assert!(baseline.len() >= 2);
    assert_prefixes(
        &deep,
        &expected,
        envelope(),
        &supported,
        &baseline,
        0..baseline.len(),
    );
    // Intentionally do not call try_fb_to_schema on rejected raw metadata.
}

#[test]
fn present_empty_schema_message_metadata_and_features_have_no_properties() {
    let expected = Field::new("source", DataType::Int64, true);
    let mut builder = FlatBufferBuilder::new();
    let root = integer_field(&mut builder, "source", true, &[]);
    let fields = builder.create_vector(&[root]);
    let metadata = builder.create_vector::<WIPOffset<ipc::KeyValue<'_>>>(&[]);
    let features = builder.create_vector::<ipc::Feature>(&[]);
    let schema = ipc::Schema::create(
        &mut builder,
        &ipc::SchemaArgs {
            endianness: ipc::Endianness::Little,
            fields: Some(fields),
            custom_metadata: Some(metadata),
            features: Some(features),
        },
    );
    let message = ipc::Message::create(
        &mut builder,
        &ipc::MessageArgs {
            version: ipc::MetadataVersion::V5,
            header_type: ipc::MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            bodyLength: 0,
            custom_metadata: Some(metadata),
        },
    );
    ipc::finish_message_buffer(&mut builder, message);
    let bytes = builder.finished_data();
    let control = Control::good();
    verify(bytes, &expected, &control).unwrap();
    let message = ipc::root_as_message(bytes).unwrap();
    assert_eq!(message.custom_metadata().unwrap().len(), 0);
    let schema = message.header_as_schema().unwrap();
    assert_eq!(schema.custom_metadata().unwrap().len(), 0);
    assert_eq!(schema.features().unwrap().len(), 0);
    let actual = ipc::convert::try_fb_to_schema(schema).unwrap();
    assert!(arrow_fields_exact(&expected, actual.field(0)));
    let baseline = control.trace();
    assert_prefixes(
        bytes,
        &expected,
        envelope(),
        &verifier(),
        &baseline,
        0..baseline.len(),
    );
}

#[test]
fn decode_metadata_byte_cap_does_not_require_encoder_backing_reservation() {
    let expected = Field::new("s", DataType::Int32, true);
    let bytes = encode_single_field_schema(&expected, envelope(), &Control::good()).unwrap();
    let exact = IpcSchemaProjectionLimits {
        max_flatbuffer_bytes: bytes.len(),
        ..envelope()
    };
    // The writer needs a conservative builder backing cover larger than its
    // finished message. Decode has no builder and admits the actual extent.
    assert!(matches!(
        encode_single_field_schema(&expected, exact, &Control::good()),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let control = Control::good();
    verify_single_field_schema_message(&bytes, &expected, exact, &verifier(), &control).unwrap();
    let baseline = control.trace();
    assert_prefixes(
        &bytes,
        &expected,
        exact,
        &verifier(),
        &baseline,
        0..baseline.len(),
    );
    let over = IpcSchemaProjectionLimits {
        max_flatbuffer_bytes: bytes.len() - 1,
        ..exact
    };
    let control = Control::good();
    assert!(matches!(
        verify_single_field_schema_message(&bytes, &expected, over, &verifier(), &control),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let baseline = control.trace();
    assert_prefixes(
        &bytes,
        &expected,
        over,
        &verifier(),
        &baseline,
        0..baseline.len(),
    );
}
