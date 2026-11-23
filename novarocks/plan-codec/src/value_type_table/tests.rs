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

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use novarocks_type_contract::{
    CompilePhase, PureCompileControl, ValueField, ValueKind, ValueLogicalType,
    ValueTypeStorageHold, ValueUnionField,
};

use super::*;
use wire::logical_type_definition::Kind;

#[derive(Debug, Eq, PartialEq)]
enum HostCause {
    Refused(usize),
}
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            stop: None,
        }
    }
    fn refusing(stop: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            stop: Some((stop, cause)),
        }
    }
    fn calls(&self) -> usize {
        self.trace.lock().unwrap().len()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if let Some((stop, cause)) = self.stop.clone()
            && trace.len() == stop
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn limits() -> LogicalTypeTableLimits {
    LogicalTypeTableLimits {
        max_definitions: 1024,
        max_expanded_nodes: 1024 * 4096,
        max_string_bytes: 1024 * 1024,
    }
}
fn value(kind: ValueKind, nullable: bool) -> ValueTypeRef {
    ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::pure()).unwrap()
}
fn named(name: &str, kind: ValueKind) -> ValueField {
    ValueField::new(name, value(kind, true), ValueTypeStorageLifetime::pure())
}
fn fixture() -> ValueTypeRef {
    value(
        ValueKind::Struct(
            vec![
                named(
                    "json_list",
                    ValueKind::List(value(ValueKind::Nominal(ValueLogicalType::Json), true)),
                ),
                named(
                    "mapping",
                    ValueKind::Map {
                        key: value(ValueKind::Nominal(ValueLogicalType::Uuid), false),
                        value: value(ValueKind::Nominal(ValueLogicalType::Variant), true),
                    },
                ),
                named(
                    "choice",
                    ValueKind::Union(
                        vec![
                            ValueUnionField {
                                type_id: 2,
                                field: named(
                                    "large",
                                    ValueKind::Nominal(ValueLogicalType::LargeInt),
                                ),
                            },
                            ValueUnionField {
                                type_id: 7,
                                field: named("bytes", ValueKind::Binary),
                            },
                        ]
                        .into(),
                    ),
                ),
                named(
                    "instant",
                    ValueKind::Scalar(DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("Asia/Shanghai".into()),
                    )),
                ),
            ]
            .into(),
        ),
        true,
    )
}
fn encode(
    roots: &[LogicalTypeRoot<'_>],
    control: &Control,
    request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<HostCause>>,
) -> Result<Vec<wire::LogicalTypeDefinition>, Failure<HostCause>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = LogicalTypeTableBuilder::new(roots).encode_observed(limits(), request, &mut work);
    match result {
        Ok(output) => {
            work.finish()?;
            Ok(output)
        }
        Err(error @ (Failure::Host(_) | Failure::Codec(LogicalTypeCodecError::Control(_)))) => {
            Err(error)
        }
        Err(error) => {
            work.finish()?;
            Err(error)
        }
    }
}
fn decode(
    definitions: &[wire::LogicalTypeDefinition],
    storage: ValueTypeStorageLifetime,
    control: &Control,
    request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<HostCause>>,
) -> Result<DecodedLogicalTypeTable, Failure<HostCause>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result =
        decode_logical_type_table_observed(definitions, limits(), storage, request, &mut work);
    match result {
        Ok(output) => {
            work.finish()?;
            Ok(output)
        }
        Err(error @ (Failure::Host(_) | Failure::Codec(LogicalTypeCodecError::Control(_)))) => {
            Err(error)
        }
        Err(error) => {
            work.finish()?;
            Err(error)
        }
    }
}
fn allow(_: Layout) -> Result<(), AdmissionRefusal<HostCause>> {
    Ok(())
}
fn definition(id: Option<u32>, nullable: bool, kind: Kind) -> wire::LogicalTypeDefinition {
    wire::LogicalTypeDefinition {
        id,
        nullable,
        kind: Some(kind),
    }
}
fn scalar(kind: wire::ScalarKind) -> Kind {
    Kind::Scalar(wire::ScalarValue {
        kind: kind as i32,
        ..Default::default()
    })
}

#[test]
fn bounded_character_logical_types_keep_width_under_recursive_declarations() {
    use novarocks_type_contract::{CharacterKind, CharacterType};

    for (kind, length) in [(CharacterKind::Char, 0), (CharacterKind::Varchar, 65_537)] {
        let item = value(
            ValueKind::BoundedCharacter(CharacterType::new(kind, length).unwrap()),
            true,
        );
        let root = value(ValueKind::List(item), false);
        let definitions = encode(
            &[LogicalTypeRoot {
                id: 0,
                value: &root,
            }],
            &Control::good(),
            &mut allow,
        )
        .unwrap();
        let decoded = decode(
            &definitions,
            ValueTypeStorageLifetime::pure(),
            &Control::good(),
            &mut allow,
        )
        .unwrap();
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        assert_eq!(decoded.get_observed(0, &mut work).unwrap(), Some(&root));
    }
}

#[test]
fn recursive_domains_named_order_and_sparse_zero_root_ids_round_trip() {
    let root = fixture();
    let other = value(ValueKind::String, false);
    let roots = [
        LogicalTypeRoot {
            id: u32::MAX,
            value: &root,
        },
        LogicalTypeRoot {
            id: 0,
            value: &other,
        },
    ];
    let mut allocations = Vec::new();
    let definitions = encode(&roots, &Control::good(), &mut |layout| {
        allocations.push(layout);
        Ok(())
    })
    .unwrap();
    assert!(definitions.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert_eq!(definitions.first().unwrap().id, Some(0));
    assert_eq!(definitions.last().unwrap().id, Some(u32::MAX));
    assert_eq!(definitions.len(), root.expanded_nodes() + 1);
    assert!(!allocations.is_empty());
    let decoded = decode(
        &definitions,
        ValueTypeStorageLifetime::pure(),
        &Control::good(),
        &mut allow,
    )
    .unwrap();
    assert_eq!(decoded.len(), definitions.len());
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert_eq!(
        decoded.get_observed(u32::MAX, &mut work).unwrap(),
        Some(&root)
    );
    assert_eq!(decoded.get_observed(0, &mut work).unwrap(), Some(&other));
    assert!(
        decoded
            .get_observed(u32::MAX - 1, &mut work)
            .unwrap()
            .is_none()
    );
    work.finish().unwrap();
    assert!(
        definitions
            .iter()
            .any(|definition| matches!(definition.kind.as_ref(), Some(Kind::MapValue(_))))
    );
}

#[test]
fn all_existing_scalar_parameters_and_nominal_domains_round_trip() {
    let data_types = [
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
        DataType::FixedSizeBinary(16),
        DataType::Date32,
        DataType::Date64,
        DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Duration(TimeUnit::Second),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Decimal32(9, -128),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 20),
        DataType::Decimal256(76, 70),
    ];
    let mut values: Vec<_> = data_types
        .into_iter()
        .map(|ty| value(ValueKind::Scalar(ty), true))
        .collect();
    values.extend([
        value(ValueKind::String, true),
        value(ValueKind::Binary, false),
        value(
            ValueKind::FixedSizeList {
                item: value(ValueKind::String, false),
                length: 0,
            },
            true,
        ),
    ]);
    values.extend(
        [
            ValueLogicalType::Json,
            ValueLogicalType::Variant,
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::Object,
            ValueLogicalType::Percentile,
            ValueLogicalType::LargeInt,
            ValueLogicalType::Uuid,
        ]
        .into_iter()
        .map(|domain| value(ValueKind::Nominal(domain), true)),
    );
    let roots: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(id, value)| LogicalTypeRoot {
            id: id as u32 * 7,
            value,
        })
        .collect();
    let definitions = encode(&roots, &Control::good(), &mut allow).unwrap();
    let decoded = decode(
        &definitions,
        ValueTypeStorageLifetime::pure(),
        &Control::good(),
        &mut allow,
    )
    .unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    for root in roots {
        assert_eq!(
            decoded.get_observed(root.id, &mut work).unwrap(),
            Some(root.value)
        );
    }
    work.finish().unwrap();
}

#[test]
fn every_definition_is_checked_even_when_it_has_no_referenced_root() {
    let valid = definition(Some(0), true, scalar(wire::ScalarKind::String));
    let mut cases = vec![
        vec![valid.clone(), valid.clone()],
        vec![
            valid.clone(),
            definition(None, true, scalar(wire::ScalarKind::String)),
        ],
        vec![
            valid.clone(),
            definition(Some(9), true, Kind::ListItemTypeId(10)),
        ],
        vec![
            valid.clone(),
            definition(Some(9), true, Kind::ListItemTypeId(9)),
        ],
        vec![
            valid.clone(),
            definition(Some(9), true, Kind::ListItemTypeId(10)),
            definition(Some(10), true, Kind::ListItemTypeId(9)),
        ],
        vec![
            valid.clone(),
            definition(
                Some(9),
                true,
                Kind::FixedSizeList(wire::FixedSizeListValue {
                    item_type_id: Some(0),
                    length: -1,
                }),
            ),
        ],
        vec![
            valid.clone(),
            definition(
                Some(9),
                true,
                Kind::MapValue(wire::MapValue {
                    key_type_id: Some(0),
                    value_type_id: Some(0),
                }),
            ),
        ],
    ];
    for malformed in [
        wire::ScalarValue {
            kind: wire::ScalarKind::Char as i32,
            ..Default::default()
        },
        wire::ScalarValue {
            kind: wire::ScalarKind::Char as i32,
            character_length: Some(65_537),
            ..Default::default()
        },
        wire::ScalarValue {
            kind: wire::ScalarKind::String as i32,
            character_length: Some(0),
            ..Default::default()
        },
        wire::ScalarValue {
            kind: wire::ScalarKind::String as i32,
            precision: Some(1),
            ..Default::default()
        },
        wire::ScalarValue {
            kind: wire::ScalarKind::Decimal128 as i32,
            precision: Some(39),
            scale: Some(0),
            ..Default::default()
        },
        wire::ScalarValue {
            kind: wire::ScalarKind::Timestamp as i32,
            ..Default::default()
        },
        wire::ScalarValue {
            kind: wire::ScalarKind::Time32 as i32,
            time_unit: Some(wire::TimeUnit::Nanosecond as i32),
            ..Default::default()
        },
    ] {
        cases.push(vec![
            valid.clone(),
            definition(Some(9), true, Kind::Scalar(malformed)),
        ]);
    }
    for case in cases {
        assert!(matches!(
            decode(
                &case,
                ValueTypeStorageLifetime::pure(),
                &Control::good(),
                &mut allow
            ),
            Err(Failure::Codec(LogicalTypeCodecError::InvalidShape(_)))
        ));
    }
    let root = fixture();
    assert!(matches!(
        encode(
            &[
                LogicalTypeRoot {
                    id: 0,
                    value: &root
                },
                LogicalTypeRoot {
                    id: 0,
                    value: &root
                }
            ],
            &Control::good(),
            &mut allow
        ),
        Err(Failure::Codec(LogicalTypeCodecError::InvalidShape(
            "duplicate logical root identity"
        )))
    ));
}

#[test]
fn actual_graph_unfolding_preserves_depth_and_expanded_node_bounds() {
    let mut definitions = vec![definition(Some(0), true, scalar(wire::ScalarKind::String))];
    for id in 1..=13 {
        definitions.push(definition(
            Some(id),
            true,
            Kind::StructValue(wire::StructValue {
                fields: vec![
                    wire::NamedValueField {
                        name: "a".into(),
                        value_type_id: Some(id - 1),
                    },
                    wire::NamedValueField {
                        name: "b".into(),
                        value_type_id: Some(id - 1),
                    },
                ],
            }),
        ));
    }
    assert!(matches!(
        decode(
            &definitions,
            ValueTypeStorageLifetime::pure(),
            &Control::good(),
            &mut allow
        ),
        Err(Failure::Codec(LogicalTypeCodecError::ValueType(
            ExecutionValueTypeError::Nodes
        )))
    ));
    let chain: Vec<_> = (0..=64)
        .map(|id| {
            definition(
                Some(id),
                true,
                if id == 64 {
                    scalar(wire::ScalarKind::String)
                } else {
                    Kind::ListItemTypeId(id + 1)
                },
            )
        })
        .collect();
    assert!(matches!(
        decode(
            &chain,
            ValueTypeStorageLifetime::pure(),
            &Control::good(),
            &mut allow
        ),
        Err(Failure::Codec(LogicalTypeCodecError::ValueType(
            ExecutionValueTypeError::Depth
        )))
    ));
}

#[test]
fn sparse_id_magnitude_does_not_change_allocation_requests() {
    let measure = |id| {
        let mut requests = Vec::new();
        decode(
            &[definition(
                Some(id),
                false,
                scalar(wire::ScalarKind::String),
            )],
            ValueTypeStorageLifetime::pure(),
            &Control::good(),
            &mut |layout| {
                requests.push((layout.size(), layout.align()));
                Ok(())
            },
        )
        .unwrap();
        requests
    };
    assert_eq!(measure(0), measure(u32::MAX));
}

#[test]
fn every_original_control_prefix_preserves_encode_and_decode_primary_causes() {
    let root = fixture();
    let roots = [LogicalTypeRoot {
        id: 0,
        value: &root,
    }];
    let control = Control::good();
    let definitions = encode(&roots, &control, &mut allow).unwrap();
    let encode_calls = control.calls();
    let control = Control::good();
    decode(
        &definitions,
        ValueTypeStorageLifetime::pure(),
        &control,
        &mut allow,
    )
    .unwrap();
    let decode_calls = control.calls();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 1..=encode_calls {
            let control = Control::refusing(stop, cause.clone());
            assert!(
                matches!(encode(&roots, &control, &mut allow), Err(Failure::Codec(LogicalTypeCodecError::Control(actual))) if actual == cause)
            );
            assert_eq!(control.calls(), stop);
        }
        for stop in 1..=decode_calls {
            let control = Control::refusing(stop, cause.clone());
            assert!(
                matches!(decode(&definitions, ValueTypeStorageLifetime::pure(), &control, &mut allow), Err(Failure::Codec(LogicalTypeCodecError::Control(actual))) if actual == cause)
            );
            assert_eq!(control.calls(), stop);
        }
    }
}

#[test]
fn each_request_refusal_keeps_its_host_payload_without_later_callbacks() {
    let root = fixture();
    let roots = [LogicalTypeRoot {
        id: 0,
        value: &root,
    }];
    let mut total = 0;
    let definitions = encode(&roots, &Control::good(), &mut |_| {
        total += 1;
        Ok(())
    })
    .unwrap();
    let encode_requests = total;
    total = 0;
    decode(
        &definitions,
        ValueTypeStorageLifetime::pure(),
        &Control::good(),
        &mut |_| {
            total += 1;
            Ok(())
        },
    )
    .unwrap();
    let decode_requests = total;
    for (decoding, total) in [(false, encode_requests), (true, decode_requests)] {
        for stop in 1..=total {
            let control = Control::good();
            let mut requests = 0;
            let mut at_refusal = 0;
            let mut reject = |_: Layout| {
                requests += 1;
                if requests == stop {
                    at_refusal = control.calls();
                    Err(AdmissionRefusal::Host(HostCause::Refused(stop)))
                } else {
                    Ok(())
                }
            };
            let result = if decoding {
                decode(
                    &definitions,
                    ValueTypeStorageLifetime::pure(),
                    &control,
                    &mut reject,
                )
                .map(|_| ())
            } else {
                encode(&roots, &control, &mut reject).map(|_| ())
            };
            assert!(
                matches!(result, Err(Failure::Host(HostCause::Refused(actual))) if actual == stop)
            );
            assert_eq!(requests, stop);
            assert_eq!(control.calls(), at_refusal);
        }
    }
}

struct Hold(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn independently_retained_decoded_child_keeps_storage_until_actual_exit() {
    let root = value(
        ValueKind::List(value(ValueKind::Nominal(ValueLogicalType::Json), true)),
        true,
    );
    let definitions = encode(
        &[LogicalTypeRoot {
            id: 0,
            value: &root,
        }],
        &Control::good(),
        &mut allow,
    )
    .unwrap();
    let exits = Arc::new(AtomicUsize::new(0));
    let decoded = decode(
        &definitions,
        ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone()))),
        &Control::good(),
        &mut allow,
    )
    .unwrap();
    let child = {
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let root = decoded.get_observed(0, &mut work).unwrap().unwrap();
        let ValueKind::List(child) = root.kind() else {
            panic!("expected List");
        };
        child.clone()
    };
    drop(decoded);
    drop(definitions);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert_eq!(child.kind(), &ValueKind::Nominal(ValueLogicalType::Json));
    drop(child);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}
