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
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueField, ValueKind, ValueLogicalType,
    ValueTypeRef, ValueTypeStorageHold, ValueUnionField,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wire::arrow_physical_type::Kind;
struct Control {
    calls: AtomicUsize,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            stop: None,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((stop, error)) = self.stop.clone()
            && ordinal == stop
        {
            return Err(error);
        }
        Ok(())
    }
}
fn limits() -> ExecutionTypeTableLimits {
    ExecutionTypeTableLimits {
        logical: LogicalTypeTableLimits {
            max_definitions: 4096,
            max_expanded_nodes: 4096 * 4096,
            max_string_bytes: 1024 * 1024,
        },
        max_bindings: 1024,
        max_layout_nodes: 4096 * 1024,
        max_layout_string_bytes: 1024 * 1024,
    }
}
fn value(kind: ValueKind, nullable: bool) -> ValueTypeRef {
    ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::pure()).unwrap()
}
fn named(name: &str, kind: ValueKind, nullable: bool) -> ValueField {
    ValueField::new(
        name,
        value(kind, nullable),
        ValueTypeStorageLifetime::pure(),
    )
}
fn fixture() -> FunctionValueType {
    let json = named("json", ValueKind::Nominal(ValueLogicalType::Json), true);
    let strings = named(
        "strings",
        ValueKind::List(value(ValueKind::String, true)),
        false,
    );
    let map = named(
        "map",
        ValueKind::Map {
            key: value(ValueKind::String, false),
            value: value(ValueKind::Nominal(ValueLogicalType::Hll), true),
        },
        false,
    );
    let union = named(
        "union",
        ValueKind::Union(
            vec![
                ValueUnionField {
                    type_id: 7,
                    field: named(
                        "decimal",
                        ValueKind::Scalar(DataType::Decimal256(76, -3)),
                        false,
                    ),
                },
                ValueUnionField {
                    type_id: 11,
                    field: named(
                        "time",
                        ValueKind::Scalar(DataType::Timestamp(
                            TimeUnit::Millisecond,
                            Some("UTC".into()),
                        )),
                        false,
                    ),
                },
            ]
            .into_boxed_slice(),
        ),
        false,
    );
    let semantic = value(
        ValueKind::Struct(vec![json, strings, map, union].into_boxed_slice()),
        true,
    );
    let data_type = DataType::Struct(
        vec![
            Arc::new(Field::new("json", DataType::Utf8, true)),
            Arc::new(Field::new(
                "strings",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8View, true))),
                false,
            )),
            Arc::new(Field::new(
                "map",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Utf8, false)),
                                Arc::new(Field::new("value", DataType::Binary, true)),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    true,
                ),
                false,
            )),
            Arc::new(Field::new(
                "union",
                DataType::Union(
                    vec![
                        (
                            7,
                            Arc::new(Field::new("decimal", DataType::Decimal256(76, -3), false)),
                        ),
                        (
                            11,
                            Arc::new(Field::new(
                                "time",
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                                false,
                            )),
                        ),
                    ]
                    .into_iter()
                    .collect::<UnionFields>(),
                    UnionMode::Dense,
                ),
                false,
            )),
        ]
        .into(),
    );
    FunctionValueType::try_from_value(
        semantic,
        ValueLayout::try_new(&data_type, ValueTypeStorageLifetime::pure()).unwrap(),
    )
    .unwrap()
}
fn encode(source: &FunctionValueType, id: u32) -> wire::ExecutionTypeTable {
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let out = ExecutionTypeTableBuilder::new(&[ExecutionTypeRoot { id, value: source }])
        .encode_observed(
            limits(),
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap();
    work.flush().unwrap();
    out.into_wire()
}
fn decode(table: &wire::ExecutionTypeTable) -> Result<DecodedExecutionTypeTable, Failure<()>> {
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let result = decode_execution_type_table_observed(
        table,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
        &mut work,
    );
    work.flush().unwrap();
    result
}
fn struct_fields(table: &mut wire::ExecutionTypeTable) -> &mut Vec<wire::ArrowPhysicalField> {
    let Some(Kind::StructType(value)) = table.value_bindings[0]
        .value_layout
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
    else {
        panic!("Struct fixture")
    };
    &mut value.fields
}
#[test]
fn recursive_semantics_exact_layout_and_sparse_zero_scope_roundtrip() {
    let source = fixture();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let roots = [
        ExecutionTypeRoot {
            id: u32::MAX,
            value: &source,
        },
        ExecutionTypeRoot {
            id: 0,
            value: &source,
        },
    ];
    let owner = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed(
            limits(),
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap();
    assert_eq!(
        owner
            .wire()
            .value_bindings
            .iter()
            .map(|v| v.id.unwrap())
            .collect::<Vec<_>>(),
        vec![0, u32::MAX]
    );
    // Binding container order is not semantic identity or a hidden ID source.
    let reversed = [
        ExecutionTypeRoot {
            id: 0,
            value: &source,
        },
        ExecutionTypeRoot {
            id: u32::MAX,
            value: &source,
        },
    ];
    let reordered = ExecutionTypeTableBuilder::new(&reversed)
        .encode_observed(
            limits(),
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut work,
        )
        .unwrap();
    assert_eq!(owner.wire(), reordered.wire());
    let reference = owner.type_reference_observed(0, &mut work).unwrap();
    assert_eq!(reference.value_binding_id, Some(0));
    let decoded = decode(owner.wire()).unwrap();
    let actual = decoded
        .type_reference_observed(&reference, &mut work)
        .unwrap();
    assert_eq!(actual, &source);
    assert!(
        decoded
            .type_reference_observed(
                &common::TypeDesc {
                    value_binding_id: Some(3)
                },
                &mut work
            )
            .is_err()
    );
    assert!(
        decoded
            .type_reference_observed(
                &common::TypeDesc {
                    value_binding_id: None
                },
                &mut work
            )
            .is_err()
    );
    work.flush().unwrap();
}
#[test]
fn map_entries_has_no_synthetic_semantics_and_roles_are_strict() {
    let original = encode(&fixture(), 0);
    let mut table = original.clone();
    let Kind::Map(map) = struct_fields(&mut table)[2]
        .r#type
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
        .unwrap()
    else {
        panic!("Map")
    };
    let entries = map.entries.as_mut().unwrap();
    assert_eq!(entries.logical_type_id, None);
    assert!(!entries.nullable);
    let Some(Kind::StructType(fields)) = entries.r#type.as_ref().unwrap().kind.as_ref() else {
        panic!("entries")
    };
    assert!(fields.fields.iter().all(|f| f.logical_type_id.is_some()));
    entries.logical_type_id = Some(0);
    assert!(decode(&table).is_err());
    let mut table = original.clone();
    let Kind::Map(map) = struct_fields(&mut table)[2]
        .r#type
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
        .unwrap()
    else {
        panic!("Map")
    };
    map.entries.as_mut().unwrap().nullable = true;
    assert!(decode(&table).is_err());
    let mut table = original;
    let Kind::LargeList(field) = struct_fields(&mut table)[1]
        .r#type
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
        .unwrap()
    else {
        panic!("List")
    };
    field.name = "arbitrary".into();
    assert!(decode(&table).is_err());
}
#[test]
fn every_binding_and_definition_is_validated_and_ref_forgery_refused() {
    let original = encode(&fixture(), 0);
    let mut table = original.clone();
    let mut unused = table.value_bindings[0].clone();
    unused.id = Some(123);
    unused.logical_type_id = None;
    table.value_bindings.push(unused);
    assert!(decode(&table).is_err());
    let mut table = original.clone();
    let mut unused = table.logical_types[0].clone();
    unused.id = Some(u32::MAX);
    unused.kind = None;
    table.logical_types.push(unused);
    assert!(decode(&table).is_err());
    // Retired metadata/encoding fields are structurally absent; the protocol
    // descriptor contract asserts their reserved tags and names.
    let mut table = original.clone();
    let wrong = struct_fields(&mut table)[1].logical_type_id;
    struct_fields(&mut table)[0].logical_type_id = wrong;
    assert!(decode(&table).is_err());
    let mut table = original.clone();
    table.value_bindings.push(table.value_bindings[0].clone());
    assert!(decode(&table).is_err());
    let mut table = original;
    struct_fields(&mut table)[0].r#type.as_mut().unwrap().kind =
        Some(Kind::Primitive(wire::ArrowPrimitiveType::Int32 as i32));
    assert!(decode(&table).is_err());
}
#[test]
fn accurate_offsets_parameters_and_union_mode_survive() {
    let layouts = [
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
        DataType::Date32,
        DataType::Date64,
        DataType::Duration(TimeUnit::Microsecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(16),
        DataType::Decimal32(9, -8),
        DataType::Decimal64(18, 18),
        DataType::Decimal128(38, -128),
        DataType::Decimal256(76, 76),
        DataType::Time32(TimeUnit::Second),
        DataType::Time64(TimeUnit::Nanosecond),
    ];
    for layout in layouts {
        let source = FunctionValueType::new(layout, false);
        let table = encode(&source, 0);
        let decoded = decode(&table).unwrap();
        assert_eq!(decoded.values[0].1, source);
    }
    let item = Arc::new(Field::new("item", DataType::LargeUtf8, true));
    for layout in [
        DataType::List(item.clone()),
        DataType::LargeList(item.clone()),
        DataType::ListView(item.clone()),
        DataType::LargeListView(item.clone()),
        DataType::FixedSizeList(item, 3),
    ] {
        let source = FunctionValueType::new(layout, false);
        assert_eq!(decode(&encode(&source, 0)).unwrap().values[0].1, source);
    }
    let source = fixture();
    let mut table = encode(&source, 0);
    let Kind::UnionType(union) = struct_fields(&mut table)[3]
        .r#type
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
        .unwrap()
    else {
        panic!("Union")
    };
    union.mode = wire::ArrowUnionMode::Sparse as i32;
    let sparse = decode(&table).unwrap();
    assert_ne!(sparse.values[0].1, source);
    assert_eq!(sparse.values[0].1.value_type(), source.value_type());
}
#[derive(Debug, PartialEq)]
struct Host(usize);
#[test]
fn every_actual_request_refusal_preserves_cause_and_stops_publication() {
    let source = fixture();
    let table = encode(&source, 0);
    for encode_case in [true, false] {
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut requests = 0;
        let mut admit = |_: Layout| {
            requests += 1;
            Ok::<_, AdmissionRefusal<Host>>(())
        };
        if encode_case {
            ExecutionTypeTableBuilder::new(&[ExecutionTypeRoot {
                id: 0,
                value: &source,
            }])
            .encode_observed(limits(), &mut admit, &mut work)
            .unwrap();
        } else {
            decode_execution_type_table_observed(
                &table,
                limits(),
                ValueTypeStorageLifetime::pure(),
                &mut admit,
                &mut work,
            )
            .unwrap();
        }
        work.flush().unwrap();
        for stop in 1..=requests {
            let control = Control::good();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut calls = 0;
            let mut admit = |_: Layout| {
                calls += 1;
                if calls == stop {
                    Err(AdmissionRefusal::Host(Host(stop)))
                } else {
                    Ok(())
                }
            };
            let error = if encode_case {
                ExecutionTypeTableBuilder::new(&[ExecutionTypeRoot {
                    id: 0,
                    value: &source,
                }])
                .encode_observed(limits(), &mut admit, &mut work)
                .err()
                .unwrap()
            } else {
                decode_execution_type_table_observed(
                    &table,
                    limits(),
                    ValueTypeStorageLifetime::pure(),
                    &mut admit,
                    &mut work,
                )
                .err()
                .unwrap()
            };
            assert!(matches!(error,ProjectionFailure::Host(Host(n)) if n==stop));
            assert_eq!(calls, stop);
        }
    }
}
#[test]
fn observer_control_refusals_and_retained_child_last_exit() {
    let table = encode(&fixture(), 0);
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    decode_execution_type_table_observed(
        &table,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
        &mut work,
    )
    .unwrap();
    work.flush().unwrap();
    let checkpoints = control.calls.load(Ordering::SeqCst);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 2..checkpoints {
            let control = Control {
                calls: AtomicUsize::new(0),
                stop: Some((stop, cause.clone())),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let error = decode_execution_type_table_observed(
                &table,
                limits(),
                ValueTypeStorageLifetime::pure(),
                &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
                &mut work,
            )
            .err()
            .unwrap();
            assert!(
                matches!(error,ProjectionFailure::Codec(LogicalTypeCodecError::Control(e)) if e==cause)
            );
            assert_eq!(control.calls.load(Ordering::SeqCst), stop);
        }
    }
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(dropped.clone())));
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let decoded = decode_execution_type_table_observed(
        &table,
        limits(),
        storage,
        &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
        &mut work,
    )
    .unwrap();
    let binding = decoded.values[0].1.clone();
    let child = binding.value_type().child_type_at(0).unwrap().clone();
    let layout = binding.typed_binding().layout().clone();
    drop(decoded);
    drop(binding);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(child);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(layout);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    work.flush().unwrap();
}

#[test]
fn retained_source_invoice_is_constructive_for_pure_scalar_without_a_grant() {
    let source = FunctionValueType::new(DataType::Int64, false);
    let decoded = decode(&encode(&source, 0)).unwrap();
    let expected = Layout::array::<(u32, FunctionValueType)>(decoded.values.capacity())
        .unwrap()
        .size()
        + ValueTypeRef::storage_allocation_layout().unwrap().size()
        + ValueLayout::storage_allocation_layout().unwrap().size();
    assert_eq!(decoded.retained_source_bytes_upper_bound(), expected);
    assert!(
        expected > 0,
        "a pure numeric source fact is not an absent grant"
    );
    assert_eq!(decoded.values[0].1, source);
}

#[test]
fn retained_source_invoice_ignores_valid_unused_nodes_but_keeps_their_validation() {
    let source = FunctionValueType::new(DataType::Int64, false);
    let mut table = encode(&source, 0);
    let baseline = decode(&table).unwrap().retained_source_bytes_upper_bound();
    let timezone = FunctionValueType::new(
        DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(1024).into())),
        true,
    );
    let mut unused = encode(&timezone, 0).logical_types.remove(0);
    unused.id = Some(u32::MAX);
    table.logical_types.push(unused);
    let mut requested = 0usize;
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let actual = decode_execution_type_table_observed(
        &table,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut |layout| {
            requested += layout.size();
            Ok::<_, AdmissionRefusal<()>>(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(actual.retained_source_bytes_upper_bound(), baseline);
    assert!(
        requested > baseline,
        "temporary graph and unused construction do not become live-source backing"
    );
    table.logical_types.last_mut().unwrap().kind = None;
    assert!(matches!(
        decode(&table),
        Err(ProjectionFailure::Codec(
            LogicalTypeCodecError::InvalidShape("missing logical type kind")
        ))
    ));
}

#[test]
fn repeated_bindings_count_one_shared_logical_node_and_each_real_layout_container() {
    let source = FunctionValueType::new(DataType::Int64, false);
    let mut table = encode(&source, 0);
    let baseline = decode(&table).unwrap();
    let mut second = table.value_bindings[0].clone();
    second.id = Some(u32::MAX);
    table.value_bindings.push(second);
    let actual = decode(&table).unwrap();
    let container_delta = Layout::array::<(u32, FunctionValueType)>(actual.values.capacity())
        .unwrap()
        .size()
        - Layout::array::<(u32, FunctionValueType)>(baseline.values.capacity())
            .unwrap()
            .size();
    assert_eq!(
        actual.retained_source_bytes_upper_bound() - baseline.retained_source_bytes_upper_bound(),
        container_delta + ValueLayout::storage_allocation_layout().unwrap().size()
    );
    assert_eq!(
        actual.values[0].1.value_type(),
        actual.values[1].1.value_type()
    );
}

fn nested_map_layout(logical_depth: usize) -> DataType {
    let mut value = DataType::Int64;
    for _ in 1..logical_depth {
        value = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", value, false)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
    }
    value
}

#[test]
fn logical_depth_64_map_layout_keeps_all_physical_occurrences() {
    let source = FunctionValueType::new(nested_map_layout(64), false);
    let table = encode(&source, 0);
    assert_eq!(decode(&table).unwrap().values[0].1, source);
    // 63 Maps, 63 entries Structs, 63 keys and one terminal value.
    // Removing entries from logical depth must not remove their node charge.
    let roots = [ExecutionTypeRoot {
        id: 0,
        value: &source,
    }];
    let mut caps = limits();
    caps.max_layout_nodes = 63 * 3 + 1;
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    ExecutionTypeTableBuilder::new(&roots)
        .encode_observed(caps, &mut |_| Ok::<_, AdmissionRefusal<()>>(()), &mut work)
        .unwrap();
    caps.max_layout_nodes -= 1;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(
        ExecutionTypeTableBuilder::new(&roots)
            .encode_observed(caps, &mut |_| Ok::<_, AdmissionRefusal<()>>(()), &mut work)
            .is_err()
    );
}
