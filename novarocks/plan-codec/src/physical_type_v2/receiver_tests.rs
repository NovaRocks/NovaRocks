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
use arrow::datatypes::TimeUnit;
use novarocks_connector_contract::ConnectorErrorKind;
use novarocks_proto_models::{physical_package_v2 as raw, plan};
use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
use std::sync::Mutex;
use wire::carrier_type_definition::Kind;

const SOURCE: usize = 128 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push(units);
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(c: &Control) -> Vec<u32> {
    c.events.lock().unwrap().clone()
}
fn limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 20000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 500000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn carrier(id: u32, kind: Kind) -> wire::CarrierTypeDefinition {
    wire::CarrierTypeDefinition {
        id,
        kind: Some(kind),
    }
}
fn primitive(id: u32, ty: plan::ArrowPrimitiveType) -> wire::CarrierTypeDefinition {
    carrier(id, Kind::Primitive(ty as i32))
}
fn field(id: u32, name: &str, carrier: u32) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: name.into(),
        nullable: false,
        carrier_type_id: Some(carrier),
        metadata: vec![],
        dictionary_id: None,
        dictionary_is_ordered: None,
    }
}
fn binding(id: u32) -> raw::ConnectorWriteFieldBinding {
    raw::ConnectorWriteFieldBinding {
        field_id: Some(id),
        field_token: vec![],
    }
}
fn data(ids: impl IntoIterator<Item = u32>) -> raw::FrozenWriterRecipe {
    raw::FrozenWriterRecipe {
        input: Some(raw::ConnectorWriteInputShape {
            kind: Some(raw::connector_write_input_shape::Kind::Data(
                raw::ConnectorWriteDataInput {
                    fields: ids.into_iter().map(binding).collect(),
                },
            )),
        }),
        ..Default::default()
    }
}
fn package(table: wire::TypeTable) -> raw::FragmentPackage {
    raw::FragmentPackage {
        types: Some(table),
        writes: vec![data([u32::MAX])],
        ..Default::default()
    }
}
fn scalar() -> raw::FragmentPackage {
    package(wire::TypeTable {
        carriers: vec![primitive(0, plan::ArrowPrimitiveType::Int64)],
        fields: vec![field(u32::MAX, "root", 0)],
        value_types: vec![],
    })
}
fn wide(n: usize) -> raw::FragmentPackage {
    let mut p = package(wire::TypeTable {
        carriers: vec![
            primitive(0, plan::ArrowPrimitiveType::Int64),
            carrier(
                u32::MAX,
                Kind::StructType(wire::StructFields {
                    field_ids: (0..n as u32).collect(),
                }),
            ),
        ],
        fields: (0..n as u32)
            .map(|n| field(n, &format!("c{n}"), 0))
            .chain([field(u32::MAX, "root", u32::MAX)])
            .collect(),
        value_types: vec![],
    });
    p.types.as_mut().unwrap().fields.last_mut().unwrap().name = "根\0schema".into();
    p
}
fn chain(depth: u32, writer: bool) -> raw::FragmentPackage {
    let mut table = wire::TypeTable {
        carriers: vec![primitive(0, plan::ArrowPrimitiveType::Int64)],
        fields: vec![],
        value_types: vec![],
    };
    for n in 1..depth {
        table.carriers.push(carrier(n, Kind::ListFieldId(n)));
        table.fields.push(field(n, "item", n - 1));
    }
    table.fields.push(field(u32::MAX, "root", depth - 1));
    let mut p = package(table);
    if !writer {
        p.writes.clear();
        p.types
            .as_mut()
            .unwrap()
            .value_types
            .push(wire::ValueTypeDefinition {
                id: u32::MAX,
                carrier_type_id: Some(depth - 1),
                nullable: false,
                logical_type: 1,
            });
    }
    p
}
fn axes(f: PackageTypeProjectionFacts) -> [usize; 7] {
    [
        f.definition_count,
        f.expanded_node_count,
        f.string_bytes,
        f.allocation_requests_upper_bound,
        f.allocation_request_bytes_upper_bound,
        f.coexisting_source_and_request_bytes_upper_bound,
        f.cumulative_work_upper_bound,
    ]
}
fn exact_limits(f: PackageTypeProjectionFacts) -> PackageTypeProjectionLimits {
    let a = axes(f);
    PackageTypeProjectionLimits {
        max_definitions: a[0],
        max_expanded_nodes: a[1],
        max_string_bytes: a[2],
        max_allocation_requests: a[3],
        max_allocation_request_bytes: a[4],
        max_coexisting_source_and_request_bytes: a[5],
        max_work: a[6],
    }
}
fn run(
    package: &raw::FragmentPackage,
    caps: PackageTypeProjectionLimits,
    pending: usize,
    c: &Control,
) -> Result<(DecodedTypeTable, PackageTypeProjectionFacts), TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    for _ in 0..pending {
        work.step()?;
    }
    let mut last = None;
    let result = decode_package_type_table_observed(
        package,
        SOURCE,
        caps,
        &mut |facts| {
            last = Some(*facts);
            Ok(())
        },
        &mut work,
    );
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result.map(|table| (table, last.unwrap()));
    }
    work.finish()?;
    result.map(|table| (table, last.expect("actual receiving admission facts")))
}
fn writer_resource(error: TypeCodecError) {
    assert!(
        matches!(error, TypeCodecError::Writer(error) if error.kind() == ConnectorErrorKind::ResourceExhausted)
    );
}

#[test]
#[allow(deprecated)]
fn receiver_writer_struct_5000_preserves_sparse_fields_dictionary_and_complete_metadata() {
    let mut p = wide(5000);
    let table = p.types.as_mut().unwrap();
    table.carriers.extend([
        carrier(
            6,
            Kind::Dictionary(wire::DictionaryTypes {
                key_type_id: Some(7),
                value_type_id: Some(8),
            }),
        ),
        primitive(7, plan::ArrowPrimitiveType::Int16),
        primitive(8, plan::ArrowPrimitiveType::Utf8),
    ]);
    table.fields[0].carrier_type_id = Some(6);
    table.fields[0].name = "字典".into();
    table.fields[0].dictionary_id = Some(-99);
    table.fields[0].dictionary_is_ordered = Some(true);
    table.fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "source".into(),
        value: "雪\0original".into(),
    }];
    let (decoded, facts) = run(&p, limits(), 0, &Control::default()).unwrap();
    let root = decoded.field(u32::MAX).unwrap();
    assert_eq!(root.name(), "根\0schema");
    let DataType::Struct(children) = root.data_type() else {
        panic!("actual Struct")
    };
    assert_eq!(children.len(), 5000);
    for (ordinal, child) in children.iter().enumerate() {
        assert!(Arc::ptr_eq(child, decoded.field(ordinal as u32).unwrap()));
        assert_eq!(
            child.name(),
            &if ordinal == 0 {
                "字典".into()
            } else {
                format!("c{ordinal}")
            }
        );
    }
    let first = decoded.field(0).unwrap();
    assert_eq!(
        first.data_type(),
        &DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8))
    );
    assert_eq!(first.dict_id(), Some(-99));
    assert_eq!(first.dict_is_ordered(), Some(true));
    assert_eq!(first.metadata().get("source").unwrap(), "雪\0original");
    assert!(!std::ptr::eq(
        first.name().as_str(),
        p.types.as_ref().unwrap().fields[0].name.as_str()
    ));
    assert_eq!(facts.definition_count, 5006);
}

#[test]
fn receiver_writer_only_large_metadata_and_timezone_return_original_arrow_attributes() {
    let mut p = scalar();
    let table = p.types.as_mut().unwrap();
    table.carriers[0].kind = Some(Kind::Timestamp(plan::ArrowTimestampType {
        unit: plan::ArrowTimeUnit::Microsecond as i32,
        timezone: Some("z".repeat(2000)),
    }));
    table.fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "k".into(),
        value: "雪".repeat(7000),
    }];
    let (decoded, _) = run(&p, limits(), 0, &Control::default()).unwrap();
    let f = decoded.field(u32::MAX).unwrap();
    assert_eq!(f.name(), "root");
    assert!(!f.is_nullable());
    assert_eq!(f.metadata().get("k").unwrap(), &"雪".repeat(7000));
    assert_eq!(
        f.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("z".repeat(2000).into()))
    );
}

#[test]
fn receiver_value_schema_ipc_and_unused_strict_roots_reject_same_writer_wide_descendant() {
    for source in 0..4 {
        let mut p = wide(5000);
        match source {
            0 => p
                .types
                .as_mut()
                .unwrap()
                .value_types
                .push(wire::ValueTypeDefinition {
                    id: 0,
                    carrier_type_id: Some(u32::MAX),
                    nullable: false,
                    logical_type: 1,
                }),
            1 => p.schemas.push(raw::SchemaDefinition {
                id: 0,
                field_ids: vec![u32::MAX],
                metadata: vec![],
            }),
            2 => p.constants.push(raw::IpcConstantPool {
                id: 0,
                field_id: Some(u32::MAX),
                ..Default::default()
            }),
            _ => p
                .types
                .as_mut()
                .unwrap()
                .carriers
                .push(carrier(9, Kind::ListFieldId(u32::MAX))),
        }
        assert!(matches!(
            run(&p, limits(), 0, &Control::default()),
            Err(TypeCodecError::ValueType(ValueTypeError::TooManyNodes))
        ));
    }
    let mut p = scalar();
    p.types.as_mut().unwrap().fields[0].metadata = (0..65)
        .map(|i| plan::ArrowFieldMetadataEntry {
            key: format!("k{i:02}"),
            value: "v".into(),
        })
        .collect();
    p.schemas.push(raw::SchemaDefinition {
        id: 0,
        field_ids: vec![u32::MAX],
        metadata: vec![],
    });
    writer_resource(run(&p, limits(), 0, &Control::default()).err().unwrap());
}

#[test]
fn receiver_writer_32_33_and_strict_64_65_keep_original_domain_errors() {
    run(&chain(32, true), limits(), 0, &Control::default()).unwrap();
    writer_resource(
        run(&chain(33, true), limits(), 0, &Control::default())
            .err()
            .unwrap(),
    );
    let (decoded, _) = run(&chain(64, false), limits(), 0, &Control::default()).unwrap();
    assert!(decoded.value_type(u32::MAX).is_some());
    assert!(matches!(
        run(&chain(65, false), limits(), 0, &Control::default()),
        Err(TypeCodecError::ValueType(ValueTypeError::TooDeep))
    ));
}

#[test]
fn receiver_writer_recipe_quota_counts_repeated_occurrences_across_roles_and_resets_per_recipe() {
    let mut p = scalar();
    p.types.as_mut().unwrap().fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "k".into(),
        value: "v".repeat(65536),
    }];
    // Each actual root law charges 128+name4+key1+value65536+String headers48+type64.
    assert!(250 * (128 + 4 + 1 + 65536 + 2 * size_of::<String>() + 64) < 16 * 1024 * 1024);
    p.writes = vec![data(std::iter::repeat_n(u32::MAX, 250))];
    run(&p, limits(), 0, &Control::default()).unwrap();
    p.writes = vec![raw::FrozenWriterRecipe {
        input: Some(raw::ConnectorWriteInputShape {
            kind: Some(raw::connector_write_input_shape::Kind::RowLineage(
                raw::ConnectorWriteRowLineageInput {
                    data_fields: vec![binding(u32::MAX); 128],
                    row_identity_fields: vec![binding(u32::MAX); 128],
                },
            )),
        }),
        ..Default::default()
    }];
    writer_resource(run(&p, limits(), 0, &Control::default()).err().unwrap());
    p.writes = vec![
        data(std::iter::repeat_n(u32::MAX, 128)),
        data(std::iter::repeat_n(u32::MAX, 128)),
    ];
    run(&p, limits(), 0, &Control::default()).unwrap();
    // One field's 512 actual child occurrences also contribute to its real law;
    // the source metadata allocation itself is shared and only 32 KiB.
    p.types.as_mut().unwrap().fields[0].id = 0;
    p.types.as_mut().unwrap().fields[0].name = "c".into();
    p.types.as_mut().unwrap().fields[0].metadata[0].value = "v".repeat(32768);
    p.types.as_mut().unwrap().carriers.push(carrier(
        u32::MAX,
        Kind::StructType(wire::StructFields {
            field_ids: vec![0; 512],
        }),
    ));
    p.types
        .as_mut()
        .unwrap()
        .fields
        .push(field(u32::MAX, "root", u32::MAX));
    p.writes = vec![data([u32::MAX])];
    writer_resource(run(&p, limits(), 0, &Control::default()).err().unwrap());
}

#[test]
fn receiver_writer_logical_metadata_error_precedes_65_entry_resource_law() {
    let mut p = scalar();
    let mut metadata: Vec<_> = (0..64)
        .map(|i| plan::ArrowFieldMetadataEntry {
            key: format!("k{i:02}"),
            value: "v".into(),
        })
        .collect();
    metadata.push(plan::ArrowFieldMetadataEntry {
        key: NR_LOGICAL_TYPE_KEY.into(),
        value: "unknown-original-logical-type".into(),
    });
    p.types.as_mut().unwrap().fields[0].metadata = metadata;
    match run(&p, limits(), 0, &Control::default()).err().unwrap() {
        TypeCodecError::Writer(error) => {
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(error.message(), "unknown logical type metadata");
        }
        error => panic!("original logical error must precede entry quota: {error:?}"),
    }
}

#[test]
fn receiver_closed_source_missing_references_unknown_inputs_and_cycles_refuse() {
    for source in 0..5 {
        let mut p = scalar();
        match source {
            0 => p.types = None,
            1 => p.writes[0].input = None,
            2 => p.writes[0].input.as_mut().unwrap().kind = None,
            3 => p.types.as_mut().unwrap().fields[0].carrier_type_id = None,
            _ => {
                p.types.as_mut().unwrap().carriers[0].kind = Some(Kind::ListFieldId(u32::MAX));
            }
        }
        assert!(matches!(
            run(&p, limits(), 0, &Control::default()),
            Err(TypeCodecError::InvalidShape(_))
        ));
    }
}

#[test]
fn receiver_numeric_envelopes_and_actual_callback_prefixes_keep_original_three_causes() {
    let p = scalar();
    let (_, generous) = run(&p, limits(), 0, &Control::default()).unwrap();
    let exact = exact_limits(generous);
    run(&p, exact, 0, &Control::default()).unwrap();
    for axis in 0..7 {
        let mut a = axes(generous);
        assert!(a[axis] > 0);
        a[axis] -= 1;
        let caps = PackageTypeProjectionLimits {
            max_definitions: a[0],
            max_expanded_nodes: a[1],
            max_string_bytes: a[2],
            max_allocation_requests: a[3],
            max_allocation_request_bytes: a[4],
            max_coexisting_source_and_request_bytes: a[5],
            max_work: a[6],
        };
        let c = Control::default();
        assert!(matches!(
            run(&p, caps, 0, &c),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let prefix = trace(&c);
        for cause in CAUSES {
            let c = Control {
                stop: Some((prefix.len(), cause)),
                ..Control::default()
            };
            assert!(matches!(
                run(&p, caps, 0, &c),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(trace(&c), prefix);
        }
    }
    for pending in [254, 255] {
        for request_cap in [0, generous.allocation_requests_upper_bound - 1] {
            let mut caps = limits();
            caps.max_allocation_requests = request_cap;
            let baseline = Control::default();
            assert!(matches!(
                run(&p, caps, pending, &baseline),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            let prefix = trace(&baseline);
            if request_cap == 0 {
                // Even initial Index/flags/stack requests are already known.
                assert_eq!(prefix, [0]);
            }
            for cause in CAUSES {
                let c = Control {
                    stop: Some((prefix.len(), cause)),
                    ..Control::default()
                };
                assert!(matches!(
                    run(&p, caps, pending, &c),
                    Err(TypeCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(trace(&c), prefix);
            }
        }
    }
    for ordinary in [false, true] {
        let mut p = scalar();
        if ordinary {
            p.writes[0].input = None;
        }
        let c = Control::default();
        assert_eq!(run(&p, limits(), 0, &c).is_err(), ordinary);
        let expected = trace(&c);
        for at in 0..expected.len() {
            for cause in CAUSES {
                let c = Control {
                    stop: Some((at, cause)),
                    ..Control::default()
                };
                assert!(
                    matches!(run(&p, limits(), 0, &c), Err(TypeCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&c), expected[..=at]);
            }
        }
    }
}

#[test]
fn receiver_known_output_headers_refuse_before_graph_source_and_late_control() {
    let mut p = scalar();
    let table = p.types.as_mut().unwrap();
    table.carriers[0].kind = Some(Kind::Timestamp(plan::ArrowTimestampType {
        unit: plan::ArrowTimeUnit::Microsecond as i32,
        timezone: Some("z".repeat(2000)),
    }));
    table.fields[0]
        .metadata
        .push(plan::ArrowFieldMetadataEntry {
            key: "k".into(),
            value: "雪".repeat(7000),
        });
    let expected = table.fields[0].name.len() + 1 + 21000 + 2000;
    let baseline = Control::default();
    let mut work = CompileCheckpoints::try_new(&baseline, CompilePhase::Decode).unwrap();
    let mut known = None;
    let result = decode_package_type_table_observed(
        &p,
        SOURCE,
        limits(),
        &mut |facts| {
            if facts.string_bytes == expected && known.is_none() {
                known = Some((facts.allocation_request_bytes_upper_bound, trace(&baseline)));
            }
            Ok(())
        },
        &mut work,
    );
    assert!(result.is_ok());
    work.finish().unwrap();
    // Actual owned UTF8/timezone facts, while no original graph source step
    // has run. No second implementation of source allocation layout is used.
    assert_eq!(known.unwrap().1, vec![0]);
    for pending in [254, 255] {
        for cause in CAUSES {
            let control = Control {
                stop: Some((1, cause)),
                events: Mutex::new(vec![]),
            };
            let mut cap = limits();
            cap.max_string_bytes = expected - 1;
            assert!(matches!(
                run(&p, cap, pending, &control),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(trace(&control), vec![0]);
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct HostRefusal(u32);
struct RecordingScope {
    calls: usize,
    ran: usize,
    refuse: bool,
    facts: Option<PackageTypeProjectionFacts>,
    entry_events: usize,
}
impl PackageTypeMaterializationScope for RecordingScope {
    type HostError = HostRefusal;
    fn materialize<B>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<
        DecodedTypeTable,
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, HostRefusal>,
    >
    where
        B: FnOnce() -> Result<DecodedTypeTable, TypeCodecError>,
    {
        self.calls += 1;
        self.facts = Some(*facts);
        if self.refuse {
            return Err(crate::host_projection_v2::ProjectionFailure::Host(
                HostRefusal(73),
            ));
        }
        self.ran += 1;
        body().map_err(crate::host_projection_v2::ProjectionFailure::Codec)
    }
}
fn scope(refuse: bool) -> RecordingScope {
    RecordingScope {
        calls: 0,
        ran: 0,
        refuse,
        facts: None,
        entry_events: 0,
    }
}
fn run_host(
    package: &raw::FragmentPackage,
    caps: PackageTypeProjectionLimits,
    c: &Control,
    host: &mut RecordingScope,
) -> Result<
    DecodedTypeTable,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, HostRefusal>,
> {
    use crate::host_projection_v2::ProjectionFailure;
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let mut entry_events = 0;
    let result = decode_package_type_table_with_host_observed(
        package,
        SOURCE,
        caps,
        &mut |_| {
            entry_events = trace(c).len();
            Ok(())
        },
        &mut work,
        host,
    );
    host.entry_events = entry_events;
    if matches!(
        &result,
        Err(ProjectionFailure::Host(_) | ProjectionFailure::Codec(TypeCodecError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn receiver_materialization_host_refusal_keeps_nominal_cause_and_zero_body() {
    use crate::host_projection_v2::ProjectionFailure;
    let p = wide(64);
    let c = Control::default();
    let mut host = scope(true);
    assert!(matches!(
        run_host(&p, limits(), &c, &mut host),
        Err(ProjectionFailure::Host(HostRefusal(73)))
    ));
    assert_eq!((host.calls, host.ran), (1, 0));
    assert!(host.facts.unwrap().allocation_request_bytes_upper_bound > 0);
    // One flush after the final gate, then no body/checkpoint/footer on refusal.
    assert!(trace(&c).len() <= host.entry_events + 1);
}

#[test]
fn receiver_materialization_host_shares_original_outputs_and_control_trace() {
    let p = wide(64);
    let direct = Control::default();
    let (expected, facts) = run(&p, limits(), 0, &direct).unwrap();
    let hosted = Control::default();
    let mut host = scope(false);
    let actual = run_host(&p, limits(), &hosted, &mut host).unwrap();
    assert_eq!((host.calls, host.ran), (1, 1));
    assert_eq!(host.facts, Some(facts));
    assert_eq!(actual.field(u32::MAX), expected.field(u32::MAX));
    assert_eq!(actual.carrier(u32::MAX), expected.carrier(u32::MAX));
    assert_eq!(trace(&hosted), trace(&direct));
    assert!(actual.metadata_namespace().is_some());
}

#[test]
fn receiver_materialization_host_preserves_every_actual_control_refusal() {
    use crate::host_projection_v2::ProjectionFailure;
    let p = scalar();
    let complete = Control::default();
    run_host(&p, limits(), &complete, &mut scope(false)).unwrap();
    for stop in 0..trace(&complete).len() {
        for cause in CAUSES {
            let c = Control {
                stop: Some((stop, cause)),
                events: Mutex::new(Vec::new()),
            };
            let mut host = scope(false);
            assert!(
                matches!(run_host(&p, limits(), &c, &mut host), Err(ProjectionFailure::Codec(TypeCodecError::Control(actual))) if actual == cause)
            );
            assert_eq!(trace(&c).len(), stop + 1);
            assert!(host.calls <= 1 && host.ran == host.calls);
        }
    }
}

#[test]
fn receiver_materialization_host_never_enters_after_original_shape_or_resource_refusal() {
    use crate::host_projection_v2::ProjectionFailure;
    let mut bad = scalar();
    bad.types.as_mut().unwrap().fields[0].carrier_type_id = Some(99);
    let mut host = scope(false);
    assert!(matches!(
        run_host(&bad, limits(), &Control::default(), &mut host),
        Err(ProjectionFailure::Codec(TypeCodecError::InvalidShape(_)))
    ));
    assert_eq!(host.calls, 0);
    let mut caps = limits();
    caps.max_allocation_request_bytes = 0;
    assert!(matches!(
        run_host(&scalar(), caps, &Control::default(), &mut host),
        Err(ProjectionFailure::Codec(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        )))
    ));
    assert_eq!(host.calls, 0);
}

#[test]
fn receiver_materialization_host_keeps_original_writer_error_and_ordinary_trace() {
    let mut p = scalar();
    p.types.as_mut().unwrap().fields[0]
        .metadata
        .push(plan::ArrowFieldMetadataEntry {
            key: NR_LOGICAL_TYPE_KEY.into(),
            value: "unknown-original-logical-type".into(),
        });
    let direct = Control::default();
    let expected = run(&p, limits(), 0, &direct).err().unwrap();
    let hosted = Control::default();
    let mut host = scope(false);
    let actual = run_host(&p, limits(), &hosted, &mut host).err().unwrap();
    match actual {
        crate::host_projection_v2::ProjectionFailure::Codec(actual) => {
            assert_eq!(actual.to_string(), expected.to_string())
        }
        crate::host_projection_v2::ProjectionFailure::Host(_) => panic!("unexpected host refusal"),
    }
    assert_eq!(trace(&hosted), trace(&direct));
    assert_eq!((host.calls, host.ran), (1, 1));
}
