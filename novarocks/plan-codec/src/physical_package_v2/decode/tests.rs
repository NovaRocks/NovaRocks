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
use crate::physical_package_v2::definition_sources::tests::{
    change_stream_packages, cv_package, repeated_change_stream_packages, rich_package,
    values_package, values_package_with_pool, writer_constant_package, writer_finish_package,
};
use crate::physical_package_v2::encode::{PackageEncodeError, encode_fragment_package};
use crate::physical_package_v2::provider_sources::tests::checked_read;
use crate::physical_package_v2::test_support::{decode_limits, encode_limits};
use crate::physical_type_v2::PackageTypeMaterializationScope;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use prost::Message;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn model() -> FragmentDecodeResourceModel {
    FragmentDecodeResourceModel::try_new(&Control::default()).unwrap()
}
fn encode(package: &p::FragmentPackage) -> Result<Vec<u8>, PackageEncodeError> {
    Ok(encode_fragment_package(package, &encode_limits(), &Control::default())?.encode_to_vec())
}

#[test]
fn json_constants_validate_only_reachable_nominal_cells() {
    use arrow::array::{Array, LargeBinaryArray, StructArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{
        FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    use std::sync::Arc;

    let field = Arc::new(Field::new("doc", DataType::LargeBinary, true));
    let plain = Arc::new(Field::new("bytes", DataType::LargeBinary, true));
    // The second JSON item has a valid root extent but an invalid boolean
    // value. Ordinary binary contents have no JSON meaning.
    let source = Arc::new(LargeBinaryArray::from(vec![
        b"\x01\x20".as_slice(),
        b"\x01\x42\x02".as_slice(),
    ]));
    let binary = Arc::new(LargeBinaryArray::from(vec![b"text".as_slice(); 2]));
    let pure = ValueTypeStorageLifetime::pure();
    let json = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        pure.clone(),
    )
    .unwrap();
    let binary_type = ValueTypeRef::try_new(ValueKind::Binary, true, pure.clone()).unwrap();
    let structure = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![
                ValueField::new("doc", json, pure.clone()),
                ValueField::new("bytes", binary_type, pure.clone()),
            ]
            .into(),
        ),
        true,
        pure.clone(),
    )
    .unwrap();
    let control = Control::default();
    for valid in [false, true] {
        let array = StructArray::new(
            vec![field.clone(), plain.clone()].into(),
            vec![source.clone(), binary.clone()],
            Some(vec![true, valid].into()),
        );
        let layout = ValueLayout::try_new(array.data_type(), pure.clone()).unwrap();
        let ty = FunctionValueType::try_from_value(structure.clone(), layout).unwrap();
        let root = Arc::new(Field::new("c", array.data_type().clone(), true));
        let pool = p::ConstantPool::try_new(
            root,
            ty,
            array.to_data(),
            crate::physical_package_v2::FROZEN_CONSTANT_POLICY,
            CompilePhase::Decode,
            &control,
        )
        .unwrap();
        let mut pools = p::ConstantPools::empty();
        pools.insert(p::ConstantPoolId::new(1), pool).unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let result = super::validate_json_constants_in(&pools, &mut work);
        if valid {
            assert!(matches!(
                result,
                Err(super::PhysicalConstantCodecError::JsonValue(
                    novarocks_functions::json_value::JsonValueError::Scalar
                ))
            ));
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn package_wire_validates_reachable_json_in_nullable_row_array_map() {
    use arrow::array::{Array, LargeBinaryArray, ListArray, MapArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::json_value::JsonValueError;
    use novarocks_type_contract::{
        FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    use std::sync::Arc;

    let json_null = b"\x01\x20".as_slice();
    // The item has the full boolean extent, but value 2 is outside its domain.
    let damaged = b"\x01\x42\x02".as_slice();
    let docs = Arc::new(LargeBinaryArray::from(vec![json_null, damaged, json_null]));
    let keys = Arc::new(StringArray::from(vec!["k"; docs.len()]));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::LargeBinary, true)),
        ]
        .into(),
        vec![keys, docs],
        None,
    );
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let storage = ValueTypeStorageLifetime::pure();
    let value = |kind, nullable| ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap();
    let json = value(ValueKind::Nominal(ValueLogicalType::Json), true);
    let map = value(
        ValueKind::Map {
            key: value(ValueKind::String, false),
            value: json,
        },
        true,
    );
    let list = value(ValueKind::List(map), true);
    let row = value(
        ValueKind::Struct(Box::from([ValueField::new("docs", list, storage.clone())])),
        true,
    );

    let validity = |valid| Some(vec![true, valid, true].into());
    let model = model();
    let control = Control::default();
    for (row_valid, list_valid, map_valid) in [
        (false, true, true),
        (true, false, true),
        (true, true, false),
        (true, true, true),
    ] {
        let maps = MapArray::new(
            entries_field.clone(),
            OffsetBuffer::from_lengths([1, 1, 1]),
            entries.clone(),
            validity(map_valid),
            false,
        );
        let lists = ListArray::new(
            Arc::new(Field::new("item", maps.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 1, 1]),
            Arc::new(maps),
            validity(list_valid),
        );
        let rows = StructArray::new(
            vec![Arc::new(Field::new(
                "docs",
                lists.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(lists)],
            validity(row_valid),
        );
        let layout = ValueLayout::try_new(rows.data_type(), storage.clone()).unwrap();
        let ty = FunctionValueType::try_from_value(row.clone(), layout).unwrap();
        let root = Arc::new(Field::new("cells", rows.data_type().clone(), true));
        let pool = p::ConstantPool::try_new(
            root,
            ty.clone(),
            rows.to_data(),
            crate::physical_package_v2::FROZEN_CONSTANT_POLICY,
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let package = values_package_with_pool(rows.len(), 1, pool);
        let bytes = encode(&package).unwrap();
        let result = decode_fragment_package(&bytes, &model, &decode_limits(), &control);
        if row_valid && list_valid && map_valid {
            assert!(matches!(
                result,
                Err(PackageDecodeError::Constant(
                    PhysicalConstantCodecError::JsonValue(JsonValueError::Scalar)
                ))
            ));
        } else {
            let decoded = result.unwrap();
            let pool = &decoded.constants().entries()[&p::ConstantPoolId::new(0)];
            assert_eq!(pool.value_type(), &ty);
            let rows = pool.array().as_any().downcast_ref::<StructArray>().unwrap();
            let lists = rows.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            let maps = lists.values().as_any().downcast_ref::<MapArray>().unwrap();
            let docs = maps
                .values()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert_eq!(
                (rows.is_valid(1), lists.is_valid(1), maps.is_valid(1)),
                (row_valid, list_valid, map_valid)
            );
            assert_eq!(
                [docs.value(0), docs.value(1), docs.value(2)],
                [json_null, damaged, json_null]
            );
        }
    }
}

fn fixtures() -> Vec<(&'static str, p::FragmentPackage)> {
    let mut fixtures = vec![
        ("rich", rich_package()),
        ("cv", cv_package()),
        ("writer", writer_constant_package()),
        ("finish", writer_finish_package()),
        ("data-read", checked_read(false)),
        ("metadata-read", checked_read(true)),
    ];
    for package in change_stream_packages() {
        let name = match package.fragment().id().get() {
            1 => "change-stream-writer",
            2 => "logical-change-finish",
            3 => "independent-router-summary",
            _ => panic!("the fixture must have exactly its three declared fragments"),
        };
        fixtures.push((name, package));
    }
    for package in repeated_change_stream_packages() {
        let name = match package.fragment().id().get() {
            1 => "repeated-change-stream-writer",
            2 => "repeated-logical-change-finish",
            3 => "repeated-independent-router-summary",
            _ => unreachable!(),
        };
        fixtures.push((name, package));
    }
    fixtures
}

// Lossless and canonical: sender bytes decode through the generated preflight
// and the original constructors into a checked package whose own encoding is
// byte-identical. No component is defaulted, re-derived or dropped.
#[test]
fn whole_package_roundtrip_is_byte_identical_through_original_constructors() {
    let model = model();
    for (name, package) in fixtures() {
        let bytes = encode(&package).unwrap_or_else(|e| panic!("{name}: {e}"));
        let decoded =
            decode_fragment_package(&bytes, &model, &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(decoded.fragment(), package.fragment(), "{name}");
        assert_eq!(decoded.cuts(), package.cuts(), "{name}");
        assert_eq!(
            decoded.fragment().nodes().len(),
            package.fragment().nodes().len(),
            "{name}"
        );
        assert_eq!(decoded.scans().len(), package.scans().len(), "{name}");
        assert_eq!(decoded.writes().len(), package.writes().len(), "{name}");
        assert_eq!(
            decoded.result().is_some(),
            package.result().is_some(),
            "{name}"
        );
        let again = encode(&decoded).unwrap_or_else(|e| panic!("{name} re-encode: {e}"));
        assert_eq!(
            again, bytes,
            "{name}: decode∘encode is the identity on sender bytes"
        );
    }
}

// Many-row VALUES lists and wide projections carry hundreds of strict scalar
// type roots. The sender charges each root's validator by its actual type
// size, so such a package fits the sender's allocation-request ceiling and
// its bytes survive the receiver and a re-encode unchanged.
#[test]
fn many_scalar_type_roots_roundtrip_byte_identically() {
    let model = model();
    for (rows, columns) in [(60, 1), (400, 1), (1000, 1), (1, 400)] {
        let name = format!("{rows}x{columns}");
        let package = values_package(rows, columns);
        let dto = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let roots = dto.types.as_ref().unwrap().value_types.len();
        assert_eq!(roots, rows * columns + 2 * columns + 1, "{name}");
        let bytes = dto.encode_to_vec();
        let decoded =
            decode_fragment_package(&bytes, &model, &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(decoded.fragment(), package.fragment(), "{name}");
        assert_eq!(encode(&decoded).unwrap(), bytes, "{name}");
    }
}

/// Nested Arrow fields (List item, Map entries and their Struct children)
/// carried by one data type, counted on the original value type.
fn nested_fields(ty: &arrow::datatypes::DataType) -> usize {
    use arrow::datatypes::DataType;
    match ty {
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            1 + nested_fields(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| 1 + nested_fields(field.data_type()))
            .sum(),
        _ => 0,
    }
}

// The finisher fragment of a checked writer plan: the writer's multiplexed
// result relation arrives over an ExchangeSource and feeds a TableFinish
// whose root write-result schema carries List and Map fields. The receiver
// reproduces its TableFinish exactly and the sender its bytes.
#[test]
fn finish_package_roundtrips_byte_identically_with_its_exact_table_finish() {
    let package = writer_finish_package();
    let finish = package
        .fragment()
        .nodes()
        .values()
        .find(|node| matches!(node.kind, p::NodeKind::TableFinish(_)))
        .expect("finish fixture has a TableFinish")
        .clone();
    assert!(package.writes().is_empty() && package.scans().is_empty());
    let bytes = encode(&package).unwrap();
    let decoded = decode_fragment_package(&bytes, &model(), &decode_limits(), &Control::default())
        .unwrap_or_else(|error| panic!("finish package receives: {error:?}"));
    assert_eq!(decoded.fragment().nodes()[&finish.id], finish);
    assert_eq!(
        decoded.fragment().values(),
        package.fragment().values(),
        "imported and derived writer values are reproduced"
    );
    assert_eq!(encode(&decoded).unwrap(), bytes);
}

// The receiver publishes through the caller-scope Package constructor with
// its host admission. Each nested field's logical metadata lookup is charged
// against that retained-source invoice; the charge must stay linear in it.
// With a source-sized key length, two nested fields under the 2 GiB receiver
// invoice overflowed the work meter and the finish package was refused.
#[test]
fn caller_scope_package_admission_charges_nested_field_lookups_linearly() {
    let package = writer_finish_package();
    let nested: usize = package
        .fragment()
        .values()
        .values()
        .map(|value| nested_fields(value.ty.data_type()))
        .sum();
    assert!(nested >= 2, "the fixture carries {nested} nested fields");
    let admission = decode_limits().admission;
    assert!(admission.source_retained_bytes >= 2 * 1024 * 1024 * 1024);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut peak = 0usize;
    let readmitted = p::FragmentPackage::try_new_in(
        package.clone().into_input(),
        admission,
        &mut |facts| {
            peak = peak.max(facts.cumulative_work_upper_bound);
            Ok(())
        },
        &mut work,
    )
    .unwrap_or_else(|error| panic!("caller-scope admission: {error:?}"));
    work.finish().unwrap();
    assert_eq!(
        readmitted.fragment().nodes(),
        package.fragment().nodes(),
        "the original constructor publishes the same fragment"
    );
    // Linear: far below the about 2 * source^2 a source-sized key charged
    // for each nested field.
    let source = admission.source_retained_bytes;
    let per_field_quadratic = source.saturating_mul(2).saturating_mul(source);
    assert!(peak < per_field_quadratic / 1024, "peak work {peak}");
}

fn mutated(
    package: &p::FragmentPackage,
    change: impl FnOnce(&mut wire::FragmentPackage),
) -> Vec<u8> {
    let mut dto = encode_fragment_package(package, &encode_limits(), &Control::default()).unwrap();
    change(&mut dto);
    dto.encode_to_vec()
}
fn refused(bytes: &[u8]) -> PackageDecodeError {
    decode_fragment_package(bytes, &model(), &decode_limits(), &Control::default())
        .expect_err("receiver must refuse")
}

#[test]
fn higher_order_claim_preserves_exact_body_and_rejects_non_control_null_policies() {
    use novarocks_expression_contract::UnitError;
    use novarocks_proto_models::physical_semantics_v2::NullBehavior;
    use novarocks_type_contract::{
        ArgumentControl, EffectContractError, EvaluationDemand, FunctionNullBehavior,
    };

    let package = rich_package();
    let unit = package
        .fragment()
        .nodes()
        .values()
        .find_map(|node| node.expressions.as_ref())
        .expect("rich fixture has a scalar call unit");
    let selected = unit.claims().keys().next().copied().unwrap();
    let call = &unit.control_flow().uses()[&selected];
    let wrapper = call.arguments[0];
    let body = unit.control_flow().uses()[&wrapper].arguments[0];
    let decoded = decode_fragment_package(
        &encode(&package).unwrap(),
        &model(),
        &decode_limits(),
        &Control::default(),
    )
    .unwrap();
    let unit = decoded
        .fragment()
        .nodes()
        .values()
        .find_map(|node| node.expressions.as_ref())
        .unwrap();
    let claim = &unit.claims()[&selected];
    assert_eq!(
        claim.effects.null_behavior,
        FunctionNullBehavior::ControlDefined
    );
    assert_eq!(
        claim.effects.argument_control,
        ArgumentControl::HigherOrder {
            body_ordinal: 0,
            body_demand: EvaluationDemand::Value,
        }
    );
    assert_eq!(unit.control_flow().uses()[&selected].arguments[0], wrapper);
    assert_eq!(unit.control_flow().uses()[&wrapper].arguments[0], body);
    assert_eq!(decoded.fragment(), package.fragment());

    for null_behavior in [NullBehavior::Strict, NullBehavior::CalledOnNull] {
        let bytes = mutated(&package, |dto| {
            let claims = dto
                .fragment
                .as_mut()
                .unwrap()
                .nodes
                .iter_mut()
                .filter_map(|node| node.expressions.as_mut())
                .find_map(|unit| {
                    unit.claims
                        .as_mut()
                        .filter(|claims| !claims.entries.is_empty())
                })
                .expect("encoded rich fixture has its selected claims");
            for claim in &mut claims.entries {
                claim.effects.as_mut().unwrap().null_behavior = null_behavior as i32;
            }
        });
        let error = refused(&bytes);
        assert!(
            matches!(&error, PackageDecodeError::Expression(
                ExpressionCodecError::ConstructedUnit(problem),
            ) if matches!(problem.cause(), UnitError::Effects(EffectContractError::ControlMismatch))),
            "{null_behavior:?}: {error:?}"
        );
    }
}

#[test]
fn change_stream_package_refuses_role_and_cut_downgrades() {
    let package = change_stream_packages()
        .into_iter()
        .find(|package| package.fragment().id().get() == 3)
        .unwrap();
    type PackageMutation = fn(&mut wire::FragmentPackage);
    let cases: [(&str, PackageMutation); 11] = [
        ("summary uses relational root", |dto| {
            dto.cuts
                .as_mut()
                .unwrap()
                .outbound
                .iter_mut()
                .find(|cut| cut.edge_id == Some(9))
                .unwrap()
                .source_port = wire::FragmentOutputPort::Root as i32;
        }),
        ("summary permits withdrawal", |dto| {
            dto.cuts
                .as_mut()
                .unwrap()
                .outbound
                .iter_mut()
                .find(|cut| cut.edge_id == Some(9))
                .unwrap()
                .delivery = wire::DeliveryRequirement::DemandDriven as i32;
        }),
        ("summary payload becomes ordinary relation", |dto| {
            let cut = dto
                .cuts
                .as_mut()
                .unwrap()
                .outbound
                .iter_mut()
                .find(|cut| cut.edge_id == Some(9))
                .unwrap();
            cut.payload = Some(wire::outbound_fragment_cut::Payload::Relational(
                novarocks_proto_models::physical_control_v2::Empty {},
            ));
            cut.source_port = wire::FragmentOutputPort::Root as i32;
            cut.delivery = wire::DeliveryRequirement::DemandDriven as i32;
        }),
        ("summary field type is absent", |dto| {
            dto.cuts
                .as_mut()
                .unwrap()
                .outbound
                .iter_mut()
                .find(|cut| cut.edge_id == Some(9))
                .unwrap()
                .projection[0]
                .value_type_id = None;
        }),
        ("summary field uses the Int8 effect type", |dto| {
            let fragment = dto.fragment.as_ref().unwrap();
            let Some(wire::fragment_sink::Kind::Router(router)) =
                &fragment.sink.as_ref().unwrap().kind
            else {
                panic!()
            };
            let effect_type = fragment
                .values
                .iter()
                .find(|value| Some(value.id) == router.effect_value_id)
                .unwrap()
                .value_type_id;
            dto.cuts
                .as_mut()
                .unwrap()
                .outbound
                .iter_mut()
                .find(|cut| cut.edge_id == Some(9))
                .unwrap()
                .projection[0]
                .value_type_id = effect_type;
        }),
        ("summary aliases two destination declarations", |dto| {
            let cut = dto
                .cuts
                .as_mut()
                .unwrap()
                .outbound
                .iter_mut()
                .find(|cut| cut.edge_id == Some(9))
                .unwrap();
            cut.destination_imports[1].destination_value_id =
                cut.destination_imports[0].destination_value_id;
        }),
        ("missing mandatory stage policy", |dto| {
            dto.fragment.as_mut().unwrap().stage_placement = None;
        }),
        ("zero explicit stage cap", |dto| {
            dto.fragment
                .as_mut()
                .unwrap()
                .stage_placement
                .as_mut()
                .unwrap()
                .kind = Some(wire::stage_placement_policy::Kind::Spread(
                wire::SpreadStagePlacement {
                    max_instances: Some(0),
                },
            ));
        }),
        ("missing independent summary", |dto| {
            dto.fragment.as_mut().unwrap().summaries.clear();
        }),
        ("missing colocation declaration", |dto| {
            let Some(wire::fragment_sink::Kind::Router(router)) =
                &mut dto.fragment.as_mut().unwrap().sink.as_mut().unwrap().kind
            else {
                panic!()
            };
            router.routes[0].requirements.as_mut().unwrap().colocation = None;
        }),
        ("missing layout declaration", |dto| {
            let Some(wire::fragment_sink::Kind::Router(router)) =
                &mut dto.fragment.as_mut().unwrap().sink.as_mut().unwrap().kind
            else {
                panic!()
            };
            router.routes[0].requirements.as_mut().unwrap().layout = None;
        }),
    ];
    for (name, change) in cases {
        let error = refused(&mutated(&package, change));
        assert!(
            !matches!(error, PackageDecodeError::Control(_)),
            "{name}: malformed contracts must not become control/capacity failures: {error}"
        );
    }
}

#[test]
fn logical_finish_package_requires_both_distinct_role_inputs() {
    let package = change_stream_packages()
        .into_iter()
        .find(|package| package.fragment().id().get() == 2)
        .unwrap();
    for case in 0..5 {
        let bytes = mutated(&package, |dto| {
            let finish = dto
                .fragment
                .as_mut()
                .unwrap()
                .nodes
                .iter_mut()
                .find_map(|node| match &mut node.kind {
                    Some(wire::physical_node::Kind::TableFinish(finish)) => Some(finish),
                    _ => None,
                })
                .unwrap();
            match case {
                0 => finish.summaries[0].input = None,
                1 => finish.summaries[0].input = finish.writer_results_input,
                2 => finish.summaries[0].columns = None,
                3 => finish.count_mode = wire::FinishCountMode::WriterReported as i32,
                4 => {
                    let columns = finish.summaries[0].columns.as_mut().unwrap();
                    columns.value_ids[2] = columns.value_ids[0];
                }
                _ => unreachable!(),
            }
        });
        let error = refused(&bytes);
        assert!(
            !matches!(error, PackageDecodeError::Control(_)),
            "case {case}: {error}"
        );
    }
}

#[test]
fn change_stream_receiver_preserves_each_interrupted_control_prefix() {
    let model = model();
    for package in change_stream_packages() {
        let bytes = encode(&package).unwrap();
        let control = Control::default();
        decode_fragment_package(&bytes, &model, &decode_limits(), &control).unwrap();
        let trace = control.trace.lock().unwrap().clone();
        assert!(trace.len() > 2);
        for at in [0, trace.len() / 2, trace.len() - 1] {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let stop = Control {
                    stop: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert!(
                    matches!(decode_fragment_package(&bytes, &model, &decode_limits(), &stop),
                    Err(PackageDecodeError::Control(actual)) if actual == cause)
                );
                assert_eq!(*stop.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn prior_plan_contract_revision_is_refused_by_the_original_package_constructor() {
    let package = rich_package();
    let prior_revision = p::PLAN_CONTRACT_REVISION
        .checked_sub(1)
        .filter(|revision| *revision != 0)
        .expect("test requires a prior plan contract revision");
    let bytes = mutated(&package, |dto| {
        dto.required.as_mut().unwrap().plan_contract_revision = prior_revision;
    });
    let error = refused(&bytes);
    match error {
        PackageDecodeError::Package(p::FragmentPackageError::Structure(errors)) => {
            assert!(errors.to_string().contains("package.required"));
            assert!(
                errors
                    .to_string()
                    .contains("plan contract revision differs")
            );
        }
        other => panic!("prior contract must reach the original package validator: {other:?}"),
    }
}

#[test]
fn absent_required_components_are_refused_without_defaults() {
    let package = rich_package();
    type PackageMutation = fn(&mut wire::FragmentPackage);
    let cases: [(&str, PackageMutation); 7] = [
        ("fragment", |d| d.fragment = None),
        ("expression_control", |d| {
            d.fragment
                .as_mut()
                .unwrap()
                .nodes
                .iter_mut()
                .find_map(|node| node.expressions.as_mut())
                .unwrap()
                .control = None;
        }),
        ("calls", |d| d.calls = None),
        ("pruning", |d| d.pruning = None),
        ("parameters", |d| d.parameters = None),
        ("cuts", |d| d.cuts = None),
        ("call_requests", |d| {
            d.fragment.as_mut().unwrap().call_requests = None;
        }),
    ];
    for (name, change) in cases {
        let error = refused(&mutated(&package, change));
        assert!(
            !matches!(error, PackageDecodeError::Control(_)),
            "{name}: absence is an ordinary refusal, got {error}"
        );
    }
}

#[test]
fn duplicate_node_ids_and_malformed_bytes_are_refused() {
    let package = rich_package();
    let duplicate = mutated(&package, |d| {
        let fragment = d.fragment.as_mut().unwrap();
        let first = fragment.nodes[0].clone();
        fragment.nodes.push(first);
    });
    assert!(matches!(
        refused(&duplicate),
        PackageDecodeError::Invalid(_)
    ));
    let mut truncated = encode(&package).unwrap();
    truncated.truncate(truncated.len() / 2);
    assert!(!matches!(
        refused(&truncated),
        PackageDecodeError::Control(_)
    ));
    // An unused constant pool is not silently accepted by the original
    // closed-pool publication law.
    let cv = cv_package();
    let extra = mutated(&cv, |d| {
        let mut pool = d.constants[0].clone();
        pool.id = pool.id.wrapping_sub(1);
        d.constants.push(pool);
    });
    assert!(!matches!(refused(&extra), PackageDecodeError::Control(_)));
}

#[test]
fn oversized_input_is_refused_by_byte_admission_before_any_dto() {
    let bytes = encode(&rich_package()).unwrap();
    let mut limits = decode_limits();
    limits.wire.max_input_bytes = bytes.len() - 1;
    let error = decode_fragment_package(&bytes, &model(), &limits, &Control::default())
        .expect_err("over byte limit");
    assert!(matches!(
        error,
        PackageDecodeError::Control(CompileControlError::ResourceExhausted)
            | PackageDecodeError::Wire(_)
    ));
}

// Package decoding and the original provider validation share the caller's
// control; every refusal keeps the exact completed prefix.
#[test]
fn caller_control_cause_stays_primary_across_the_receiver() {
    let package = checked_read(true);
    let bytes = encode(&package).unwrap();
    let control = Control::default();
    decode_fragment_package(&bytes, &model(), &decode_limits(), &control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    let callbacks = trace.len();
    assert!(callbacks > 2);
    assert!(
        trace
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::Decode)
    );
    assert!(
        trace
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::ProviderValidation)
    );
    assert!(trace.iter().all(|(phase, _)| matches!(
        phase,
        CompilePhase::Decode | CompilePhase::ProviderValidation
    )));
    for at in [0, callbacks / 3, callbacks / 2, callbacks - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = Control {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(
                    decode_fragment_package(&bytes, &model(), &decode_limits(), &stop),
                    Err(PackageDecodeError::Control(actual)) if actual == cause
                ),
                "position {at}"
            );
            assert_eq!(*stop.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

/// Every checked package of one plan that reads and writes nothing: eager
/// node-local units already sealed by its author and explicit empty pruning tables.
fn plan_packages(plan: &p::PhysicalPlan) -> BTreeMap<p::FragmentId, p::FragmentPackage> {
    let setup = Control::default();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for id in plan.fragments().keys() {
        pruning.insert(
            *id,
            p::FrozenFragmentPruning::try_new(*id, vec![], &setup).unwrap(),
        );
        admissions.insert(
            *id,
            p::FragmentPackageAdmission {
                plan_limits: p::PlanLimits::FROZEN,
                source_retained_bytes: 128 * 1024 * 1024,
                property_projection_limits: p::PropertyProofProjectionLimits {
                    max_request_bytes: 64 * 1024 * 1024,
                    max_coexisting_bytes: 512 * 1024 * 1024,
                    max_projection_work: 128 * 1024 * 1024,
                },
            },
        );
    }
    p::extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &setup,
    )
    .unwrap()
}

// Each package carries its own slice of the plan-global runtime-filter
// binding numbering. The whole-package wire preserves it exactly, and the
// receiver's package law refuses identities the plan never gave the slice.
#[test]
fn runtime_filter_package_roundtrips_its_plan_binding_slice_byte_identically() {
    use p::RuntimeFilterBindingRole::{Consumer, Producer};
    let plan = crate::runtime_filter_test_support::finish_ordered_join_build_filter_plan();
    let numbered = p::runtime_filter_bindings(&plan).unwrap();
    let join_fragment = p::FragmentId::new(42);
    let filter = p::RuntimeFilterId::new(40);
    assert_eq!(
        numbered
            .iter()
            .map(|binding| (binding.binding_id, binding.fragment, binding.role))
            .collect::<Vec<_>>(),
        [
            (1, join_fragment, Producer(0)),
            (2, join_fragment, Consumer(0))
        ]
    );
    let packages = plan_packages(&plan);
    assert_eq!(packages.len(), 3);
    for (id, package) in &packages {
        let slice = numbered
            .iter()
            .filter(|binding| binding.fragment == *id)
            .map(p::RuntimeFilterBinding::cut)
            .collect::<Vec<_>>();
        assert_eq!(
            &*package.cuts().runtime_filter_bindings,
            slice,
            "{}",
            id.get()
        );
        let bytes = encode(package).unwrap_or_else(|e| panic!("{}: {e}", id.get()));
        let decoded =
            decode_fragment_package(&bytes, &model(), &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{}: {e}", id.get()));
        assert_eq!(decoded.cuts(), package.cuts(), "{}", id.get());
        assert_eq!(encode(&decoded).unwrap(), bytes, "{}", id.get());
    }

    let package = &packages[&join_fragment];
    let dto = encode_fragment_package(package, &encode_limits(), &Control::default()).unwrap();
    let wire_bindings = &dto.cuts.as_ref().unwrap().runtime_filter_bindings;
    use wire::runtime_filter_binding_cut::Role;
    assert_eq!(
        wire_bindings
            .iter()
            .map(|binding| (binding.binding_id, binding.runtime_filter_id, binding.role))
            .collect::<Vec<_>>(),
        [
            (Some(1), Some(filter.get()), Some(Role::ProducerIndex(0))),
            (Some(2), Some(filter.get()), Some(Role::ConsumerIndex(0))),
        ]
    );
    let structure = |error: PackageDecodeError| match error {
        PackageDecodeError::Package(p::FragmentPackageError::Structure(errors)) => errors,
        other => panic!("binding table was not refused by the package law: {other}"),
    };
    let swapped = refused(&mutated(package, |d| {
        let bindings = &mut d.cuts.as_mut().unwrap().runtime_filter_bindings;
        bindings[0].binding_id = Some(2);
        bindings[1].binding_id = Some(1);
    }));
    let errors = structure(swapped);
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[42].cuts.runtime_filter_bindings[1]"
            && error.message()
                == "runtime-filter binding identities of one fragment are not consecutive"),
        "{errors}"
    );
    let dropped = refused(&mutated(package, |d| {
        d.cuts.as_mut().unwrap().runtime_filter_bindings.pop();
    }));
    let errors = structure(dropped);
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[42].cuts.runtime_filter_bindings"
            && error.message() == "local consumer 0 of runtime filter 40 has no binding"),
        "{errors}"
    );
    // A shapeless entry is refused by the cut codec before any package law.
    let shapeless = refused(&mutated(package, |d| {
        d.cuts.as_mut().unwrap().runtime_filter_bindings[0].role = None;
    }));
    assert!(
        matches!(shapeless, PackageDecodeError::Node(_)),
        "{shapeless}"
    );
}

struct TypeScope {
    calls: usize,
    ran: usize,
    refuse: bool,
}
impl PackageTypeMaterializationScope for TypeScope {
    type HostError = u32;
    fn materialize<C, B>(
        &mut self,
        _: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<crate::physical_type_v2::DecodedTypeTable, ProjectionFailure<C, u32>>
    where
        C: From<TypeCodecError>,
        B: FnOnce() -> Result<crate::physical_type_v2::DecodedTypeTable, C>,
    {
        self.calls += 1;
        if self.refuse {
            return Err(ProjectionFailure::Host(79));
        }
        self.ran += 1;
        body().map_err(ProjectionFailure::Codec)
    }
}
#[test]
fn whole_package_type_host_keeps_original_bytes_and_every_checkpoint() {
    let model = model();
    for (name, package) in fixtures() {
        let bytes = encode(&package).unwrap();
        let direct = Control::default();
        let expected = decode_fragment_package(&bytes, &model, &decode_limits(), &direct).unwrap();
        let hosted = Control::default();
        let mut host = TypeScope {
            calls: 0,
            ran: 0,
            refuse: false,
        };
        let actual = decode_fragment_package_with_type_host(
            &bytes,
            &model,
            &decode_limits(),
            &hosted,
            &mut host,
        )
        .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert_eq!(
            encode(&actual).unwrap(),
            encode(&expected).unwrap(),
            "{name}"
        );
        assert_eq!(
            *hosted.trace.lock().unwrap(),
            *direct.trace.lock().unwrap(),
            "{name}"
        );
        assert_eq!((host.calls, host.ran), (2, 2), "{name}");
    }
}
#[test]
fn whole_package_type_host_refusal_is_nominal_without_completion_callback() {
    let bytes = encode(&rich_package()).unwrap();
    let model = model();
    let mut host = TypeScope {
        calls: 0,
        ran: 0,
        refuse: true,
    };
    let original = Control::default();
    assert!(matches!(
        decode_fragment_package_with_type_host(
            &bytes,
            &model,
            &decode_limits(),
            &original,
            &mut host
        ),
        Err(ProjectionFailure::Host(79))
    ));
    let length = original.trace.lock().unwrap().len();
    // This callback would refuse the ordinary footer. It must never be called
    // after the same host refusal, or replace that original nominal cause.
    let stop = Control {
        stop: Some((length, CompileControlError::Cancelled)),
        trace: Mutex::new(Vec::new()),
    };
    let mut host = TypeScope {
        calls: 0,
        ran: 0,
        refuse: true,
    };
    assert!(matches!(
        decode_fragment_package_with_type_host(&bytes, &model, &decode_limits(), &stop, &mut host),
        Err(ProjectionFailure::Host(79))
    ));
    assert_eq!(stop.trace.lock().unwrap().len(), length);
    assert_eq!((host.calls, host.ran), (1, 0));
    let mut host = TypeScope {
        calls: 0,
        ran: 0,
        refuse: false,
    };
    assert!(matches!(
        decode_fragment_package_with_type_host(
            &[0x80],
            &model,
            &decode_limits(),
            &Control::default(),
            &mut host
        ),
        Err(ProjectionFailure::Codec(_))
    ));
    assert_eq!((host.calls, host.ran), (0, 0));
}
