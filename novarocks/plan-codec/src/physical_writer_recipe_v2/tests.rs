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
use crate::physical_connector_payload_v2::{
    ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
};
use crate::physical_provider_binding_v2::{
    ProviderBindingProjectionLimits, ProviderBindingSource, decode_joint_provider_bindings,
    decode_provider_bindings, encode_joint_provider_bindings,
};
use crate::physical_type_v2::{
    PackageTypeProjectionLimits, WriterTypeSource, decode_package_type_table_observed,
    encode_borrowed_type_namespaces_sources_in,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::{collections::HashMap, sync::Mutex};

// Conservative union invoice for these bounded fresh owners, not a private
// HashMap capacity measurement or an allocator/host grant.
const SOURCE: usize = 128 * 1024 * 1024;
// Subcomponent invoices exclude this leaf and the other coexisting namespaces.
const NAMESPACE_SOURCE: usize = 1024 * 1024;
type Run<T> = (Result<(Vec<T>, WriterRecipeProjectionFacts), E>, Vec<u32>);
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    events: Mutex<Vec<u32>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let stop = self.stop.lock().unwrap().clone();
        let at = events.len();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push(units);
        match stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn reset(&self, stop: Option<(usize, CompileControlError)>) {
        self.events.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<u32> {
        self.events.lock().unwrap().clone()
    }
}
fn limits() -> WriterRecipeProjectionLimits {
    WriterRecipeProjectionLimits {
        max_recipes: 1000,
        max_input_fields: 100_000,
        max_token_bytes: 4 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn provider_limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 1000,
        max_payload_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn type_limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn draft(input: c::ConnectorWriteInputShape) -> c::ConnectorWriteRecipeDraft {
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let binding = c::ConnectorWriteBinding::new(
        c::ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = c::ConnectorEncodedPayload::new(
        c::ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            c::ConnectorCodecCategory::WriteHandle,
            c::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7u8].into(),
    );
    c::ConnectorWriteRecipeDraft::try_new(binding, payload, input).unwrap()
}
fn field(token: u8, name: &str) -> c::ConnectorWriteFieldBinding {
    c::ConnectorWriteFieldBinding::try_new_typed(
        c::ConnectorWriteFieldToken::from_bytes([token; 32]),
        Field::new(name, DataType::Int64, false),
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
    )
    .unwrap()
}
fn data(field: Field) -> c::ConnectorWriteRecipeDraft {
    draft(c::ConnectorWriteInputShape::Data {
        fields: vec![
            c::ConnectorWriteFieldBinding::try_new_typed(
                c::ConnectorWriteFieldToken::from_bytes([0; 32]),
                field.clone(),
                novarocks_type_contract::FunctionValueType::new(
                    field.data_type().clone(),
                    field.is_nullable(),
                ),
            )
            .unwrap(),
        ],
    })
}
fn five() -> Vec<c::ConnectorWriteRecipeDraft> {
    vec![
        draft(c::ConnectorWriteInputShape::Data {
            fields: vec![field(0, "a"), field(1, "b")],
        }),
        draft(c::ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field(2, "c")],
            row_identity_fields: vec![field(3, "d")],
        }),
        draft(c::ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![field(4, "e")],
            partition_source_fields: vec![field(5, "f")],
        }),
        draft(c::ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![field(6, "g")],
            partition_source_fields: vec![field(7, "h")],
        }),
        draft(c::ConnectorWriteInputShape::EqualityDelete {
            equality_fields: vec![field(8, "i"), field(9, "j")],
        }),
    ]
}
fn with_encoded<T>(
    recipes: &[c::ConnectorWriteRecipeDraft],
    ids: &[Vec<u32>],
    control: &dyn PureCompileControl,
    run: impl FnOnce(WriterRecipeEncodeContext<'_, '_, '_>) -> T,
) -> T {
    with_encoded_storage_roots(recipes, ids, control, None, false, run)
}
fn with_encoded_storage_roots<T>(
    recipes: &[c::ConnectorWriteRecipeDraft],
    ids: &[Vec<u32>],
    control: &dyn PureCompileControl,
    foreign_storage: Option<&novarocks_type_contract::FunctionValueType>,
    duplicate_storage: bool,
    run: impl FnOnce(WriterRecipeEncodeContext<'_, '_, '_>) -> T,
) -> T {
    let providers = recipes
        .iter()
        .enumerate()
        .map(|(i, r)| (i as u32, ProviderBindingSource::Write(r.binding())))
        .collect::<Vec<_>>();
    let payloads = recipes
        .iter()
        .enumerate()
        .map(|(i, r)| (if i == 0 { u32::MAX } else { i as u32 }, r.payload()))
        .collect::<Vec<_>>();
    let mut values = Vec::new();
    let value_ids = recipes
        .iter()
        .map(|recipe| {
            recipe
                .input()
                .fields_iter()
                .map(|binding| {
                    let id = u32::try_from(values.len()).unwrap();
                    values.push((
                        id,
                        binding
                            .execution_type()
                            .expect("complete independent fixture declaration"),
                    ));
                    if let Some(storage) = binding.storage_type() {
                        values.push((
                            u32::try_from(values.len()).unwrap(),
                            foreign_storage.unwrap_or(storage),
                        ));
                        if duplicate_storage {
                            values.push((u32::try_from(values.len()).unwrap(), storage));
                        }
                    }
                    id
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let writers = recipes
        .iter()
        .zip(ids)
        .zip(&value_ids)
        .map(|((recipe, source_ids), value_ids)| {
            WriterTypeSource::new_with_value_ids(recipe, source_ids, value_ids)
        })
        .collect::<Vec<_>>();
    let bindings =
        encode_joint_provider_bindings(&providers, NAMESPACE_SOURCE, provider_limits(), control)
            .unwrap();
    let payloads =
        encode_connector_payloads(&payloads, NAMESPACE_SOURCE, payload_limits(), control).unwrap();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode).unwrap();
    let types = encode_borrowed_type_namespaces_sources_in(
        &values,
        &[],
        &[],
        &writers,
        SOURCE,
        type_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    run(WriterRecipeEncodeContext {
        bindings: &bindings,
        payloads: &payloads,
        types: &types,
    })
}
fn encode_run(
    recipes: &[c::ConnectorWriteRecipeDraft],
    ids: &[Vec<u32>],
    stop: Option<(usize, CompileControlError)>,
    l: impl Into<Option<WriterRecipeProjectionLimits>>,
) -> Run<wire::FrozenWriterRecipe> {
    encode_with_views(recipes, ids, ids, stop, l)
}
fn encode_with_views(
    recipes: &[c::ConnectorWriteRecipeDraft],
    ids: &[Vec<u32>],
    views: &[Vec<u32>],
    stop: Option<(usize, CompileControlError)>,
    l: impl Into<Option<WriterRecipeProjectionLimits>>,
) -> Run<wire::FrozenWriterRecipe> {
    let control = Control::default();
    let out = with_encoded(recipes, ids, &control, |context| {
        let sources = recipes
            .iter()
            .zip(views)
            .enumerate()
            .map(|(i, (recipe, ids))| WriterRecipeSource {
                node: NodeId::new(if i == 0 { u32::MAX } else { i as u32 - 1 }),
                recipe,
                field_ids: ids,
            })
            .collect::<Vec<_>>();
        control.reset(stop);
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)?;
        let result = encode_writer_recipes_observed(
            &sources,
            context,
            SOURCE,
            l,
            &mut |_| Ok(()),
            &mut work,
        );
        if !matches!(&result, Err(E::Control(_))) {
            work.finish()?;
        }
        result
    });
    (out, control.trace())
}
#[derive(Clone)]
struct Raw {
    type_roots: Vec<wire::FrozenWriterRecipe>,
    providers: Vec<wire::ProviderBindingDefinition>,
    payloads: Vec<wire::ConnectorPayloadDefinition>,
    package: wire::FragmentPackage,
}
fn raw(recipes: &[c::ConnectorWriteRecipeDraft], ids: &[Vec<u32>]) -> Raw {
    let nodes = (0..recipes.len())
        .map(|i| NodeId::new(if i == 0 { u32::MAX } else { i as u32 - 1 }))
        .collect::<Vec<_>>();
    raw_with_nodes(recipes, ids, &nodes)
}
fn raw_with_nodes(
    recipes: &[c::ConnectorWriteRecipeDraft],
    ids: &[Vec<u32>],
    nodes: &[NodeId],
) -> Raw {
    let control = Control::default();
    with_encoded(recipes, ids, &control, |context| {
        let sources = recipes
            .iter()
            .zip(ids)
            .zip(nodes)
            .map(|((recipe, ids), node)| WriterRecipeSource {
                node: *node,
                recipe,
                field_ids: ids,
            })
            .collect::<Vec<_>>();
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        let (writes, _) = encode_writer_recipes_observed(
            &sources,
            WriterRecipeEncodeContext {
                bindings: context.bindings,
                payloads: context.payloads,
                types: context.types,
            },
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        Raw {
            type_roots: writes.clone(),
            providers: context.bindings.as_wire().to_vec(),
            payloads: context.payloads.as_wire().to_vec(),
            package: wire::FragmentPackage {
                types: Some(context.types.as_wire().clone()),
                source_fields: Some(
                    context
                        .types
                        .as_source_wire()
                        .expect("actual emitted source namespace")
                        .clone(),
                ),
                writes,
                ..Default::default()
            },
        }
    })
}
fn decode_run(
    raw: &Raw,
    stop: Option<(usize, CompileControlError)>,
    l: impl Into<Option<WriterRecipeProjectionLimits>>,
    read_only: bool,
) -> Run<(NodeId, c::ConnectorWriteRecipeDraft)> {
    let control = Control::default();
    let original: &dyn PureCompileControl = &control;
    let bindings = if read_only {
        decode_provider_bindings(
            &raw.providers,
            NAMESPACE_SOURCE,
            provider_limits(),
            original,
        )
    } else {
        decode_joint_provider_bindings(
            &raw.providers,
            NAMESPACE_SOURCE,
            provider_limits(),
            original,
        )
    }
    .unwrap();
    let payloads =
        decode_connector_payloads(&raw.payloads, NAMESPACE_SOURCE, payload_limits(), original)
            .unwrap();
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Decode).unwrap();
    let mut type_package = raw.package.clone();
    type_package.writes = raw.type_roots.clone();
    let types = decode_package_type_table_observed(
        &type_package,
        SOURCE,
        type_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    control.reset(stop);
    let out = (|| {
        let mut work = CompileCheckpoints::try_new(original, CompilePhase::Decode)?;
        let result = decode_writer_recipes_observed(
            &raw.package.writes,
            WriterRecipeDecodeContext {
                bindings: &bindings,
                payloads: &payloads,
                types: &types,
            },
            SOURCE,
            l,
            &mut |_| Ok(()),
            &mut work,
        );
        if !matches!(&result, Err(E::Control(_))) {
            work.finish()?;
        }
        result
    })();
    (out, control.trace())
}
fn fb(token: u8, id: u32) -> wire::ConnectorWriteFieldBinding {
    wire::ConnectorWriteFieldBinding {
        storage_value_type_id: None,
        field_token: vec![token; 32],
        source_field_id: Some(id),
        value_type_id: Some(u32::from(token)),
    }
}
fn expected_shapes() -> Vec<wire::ConnectorWriteInputShape> {
    use wire::connector_write_input_shape::Kind;
    vec![
        wire::ConnectorWriteInputShape {
            kind: Some(Kind::Data(wire::ConnectorWriteDataInput {
                fields: vec![fb(0, 0), fb(1, u32::MAX)],
            })),
        },
        wire::ConnectorWriteInputShape {
            kind: Some(Kind::RowLineage(wire::ConnectorWriteRowLineageInput {
                data_fields: vec![fb(2, 7)],
                row_identity_fields: vec![fb(3, 9)],
            })),
        },
        wire::ConnectorWriteInputShape {
            kind: Some(Kind::PositionDelete(
                wire::ConnectorWritePositionDeleteInput {
                    identity_fields: vec![fb(4, 11)],
                    partition_source_fields: vec![fb(5, 13)],
                },
            )),
        },
        wire::ConnectorWriteInputShape {
            kind: Some(Kind::DeletionVector(
                wire::ConnectorWriteDeletionVectorInput {
                    identity_fields: vec![fb(6, 15)],
                    partition_source_fields: vec![fb(7, 17)],
                },
            )),
        },
        wire::ConnectorWriteInputShape {
            kind: Some(Kind::EqualityDelete(
                wire::ConnectorWriteEqualityDeleteInput {
                    equality_fields: vec![fb(8, 19), fb(9, 21)],
                },
            )),
        },
    ]
}
fn five_ids() -> Vec<Vec<u32>> {
    vec![
        vec![0, u32::MAX],
        vec![7, 9],
        vec![11, 13],
        vec![15, 17],
        vec![19, 21],
    ]
}

#[test]
fn all_five_shapes_have_independent_wire_and_owned_role_oracles() {
    let recipes = five();
    let ids = five_ids();
    let (result, _) = encode_run(&recipes, &ids, None, limits());
    let (wire, facts) = result.unwrap();
    assert_eq!(facts.recipe_count, 5);
    assert_eq!(facts.input_field_count, 10);
    assert_eq!(facts.token_bytes, 320);
    let expected = expected_shapes()
        .into_iter()
        .enumerate()
        .map(|(i, input)| wire::FrozenWriterRecipe {
            node_id: Some(if i == 0 { u32::MAX } else { i as u32 - 1 }),
            provider_binding_id: Some(i as u32),
            handle_payload_id: Some(if i == 0 { u32::MAX } else { i as u32 }),
            input: Some(input),
        })
        .collect::<Vec<_>>();
    assert_eq!(wire, expected);
    let raw = raw(&recipes, &ids);
    let (decoded, _) = decode_run(&raw, None, limits(), false);
    let (decoded, _) = decoded.unwrap();
    for (i, ((node, actual), original)) in decoded.iter().zip(&recipes).enumerate() {
        assert_eq!(node.get(), if i == 0 { u32::MAX } else { i as u32 - 1 });
        assert_eq!(actual.input(), original.input());
        assert_eq!(actual.binding(), original.binding());
        assert_eq!(actual.payload(), original.payload());
        for (a, b) in actual
            .input()
            .fields_iter()
            .zip(original.input().fields_iter())
        {
            assert!(!std::ptr::eq(a.field(), b.field()));
            assert_eq!(a.field(), b.field());
        }
    }
}

#[test]
fn checked_package_large_metadata_projects_and_reassembles_original_constructor() {
    let field = Field::new("v", DataType::Int64, false)
        .with_metadata(HashMap::from([("large".into(), "雪".repeat(6826) + "ab")]));
    let package = crate::physical_type_v2::sender_tests::checked_writer_package(data(field));
    let (node, recipe) = package.writes().iter().next().unwrap();
    let ids = vec![vec![u32::MAX]];
    let raw = raw_with_nodes(std::slice::from_ref(recipe), &ids, &[*node]);
    assert_eq!(raw.package.writes[0].node_id, Some(node.get()));
    let shape = raw
        .package
        .source_fields
        .as_ref()
        .unwrap()
        .shape
        .as_ref()
        .unwrap();
    assert!(shape.value_types.is_empty() && shape.logical_types.is_empty());
    assert!(
        shape
            .fields
            .iter()
            .all(|field| field.logical_type_id.is_none() && field.value_binding_id.is_none())
    );
    assert_eq!(
        raw.package
            .source_fields
            .as_ref()
            .unwrap()
            .field_facts
            .iter()
            .find(|facts| facts.field_id == Some(u32::MAX))
            .unwrap()
            .metadata[0]
            .value
            .len(),
        20 * 1024
    );
    assert_eq!(
        raw.package.writes[0].input,
        Some(wire::ConnectorWriteInputShape {
            kind: Some(wire::connector_write_input_shape::Kind::Data(
                wire::ConnectorWriteDataInput {
                    fields: vec![fb(0, u32::MAX)]
                }
            ))
        })
    );
    let (decoded, _) = decode_run(&raw, None, limits(), false);
    let (mut decoded, _) = decoded.unwrap();
    let (_, recipe) = decoded.remove(0);
    assert_eq!(
        recipe
            .input()
            .fields_iter()
            .next()
            .unwrap()
            .field()
            .metadata()["large"]
            .len(),
        20 * 1024
    );
    let mut input = package.clone().into_input();
    input.writes = std::collections::BTreeMap::from([(*node, recipe)]);
    let control = Control::default();
    let result = p::FragmentPackage::try_new(
        input,
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: 64 * 1024 * 1024,
                max_coexisting_bytes: 512 * 1024 * 1024,
                max_projection_work: 128 * 1024 * 1024,
            },
        },
        &control,
    )
    .unwrap();
    assert_eq!(
        result.writes()[node].input(),
        package.writes()[node].input()
    );
    // Original whole Package admission, not a full wire Package codec or seal.
}

fn first_field(raw: &mut Raw) -> &mut wire::ConnectorWriteFieldBinding {
    let wire::connector_write_input_shape::Kind::Data(v) = raw.package.writes[0]
        .input
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
        .unwrap()
    else {
        panic!("data fixture")
    };
    &mut v.fields[0]
}
#[test]
fn structural_presence_unknown_references_and_original_token_name_laws_refuse() {
    let recipe = data(Field::new("v", DataType::Int64, false));
    let original = raw(&[recipe], &[vec![0]]);
    for case in 0..13 {
        let mut bad = original.clone();
        match case {
            0 => bad.package.writes[0].node_id = None,
            1 => bad.package.writes[0].provider_binding_id = None,
            2 => bad.package.writes[0].handle_payload_id = None,
            3 => bad.package.writes[0].input = None,
            4 => bad.package.writes[0].input.as_mut().unwrap().kind = None,
            5 => first_field(&mut bad).source_field_id = None,
            6 => first_field(&mut bad).field_token.pop().map(|_| ()).unwrap(),
            7 => first_field(&mut bad).field_token.push(0),
            8 => bad.package.writes.push(bad.package.writes[0].clone()),
            9 => bad.package.writes[0].provider_binding_id = Some(99),
            10 => bad.package.writes[0].handle_payload_id = Some(99),
            11 => first_field(&mut bad).source_field_id = Some(99),
            _ => {
                let field = first_field(&mut bad).clone();
                let wire::connector_write_input_shape::Kind::Data(v) = bad.package.writes[0]
                    .input
                    .as_mut()
                    .unwrap()
                    .kind
                    .as_mut()
                    .unwrap()
                else {
                    unreachable!()
                };
                v.fields.push(field);
            }
        }
        let (result, _) = decode_run(&bad, None, limits(), false);
        assert!(
            matches!(
                result,
                Err(E::InvalidShape(_) | E::Index(_) | E::Provider(_))
            ),
            "case {case}: {result:?}"
        );
    }
    let (result, _) = decode_run(&original, None, limits(), true);
    assert!(matches!(
        result,
        Err(E::InvalidShape(
            "writer provider binding ID is absent from namespace"
        ))
    ));
    // Distinct tokens still cannot turn one original Field name into two roles.
    let mut names = original.clone();
    let mut repeated = first_field(&mut names).clone();
    repeated.field_token = vec![9; 32];
    let wire::connector_write_input_shape::Kind::Data(v) = names.package.writes[0]
        .input
        .as_mut()
        .unwrap()
        .kind
        .as_mut()
        .unwrap()
    else {
        unreachable!()
    };
    v.fields.push(repeated);
    assert!(matches!(
        decode_run(&names, None, limits(), false).0,
        Err(E::Provider(_))
    ));
}

#[test]
fn actual_namespace_provenance_and_write_handle_category_are_not_fallbacks() {
    let original = data(Field::new("v", DataType::Int64, false));
    let foreign = original.clone();
    let ids = vec![vec![0]];
    let control = Control::default();
    with_encoded(std::slice::from_ref(&original), &ids, &control, |context| {
        control.reset(None);
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        let result = encode_writer_recipes_observed(
            &[WriterRecipeSource {
                node: NodeId::new(0),
                recipe: &foreign,
                field_ids: &ids[0],
            }],
            context,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(result, Err(E::Binding(_))));
        work.finish().unwrap();
    });
    let raw = raw(std::slice::from_ref(&original), &ids);
    // A valid payload of another category passes its namespace's own header law
    // and is then refused by the original Writer Draft WriteHandle expectation.
    let other = c::ConnectorEncodedPayload::new(
        c::ConnectorEnvelopeHeader::new(
            original.payload().header().provider_id().clone(),
            original.binding().catalog_handle().clone(),
            c::ConnectorCodecCategory::ReadTable,
            c::ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7u8].into(),
    );
    let inputs = [(u32::MAX, &other)];
    let payloads =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, payload_limits(), &control).unwrap();
    let mut wrong = raw;
    wrong.payloads = payloads.as_wire().to_vec();
    assert!(matches!(
        decode_run(&wrong, None, limits(), false).0,
        Err(E::Provider(_))
    ));
}

fn set_axis(
    l: &mut WriterRecipeProjectionLimits,
    f: WriterRecipeProjectionFacts,
    axis: usize,
    under: bool,
) {
    let decrement = usize::from(under);
    match axis {
        0 => l.max_recipes = f.recipe_count - decrement,
        1 => l.max_input_fields = f.input_field_count - decrement,
        2 => l.max_token_bytes = f.token_bytes - decrement,
        3 => l.max_allocation_requests = f.allocation_requests_upper_bound - decrement,
        4 => l.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound - decrement,
        5 => {
            l.max_coexisting_source_and_request_bytes =
                f.coexisting_source_and_request_bytes_upper_bound - decrement
        }
        _ => l.max_work = f.cumulative_work_upper_bound - decrement,
    }
}
#[test]
fn independent_sender_layout_and_all_seven_exact_under_axes_preserve_resource() {
    let recipes = [data(Field::new("v", DataType::Int64, false))];
    let ids = [vec![0]];
    let (baseline, _) = encode_run(&recipes, &ids, None, limits());
    let (_, facts) = baseline.unwrap();
    let golden = std::alloc::Layout::array::<usize>(1).unwrap().size()
        + std::alloc::Layout::array::<wire::FrozenWriterRecipe>(1)
            .unwrap()
            .size()
        + std::alloc::Layout::array::<wire::ConnectorWriteFieldBinding>(1)
            .unwrap()
            .size()
        + 32;
    assert_eq!(facts.allocation_requests_upper_bound, 4);
    assert_eq!(facts.allocation_request_bytes_upper_bound, golden);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + golden
    );
    let raw = raw(&recipes, &ids);
    let (baseline, _) = decode_run(&raw, None, limits(), false);
    let (_, df) = baseline.unwrap();
    for axis in 0..7 {
        for under in [false, true] {
            let mut l = limits();
            set_axis(&mut l, facts, axis, under);
            let (result, _) = encode_run(&recipes, &ids, None, l);
            if under {
                assert!(matches!(
                    result,
                    Err(E::Control(CompileControlError::ResourceExhausted))
                ));
            } else {
                assert_eq!(result.unwrap().1, facts);
            }
            let mut l = limits();
            set_axis(&mut l, df, axis, under);
            let (result, _) = decode_run(&raw, None, l, false);
            if under {
                assert!(matches!(
                    result,
                    Err(E::Control(CompileControlError::ResourceExhausted))
                ));
            } else {
                assert_eq!(result.unwrap().1, df);
            }
        }
    }
    // The first original Model knows both container requests before its first
    // checkpoint. A hostile later control cannot replace that numerical cause.
    for cause in CAUSES {
        let mut l = limits();
        l.max_allocation_requests = 1;
        let (result, trace) = encode_run(&recipes, &ids, Some((1, cause.clone())), l);
        assert!(matches!(
            result,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(trace, vec![0]);
        let (result, trace) = decode_run(&raw, Some((1, cause)), l, false);
        assert!(matches!(
            result,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(trace, vec![0]);
    }
}

fn every_prefix(
    expected_success: bool,
    run: impl Fn(Option<(usize, CompileControlError)>) -> (Result<(), E>, Vec<u32>),
) {
    let (baseline, trace) = run(None);
    assert_eq!(baseline.is_ok(), expected_success, "baseline: {baseline:?}");
    assert!(!matches!(baseline, Err(E::Control(_))));
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in CAUSES {
            let (result, actual) = run(Some((stop, cause.clone())));
            assert!(matches!(result,Err(E::Control(c)) if c==cause));
            assert_eq!(actual, trace[..=stop]);
        }
    }
}
#[test]
fn every_actual_small_success_and_ordinary_footer_callback_preserves_three_causes() {
    let recipes = [data(Field::new("v", DataType::Int64, false))];
    let ids = [vec![0]];
    let raw = raw(&recipes, &ids);
    every_prefix(true, |stop| {
        let (result, trace) = encode_run(&recipes, &ids, stop, limits());
        (result.map(|_| ()), trace)
    });
    every_prefix(true, |stop| {
        let (result, trace) = decode_run(&raw, stop, limits(), false);
        (result.map(|_| ()), trace)
    });
    let mut bad = raw.clone();
    first_field(&mut bad).field_token.pop();
    assert!(matches!(
        decode_run(&bad, None, limits(), false).0,
        Err(E::InvalidShape(_))
    ));
    every_prefix(false, |stop| {
        let (result, trace) = decode_run(&bad, stop, limits(), false);
        (result.map(|_| ()), trace)
    });
    let short = [vec![]];
    assert!(matches!(
        encode_with_views(&recipes, &ids, &short, None, limits()).0,
        Err(E::InvalidShape(_))
    ));
    every_prefix(false, |stop| {
        let (result, trace) = encode_with_views(&recipes, &ids, &short, stop, limits());
        (result.map(|_| ()), trace)
    });
}

#[test]
fn standalone_writer_source_law_preserves_wide_carrier_but_does_not_author_execution_binding() {
    let children = (0..5000)
        .map(|i| Field::new(format!("n{i}"), DataType::Int64, i % 2 == 0))
        .collect::<Vec<_>>();
    let fields = [
        Field::new("wide", DataType::Struct(children.into()), false),
        Field::new(
            "clock",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(2000).into())),
            true,
        ),
    ];
    // The original standalone Draft test declared external source facts only.
    // It never supplied a complete execution binding or a Package admission.
    let recipes = fields
        .iter()
        .map(|field| {
            draft(c::ConnectorWriteInputShape::Data {
                fields: vec![c::ConnectorWriteFieldBinding::new(
                    c::ConnectorWriteFieldToken::from_bytes([0; 32]),
                    field.clone(),
                )],
            })
        })
        .collect::<Vec<_>>();
    let ids = [vec![0], vec![u32::MAX]];
    let writers = recipes
        .iter()
        .zip(&ids)
        .map(|(recipe, ids)| WriterTypeSource::new(recipe, ids))
        .collect::<Vec<_>>();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let source = encode_borrowed_type_namespaces_sources_in(
        &[],
        &[],
        &[],
        &writers,
        SOURCE,
        type_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap()
    .into_namespaces()
    .1
    .unwrap();
    work.finish().unwrap();
    let shape = source.shape.as_ref().unwrap();
    assert!(shape.value_types.is_empty() && shape.logical_types.is_empty());
    assert_eq!(source.field_facts.len(), shape.fields.len());
    let root = shape.fields.iter().find(|field| field.id == 0).unwrap();
    let carrier = shape
        .carriers
        .iter()
        .find(|carrier| Some(carrier.id) == root.carrier_type_id)
        .unwrap();
    let Some(novarocks_proto_models::physical_type_v2::carrier_type_definition::Kind::StructType(
        children,
    )) = &carrier.kind
    else {
        panic!("source Struct")
    };
    assert_eq!(children.field_ids.len(), 5000);
    let clock = shape
        .fields
        .iter()
        .find(|field| field.id == u32::MAX)
        .unwrap();
    let carrier = shape
        .carriers
        .iter()
        .find(|carrier| Some(carrier.id) == clock.carrier_type_id)
        .unwrap();
    let Some(novarocks_proto_models::physical_type_v2::carrier_type_definition::Kind::Timestamp(
        clock,
    )) = &carrier.kind
    else {
        panic!("source Timestamp")
    };
    assert_eq!(clock.timezone.as_ref().unwrap().len(), 2000);
    for field in &fields {
        assert!(
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                field.data_type().clone(),
                field.is_nullable(),
                novarocks_type_contract::ValueLogicalType::Physical
            )
            .is_ok(),
            "wide legal types must also be representable as values"
        );
    }
    let baseline = control.trace();
    for stop in [0, baseline.len() / 2, baseline.len() - 1] {
        for cause in CAUSES {
            control.reset(Some((stop, cause.clone())));
            let mut work = match CompileCheckpoints::try_new(&control, CompilePhase::Encode) {
                Ok(work) => work,
                Err(actual) => {
                    assert_eq!(actual, cause);
                    assert_eq!(control.trace(), baseline[..=stop]);
                    continue;
                }
            };
            let result = encode_borrowed_type_namespaces_sources_in(
                &[],
                &[],
                &[],
                &writers,
                SOURCE,
                type_limits(),
                &mut |_| Ok(()),
                &mut work,
            );
            if result.is_ok() {
                assert_eq!(work.finish().unwrap_err(), cause);
            } else {
                assert!(
                    matches!(result,Err(crate::physical_type_v2::TypeCodecError::Control(actual)) if actual==cause)
                );
            }
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}

#[test]
fn source_invoice_and_original_control_loans_remain_explicit() {
    let recipes = [data(Field::new("v", DataType::Int64, false))];
    let ids = [vec![0]];
    let control = Control::default();
    with_encoded(&recipes, &ids, &control, |context| {
        let sources = [WriterRecipeSource {
            node: NodeId::new(0),
            recipe: &recipes[0],
            field_ids: &ids[0],
        }];
        control.reset(None);
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        let result = encode_writer_recipes_observed(
            &sources,
            WriterRecipeEncodeContext {
                bindings: context.bindings,
                payloads: context.payloads,
                types: context.types,
            },
            0,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(E::InvalidShape(
                "writer recipe source invoice is understated"
            ))
        ));
        work.finish().unwrap();
        let foreign = Control::default();
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
        let result = encode_writer_recipes_observed(
            &sources,
            context,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(E::InvalidShape(
                "writer recipe namespaces use another original control"
            ))
        ));
        work.finish().unwrap();
    });
}

// The original statistics seed has 1024 fields. Check both one wide writer
// and cumulative fields from two writers, with independent token identities.
#[test]
fn production_writer_policy_round_trips_wide_and_cumulative_field_tokens() {
    for widths in [&[1024usize][..], &[512usize, 512][..]] {
        let mut ordinal = 0u32;
        let mut ids = Vec::new();
        let recipes = widths
            .iter()
            .map(|&width| {
                let mut source_ids = Vec::new();
                let fields = (0..width)
                    .map(|_| {
                        let id = ordinal;
                        ordinal += 1;
                        source_ids.push(id);
                        let mut token = [0; 32];
                        token[..4].copy_from_slice(&id.to_le_bytes());
                        c::ConnectorWriteFieldBinding::try_new_typed(
                            c::ConnectorWriteFieldToken::from_bytes(token),
                            Field::new(format!("v{id}"), DataType::Int64, false),
                            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                        )
                        .unwrap()
                    })
                    .collect();
                ids.push(source_ids);
                draft(c::ConnectorWriteInputShape::Data { fields })
            })
            .collect::<Vec<_>>();
        let (encoded, _) = encode_run(&recipes, &ids, None, None);
        let (encoded, facts) = encoded.unwrap();
        assert_eq!(facts.recipe_count, widths.len());
        assert_eq!(facts.input_field_count, 1024);
        assert_eq!(facts.token_bytes, 32768);
        let raw = raw(&recipes, &ids);
        assert_eq!(encoded, raw.package.writes);
        let (decoded, _) = decode_run(&raw, None, None, false);
        let (decoded, decoded_facts) = decoded.unwrap();
        assert_eq!(decoded.len(), widths.len());
        assert_eq!(decoded_facts.recipe_count, widths.len());
        assert_eq!(decoded_facts.input_field_count, 1024);
        assert_eq!(decoded_facts.token_bytes, 32768);
        for (index, ((node, actual), expected)) in decoded.iter().zip(&recipes).enumerate() {
            assert_eq!(
                node.get(),
                if index == 0 {
                    u32::MAX
                } else {
                    index as u32 - 1
                }
            );
            assert_eq!(actual.input(), expected.input());
            assert_eq!(actual.binding(), expected.binding());
            assert_eq!(actual.payload(), expected.payload());
        }
        let finite_encode = WriterRecipeProjectionLimits {
            max_token_bytes: 32000,
            ..limits()
        };
        let finite_decode = WriterRecipeProjectionLimits {
            max_token_bytes: 32000,
            ..limits()
        };
        assert!(matches!(
            encode_run(&recipes, &ids, None, finite_encode).0,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            decode_run(&raw, None, finite_decode, false).0,
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
}

fn fixed_storage_field(token: u8, nested: bool) -> c::ConnectorWriteFieldBinding {
    let (input, storage) = if nested {
        (
            DataType::Struct(vec![Field::new("leaf", DataType::Binary, false)].into()),
            DataType::Struct(vec![Field::new("leaf", DataType::FixedSizeBinary(4), false)].into()),
        )
    } else {
        (DataType::Binary, DataType::FixedSizeBinary(4))
    };
    c::ConnectorWriteFieldBinding::try_new_typed(
        c::ConnectorWriteFieldToken::from_bytes([token; 32]),
        Field::new("raw", input.clone(), false).with_metadata(std::collections::HashMap::from([(
            "source-only".to_string(),
            "untrusted-nominal-tag".to_string(),
        )])),
        novarocks_type_contract::FunctionValueType::new(input, false),
    )
    .unwrap()
    .with_storage_type(novarocks_type_contract::FunctionValueType::new(
        storage, false,
    ))
    .unwrap()
}

#[test]
fn complete_storage_targets_roundtrip_independently_from_input_and_metadata() {
    for nested in [false, true] {
        let recipes = [draft(c::ConnectorWriteInputShape::Data {
            fields: vec![fixed_storage_field(31, nested)],
        })];
        let ids = [vec![7]];
        let raw = raw(&recipes, &ids);
        let input = raw.package.writes[0].input.as_ref().unwrap();
        let wire::connector_write_input_shape::Kind::Data(data) = input.kind.as_ref().unwrap()
        else {
            unreachable!()
        };
        assert_ne!(
            data.fields[0].storage_value_type_id,
            data.fields[0].value_type_id
        );
        assert!(data.fields[0].storage_value_type_id.is_some());
        let (decoded, _) = decode_run(&raw, None, limits(), false);
        let (decoded, _) = decoded.unwrap();
        let actual = decoded[0].1.input().fields_iter().next().unwrap();
        let expected = recipes[0].input().fields_iter().next().unwrap();
        assert_eq!(actual.execution_type(), expected.execution_type());
        assert_eq!(actual.storage_type(), expected.storage_type());
        assert_eq!(
            actual.field().metadata()["source-only"],
            "untrusted-nominal-tag"
        );
        assert_ne!(actual.execution_type(), actual.storage_type());
    }
}

#[test]
fn storage_target_requires_its_original_source_author_even_for_an_equal_type() {
    let recipes = [draft(c::ConnectorWriteInputShape::Data {
        fields: vec![fixed_storage_field(32, false)],
    })];
    let ids = [vec![7]];
    let foreign = recipes[0]
        .input()
        .fields_iter()
        .next()
        .unwrap()
        .storage_type()
        .unwrap()
        .clone();
    with_encoded_storage_roots(
        &recipes,
        &ids,
        &Control::default(),
        Some(&foreign),
        false,
        |context| {
            let sources = [WriterRecipeSource {
                node: NodeId::new(1),
                recipe: &recipes[0],
                field_ids: &ids[0],
            }];
            let mut work = CompileCheckpoints::try_new(
                context.bindings.original_control(),
                CompilePhase::Encode,
            )
            .unwrap();
            let error = encode_writer_recipes_observed(
                &sources,
                context,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap_err();
            assert!(matches!(
                error,
                E::InvalidShape("writer storage value ID has no original execution author")
            ));
        },
    );
}

#[test]
fn unknown_storage_target_reference_is_rejected_in_original_preflight() {
    let recipes = [draft(c::ConnectorWriteInputShape::Data {
        fields: vec![fixed_storage_field(33, false)],
    })];
    let mut raw = raw(&recipes, &[vec![7]]);
    first_field(&mut raw).storage_value_type_id = Some(u32::MAX);
    let (result, _) = decode_run(&raw, None, limits(), false);
    assert!(matches!(
        result,
        Err(E::InvalidShape(
            "writer storage value binding ID is absent from execution namespace"
        ))
    ));
}

#[test]
fn explicit_storage_declaration_is_not_lost_on_non_data_roles() {
    let recipes = five();
    let mut raw = raw(&recipes, &five_ids());
    let input = raw.package.writes[4].input.as_mut().unwrap();
    let wire::connector_write_input_shape::Kind::EqualityDelete(input) =
        input.kind.as_mut().unwrap()
    else {
        unreachable!()
    };
    input.equality_fields[0].storage_value_type_id = Some(0);
    let (decoded, _) = decode_run(&raw, None, limits(), false);
    assert!(
        matches!(decoded, Err(E::Provider(ref error)) if error.message().contains("only admitted for data fields"))
    );
}

#[test]
fn shared_original_storage_loan_can_have_multiple_stable_root_ids() {
    let recipes = [draft(c::ConnectorWriteInputShape::Data {
        fields: vec![fixed_storage_field(34, false)],
    })];
    let ids = [vec![7]];
    let control = Control::default();
    with_encoded_storage_roots(&recipes, &ids, &control, None, true, |context| {
        let storage = recipes[0]
            .input()
            .fields_iter()
            .next()
            .unwrap()
            .storage_type()
            .unwrap();
        let mut work =
            CompileCheckpoints::try_new(context.bindings.original_control(), CompilePhase::Encode)
                .unwrap();
        let mut aliases = Vec::new();
        context
            .types
            .visit_value_type_sources(
                &mut |id, value, _| -> Result<(), E> {
                    if std::ptr::eq(storage, value) {
                        aliases.push(id);
                    }
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        assert_eq!(
            aliases.len(),
            2,
            "both roots retain the same real immutable FVT loan"
        );
        let sources = [WriterRecipeSource {
            node: NodeId::new(1),
            recipe: &recipes[0],
            field_ids: &ids[0],
        }];
        for _ in 0..2 {
            let (encoded, _) = encode_writer_recipes_observed(
                &sources,
                WriterRecipeEncodeContext {
                    bindings: context.bindings,
                    payloads: context.payloads,
                    types: context.types,
                },
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
            let wire::connector_write_input_shape::Kind::Data(data) =
                encoded[0].input.as_ref().unwrap().kind.as_ref().unwrap()
            else {
                unreachable!()
            };
            assert_eq!(data.fields[0].storage_value_type_id, Some(aliases[0]));
            let raw = Raw {
                type_roots: encoded.clone(),
                providers: context.bindings.as_wire().to_vec(),
                payloads: context.payloads.as_wire().to_vec(),
                package: wire::FragmentPackage {
                    types: Some(context.types.as_wire().clone()),
                    source_fields: context.types.as_source_wire().cloned(),
                    writes: encoded,
                    ..Default::default()
                },
            };
            let (decoded, _) = decode_run(&raw, None, limits(), false);
            let (decoded, _) = decoded.unwrap();
            assert_eq!(
                decoded[0]
                    .1
                    .input()
                    .fields_iter()
                    .next()
                    .unwrap()
                    .storage_type(),
                Some(storage)
            );
        }
    });
}

#[test]
fn explicit_storage_root_retains_nominal_identity_without_reading_field_tags() {
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let nominal = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let field = c::ConnectorWriteFieldBinding::try_new_typed(
        c::ConnectorWriteFieldToken::from_bytes([35; 32]),
        Field::new("nominal", DataType::FixedSizeBinary(16), false).with_metadata(HashMap::from([
            ("novarocks.logical_type".into(), "uuid".into()),
        ])),
        nominal.clone(),
    )
    .unwrap()
    .with_storage_type(nominal)
    .unwrap();
    let recipes = [draft(c::ConnectorWriteInputShape::Data {
        fields: vec![field],
    })];
    let raw = raw(&recipes, &[vec![7]]);
    let (decoded, _) = decode_run(&raw, None, limits(), false);
    let (decoded, _) = decoded.unwrap();
    let actual = decoded[0].1.input().fields_iter().next().unwrap();
    assert_eq!(
        actual.execution_type().unwrap().logical_type(),
        ValueLogicalType::LargeInt
    );
    assert_eq!(
        actual.storage_type().unwrap().logical_type(),
        ValueLogicalType::LargeInt
    );
    assert_eq!(actual.field().metadata()["novarocks.logical_type"], "uuid");
}
