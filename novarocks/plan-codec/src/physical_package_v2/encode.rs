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

//! Whole-package sender from a checked FragmentPackage. Structural components
//! retain their own frozen limits. Snapshot data is a separate final component
//! funded by its actual creator, without a whole-DTO size quota.

use super::binding_sources::collect_binding_sources_in;
use super::definition_sources::collect_definition_sources_in;
use super::nodes::{NodeDispatchLimits, NodeEncodeContext, prepare_node_encode_in};
use super::provider_sources::{ProviderSourceError, collect_provider_sources_in};
use super::type_views::{TypeViewBudget, TypeViewError, collect_package_type_views_in};
use crate::physical_aggregate_binding_v2::encode_aggregate_bindings_in;
use crate::physical_binding_v2::{BindingCodecError, encode_function_bindings_in};
use crate::physical_call_requests_v2::{CallRequestCodecError, prepare_call_requests_encode_in};
use crate::physical_connector_payload_v2::{
    ConnectorPayloadCodecError, encode_connector_payloads_in,
};
use crate::physical_constant_v2::{
    ConstantWriteRequest, PhysicalConstantCodecError, encode_constant_namespace_input_derived_in,
};
use crate::physical_control_v2::{ControlCodecError, encode_expression_control_observed};
use crate::physical_cuts_v2::{
    CutsProjectionLimits, EncodedCutsContext, encode_fragment_cuts_observed,
};
use crate::physical_expression_v2::{ExpressionCodecError, prepare_expression_definitions_in};
use crate::physical_fragment_envelope_v2::prepare_fragment_envelope_encode_in;
use crate::physical_node_v2::NodeCodecError;
use crate::physical_package_metadata_v2::{
    PackageMetadataCodecError, encode_package_metadata_observed,
};
use crate::physical_provider_binding_v2::{
    ProviderBindingCodecError, encode_joint_provider_bindings_in,
};
use crate::physical_provider_read_v2::{ProviderReadCodecError, encode_provider_reads_in};
use crate::physical_public_source_schema_v2::{
    PublicSourceSchemaCodecError, encode_public_source_schemas_pure_in,
    encoded_public_source_schemas_backing_bytes,
};
use crate::physical_read_scan_v2::{
    ReadScanCodecError, ReadScanEncodeContext, encode_read_scans_observed,
};
use crate::physical_relation_v2::{RelationCodecError, encode_relations_in};
use crate::physical_result_v2::{prepare_result_encode_in, result_source_backing};
use crate::physical_schema_v2::{SchemaCodecError, prepare_schemas_encode_observed_in};
use crate::physical_semantics_v2::{
    SemanticsCodecError, encode_frozen_calls_observed, encode_frozen_pruning_observed,
    prepare_semantic_parameters_encode_observed_in,
};
use crate::physical_type_v2::{TypeCodecError, encode_borrowed_type_namespaces_sources_in};
use crate::physical_value_v2::{ValueCodecError, encode_values_observed_in};
use crate::physical_writer_recipe_v2::{
    WriterRecipeCodecError, WriterRecipeEncodeContext, encode_writer_recipes_observed,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
#[cfg(test)]
use prost::Message;
use std::{cell::Cell, fmt};

#[derive(Debug)]
pub enum PackageEncodeError {
    Control(CompileControlError),
    SystemRelation(super::system_relation::SystemRelationError),
    Source(TypeViewError),
    Provider(ProviderSourceError),
    Metadata(PackageMetadataCodecError),
    Type(TypeCodecError),
    Binding(BindingCodecError),
    ProviderBinding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Read(ProviderReadCodecError),
    Relation(RelationCodecError),
    Schema(SchemaCodecError),
    PublicSourceSchema(PublicSourceSchemaCodecError),
    Constant(PhysicalConstantCodecError),
    Semantics(SemanticsCodecError),
    Value(ValueCodecError),
    Expression(ExpressionCodecError),
    Request(CallRequestCodecError),
    Node(NodeCodecError),
    ExpressionControl(ControlCodecError),
    Scan(ReadScanCodecError),
    Writer(WriterRecipeCodecError),
}
impl From<super::system_relation::SystemRelationError> for PackageEncodeError {
    fn from(error: super::system_relation::SystemRelationError) -> Self {
        match error {
            super::system_relation::SystemRelationError::Control(e) => Self::Control(e),
            e => Self::SystemRelation(e),
        }
    }
}
impl From<CompileControlError> for PackageEncodeError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
// Every component keeps its own primary control cause primary here.
macro_rules! component_error {
    ($($variant:ident($error:ident)),* $(,)?) => {$(
        impl From<$error> for PackageEncodeError {
            fn from(error: $error) -> Self {
                match error {
                    $error::Control(cause) => Self::Control(cause),
                    other => Self::$variant(other),
                }
            }
        }
    )*};
}
component_error!(
    Source(TypeViewError),
    Provider(ProviderSourceError),
    Metadata(PackageMetadataCodecError),
    Type(TypeCodecError),
    Binding(BindingCodecError),
    ProviderBinding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Read(ProviderReadCodecError),
    Relation(RelationCodecError),
    Schema(SchemaCodecError),
    PublicSourceSchema(PublicSourceSchemaCodecError),
    Constant(PhysicalConstantCodecError),
    Semantics(SemanticsCodecError),
    Value(ValueCodecError),
    Expression(ExpressionCodecError),
    Request(CallRequestCodecError),
    Node(NodeCodecError),
    ExpressionControl(ControlCodecError),
    Scan(ReadScanCodecError),
    Writer(WriterRecipeCodecError),
);
impl fmt::Display for PackageEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::SystemRelation(e) => e.fmt(f),
            Self::Source(e) => e.fmt(f),
            Self::Provider(e) => write!(f, "{e:?}"),
            Self::Metadata(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::ProviderBinding(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Read(e) => e.fmt(f),
            Self::Relation(e) => e.fmt(f),
            Self::Schema(e) => e.fmt(f),
            Self::PublicSourceSchema(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Semantics(e) => e.fmt(f),
            Self::Value(e) => e.fmt(f),
            Self::Expression(e) => e.fmt(f),
            Self::Request(e) => e.fmt(f),
            Self::Node(e) => e.fmt(f),
            Self::ExpressionControl(e) => e.fmt(f),
            Self::Scan(e) => e.fmt(f),
            Self::Writer(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for PackageEncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::SystemRelation(source) => Some(source),
            Self::Source(source) => Some(source),
            Self::Provider(source) => Some(source),
            Self::Metadata(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::ProviderBinding(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Read(source) => Some(source),
            Self::Relation(source) => Some(source),
            Self::Schema(source) => Some(source),
            Self::PublicSourceSchema(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Semantics(source) => Some(source),
            Self::Value(source) => Some(source),
            Self::Expression(source) => Some(source),
            Self::Request(source) => Some(source),
            Self::Node(source) => Some(source),
            Self::ExpressionControl(source) => Some(source),
            Self::Scan(source) => Some(source),
            Self::Writer(source) => Some(source),
        }
    }
}

/// Encode one checked package on its own Encode scope of `control`.
pub fn encode_fragment_package(
    package: &p::FragmentPackage,
    control: &dyn PureCompileControl,
) -> Result<wire::FragmentPackage, PackageEncodeError> {
    let mut request = |_: std::alloc::Layout| Ok::<_, std::convert::Infallible>(());
    let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut opaque,
    );
    encode_fragment_package_with_creator(package, &mut creation, control)
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

pub fn encode_fragment_package_with_creator<H>(
    package: &p::FragmentPackage,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    control: &dyn PureCompileControl,
) -> Result<
    wire::FragmentPackage,
    crate::host_projection_v2::ProjectionFailure<PackageEncodeError, H>,
> {
    use crate::host_projection_v2::ProjectionFailure;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)
        .map_err(|e| ProjectionFailure::Codec(PackageEncodeError::Control(e)))?;
    let result = (|| {
        let mut original_host = None;
        let encoded = encode_fragment_package_structural_in(package, &mut work, &mut |request| {
            let admitted = match request {
                ConstantWriteRequest::Layout(layout) if layout.size() != 0 => {
                    creation.request_layout(layout)
                }
                ConstantWriteRequest::OpaqueUpperBound(bytes) if bytes != 0 => {
                    creation.observe_opaque_upper_bound(bytes)
                }
                _ => Ok(()),
            };
            match admitted {
                Ok(()) => Ok(()),
                Err(error) => {
                    original_host = Some(error);
                    Err(PackageEncodeError::Control(
                        CompileControlError::ResourceExhausted,
                    ))
                }
            }
        });
        if let Some(error) = original_host {
            return Err(ProjectionFailure::Host(error));
        }
        let mut output = encoded.map_err(ProjectionFailure::Codec)?;
        super::system_relation::encode_units(package, &mut output, creation, &mut work)
            .map_err(|e| super::system_relation::map_failure(e, PackageEncodeError::from))?;
        Ok(output)
    })();
    // Complete only successful work. Every already-authored codec or creator
    // refusal remains primary; a later footer must not replace that cause.
    work.finish_result(result, |error| {
        ProjectionFailure::Codec(PackageEncodeError::Control(error))
    })
}

fn encode_fragment_package_structural_in<'s>(
    package: &'s p::FragmentPackage,
    work: &mut CompileCheckpoints<'s>,
    request: &mut dyn FnMut(ConstantWriteRequest) -> Result<(), PackageEncodeError>,
) -> Result<wire::FragmentPackage, PackageEncodeError> {
    let fragment = package.fragment();
    if fragment.nodes().values().any(|node| {
        matches!(
            &node.kind,
            p::NodeKind::SystemRelation {
                data: p::SystemRelationData::Deferred,
                ..
            }
        )
    }) {
        return Err(super::system_relation::SystemRelationError::Deferred.into());
    }

    // This collector borrows the package's original sources. Its allocation
    // facts cover only the local request geometry for its indexes.
    let mut collection = |_: &_| Ok(());
    let mut budget = TypeViewBudget::new_input_derived_in(package, &mut collection, work)?;
    let views = collect_package_type_views_in(&mut budget, work)?;
    let writer_types = views.writer_sources_in(&mut budget, work)?;
    let bindings = collect_binding_sources_in(package, &views, None, &mut budget, work)?;
    let providers =
        collect_provider_sources_in(package, &views, &writer_types, None, &mut budget, work)?;
    let definitions =
        collect_definition_sources_in(package, &views, &bindings, None, &mut budget, work)?;
    let arguments = bindings.argument_inputs_in(&mut budget, work)?;
    let function_inputs = arguments.function_inputs_in(&mut budget, work)?;
    let aggregate_inputs = bindings.aggregate_inputs_in(&mut budget, work)?;
    let provider_inputs = providers.prepare_inputs_in(&mut budget, work)?;
    let request_arguments = definitions.request_arguments_in(&mut budget, work)?;
    let request_ids = request_arguments.request_type_ids_in(&mut budget, work)?;
    let mut writer_ids = Vec::new();
    writer_ids
        .try_reserve_exact(fragment.nodes().len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for node in fragment.nodes().values() {
        writer_ids.push(definitions.writer_type_ids_in(node, &mut budget, work)?);
        work.step()?;
    }
    // Every later component's source invoice includes the collection buffers
    // that stay live through encoding.
    let mut source = add(
        budget
            .facts()
            .coexisting_source_and_request_bytes_upper_bound,
        result_source_backing(package.result(), work)?,
    )?;

    // Original component encoders in hard dependency order. A completed
    // namespace stays live for later components, so its requested-backing
    // upper bound and inline header join every later invoice once
    // (conservative: temporary requests are included).
    let requested = Cell::new(0usize);
    let mut unit_roots = 0;
    macro_rules! seen {
        ($field:ident) => {
            &mut |f: &_| {
                requested.set(requested.get().max(f.$field));
                Ok(())
            }
        };
        ($field:ident, funded) => {
            &mut |facts: &_| {
                let bytes = facts.$field;
                if bytes > requested.get() {
                    request(ConstantWriteRequest::OpaqueUpperBound(
                        bytes - requested.get(),
                    ))
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                    requested.set(bytes);
                }
                Ok(())
            }
        };
    }
    let (metadata, _) = encode_package_metadata_observed(
        package,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &metadata)?;
    let types = encode_borrowed_type_namespaces_sources_in(
        views.values(),
        views.fields(),
        views.source_fields(),
        &writer_types,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &types)?;
    let functions = encode_function_bindings_in(
        &types,
        &function_inputs,
        source,
        None,
        seen!(request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &functions)?;
    let aggregates = encode_aggregate_bindings_in(
        &types,
        &functions,
        &aggregate_inputs,
        source,
        None,
        seen!(request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &aggregates)?;
    let provider_bindings = encode_joint_provider_bindings_in(
        providers.provider_inputs(),
        source,
        None,
        seen!(allocation_request_bytes_upper_bound, funded),
        work,
    )?;
    source = grow(source, &requested, &provider_bindings)?;
    let payloads = encode_connector_payloads_in(
        providers.payload_inputs(),
        source,
        None,
        seen!(allocation_request_bytes_upper_bound, funded),
        work,
    )?;
    source = grow(source, &requested, &payloads)?;
    let reads = encode_provider_reads_in(
        providers.read_inputs(),
        &provider_bindings,
        &payloads,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound, funded),
        work,
    )?;
    source = grow(source, &requested, &reads)?;
    let relations = encode_relations_in(
        provider_inputs.relations(),
        &reads,
        &types,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &relations)?;
    let schemas = prepare_schemas_encode_observed_in(
        provider_inputs.schemas(),
        &types,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_observed_in(seen!(allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &schemas)?;
    let public_source_schemas =
        encode_public_source_schemas_pure_in(provider_inputs.source_schemas(), work)?;
    source = source
        .checked_add(encoded_public_source_schemas_backing_bytes(
            &public_source_schemas,
        )?)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let constants = encode_constant_namespace_input_derived_in(
        package.constants(),
        definitions.constant_type_ids(),
        &types,
        request,
        work,
    )
    .map_err(|error| match error {
        crate::host_projection_v2::ProjectionFailure::Codec(cause) => {
            PackageEncodeError::from(cause)
        }
        crate::host_projection_v2::ProjectionFailure::Host(cause) => cause,
    })?;
    source = grow(source, &requested, &constants)?;
    let (parameters, _) = prepare_semantic_parameters_encode_observed_in(
        package.parameters(),
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_observed_in(seen!(allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &parameters)?;
    let values = encode_values_observed_in(
        definitions.values(),
        &payloads,
        &types,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &values)?;
    let call_requests_token = prepare_call_requests_encode_in(
        fragment.call_requests(),
        &types,
        &request_ids,
        package.constants(),
        source,
        None,
        &mut |facts: &crate::physical_call_requests_v2::CallRequestProjectionFacts| {
            requested.set(requested.get().max(facts.request_bytes_upper_bound));
            Ok(())
        },
        work,
    )?;

    let call_requests = call_requests_token.emit_in(seen!(request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &call_requests)?;
    let (envelope, _) = prepare_fragment_envelope_encode_in(
        fragment,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_in(seen!(allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &envelope)?;
    let dispatch = NodeDispatchLimits {
        node: None,
        binding: None,
        relation: None,
        writer_schema: None,
    };
    let mut nodes = Vec::new();
    nodes
        .try_reserve_exact(fragment.nodes().len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    source = add(
        source,
        std::mem::size_of::<wire::PhysicalNode>() * nodes.capacity(),
    )?;
    for (node, ids) in fragment.nodes().values().zip(writer_ids) {
        let expression_ids = definitions.expressions_in(node.id, &mut budget, work)?;
        let expressions = prepare_expression_definitions_in(
            node.expressions
                .as_deref()
                .map(crate::physical_expression_v2::ExpressionDefinitionSource::from_unit),
            &expression_ids,
            &types,
            &functions,
            &aggregates,
            package.parameters(),
            package.constants(),
            source,
            None,
            &mut |facts: &crate::physical_expression_v2::ExpressionNamespaceWriteFacts| {
                requested.set(
                    requested
                        .get()
                        .max(facts.new_allocation_request_bytes_upper_bound),
                );
                Ok(())
            },
            work,
        )?
        .emit_in(
            &mut |facts: &crate::physical_expression_v2::ExpressionNamespaceWriteFacts| {
                requested.set(
                    requested
                        .get()
                        .max(facts.new_allocation_request_bytes_upper_bound),
                );
                Ok(())
            },
            work,
        )?;

        source = grow(source, &requested, &expressions)?;
        let context = NodeEncodeContext {
            values: &values,
            expressions: &expressions,
            relations: &relations,
        };
        let (mut encoded, _) = prepare_node_encode_in(
            node,
            &context,
            ids,
            source,
            dispatch,
            seen!(allocation_request_bytes_upper_bound),
            work,
        )?
        .emit_in(seen!(allocation_request_bytes_upper_bound), work)?;
        if let Some(unit) = node.expressions.as_deref() {
            super::unit_headers::admit_fragment_roots(&mut unit_roots, unit.roots().len())?;
            let _ = super::unit_headers::encode(
                unit,
                source,
                None,
                seen!(allocation_request_bytes_upper_bound),
                work,
            )?;
            source = add(source, requested.replace(0))?;
            let unit_request_ids =
                request_arguments.unit_request_type_ids_in(node.id, &mut budget, work)?;
            let mut ports = Vec::new();
            ports
                .try_reserve_exact(unit.ports().len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for (port, input) in unit.ports().iter().enumerate() {
                let mut values = Vec::new();
                values
                    .try_reserve_exact(input.values.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                for (ordinal, (value, ty)) in input.values.iter().enumerate() {
                    let value_type_id = views.value_root_for_in(
                        super::type_sources::PackageTypeOccurrence {
                            fragment: fragment.id(),
                            owner: super::type_sources::PackageTypeOwner::UnitPort {
                                node: node.id,
                                port,
                                ordinal,
                            },
                            channel: super::type_sources::PackageTypeChannel::Value,
                        },
                        ty,
                        &mut budget,
                        work,
                    )?;
                    values.push(wire::ExpressionInput {
                        value_id: Some(value.get()),
                        value_type_id: Some(value_type_id),
                    });
                    work.step()?;
                }
                ports.push(wire::ExpressionPort { values });
                work.step()?;
            }
            let mut roots = Vec::new();
            roots
                .try_reserve_exact(unit.roots().len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for (root, use_id) in unit.roots().iter().zip(unit.root_use_ids()) {
                let kind = match root.scope {
                    novarocks_expression_contract::RootScope::Port(port) => {
                        wire::root_scope::Kind::Port(port)
                    }
                    novarocks_expression_contract::RootScope::AllPorts => {
                        wire::root_scope::Kind::AllPorts(
                            novarocks_proto_models::physical_control_v2::Empty {},
                        )
                    }
                };
                roots.push(wire::ExpressionRoot {
                    definition_id: Some(root.definition.get()),
                    demand: crate::physical_control_v2::encode_demand(root.demand),
                    scope: Some(wire::RootScope { kind: Some(kind) }),
                    use_id: Some(use_id.get()),
                });
                work.step()?;
            }
            let expansions = unit.control_expansions().records();
            let mut control_expansions = Vec::new();
            control_expansions
                .try_reserve_exact(expansions.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for expansion in expansions {
                control_expansions.push(wire::SourceControlExpansion {
                    root_expr_id: Some(expansion.root.get()),
                    positive_expr_id: Some(expansion.positive.get()),
                    conjunction_expr_id: Some(expansion.conjunction.get()),
                    first_branch_expr_id: Some(expansion.branches[0].get()),
                    second_branch_expr_id: Some(expansion.branches[1].get()),
                });
                work.step()?;
            }
            let (control, _) = encode_expression_control_observed(
                unit.control_flow(),
                source,
                None,
                &mut |facts: &crate::physical_control_v2::ControlProjectionFacts| {
                    requested.set(
                        requested
                            .get()
                            .max(facts.allocation_request_bytes_upper_bound),
                    );
                    Ok(())
                },
                work,
            )?;

            source = grow(source, &requested, &control)?;
            let (claims, _) = crate::physical_semantics_v2::encode_expression_claims_in(
                unit,
                source,
                None,
                &mut |facts: &crate::physical_node_v2::NodeProjectionFacts| {
                    requested.set(
                        requested
                            .get()
                            .max(facts.allocation_request_bytes_upper_bound),
                    );
                    Ok(())
                },
                work,
            )?;

            source = grow(source, &requested, &claims)?;
            let (requests, _) = crate::physical_call_requests_v2::encode_unit_requests_in(
                unit,
                &types,
                &unit_request_ids,
                package.constants(),
                source,
                None,
                &mut |facts: &crate::physical_call_requests_v2::CallRequestProjectionFacts| {
                    requested.set(requested.get().max(facts.request_bytes_upper_bound));
                    Ok(())
                },
                work,
            )?;

            source = grow(source, &requested, &requests)?;
            encoded.expressions = Some(wire::ExpressionUnit {
                ports,
                definitions: expressions.into_wire(),
                roots,
                control_expansions,
                control: Some(control),
                claims: Some(claims),
                requests,
            });
        }
        // Each emitted node stays live in the output table.
        source = add(source, requested.replace(0))?;
        nodes.push(encoded);
    }
    let (calls, _) = encode_frozen_calls_observed(
        package.calls(),
        source,
        None,
        &mut |facts: &crate::physical_node_v2::NodeProjectionFacts| {
            requested.set(
                requested
                    .get()
                    .max(facts.allocation_request_bytes_upper_bound),
            );
            Ok(())
        },
        work,
    )?;

    source = grow(source, &requested, &calls)?;
    let (pruning, _) = encode_frozen_pruning_observed(
        package.pruning(),
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &pruning)?;
    let cut_ids = definitions.cuts_type_ids();
    let (cuts, _) = encode_fragment_cuts_observed(
        package.cuts(),
        EncodedCutsContext {
            types: &types,
            type_ids: &cut_ids,
            source_retained_bytes: source,
            limits: CutsProjectionLimits {
                node: None,
                binding: None,
            },
        },
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &cuts)?;
    let (result, _) = prepare_result_encode_in(
        package.result(),
        definitions.result_type_ids(),
        &values,
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_in(seen!(allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &result)?;
    let (scans, connector_expressions, _) = encode_read_scans_observed(
        provider_inputs.scans(),
        ReadScanEncodeContext {
            bindings: &provider_bindings,
            payloads: &payloads,
            schemas: &schemas,
            source_schemas: provider_inputs.source_schemas(),
        },
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &scans)?;
    let (writes, _) = encode_writer_recipes_observed(
        provider_inputs.writers(),
        WriterRecipeEncodeContext {
            bindings: &provider_bindings,
            payloads: &payloads,
            types: &types,
        },
        source,
        None,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;

    let layouts = if package.layouts().is_empty() {
        Vec::new()
    } else {
        let source = grow(source, &requested, &writes)?;
        crate::physical_writer_recipe_v2::encode_write_layouts_in(
            package.layouts(),
            &provider_bindings,
            &payloads,
            source,
            None,
            work,
        )?
        .0
    };
    let system_relation_units = super::system_relation::encode_headers(
        package,
        &views,
        &mut budget,
        &mut |layout| request(ConstantWriteRequest::Layout(layout)),
        work,
    )?;
    // Namespaces borrow earlier ones; release them in reverse dependency order.
    let values = values.into_wire();
    let aggregate_bindings = aggregates.into_wire();
    let function_bindings = functions.into_wire();
    let relations = relations.into_wire();
    let read_references = reads.into_wire();
    let schemas = schemas.into_wire();
    let provider_bindings = provider_bindings.into_wire();
    let provider_payloads = payloads.into_wire();
    let (types, source_fields) = types.into_namespaces();
    let output = wire::FragmentPackage {
        public_source_schemas,
        plan_version: metadata.plan_version,
        required: Some(metadata.required),
        fragment: Some(wire::Fragment {
            id: envelope.id,
            root_node_id: Some(envelope.root_node_id),
            values,
            nodes,
            sink: Some(envelope.sink),
            summaries: envelope.summaries,
            dop_domain: Some(envelope.dop_domain),
            stage_placement: Some(envelope.stage_placement),
            runtime_filter_ids: envelope.runtime_filter_ids,
            call_requests: Some(call_requests),
        }),
        types: Some(types),
        source_fields,
        schemas,
        constants,
        function_bindings,
        aggregate_bindings,
        provider_bindings,
        provider_payloads,
        read_references,
        relations,
        calls: Some(calls),
        pruning: Some(pruning),
        parameters: Some(parameters),
        cuts: Some(cuts),
        result,
        scans,
        writes,
        layouts,
        annotations: metadata.annotations,
        connector_expressions,
        system_relation_units,
    };
    work.step()?;
    Ok(output)
}

fn add(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
/// Next cumulative invoice after one namespace completes: its largest
/// admitted request upper bound plus its inline header, counted once.
fn grow<T>(
    source: usize,
    requested: &Cell<usize>,
    namespace: &T,
) -> Result<usize, CompileControlError> {
    add(
        add(source, requested.replace(0))?,
        std::mem::size_of_val(namespace),
    )
}

#[cfg(test)]
#[path = "encode/tests.rs"]
pub(super) mod tests;
