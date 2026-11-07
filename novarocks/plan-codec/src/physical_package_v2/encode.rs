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

//! Whole-package sender: one checked FragmentPackage to the complete 22-field
//! wire DTO. The input is already checked by its FE owner; this author only
//! composes the original component encoders on one caller control. Each
//! component gates its own limits; the whole DTO has one coarse size bound.
//! There is no second cross-component ledger on the sender side.

use super::binding_sources::{BindingSourceLimits, collect_binding_sources_in};
use super::definition_sources::{DefinitionSourceLimits, collect_definition_sources_in};
use super::nodes::{NodeDispatchLimits, NodeEncodeContext, prepare_node_encode_in};
use super::provider_sources::{
    ProviderSourceError, ProviderSourceLimits, collect_provider_sources_in,
};
use super::type_views::{
    TypeViewBudget, TypeViewError, TypeViewLimits, collect_package_type_views_in,
};
use crate::physical_aggregate_binding_v2::encode_aggregate_bindings_in;
use crate::physical_binding_v2::{
    BindingCodecError, BindingProjectionLimits, encode_function_bindings_in,
};
use crate::physical_call_requests_v2::{
    CallRequestCodecError, CallRequestProjectionLimits, prepare_call_requests_encode_in,
};
use crate::physical_connector_payload_v2::{
    ConnectorPayloadCodecError, ConnectorPayloadProjectionLimits, encode_connector_payloads_in,
};
use crate::physical_constant_v2::{
    ConstantNamespaceProjectionLimits, ConstantWriteProjectionLimits, PhysicalConstantCodecError,
    prepare_constant_namespace_write_in,
};
use crate::physical_control_v2::{
    ControlCodecError, ControlProjectionLimits, encode_expression_control_observed,
};
use crate::physical_cuts_v2::{
    CutsProjectionLimits, EncodedCutsContext, encode_fragment_cuts_observed,
};
use crate::physical_expression_v2::{
    ExpressionCodecError, ExpressionProjectionLimits, prepare_expression_definitions_in,
};
use crate::physical_fragment_envelope_v2::prepare_fragment_envelope_encode_in;
use crate::physical_node_v2::{NodeCodecError, NodeProjectionLimits};
use crate::physical_package_metadata_v2::{
    PackageMetadataCodecError, encode_package_metadata_observed,
};
use crate::physical_provider_binding_v2::{
    ProviderBindingCodecError, ProviderBindingProjectionLimits, encode_joint_provider_bindings_in,
};
use crate::physical_provider_read_v2::{
    ProviderReadCodecError, ProviderReadProjectionLimits, encode_provider_reads_in,
};
use crate::physical_public_source_schema_v2::{
    PublicSourceSchemaCodecError, encode_public_source_schemas_pure_in,
    encoded_public_source_schemas_backing_bytes,
};
use crate::physical_read_scan_v2::{
    ReadScanCodecError, ReadScanEncodeContext, ReadScanProjectionLimits, encode_read_scans_observed,
};
use crate::physical_relation_v2::{
    RelationCodecError, RelationProjectionLimits, encode_relations_in,
};
use crate::physical_result_v2::prepare_result_encode_in;
use crate::physical_schema_v2::{SchemaCodecError, prepare_schemas_encode_observed_in};
use crate::physical_semantics_v2::{
    ParameterProjectionLimits, SemanticsCodecError, encode_frozen_calls_observed,
    encode_frozen_pruning_observed, prepare_semantic_parameters_encode_observed_in,
};
use crate::physical_type_v2::{
    PackageTypeProjectionLimits, TypeCodecError, encode_borrowed_type_namespaces_sources_in,
};
use crate::physical_value_v2::{ValueCodecError, ValueProjectionLimits, encode_values_observed_in};
use crate::physical_writer_recipe_v2::{
    WriterRecipeCodecError, WriterRecipeEncodeContext, WriterRecipeProjectionLimits,
    encode_writer_recipes_observed,
};
use crate::physical_writer_schema_v2::WriterSchemaProjectionLimits;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use prost::Message;
use std::{cell::Cell, fmt};

/// Host-provided component limits. No field has a default; the FE owner
/// chooses them. `source_retained_bytes` is the checked package's retained
/// backing, invoiced once to every component that requires a source floor.
#[derive(Clone, Copy, Debug)]
pub struct PackageEncodeLimits {
    pub source_retained_bytes: usize,
    /// Coarse bound on the complete encoded DTO, checked before return.
    pub max_wire_bytes: usize,
    pub views: TypeViewLimits,
    pub binding_sources: BindingSourceLimits,
    pub provider_sources: ProviderSourceLimits,
    pub definition_sources: DefinitionSourceLimits,
    /// Metadata, schemas, envelope, calls, pruning, result and node headers.
    pub node: NodeProjectionLimits,
    pub types: PackageTypeProjectionLimits,
    pub bindings: BindingProjectionLimits,
    pub provider_bindings: ProviderBindingProjectionLimits,
    pub payloads: ConnectorPayloadProjectionLimits,
    pub reads: ProviderReadProjectionLimits,
    pub relations: RelationProjectionLimits,
    pub constant_records: ConstantWriteProjectionLimits,
    pub constants: ConstantNamespaceProjectionLimits,
    pub parameters: ParameterProjectionLimits,
    pub values: ValueProjectionLimits,
    pub expressions: ExpressionProjectionLimits,
    pub requests: CallRequestProjectionLimits,
    pub writer_schema: WriterSchemaProjectionLimits,
    pub control: ControlProjectionLimits,
    pub cuts: CutsProjectionLimits,
    pub scans: ReadScanProjectionLimits,
    pub writers: WriterRecipeProjectionLimits,
}

#[derive(Debug)]
pub enum PackageEncodeError {
    Control(CompileControlError),
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
    limits: &PackageEncodeLimits,
    control: &dyn PureCompileControl,
) -> Result<wire::FragmentPackage, PackageEncodeError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_fragment_package_in(package, limits, &mut work);
    work.finish_result(result, PackageEncodeError::Control)
}

fn encode_fragment_package_in<'s>(
    package: &'s p::FragmentPackage,
    l: &PackageEncodeLimits,
    work: &mut CompileCheckpoints<'s>,
) -> Result<wire::FragmentPackage, PackageEncodeError> {
    let source = l.source_retained_bytes;
    let fragment = package.fragment();

    // Source collection on one budget. The budget gates its own limits; its
    // parent hook is the sender itself, which keeps no second ledger.
    let mut collection = |_: &_| Ok(());
    let mut budget = TypeViewBudget::new_in(package, source, l.views, &mut collection, work)?;
    let views = collect_package_type_views_in(&mut budget, work)?;
    let writer_types = views.writer_sources_in(&mut budget, work)?;
    let bindings =
        collect_binding_sources_in(package, &views, l.binding_sources, &mut budget, work)?;
    let providers = collect_provider_sources_in(
        package,
        &views,
        &writer_types,
        l.provider_sources,
        &mut budget,
        work,
    )?;
    let definitions = collect_definition_sources_in(
        package,
        &views,
        &bindings,
        l.definition_sources,
        &mut budget,
        work,
    )?;
    let arguments = bindings.argument_inputs_in(&mut budget, work)?;
    let function_inputs = arguments.function_inputs_in(&mut budget, work)?;
    let aggregate_inputs = bindings.aggregate_inputs_in(&mut budget, work)?;
    let provider_inputs = providers.prepare_inputs_in(&mut budget, work)?;
    let expression_ids = definitions.expressions_in(&mut budget, work)?;
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
    let mut source = budget
        .facts()
        .coexisting_source_and_request_bytes_upper_bound;

    // Original component encoders in hard dependency order. A completed
    // namespace stays live for later components, so its requested-backing
    // upper bound and inline header join every later invoice once
    // (conservative: temporary requests are included).
    let requested = Cell::new(0usize);
    macro_rules! seen {
        ($field:ident) => {
            &mut |f: &_| {
                requested.set(requested.get().max(f.$field));
                Ok(())
            }
        };
    }
    let (metadata, _) = encode_package_metadata_observed(
        package,
        source,
        l.node,
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
        l.types,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &types)?;
    let functions = encode_function_bindings_in(
        &types,
        &function_inputs,
        source,
        l.bindings,
        seen!(request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &functions)?;
    let aggregates = encode_aggregate_bindings_in(
        &types,
        &functions,
        &aggregate_inputs,
        source,
        l.bindings,
        seen!(request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &aggregates)?;
    let provider_bindings = encode_joint_provider_bindings_in(
        providers.provider_inputs(),
        source,
        l.provider_bindings,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &provider_bindings)?;
    let payloads = encode_connector_payloads_in(
        providers.payload_inputs(),
        source,
        l.payloads,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &payloads)?;
    let reads = encode_provider_reads_in(
        providers.read_inputs(),
        &provider_bindings,
        &payloads,
        source,
        l.reads,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &reads)?;
    let relations = encode_relations_in(
        provider_inputs.relations(),
        &reads,
        &types,
        source,
        l.relations,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &relations)?;
    let schemas = prepare_schemas_encode_observed_in(
        provider_inputs.schemas(),
        &types,
        source,
        l.node,
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
    let constants = prepare_constant_namespace_write_in(
        package.constants(),
        definitions.constant_type_ids(),
        &types,
        source,
        l.constant_records,
        l.constants,
        seen!(new_allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_in(seen!(new_allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &constants)?;
    let (parameters, _) = prepare_semantic_parameters_encode_observed_in(
        package.parameters(),
        source,
        l.parameters,
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
        l.values,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &values)?;
    let expressions = prepare_expression_definitions_in(
        fragment.expressions(),
        &expression_ids,
        &types,
        &functions,
        &aggregates,
        package.parameters(),
        package.constants(),
        source,
        l.expressions,
        seen!(new_allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_in(seen!(new_allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &expressions)?;
    let call_requests = prepare_call_requests_encode_in(
        fragment.call_requests(),
        &types,
        &request_ids,
        package.constants(),
        source,
        l.requests,
        seen!(request_bytes_upper_bound),
        work,
    )?
    .emit_in(seen!(request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &call_requests)?;
    let (envelope, _) = prepare_fragment_envelope_encode_in(
        fragment,
        source,
        l.node,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?
    .emit_in(seen!(allocation_request_bytes_upper_bound), work)?;
    source = grow(source, &requested, &envelope)?;
    let dispatch = NodeDispatchLimits {
        node: l.node,
        binding: l.bindings,
        relation: l.relations,
        writer_schema: l.writer_schema,
    };
    let context = NodeEncodeContext {
        values: &values,
        expressions: &expressions,
        relations: &relations,
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
        let (encoded, _) = prepare_node_encode_in(
            node,
            &context,
            ids,
            source,
            dispatch,
            seen!(allocation_request_bytes_upper_bound),
            work,
        )?
        .emit_in(seen!(allocation_request_bytes_upper_bound), work)?;
        // Each emitted node stays live in the output table.
        source = add(source, requested.replace(0))?;
        nodes.push(encoded);
    }
    let (expression_control, _) = encode_expression_control_observed(
        fragment,
        package.expression_uses(),
        source,
        l.control,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &expression_control)?;
    let (calls, _) = encode_frozen_calls_observed(
        package.calls(),
        source,
        l.node,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;
    source = grow(source, &requested, &calls)?;
    let (pruning, _) = encode_frozen_pruning_observed(
        package.pruning(),
        source,
        l.node,
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
            limits: l.cuts,
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
        l.node,
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
        l.scans,
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
        l.writers,
        seen!(allocation_request_bytes_upper_bound),
        work,
    )?;

    // Namespaces borrow earlier ones; release them in reverse dependency order.
    let expressions = expressions.into_wire();
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
            expressions,
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
        expression_control: Some(expression_control),
        calls: Some(calls),
        pruning: Some(pruning),
        parameters: Some(parameters),
        cuts: Some(cuts),
        result,
        scans,
        writes,
        annotations: metadata.annotations,
        connector_expressions,
    };
    work.step()?;
    if output.encoded_len() > l.max_wire_bytes {
        return Err(CompileControlError::ResourceExhausted.into());
    }
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
