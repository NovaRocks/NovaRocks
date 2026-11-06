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

//! Whole-package receiver at the BE trust boundary. Byte admission precedes
//! Prost; every wire field then enters its original component decoder, and
//! the result is published only through the original Fragment and Package
//! constructors. One Decode scope; no default, fallback or second grammar.
//!
//! Each component gates its own host limits. The sole cross-component number
//! is the source invoice chain: the admitted DTO plus every still-live decoded
//! namespace, carried forward through each namespace's own retained floor.

use super::nodes::{NodeDecodeContext, NodeDispatchLimits, prepare_node_decode_in};
use super::{PackageWireError, prepare_package_wire_in};
use crate::host_projection_v2::ProjectionFailure;
use crate::physical_public_source_schema_v2::{
    PublicSourceSchemaCodecError, decode_public_source_schemas_pure_in,
    decode_public_source_schemas_with_creator_in, decode_public_source_schemas_with_factory_in,
};
use crate::{
    physical_aggregate_binding_v2::{
        materialize_aggregate_bindings_in, prepare_aggregate_binding_headers_in,
        prepare_aggregate_bindings_materialization_in,
    },
    physical_binding_v2::{
        BindingCodecError, BindingProjectionLimits, materialize_function_bindings_in,
        prepare_function_binding_headers_in, prepare_function_bindings_materialization_in,
    },
    physical_call_requests_v2::{
        CallRequestCodecError, CallRequestProjectionLimits, CallRequestTypeOwner,
        decode_call_requests_in, decode_call_requests_with_creator_in,
        prepare_call_requests_decode_in, prepare_call_requests_decode_with_creator_in,
    },
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, ConnectorPayloadProjectionLimits, decode_connector_payloads_in,
    },
    physical_constant_v2::{
        ConstantDecodeProjectionLimits, ConstantNamespaceProjectionLimits,
        PhysicalConstantCodecError, prepare_constant_namespace_in,
        prepare_constant_namespace_with_creator_in, prepare_constant_namespace_with_factory_in,
    },
    physical_control_v2::{
        ControlCodecError, ControlProjectionLimits, decode_expression_control_observed,
    },
    physical_cuts_v2::{CutsProjectionLimits, decode_fragment_cuts_observed},
    physical_expression_v2::{
        ExpressionCodecError, ExpressionProjectionLimits, decode_expression_definitions_in,
        materialize_expressions_in, prepare_expression_materialization_in,
    },
    physical_fragment_envelope_v2::prepare_fragment_envelope_decode_in,
    physical_node_v2::{NodeCodecError, NodeProjectionLimits},
    physical_package_metadata_v2::{PackageMetadataCodecError, decode_package_metadata_observed},
    physical_provider_binding_v2::{
        ProviderBindingCodecError, ProviderBindingProjectionLimits,
        decode_joint_provider_bindings_in,
    },
    physical_provider_read_v2::{
        ProviderReadCodecError, ProviderReadProjectionLimits, decode_provider_reads_in,
    },
    physical_read_scan_v2::{
        ReadScanCodecError, ReadScanDecodeContext, ReadScanProjectionLimits,
        decode_read_scans_observed, decode_read_scans_with_creator,
    },
    physical_relation_v2::{RelationCodecError, RelationProjectionLimits, decode_relations_in},
    physical_result_v2::prepare_result_decode_in,
    physical_schema_v2::{
        SchemaCodecError, decode_schemas_with_factory_in, prepare_schemas_decode_observed_in,
        prepare_schemas_decode_with_creator_in,
    },
    physical_semantics_v2::{
        ParameterProjectionLimits, SemanticsCodecError, decode_frozen_calls_observed,
        decode_frozen_pruning_observed, prepare_semantic_parameters_decode_observed_in,
    },
    physical_type_v2::{
        PackageTypeProjectionFacts, PackageTypeProjectionLimits, TypeCodecError,
        decode_package_type_table_observed, decode_package_type_table_with_creator_in,
        decode_package_type_table_with_factory_in,
    },
    physical_value_v2::{ValueCodecError, ValueProjectionLimits, decode_values_observed_in},
    physical_writer_recipe_v2::{
        WriterRecipeCodecError, WriterRecipeDecodeContext, WriterRecipeProjectionLimits,
        decode_writer_recipes_observed, decode_writer_recipes_with_factory_in,
    },
    physical_writer_schema_v2::WriterSchemaProjectionLimits,
    resource_preflight_v2::{DecodeProjectionLimits, FragmentDecodeResourceModel},
};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::ConstantPolicy;
use novarocks_physical_plan::{
    self as p, CallRequestError, FragmentPackageError, FragmentStructureError,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
use std::{
    alloc::Layout,
    collections::{BTreeMap, btree_map::Entry},
    fmt,
    mem::{size_of, size_of_val},
};

/// Host-authored receiving envelopes; none of these is on the wire and no
/// default exists. `admission.plan_limits` is the one structural PlanLimits
/// for the owned expression arena, the Fragment and the Package.
#[derive(Clone, Debug)]
pub struct PackageDecodeLimits {
    /// Generated-layout byte admission before Prost allocates.
    pub wire: DecodeProjectionLimits,
    pub types: PackageTypeProjectionLimits,
    /// Metadata, schemas, envelope, calls, pruning, result and node headers.
    pub node: NodeProjectionLimits,
    pub constant_policy: ConstantPolicy,
    pub constant_records: ConstantDecodeProjectionLimits,
    pub constants: ConstantNamespaceProjectionLimits,
    pub verifier: VerifierOptions,
    /// Function and aggregate headers, their owned materializations and the
    /// per-node signature copies.
    pub bindings: BindingProjectionLimits,
    pub provider_bindings: ProviderBindingProjectionLimits,
    pub payloads: ConnectorPayloadProjectionLimits,
    pub reads: ProviderReadProjectionLimits,
    /// The relation namespace and each Scan node's relation copy.
    pub relations: RelationProjectionLimits,
    pub values: ValueProjectionLimits,
    /// The receiving expression namespace and its owned arena materialization.
    pub expressions: ExpressionProjectionLimits,
    pub requests: CallRequestProjectionLimits,
    pub parameters: ParameterProjectionLimits,
    pub cuts: CutsProjectionLimits,
    pub scans: ReadScanProjectionLimits,
    pub writers: WriterRecipeProjectionLimits,
    pub writer_schema: WriterSchemaProjectionLimits,
    pub control: ControlProjectionLimits,
    pub admission: p::FragmentPackageAdmission,
}

#[derive(Debug)]
pub enum PackageDecodeError {
    Control(CompileControlError),
    Wire(PackageWireError),
    Metadata(PackageMetadataCodecError),
    Type(TypeCodecError),
    Schema(SchemaCodecError),
    PublicSourceSchema(PublicSourceSchemaCodecError),
    Constant(PhysicalConstantCodecError),
    Binding(BindingCodecError),
    ProviderBinding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Read(ProviderReadCodecError),
    Relation(RelationCodecError),
    Value(ValueCodecError),
    Expression(ExpressionCodecError),
    Node(NodeCodecError),
    CallRequests(CallRequestCodecError),
    ExpressionControl(ControlCodecError),
    Semantics(SemanticsCodecError),
    Scan(ReadScanCodecError),
    Writer(WriterRecipeCodecError),
    Structure(FragmentStructureError),
    Requests(CallRequestError),
    Package(FragmentPackageError),
    Invalid(&'static str),
}
impl From<CompileControlError> for PackageDecodeError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl<H> From<CompileControlError> for ProjectionFailure<PackageDecodeError, H> {
    fn from(error: CompileControlError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<ProjectionFailure<BindingCodecError, H>> for ProjectionFailure<PackageDecodeError, H> {
    fn from(error: ProjectionFailure<BindingCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<ProjectionFailure<TypeCodecError, H>> for ProjectionFailure<PackageDecodeError, H> {
    fn from(error: ProjectionFailure<TypeCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<ProjectionFailure<PhysicalConstantCodecError, H>>
    for ProjectionFailure<PackageDecodeError, H>
{
    fn from(error: ProjectionFailure<PhysicalConstantCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<ProjectionFailure<WriterRecipeCodecError, H>>
    for ProjectionFailure<PackageDecodeError, H>
{
    fn from(error: ProjectionFailure<WriterRecipeCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(e) => Self::Host(e),
            ProjectionFailure::Codec(e) => Self::Codec(e.into()),
        }
    }
}
impl<H> From<ProjectionFailure<SchemaCodecError, H>> for ProjectionFailure<PackageDecodeError, H> {
    fn from(error: ProjectionFailure<SchemaCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<ProjectionFailure<CallRequestCodecError, H>>
    for ProjectionFailure<PackageDecodeError, H>
{
    fn from(error: ProjectionFailure<CallRequestCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<ProjectionFailure<ReadScanCodecError, H>>
    for ProjectionFailure<PackageDecodeError, H>
{
    fn from(error: ProjectionFailure<ReadScanCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<ProjectionFailure<PublicSourceSchemaCodecError, H>>
    for ProjectionFailure<PackageDecodeError, H>
{
    fn from(error: ProjectionFailure<PublicSourceSchemaCodecError, H>) -> Self {
        match error {
            ProjectionFailure::Host(error) => Self::Host(error),
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
        }
    }
}
impl<H> From<p::CallRequestCreationFailure<H>> for ProjectionFailure<PackageDecodeError, H> {
    fn from(error: p::CallRequestCreationFailure<H>) -> Self {
        match error {
            p::CallRequestCreationFailure::Creator(error) => Self::Host(error),
            p::CallRequestCreationFailure::Request(error) => Self::Codec(error.into()),
        }
    }
}
// Every component's own control cause stays primary; nothing else is lifted.
macro_rules! component_error {
    ($($source:ident => $variant:ident),+ $(,)?) => {$(
        impl From<$source> for PackageDecodeError {
            fn from(error: $source) -> Self {
                match error {
                    $source::Control(cause) => Self::Control(cause),
                    error => Self::$variant(error),
                }
            }
        }
        impl<H> From<$source> for ProjectionFailure<PackageDecodeError,H> {
            fn from(error:$source)->Self { Self::Codec(PackageDecodeError::from(error)) }
        }
    )+};
}
component_error!(
    PackageWireError => Wire,
    PackageMetadataCodecError => Metadata,
    TypeCodecError => Type,
    SchemaCodecError => Schema,
    PublicSourceSchemaCodecError => PublicSourceSchema,
    PhysicalConstantCodecError => Constant,
    BindingCodecError => Binding,
    ProviderBindingCodecError => ProviderBinding,
    ConnectorPayloadCodecError => Payload,
    ProviderReadCodecError => Read,
    RelationCodecError => Relation,
    ValueCodecError => Value,
    ExpressionCodecError => Expression,
    NodeCodecError => Node,
    CallRequestCodecError => CallRequests,
    ControlCodecError => ExpressionControl,
    SemanticsCodecError => Semantics,
    ReadScanCodecError => Scan,
    WriterRecipeCodecError => Writer,
    FragmentStructureError => Structure,
    CallRequestError => Requests,
    FragmentPackageError => Package,
);
impl fmt::Display for PackageDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Wire(error) => error.fmt(f),
            Self::Metadata(error) => error.fmt(f),
            Self::Type(error) => error.fmt(f),
            Self::Schema(error) => error.fmt(f),
            Self::PublicSourceSchema(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
            Self::ProviderBinding(error) => error.fmt(f),
            Self::Payload(error) => error.fmt(f),
            Self::Read(error) => error.fmt(f),
            Self::Relation(error) => error.fmt(f),
            Self::Value(error) => error.fmt(f),
            Self::Expression(error) => error.fmt(f),
            Self::Node(error) => error.fmt(f),
            Self::CallRequests(error) => error.fmt(f),
            Self::ExpressionControl(error) => error.fmt(f),
            Self::Semantics(error) => error.fmt(f),
            Self::Scan(error) => error.fmt(f),
            Self::Writer(error) => error.fmt(f),
            Self::Structure(error) => error.fmt(f),
            Self::Requests(error) => error.fmt(f),
            Self::Package(error) => error.fmt(f),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for PackageDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Wire(source) => Some(source),
            Self::Metadata(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Schema(source) => Some(source),
            Self::PublicSourceSchema(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::ProviderBinding(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Read(source) => Some(source),
            Self::Relation(source) => Some(source),
            Self::Value(source) => Some(source),
            Self::Expression(source) => Some(source),
            Self::Node(source) => Some(source),
            Self::CallRequests(source) => Some(source),
            Self::ExpressionControl(source) => Some(source),
            Self::Semantics(source) => Some(source),
            Self::Scan(source) => Some(source),
            Self::Writer(source) => Some(source),
            Self::Structure(source) => Some(source),
            Self::Requests(source) => Some(source),
            Self::Package(source) => Some(source),
            _ => None,
        }
    }
}

type Error = PackageDecodeError;

fn exhausted() -> Error {
    Error::Control(CompileControlError::ResourceExhausted)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b).ok_or_else(exhausted)
}
fn array_bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|layout| layout.size())
        .map_err(|_| exhausted())
}
fn required<T>(value: Option<T>, message: &'static str) -> Result<T, Error> {
    value.ok_or(Error::Invalid(message))
}
/// A known numerical refusal precedes the first insertion into a new map.
fn admit_count(count: usize, maximum: usize) -> Result<(), Error> {
    if count > maximum {
        Err(exhausted())
    } else {
        Ok(())
    }
}
/// A repeated sparse key is refused, never overwritten.
fn insert_unique<K: Ord, V>(
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    duplicate: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let inserted = match map.entry(key) {
        Entry::Vacant(slot) => {
            slot.insert(value);
            true
        }
        Entry::Occupied(_) => false,
    };
    work.step()?;
    if inserted {
        Ok(())
    } else {
        Err(Error::Invalid(duplicate))
    }
}
fn unique_map<K: Ord, V>(
    entries: Vec<(K, V)>,
    maximum: usize,
    duplicate: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeMap<K, V>, Error> {
    admit_count(entries.len(), maximum)?;
    let mut map = BTreeMap::new();
    for (key, value) in entries {
        insert_unique(&mut map, key, value, duplicate, work)?;
    }
    Ok(map)
}
/// The aggregate header law charges its own DTO roots on top of the function
/// header invoice it borrows; that exact original backing is added once.
fn aggregate_definition_backing(
    definitions: &Vec<wire::AggregateBindingDefinition>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    let mut bytes = array_bytes::<wire::AggregateBindingDefinition>(definitions.capacity())?;
    for definition in definitions {
        bytes = add(bytes, definition.state_format.capacity())?;
        if let Some(receipt) = &definition.state_interpretation {
            bytes = add(
                bytes,
                array_bytes::<wire::AggregateStateOrderKey>(receipt.order_keys.capacity())?,
            )?;
        }
        work.step()?;
    }
    Ok(bytes)
}
/// The result law charges its own DTO backing on top of the value namespace
/// floor; that exact original backing is added once. Absence adds nothing.
fn result_port_backing(
    port: Option<&wire::ResultPort>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    let Some(port) = port else {
        return Ok(0);
    };
    let mut bytes = add(
        size_of::<wire::ResultPort>(),
        array_bytes::<wire::ResultField>(port.fields.capacity())?,
    )?;
    if let Some(output) = &port.output {
        bytes = add(bytes, array_bytes::<u32>(output.value_ids.capacity())?)?;
    }
    for field in &port.fields {
        bytes = add(
            bytes,
            add(
                field.name.capacity(),
                field.alias.as_ref().map_or(0, String::capacity),
            )?,
        )?;
        work.step()?;
    }
    Ok(bytes)
}

/// Decode one complete package on a single Decode scope. A control refusal
/// returns directly; every other outcome observes the ordinary completion.
pub fn decode_fragment_package(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    control: &dyn PureCompileControl,
) -> Result<p::FragmentPackage, PackageDecodeError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_fragment_package_in(raw, model, limits, &mut work);
    if matches!(&result, Err(PackageDecodeError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// Same receiver on the caller's scope; the caller owns entry and completion.
/// Admission callbacks are pass-through: each component gates its own limits.
pub(crate) fn decode_fragment_package_in(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::FragmentPackage, PackageDecodeError> {
    decode_fragment_package_core::<std::convert::Infallible>(raw, model, limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

/// Actual BE creator for the same receiver body. Pure preparation uses the
/// separate legacy expansion entry; it never mints a runtime storage hold.
pub fn decode_fragment_package_with_creator<H>(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn PureCompileControl,
) -> Result<p::FragmentPackage, ProjectionFailure<PackageDecodeError, H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_fragment_package_core(raw, model, limits, Some(creation), &mut work);
    if matches!(
        &result,
        Err(ProjectionFailure::Host(_))
            | Err(ProjectionFailure::Codec(PackageDecodeError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn decode_fragment_package_core<H>(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    creation: Option<&mut ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::FragmentPackage, ProjectionFailure<PackageDecodeError, H>> {
    decode_fragment_package_factory_core::<H, crate::value_type_table::resources::NoFactory>(
        raw, model, limits, creation, None, work, &mut None,
    )
}

/// The actual BE supplies its sealed factory alongside the existing physical
/// creator. Neither port is derived from storage or transported policies.
pub fn decode_fragment_package_with_factory<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    creation: &mut ValueSchemaCreation<'_, H>,
    factory: &F,
    control: &dyn PureCompileControl,
) -> Result<p::FragmentPackage, ProjectionFailure<PackageDecodeError, H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_fragment_package_factory_core(
        raw,
        model,
        limits,
        Some(creation),
        Some(factory),
        &mut work,
        &mut None,
    );
    if matches!(
        &result,
        Err(ProjectionFailure::Host(_))
            | Err(ProjectionFailure::Codec(PackageDecodeError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}

/// The existing receiver with the caller's actual creator and narrow original
/// Arrow/source tail funding. Other namespaces retain their existing responsibility.
pub fn decode_fragment_package_with_factory_and_funding<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    creation: &mut ValueSchemaCreation<'_, H>,
    factory: &F,
    control: &dyn PureCompileControl,
    funding: &mut dyn crate::physical_type_v2::PackageTypeFundingScope<H>,
) -> Result<p::FragmentPackage, ProjectionFailure<PackageDecodeError, H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_fragment_package_factory_core(
        raw,
        model,
        limits,
        Some(creation),
        Some(factory),
        &mut work,
        &mut Some(funding),
    );
    if matches!(
        &result,
        Err(ProjectionFailure::Host(_) | ProjectionFailure::Codec(PackageDecodeError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}

pub fn decode_fragment_package_with_type_host<
    G: crate::physical_type_v2::PackageTypeMaterializationScope,
>(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    control: &dyn PureCompileControl,
    host: &mut G,
) -> Result<p::FragmentPackage, ProjectionFailure<PackageDecodeError, G::HostError>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_fragment_package_factory_core::<
        G::HostError,
        crate::value_type_table::resources::NoFactory,
    >(raw, model, limits, None, None, &mut work, &mut Some(host));
    if matches!(
        &result,
        Err(ProjectionFailure::Host(_) | ProjectionFailure::Codec(PackageDecodeError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}

fn decode_fragment_package_factory_core<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    limits: &PackageDecodeLimits,
    mut creation: Option<&mut ValueSchemaCreation<'_, H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
    funding: &mut Option<&mut dyn crate::physical_type_v2::PackageTypeFundingScope<H>>,
) -> Result<p::FragmentPackage, ProjectionFailure<PackageDecodeError, H>> {
    let plan_limits = limits.admission.plan_limits;

    // L0: generated-layout byte admission, then the original Prost verdict.
    let prepared = prepare_package_wire_in(raw, model, limits.wire, &mut |_| Ok(()), work)?;
    let usage = prepared.projection().usage;
    if let Some(creator) = creation.as_deref_mut() {
        // Original generated-layout Prost heap request upper. Raw input and
        // inline DTO bytes are borrowed source, never new allocator requests.
        creator
            .observe_opaque_upper_bound(usage.peak_requested_heap_bytes_upper)
            .map_err(ProjectionFailure::<PackageDecodeError, H>::Host)?;
    }
    let wire_package = prepared.materialize_in(&mut |_| Ok(()), work)?;
    // Raw input, DTO root and every requested DTO heap block stay live until
    // publication. Each decoded namespace floor carries the invoice it was
    // given, so the chain keeps their maximum and never sums an invoice twice.
    let mut invoice = add(
        add(usage.input_bytes, usage.root_inline_bytes)?,
        usage.peak_requested_heap_bytes_upper,
    )?;

    // Mandatory singular components have no default.
    let fragment_wire = required(wire_package.fragment.as_ref(), "package fragment is absent")?;
    let control_wire = required(
        wire_package.expression_control.as_ref(),
        "package expression control is absent",
    )?;
    let calls_wire = required(
        wire_package.calls.as_ref(),
        "package frozen calls are absent",
    )?;
    let pruning_wire = required(
        wire_package.pruning.as_ref(),
        "package frozen pruning is absent",
    )?;
    let parameters_wire = required(
        wire_package.parameters.as_ref(),
        "package semantic parameters are absent",
    )?;
    let cuts_wire = required(
        wire_package.cuts.as_ref(),
        "package fragment cuts are absent",
    )?;

    // L1: owners that borrow only the DTO.
    let (metadata, _) = decode_package_metadata_observed(
        &wire_package,
        invoice,
        limits.node,
        &mut |_| Ok(()),
        work,
    )?;
    // Types and constant pools have no retained-floor author, yet later laws
    // charge their roots; their own admitted request upper bound and inline
    // header join the invoice once.
    let mut type_requests = 0usize;
    let types = if let Some(creator) = creation.as_deref_mut() {
        let types = match (factory, funding.as_deref_mut()) {
            (Some(factory), Some(funding)) => {
                crate::physical_type_v2::decode_package_type_table_with_factory_and_funding_in(
                    &wire_package,
                    invoice,
                    limits.types,
                    creator,
                    factory,
                    work,
                    funding,
                )?
            }
            (Some(factory), None) => decode_package_type_table_with_factory_in(
                &wire_package,
                invoice,
                limits.types,
                creator,
                factory,
                work,
            )?,
            (None, _) => decode_package_type_table_with_creator_in(
                &wire_package,
                invoice,
                limits.types,
                creator,
                work,
            )?,
        };
        type_requests =
            types
                .creation_request_bytes_upper_bound()
                .ok_or(PackageDecodeError::Invalid(
                    "package type creator omitted its original request receipt",
                ))?;
        types
    } else if let Some(funding) = funding.as_deref_mut() {
        crate::physical_type_v2::decode_package_type_table_with_funding_in(
            &wire_package,
            invoice,
            limits.types,
            &mut |facts: &PackageTypeProjectionFacts| {
                type_requests = type_requests.max(facts.allocation_request_bytes_upper_bound);
                Ok(())
            },
            work,
            funding,
        )?
    } else {
        decode_package_type_table_observed(
            &wire_package,
            invoice,
            limits.types,
            &mut |facts: &PackageTypeProjectionFacts| {
                type_requests = type_requests.max(facts.allocation_request_bytes_upper_bound);
                Ok(())
            },
            work,
        )?
    };
    invoice = add(add(invoice, type_requests)?, size_of_val(&types))?;
    // Joint mode is required: writer views exist only on joint bindings.
    let provider_bindings = decode_joint_provider_bindings_in(
        &wire_package.provider_bindings,
        invoice,
        limits.provider_bindings,
        &mut |_| Ok(()),
        work,
    )?;
    invoice = invoice.max(provider_bindings.retained_invoice_floor_in(work)?);
    let payloads = decode_connector_payloads_in(
        &wire_package.provider_payloads,
        invoice,
        limits.payloads,
        &mut |_| Ok(()),
        work,
    )?;
    invoice = invoice.max(payloads.retained_invoice_floor_in(work)?);
    let (parameters, _) = prepare_semantic_parameters_decode_observed_in(
        parameters_wire,
        invoice,
        limits.parameters,
        &mut |_| Ok(()),
        work,
    )?
    .emit_observed_in(&mut |_| Ok(()), work)?;
    let (envelope, _) = prepare_fragment_envelope_decode_in(
        fragment_wire,
        invoice,
        limits.node,
        &mut |_| Ok(()),
        work,
    )?
    .emit_in(&mut |_| Ok(()), work)?;

    // L2: owners over types and provider namespaces.
    let writer_context = WriterRecipeDecodeContext {
        bindings: &provider_bindings,
        payloads: &payloads,
        types: &types,
    };
    let (writes, writer_facts, writer_temporary) = if let Some(factory) = factory {
        decode_writer_recipes_with_factory_in(
            &wire_package.writes,
            writer_context,
            invoice,
            limits.writers,
            factory,
            &mut |_| Ok(()),
            work,
        )?
        .into_parts()
    } else {
        let (writes, facts) = decode_writer_recipes_observed(
            &wire_package.writes,
            writer_context,
            invoice,
            limits.writers,
            &mut |_| Ok(()),
            work,
        )?;
        (writes, facts, ValueTypeStorageLifetime::pure())
    };
    let writes = unique_map(
        writes,
        plan_limits.fragment_nodes,
        "writer recipe node ID is duplicated",
        work,
    )?;
    drop(writer_temporary);
    // The sole writer model combines the original source B exactly once with
    // completed constructor requests. This conservative numerical provenance
    // includes temporary requests; it is neither retained precision nor grant.
    // Subsequent namespace models must not lose newly owned writer backing.
    invoice = invoice.max(writer_facts.coexisting_source_and_request_bytes_upper_bound);
    let schemas = if let Some(factory) = factory {
        decode_schemas_with_factory_in(
            &wire_package.schemas,
            &types,
            invoice,
            limits.node,
            factory,
            &mut |_| Ok(()),
            work,
        )?
    } else if let Some(creator) = creation.as_deref_mut() {
        prepare_schemas_decode_with_creator_in(
            &wire_package.schemas,
            &types,
            invoice,
            limits.node,
            creator,
            work,
        )?
        .emit_observed_in(&mut |_| Ok(()), work)?
    } else {
        prepare_schemas_decode_observed_in(
            &wire_package.schemas,
            &types,
            invoice,
            limits.node,
            &mut |_| Ok(()),
            work,
        )?
        .emit_observed_in(&mut |_| Ok(()), work)?
    };
    invoice = invoice.max(if factory.is_some() {
        // Completed actual constructor upper is numerical source provenance,
        // not a grant or an exact retained-byte ledger. It includes the new
        // scopes/loan holders once, on the unchanged namespace model.
        schemas
            .facts()
            .coexisting_source_and_request_bytes_upper_bound
    } else {
        schemas.retained_invoice_floor()?
    });
    let public_source_schemas = if let Some(factory) = factory {
        decode_public_source_schemas_with_factory_in(
            &wire_package.public_source_schemas,
            &schemas,
            invoice,
            limits.node,
            factory,
            work,
        )?
    } else if let Some(creator) = creation.as_deref_mut() {
        decode_public_source_schemas_with_creator_in(
            &wire_package.public_source_schemas,
            &schemas,
            invoice,
            limits.node,
            creator,
            work,
        )?
    } else {
        decode_public_source_schemas_pure_in(
            &wire_package.public_source_schemas,
            &schemas,
            invoice,
            limits.node,
            work,
        )?
    };
    let scan_context = ReadScanDecodeContext {
        bindings: &provider_bindings,
        payloads: &payloads,
        schemas: &schemas,
        source_schemas: Some(&public_source_schemas),
    };
    let (scans, _) = if let Some(creator) = creation.as_deref_mut() {
        decode_read_scans_with_creator(
            &wire_package.scans,
            &wire_package.connector_expressions,
            scan_context,
            invoice,
            limits.scans,
            &mut |_| Ok(()),
            creator,
            work,
        )?
    } else {
        decode_read_scans_observed(
            &wire_package.scans,
            &wire_package.connector_expressions,
            scan_context,
            invoice,
            limits.scans,
            &mut |_| Ok(()),
            work,
        )?
    };
    let scans = unique_map(
        scans,
        plan_limits.fragment_nodes,
        "read scan node ID is duplicated",
        work,
    )?;
    let reads = decode_provider_reads_in(
        &wire_package.read_references,
        &provider_bindings,
        &payloads,
        invoice,
        limits.reads,
        &mut |_| Ok(()),
        work,
    )?;
    invoice = invoice.max(reads.retained_invoice_floor_in(work)?);
    let relations = decode_relations_in(
        &wire_package.relations,
        &reads,
        &types,
        invoice,
        limits.relations,
        &mut |_| Ok(()),
        work,
    )?;
    invoice = invoice.max(relations.retained_invoice_floor_in(work)?);
    let (pools, pool_requests) = if let Some(factory) = factory {
        let prepared = prepare_constant_namespace_with_factory_in(
            &wire_package.constants,
            &types,
            invoice,
            types.source_metadata_namespace(),
            limits.constant_policy,
            limits.constant_records,
            limits.constants,
            &limits.verifier,
            factory,
            &mut |_| Ok(()),
            work,
        )?;
        let (pools, facts) =
            prepared.materialize_with_factory_in(factory, &mut |_| Ok(()), work)?;
        (
            pools,
            facts.retained_new_allocation_request_bytes_upper_bound,
        )
    } else if let Some(creator) = creation.as_deref_mut() {
        // No source Fields/constants means no metadata namespace. Every
        // captured source occurrence still requires its original receipt.
        let source_metadata = types.source_metadata_namespace();
        let prepared = prepare_constant_namespace_with_creator_in(
            &wire_package.constants,
            &types,
            invoice,
            source_metadata,
            limits.constant_policy,
            limits.constant_records,
            limits.constants,
            &limits.verifier,
            creator,
            &mut |_| Ok(()),
            work,
        )?;
        let requests = prepared.facts().new_allocation_request_bytes_upper_bound;
        (
            prepared.materialize_with_creator_in(creator, &mut |_| Ok(()), work)?,
            requests,
        )
    } else {
        let prepared = prepare_constant_namespace_in(
            &wire_package.constants,
            &types,
            invoice,
            limits.constant_policy,
            limits.constant_records,
            limits.constants,
            &limits.verifier,
            &mut |_| Ok(()),
            work,
        )?;
        let requests = prepared.facts().new_allocation_request_bytes_upper_bound;
        (prepared.materialize_in(&mut |_| Ok(()), work)?, requests)
    };
    invoice = add(add(invoice, pool_requests)?, size_of_val(&pools))?;
    let (requests, request_storage) = if let Some(creator) = creation.as_deref_mut() {
        let prepared = prepare_call_requests_decode_with_creator_in(
            fragment_wire.call_requests.as_ref(),
            CallRequestTypeOwner::Package(&types),
            &pools,
            invoice,
            limits.requests,
            creator,
            work,
        )?;
        decode_call_requests_with_creator_in::<H>(prepared, work)?.into_parts()
    } else {
        let prepared = prepare_call_requests_decode_in(
            fragment_wire.call_requests.as_ref(),
            &types,
            &pools,
            invoice,
            limits.requests,
            &mut |_| Ok(()),
            work,
        )?;
        (
            decode_call_requests_in(prepared, &mut |_| Ok(()), work)?,
            ValueTypeStorageLifetime::pure(),
        )
    };
    let (cuts, _) = decode_fragment_cuts_observed(
        cuts_wire,
        &types,
        invoice,
        limits.cuts,
        &mut |_| Ok(()),
        work,
    )?;
    let values = decode_values_observed_in(
        &fragment_wire.values,
        &payloads,
        &types,
        invoice,
        limits.values,
        &mut |_| Ok(()),
        work,
    )?;
    invoice = invoice.max(values.retained_floor_observed(work)?);

    // L3-L5: every loan of values, parameters and pools ends with this block.
    // Headers and the expression namespace precede the materializations whose
    // laws add only their own outputs to those borrowed floors.
    let (arena, nodes, result) = {
        let function_headers = prepare_function_binding_headers_in(
            &wire_package.function_bindings,
            &types,
            invoice,
            limits.bindings,
            &mut |_| Ok(()),
            work,
        )?;
        invoice = invoice.max(function_headers.retained_invoice_floor()?);
        let aggregate_source = add(
            invoice,
            aggregate_definition_backing(&wire_package.aggregate_bindings, work)?,
        )?;
        let aggregate_headers = prepare_aggregate_binding_headers_in(
            &wire_package.aggregate_bindings,
            &function_headers,
            aggregate_source,
            limits.bindings,
            &mut |_| Ok(()),
            work,
        )?;
        invoice = invoice.max(aggregate_headers.retained_invoice_floor()?);
        let expressions = decode_expression_definitions_in(
            &fragment_wire.expressions,
            &values,
            &function_headers,
            &aggregate_headers,
            &parameters,
            &pools,
            invoice,
            limits.expressions,
            &mut |_| Ok(()),
            work,
        )?;
        invoice = invoice.max(expressions.retained_floor_observed(work)?);
        let function_token = prepare_function_bindings_materialization_in(
            &function_headers,
            invoice,
            limits.bindings,
            &mut |_| Ok(()),
            work,
        )?;
        let functions = if let Some(creator) = creation.as_deref_mut() {
            crate::physical_binding_v2::materialize_function_bindings_with_creator_in(
                function_token,
                creator,
                work,
            )?
        } else {
            materialize_function_bindings_in(function_token, &mut |_| Ok(()), work)?
        };
        invoice = if creation.is_some() {
            let requests = functions.creation_request_bytes_upper_bound().ok_or(
                PackageDecodeError::Invalid("strong binding construction provenance is absent"),
            )?;
            add(invoice, add(std::mem::size_of_val(&functions), requests)?)?
        } else {
            invoice.max(functions.retained_invoice_floor()?)
        };
        let aggregate_token = prepare_aggregate_bindings_materialization_in(
            &aggregate_headers,
            &functions,
            invoice,
            limits.bindings,
            &mut |_| Ok(()),
            work,
        )?;
        let aggregates = if let Some(creator) = creation.as_deref_mut() {
            crate::physical_aggregate_binding_v2::materialize_aggregate_bindings_with_creator_in(
                aggregate_token,
                creator,
                work,
            )?
        } else {
            materialize_aggregate_bindings_in(aggregate_token, &mut |_| Ok(()), work)?
        };
        invoice = if creation.is_some() {
            let requests = aggregates.creation_request_bytes_upper_bound().ok_or(
                PackageDecodeError::Invalid("strong binding construction provenance is absent"),
            )?;
            add(invoice, add(std::mem::size_of_val(&aggregates), requests)?)?
        } else {
            invoice.max(aggregates.retained_invoice_floor()?)
        };
        let materialized = materialize_expressions_in(
            prepare_expression_materialization_in(
                &expressions,
                &functions,
                &aggregates,
                &plan_limits,
                invoice,
                limits.expressions,
                &mut |_| Ok(()),
                work,
            )?,
            &mut |_| Ok(()),
            work,
        )?;
        invoice = invoice.max(materialized.retained_invoice_floor()?);
        let arena = materialized.into_arena();
        let result_source = add(
            invoice,
            result_port_backing(wire_package.result.as_ref(), work)?,
        )?;
        let (result, _) = prepare_result_decode_in(
            wire_package.result.as_ref(),
            &values,
            result_source,
            limits.node,
            &mut |_| Ok(()),
            work,
        )?
        .emit_in(&mut |_| Ok(()), work)?;
        let context = NodeDecodeContext {
            expressions: &expressions,
            relations: &relations,
            functions: &functions,
            aggregates: &aggregates,
        };
        let dispatch = NodeDispatchLimits {
            node: limits.node,
            binding: limits.bindings,
            relation: limits.relations,
            writer_schema: limits.writer_schema,
        };
        admit_count(fragment_wire.nodes.len(), plan_limits.fragment_nodes)?;
        let mut nodes = BTreeMap::new();
        for input in &fragment_wire.nodes {
            let (node, _) =
                prepare_node_decode_in(input, &context, invoice, dispatch, &mut |_| Ok(()), work)?
                    .emit_in(&mut |_| Ok(()), work)?;
            insert_unique(
                &mut nodes,
                node.id,
                node,
                "fragment node ID is duplicated",
                work,
            )?;
        }
        (arena, nodes, result)
    };

    // L6: all namespace loans have ended; move the owned values out.
    let definitions = values.into_values();
    admit_count(definitions.len(), plan_limits.fragment_values)?;
    let mut values = BTreeMap::new();
    for value in definitions {
        insert_unique(
            &mut values,
            value.id,
            value,
            "fragment value ID is duplicated",
            work,
        )?;
    }

    // L7: the original structural constructor and request publication.
    let fragment = p::Fragment::try_from_structure_in(
        p::FragmentStructureInput {
            id: envelope.id,
            root: envelope.root,
            values,
            expressions: arena,
            nodes,
            sink: envelope.sink,
            summaries: envelope.summaries,
            dop_domain: envelope.dop_domain,
            stage_placement: envelope.stage_placement,
            runtime_filters: envelope.runtime_filters,
        },
        plan_limits,
        work,
    )?;
    let fragment = if let Some(creator) = creation {
        fragment.with_call_requests_created_in(requests, request_storage, creator, work)?
    } else {
        fragment.with_call_requests_in(requests, work)?
    };

    // L8: components whose membership is the final Fragment.
    let (expression_uses, _) = decode_expression_control_observed(
        &fragment,
        control_wire,
        invoice,
        limits.control,
        &mut |_| Ok(()),
        work,
    )?;
    let (calls, _) = decode_frozen_calls_observed(
        &fragment,
        &expression_uses,
        calls_wire,
        invoice,
        limits.node,
        &mut |_| Ok(()),
        work,
    )?;
    let (pruning, _) = decode_frozen_pruning_observed(
        fragment.id(),
        pruning_wire,
        invoice,
        limits.node,
        &mut |_| Ok(()),
        work,
    )?;

    // L9: the original checked Package constructor on this same scope.
    let package = p::FragmentPackage::try_new_in(
        p::FragmentPackageInput {
            constants: pools,
            version: metadata.version,
            required: metadata.required,
            fragment,
            expression_uses,
            calls,
            pruning,
            cuts,
            result,
            parameters,
            scans,
            writes,
            annotations: metadata.annotations,
        },
        limits.admission,
        &mut |_| Ok(()),
        work,
    )?;
    Ok(match types.source_metadata_namespace() {
        Some(namespace) => package.with_original_metadata_namespace(namespace.clone()),
        None => package,
    })
}

#[cfg(test)]
#[path = "decode/tests.rs"]
mod tests;
