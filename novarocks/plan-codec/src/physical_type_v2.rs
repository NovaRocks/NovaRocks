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

//! Exact flat v2 type projection after resource-safe DTO admission. Explicit
//! caller limits bound this projection; they are not a MEM allocation grant
//! or a pre-Prost raw-byte resource model. Sparse identities remain separate
//! from table positions, and every decoded value retains its logical domain.

use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::physical_type_v2 as wire;
#[cfg(test)]
use novarocks_type_contract::ValueLogicalType;
use novarocks_type_contract::{
    CarrierParameterError, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionValueType, PureCompileControl, ValueTypeError, ValueTypeVisit,
    validate_arrow_carrier_parameters_observed,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

mod decode;
mod decode_resources;
mod encode;
mod encode_resources;
mod graph;
mod graph_domains;
mod package_graph;
mod root_sources;
mod scalars;
mod semantic;

pub use root_sources::{
    PackageTypeRootSource, PackageTypeRootSourceFacts, PackageTypeRootSources, WriterTypeRootRole,
    prepare_package_type_root_sources,
};

pub use package_graph::{
    PackageTypeGraphDefinition, PackageTypeGraphFacts, PackageTypeRootDomain,
    PreparedPackageTypeGraph, prepare_package_type_graph,
};

#[derive(Debug)]
pub enum TypeCodecError {
    InvalidShape(&'static str),
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Carrier(CarrierParameterError),
    Writer(novarocks_connector_contract::ConnectorError),
    ResourceSource(&'static str),
    Logical(crate::value_type_table::LogicalTypeCodecError),
    ExecutionValue(novarocks_type_contract::ExecutionValueTypeError),
}
impl fmt::Display for TypeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) => f.write_str(message),
            Self::Control(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::Carrier(error) => error.fmt(f),
            Self::Writer(error) => error.fmt(f),
            Self::ResourceSource(message) => f.write_str(message),
            Self::Logical(error) => error.fmt(f),
            Self::ExecutionValue(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for TypeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::ValueType(source) => Some(source),
            Self::Carrier(source) => Some(source),
            Self::Writer(source) => Some(source),
            Self::Logical(source) => Some(source),
            Self::ExecutionValue(source) => Some(source),
            Self::InvalidShape(_) | Self::ResourceSource(_) => None,
        }
    }
}
impl From<crate::value_type_table::LogicalTypeCodecError> for TypeCodecError {
    fn from(error: crate::value_type_table::LogicalTypeCodecError) -> Self {
        match error {
            crate::value_type_table::LogicalTypeCodecError::Control(cause) => Self::Control(cause),
            error => Self::Logical(error),
        }
    }
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for TypeCodecError {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::ExecutionValue(error)
    }
}

impl From<CompileControlError> for TypeCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ValueTypeError> for TypeCodecError {
    fn from(value: ValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl From<CarrierParameterError> for TypeCodecError {
    fn from(value: CarrierParameterError) -> Self {
        Self::Carrier(value)
    }
}

impl From<novarocks_connector_contract::ConnectorError> for TypeCodecError {
    fn from(error: novarocks_connector_contract::ConnectorError) -> Self {
        Self::Writer(error)
    }
}

impl<H> From<CompileControlError>
    for crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>
{
    fn from(cause: CompileControlError) -> Self {
        Self::Codec(TypeCodecError::Control(cause))
    }
}
impl<H> From<ValueTypeError> for crate::host_projection_v2::ProjectionFailure<TypeCodecError, H> {
    fn from(error: ValueTypeError) -> Self {
        Self::Codec(TypeCodecError::ValueType(error))
    }
}
impl<H> From<CarrierParameterError>
    for crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>
{
    fn from(error: CarrierParameterError) -> Self {
        Self::Codec(TypeCodecError::Carrier(error))
    }
}
impl<H> From<novarocks_connector_contract::ConnectorError>
    for crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>
{
    fn from(error: novarocks_connector_contract::ConnectorError) -> Self {
        Self::Codec(TypeCodecError::Writer(error))
    }
}

/// Explicit whole-package Type projection ceilings. These numerical bounds
/// admit the actual original source, graph, Arrow requests and opaque work;
/// they neither grant host memory nor validate the rest of the package.
#[derive(Clone, Copy, Debug)]
pub struct PackageTypeProjectionLimits {
    pub max_definitions: usize,
    pub max_expanded_nodes: usize,
    pub max_string_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackageTypeProjectionFacts {
    pub definition_count: usize,
    pub expanded_node_count: usize,
    pub string_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

/// Decode the original package Type table under its actual root domains.
/// Topology and numerical requests pass before Arrow materialization. The
/// original Writer field law then validates each real recipe occurrence before
/// publication; full writer/provider/package admission remains with its owners.
/// The caller lends its existing scope and owns entry and ordinary/success
/// tails. A control refusal returns directly without an additional callback.
pub fn decode_package_type_table_observed(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, TypeCodecError> {
    decode::decode_package(package, source_retained_bytes, limits, admit, work)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeMaterializationNamespace {
    ExecutionPhysical,
    Source,
}

/// Funding around the original physical/source materialization tail only.
/// Logical namespace construction retains its own creator responsibility.
/// A host may run the borrowed continuation at most once; it authors no types.
pub trait PackageTypeMaterializationScope {
    type HostError;
    fn materialize<C, B>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<C, Self::HostError>>
    where
        C: From<TypeCodecError>,
        B: FnOnce() -> Result<DecodedTypeTable, C>;

    fn materialize_namespace<C, B>(
        &mut self,
        _namespace: TypeMaterializationNamespace,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<C, Self::HostError>>
    where
        C: From<TypeCodecError>,
        B: FnOnce() -> Result<DecodedTypeTable, C>,
    {
        self.materialize(facts, body)
    }
}

/// Object-safe funding for a receiver with an already selected real creator.
/// Both original creator and funding failures stay in the caller's typed port.
pub trait PackageTypeFundingScope<H> {
    fn materialize(
        &mut self,
        namespace: TypeMaterializationNamespace,
        facts: &PackageTypeProjectionFacts,
        body: &mut dyn FnMut() -> Result<
            DecodedTypeTable,
            crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
        >,
    ) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>;
}

impl<H, T: PackageTypeMaterializationScope<HostError = H>> PackageTypeFundingScope<H> for T {
    fn materialize(
        &mut self,
        namespace: TypeMaterializationNamespace,
        facts: &PackageTypeProjectionFacts,
        body: &mut dyn FnMut() -> Result<
            DecodedTypeTable,
            crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
        >,
    ) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>
    {
        use crate::host_projection_v2::ProjectionFailure;
        match PackageTypeMaterializationScope::materialize_namespace(self, namespace, facts, body) {
            Ok(value) => Ok(value),
            Err(ProjectionFailure::Host(cause)) => Err(ProjectionFailure::Host(cause)),
            Err(ProjectionFailure::Codec(original)) => Err(original),
        }
    }
}

/// Pure/direct expansion has no construction funding capability.
pub struct DirectTypeMaterialization;
impl PackageTypeMaterializationScope for DirectTypeMaterialization {
    type HostError = std::convert::Infallible;
    fn materialize<C, B>(
        &mut self,
        _: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<C, Self::HostError>>
    where
        C: From<TypeCodecError>,
        B: FnOnce() -> Result<DecodedTypeTable, C>,
    {
        body().map_err(crate::host_projection_v2::ProjectionFailure::Codec)
    }
}

pub fn decode_package_type_table_with_host_observed<G: PackageTypeMaterializationScope>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
    host: &mut G,
) -> Result<
    DecodedTypeTable,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, G::HostError>,
> {
    decode::decode_package_with_funding(package, source_retained_bytes, limits, admit, work, host)
}

pub(crate) fn decode_package_type_table_with_funding_in<H>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
    funding: &mut dyn PackageTypeFundingScope<H>,
) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    decode::decode_package_with_funding(package, source, limits, admit, work, funding)
}

pub fn decode_package_type_table_with_factory_and_funding_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
    funding: &mut dyn PackageTypeFundingScope<H>,
) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    decode::decode_package_with_factory_and_funding(
        package, source, limits, creation, factory, work, funding,
    )
}

/// Production creator supplies the real retained storage lifetime. The caller
/// owns admission/control; this entry neither mints a hold nor a capacity grant.
pub fn decode_package_type_table_with_storage_observed(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source_retained_bytes: usize,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, TypeCodecError> {
    decode::decode_package_with_storage(
        package,
        source_retained_bytes,
        storage,
        limits,
        admit,
        work,
    )
}

/// Decode the actual package owner with its original creator and control.
/// Opaque library/scratch upper bounds stay distinct from exact Layout requests.
pub fn decode_package_type_table_with_creator_in<H>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    original_source_bytes: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    decode::decode_package_with_creator(package, original_source_bytes, limits, creation, work)
}

/// Independent logical entity lifetimes supplied by the actual BE creator.
/// The original complete-table numeric model remains the sole cap author.
pub fn decode_package_type_table_with_factory_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    original_source_bytes: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedTypeTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    decode::decode_package_with_factory(
        package,
        original_source_bytes,
        limits,
        creation,
        factory,
        work,
    )
}

/// A caller-authored projection envelope. No guessed or unbounded default is
/// available. Definitions count logical, carrier, field and binding namespaces;
/// expanded nodes count
/// each definition's unfolded carrier subtree, including field/value copies and
/// repeated references. String
/// bytes include field names, metadata and timestamp zones before copying.
#[derive(Clone, Copy, Debug)]
pub struct TypeProjectionLimits {
    pub max_definitions: usize,
    pub max_expanded_nodes: usize,
    pub max_string_bytes: usize,
}

impl TypeProjectionLimits {
    /// Finite source grammar bound, derived from this owner's actual root count.
    /// Source attributes have their own byte laws and never authorize values.
    pub fn for_validated_source_root_count(roots: usize) -> Result<Self, CompileControlError> {
        use novarocks_type_contract::{
            MAX_ARROW_FIELD_METADATA_BYTES as ATTRS, MAX_ARROW_FIELD_NAME_BYTES as NAME,
            MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES as ZONE, MAX_VALUE_TYPE_DEPTH as D,
            MAX_VALUE_TYPE_NODES as N,
        };
        let overflow = || CompileControlError::ResourceExhausted;
        let nodes = roots
            .checked_mul(N.checked_add(1).ok_or_else(overflow)?)
            .ok_or_else(overflow)?;
        let definitions = nodes.checked_mul(2).ok_or_else(overflow)?;
        Ok(Self {
            max_definitions: definitions,
            max_expanded_nodes: definitions
                .checked_mul(D.checked_add(1).ok_or_else(overflow)?)
                .ok_or_else(overflow)?,
            max_string_bytes: nodes
                .checked_mul(
                    NAME.checked_add(ATTRS)
                        .and_then(|n| n.checked_add(ZONE))
                        .ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?,
        })
    }
}

// The physical backing and its allocation responsibility travel together.
// Inline map entries allocate no extra owner; independently held layouts
// borrow only their corresponding entry's reachable backing lifetime.
pub(super) struct Materialized<T> {
    // Only actual value-layout roots freeze this cache. Source/unused Fields
    // remain physical facts and never manufacture a semantic declaration.
    pub(super) layout: Option<novarocks_type_contract::ValueLayout>,
    pub(super) value: T,
    pub(super) storage: novarocks_type_contract::ValueTypeStorageLifetime,
    // DataType Dictionary clones copy Boxes; only their shared descendants
    // borrow original backing. Arc<Field> clones borrow the complete Field.
    pub(super) clone_storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl<T> std::ops::Deref for Materialized<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

pub struct DecodedTypeTable {
    pub(super) source_fields: Option<DecodedSourceFieldTable>,
    pub(super) field_execution_refs: Vec<(u32, bool)>,
    pub(super) creation_requests: Option<usize>,
    pub(super) metadata_namespace: Option<novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
    pub(super) carriers: BTreeMap<u32, Materialized<DataType>>,
    pub(super) fields: BTreeMap<u32, Materialized<Arc<Field>>>,
    pub(super) values: BTreeMap<u32, FunctionValueType>,
    pub(super) field_values: BTreeMap<u32, FunctionValueType>,
    // Drop original temporary/container backing before its responsibility.
    pub(super) storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl DecodedTypeTable {
    /// Validate a source-only role against this exact decoded wire graph.
    /// Shared physical carriers are legal; any reachable Field logical/value
    /// reference is not a source-shape fact, even below an unpaired root.
    pub fn validate_source_field_root_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeCodecError> {
        let position = crate::value_type_table::resources::find(
            &self.field_execution_refs,
            id,
            |field| field.0,
            work,
        )?
        .ok_or(TypeCodecError::InvalidShape("dangling source field root"))?;
        work.step()?;
        if self.field_execution_refs[position].1 {
            Err(TypeCodecError::InvalidShape(
                "source-only field graph contains execution references",
            ))
        } else {
            Ok(())
        }
    }

    /// Carried original decoder construction upper, not private retained size.
    /// Pure expansion owners have no supplied construction provenance.
    pub fn creation_request_bytes_upper_bound(&self) -> Option<usize> {
        self.creation_requests
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub fn metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.metadata_namespace.as_ref()
    }
    pub fn source_metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.source_fields
            .as_ref()
            .and_then(DecodedSourceFieldTable::metadata_namespace)
    }
    /// Borrow the exact source occurrence and only its reachable backing loan.
    /// This token contains no table/container marker or allocation authority.
    pub fn source_field_borrow(&self, id: u32) -> Option<SourceFieldBorrow<'_>> {
        self.source_fields.as_ref()?.field_borrow(id)
    }
    pub fn source_field(&self, id: u32) -> Option<&Arc<Field>> {
        self.source_fields
            .as_ref()
            .and_then(|source| source.fields.get(&id).map(|field| &field.value))
    }
    pub(crate) fn source_field_count(&self) -> usize {
        self.source_fields
            .as_ref()
            .map_or(0, |source| source.fields.len())
    }
    pub(crate) fn source_field_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Arc<Field>>, TypeCodecError> {
        match &self.source_fields {
            Some(source) => source.field_observed(id, work),
            None => {
                work.step()?;
                Ok(None)
            }
        }
    }
    pub fn carrier(&self, id: u32) -> Option<&DataType> {
        self.carriers.get(&id).map(|carrier| &carrier.value)
    }
    pub fn field(&self, id: u32) -> Option<&Arc<Field>> {
        self.fields.get(&id).map(|field| &field.value)
    }
    /// Actual sparse Field namespace size for admission of opaque lookups.
    pub(crate) fn field_count(&self) -> usize {
        self.fields.len()
    }
    /// Necessary inline table and occupied Field key/handle bytes only. This
    /// is not a private BTree capacity estimate or a complete shared backing
    /// invoice; the caller owns the truthful original-source union.
    pub(crate) fn necessary_source_fields_retained_floor(&self) -> Result<usize, TypeCodecError> {
        let entries = self
            .source_field_count()
            .checked_mul(std::mem::size_of::<u32>() + std::mem::size_of::<Arc<Field>>())
            .ok_or(CompileControlError::ResourceExhausted)?;
        entries
            .checked_add(std::mem::size_of::<Self>())
            .ok_or_else(|| CompileControlError::ResourceExhausted.into())
    }
    pub(crate) fn necessary_fields_retained_floor(&self) -> Result<usize, TypeCodecError> {
        let entry = std::mem::size_of::<u32>()
            .checked_add(std::mem::size_of::<Arc<Field>>())
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.fields
            .len()
            .checked_mul(entry)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| CompileControlError::ResourceExhausted.into())
    }
    /// The caller first admits the sole BTree lookup bound using field_count,
    /// and lends its existing scope. Types own no fabricated control loan.
    pub(crate) fn field_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Arc<Field>>, TypeCodecError> {
        self.field_backing_observed(id, work)
            .map(|field| field.map(|field| field.field()))
    }
    /// Borrow one exact SchemaDefinition Field and its reachable Arrow backing.
    /// This loan carries neither a table marker nor semantic declaration/cache.
    pub(crate) fn field_backing_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<SourceFieldBorrow<'_>>, TypeCodecError> {
        work.flush()?;
        let field = self.fields.get(&id).map(|field| SourceFieldBorrow {
            field: &field.value,
            storage: &field.clone_storage,
        });
        work.step()?;
        work.flush()?;
        Ok(field)
    }
    /// Only an explicitly paired source projection has an execution type.
    pub fn field_execution_type(&self, id: u32) -> Option<&FunctionValueType> {
        self.field_values.get(&id)
    }
    pub(crate) fn field_execution_type_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&FunctionValueType>, TypeCodecError> {
        work.flush()?;
        let value = self.field_values.get(&id);
        work.step()?;
        work.flush()?;
        Ok(value)
    }
    pub(crate) fn value_type_count(&self) -> usize {
        self.values.len()
    }
    pub(crate) fn value_type_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&FunctionValueType>, TypeCodecError> {
        work.flush()?;
        let value = self.values.get(&id);
        work.step()?;
        work.flush()?;
        Ok(value)
    }
    pub fn value_type(&self, id: u32) -> Option<&FunctionValueType> {
        self.values.get(&id)
    }
    pub fn value_types(&self) -> impl ExactSizeIterator<Item = (u32, &FunctionValueType)> {
        self.values.iter().map(|(id, value)| (*id, value))
    }
}

/// Explicit pure author: no source invoice or receiving memory grant is minted.
/// The caller retains its original control and borrowed source field owners.
pub fn encode_source_field_table_pure_in(
    fields: &[(u32, &Arc<Field>)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SourceFieldTable, TypeCodecError> {
    encode::encode_source_fields_pure_in(fields, limits, work)
}
pub fn encode_source_field_table_with_creator<H>(
    fields: &[(u32, &Arc<Field>)],
    original_source_bytes: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SourceFieldTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>
{
    encode::encode_source_fields_with_creator(fields, original_source_bytes, limits, creation, work)
}

/// Independently held source fields, with no value/logical namespace. Callers
/// borrow both the exact Field and this creation lifetime into IPC preparation.
/// One exact source occurrence. Cloning its lifetime preserves only the
/// original Field and reachable physical child backing, never its namespace.
#[derive(Clone, Copy)]
pub struct SourceFieldBorrow<'a> {
    field: &'a Arc<Field>,
    storage: &'a novarocks_type_contract::ValueTypeStorageLifetime,
}
impl<'a> SourceFieldBorrow<'a> {
    pub fn field(&self) -> &'a Arc<Field> {
        self.field
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
    }
}

pub struct DecodedSourceFieldTable {
    pub(super) fields: BTreeMap<u32, Materialized<Arc<Field>>>,
    pub(super) metadata_namespace: Option<novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
    pub(super) creation_requests: Option<usize>,
    pub(super) storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl DecodedSourceFieldTable {
    pub fn creation_request_bytes_upper_bound(&self) -> Option<usize> {
        self.creation_requests
    }
    pub fn field_borrow(&self, id: u32) -> Option<SourceFieldBorrow<'_>> {
        self.fields.get(&id).map(|field| SourceFieldBorrow {
            field: &field.value,
            storage: &field.storage,
        })
    }
    pub fn field_borrow_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<SourceFieldBorrow<'_>>, TypeCodecError> {
        work.flush()?;
        let field = self.field_borrow(id);
        work.step()?;
        work.flush()?;
        Ok(field)
    }
    pub fn field_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Arc<Field>>, TypeCodecError> {
        self.field_borrow_observed(id, work)
            .map(|field| field.map(|field| field.field()))
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub fn metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.metadata_namespace.as_ref()
    }
}

pub fn decode_source_field_table_with_creator<H>(
    table: &wire::SourceFieldTable,
    original_source_bytes: usize,
    limits: PackageTypeProjectionLimits,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedSourceFieldTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>
{
    decode::decode_source_fields_with_creator(table, original_source_bytes, limits, creation, work)
}

/// Derive finite structural bounds from this source owner's actual vertices.
/// The caller's actual source provenance and creator remain mandatory; no
/// sender policy or synthetic receiving quota authorizes construction.
pub fn decode_source_field_table_input_derived_with_creator<H>(
    table: &wire::SourceFieldTable,
    original_source_bytes: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedSourceFieldTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>
{
    decode::decode_source_fields_input_derived_with_creator(
        table,
        original_source_bytes,
        creation,
        work,
    )
}

/// The actual factory creates independent source Fields/carriers under the
/// same input-derived receiver model, checkpoints and full namespace grammar.
pub fn decode_source_field_table_input_derived_with_factory<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    table: &wire::SourceFieldTable,
    original_source_bytes: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedSourceFieldTable, crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>>
{
    decode::decode_source_fields_input_derived_with_factory(
        table,
        original_source_bytes,
        creation,
        factory,
        work,
    )
}

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// Input storage is a loan policy, not a second type grammar. Borrowed roots
/// preserve the original checked Package objects without cloning Dictionary
/// boxes or manufacturing Field handles. Both policies use the same emitter.
#[derive(Clone, Copy)]
pub(crate) enum ValueRootSources<'source> {
    Owned(&'source [(u32, FunctionValueType)]),
    Borrowed(&'source [(u32, &'source FunctionValueType)]),
}
impl<'source> ValueRootSources<'source> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Owned(roots) => roots.len(),
            Self::Borrowed(roots) => roots.len(),
        }
    }
    pub(super) fn iter(self) -> impl ExactSizeIterator<Item = (u32, &'source FunctionValueType)> {
        (0..self.len()).map(move |index| match self {
            Self::Owned(roots) => (roots[index].0, &roots[index].1),
            Self::Borrowed(roots) => roots[index],
        })
    }
    pub(super) fn payload_layout(self) -> Result<std::alloc::Layout, CompileControlError> {
        let layout = match self {
            Self::Owned(roots) => {
                std::alloc::Layout::array::<(u32, FunctionValueType)>(roots.len())
            }
            Self::Borrowed(roots) => {
                std::alloc::Layout::array::<(u32, &FunctionValueType)>(roots.len())
            }
        };
        layout.map_err(|_| CompileControlError::ResourceExhausted)
    }
}
/// One original field root and its optional explicitly authored execution binding.
/// A missing binding denotes source facts only; attributes never infer semantics.
#[derive(Clone, Copy)]
pub struct BorrowedFieldTypeRoot<'source> {
    pub id: u32,
    pub field: &'source Arc<Field>,
    pub value_type_id: Option<u32>,
}

#[derive(Clone, Copy)]
pub(crate) enum FieldRootSources<'source> {
    Owned(&'source [(u32, Arc<Field>)]),
    Borrowed(&'source [(u32, &'source Arc<Field>)]),
    Associated(&'source [BorrowedFieldTypeRoot<'source>]),
}
impl<'source> FieldRootSources<'source> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Owned(roots) => roots.len(),
            Self::Borrowed(roots) => roots.len(),
            Self::Associated(roots) => roots.len(),
        }
    }
    pub(super) fn iter(self) -> impl ExactSizeIterator<Item = (u32, &'source Arc<Field>)> {
        (0..self.len()).map(move |index| match self {
            Self::Owned(roots) => (roots[index].0, &roots[index].1),
            Self::Borrowed(roots) => roots[index],
            Self::Associated(roots) => (roots[index].id, roots[index].field),
        })
    }
    pub(super) fn value_binding(self, ordinal: usize) -> Option<u32> {
        match self {
            Self::Associated(roots) => roots[ordinal].value_type_id,
            Self::Owned(_) | Self::Borrowed(_) => None,
        }
    }
    pub(super) fn payload_layout(self) -> Result<std::alloc::Layout, CompileControlError> {
        let layout = match self {
            Self::Owned(roots) => std::alloc::Layout::array::<(u32, Arc<Field>)>(roots.len()),
            Self::Borrowed(roots) => std::alloc::Layout::array::<(u32, &Arc<Field>)>(roots.len()),
            Self::Associated(roots) => {
                std::alloc::Layout::array::<BorrowedFieldTypeRoot<'_>>(roots.len())
            }
        };
        layout.map_err(|_| CompileControlError::ResourceExhausted)
    }
}

pub(super) fn combine_namespace_facts(
    left: PackageTypeProjectionFacts,
    right: PackageTypeProjectionFacts,
    source: usize,
) -> Result<PackageTypeProjectionFacts, TypeCodecError> {
    let add = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(CompileControlError::ResourceExhausted)
    };
    let left_extra = if left == PackageTypeProjectionFacts::default() {
        0
    } else {
        left.coexisting_source_and_request_bytes_upper_bound
            .checked_sub(source)
            .ok_or(TypeCodecError::ResourceSource(
                "completed namespace source prefix is understated",
            ))?
    };
    let right_extra = right
        .coexisting_source_and_request_bytes_upper_bound
        .checked_sub(source)
        .ok_or(TypeCodecError::ResourceSource(
            "namespace source prefix is understated",
        ))?;
    Ok(PackageTypeProjectionFacts {
        definition_count: add(left.definition_count, right.definition_count)?,
        expanded_node_count: add(left.expanded_node_count, right.expanded_node_count)?,
        string_bytes: add(left.string_bytes, right.string_bytes)?,
        allocation_requests_upper_bound: add(
            left.allocation_requests_upper_bound,
            right.allocation_requests_upper_bound,
        )?,
        allocation_request_bytes_upper_bound: add(
            left.allocation_request_bytes_upper_bound,
            right.allocation_request_bytes_upper_bound,
        )?,
        coexisting_source_and_request_bytes_upper_bound: add(
            source,
            add(left_extra, right_extra)?,
        )?,
        cumulative_work_upper_bound: add(
            left.cumulative_work_upper_bound,
            right.cumulative_work_upper_bound,
        )?,
    })
}
pub(super) fn validate_namespace_facts(
    f: &PackageTypeProjectionFacts,
    l: PackageTypeProjectionLimits,
) -> Result<(), CompileControlError> {
    if f.definition_count > l.max_definitions
        || f.expanded_node_count > l.max_expanded_nodes
        || f.string_bytes > l.max_string_bytes
        || f.allocation_requests_upper_bound > l.max_allocation_requests
        || f.allocation_request_bytes_upper_bound > l.max_allocation_request_bytes
        || f.coexisting_source_and_request_bytes_upper_bound
            > l.max_coexisting_source_and_request_bytes
        || f.cumulative_work_upper_bound > l.max_work
    {
        Err(CompileControlError::ResourceExhausted)
    } else {
        Ok(())
    }
}

/// The original source roots and their immutable wire projection from one
/// successful emission. Root IDs occupy separate sparse namespaces; borrowing
/// this proof does not reconstruct source Fields or types from the wire DTO.
pub struct EncodedTypeTable<'source> {
    table: wire::TypeTable,
    values: ValueRootSources<'source>,
    fields: FieldRootSources<'source>,
    source_table: Option<wire::SourceFieldTable>,
    source_fields: FieldRootSources<'source>,
    writers: &'source [WriterTypeSource<'source>],
}
impl<'source> EncodedTypeTable<'source> {
    pub fn as_wire(&self) -> &wire::TypeTable {
        &self.table
    }
    pub fn into_wire(self) -> wire::TypeTable {
        self.table
    }
    #[cfg(test)]
    pub(crate) fn as_source_wire(&self) -> Option<&wire::SourceFieldTable> {
        self.source_table.as_ref()
    }
    pub fn into_namespaces(self) -> (wire::TypeTable, Option<wire::SourceFieldTable>) {
        (self.table, self.source_table)
    }
    pub(crate) fn source_field_captured<E>(
        &self,
        id: u32,
        capture: &mut impl FnMut(&'source Arc<Field>, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source Arc<Field>>, E>
    where
        E: From<TypeCodecError>,
    {
        for (candidate, field) in self.source_fields.iter() {
            let matches = candidate == id;
            if matches {
                capture(field, work)?;
            }
            work.step().map_err(TypeCodecError::from)?;
            if matches {
                return Ok(Some(field));
            }
        }
        Ok(None)
    }
    pub(crate) fn source_field_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source Arc<Field>>, TypeCodecError> {
        self.source_field_captured(id, &mut |_, _| Ok(()), work)
    }
    pub fn source_field_source_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source Field>, TypeCodecError> {
        if let Some(field) = self.source_field_observed(id, work)? {
            return Ok(Some(field.as_ref()));
        }
        for source in self.writers {
            for (candidate, binding) in source
                .field_ids
                .iter()
                .zip(source.recipe.input().fields_iter())
            {
                let matches = *candidate == id;
                work.step()?;
                if matches {
                    return Ok(Some(binding.field()));
                }
            }
        }
        Ok(None)
    }
    pub fn source_field_value_type_id_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<u32>, TypeCodecError> {
        for source in self.writers {
            for (ordinal, candidate) in source.field_ids.iter().enumerate() {
                let matches = *candidate == id;
                work.step()?;
                if matches {
                    let ids = source.value_ids.ok_or(TypeCodecError::InvalidShape(
                        "writer source has no explicit value namespace association",
                    ))?;
                    return ids.get(ordinal).copied().map(Some).ok_or(
                        TypeCodecError::InvalidShape("writer value association is absent"),
                    );
                }
            }
        }
        Ok(None)
    }
    /// Counts for caller admission of repeated root lookups. No index, source
    /// copy or maximum-ID-indexed storage is created by this owner.
    pub(crate) fn source_counts(&self) -> (usize, usize) {
        (
            self.values.len(),
            self.fields.len()
                + self.source_fields.len()
                + self
                    .writers
                    .iter()
                    .map(|source| source.field_ids.len())
                    .sum::<usize>(),
        )
    }
    /// The caller owns entry/ordinary/success tails on the original meter.
    /// Only completed ID comparisons are charged here, including a miss.
    pub(crate) fn value_type_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source FunctionValueType>, TypeCodecError> {
        self.root_value_binding_observed(id, work)
            .map(|binding| binding.map(|(_, value)| value))
    }
    /// Capture an original root FVT before the matched lookup's completed
    /// observation. The consumer synchronously admits its own comparison or
    /// clone requests; the type namespace creates no replacement source.
    pub(crate) fn value_type_captured<E>(
        &self,
        id: u32,
        capture: &mut impl FnMut(
            &'source FunctionValueType,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source FunctionValueType>, E>
    where
        E: From<TypeCodecError>,
    {
        self.root_value_binding_captured(id, capture, work)
            .map(|binding| binding.map(|(_, value)| value))
    }
    /// Bind an authored value root to the carrier occurrence assigned by the
    /// same emission. This is not a lookup of arbitrary nested carriers, and
    /// the carrier ID does not replace the complete source value type.
    /// The caller admits repeated linear lookups using `source_counts` and
    /// owns entry/ordinary/success tails on the original meter.
    pub(crate) fn root_value_binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<(u32, &'source FunctionValueType)>, TypeCodecError> {
        self.root_value_binding_captured(id, &mut |_, _| Ok(()), work)
    }
    fn root_value_binding_captured<E>(
        &self,
        id: u32,
        capture: &mut impl FnMut(
            &'source FunctionValueType,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<(u32, &'source FunctionValueType)>, E>
    where
        E: From<TypeCodecError>,
    {
        for (position, (candidate, value)) in self.values.iter().enumerate() {
            let matches = candidate == id;
            if matches {
                capture(value, work)?;
            }
            work.step().map_err(TypeCodecError::from)?;
            if matches {
                // Root value definitions are emitted one-for-one in source
                // order. These constant-time defensive checks neither build
                // an index nor reconstruct a source type from the DTO.
                let emitted =
                    self.table
                        .value_types
                        .get(position)
                        .ok_or(TypeCodecError::InvalidShape(
                            "encoded root value definition is absent",
                        ))?;
                if emitted.id != candidate {
                    return Err(TypeCodecError::InvalidShape(
                        "encoded root value definition differs from its source",
                    )
                    .into());
                }
                let carrier = emitted.carrier_type_id.ok_or(TypeCodecError::InvalidShape(
                    "encoded root value carrier is absent",
                ))?;
                return Ok(Some((carrier, value)));
            }
        }
        Ok(None)
    }
    /// Borrow the complete explicitly paired execution author retained by the
    /// immutable emission. Source-only fields return None rather than deriving
    /// semantics from Arrow facts.
    pub fn field_execution_type_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source FunctionValueType>, TypeCodecError> {
        for (ordinal, (candidate, _)) in self.fields.iter().enumerate() {
            let matches = candidate == id;
            work.step()?;
            if matches {
                return match self.fields.value_binding(ordinal) {
                    Some(binding_id) => self.value_type_observed(binding_id, work),
                    None => Ok(None),
                };
            }
        }
        for source in self.writers {
            for (candidate, binding) in source
                .field_ids
                .iter()
                .zip(source.recipe.input().fields_iter())
            {
                let matches = *candidate == id;
                work.step()?;
                if matches {
                    return Ok(binding.execution_type());
                }
            }
        }
        Ok(None)
    }
    /// Borrow the actual authored root Field. Writer fields remain inline in
    /// their original immutable recipe; no Arc wrapper or equal-content copy
    /// is manufactured to satisfy a later namespace consumer.
    pub fn field_source_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source Field>, TypeCodecError> {
        if let Some(field) = self.field_observed(id, work)? {
            return Ok(Some(field.as_ref()));
        }
        for source in self.writers {
            for (candidate, binding) in source
                .field_ids
                .iter()
                .zip(source.recipe.input().fields_iter())
            {
                let matches = *candidate == id;
                work.step()?;
                if matches {
                    return Ok(Some(binding.field()));
                }
            }
        }
        Ok(None)
    }
    pub(crate) fn field_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source Arc<Field>>, TypeCodecError> {
        self.field_captured(id, &mut |_, _| Ok(()), work)
    }
    /// Synchronously admit a consumer's known requests while retaining the
    /// actual Field loan, before the matched comparison's completed callback.
    pub(crate) fn field_captured<E>(
        &self,
        id: u32,
        capture: &mut impl FnMut(&'source Arc<Field>, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source Arc<Field>>, E>
    where
        E: From<TypeCodecError>,
    {
        for (candidate, field) in self.fields.iter() {
            let matches = candidate == id;
            if matches {
                capture(field, work)?;
            }
            work.step().map_err(TypeCodecError::from)?;
            if matches {
                return Ok(Some(field));
            }
        }
        Ok(None)
    }
}

/// Ordered root IDs for every actual input-field occurrence of one checked,
/// immutable Writer recipe. IDs are a projection mapping, never type facts or
/// proof of provider capability. Encoding checks the exact occurrence count
/// and root namespace uniqueness before emitting any definition.
pub struct WriterTypeSource<'source> {
    recipe: &'source novarocks_connector_contract::ConnectorWriteRecipeDraft,
    field_ids: &'source [u32],
    value_ids: Option<&'source [u32]>,
}
impl<'source> WriterTypeSource<'source> {
    pub(crate) fn recipe(
        &self,
    ) -> &'source novarocks_connector_contract::ConnectorWriteRecipeDraft {
        self.recipe
    }
    pub(crate) fn field_ids(&self) -> &'source [u32] {
        self.field_ids
    }
    pub fn new(
        recipe: &'source novarocks_connector_contract::ConnectorWriteRecipeDraft,
        field_ids: &'source [u32],
    ) -> Self {
        Self {
            recipe,
            field_ids,
            value_ids: None,
        }
    }
    pub(crate) fn new_with_value_ids(
        recipe: &'source novarocks_connector_contract::ConnectorWriteRecipeDraft,
        field_ids: &'source [u32],
        value_ids: &'source [u32],
    ) -> Self {
        Self {
            recipe,
            field_ids,
            value_ids: Some(value_ids),
        }
    }
}

/// Encode original roots under their actual source laws on the caller's
/// existing meter. Only a checked Writer recipe can lend the Writer domain.
/// Strict roots and Writer occurrences receive distinct definitions; the
/// original occurrence emitter performs no structural interning.
/// This port grants no provider capability or complete-package admission.
pub fn encode_type_table_writer_sources_observed<'source>(
    values: &'source [(u32, FunctionValueType)],
    fields: &'source [(u32, Arc<Field>)],
    writers: &'source [WriterTypeSource<'source>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    let values = ValueRootSources::Owned(values);
    let fields = FieldRootSources::Owned(fields);
    let table = encode::encode_writer_sources(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        admit,
        work,
    )?;
    Ok(EncodedTypeTable {
        table,
        values,
        fields,
        writers,
        source_table: None,
        source_fields: FieldRootSources::Borrowed(&[]),
    })
}

/// Whole-package source views lend original roots; no owned type/Field
/// copies are made to adapt a namespace input. The caller retains all views
/// for the token lifetime and owns admission and completion on this meter.
pub fn encode_borrowed_type_table_writer_sources_in<'source>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [(u32, &'source Arc<Field>)],
    writers: &'source [WriterTypeSource<'source>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    let values = ValueRootSources::Borrowed(values);
    let fields = FieldRootSources::Borrowed(fields);
    let table = encode::encode_writer_sources(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        admit,
        work,
    )?;
    Ok(EncodedTypeTable {
        table,
        values,
        fields,
        writers,
        source_table: None,
        source_fields: FieldRootSources::Borrowed(&[]),
    })
}

/// The same original type sender with a nominal host-refusal channel. Only the
/// host owns the payload; a host refusal stops this capture without a Control lie.
/// Existing Control-only APIs remain adapters to the same computational body.
pub fn encode_borrowed_type_table_writer_sources_with_host_in<'source, H>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [(u32, &'source Arc<Field>)],
    writers: &'source [WriterTypeSource<'source>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(
        &PackageTypeProjectionFacts,
    ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    EncodedTypeTable<'source>,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
> {
    let values = ValueRootSources::Borrowed(values);
    let fields = FieldRootSources::Borrowed(fields);
    let table = encode::encode_writer_sources_with_host(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        admit,
        work,
    )?;
    Ok(EncodedTypeTable {
        table,
        values,
        fields,
        writers,
        source_table: None,
        source_fields: FieldRootSources::Borrowed(&[]),
    })
}

/// Pure original-source author using the same associated field emitter.
/// No source invoice or receiving memory profile is invented by this entry.
pub fn encode_borrowed_type_table_associated_sources_pure_in<'source>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [BorrowedFieldTypeRoot<'source>],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    let table = encode::encode_associated_sources_pure_in(values, fields, limits, work)?;
    Ok(EncodedTypeTable {
        table,
        values: ValueRootSources::Borrowed(values),
        fields: FieldRootSources::Associated(fields),
        writers: &[],
        source_table: None,
        source_fields: FieldRootSources::Borrowed(&[]),
    })
}

/// Encode source fields and explicit execution-field associations under the
/// same original resource/graph body. Associations refer to supplied value IDs;
/// they never manufacture a value author from a Field or its attributes.
pub fn encode_borrowed_type_table_associated_sources_with_host_in<'source, H>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [BorrowedFieldTypeRoot<'source>],
    writers: &'source [WriterTypeSource<'source>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(
        &PackageTypeProjectionFacts,
    ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    EncodedTypeTable<'source>,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
> {
    let values = ValueRootSources::Borrowed(values);
    let fields = FieldRootSources::Associated(fields);
    let table = encode::encode_writer_sources_with_host(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        admit,
        work,
    )?;
    Ok(EncodedTypeTable {
        table,
        values,
        fields,
        writers,
        source_table: None,
        source_fields: FieldRootSources::Borrowed(&[]),
    })
}

pub fn encode_borrowed_type_table_associated_sources_in<'source>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [BorrowedFieldTypeRoot<'source>],
    writers: &'source [WriterTypeSource<'source>],
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    encode_borrowed_type_table_associated_sources_with_host_in(
        values,
        fields,
        writers,
        source_retained_bytes,
        limits,
        &mut |facts| {
            admit(facts).map_err(
                crate::host_projection_v2::AdmissionRefusal::<std::convert::Infallible>::Control,
            )
        },
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

/// Project source roots once and retain their original borrowed identity for
/// later namespace binding. The same emission and resource gates author both
/// this token and the existing DTO-only public APIs.
pub fn encode_type_table_sources<'source>(
    values: &'source [(u32, FunctionValueType)],
    fields: &'source [(u32, Arc<Field>)],
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let table = encode::encode_with_fields(values, fields, limits, &mut work);
    let table = finish(work, table)?;
    Ok(EncodedTypeTable {
        table,
        values: ValueRootSources::Owned(values),
        fields: FieldRootSources::Owned(fields),
        writers: &[],
        source_table: None,
        source_fields: FieldRootSources::Borrowed(&[]),
    })
}

pub fn encode_type_table(
    values: &[(u32, FunctionValueType)],
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<wire::TypeTable, TypeCodecError> {
    encode_type_table_sources(values, &[], limits, control).map(EncodedTypeTable::into_wire)
}

/// Project explicit complete root fields as well as value types. Authored
/// field IDs are retained in their own namespace; nested occurrence fields
/// receive fresh IDs that never collide with those reserved identities.
/// This does not infer a root field from a value type or add logical metadata.
pub fn encode_type_table_with_fields(
    values: &[(u32, FunctionValueType)],
    fields: &[(u32, Arc<Field>)],
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<wire::TypeTable, TypeCodecError> {
    encode_type_table_sources(values, fields, limits, control).map(EncodedTypeTable::into_wire)
}

/// Expansion-only alias for the independently controlled pure codec helper.
/// Production package creators must supply storage through the observed entry.
pub fn decode_type_table(
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<DecodedTypeTable, TypeCodecError> {
    decode_type_table_pure(table, limits, control)
}

/// Legacy DTO-only post-Prost projection with the original Physical64 policy.
/// A bare table has no original package root-occurrence loan. Complete typed
/// roots use the package decoder, which selects their explicit value profile.
/// This helper holds no host stock or execution-depth capability.
pub fn decode_type_table_pure(
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<DecodedTypeTable, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode::decode(table, limits, &mut work);
    finish(work, result)
}

fn observe_bytes(bytes: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<(), TypeCodecError> {
    for _ in bytes.chunks(1024) {
        work.step()?;
    }
    Ok(())
}

/// New requests of cloning an already checked value root. Fields and other
/// Arc-backed carriers are shared; only owned Dictionary children allocate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ValueTypeCloneFacts {
    requests: usize,
    bytes: usize,
    work: usize,
}
impl ValueTypeCloneFacts {
    pub(crate) const fn allocation_requests_upper_bound(&self) -> usize {
        self.requests
    }
    pub(crate) const fn allocation_request_bytes_upper_bound(&self) -> usize {
        self.bytes
    }
    pub(crate) const fn work_upper_bound(&self) -> usize {
        self.work
    }
}

/// The immutable FVT clone performs two strong-handle increments. The
/// preflight inspects no semantic/layout tree and reserves no allocation.
pub(crate) const fn value_type_clone_preflight_work_upper_bound() -> usize {
    2
}

pub(crate) fn value_type_clone_root_facts(
    _value: &FunctionValueType,
) -> Result<ValueTypeCloneFacts, TypeCodecError> {
    Ok(ValueTypeCloneFacts {
        requests: 0,
        bytes: 0,
        work: 2,
    })
}
pub(crate) fn preflight_value_type_clone(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueTypeCloneFacts, TypeCodecError> {
    preflight_value_type_clone_admitted(value, &mut |_, _| Ok(()), work)
}
pub(crate) fn preflight_value_type_clone_admitted<E: From<TypeCodecError>>(
    value: &FunctionValueType,
    admit: &mut impl FnMut(ValueTypeCloneFacts, &mut CompileCheckpoints<'_>) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueTypeCloneFacts, E> {
    let facts = value_type_clone_root_facts(value)?;
    admit(facts, work)?;
    work.step().map_err(TypeCodecError::from)?;
    Ok(facts)
}
pub(crate) fn clone_value_type_observed(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, TypeCodecError> {
    value.clone_observed(|| work.step().map_err(TypeCodecError::from))
}

fn clone_carrier(
    ty: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DataType, TypeCodecError> {
    work.step()?;
    // Dictionary boxes are the only recursively owned carrier children.
    // Other nested carriers share Arrow FieldRef/Fields backing on clone.
    Ok(match ty {
        DataType::Dictionary(key, value) => {
            let key = clone_carrier(key, work)?;
            work.flush()?;
            let key = Box::new(key);
            work.flush()?;
            let value = clone_carrier(value, work)?;
            work.flush()?;
            let value = Box::new(value);
            work.flush()?;
            DataType::Dictionary(key, value)
        }
        ty => ty.clone(),
    })
}

pub(crate) fn validate_field(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    use novarocks_type_contract::{
        MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_METADATA_ENTRIES,
        MAX_ARROW_FIELD_METADATA_KEY_BYTES, MAX_ARROW_FIELD_METADATA_VALUE_BYTES,
        MAX_ARROW_FIELD_NAME_BYTES,
    };
    work.step()?;
    if field.name().len() > MAX_ARROW_FIELD_NAME_BYTES
        || field.metadata().len() > MAX_ARROW_FIELD_METADATA_ENTRIES
    {
        return Err(TypeCodecError::InvalidShape(
            "Arrow field attributes exceed their owner bounds",
        ));
    }
    observe_bytes(field.name().as_bytes(), work)?;
    let mut bytes = 0usize;
    for (key, value) in field.metadata() {
        work.step()?;
        if key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
            || value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES
        {
            return Err(TypeCodecError::InvalidShape(
                "Arrow field metadata entry exceeds its owner bound",
            ));
        }
        bytes = bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(TypeCodecError::InvalidShape(
                "Arrow field metadata size overflow",
            ))?;
        if bytes > MAX_ARROW_FIELD_METADATA_BYTES {
            return Err(TypeCodecError::InvalidShape(
                "Arrow field metadata exceeds its owner bound",
            ));
        }
        observe_bytes(key.as_bytes(), work)?;
        observe_bytes(value.as_bytes(), work)?;
    }
    Ok(())
}

/// Validate this carrier's own parameters without traversing child fields.
/// Root-domain traversal stays with its owner: a writer root must not inherit
/// the Value owner's unfolded-node or field-metadata limits.
pub(crate) fn validate_type_node(
    ty: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    validate_arrow_carrier_parameters_observed(ty, || work.step().map_err(TypeCodecError::from))?;
    match ty {
        DataType::FixedSizeBinary(size) | DataType::FixedSizeList(_, size)
            if *size > novarocks_physical_plan::MAX_FIXED_SIZE_LENGTH =>
        {
            return Err(TypeCodecError::InvalidShape(
                "Arrow fixed size exceeds its owner bound",
            ));
        }
        DataType::Timestamp(_, Some(zone)) => {
            if zone.len() > novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                return Err(TypeCodecError::InvalidShape(
                    "Arrow timestamp zone exceeds its owner bound",
                ));
            }
            observe_bytes(zone.as_bytes(), work)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_type_visit(
    visit: ValueTypeVisit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    work.step()?;
    match visit {
        ValueTypeVisit::TypeNode(ty) => validate_type_node(ty, work),
        ValueTypeVisit::Field(field) => validate_field(field, work),
        ValueTypeVisit::ChildEdge(_) => Ok(()),
    }
}

pub(crate) fn validate_type(
    ty: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    validate_type_with_profile(
        ty,
        novarocks_type_contract::ArrowTypeProfile::Physical,
        work,
    )
}

fn validate_type_with_profile(
    ty: &DataType,
    profile: novarocks_type_contract::ArrowTypeProfile,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    novarocks_type_contract::validate_arrow_layout_structure_with_profile_observed(
        ty,
        profile,
        |visit| validate_type_visit(visit, work),
    )
}

/// The same carrier and Field author on the sole shared grammar. The caller
/// admits initialization and captured source work before any later observation;
/// the fixed scratch has no heap request, control scope or namespace authority.
#[cfg(test)]
pub(crate) fn validate_type_with_scratch_observed<'source>(
    ty: &'source DataType,
    admit_scratch: &mut impl FnMut(std::alloc::Layout) -> Result<(), CompileControlError>,
    capture: &mut impl FnMut(ValueTypeVisit<'source>) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    validate_type_with_scratch_host_observed(
        ty,
        &mut |layout| {
            admit_scratch(layout).map_err(
                crate::host_projection_v2::ProjectionFailure::<
                    TypeCodecError,
                    std::convert::Infallible,
                >::from,
            )
        },
        &mut |visit| {
            capture(visit).map_err(
                crate::host_projection_v2::ProjectionFailure::<
                    TypeCodecError,
                    std::convert::Infallible,
                >::from,
            )
        },
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

pub(crate) fn validate_type_with_scratch_host_observed<'source, H>(
    ty: &'source DataType,
    admit_scratch: &mut impl FnMut(
        std::alloc::Layout,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
    capture: &mut impl FnMut(
        ValueTypeVisit<'source>,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    let layout = std::alloc::Layout::new::<
        novarocks_type_contract::owned_resources::type_validation::TypeValidationScratch<'_>,
    >();
    admit_scratch(layout)?;
    let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    novarocks_type_contract::validate_arrow_layout_structure_with_scratch_observed(
        ty,
        &mut scratch,
        |visit| {
            capture(visit)?;
            validate_type_visit(visit, work).map_err(Into::into)
        },
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod fields_tests;

#[cfg(test)]
mod sources_tests;

#[cfg(test)]
mod node_tests;

#[cfg(test)]
mod graph_tests;

#[cfg(test)]
mod package_graph_tests;

#[cfg(test)]
mod receiver_tests;

#[cfg(test)]
mod borrowed_sender_tests;
#[cfg(test)]
pub(crate) mod sender_tests;

#[cfg(test)]
mod constant_source_tests;

#[cfg(test)]
mod typed_binding_tests;

#[cfg(test)]
#[path = "physical_type_v2/source_fields_tests.rs"]
pub(crate) mod source_fields_tests;

/// Explicit pure sender: both namespaces borrow their real original authors.
/// No receiving memory/work profile or source invoice is minted for FE.
#[cfg(test)]
pub(crate) fn encode_borrowed_type_namespaces_pure_in<'source>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [BorrowedFieldTypeRoot<'source>],
    source_fields: &'source [(u32, &'source Arc<Field>)],
    limits: TypeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    let table = encode::encode_associated_sources_pure_in(values, fields, limits, work)?;
    let source_table = encode::encode_source_fields_pure_in(source_fields, limits, work)?;
    Ok(EncodedTypeTable {
        table,
        values: ValueRootSources::Borrowed(values),
        fields: FieldRootSources::Associated(fields),
        source_table: Some(source_table),
        source_fields: FieldRootSources::Borrowed(source_fields),
        writers: &[],
    })
}

/// Actual package sender: independent execution and source owners share the
/// original numerical admission prefix, source invoice and caller checkpoints.
pub(crate) fn encode_borrowed_type_namespaces_sources_in<'source>(
    values: &'source [(u32, &'source FunctionValueType)],
    fields: &'source [BorrowedFieldTypeRoot<'source>],
    source_fields: &'source [(u32, &'source Arc<Field>)],
    writers: &'source [WriterTypeSource<'source>],
    source: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedTypeTable<'source>, TypeCodecError> {
    let mut completed = PackageTypeProjectionFacts::default();
    let table = encode::encode_writer_sources(
        ValueRootSources::Borrowed(values),
        FieldRootSources::Associated(fields),
        &[],
        source,
        limits,
        &mut |facts| {
            validate_namespace_facts(facts, limits)?;
            admit(facts)?;
            completed = *facts;
            Ok(())
        },
        work,
    )?;
    let source_table = encode::encode_source_writer_sources(
        source_fields,
        writers,
        source,
        limits,
        &mut |facts| {
            let combined = combine_namespace_facts(completed, *facts, source)?;
            validate_namespace_facts(&combined, limits)?;
            admit(&combined).map_err(TypeCodecError::from)
        },
        work,
    )?;
    Ok(EncodedTypeTable {
        table,
        values: ValueRootSources::Borrowed(values),
        fields: FieldRootSources::Associated(fields),
        source_table: Some(source_table),
        source_fields: FieldRootSources::Borrowed(source_fields),
        writers,
    })
}

#[cfg(test)]
mod depth_profile_tests;
