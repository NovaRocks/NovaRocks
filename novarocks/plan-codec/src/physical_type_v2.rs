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
use novarocks_type_contract::{
    CarrierParameterError, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionValueType, PureCompileControl, ValueLogicalType, ValueTypeError, ValueTypeVisit,
    validate_arrow_carrier_parameters_observed, validate_value_type_structure_observed,
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
        }
    }
}
impl std::error::Error for TypeCodecError {}
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

/// Host capability for exactly the original final materialization tail.
/// Preflight, topology and the last numerical gate have already succeeded.
/// The synchronous continuation retains the original source borrows and may
/// execute at most once; this trait supplies no allocation or account policy.
pub trait PackageTypeMaterializationScope {
    type HostError;

    fn materialize<B>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<
        DecodedTypeTable,
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, Self::HostError>,
    >
    where
        B: FnOnce() -> Result<DecodedTypeTable, TypeCodecError>;
}

/// The original decoder's adapter, with an uninhabited host failure channel.
/// Runtime callers needing funding must supply their own explicit capability.
pub struct DirectTypeMaterialization;
impl PackageTypeMaterializationScope for DirectTypeMaterialization {
    type HostError = std::convert::Infallible;

    fn materialize<B>(
        &mut self,
        _: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<
        DecodedTypeTable,
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, Self::HostError>,
    >
    where
        B: FnOnce() -> Result<DecodedTypeTable, TypeCodecError>,
    {
        body().map_err(crate::host_projection_v2::ProjectionFailure::Codec)
    }
}

/// The same package decoder with a borrowed host at its one materialization
/// boundary. The original type/control failure and nominal host refusal remain
/// separate, with no extra decoder, checkpoint or fallible completion callback.
pub fn decode_package_type_table_with_host_observed<H: PackageTypeMaterializationScope>(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source_retained_bytes: usize,
    limits: PackageTypeProjectionLimits,
    admit: &mut impl FnMut(&PackageTypeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
    host: &mut H,
) -> Result<
    DecodedTypeTable,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H::HostError>,
> {
    decode::decode_package_with_host(package, source_retained_bytes, limits, admit, work, host)
}

/// A caller-authored projection envelope. No guessed or unbounded default is
/// available. Definitions count all three namespaces; expanded nodes count
/// each definition's unfolded carrier subtree, including field/value copies and
/// repeated references. String
/// bytes include field names, metadata and timestamp zones before copying.
#[derive(Clone, Copy, Debug)]
pub struct TypeProjectionLimits {
    pub max_definitions: usize,
    pub max_expanded_nodes: usize,
    pub max_string_bytes: usize,
}

pub struct DecodedTypeTable {
    pub(super) metadata_namespace: Option<novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
    pub(super) carriers: BTreeMap<u32, DataType>,
    pub(super) fields: BTreeMap<u32, Arc<Field>>,
    pub(super) values: BTreeMap<u32, FunctionValueType>,
}
impl DecodedTypeTable {
    pub fn metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.metadata_namespace.as_ref()
    }
    pub fn carrier(&self, id: u32) -> Option<&DataType> {
        self.carriers.get(&id)
    }
    pub fn field(&self, id: u32) -> Option<&Arc<Field>> {
        self.fields.get(&id)
    }
    /// Actual sparse Field namespace size for admission of opaque lookups.
    pub(crate) fn field_count(&self) -> usize {
        self.fields.len()
    }
    /// Necessary inline table and occupied Field key/handle bytes only. This
    /// is not a private BTree capacity estimate or a complete shared backing
    /// invoice; the caller owns the truthful original-source union.
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
        work.flush()?;
        let field = self.fields.get(&id);
        work.step()?;
        work.flush()?;
        Ok(field)
    }
    pub fn value_type(&self, id: u32) -> Option<&FunctionValueType> {
        self.values.get(&id)
    }
    pub fn value_types(&self) -> impl ExactSizeIterator<Item = (u32, &FunctionValueType)> {
        self.values.iter().map(|(id, value)| (*id, value))
    }
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
#[derive(Clone, Copy)]
pub(crate) enum FieldRootSources<'source> {
    Owned(&'source [(u32, Arc<Field>)]),
    Borrowed(&'source [(u32, &'source Arc<Field>)]),
}
impl<'source> FieldRootSources<'source> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Owned(roots) => roots.len(),
            Self::Borrowed(roots) => roots.len(),
        }
    }
    pub(super) fn iter(self) -> impl ExactSizeIterator<Item = (u32, &'source Arc<Field>)> {
        (0..self.len()).map(move |index| match self {
            Self::Owned(roots) => (roots[index].0, &roots[index].1),
            Self::Borrowed(roots) => roots[index],
        })
    }
    pub(super) fn payload_layout(self) -> Result<std::alloc::Layout, CompileControlError> {
        let layout = match self {
            Self::Owned(roots) => std::alloc::Layout::array::<(u32, Arc<Field>)>(roots.len()),
            Self::Borrowed(roots) => std::alloc::Layout::array::<(u32, &Arc<Field>)>(roots.len()),
        };
        layout.map_err(|_| CompileControlError::ResourceExhausted)
    }
}

/// The original source roots and their immutable wire projection from one
/// successful emission. Root IDs occupy separate sparse namespaces; borrowing
/// this proof does not reconstruct source Fields or types from the wire DTO.
pub struct EncodedTypeTable<'source> {
    table: wire::TypeTable,
    values: ValueRootSources<'source>,
    fields: FieldRootSources<'source>,
    writers: &'source [WriterTypeSource<'source>],
}
impl<'source> EncodedTypeTable<'source> {
    pub fn as_wire(&self) -> &wire::TypeTable {
        &self.table
    }
    pub fn into_wire(self) -> wire::TypeTable {
        self.table
    }
    /// Counts for caller admission of repeated root lookups. No index, source
    /// copy or maximum-ID-indexed storage is created by this owner.
    pub(crate) fn source_counts(&self) -> (usize, usize) {
        (
            self.values.len(),
            self.fields.len()
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
        Self { recipe, field_ids }
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
    })
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

pub fn decode_type_table(
    table: &wire::TypeTable,
    limits: TypeProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<DecodedTypeTable, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode::decode(table, limits, &mut work);
    finish(work, result)
}

pub(crate) fn encode_logical(logical: ValueLogicalType) -> i32 {
    use wire::LogicalType as W;
    (match logical {
        ValueLogicalType::Physical => W::Physical,
        ValueLogicalType::Json => W::Json,
        ValueLogicalType::Variant => W::Variant,
        ValueLogicalType::Hll => W::Hll,
        ValueLogicalType::Bitmap => W::Bitmap,
        ValueLogicalType::Object => W::Object,
        ValueLogicalType::Percentile => W::Percentile,
        ValueLogicalType::LargeInt => W::LargeInt,
        ValueLogicalType::Uuid => W::Uuid,
    }) as i32
}

pub(crate) fn decode_logical(logical: i32) -> Result<ValueLogicalType, TypeCodecError> {
    use wire::LogicalType as W;
    Ok(match W::try_from(logical) {
        Ok(W::Physical) => ValueLogicalType::Physical,
        Ok(W::Json) => ValueLogicalType::Json,
        Ok(W::Variant) => ValueLogicalType::Variant,
        Ok(W::Hll) => ValueLogicalType::Hll,
        Ok(W::Bitmap) => ValueLogicalType::Bitmap,
        Ok(W::Object) => ValueLogicalType::Object,
        Ok(W::Percentile) => ValueLogicalType::Percentile,
        Ok(W::LargeInt) => ValueLogicalType::LargeInt,
        Ok(W::Uuid) => ValueLogicalType::Uuid,
        _ => {
            return Err(TypeCodecError::InvalidShape(
                "unknown or unspecified logical type",
            ));
        }
    })
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

/// Admit the original bounded clone preflight before walking its immutable
/// source. This is a numerical ceiling, never synthetic completed work.
pub(crate) const fn value_type_clone_preflight_work_upper_bound() -> usize {
    16 + 8 * novarocks_type_contract::MAX_VALUE_TYPE_NODES
}

/// The sole clone author's known root contribution, before a lookup callback.
/// Shared FieldRef children have no independently owned request at this root.
pub(crate) fn value_type_clone_root_facts(
    value: &FunctionValueType,
) -> Result<ValueTypeCloneFacts, TypeCodecError> {
    value_type_clone_facts(1, value_type_clone_requests(&value.data_type, 0)?)
}

fn value_type_clone_requests(carrier: &DataType, requests: usize) -> Result<usize, TypeCodecError> {
    if matches!(carrier, DataType::Dictionary(_, _)) {
        requests.checked_add(2).ok_or(TypeCodecError::InvalidShape(
            "value type clone request count overflow",
        ))
    } else {
        Ok(requests)
    }
}

/// Fixed borrowed scratch covers only children which the sole clone author
/// actually clones. Dictionaries underneath shared FieldRef owners allocate
/// nothing here. This is numerical admission, not a host allocation grant.
pub(crate) fn preflight_value_type_clone(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueTypeCloneFacts, TypeCodecError> {
    preflight_value_type_clone_admitted(value, &mut |_, _| Ok(()), work)
}

/// The same clone grammar exposes each captured request prefix before the
/// next completed-work checkpoint. Admission owns no scope or observation.
pub(crate) fn preflight_value_type_clone_admitted<E: From<TypeCodecError>>(
    value: &FunctionValueType,
    admit: &mut impl FnMut(ValueTypeCloneFacts, &mut CompileCheckpoints<'_>) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueTypeCloneFacts, E> {
    use novarocks_type_contract::{MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES};
    let mut pending = [None; MAX_VALUE_TYPE_DEPTH + 1];
    pending[0] = Some((&value.data_type, 1usize));
    let mut length = 1usize;
    let mut nodes = 0usize;
    let mut requests = 0usize;
    while length != 0 {
        length -= 1;
        let (carrier, depth) = pending[length].take().ok_or(TypeCodecError::InvalidShape(
            "value type clone scratch is empty",
        ))?;
        nodes = nodes.checked_add(1).ok_or(TypeCodecError::InvalidShape(
            "value type clone node count overflow",
        ))?;
        if nodes > MAX_VALUE_TYPE_NODES || depth > MAX_VALUE_TYPE_DEPTH {
            work.step().map_err(TypeCodecError::from)?;
            return Err(TypeCodecError::InvalidShape(
                "value type clone exceeds the checked carrier grammar",
            )
            .into());
        }
        requests = value_type_clone_requests(carrier, requests)?;
        if let DataType::Dictionary(key, item) = carrier {
            // Preserve the original ordinary-error checkpoint and its grammar.
            if length + 2 > pending.len() {
                work.step().map_err(TypeCodecError::from)?;
                return Err(TypeCodecError::InvalidShape(
                    "value type clone scratch capacity exceeded",
                )
                .into());
            }
            admit(value_type_clone_facts(nodes, requests)?, work)?;
            work.step().map_err(TypeCodecError::from)?;
            pending[length] = Some((item.as_ref(), depth + 1));
            pending[length + 1] = Some((key.as_ref(), depth + 1));
            length += 2;
            work.step().map_err(TypeCodecError::from)?;
        } else {
            admit(value_type_clone_facts(nodes, requests)?, work)?;
            work.step().map_err(TypeCodecError::from)?;
        }
    }
    let facts = value_type_clone_facts(nodes, requests)?;
    admit(facts, work)?;
    work.step().map_err(TypeCodecError::from)?;
    Ok(facts)
}

fn value_type_clone_facts(
    nodes: usize,
    requests: usize,
) -> Result<ValueTypeCloneFacts, TypeCodecError> {
    let bytes = std::alloc::Layout::array::<DataType>(requests)
        .map_err(|_| TypeCodecError::InvalidShape("value type clone layout is unrepresentable"))?
        .size();
    let bound = nodes.checked_mul(8).and_then(|n| n.checked_add(16)).ok_or(
        TypeCodecError::InvalidShape("value type clone work overflow"),
    )?;
    Ok(ValueTypeCloneFacts {
        requests,
        bytes,
        work: bound,
    })
}

/// Preserve every value-root flag through the same carrier clone author.
/// The composing caller must first admit preflight_value_type_clone's facts.
pub(crate) fn clone_value_type_observed(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, TypeCodecError> {
    Ok(FunctionValueType {
        data_type: clone_carrier(&value.data_type, work)?,
        nullable: value.nullable,
        logical_type: value.logical_type,
    })
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
    validate_value_type_structure_observed(ty, |visit| validate_type_visit(visit, work))
}

/// The same carrier and Field author on the sole shared grammar. The caller
/// admits initialization and captured source work before any later observation;
/// the fixed scratch has no heap request, control scope or namespace authority.
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
        [Option<(&DataType, usize)>; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
    >();
    admit_scratch(layout)?;
    let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    novarocks_type_contract::validate_value_type_structure_with_scratch_observed(
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
