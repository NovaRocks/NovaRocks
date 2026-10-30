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

//! Complete Arrow Schema namespace representation. Field/type semantics and
//! Connector public read facts remain with their original owners. Numerical
//! request projections are not grants for opaque std/Arrow allocations.

use crate::host_projection_v2::ProjectionFailure;
use crate::{
    binding_index_v2::{BindingIndex, prepare_work_upper_bound},
    hashmap_resources_v2::HashMapResourceError,
    physical_binding_v2::BindingCodecError,
    physical_node_v2::{
        self as resources, Model, NodeCodecError, NodeProjectionFacts, NodeProjectionLimits,
    },
    physical_type_v2::{DecodedTypeTable, EncodedTypeTable, TypeCodecError},
};
use arrow::datatypes::{Field, Fields, Schema};
use novarocks_type_contract::owned_resources::metadata_materialization::{
    MaterializedFieldNamespace, MaterializedMetadataMap, MaterializedSchema,
};
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
mod creator;
mod scopes;
pub use creator::prepare_schemas_decode_with_creator_in;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
pub use scopes::decode_schemas_with_factory_in;
use std::{alloc::Layout, fmt, mem::size_of, sync::Arc};

#[derive(Debug)]
pub enum SchemaCodecError {
    Control(CompileControlError),
    Type(TypeCodecError),
    Binding(BindingCodecError),
    Node(NodeCodecError),
    SourceModel(&'static str),
    ConstructionResource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    InvalidShape(&'static str),
    SourceCustody(novarocks_constant_contract::ConstantError),
    SourceMetadata(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError),
}
impl fmt::Display for SchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceCustody(e) => e.fmt(f),
            Self::SourceMetadata(e) => write!(f, "schema original metadata: {e:?}"),
            Self::Control(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Node(e) => e.fmt(f),
            Self::SourceModel(s) | Self::InvalidShape(s) => f.write_str(s),
            Self::ConstructionResource(error) => write!(f, "source schema construction: {error:?}"),
        }
    }
}
impl std::error::Error for SchemaCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Node(source) => Some(source),
            Self::ConstructionResource(source) => Some(source),
            Self::SourceCustody(source) => Some(source),
            Self::SourceMetadata(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for SchemaCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<TypeCodecError> for SchemaCodecError {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(c) => Self::Control(c),
            e => Self::Type(e),
        }
    }
}
impl From<BindingCodecError> for SchemaCodecError {
    fn from(e: BindingCodecError) -> Self {
        match e {
            BindingCodecError::Control(c) => Self::Control(c),
            e => Self::Binding(e),
        }
    }
}
impl From<NodeCodecError> for SchemaCodecError {
    fn from(e: NodeCodecError) -> Self {
        match e {
            NodeCodecError::Control(c) => Self::Control(c),
            e => Self::Node(e),
        }
    }
}
impl From<HashMapResourceError> for SchemaCodecError {
    fn from(e: HashMapResourceError) -> Self {
        match e {
            HashMapResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
            HashMapResourceError::SourceModel(s) => Self::SourceModel(s),
        }
    }
}
type Error = SchemaCodecError;
type SchemaDefinitions = Box<[(u32, MaterializedSchema)]>;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    Ok(resources::add(a, b)?)
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    Ok(resources::mul(a, b)?)
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Ok(resources::bytes::<T>(n)?)
}
fn finish<T>(work: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn fields_layout(n: usize) -> Result<Layout, Error> {
    let payload =
        Layout::array::<Arc<Field>>(n).map_err(|_| CompileControlError::ResourceExhausted)?;
    // This sole port has checked Layout/arithmetic errors only, never a source
    // grammar diagnostic. Empty Fields still have a real Arc header request.
    crate::ipc_flat_stream_v2::reader_allocations::arc_layout(payload)
        .map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn shape_equal(
    equal: bool,
    work: &mut CompileCheckpoints<'_>,
    message: &'static str,
) -> Result<(), Error> {
    work.step()?;
    if equal { Ok(()) } else { Err(invalid(message)) }
}
#[derive(Clone, Copy)]
pub struct SchemaSource<'source> {
    pub id: u32,
    pub source: &'source Schema,
    pub field_ids: &'source [u32],
}

type Admission<'a> = dyn FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError> + 'a;

// Parent admission shares the original numerical author and precedes every
// completed gate observation. It creates neither a budget nor a scope.
fn admit_model(
    model: &Model,
    source: usize,
    fields: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let facts = model.numerical_facts(source, fields, limits)?;
    admit(&facts)?;
    Ok(facts)
}
fn admitted_facts(
    model: &Model,
    source: usize,
    fields: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    admit_model(model, source, fields, limits, admit)?;
    Ok(model.facts(source, fields, limits, work)?)
}

fn encode_preflight(
    sources: &[SchemaSource<'_>],
    types: &EncodedTypeTable<'_>,
    source: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut model = Model {
        items: sources.len(),
        ..Model::default()
    };
    model.request::<usize>(sources.len(), 1)?;
    model.request::<wire::SchemaDefinition>(sources.len(), 1)?;
    model.delegated_work = prepare_work_upper_bound(sources.len())?;
    admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
    let source_slice = bytes::<SchemaSource<'_>>(sources.len())?;
    let types_floor = add(
        size_of::<EncodedTypeTable<'_>>(),
        bytes::<(u32, Arc<Field>)>(types.source_counts().1)?,
    )?;
    resources::floor(source, source_slice.max(types_floor), work)?;
    let mut max_schema_floor = 0;
    for entry in sources {
        // Shape namespace cannot retain even empty spare annotation backing.
        // Source annotations are authored independently in public source facts.
        work.step()?;
        if entry.source.metadata().as_arc().is_some() {
            return Err(invalid("Schema shape retains retired annotation backing"));
        }
        let fields = entry.source.fields();
        model.refs = add(model.refs, entry.field_ids.len())?;
        model.items = add(model.items, entry.field_ids.len())?;
        model.request::<u32>(entry.field_ids.len(), 1)?;
        model.delegated_work = add(
            model.delegated_work,
            mul(entry.field_ids.len(), add(types.source_counts().1, 1)?)?,
        )?;
        admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
        shape_equal(
            fields.len() == entry.field_ids.len(),
            work,
            "Schema field occurrence count differs from original field IDs",
        )?;
        let known = add(
            size_of::<Schema>(),
            add(
                fields_layout(fields.len())?.size(),
                bytes::<u32>(entry.field_ids.len())?,
            )?,
        )?;
        max_schema_floor = max_schema_floor.max(known);
        resources::floor(
            source,
            source_slice.max(types_floor).max(max_schema_floor),
            work,
        )?;
        admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
        for (field, id) in fields.iter().zip(entry.field_ids) {
            let original = types.field_observed(*id, work)?;
            work.step()?;
            let same_source = original.is_some_and(|original| Arc::ptr_eq(field, original));
            shape_equal(
                same_source,
                work,
                "Schema field is not the original Field namespace source",
            )?;
        }
    }
    admitted_facts(&model, source, types.source_counts().1, limits, admit, work)
}
fn btree_lookup(n: usize) -> Result<usize, Error> {
    crate::btree_resources_v2::lookup_work_typed(n).map_err(|e| match e {
        crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
        crate::btree_resources_v2::BTreeResourceError::SourceModel(s) => Error::SourceModel(s),
    })
}
fn decode_preflight(
    definitions: &[wire::SchemaDefinition],
    types: &DecodedTypeTable,
    source: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Model, NodeProjectionFacts), Error> {
    let mut model = Model {
        items: definitions.len(),
        ..Model::default()
    };
    model.request::<usize>(definitions.len(), 1)?;
    model.request::<(u32, MaterializedSchema)>(definitions.len(), 2)?;
    model.delegated_work = prepare_work_upper_bound(definitions.len())?;
    admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
    let mut known = add(
        bytes::<wire::SchemaDefinition>(definitions.len())?,
        types.necessary_fields_retained_floor()?,
    )?;
    resources::floor(source, known, work)?;
    for entry in definitions {
        model.refs = add(model.refs, entry.field_ids.len())?;
        model.items = add(model.items, entry.field_ids.len())?;
        model.request::<Arc<Field>>(entry.field_ids.len(), 1)?;
        model.layout_request(fields_layout(entry.field_ids.len())?, 1)?;
        known = add(known, bytes::<u32>(entry.field_ids.capacity())?)?;
        model.delegated_work = add(
            model.delegated_work,
            mul(
                mul(entry.field_ids.len(), btree_lookup(types.field_count())?)?,
                2,
            )?,
        )?;
        resources::floor(source, known, work)?;
        admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
        for id in &entry.field_ids {
            let found = types.field_observed(*id, work)?.is_some();
            shape_equal(found, work, "Schema Field reference is unknown")?;
        }
    }

    let facts = admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
    Ok((model, facts))
}
fn encode_emit(
    sources: &[SchemaSource<'_>],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::SchemaDefinition>, Error> {
    let mut definitions = resources::reserve(sources.len(), work)?;
    for entry in sources {
        let mut fields = resources::reserve(entry.field_ids.len(), work)?;
        for id in entry.field_ids {
            fields.push(*id);
            work.step()?;
        }
        definitions.push(wire::SchemaDefinition {
            id: entry.id,
            field_ids: fields,
        });
        work.step()?;
    }
    Ok(definitions)
}
fn decode_emit<H>(
    emission: &mut impl scopes::SchemaEmission<H>,
    definitions: &[wire::SchemaDefinition],
    types: &DecodedTypeTable,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(SchemaDefinitions, usize), ProjectionFailure<Error, H>> {
    emission.output(definitions.len(), work)?;
    let mut output = resources::reserve(definitions.len(), work).map_err(Error::from)?;
    let mut retained = bytes::<(u32, MaterializedSchema)>(definitions.len())?;
    for entry in definitions {
        emission.schema(entry.field_ids.len(), work)?;
        let mut fields = resources::reserve(entry.field_ids.len(), work).map_err(Error::from)?;
        for id in &entry.field_ids {
            let original = types.field_backing_observed(*id, work)?;
            work.step()?;
            let original =
                original.ok_or_else(|| invalid("prepared Schema Field reference changed"))?;
            work.flush()?;
            emission.field(original, work)?;
            fields.push(Arc::clone(original.field()));
            work.step()?;
            work.flush()?;
        }
        retained = add(retained, fields_layout(fields.len())?.size())?;
        work.flush()?;
        let storage = emission.fields(fields.len(), work)?;
        let fields = Fields::from(fields);
        work.step()?;
        work.flush()?;
        // Metadata-free shape retains the original empty-map provenance only.
        // Schema annotations belong to their separately authored facts owner.
        let metadata = MaterializedMetadataMap::new_for_original_reserve();
        work.step()?;
        let schema = metadata.into_schema(fields).with_storage_lifetime(storage);
        output.push((entry.id, schema));
        work.step()?;
    }
    emission.boxed(&output, work)?;
    Ok((
        resources::boxed(output, work).map_err(Error::from)?,
        retained,
    ))
}
/// Retains the actual borrowed source schema/Field/control owners.
pub struct PreparedSchemasEncode<'loan, 'source, 'control> {
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
}
pub struct EncodedSchemas<'loan, 'source, 'control> {
    definitions: Vec<wire::SchemaDefinition>,
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
}
impl EncodedSchemas<'_, '_, '_> {
    pub fn as_wire(&self) -> &[wire::SchemaDefinition] {
        &self.definitions
    }
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn original_control(&self) -> &dyn PureCompileControl {
        self.control
    }
    pub fn types(&self) -> &EncodedTypeTable<'_> {
        self.types
    }
    pub fn source_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Schema>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let at = self.index.find(id, |at| self.sources[at].id, work)?;
        Ok(at.map(|at| self.sources[at].source))
    }
    pub fn execution_binding_observed(
        &self,
        id: u32,
        ordinal: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&novarocks_type_contract::FunctionValueType>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let Some(at) = self.index.find(id, |at| self.sources[at].id, work)? else {
            return Ok(None);
        };
        let Some(field) = self.sources[at].field_ids.get(ordinal) else {
            return Ok(None);
        };
        self.types
            .field_execution_type_observed(*field, work)
            .map_err(Into::into)
    }
    pub fn into_wire(self) -> Vec<wire::SchemaDefinition> {
        self.definitions
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(
            self.source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.index.backing_bytes()?,
                    bytes::<wire::SchemaDefinition>(self.definitions.capacity())?,
                )?,
            )?,
        )
    }
}
impl<'loan, 'source, 'control> PreparedSchemasEncode<'loan, 'source, 'control> {
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = self.emit_observed_in(&mut |_| Ok(()), &mut work);
        finish(work, result)
    }
    /// Uses the caller's original scope. The caller owns the ordinary footer.
    pub fn emit_observed_in(
        self,
        admit: &mut Admission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
        if !std::ptr::eq(work.control(), self.control) {
            return Err(invalid("Schema emission has a different original control"));
        }
        admit(&self.facts)?;
        let definitions = encode_emit(self.sources, work)?;
        Ok(EncodedSchemas {
            definitions,
            sources: self.sources,
            types: self.types,
            control: self.control,
            source_bytes: self.source_bytes,
            index: self.index,
            facts: self.facts,
        })
    }
}
pub fn prepare_schemas_encode<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedSchemasEncode<'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare_schemas_encode_observed_in(
        sources,
        types,
        source_retained_bytes,
        limits,
        &mut |_| Ok(()),
        &mut work,
    );
    finish(work, result)
}
/// Prepares the original schema body in an existing caller-owned scope. The
/// synchronous parent sees cumulative contributions, with source B once.
pub fn prepare_schemas_encode_observed_in<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedSchemasEncode<'loan, 'source, 'control>, Error> {
    let facts = encode_preflight(sources, types, source_retained_bytes, limits, admit, work)?;
    let index = BindingIndex::prepare(sources.len(), |at| sources[at].id, work)?;
    Ok(PreparedSchemasEncode {
        sources,
        types,
        control: work.control(),
        source_bytes: source_retained_bytes,
        index,
        facts,
    })
}
pub struct PreparedSchemasDecode<'loan, 'control> {
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
    storage: ValueTypeStorageLifetime,
}
pub struct DecodedSchemas<'loan, 'control> {
    wire_definitions: &'loan [wire::SchemaDefinition],
    definitions: SchemaDefinitions,
    retained_bytes: usize,
    types: &'loan DecodedTypeTable,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
    storage: ValueTypeStorageLifetime,
    metadata: Option<MaterializedFieldNamespace>,
    _index_storage: ValueTypeStorageLifetime,
}
impl DecodedSchemas<'_, '_> {
    pub fn definitions(&self) -> &[(u32, MaterializedSchema)] {
        &self.definitions
    }
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn original_control(&self) -> &dyn PureCompileControl {
        self.control
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.types
    }
    /// Borrow the actual source role roots from this validated occurrence.
    /// The type namespace remains the sole field-graph validation authority.
    pub fn field_roots_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&[u32]>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let at = self
            .index
            .find(id, |at| self.wire_definitions[at].id, work)?;
        Ok(at.map(|at| self.wire_definitions[at].field_ids.as_slice()))
    }
    pub fn execution_binding_observed(
        &self,
        id: u32,
        ordinal: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&novarocks_type_contract::FunctionValueType>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let Some(at) = self
            .index
            .find(id, |at| self.wire_definitions[at].id, work)?
        else {
            return Ok(None);
        };
        let Some(field) = self.wire_definitions[at].field_ids.get(ordinal) else {
            return Ok(None);
        };
        self.types
            .field_execution_type_observed(*field, work)
            .map_err(Into::into)
    }
    pub fn schema_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Schema>, Error> {
        self.materialized_schema_observed(id, work)
            .map(|owner| owner.map(MaterializedSchema::schema))
    }
    /// Borrow the actual schema and its original root-map creation provenance.
    /// Format roles stay with the explicit consumer; metadata never authors an
    /// execution binding at this source-only projection boundary.
    pub fn materialized_schema_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&MaterializedSchema>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let at = self.index.find(id, |at| self.definitions[at].0, work)?;
        Ok(at.map(|at| &self.definitions[at].1))
    }
    pub fn retained_output_floor(&self) -> Result<usize, Error> {
        add(
            size_of::<Self>(),
            add(self.index.backing_bytes()?, self.retained_bytes)?,
        )
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(self.source_bytes, self.retained_output_floor()?)
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    /// Move the actual source schema owners with their creator and source holds.
    pub fn into_owned_definitions(self) -> OwnedSchemaDefinitions {
        OwnedSchemaDefinitions {
            definitions: self.definitions,
            metadata: self.metadata,
            storage: self.storage,
        }
    }
}
impl<'loan, 'control> PreparedSchemasDecode<'loan, 'control> {
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<DecodedSchemas<'loan, 'control>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = self.emit_observed_in(&mut |_| Ok(()), &mut work);
        finish(work, result)
    }
    /// Materializes in the same caller scope; no entry, reset or footer.
    pub fn emit_observed_in(
        self,
        admit: &mut Admission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<DecodedSchemas<'loan, 'control>, Error> {
        if !std::ptr::eq(work.control(), self.control) {
            return Err(invalid("Schema emission has a different original control"));
        }
        admit(&self.facts)?;
        let (definitions, retained_bytes) = decode_emit(
            &mut scopes::UnscopedEmission(self.storage.clone()),
            self.definitions,
            self.types,
            work,
        )
        .map_err(ProjectionFailure::without_host)?;
        Ok(DecodedSchemas {
            wire_definitions: self.definitions,
            definitions,
            retained_bytes,
            types: self.types,
            control: self.control,
            source_bytes: self.source_bytes,
            index: self.index,
            facts: self.facts,
            storage: self.storage.clone(),
            metadata: self.types.metadata_namespace().cloned(),
            _index_storage: self.storage.clone(),
        })
    }
}
pub fn prepare_schemas_decode<'loan, 'control>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedSchemasDecode<'loan, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = prepare_schemas_decode_observed_in(
        definitions,
        types,
        source_retained_bytes,
        limits,
        &mut |_| Ok(()),
        &mut work,
    );
    finish(work, result)
}
/// Retains the exact raw definitions and decoded Field namespace loans.
pub fn prepare_schemas_decode_observed_in<'loan, 'control>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedSchemasDecode<'loan, 'control>, Error> {
    prepare_schemas_decode_core::<std::convert::Infallible>(
        definitions,
        types,
        source_retained_bytes,
        limits,
        admit,
        None,
        work,
    )
    .map_err(ProjectionFailure::without_host)
}
fn prepare_schemas_decode_core<'loan, 'control, H>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    creation: Option<&mut ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedSchemasDecode<'loan, 'control>, ProjectionFailure<Error, H>> {
    let (_, facts) = decode_preflight(
        definitions,
        types,
        source_retained_bytes,
        limits,
        admit,
        work,
    )?;
    let storage = if let Some(creation) = creation {
        creator::observe_schema_construction(definitions, creation, work)?;
        creation
            .storage_lifetime()
            .retaining_observed(&types.storage_lifetime(), |layout| {
                creation
                    .request_layout(layout)
                    .map_err(ProjectionFailure::Host)
            })?
    } else {
        // Pure new wrappers still retain the actual original Field creators.
        types.storage_lifetime()
    };
    let index = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    Ok(PreparedSchemasDecode {
        definitions,
        types,
        control: work.control(),
        source_bytes: source_retained_bytes,
        index,
        facts,
        storage,
    })
}
#[cfg(test)]
#[path = "physical_schema_v2/tests.rs"]
mod tests;

/// Independent source schema owners, original metadata provenance and finite
/// creator responsibility. It retains no complete decoded type table/control.
pub struct OwnedSchemaDefinitions {
    definitions: SchemaDefinitions,
    metadata: Option<MaterializedFieldNamespace>,
    storage: ValueTypeStorageLifetime,
}
impl OwnedSchemaDefinitions {
    pub fn definitions(&self) -> &[(u32, MaterializedSchema)] {
        &self.definitions
    }
    pub fn source_field_metadata_namespace(&self) -> Option<&MaterializedFieldNamespace> {
        self.metadata.as_ref()
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
}
impl<H> From<CompileControlError> for ProjectionFailure<SchemaCodecError, H> {
    fn from(error: CompileControlError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<TypeCodecError> for ProjectionFailure<SchemaCodecError, H> {
    fn from(error: TypeCodecError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<BindingCodecError> for ProjectionFailure<SchemaCodecError, H> {
    fn from(error: BindingCodecError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for ProjectionFailure<SchemaCodecError, H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Codec(SchemaCodecError::ConstructionResource(error))
    }
}
