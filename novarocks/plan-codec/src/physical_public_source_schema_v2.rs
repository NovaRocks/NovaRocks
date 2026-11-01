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

//! Independent provider source facts. Neither attributes nor source Arrow
//! shapes author an execution type. Every definition, including unused ones,
//! passes the same receiving constructor and occurrence laws.
use crate::{
    allocation_exit_v2::reserve_exit,
    arrow_metadata_v2::{compare_keys, copy_string, ordered_key},
    host_projection_v2::ProjectionFailure,
    physical_node_v2::{Model, NodeProjectionLimits},
    physical_schema_v2::{DecodedSchemas, SchemaCodecError},
    physical_type_v2::TypeCodecError,
};
use novarocks_connector_contract::{
    ConnectorError, ConnectorSourceFieldFacts, ConnectorSourceInitialDefault,
    ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape,
};
use novarocks_proto_models::{arrow_type::ArrowFieldMetadataEntry, physical_package_v2 as wire};
use novarocks_type_contract::owned_resources::{
    hashmap,
    layout::{LayoutResourceError, arc_layout},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueSchemaCreation, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, collections::HashMap, fmt, mem::size_of, sync::Arc};

#[derive(Debug)]
pub enum PublicSourceSchemaCodecError {
    Control(CompileControlError),
    Schema(SchemaCodecError),
    Type(TypeCodecError),
    Connector(ConnectorError),
    Layout(LayoutResourceError),
    InvalidShape(&'static str),
    SourceModel(&'static str),
}
impl fmt::Display for PublicSourceSchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Schema(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Connector(e) => e.fmt(f),
            Self::Layout(e) => write!(f, "source facts construction: {e:?}"),
            Self::InvalidShape(e) | Self::SourceModel(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for PublicSourceSchemaCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Schema(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Connector(source) => Some(source),
            Self::Layout(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for PublicSourceSchemaCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<SchemaCodecError> for PublicSourceSchemaCodecError {
    fn from(e: SchemaCodecError) -> Self {
        match e {
            SchemaCodecError::Control(e) => Self::Control(e),
            e => Self::Schema(e),
        }
    }
}
impl From<TypeCodecError> for PublicSourceSchemaCodecError {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(e) => Self::Control(e),
            e => Self::Type(e),
        }
    }
}
impl From<ConnectorError> for PublicSourceSchemaCodecError {
    fn from(e: ConnectorError) -> Self {
        Self::Connector(e)
    }
}
impl From<LayoutResourceError> for PublicSourceSchemaCodecError {
    fn from(e: LayoutResourceError) -> Self {
        Self::Layout(e)
    }
}
type Error = PublicSourceSchemaCodecError;
impl<H> From<CompileControlError> for ProjectionFailure<Error, H> {
    fn from(e: CompileControlError) -> Self {
        Self::Codec(e.into())
    }
}
impl<H> From<ConnectorError> for ProjectionFailure<Error, H> {
    fn from(e: ConnectorError) -> Self {
        Self::Codec(e.into())
    }
}
impl<H> From<SchemaCodecError> for ProjectionFailure<Error, H> {
    fn from(e: SchemaCodecError) -> Self {
        Self::Codec(e.into())
    }
}
impl<H> From<TypeCodecError> for ProjectionFailure<Error, H> {
    fn from(e: TypeCodecError) -> Self {
        Self::Codec(e.into())
    }
}
impl<H> From<LayoutResourceError> for ProjectionFailure<Error, H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Codec(e.into())
    }
}
fn invalid(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
fn layout<T>(count: usize) -> Result<Layout, Error> {
    Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn work_units(work: &mut CompileCheckpoints<'_>, count: usize) -> Result<(), CompileControlError> {
    for _ in 0..count {
        work.step()?;
    }
    Ok(())
}
fn reserve<T>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    layout::<T>(count)?;
    work.flush()?;
    let mut values = Vec::new();
    let result = values.try_reserve_exact(count);
    reserve_exit::<Error>(result, work)?;
    Ok(values)
}

#[derive(Clone, Copy)]
pub struct SourceSchemaFactsSource<'a> {
    pub id: u32,
    pub source_shape_schema_id: u32,
    pub facts: &'a ConnectorSourceSchemaFacts,
}

// Heap-sort controls every actual byte comparison and movement. HashMap's
// private bucket iteration is opaque, with checks before and after each next;
// these callbacks never pretend that buckets or source capacity are known.
fn encode_attributes(
    values: &HashMap<String, String>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ArrowFieldMetadataEntry>, Error> {
    let mut ordered = reserve(values.len(), work)?;
    work.flush()?;
    let mut iter = values.iter();
    loop {
        work.flush()?;
        let next = iter.next();
        work.flush()?;
        let Some((k, v)) = next else { break };
        ordered.push((k.as_str(), v.as_str()));
        work.step()?;
    }
    fn sift(
        values: &mut [(&str, &str)],
        mut root: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        loop {
            let left = root
                .checked_mul(2)
                .and_then(|v| v.checked_add(1))
                .ok_or(CompileControlError::ResourceExhausted)?;
            if left >= values.len() {
                return Ok(());
            }
            let mut next = left;
            if left + 1 < values.len()
                && compare_keys(values[left].0, values[left + 1].0, work)?
                    == std::cmp::Ordering::Less
            {
                next = left + 1;
            }
            if compare_keys(values[root].0, values[next].0, work)? != std::cmp::Ordering::Less {
                return Ok(());
            }
            values.swap(root, next);
            work.step()?;
            root = next;
        }
    }
    for root in (0..ordered.len() / 2).rev() {
        sift(&mut ordered, root, work)?;
    }
    for end in (1..ordered.len()).rev() {
        ordered.swap(0, end);
        work.step()?;
        sift(&mut ordered[..end], 0, work)?;
    }
    let mut output = reserve(ordered.len(), work)?;
    for (k, v) in ordered {
        output.push(ArrowFieldMetadataEntry {
            key: copy_string(k, work)?,
            value: copy_string(v, work)?,
        });
        work.step()?;
    }
    Ok(output)
}

/// Pure FE preparation uses original control and real copy operations. It
/// allocates no quota/account authority and invents no source invoice.
pub fn encode_public_source_schemas_pure_in(
    sources: &[SourceSchemaFactsSource<'_>],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::PublicSourceSchemaFactsDefinition>, Error> {
    let mut output = reserve(sources.len(), work)?;
    for source in sources {
        let mut fields = reserve(source.facts.field_facts().len(), work)?;
        for fact in source.facts.field_facts() {
            let mut path = reserve(fact.field_path().len(), work)?;
            for &ordinal in fact.field_path() {
                path.push(ordinal);
                work.step()?;
            }
            let initial_default = fact
                .initial_default()
                .map(|default| {
                    copy_string(default.as_json(), work)
                        .map(|json| wire::SourceInitialDefault { json: Some(json) })
                })
                .transpose()?;
            fields.push(wire::PublicSourceFieldFactsDefinition {
                field_path: path,
                field_id: fact.field_id(),
                initial_default,
                attributes: encode_attributes(fact.attributes(), work)?,
            });
            work.step()?;
        }
        output.push(wire::PublicSourceSchemaFactsDefinition {
            id: Some(source.id),
            source_shape_schema_id: Some(source.source_shape_schema_id),
            schema_attributes: encode_attributes(source.facts.schema_attributes(), work)?,
            field_facts: fields,
        });
        work.step()?;
    }
    Ok(output)
}

/// Backing of THIS emitted Vec and its actual public Vec/String allocations.
/// Original sources, opaque map capacity and allocator usable size are absent.
pub fn encoded_public_source_schemas_backing_bytes(
    defs: &Vec<wire::PublicSourceSchemaFactsDefinition>,
) -> Result<usize, Error> {
    fn attrs(entries: &Vec<ArrowFieldMetadataEntry>) -> Result<usize, Error> {
        let mut n = layout::<ArrowFieldMetadataEntry>(entries.capacity())?.size();
        for entry in entries {
            n = add(n, add(entry.key.capacity(), entry.value.capacity())?)?;
        }
        Ok(n)
    }
    let mut bytes = add(
        size_of::<Vec<wire::PublicSourceSchemaFactsDefinition>>(),
        layout::<wire::PublicSourceSchemaFactsDefinition>(defs.capacity())?.size(),
    )?;
    for def in defs {
        bytes = add(bytes, attrs(&def.schema_attributes)?)?;
        bytes = add(
            bytes,
            layout::<wire::PublicSourceFieldFactsDefinition>(def.field_facts.capacity())?.size(),
        )?;
        for fact in &def.field_facts {
            bytes = add(bytes, layout::<u32>(fact.field_path.capacity())?.size())?;
            bytes = add(bytes, attrs(&fact.attributes)?)?;
            if let Some(default) = &fact.initial_default
                && let Some(json) = &default.json
            {
                bytes = add(bytes, json.capacity())?;
            }
        }
    }
    Ok(bytes)
}

struct Entry {
    id: u32,
    shape_id: u32,
    facts: ConnectorSourceSchemaFacts,
}
pub struct DecodedPublicSourceSchemas {
    entries: Vec<Entry>,
    storage: ValueTypeStorageLifetime,
}
impl DecodedPublicSourceSchemas {
    fn find(&self, id: u32, work: &mut CompileCheckpoints<'_>) -> Result<Option<&Entry>, Error> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            let mid = low + (high - low) / 2;
            let entry = &self.entries[mid];
            let order = entry.id.cmp(&id);
            work.step()?;
            match order {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }
    pub fn lookup_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorSourceSchemaFacts>, Error> {
        Ok(self.find(id, work)?.map(|entry| &entry.facts))
    }
    pub fn source_shape_schema_id_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<u32>, Error> {
        Ok(self.find(id, work)?.map(|entry| entry.shape_id))
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
}

fn map_layout_error(error: hashmap::HashMapResourceError) -> Error {
    match error {
        hashmap::HashMapResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
        hashmap::HashMapResourceError::SourceModel(s) => Error::SourceModel(s),
    }
}
fn request_vec<T, H>(
    count: usize,
    creation: &mut Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, ProjectionFailure<Error, H>> {
    let l = layout::<T>(count)?;
    if l.size() != 0
        && let Some(creation) = creation.as_deref_mut()
    {
        work.flush()?;
        creation
            .request_layout(l)
            .map_err(ProjectionFailure::Host)?;
    }
    Ok(reserve(count, work)?)
}
fn decode_attributes<H>(
    source: &[ArrowFieldMetadataEntry],
    creation: &mut Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(HashMap<String, String>, usize), ProjectionFailure<Error, H>> {
    let mut previous = None;
    for entry in source {
        if !ordered_key(previous, &entry.key, work)? {
            return Err(invalid("source attributes are duplicated or unordered").into());
        }
        previous = Some(entry.key.as_str());
    }
    let map =
        hashmap::fresh_table_layout::<String, String>(source.len()).map_err(map_layout_error)?;
    if let Some(l) = map.layout
        && let Some(creation) = creation.as_deref_mut()
    {
        work.flush()?;
        creation
            .observe_opaque_upper_bound(l.size())
            .map_err(ProjectionFailure::Host)?;
    }
    work.flush()?;
    let mut values = HashMap::new();
    let result = values.try_reserve(source.len());
    reserve_exit::<Error>(result, work)?;
    for entry in source {
        for text in [&entry.key, &entry.value] {
            let l = layout::<u8>(text.len())?;
            if l.size() != 0
                && let Some(creation) = creation.as_deref_mut()
            {
                work.flush()?;
                creation
                    .request_layout(l)
                    .map_err(ProjectionFailure::Host)?;
            }
        }
        let key = copy_string(&entry.key, work)?;
        let value = copy_string(&entry.value, work)?;
        work.flush()?;
        let previous = values.insert(key, value);
        work.step()?;
        work.flush()?;
        if previous.is_some() {
            return Err(invalid("prepared source attribute identity changed").into());
        }
    }
    let mut owned = add(
        size_of::<HashMap<String, String>>(),
        map.layout.map_or(0, |layout| layout.size()),
    )?;
    for entry in source {
        owned = add(owned, add(entry.key.len(), entry.value.len())?)?;
        work.step()?;
    }
    Ok((values, owned))
}

/// Decode the entire independent source namespace, including unused entries.
/// The neutral wire author owns NO recognized attribute keys: [] is explicit,
/// not provider policy. IDs/defaults come only from their dedicated DTO fields;
/// provider-specific read/private owners keep their original semantic laws.
pub fn decode_public_source_schemas_with_creator_in<H>(
    defs: &[wire::PublicSourceSchemaFactsDefinition],
    schemas: &DecodedSchemas<'_, '_>,
    original_source_bytes: usize,
    limits: NodeProjectionLimits,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedPublicSourceSchemas, ProjectionFailure<Error, H>> {
    decode_core::<H, crate::value_type_table::resources::NoFactory>(
        defs,
        schemas,
        original_source_bytes,
        limits,
        Some(creation),
        None,
        work,
    )
}
/// Explicit pure receiver on the same grammar/constructor body. It retains
/// source creators but installs no memory authority or fabricated grant port.
pub fn decode_public_source_schemas_pure_in(
    defs: &[wire::PublicSourceSchemaFactsDefinition],
    schemas: &DecodedSchemas<'_, '_>,
    original_source_bytes: usize,
    limits: NodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedPublicSourceSchemas, Error> {
    decode_core::<std::convert::Infallible, crate::value_type_table::resources::NoFactory>(
        defs,
        schemas,
        original_source_bytes,
        limits,
        None,
        None,
        work,
    )
    .map_err(ProjectionFailure::without_host)
}
/// The same full source namespace decoder with actual independent entities.
/// A selected shape retains only that shape's original Field/backing loans.
pub fn decode_public_source_schemas_with_factory_in<H, F: ValueSchemaCreationFactory<H>>(
    defs: &[wire::PublicSourceSchemaFactsDefinition],
    schemas: &DecodedSchemas<'_, '_>,
    original_source_bytes: usize,
    limits: NodeProjectionLimits,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedPublicSourceSchemas, ProjectionFailure<Error, H>> {
    decode_core(
        defs,
        schemas,
        original_source_bytes,
        limits,
        None,
        Some(factory),
        work,
    )
}
fn begin<'a, H, F: ValueSchemaCreationFactory<H>>(
    factory: Option<&'a F>,
    model: &mut Model,
    source: usize,
    count: usize,
    limits: NodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<F::Scope<'a>>, ProjectionFailure<Error, H>> {
    let Some(factory) = factory else {
        return Ok(None);
    };
    work.flush()?;
    factory
        .begin_scope_observed(&mut |layout| {
            model
                .layout_request(layout, 1)
                .map_err(SchemaCodecError::from)?;
            model
                .numerical_facts(source, count, limits)
                .map_err(SchemaCodecError::from)?;
            Ok::<_, ProjectionFailure<Error, H>>(())
        })
        .map(Some)
        .map_err(|error| match error {
            ValueSchemaScopeBeginFailure::Observer(error) => error,
            ValueSchemaScopeBeginFailure::Creator(error) => ProjectionFailure::Host(error),
        })
}
fn decode_core<H, F: ValueSchemaCreationFactory<H>>(
    defs: &[wire::PublicSourceSchemaFactsDefinition],
    schemas: &DecodedSchemas<'_, '_>,
    original_source_bytes: usize,
    limits: NodeProjectionLimits,
    mut creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedPublicSourceSchemas, ProjectionFailure<Error, H>> {
    let mut model = Model {
        items: defs.len(),
        ..Model::default()
    };
    model
        .request::<Entry>(defs.len(), 1)
        .map_err(crate::physical_schema_v2::SchemaCodecError::from)?;
    fn attrs_model(
        model: &mut Model,
        attrs: &[ArrowFieldMetadataEntry],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        model.items = add(model.items, attrs.len())?;
        let map =
            hashmap::fresh_table_layout::<String, String>(attrs.len()).map_err(map_layout_error)?;
        if let Some(l) = map.layout {
            model.layout_request(l, 1).map_err(SchemaCodecError::from)?;
        }
        for entry in attrs {
            model
                .request::<u8>(entry.key.len(), 1)
                .map_err(SchemaCodecError::from)?;
            model
                .request::<u8>(entry.value.len(), 1)
                .map_err(SchemaCodecError::from)?;
            work.step()?;
        }
        Ok(())
    }
    for def in defs {
        def.id
            .ok_or_else(|| invalid("public source schema facts ID is absent"))?;
        def.source_shape_schema_id
            .ok_or_else(|| invalid("public source shape schema ID is absent"))?;
        model.items = add(model.items, def.field_facts.len())?;
        model
            .layout_request(arc_layout(Layout::new::<arrow::datatypes::Schema>())?, 1)
            .map_err(SchemaCodecError::from)?;
        model
            .layout_request(ConnectorSourceSchemaFacts::storage_allocation_layout()?, 1)
            .map_err(SchemaCodecError::from)?;
        model
            .request::<ConnectorSourceFieldFacts>(def.field_facts.len(), 2)
            .map_err(SchemaCodecError::from)?;
        attrs_model(&mut model, &def.schema_attributes, work)?;
        for fact in &def.field_facts {
            if fact.field_path.is_empty()
                || fact.field_path.len() > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 1
            {
                return Err(invalid("source field path has an invalid occurrence depth").into());
            }
            model.items = add(model.items, fact.field_path.len())?;
            model.refs = add(model.refs, fact.field_path.len())?;
            model
                .request::<u32>(fact.field_path.len(), 2)
                .map_err(SchemaCodecError::from)?;
            model
                .layout_request(ConnectorSourceFieldFacts::storage_allocation_layout()?, 1)
                .map_err(SchemaCodecError::from)?;
            if let Some(default) = &fact.initial_default {
                let json = default
                    .json
                    .as_ref()
                    .ok_or_else(|| invalid("source initial default JSON is absent"))?;
                model
                    .layout_request(arc_layout(layout::<u8>(json.len())?)?, 1)
                    .map_err(SchemaCodecError::from)?;
            }
            attrs_model(&mut model, &fact.attributes, work)?;
            work.step()?;
        }
        work.step()?;
    }
    model
        .facts(original_source_bytes, defs.len(), limits, work)
        .map_err(SchemaCodecError::from)?;
    let mut container = begin(
        factory,
        &mut model,
        original_source_bytes,
        defs.len(),
        limits,
        work,
    )?;
    let storage = if let Some(container) = container.as_ref() {
        container.storage_lifetime()
    } else if let Some(creation) = creation.as_deref_mut() {
        creation
            .storage_lifetime()
            .retaining_observed(&schemas.storage_lifetime(), |l| {
                creation.request_layout(l).map_err(ProjectionFailure::Host)
            })?
    } else {
        schemas.storage_lifetime()
    };
    let mut container_port = if let Some(container) = container.as_mut() {
        Some(container as &mut dyn ValueSchemaCreationScope<H>)
    } else {
        creation
            .as_mut()
            .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
    };
    let mut entries = request_vec::<Entry, H>(defs.len(), &mut container_port, work)?;
    for def in defs {
        let id = def
            .id
            .ok_or_else(|| invalid("public source schema facts ID is absent"))?;
        let shape_id = def
            .source_shape_schema_id
            .ok_or_else(|| invalid("public source shape schema ID is absent"))?;
        let source = schemas
            .materialized_schema_observed(shape_id, work)?
            .ok_or_else(|| invalid("public source shape schema reference is absent"))?;
        let roots = schemas
            .field_roots_observed(shape_id, work)?
            .ok_or_else(|| invalid("public source shape schema roots are absent"))?;
        for &root in roots {
            // The validated type graph owns the complete role proof, including
            // otherwise metadata-free descendants with semantic references.
            schemas
                .types()
                .validate_source_field_root_observed(root, work)?;
        }
        if source.schema().metadata().as_arc().is_some() {
            return Err(invalid("public source shape Schema retains annotation backing").into());
        }
        // The scope is created only after the original shape/role gates.
        // Schema::clone shares Fields; it cannot re-author a semantic binding.
        let mut shape_scope = begin(
            factory,
            &mut model,
            original_source_bytes,
            defs.len(),
            limits,
            work,
        )?;
        let shape_storage = if let Some(scope) = shape_scope.as_mut() {
            let original = source.storage_lifetime();
            scope
                .storage_lifetime()
                .retaining_observed(&original, |layout| {
                    model
                        .layout_request(layout, 1)
                        .map_err(SchemaCodecError::from)?;
                    model
                        .numerical_facts(original_source_bytes, defs.len(), limits)
                        .map_err(SchemaCodecError::from)?;
                    work.flush()?;
                    scope
                        .request_layout(layout)
                        .map_err(ProjectionFailure::Host)
                })?
        } else {
            storage.clone()
        };
        let shape_port = if let Some(scope) = shape_scope.as_mut() {
            Some(scope as &mut dyn ValueSchemaCreationScope<H>)
        } else {
            creation
                .as_mut()
                .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
        };
        if let Some(port) = shape_port {
            work.flush()?;
            port.request_layout(arc_layout(Layout::new::<arrow::datatypes::Schema>())?)
                .map_err(ProjectionFailure::Host)?;
        }
        work.flush()?;
        let shape_schema = Arc::new(source.schema().clone());
        work.flush()?;
        let shape =
            ConnectorSourceSchemaShape::try_new_observed(shape_schema, shape_storage, |units| {
                work_units(work, units).map_err(ProjectionFailure::<Error, H>::from)
            })?;
        let mut facts_scope = begin(
            factory,
            &mut model,
            original_source_bytes,
            defs.len(),
            limits,
            work,
        )?;
        let facts_storage = facts_scope
            .as_ref()
            .map(ValueSchemaCreationScope::<H>::storage_lifetime)
            .unwrap_or_else(|| storage.clone());
        let mut facts_port = if let Some(scope) = facts_scope.as_mut() {
            Some(scope as &mut dyn ValueSchemaCreationScope<H>)
        } else {
            creation
                .as_mut()
                .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
        };
        let (attrs, schema_attributes_invoice) =
            decode_attributes(&def.schema_attributes, &mut facts_port, work)?;
        let mut fields = request_vec::<ConnectorSourceFieldFacts, H>(
            def.field_facts.len(),
            &mut facts_port,
            work,
        )?;
        for fact in &def.field_facts {
            let mut field_scope = begin(
                factory,
                &mut model,
                original_source_bytes,
                defs.len(),
                limits,
                work,
            )?;
            let field_storage = field_scope
                .as_ref()
                .map(ValueSchemaCreationScope::<H>::storage_lifetime)
                .unwrap_or_else(|| storage.clone());
            let mut field_port = if let Some(scope) = field_scope.as_mut() {
                Some(scope as &mut dyn ValueSchemaCreationScope<H>)
            } else {
                creation
                    .as_mut()
                    .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
            };
            let mut path = request_vec::<u32, H>(fact.field_path.len(), &mut field_port, work)?;
            for &part in &fact.field_path {
                path.push(part);
                work.step()?;
            }
            if !path.is_empty()
                && let Some(creation) = field_port
            {
                creation
                    .observe_opaque_upper_bound(layout::<u32>(path.len())?.size())
                    .map_err(ProjectionFailure::Host)?;
            }
            work.flush()?;
            let path = path.into_boxed_slice();
            work.flush()?;
            let default = if let Some(default) = &fact.initial_default {
                let json = default
                    .json
                    .as_ref()
                    .ok_or_else(|| invalid("source initial default JSON is absent"))?;
                let mut default_scope = begin(
                    factory,
                    &mut model,
                    original_source_bytes,
                    defs.len(),
                    limits,
                    work,
                )?;
                let default_storage = default_scope
                    .as_ref()
                    .map(ValueSchemaCreationScope::<H>::storage_lifetime)
                    .unwrap_or_else(|| storage.clone());
                let default_port = if let Some(scope) = default_scope.as_mut() {
                    Some(scope as &mut dyn ValueSchemaCreationScope<H>)
                } else {
                    creation
                        .as_mut()
                        .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
                };
                if let Some(port) = default_port {
                    work.flush()?;
                    port.request_layout(arc_layout(layout::<u8>(json.len())?)?)
                        .map_err(ProjectionFailure::Host)?;
                }
                work.flush()?;
                let text: Arc<str> = Arc::from(json.as_str());
                work.flush()?;
                work_units(work, json.len())?;
                Some(ConnectorSourceInitialDefault::json(text, default_storage))
            } else {
                None
            };
            let mut field_port = if let Some(scope) = field_scope.as_mut() {
                Some(scope as &mut dyn ValueSchemaCreationScope<H>)
            } else {
                creation
                    .as_mut()
                    .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
            };
            let (attrs, attributes_invoice) =
                decode_attributes(&fact.attributes, &mut field_port, work)?;
            let field = ConnectorSourceFieldFacts::try_new_observed(
                path,
                fact.field_id,
                default,
                attrs,
                &[],
                field_storage.clone(),
                attributes_invoice,
                |l| {
                    if let Some(creation) = field_port.as_deref_mut() {
                        creation.request_layout(l).map_err(ProjectionFailure::Host)
                    } else {
                        Ok(())
                    }
                },
                |units| work_units(work, units).map_err(ProjectionFailure::<Error, H>::from),
            )?;
            fields.push(field);
            work.step()?;
        }
        let mut facts_port = if let Some(scope) = facts_scope.as_mut() {
            Some(scope as &mut dyn ValueSchemaCreationScope<H>)
        } else {
            creation
                .as_mut()
                .map(|port| &mut **port as &mut dyn ValueSchemaCreationScope<H>)
        };
        if !fields.is_empty()
            && let Some(creation) = facts_port.as_deref_mut()
        {
            creation
                .observe_opaque_upper_bound(
                    layout::<ConnectorSourceFieldFacts>(fields.len())?.size(),
                )
                .map_err(ProjectionFailure::Host)?;
        }
        work.flush()?;
        let fields = fields.into_boxed_slice();
        work.flush()?;
        let facts = ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
            shape,
            attrs,
            fields,
            facts_storage,
            schema_attributes_invoice,
            |l| {
                if let Some(creation) = facts_port.as_deref_mut() {
                    creation.request_layout(l).map_err(ProjectionFailure::Host)
                } else {
                    Ok(())
                }
            },
            |units| work_units(work, units).map_err(ProjectionFailure::<Error, H>::from),
        )?;
        entries.push(Entry {
            id,
            shape_id,
            facts,
        });
        work.step()?;
    }
    fn sift(
        values: &mut [Entry],
        mut root: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        loop {
            let left = root
                .checked_mul(2)
                .and_then(|v| v.checked_add(1))
                .ok_or(CompileControlError::ResourceExhausted)?;
            if left >= values.len() {
                return Ok(());
            }
            let mut next = left;
            if left + 1 < values.len() {
                let larger = values[left + 1].id > values[left].id;
                work.step()?;
                if larger {
                    next = left + 1;
                }
            }
            let larger = values[next].id > values[root].id;
            work.step()?;
            if !larger {
                return Ok(());
            }
            values.swap(root, next);
            work.step()?;
            root = next;
        }
    }
    for root in (0..entries.len() / 2).rev() {
        sift(&mut entries, root, work)?;
    }
    for end in (1..entries.len()).rev() {
        entries.swap(0, end);
        work.step()?;
        sift(&mut entries[..end], 0, work)?;
    }
    for pair in entries.windows(2) {
        let duplicate = pair[0].id == pair[1].id;
        work.step()?;
        if duplicate {
            return Err(invalid("public source schema facts ID is duplicated").into());
        }
    }
    model
        .numerical_facts(original_source_bytes, defs.len(), limits)
        .map_err(SchemaCodecError::from)?;
    Ok(DecodedPublicSourceSchemas { entries, storage })
}

#[cfg(test)]
#[path = "physical_public_source_schema_v2_tests.rs"]
mod tests;
