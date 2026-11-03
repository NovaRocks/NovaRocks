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

//! Provider-authored source facts, independent of execution value semantics.
//!
//! Callers own every incoming allocation and supply its original creator
//! lifetime. These wrappers never retain a Query or provider service and never
//! infer field IDs, defaults or logical types from generic attributes.

use crate::{ConnectorError, ConnectorErrorKind, MAX_CONNECTOR_RECIPE_COLUMNS};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{
    FunctionValueType, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, ValueTypeStorageLifetime,
    ValueTypeVisit, validate_arrow_layout_structure_with_scratch_observed,
};
use std::{alloc::Layout, collections::HashMap, sync::Arc};

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn extent() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "source schema construction extent exceeds address space",
    )
}
fn arc_layout<T>() -> Result<Layout, ConnectorError> {
    novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
        .map_err(|_| extent())
}

/// One original creator's source Arrow shape. Names, child order, NULL flags,
/// offsets and physical encoding structure belong to this namespace. Semantic
/// type IDs and complete execution bindings belong to a different namespace.
#[derive(Clone, Debug)]
pub struct ConnectorSourceSchemaShape {
    schema: Arc<Schema>,
    _storage: ValueTypeStorageLifetime,
}
impl ConnectorSourceSchemaShape {
    /// The caller has already covered the supplied Schema and each Field.
    /// This validation adds no schema allocation or metadata interpretation.
    pub fn try_new(
        schema: Arc<Schema>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_observed(schema, storage, |_| Ok::<_, ConnectorError>(()))
    }
    pub fn try_new_observed<E: From<ConnectorError>>(
        schema: Arc<Schema>,
        storage: ValueTypeStorageLifetime,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        validate_empty_metadata_shape(&schema, &mut work)?;
        Ok(Self {
            schema,
            _storage: storage,
        })
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    /// Borrow the exact source occurrence while its actual schema owner lives.
    /// This path lookup allocates no Field view or storage holder. Source
    /// paths include Map entries and preserve original Arrow child order.
    pub fn field_at(&self, path: &[u32]) -> Option<&Field> {
        field_at(&self.schema, path).map(Arc::as_ref)
    }
}
impl PartialEq for ConnectorSourceSchemaShape {
    fn eq(&self, other: &Self) -> bool {
        novarocks_type_contract::arrow_schemas_exact(&self.schema, &other.schema)
    }
}
impl Eq for ConnectorSourceSchemaShape {}

/// Provider-validated, canonical JSON serialization of an initial default.
/// The contract neither parses an annotation nor invents a default encoding.
#[derive(Clone, Debug)]
pub struct ConnectorSourceInitialDefault {
    json: Arc<str>,
    _storage: ValueTypeStorageLifetime,
}
impl ConnectorSourceInitialDefault {
    pub fn json(json: Arc<str>, storage: ValueTypeStorageLifetime) -> Self {
        Self {
            json,
            _storage: storage,
        }
    }
    pub fn as_json(&self) -> &str {
        &self.json
    }
}
impl PartialEq for ConnectorSourceInitialDefault {
    fn eq(&self, other: &Self) -> bool {
        self.json == other.json
    }
}
impl Eq for ConnectorSourceInitialDefault {}

#[derive(Debug)]
struct SourceFieldFactsStorage {
    path: Box<[u32]>,
    field_id: Option<i32>,
    initial_default: Option<ConnectorSourceInitialDefault>,
    attributes: HashMap<String, String>,
    // Opaque inspection work bound from the actual source invoice. This is
    // neither an allocator request nor a semantic/source identity field.
    attribute_lookup_work_upper_bound: Option<usize>,
    _storage: ValueTypeStorageLifetime,
}
/// Explicit source identity/default/attributes at one physical occurrence.
#[derive(Clone, Debug)]
pub struct ConnectorSourceFieldFacts(Arc<SourceFieldFactsStorage>);
impl ConnectorSourceFieldFacts {
    /// Incoming path/attributes/default backing is already covered by storage.
    /// Identified keys are supplied by the actual provider author; checking
    /// their absence does not recognize or parse any generic metadata key.
    pub fn try_new(
        path: Box<[u32]>,
        field_id: Option<i32>,
        initial_default: Option<ConnectorSourceInitialDefault>,
        attributes: HashMap<String, String>,
        identified_attribute_keys: &[&str],
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_core(
            path,
            field_id,
            initial_default,
            attributes,
            identified_attribute_keys,
            storage,
            None,
            |_| Ok::<_, ConnectorError>(()),
            |_| Ok(()),
        )
    }
    pub fn try_new_observed<E: From<ConnectorError>>(
        path: Box<[u32]>,
        field_id: Option<i32>,
        initial_default: Option<ConnectorSourceInitialDefault>,
        attributes: HashMap<String, String>,
        identified_attribute_keys: &[&str],
        storage: ValueTypeStorageLifetime,
        source_retained_bytes: usize,
        request: impl FnMut(Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::try_new_core(
            path,
            field_id,
            initial_default,
            attributes,
            identified_attribute_keys,
            storage,
            Some(source_retained_bytes),
            request,
            work,
        )
    }
    fn try_new_core<E: From<ConnectorError>>(
        path: Box<[u32]>,
        field_id: Option<i32>,
        initial_default: Option<ConnectorSourceInitialDefault>,
        attributes: HashMap<String, String>,
        identified_attribute_keys: &[&str],
        storage: ValueTypeStorageLifetime,
        source_retained_bytes: Option<usize>,
        mut request: impl FnMut(Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        work(1)?;
        if path.is_empty() || path.len() > MAX_VALUE_TYPE_DEPTH + 1 {
            return Err(E::from(invalid(
                "source field path has an invalid occurrence depth",
            )));
        }
        let attribute_lookup_work_upper_bound = if let Some(source) = source_retained_bytes {
            let lookup = novarocks_type_contract::owned_resources::hashmap::source_iterator_work_upper_bound(source, attributes.len())
                .map_err(|_| E::from(extent()))?;
            work(
                lookup
                    .checked_mul(identified_attribute_keys.len())
                    .ok_or_else(|| E::from(extent()))?,
            )?;
            Some(lookup)
        } else {
            None
        };
        for key in identified_attribute_keys {
            // Account actual key spelling before the closed lookup. No table
            // iteration or source-capacity estimate is hidden in this check.
            work(key.len().checked_add(1).ok_or_else(|| E::from(extent()))?)?;
            if attributes.contains_key(*key) {
                return Err(E::from(invalid(
                    "identified source fact is duplicated in generic attributes",
                )));
            }
        }
        request(arc_layout::<SourceFieldFactsStorage>().map_err(E::from)?)?;
        work(1)?;
        Ok(Self(Arc::new(SourceFieldFactsStorage {
            path,
            field_id,
            initial_default,
            attributes,
            attribute_lookup_work_upper_bound,
            _storage: storage,
        })))
    }
    pub fn storage_allocation_layout() -> Result<Layout, ConnectorError> {
        arc_layout::<SourceFieldFactsStorage>()
    }
    pub fn field_path(&self) -> &[u32] {
        &self.0.path
    }
    pub fn field_id(&self) -> Option<i32> {
        self.0.field_id
    }
    pub fn initial_default(&self) -> Option<&ConnectorSourceInitialDefault> {
        self.0.initial_default.as_ref()
    }
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.0.attributes
    }
    pub(crate) fn attribute_work_upper_bound(&self) -> Option<usize> {
        self.0.attribute_lookup_work_upper_bound
    }
    /// Read a borrowed attribute with the creating caller's real source-work
    /// bound. Empty maps return through an actual O(1) length check, without a
    /// hash/probe or any claim that retained capacity is zero.
    pub fn attribute_observed<E: From<ConnectorError>>(
        &self,
        key: &str,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Option<&str>, E> {
        work(1)?;
        if self.0.attributes.is_empty() {
            return Ok(None);
        }
        let bound = self.0.attribute_lookup_work_upper_bound.ok_or_else(|| {
            E::from(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "source attributes have no declared inspection work bound",
            ))
        })?;
        work(bound)?;
        work(key.len().checked_add(1).ok_or_else(|| E::from(extent()))?)?;
        Ok(self.0.attributes.get(key).map(String::as_str))
    }
}
impl PartialEq for ConnectorSourceFieldFacts {
    fn eq(&self, other: &Self) -> bool {
        self.0.path == other.0.path
            && self.0.field_id == other.0.field_id
            && self.0.initial_default == other.0.initial_default
            && self.0.attributes == other.0.attributes
    }
}
impl Eq for ConnectorSourceFieldFacts {}

#[derive(Debug)]
struct SourceSchemaFactsStorage {
    shape: ConnectorSourceSchemaShape,
    schema_attributes: HashMap<String, String>,
    field_facts: Box<[ConnectorSourceFieldFacts]>,
    attribute_iteration_work_upper_bound: Option<usize>,
    _storage: ValueTypeStorageLifetime,
}
/// Source facts use their own shape/occurrence namespace, never execution IDs.
#[derive(Clone, Debug)]
pub struct ConnectorSourceSchemaFacts(Arc<SourceSchemaFactsStorage>);
impl ConnectorSourceSchemaFacts {
    /// Sparse field facts must be in strict path order. A repeated source field
    /// ID at different projected root occurrences remains legal.
    pub fn try_new(
        shape: ConnectorSourceSchemaShape,
        schema_attributes: HashMap<String, String>,
        field_facts: Box<[ConnectorSourceFieldFacts]>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_observed(
            shape,
            schema_attributes,
            field_facts,
            storage,
            |_| Ok::<_, ConnectorError>(()),
            |_| Ok(()),
        )
    }
    pub fn try_new_observed<E: From<ConnectorError>>(
        shape: ConnectorSourceSchemaShape,
        schema_attributes: HashMap<String, String>,
        field_facts: Box<[ConnectorSourceFieldFacts]>,
        storage: ValueTypeStorageLifetime,
        request: impl FnMut(Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::try_new_core(
            shape,
            schema_attributes,
            field_facts,
            storage,
            None,
            request,
            work,
        )
    }
    /// Record the original schema-attribute source invoice as opaque iteration
    /// work provenance only. It is not an allocator Layout or capacity grant.
    pub fn try_new_observed_with_source_invoice<E: From<ConnectorError>>(
        shape: ConnectorSourceSchemaShape,
        schema_attributes: HashMap<String, String>,
        field_facts: Box<[ConnectorSourceFieldFacts]>,
        storage: ValueTypeStorageLifetime,
        source_retained_bytes: usize,
        request: impl FnMut(Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::try_new_core(
            shape,
            schema_attributes,
            field_facts,
            storage,
            Some(source_retained_bytes),
            request,
            work,
        )
    }
    fn try_new_core<E: From<ConnectorError>>(
        shape: ConnectorSourceSchemaShape,
        schema_attributes: HashMap<String, String>,
        field_facts: Box<[ConnectorSourceFieldFacts]>,
        storage: ValueTypeStorageLifetime,
        source_retained_bytes: Option<usize>,
        mut request: impl FnMut(Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        let attribute_iteration_work_upper_bound = source_retained_bytes
            .map(|source| {
                novarocks_type_contract::owned_resources::hashmap::source_iterator_work_upper_bound(
                    source,
                    schema_attributes.len(),
                )
                .map_err(|_| E::from(extent()))
            })
            .transpose()?;
        let mut previous: Option<&[u32]> = None;
        for fact in field_facts.iter() {
            work(1)?;
            let path = fact.field_path();
            if let Some(previous) = previous {
                let mut order = std::cmp::Ordering::Equal;
                for (a, b) in previous.iter().zip(path) {
                    work(1)?;
                    order = a.cmp(b);
                    if order != std::cmp::Ordering::Equal {
                        break;
                    }
                }
                if order == std::cmp::Ordering::Equal {
                    order = previous.len().cmp(&path.len());
                }
                if order != std::cmp::Ordering::Less {
                    return Err(E::from(invalid(
                        "source field facts have duplicate or unordered occurrence paths",
                    )));
                }
            }
            // Resolve without copying an Arrow tree or allocating a field view.
            if field_at_observed(shape.schema(), path, &mut work)?.is_none() {
                return Err(E::from(invalid(
                    "source field fact names an absent physical occurrence",
                )));
            }
            previous = Some(path);
        }
        request(arc_layout::<SourceSchemaFactsStorage>().map_err(E::from)?)?;
        work(1)?;
        Ok(Self(Arc::new(SourceSchemaFactsStorage {
            shape,
            schema_attributes,
            field_facts,
            attribute_iteration_work_upper_bound,
            _storage: storage,
        })))
    }
    pub fn storage_allocation_layout() -> Result<Layout, ConnectorError> {
        arc_layout::<SourceSchemaFactsStorage>()
    }
    pub fn shape(&self) -> &ConnectorSourceSchemaShape {
        &self.0.shape
    }
    pub fn schema_attributes(&self) -> &HashMap<String, String> {
        &self.0.schema_attributes
    }
    pub(crate) fn attribute_work_upper_bound(&self) -> Option<usize> {
        self.0.attribute_iteration_work_upper_bound
    }

    pub fn field_facts(&self) -> &[ConnectorSourceFieldFacts] {
        &self.0.field_facts
    }
    pub fn field_fact(&self, path: &[u32]) -> Option<&ConnectorSourceFieldFacts> {
        self.field_facts()
            .binary_search_by(|fact| fact.field_path().cmp(path))
            .ok()
            .map(|index| &self.field_facts()[index])
    }
    /// Inspect the sorted occurrence table without allocating or concealing
    /// binary-search comparisons from the caller's original control.
    pub fn field_fact_observed<E>(
        &self,
        path: &[u32],
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Option<&ConnectorSourceFieldFacts>, E> {
        let mut low = 0;
        let mut high = self.field_facts().len();
        while low < high {
            let middle = low + (high - low) / 2;
            let candidate = &self.field_facts()[middle];
            // One bounded comparison per component, including the length
            // decision. Immutable construction already checked every path.
            work(path.len().min(candidate.field_path().len()) + 1)?;
            match candidate.field_path().cmp(path) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(candidate)),
            }
        }
        Ok(None)
    }
}
impl PartialEq for ConnectorSourceSchemaFacts {
    fn eq(&self, other: &Self) -> bool {
        self.0.shape == other.0.shape
            && self.0.schema_attributes == other.0.schema_attributes
            && self.0.field_facts == other.0.field_facts
    }
}
impl Eq for ConnectorSourceSchemaFacts {}

/// Complete value bindings paired with their explicit metadata-free execution
/// projection. No source shape or root-only label list authors these values.
#[derive(Clone, Debug)]
pub struct ConnectorReadExecutionSchema {
    schema: Arc<Schema>,
    bindings: Arc<[FunctionValueType]>,
    _storage: ValueTypeStorageLifetime,
}
impl ConnectorReadExecutionSchema {
    /// The caller owns incoming Schema/Field and Box backing before this call.
    pub fn try_new(
        schema: Arc<Schema>,
        bindings: Box<[FunctionValueType]>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_observed(
            schema,
            bindings,
            storage,
            |_| Ok::<_, ConnectorError>(()),
            |_| Ok(()),
        )
    }
    pub fn try_new_observed<E: From<ConnectorError>>(
        schema: Arc<Schema>,
        bindings: Box<[FunctionValueType]>,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        validate_empty_metadata_shape(&schema, &mut work)?;
        work(1)?;
        if schema.fields().len() != bindings.len() {
            return Err(E::from(invalid(
                "execution read schema differs from its complete binding count",
            )));
        }
        for (field, binding) in schema.fields().iter().zip(bindings.iter()) {
            work(1)?;
            if field.is_nullable() != binding.nullable()
                || !binding
                    .typed_binding()
                    .layout()
                    .matches_projection_observed::<ProjectionFailure<E>>(
                        field.data_type(),
                        |units| work(units).map_err(ProjectionFailure::Caller),
                    )
                    .map_err(|cause| match cause {
                        ProjectionFailure::Caller(cause) => cause,
                        ProjectionFailure::Source => E::from(invalid(
                            "execution read field has an invalid static projection",
                        )),
                    })?
            {
                return Err(E::from(invalid(
                    "execution read field differs from complete binding NULL/layout contract",
                )));
            }
        }
        let payload =
            Layout::array::<FunctionValueType>(bindings.len()).map_err(|_| E::from(extent()))?;
        let layout = novarocks_type_contract::owned_resources::layout::arc_layout(payload)
            .map_err(|_| E::from(extent()))?;
        request(layout)?;
        work(
            bindings
                .len()
                .checked_mul(std::mem::size_of::<FunctionValueType>())
                .ok_or_else(|| E::from(extent()))?,
        )?;
        Ok(Self {
            schema,
            bindings: bindings.into(),
            _storage: storage,
        })
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn bindings(&self) -> &[FunctionValueType] {
        &self.bindings
    }
    pub fn binding(&self, ordinal: usize) -> Option<&FunctionValueType> {
        self.bindings.get(ordinal)
    }
    pub(crate) fn schema_ref(&self) -> &Arc<Schema> {
        &self.schema
    }
}
impl PartialEq for ConnectorReadExecutionSchema {
    fn eq(&self, other: &Self) -> bool {
        self.bindings == other.bindings
            && novarocks_type_contract::arrow_schemas_exact(&self.schema, &other.schema)
    }
}
impl Eq for ConnectorReadExecutionSchema {}

fn validate_empty_metadata_shape<E: From<ConnectorError>>(
    schema: &Schema,
    work: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<(), E> {
    work(1)?;
    if !schema.metadata().is_empty() || schema.fields().len() > MAX_CONNECTOR_RECIPE_COLUMNS {
        return Err(E::from(invalid(
            "source/execution shape has attributes or exceeds the frozen column count",
        )));
    }
    // Admit the exact fixed stack initialization before visiting any source.
    work(novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound())?;
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    for field in schema.fields() {
        work(1)?;
        if !field.metadata().is_empty() {
            return Err(E::from(invalid(
                "source/execution shape field retains generic attributes",
            )));
        }
        validate_arrow_layout_structure_with_scratch_observed::<ShapeFailure<E>>(
            field.data_type(),
            &mut scratch,
            |visit| {
                work(1).map_err(ShapeFailure::Caller)?;
                if let ValueTypeVisit::Field(field) = visit
                    && !field.metadata().is_empty()
                {
                    return Err(ShapeFailure::Source(invalid(
                        "source/execution nested shape field retains generic attributes",
                    )));
                }
                Ok(())
            },
        )
        .map_err(|cause| match cause {
            ShapeFailure::Caller(cause) => cause,
            ShapeFailure::Source(cause) => E::from(cause),
        })?;
    }
    Ok(())
}
enum ProjectionFailure<E> {
    Caller(E),
    Source,
}
impl<E> From<novarocks_type_contract::ExecutionValueTypeError> for ProjectionFailure<E> {
    fn from(_: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Source
    }
}
enum ShapeFailure<E> {
    Caller(E),
    Source(ConnectorError),
}
impl<E> From<novarocks_type_contract::ValueTypeError> for ShapeFailure<E> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        let kind = match error {
            novarocks_type_contract::ValueTypeError::TooDeep
            | novarocks_type_contract::ValueTypeError::TooManyNodes => {
                ConnectorErrorKind::ResourceExhausted
            }
            _ => ConnectorErrorKind::InvalidRequest,
        };
        Self::Source(ConnectorError::new(kind, error.to_string()))
    }
}
fn field_at<'a>(schema: &'a Schema, path: &[u32]) -> Option<&'a Arc<Field>> {
    let (&root, rest) = path.split_first()?;
    let mut field = schema.fields().get(root as usize)?;
    for &ordinal in rest {
        field = child_field(field.data_type(), ordinal)?;
    }
    Some(field)
}
fn field_at_observed<'a, E: From<ConnectorError>>(
    schema: &'a Schema,
    path: &[u32],
    work: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<Option<&'a Arc<Field>>, E> {
    work(1)?;
    let Some((&root, rest)) = path.split_first() else {
        return Ok(None);
    };
    let Some(mut field) = schema.fields().get(root as usize) else {
        return Ok(None);
    };
    for &ordinal in rest {
        work(1)?;
        let Some(child) = child_field_observed(field.data_type(), ordinal, work)? else {
            return Ok(None);
        };
        field = child;
    }
    Ok(Some(field))
}
fn child_field(data_type: &DataType, ordinal: u32) -> Option<&Arc<Field>> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => (ordinal == 0).then_some(field),
        DataType::Struct(fields) => fields.get(ordinal as usize),
        DataType::Union(fields, _) => fields.iter().nth(ordinal as usize).map(|(_, field)| field),
        DataType::RunEndEncoded(ends, values) => match ordinal {
            0 => Some(ends),
            1 => Some(values),
            _ => None,
        },
        // Dictionary values have no Arrow Field of their own. Embedded named
        // children keep their physical occurrence path beneath the wrapper.
        DataType::Dictionary(_, value) => child_field(value, ordinal),
        _ => None,
    }
}

fn child_field_observed<'a, E>(
    data_type: &'a DataType,
    ordinal: u32,
    work: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<Option<&'a Arc<Field>>, E> {
    work(1)?;
    match data_type {
        DataType::Union(fields, _) => {
            for (at, (_, field)) in fields.iter().enumerate() {
                work(1)?;
                if at == ordinal as usize {
                    return Ok(Some(field));
                }
            }
            Ok(None)
        }
        DataType::Dictionary(_, value) => child_field_observed(value, ordinal, work),
        _ => Ok(child_field(data_type, ordinal)),
    }
}

#[cfg(test)]
mod tests;
