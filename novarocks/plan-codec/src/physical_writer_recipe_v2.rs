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

//! Complete writer recipe representation through the original borrowed-input
//! constructor. Namespace/source loans are exact; this is not provider sealing,
//! package certification, a control scope or a host memory grant.

use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::{BindingIndex, lookup_work_upper_bound, prepare_work_upper_bound},
    btree_resources_v2,
    host_projection_v2::ProjectionFailure,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
    },
    physical_provider_binding_v2::{
        DecodedProviderBindings, EncodedProviderBindings, ProviderBindingCodecError,
    },
    physical_type_v2::{DecodedTypeTable, EncodedTypeTable, TypeCodecError},
};
use novarocks_connector_contract::{
    ConnectorError, ConnectorWriteFieldBinding, ConnectorWriteFieldRef, ConnectorWriteFieldToken,
    ConnectorWriteInputRef, ConnectorWriteInputShape, ConnectorWriteLayoutRecipeDraft,
    ConnectorWriteRecipeDraft, PureProviderCompileError, WriterOwnedCreationFailure,
    WriterOwnedResourceFacts,
};
use novarocks_physical_plan::NodeId;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, fmt, mem::size_of};

type E = WriterRecipeCodecError;
#[derive(Debug)]
pub enum WriterRecipeCodecError {
    Control(CompileControlError),
    ConstructionResource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Provider(ConnectorError),
    Binding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Type(TypeCodecError),
    Index(BindingCodecError),
    SourceModel(&'static str),
    InvalidShape(&'static str),
}
impl fmt::Display for E {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::ConstructionResource(e) => write!(f, "writer construction layout: {e:?}"),
            Self::Provider(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::SourceModel(s) | Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for WriterRecipeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::ConstructionResource(source) => Some(source),
            Self::Provider(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Index(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for E {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ProviderBindingCodecError> for E {
    fn from(e: ProviderBindingCodecError) -> Self {
        match e {
            ProviderBindingCodecError::Control(c) => Self::Control(c),
            e => Self::Binding(e),
        }
    }
}
impl From<ConnectorPayloadCodecError> for E {
    fn from(e: ConnectorPayloadCodecError) -> Self {
        match e {
            ConnectorPayloadCodecError::Control(c) => Self::Control(c),
            e => Self::Payload(e),
        }
    }
}
impl From<TypeCodecError> for E {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(c) => Self::Control(c),
            e => Self::Type(e),
        }
    }
}
impl From<BindingCodecError> for E {
    fn from(e: BindingCodecError) -> Self {
        match e {
            BindingCodecError::Control(c) => Self::Control(c),
            e => Self::Index(e),
        }
    }
}
impl From<PureProviderCompileError<ConnectorError>> for E {
    fn from(e: PureProviderCompileError<ConnectorError>) -> Self {
        match e {
            PureProviderCompileError::Control(c) => Self::Control(c),
            PureProviderCompileError::Provider(e) => Self::Provider(e),
        }
    }
}

type Failure<H> = ProjectionFailure<E, H>;
macro_rules! decode_failure {
    ($($error:ty),*) => {$(impl<H> From<$error> for Failure<H> {
        fn from(error:$error)->Self {Self::Codec(E::from(error))}
    })*};
}
decode_failure!(
    CompileControlError,
    ProviderBindingCodecError,
    ConnectorPayloadCodecError,
    TypeCodecError,
    BindingCodecError,
    PureProviderCompileError<ConnectorError>
);
fn created_failure<H>(error: WriterOwnedCreationFailure<H>) -> Failure<H> {
    match error {
        WriterOwnedCreationFailure::Creator(e) => ProjectionFailure::Host(e),
        WriterOwnedCreationFailure::Contract(e) => ProjectionFailure::Codec(e.into()),
        WriterOwnedCreationFailure::Resource(e) => {
            ProjectionFailure::Codec(E::ConstructionResource(e))
        }
    }
}
/// Temporary namespace decode backing only. Individual recipes/Field clones
/// retain their own factory scopes; they never retain this decode scratch.
pub(crate) struct CreatedWriterRecipes {
    entries: Vec<(NodeId, ConnectorWriteRecipeDraft)>,
    facts: WriterRecipeProjectionFacts,
    temporary_storage: ValueTypeStorageLifetime,
}
impl CreatedWriterRecipes {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<(NodeId, ConnectorWriteRecipeDraft)>,
        WriterRecipeProjectionFacts,
        ValueTypeStorageLifetime,
    ) {
        (self.entries, self.facts, self.temporary_storage)
    }
}

fn add(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_mul(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn array<T>(n: usize) -> Result<Layout, E> {
    Layout::array::<T>(n).map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn observed<T>(outcome: Result<T, E>, work: &mut CompileCheckpoints<'_>) -> Result<T, E> {
    if matches!(&outcome, Err(E::Control(_))) {
        return outcome;
    }
    work.step()?;
    outcome
}
fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, E> {
    work.flush()?;
    let mut v = Vec::new();
    let result = v.try_reserve_exact(n);
    if result.is_ok() {
        work.step()?;
    }
    reserve_exit::<E>(result, work)?;
    Ok(v)
}

#[derive(Clone, Copy, Debug)]
pub struct WriterRecipeProjectionLimits {
    pub max_recipes: usize,
    pub max_input_fields: usize,
    pub max_token_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriterRecipeProjectionFacts {
    pub recipe_count: usize,
    pub input_field_count: usize,
    pub token_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
pub struct WriterRecipeSource<'source> {
    pub node: NodeId,
    pub recipe: &'source ConnectorWriteRecipeDraft,
    pub field_ids: &'source [u32],
}
pub struct WriterRecipeEncodeContext<'loan, 'source, 'control> {
    pub bindings: &'loan EncodedProviderBindings<'source, 'control>,
    pub payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    pub types: &'loan EncodedTypeTable<'source>,
}
pub struct WriterRecipeDecodeContext<'loan, 'wire, 'control> {
    pub bindings: &'loan DecodedProviderBindings<'wire, 'control>,
    pub payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    pub types: &'loan DecodedTypeTable,
}
struct Model {
    source: usize,
    facts: WriterRecipeProjectionFacts,
    completed: WriterOwnedResourceFacts,
}
impl Model {
    fn new<T>(recipes: usize, source: usize) -> Result<Self, E> {
        let mut m = Self {
            source,
            facts: WriterRecipeProjectionFacts {
                recipe_count: recipes,
                cumulative_work_upper_bound: add(256, mul(source, 16)?)?,
                ..Default::default()
            },
            completed: Default::default(),
        };
        m.request(array::<usize>(recipes)?, 1)?;
        m.request(array::<T>(recipes)?, 1)?;
        m.work(prepare_work_upper_bound(recipes)?)?;
        Ok(m)
    }
    fn work(&mut self, n: usize) -> Result<(), CompileControlError> {
        self.facts.cumulative_work_upper_bound = add(self.facts.cumulative_work_upper_bound, n)?;
        Ok(())
    }
    fn request(&mut self, layout: Layout, copies: usize) -> Result<(), E> {
        if layout.size() == 0 || copies == 0 {
            return Ok(());
        }
        let bytes = mul(layout.size(), copies)?;
        self.facts.allocation_requests_upper_bound =
            add(self.facts.allocation_requests_upper_bound, copies)?;
        self.facts.allocation_request_bytes_upper_bound =
            add(self.facts.allocation_request_bytes_upper_bound, bytes)?;
        self.work(add(mul(bytes, 4)?, mul(copies, 128)?)?)?;
        Ok(())
    }
    fn fields(&mut self, n: usize) -> Result<(), E> {
        self.facts.input_field_count = add(self.facts.input_field_count, n)?;
        self.facts.token_bytes = add(self.facts.token_bytes, mul(n, 32)?)?;
        self.work(mul(n, 256)?)?;
        Ok(())
    }
    fn floor(&self, n: usize) -> Result<(), E> {
        if n > self.source {
            Err(E::InvalidShape(
                "writer recipe source invoice is understated",
            ))
        } else {
            Ok(())
        }
    }
    fn combined(
        &self,
        current: &WriterOwnedResourceFacts,
        limits: Option<WriterRecipeProjectionLimits>,
    ) -> Result<WriterRecipeProjectionFacts, CompileControlError> {
        let mut f = self.facts;
        f.allocation_requests_upper_bound = add(
            f.allocation_requests_upper_bound,
            add(
                self.completed.allocation_requests,
                current.allocation_requests,
            )?,
        )?;
        f.allocation_request_bytes_upper_bound = add(
            f.allocation_request_bytes_upper_bound,
            add(self.completed.requested_bytes, current.requested_bytes)?,
        )?;
        f.cumulative_work_upper_bound = add(
            f.cumulative_work_upper_bound,
            add(self.completed.work_units, current.work_units)?,
        )?;
        // The constructor's source invoice already contains the original B
        // and this leaf's live temporary FieldRef view. Add requests only.
        f.coexisting_source_and_request_bytes_upper_bound =
            add(self.source, f.allocation_request_bytes_upper_bound)?;
        if limits.is_some_and(|limits| {
            f.recipe_count > limits.max_recipes
                || f.input_field_count > limits.max_input_fields
                || f.token_bytes > limits.max_token_bytes
                || f.allocation_requests_upper_bound > limits.max_allocation_requests
                || f.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
                || f.coexisting_source_and_request_bytes_upper_bound
                    > limits.max_coexisting_source_and_request_bytes
                || f.cumulative_work_upper_bound > limits.max_work
        }) {
            return Err(CompileControlError::ResourceExhausted);
        }
        Ok(f)
    }
    fn gate(
        &self,
        limits: Option<WriterRecipeProjectionLimits>,
        admit: &mut impl FnMut(&WriterRecipeProjectionFacts) -> Result<(), CompileControlError>,
    ) -> Result<(), E> {
        admit(&self.combined(&Default::default(), limits)?)?;
        Ok(())
    }
    fn complete(&mut self, current: WriterOwnedResourceFacts) -> Result<(), CompileControlError> {
        self.completed.allocation_requests = add(
            self.completed.allocation_requests,
            current.allocation_requests,
        )?;
        self.completed.requested_bytes =
            add(self.completed.requested_bytes, current.requested_bytes)?;
        self.completed.work_units = add(self.completed.work_units, current.work_units)?;
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    Data,
    RowLineage,
    PositionDelete,
    DeletionVector,
    EqualityDelete,
}
fn source_roles(input: &ConnectorWriteInputShape) -> (Shape, [&[ConnectorWriteFieldBinding]; 2]) {
    match input {
        ConnectorWriteInputShape::Data { fields } => (Shape::Data, [fields, &[]]),
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => (Shape::RowLineage, [data_fields, row_identity_fields]),
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => (
            Shape::PositionDelete,
            [identity_fields, partition_source_fields],
        ),
        ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => (
            Shape::DeletionVector,
            [identity_fields, partition_source_fields],
        ),
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
            (Shape::EqualityDelete, [equality_fields, &[]])
        }
    }
}
fn raw_roles(
    input: &wire::ConnectorWriteInputShape,
) -> Result<(Shape, [&[wire::ConnectorWriteFieldBinding]; 2], usize), E> {
    use wire::connector_write_input_shape::Kind;
    Ok(
        match input
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("writer input kind is absent"))?
        {
            Kind::Data(v) => (Shape::Data, [&v.fields, &[]], v.fields.capacity()),
            Kind::RowLineage(v) => (
                Shape::RowLineage,
                [&v.data_fields, &v.row_identity_fields],
                add(v.data_fields.capacity(), v.row_identity_fields.capacity())?,
            ),
            Kind::PositionDelete(v) => (
                Shape::PositionDelete,
                [&v.identity_fields, &v.partition_source_fields],
                add(
                    v.identity_fields.capacity(),
                    v.partition_source_fields.capacity(),
                )?,
            ),
            Kind::DeletionVector(v) => (
                Shape::DeletionVector,
                [&v.identity_fields, &v.partition_source_fields],
                add(
                    v.identity_fields.capacity(),
                    v.partition_source_fields.capacity(),
                )?,
            ),
            Kind::EqualityDelete(v) => (
                Shape::EqualityDelete,
                [&v.equality_fields, &[]],
                v.equality_fields.capacity(),
            ),
        },
    )
}
fn wire_shape(
    shape: Shape,
    first: Vec<wire::ConnectorWriteFieldBinding>,
    second: Vec<wire::ConnectorWriteFieldBinding>,
) -> wire::ConnectorWriteInputShape {
    use wire::connector_write_input_shape::Kind;
    let kind = match shape {
        Shape::Data => Kind::Data(wire::ConnectorWriteDataInput { fields: first }),
        Shape::RowLineage => Kind::RowLineage(wire::ConnectorWriteRowLineageInput {
            data_fields: first,
            row_identity_fields: second,
        }),
        Shape::PositionDelete => Kind::PositionDelete(wire::ConnectorWritePositionDeleteInput {
            identity_fields: first,
            partition_source_fields: second,
        }),
        Shape::DeletionVector => Kind::DeletionVector(wire::ConnectorWriteDeletionVectorInput {
            identity_fields: first,
            partition_source_fields: second,
        }),
        Shape::EqualityDelete => Kind::EqualityDelete(wire::ConnectorWriteEqualityDeleteInput {
            equality_fields: first,
        }),
    };
    wire::ConnectorWriteInputShape { kind: Some(kind) }
}
fn borrowed_shape<'a>(
    shape: Shape,
    first: &'a [ConnectorWriteFieldRef<'a>],
    second: &'a [ConnectorWriteFieldRef<'a>],
) -> ConnectorWriteInputRef<'a> {
    match shape {
        Shape::Data => ConnectorWriteInputRef::Data { fields: first },
        Shape::RowLineage => ConnectorWriteInputRef::RowLineage {
            data_fields: first,
            row_identity_fields: second,
        },
        Shape::PositionDelete => ConnectorWriteInputRef::PositionDelete {
            identity_fields: first,
            partition_source_fields: second,
        },
        Shape::DeletionVector => ConnectorWriteInputRef::DeletionVector {
            identity_fields: first,
            partition_source_fields: second,
        },
        Shape::EqualityDelete => ConnectorWriteInputRef::EqualityDelete {
            equality_fields: first,
        },
    }
}
use crate::physical_type_v2::OriginalValueTypeSourceIndex as StorageSourceIndex;

fn encode_group(
    fields: &[ConnectorWriteFieldBinding],
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    storage_sources: Option<&StorageSourceIndex<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::ConnectorWriteFieldBinding>, E> {
    let mut output = reserve(fields.len(), work)?;
    for (field, id) in fields.iter().zip(ids) {
        let actual = types.source_field_source_observed(*id, work)?;
        observed(
            if actual.is_some_and(|actual| std::ptr::eq(actual, field.field())) {
                Ok(())
            } else {
                Err(E::InvalidShape(
                    "writer Field ID belongs to another source owner",
                ))
            },
            work,
        )?;
        let value_id = types
            .source_field_value_type_id_observed(*id, work)?
            .ok_or(E::InvalidShape(
                "writer source lacks its explicit complete value binding",
            ))?;
        let value = types.value_type_observed(value_id, work)?;
        observed(
            if value.is_some_and(|v| {
                field
                    .execution_type()
                    .is_some_and(|expected| std::ptr::eq(v, expected))
            }) {
                Ok(())
            } else {
                Err(E::InvalidShape(
                    "writer value ID belongs to another original execution author",
                ))
            },
            work,
        )?;
        let storage_value_type_id = field
            .storage_type()
            .map(|storage| {
                storage_sources
                    .ok_or(E::InvalidShape("writer storage source index is absent"))?
                    .source_id_observed(storage, work)?
                    .ok_or(E::InvalidShape(
                        "writer storage value ID has no original execution author",
                    ))
            })
            .transpose()?;
        let mut token = reserve(32, work)?;
        for byte in field.token().to_bytes() {
            token.push(byte);
            work.step()?;
        }
        output.push(wire::ConnectorWriteFieldBinding {
            field_token: token,
            source_field_id: Some(*id),
            value_type_id: Some(value_id),
            storage_value_type_id,
        });
        work.step()?;
    }
    Ok(output)
}
fn decode_group<'a>(
    fields: &[wire::ConnectorWriteFieldBinding],
    types: &'a DecodedTypeTable,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ConnectorWriteFieldRef<'a>>, E> {
    let mut output = reserve(fields.len(), work)?;
    for field in fields {
        let id = observed(
            field
                .source_field_id
                .ok_or(E::InvalidShape("writer Field ID is absent")),
            work,
        )?;
        observed(
            if field.field_token.len() == 32 {
                Ok(())
            } else {
                Err(E::InvalidShape(
                    "writer field token must contain exactly 32 bytes",
                ))
            },
            work,
        )?;
        let actual = types.source_field_observed(id, work)?;
        let actual = observed(
            actual.ok_or(E::InvalidShape(
                "writer Field ID is absent from type namespace",
            )),
            work,
        )?;
        let mut token = [0; 32];
        for (dst, src) in token.iter_mut().zip(&field.field_token) {
            *dst = *src;
            work.step()?;
        }
        let value_id = observed(
            field
                .value_type_id
                .ok_or(E::InvalidShape("writer value binding ID is absent")),
            work,
        )?;
        let value = types
            .value_type_observed(value_id, work)?
            .ok_or(E::InvalidShape(
                "writer value binding ID is absent from execution namespace",
            ))?;
        work.step()?;
        let storage = field
            .storage_value_type_id
            .map(|id| {
                types.value_type_observed(id, work)?.ok_or(E::InvalidShape(
                    "writer storage value binding ID is absent from execution namespace",
                ))
            })
            .transpose()?;
        output.push(ConnectorWriteFieldRef::new_typed_with_storage(
            ConnectorWriteFieldToken::from_bytes(token),
            actual.as_ref(),
            value,
            storage,
        ));
        work.step()?;
    }
    Ok(output)
}

// A layout recipe borrows the same provider and payload namespaces as a writer.
// Its only additional backing is the exact sequence of input tokens.
pub(crate) fn encode_write_layouts_in(
    sources: &std::collections::BTreeMap<NodeId, ConnectorWriteLayoutRecipeDraft>,
    bindings: &EncodedProviderBindings<'_, '_>,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source: usize,
    limits: impl Into<Option<WriterRecipeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<wire::FrozenWriteLayoutRecipe>, usize), E> {
    let limits = limits.into();
    let mut model = Model::new::<wire::FrozenWriteLayoutRecipe>(sources.len(), source)?;
    for recipe in sources.values() {
        let count = recipe.inputs().len();
        model.fields(count)?;
        model.request(array::<[u8; 32]>(count)?, 1)?;
        work.step()?;
    }
    let facts = model.combined(&Default::default(), limits)?;
    let mut output = reserve(sources.len(), work)?;
    for (node, recipe) in sources {
        let provider_binding_id = bindings.write_source_id_observed(recipe.binding(), work)?;
        let recipe_payload_id = payloads.source_id_observed(recipe.payload(), work)?;
        let mut input_field_tokens = reserve(mul(recipe.inputs().len(), 32)?, work)?;
        for token in recipe.inputs() {
            input_field_tokens.extend_from_slice(&token.to_bytes());
            work.step()?;
        }
        output.push(wire::FrozenWriteLayoutRecipe {
            node_id: Some(node.get()),
            provider_binding_id: Some(provider_binding_id),
            recipe_payload_id: Some(recipe_payload_id),
            input_field_tokens,
        });
        work.step()?;
    }
    Ok((
        output,
        facts.coexisting_source_and_request_bytes_upper_bound,
    ))
}

pub(crate) fn decode_write_layouts_in(
    sources: &[wire::FrozenWriteLayoutRecipe],
    bindings: &DecodedProviderBindings<'_, '_>,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source: usize,
    limits: impl Into<Option<WriterRecipeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        std::collections::BTreeMap<NodeId, ConnectorWriteLayoutRecipeDraft>,
        usize,
    ),
    E,
> {
    let limits = limits.into();
    let recipe = match sources {
        [] => return Ok((std::collections::BTreeMap::new(), source)),
        [recipe] => recipe,
        _ => {
            return Err(E::InvalidShape(
                "a fragment has at most one write layout recipe at its root",
            ));
        }
    };
    let count = recipe.input_field_tokens.len() / 32;
    if count == 0 || !recipe.input_field_tokens.len().is_multiple_of(32) {
        return Err(E::InvalidShape(
            "write layout recipe input tokens must have 32 bytes each",
        ));
    }
    let mut model = Model::new::<(NodeId, ConnectorWriteLayoutRecipeDraft)>(1, source)?;
    model.fields(count)?;
    let slice_layout = array::<ConnectorWriteFieldToken>(count)?;
    model.request(slice_layout, 1)?;
    let arc_layout = novarocks_type_contract::owned_resources::layout::arc_layout(slice_layout)
        .map_err(E::ConstructionResource)?;
    model.request(arc_layout, 1)?;
    // Ordered input identities are validated before the draft is published.
    let set_node = btree_resources_v2::node_layout_typed::<&ConnectorWriteFieldToken, ()>()
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    model.request(set_node, count)?;
    let map_node =
        btree_resources_v2::node_layout_typed::<NodeId, ConnectorWriteLayoutRecipeDraft>()
            .map_err(|_| CompileControlError::ResourceExhausted)?;
    model.request(map_node, 1)?;
    let facts = model.combined(&Default::default(), limits)?;
    let node = recipe
        .node_id
        .ok_or(E::InvalidShape("write layout node ID is absent"))?;
    let binding_id = recipe.provider_binding_id.ok_or(E::InvalidShape(
        "write layout provider binding ID is absent",
    ))?;
    let payload_id = recipe
        .recipe_payload_id
        .ok_or(E::InvalidShape("write layout payload ID is absent"))?;
    let binding = bindings
        .write_binding_observed(binding_id, work)?
        .ok_or(E::InvalidShape("write layout provider binding is missing"))?;
    let payload = payloads
        .payload_observed(payload_id, work)?
        .ok_or(E::InvalidShape("write layout payload is missing"))?;
    let mut inputs = reserve(count, work)?;
    for token in recipe.input_field_tokens.as_chunks::<32>().0 {
        inputs.push(ConnectorWriteFieldToken::from_bytes(*token));
        work.step()?;
    }
    work.flush()?;
    let draft = ConnectorWriteLayoutRecipeDraft::try_new(binding.clone(), payload.clone(), inputs)
        .map_err(E::Provider)?;
    let output = std::collections::BTreeMap::from([(NodeId::new(node), draft)]);
    work.step()?;
    Ok((
        output,
        facts.coexisting_source_and_request_bytes_upper_bound,
    ))
}

/// Encode all five original ordered roles on the caller's existing scope.
/// Every binding/payload/Field reference must belong to the exact source loan.
/// The caller retains original source union B and owns entry/ordinary/success
/// tails; a typed control refusal performs no additional checkpoint here.
pub fn encode_writer_recipes_observed(
    sources: &[WriterRecipeSource<'_>],
    context: WriterRecipeEncodeContext<'_, '_, '_>,
    source: usize,
    limits: impl Into<Option<WriterRecipeProjectionLimits>>,
    admit: &mut impl FnMut(&WriterRecipeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<wire::FrozenWriterRecipe>, WriterRecipeProjectionFacts), E> {
    let limits = limits.into();
    let mut model = Model::new::<wire::FrozenWriterRecipe>(sources.len(), source)?;
    model.gate(limits, admit)?;
    observed(
        if std::ptr::addr_eq(work.control(), context.bindings.original_control())
            && std::ptr::addr_eq(work.control(), context.payloads.original_control())
        {
            Ok(())
        } else {
            Err(E::InvalidShape(
                "writer recipe namespaces use another original control",
            ))
        },
        work,
    )?;
    model.floor(array::<WriterRecipeSource<'_>>(sources.len())?.size())?;
    // Opaque namespace floors/lookups borrow the original owners. Their floor
    // includes an old invoice, so take max; never sum two copies of source B.
    model.work(mul(
        add(
            context.bindings.source_count(),
            context.payloads.source_count(),
        )?,
        add(256, mul(source, 4)?)?,
    )?)?;
    model.gate(limits, admit)?;
    let provider_floor = context.bindings.retained_floor_observed(work)?;
    let payload_floor = context.payloads.retained_floor_observed(work)?;
    observed(model.floor(provider_floor.max(payload_floor)), work)?;
    let field_roots = context.types.source_counts().1;
    let mut storage_count = 0;
    for recipe in sources {
        let (_, groups) = source_roles(recipe.recipe.input());
        let count = add(groups[0].len(), groups[1].len())?;
        model.fields(count)?;
        for group in groups {
            model.request(array::<wire::ConnectorWriteFieldBinding>(group.len())?, 1)?;
        }
        model.request(array::<u8>(32)?, count)?;
        model.work(add(
            mul(
                count,
                add(mul(field_roots, 3)?, context.types.source_counts().0)?,
            )?,
            mul(
                add(
                    context.bindings.source_count(),
                    context.payloads.source_count(),
                )?,
                2,
            )?,
        )?)?;
        model.gate(limits, admit)?;
        observed(
            if recipe.field_ids.len() == count {
                Ok(())
            } else {
                Err(E::InvalidShape(
                    "writer Field IDs do not cover original input occurrences",
                ))
            },
            work,
        )?;
        let floor = add(
            size_of::<ConnectorWriteRecipeDraft>(),
            add(
                array::<u32>(recipe.field_ids.len())?.size(),
                add(
                    array::<ConnectorWriteFieldBinding>(match recipe.recipe.input() {
                        ConnectorWriteInputShape::Data { fields } => fields.capacity(),
                        ConnectorWriteInputShape::RowLineage {
                            data_fields,
                            row_identity_fields,
                        } => add(data_fields.capacity(), row_identity_fields.capacity())?,
                        ConnectorWriteInputShape::PositionDelete {
                            identity_fields,
                            partition_source_fields,
                        }
                        | ConnectorWriteInputShape::DeletionVector {
                            identity_fields,
                            partition_source_fields,
                        } => add(
                            identity_fields.capacity(),
                            partition_source_fields.capacity(),
                        )?,
                        ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                            equality_fields.capacity()
                        }
                    })?
                    .size(),
                    recipe.recipe.payload().payload().len(),
                )?,
            )?,
        )?;
        observed(model.floor(floor), work)?;
        context
            .bindings
            .write_source_id_observed(recipe.recipe.binding(), work)?;
        context
            .payloads
            .source_id_observed(recipe.recipe.payload(), work)?;
        for (binding, id) in recipe.recipe.input().fields_iter().zip(recipe.field_ids) {
            storage_count = add(storage_count, usize::from(binding.storage_type().is_some()))?;
            let actual = context.types.source_field_source_observed(*id, work)?;
            observed(
                if actual.is_some_and(|actual| std::ptr::eq(actual, binding.field())) {
                    Ok(())
                } else {
                    Err(E::InvalidShape(
                        "writer Field ID belongs to another source owner",
                    ))
                },
                work,
            )?;
        }
    }
    let storage_sources = (storage_count != 0)
        .then(|| {
            StorageSourceIndex::prepare(
                context.types,
                mul(storage_count, 2)?,
                &mut |facts| {
                    model.request(facts.node_layout, facts.allocation_requests_upper_bound)?;
                    model.work(facts.cumulative_work_upper_bound)?;
                    model.gate(limits, admit)
                },
                &mut E::SourceModel,
                work,
            )
        })
        .transpose()?;
    if let Some(index) = storage_sources.as_ref() {
        for source in sources {
            for binding in source.recipe.input().fields_iter() {
                if let Some(storage) = binding.storage_type() {
                    index
                        .source_id_observed(storage, work)?
                        .ok_or(E::InvalidShape(
                            "writer storage value ID has no original execution author",
                        ))?;
                }
            }
        }
    }
    let _index = BindingIndex::prepare(sources.len(), |i| sources[i].node.get(), work)?;
    let mut output = reserve(sources.len(), work)?;
    for recipe in sources {
        let provider = context
            .bindings
            .write_source_id_observed(recipe.recipe.binding(), work)?;
        let payload = context
            .payloads
            .source_id_observed(recipe.recipe.payload(), work)?;
        let (shape, groups) = source_roles(recipe.recipe.input());
        let (first_ids, second_ids) = recipe.field_ids.split_at(groups[0].len());
        let first = encode_group(
            groups[0],
            first_ids,
            context.types,
            storage_sources.as_ref(),
            work,
        )?;
        let second = encode_group(
            groups[1],
            second_ids,
            context.types,
            storage_sources.as_ref(),
            work,
        )?;
        output.push(wire::FrozenWriterRecipe {
            node_id: Some(recipe.node.get()),
            provider_binding_id: Some(provider),
            handle_payload_id: Some(payload),
            input: Some(wire_shape(shape, first, second)),
        });
        work.step()?;
    }
    let facts = model.combined(&Default::default(), limits)?;
    admit(&facts)?;
    Ok((output, facts))
}

/// Decode owned Drafts through the original law/owned-copy author. This does
/// not seal them with a provider compiler, certify a Package or install writers.
/// Temporary FieldRef vectors borrow original decoded Arc<Field> contents.
pub fn decode_writer_recipes_observed(
    definitions: &[wire::FrozenWriterRecipe],
    context: WriterRecipeDecodeContext<'_, '_, '_>,
    source: usize,
    limits: impl Into<Option<WriterRecipeProjectionLimits>>,
    admit: &mut impl FnMut(&WriterRecipeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        Vec<(NodeId, ConnectorWriteRecipeDraft)>,
        WriterRecipeProjectionFacts,
    ),
    E,
> {
    let limits = limits.into();
    let decoded = decode_writer_recipes_core::<
        std::convert::Infallible,
        crate::value_type_table::resources::NoFactory,
    >(definitions, context, source, limits, None, admit, work)
    .map_err(ProjectionFailure::without_host)?;
    let (entries, facts, _) = decoded.into_parts();
    Ok((entries, facts))
}
/// Same original whole-namespace grammar/model and owned-copy constructor;
/// each independently escaping recipe/Field has its actual finite creator.
pub(crate) fn decode_writer_recipes_with_factory_in<H, F: ValueSchemaCreationFactory<H>>(
    definitions: &[wire::FrozenWriterRecipe],
    context: WriterRecipeDecodeContext<'_, '_, '_>,
    source: usize,
    limits: impl Into<Option<WriterRecipeProjectionLimits>>,
    factory: &F,
    admit: &mut impl FnMut(&WriterRecipeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CreatedWriterRecipes, Failure<H>> {
    let limits = limits.into();
    decode_writer_recipes_core(
        definitions,
        context,
        source,
        limits,
        Some(factory),
        admit,
        work,
    )
}
fn request_temporary<H, S: ValueSchemaCreationScope<H>>(
    scope: &mut Option<S>,
    layout: Layout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    if layout.size() != 0
        && let Some(scope) = scope
    {
        work.flush()?;
        scope
            .request_layout(layout)
            .map_err(ProjectionFailure::Host)?;
    }
    Ok(())
}
fn decode_writer_recipes_core<H, F: ValueSchemaCreationFactory<H>>(
    definitions: &[wire::FrozenWriterRecipe],
    context: WriterRecipeDecodeContext<'_, '_, '_>,
    source: usize,
    limits: Option<WriterRecipeProjectionLimits>,
    factory: Option<&F>,
    admit: &mut impl FnMut(&WriterRecipeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CreatedWriterRecipes, Failure<H>> {
    let mut model = Model::new::<(NodeId, ConnectorWriteRecipeDraft)>(definitions.len(), source)?;
    model.gate(limits, admit)?;
    observed(
        if std::ptr::addr_eq(work.control(), context.bindings.original_control())
            && std::ptr::addr_eq(work.control(), context.payloads.original_control())
        {
            Ok(())
        } else {
            Err(E::InvalidShape(
                "writer recipe namespaces use another original control",
            ))
        },
        work,
    )?;
    model.floor(array::<wire::FrozenWriterRecipe>(definitions.len())?.size())?;
    model.work(mul(
        add(
            context.bindings.source_count(),
            context.payloads.source_count(),
        )?,
        add(256, mul(source, 4)?)?,
    )?)?;
    model.gate(limits, admit)?;
    let provider_floor = context.bindings.retained_floor_observed(work)?;
    let payload_floor = context.payloads.retained_floor_observed(work)?;
    observed(
        model.floor(
            provider_floor
                .max(payload_floor)
                .max(context.types.necessary_source_fields_retained_floor()?),
        ),
        work,
    )?;
    let mut type_lookup = 0usize;
    for cardinality in [
        context.types.source_field_count(),
        context.types.value_type_count(),
    ] {
        let bound = btree_resources_v2::lookup_work_typed(cardinality).map_err(|e| match e {
            btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
                E::Control(CompileControlError::ResourceExhausted)
            }
            btree_resources_v2::BTreeResourceError::SourceModel(s) => E::SourceModel(s),
        })?;
        type_lookup = add(type_lookup, bound)?;
    }
    for definition in definitions {
        observed(
            definition
                .node_id
                .ok_or(E::InvalidShape("writer recipe node ID is absent")),
            work,
        )?;
        observed(
            definition
                .provider_binding_id
                .ok_or(E::InvalidShape("writer provider binding ID is absent")),
            work,
        )?;
        observed(
            definition
                .handle_payload_id
                .ok_or(E::InvalidShape("writer handle payload ID is absent")),
            work,
        )?;
        let input = observed(
            definition
                .input
                .as_ref()
                .ok_or(E::InvalidShape("writer input is absent")),
            work,
        )?;
        // Capturing the original kind also makes role counts/capacities
        // known. Numerical contributions must pass before its success step.
        let roles = raw_roles(input);
        let (_, groups, capacity) = match roles {
            Ok(roles) => roles,
            Err(error) => return observed(Err(error), work).map_err(ProjectionFailure::Codec),
        };
        let count = add(groups[0].len(), groups[1].len())?;
        model.fields(count)?;
        for group in groups {
            model.request(array::<ConnectorWriteFieldRef<'_>>(group.len())?, 1)?;
        }
        model.work(add(
            mul(mul(count, type_lookup)?, 2)?,
            mul(
                2,
                add(
                    lookup_work_upper_bound(context.bindings.source_count()),
                    lookup_work_upper_bound(context.payloads.source_count()),
                )?,
            )?,
        )?)?;
        let mut raw_floor = add(
            size_of::<wire::FrozenWriterRecipe>(),
            array::<wire::ConnectorWriteFieldBinding>(capacity)?.size(),
        )?;
        model.gate(limits, admit)?;
        work.step()?;
        // No output is allocated before required source references resolve.
        let provider = context.bindings.write_binding_observed(
            definition
                .provider_binding_id
                .expect("preflight required provider ID"),
            work,
        )?;
        observed(
            provider.ok_or(E::InvalidShape(
                "writer provider binding ID is absent from namespace",
            )),
            work,
        )?;
        let payload = context.payloads.payload_observed(
            definition
                .handle_payload_id
                .expect("preflight required payload ID"),
            work,
        )?;
        observed(
            payload.ok_or(E::InvalidShape(
                "writer handle payload ID is absent from namespace",
            )),
            work,
        )?;
        for group in groups {
            for field in group {
                observed(
                    field
                        .source_field_id
                        .ok_or(E::InvalidShape("writer Field ID is absent")),
                    work,
                )?;
                observed(
                    if field.field_token.len() == 32 {
                        Ok(())
                    } else {
                        Err(E::InvalidShape(
                            "writer field token must contain exactly 32 bytes",
                        ))
                    },
                    work,
                )?;
                let value_id = observed(
                    field
                        .value_type_id
                        .ok_or(E::InvalidShape("writer value binding ID is absent")),
                    work,
                )?;
                observed(
                    context
                        .types
                        .value_type_observed(value_id, work)?
                        .ok_or(E::InvalidShape(
                            "writer value binding ID is absent from execution namespace",
                        )),
                    work,
                )?;
                if let Some(storage_id) = field.storage_value_type_id {
                    // This optional reference is now captured. Admit both its
                    // preflight and borrowed-input lookups before observing it.
                    model.work(mul(type_lookup, 2)?)?;
                    model.gate(limits, admit)?;
                    observed(context.types.value_type_observed(storage_id, work)?.ok_or(
                        E::InvalidShape("writer storage value binding ID is absent from execution namespace")), work)?;
                }
                raw_floor = add(raw_floor, field.field_token.capacity())?;
                observed(model.floor(raw_floor), work)?;
                let actual = context.types.source_field_observed(
                    field.source_field_id.expect("preflight required Field ID"),
                    work,
                )?;
                observed(
                    actual.ok_or(E::InvalidShape(
                        "writer Field ID is absent from type namespace",
                    )),
                    work,
                )?;
            }
        }
    }
    // Original complete header/reference checks precede even scope headers.
    let mut temporary = if let Some(factory) = factory.filter(|_| !definitions.is_empty()) {
        work.flush()?;
        Some(
            factory
                .begin_scope_observed(&mut |layout| {
                    model.request(layout, 1)?;
                    model.gate(limits, admit)
                })
                .map_err(|error| match error {
                    ValueSchemaScopeBeginFailure::Observer(e) => ProjectionFailure::Codec(e),
                    ValueSchemaScopeBeginFailure::Creator(e) => ProjectionFailure::Host(e),
                })?,
        )
    } else {
        None
    };
    request_temporary(&mut temporary, array::<usize>(definitions.len())?, work)?;
    let _index = BindingIndex::prepare(
        definitions.len(),
        |i| definitions[i].node_id.expect("preflight required node ID"),
        work,
    )?;
    request_temporary(
        &mut temporary,
        array::<(NodeId, ConnectorWriteRecipeDraft)>(definitions.len())?,
        work,
    )?;
    let mut output = reserve(definitions.len(), work)?;
    for definition in definitions {
        let provider = context.bindings.write_binding_observed(
            definition
                .provider_binding_id
                .expect("preflight required provider ID"),
            work,
        )?;
        let provider = observed(
            provider.ok_or(E::InvalidShape(
                "writer provider binding ID is absent from namespace",
            )),
            work,
        )?;
        let payload = context.payloads.payload_observed(
            definition
                .handle_payload_id
                .expect("preflight required payload ID"),
            work,
        )?;
        let payload = observed(
            payload.ok_or(E::InvalidShape(
                "writer handle payload ID is absent from namespace",
            )),
            work,
        )?;
        let (shape, groups, _) =
            raw_roles(definition.input.as_ref().expect("preflight required input"))?;
        request_temporary(
            &mut temporary,
            array::<ConnectorWriteFieldRef<'_>>(groups[0].len())?,
            work,
        )?;
        let first = decode_group(groups[0], context.types, work)?;
        request_temporary(
            &mut temporary,
            array::<ConnectorWriteFieldRef<'_>>(groups[1].len())?,
            work,
        )?;
        let second = decode_group(groups[1], context.types, work)?;
        let input = borrowed_shape(shape, &first, &second);
        let view_bytes = add(
            array::<ConnectorWriteFieldRef<'_>>(first.capacity())?.size(),
            array::<ConnectorWriteFieldRef<'_>>(second.capacity())?.size(),
        )?;
        let constructor_source = add(source, view_bytes)?;
        let mut current = WriterOwnedResourceFacts::default();
        let mut constructor_admit = |facts: &WriterOwnedResourceFacts| {
            current = *facts;
            admit(&model.combined(facts, limits)?)
        };
        let recipe = if let Some(factory) = factory {
            ConnectorWriteRecipeDraft::try_new_from_borrowed_input_with_factory_in(
                provider,
                payload,
                &input,
                constructor_source,
                factory,
                &mut constructor_admit,
                work,
            )
            .map_err(created_failure)?
        } else {
            ConnectorWriteRecipeDraft::try_new_from_borrowed_input_observed(
                provider,
                payload,
                &input,
                constructor_source,
                &mut constructor_admit,
                work,
            )?
        };
        model.complete(current)?;
        model.gate(limits, admit)?;
        output.push((
            NodeId::new(definition.node_id.expect("preflight required node ID")),
            recipe,
        ));
        work.step()?;
    }
    let facts = model.combined(&Default::default(), limits)?;
    admit(&facts)?;
    let temporary_storage = temporary
        .as_ref()
        .map_or_else(ValueTypeStorageLifetime::pure, |scope| {
            scope.storage_lifetime()
        });
    Ok(CreatedWriterRecipes {
        entries: output,
        facts,
        temporary_storage,
    })
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    fn limits() -> WriterRecipeProjectionLimits {
        WriterRecipeProjectionLimits {
            max_recipes: usize::MAX,
            max_input_fields: usize::MAX,
            max_token_bytes: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        }
    }
    struct Control {
        cause: Option<CompileControlError>,
        trace: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            if trace.len() == 2
                && let Some(cause) = self.cause.clone()
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    #[test]
    fn parent_composition_replaces_current_constructor_facts_and_never_adds_child_source_again() {
        let mut model = Model::new::<wire::FrozenWriterRecipe>(2, 4096).unwrap();
        model.fields(3).unwrap();
        model
            .request(array::<ConnectorWriteFieldRef<'_>>(3).unwrap(), 1)
            .unwrap();
        let base = model.combined(&Default::default(), Some(limits())).unwrap();
        let first = WriterOwnedResourceFacts {
            source_retained_bytes: 5000,
            source_floor: 1024,
            allocation_requests: 4,
            requested_bytes: 128,
            coexistence_bytes: 5128,
            work_units: 64,
        };
        let first_facts = model.combined(&first, Some(limits())).unwrap();
        assert_eq!(
            first_facts.allocation_requests_upper_bound,
            base.allocation_requests_upper_bound + 4
        );
        assert_eq!(
            first_facts.coexisting_source_and_request_bytes_upper_bound,
            4096 + base.allocation_request_bytes_upper_bound + 128
        );
        assert_eq!(model.combined(&first, Some(limits())).unwrap(), first_facts);
        model.complete(first).unwrap();
        assert_eq!(
            model.combined(&Default::default(), Some(limits())).unwrap(),
            first_facts
        );
        let second = WriterOwnedResourceFacts {
            source_retained_bytes: usize::MAX,
            allocation_requests: 2,
            requested_bytes: 32,
            work_units: 9,
            ..Default::default()
        };
        let final_facts = model.combined(&second, Some(limits())).unwrap();
        assert_eq!(
            final_facts.allocation_request_bytes_upper_bound,
            base.allocation_request_bytes_upper_bound + 160
        );
        assert_eq!(
            final_facts.coexisting_source_and_request_bytes_upper_bound,
            4096 + final_facts.allocation_request_bytes_upper_bound
        );
        assert_eq!(
            final_facts.cumulative_work_upper_bound,
            base.cumulative_work_upper_bound + 73
        );
    }
    #[test]
    fn seven_axes_use_actual_index_output_view_layout_and_reject_exact_one_under_without_admitter()
    {
        let mut model = Model::new::<wire::FrozenWriterRecipe>(2, 4096).unwrap();
        model.fields(3).unwrap();
        model
            .request(array::<ConnectorWriteFieldRef<'_>>(3).unwrap(), 1)
            .unwrap();
        let f = model.combined(&Default::default(), Some(limits())).unwrap();
        assert_eq!(f.recipe_count, 2);
        assert_eq!(f.input_field_count, 3);
        assert_eq!(f.token_bytes, 96);
        assert_eq!(f.allocation_requests_upper_bound, 3);
        assert_eq!(
            f.allocation_request_bytes_upper_bound,
            2 * size_of::<usize>()
                + 2 * size_of::<wire::FrozenWriterRecipe>()
                + 3 * size_of::<ConnectorWriteFieldRef<'_>>()
        );
        let exact = WriterRecipeProjectionLimits {
            max_recipes: 2,
            max_input_fields: 3,
            max_token_bytes: 96,
            max_allocation_requests: f.allocation_requests_upper_bound,
            max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: f
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: f.cumulative_work_upper_bound,
        };
        let mut calls = 0;
        model
            .gate(Some(exact), &mut |actual| {
                calls += 1;
                assert_eq!(*actual, f);
                Ok(())
            })
            .unwrap();
        for axis in 0..7 {
            let mut under = exact;
            match axis {
                0 => under.max_recipes -= 1,
                1 => under.max_input_fields -= 1,
                2 => under.max_token_bytes -= 1,
                3 => under.max_allocation_requests -= 1,
                4 => under.max_allocation_request_bytes -= 1,
                5 => under.max_coexisting_source_and_request_bytes -= 1,
                _ => under.max_work -= 1,
            }
            assert!(matches!(
                model.gate(Some(under), &mut |_| {
                    calls += 1;
                    Ok(())
                }),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
        }
        assert_eq!(calls, 1);
    }
    #[test]
    fn unlimited_thresholds_do_not_allow_unrepresentable_field_facts_or_layouts() {
        let mut multiplication = Model::new::<wire::FrozenWriterRecipe>(0, 0).unwrap();
        assert!(matches!(
            multiplication.fields(usize::MAX / 32 + 1),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let mut count = Model::new::<wire::FrozenWriterRecipe>(0, 0).unwrap();
        count.facts.input_field_count = usize::MAX;
        assert!(matches!(
            count.fields(1),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        let mut tokens = Model::new::<wire::FrozenWriterRecipe>(0, 0).unwrap();
        tokens.facts.token_bytes = usize::MAX - 31;
        assert!(matches!(
            tokens.fields(1),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(matches!(
            array::<ConnectorWriteFieldRef<'_>>(usize::MAX),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }

    #[test]
    fn known_field_and_token_gate_precedes_real_pending255_late_control_without_scope_footer() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                cause: Some(cause),
                trace: Mutex::new(Vec::new()),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut actual = [0; 255];
            for (i, dst) in actual.iter_mut().enumerate() {
                *dst = i as u8;
                work.step().unwrap();
            }
            assert_eq!(actual[254], 254);
            let mut model = Model::new::<wire::FrozenWriterRecipe>(1, 4096).unwrap();
            model.fields(320).unwrap();
            let mut cap = limits();
            cap.max_token_bytes = 320 * 32 - 1;
            assert!(matches!(
                model.gate(Some(cap), &mut |_| Ok(())),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*control.trace.lock().unwrap(), vec![0]);
        }
    }
}

#[cfg(test)]
#[path = "physical_writer_recipe_v2/tests.rs"]
mod tests;
