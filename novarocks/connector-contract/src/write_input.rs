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

//! Pure provider-issued input identities and role-preserving field shapes.
use crate::owned_copy::{OwnedCopy, PlainCopy};
use crate::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorWriteFieldToken,
};
use arrow_schema::Field;
use novarocks_type_contract::owned_resources::hashmap;
use std::collections::HashSet;

/// The exact provider generation a write value belongs to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteBinding {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl ConnectorWriteBinding {
    pub const fn new(
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    ) -> Self {
        Self {
            descriptor,
            catalog_handle,
        }
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

#[derive(Debug)]
pub(crate) struct CreatedWriterField {
    field: novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization,
    // Only this copied root and its actual reachable child backing loans.
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
#[derive(Clone, Debug)]
enum WriterFieldStorage {
    Pure(novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization),
    Created(std::sync::Arc<CreatedWriterField>),
}
impl WriterFieldStorage {
    fn original(&self) -> &novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization{
        match self {
            Self::Pure(field) => field,
            Self::Created(field) => &field.field,
        }
    }
}
#[derive(Clone, Debug)]
pub struct ConnectorWriteFieldBinding {
    token: ConnectorWriteFieldToken,
    field: WriterFieldStorage,
    execution_type: Option<novarocks_type_contract::FunctionValueType>,
    // None is the original exact/NULL-only input relation. Some(T) explicitly
    // licenses only the checked plain-Fixed-byte projection into input W.
    storage_type: Option<novarocks_type_contract::FunctionValueType>,
}

impl PartialEq for ConnectorWriteFieldBinding {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
            && self.execution_type == other.execution_type
            && self.storage_type == other.storage_type
            && crate::arrow_fields_exact(self.field(), other.field())
    }
}
impl Eq for ConnectorWriteFieldBinding {}

impl ConnectorWriteFieldBinding {
    pub fn new(token: ConnectorWriteFieldToken, field: Field) -> Self {
        Self { token, field: WriterFieldStorage::Pure(novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization::Plain(field)), execution_type: None, storage_type: None }
    }

    pub(crate) fn from_owned_materialization(
        token: ConnectorWriteFieldToken,
        field: novarocks_type_contract::owned_resources::metadata_materialization::MaterializedField,
    ) -> Self {
        Self { token, field: WriterFieldStorage::Pure(novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization::Materialized(field)), execution_type: None, storage_type: None }
    }

    pub(crate) fn created_allocation_layout() -> Result<
        std::alloc::Layout,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            CreatedWriterField,
        >())
    }
    pub(crate) fn from_created_materialization(
        token: ConnectorWriteFieldToken,
        field:novarocks_type_contract::owned_resources::metadata_materialization::MaterializedField,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        Self {
            token,
            field:WriterFieldStorage::Created(std::sync::Arc::new(CreatedWriterField {
                field:novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization::Materialized(field),
                _storage:storage,
            })),
            execution_type:None,
            storage_type:None,
        }
    }
    /// Pair provider-owned source facts with their explicitly authored execution
    /// projection. An absent projection belongs only to the expansion adapter.
    pub fn try_new_typed(
        token: ConnectorWriteFieldToken,
        field: Field,
        execution_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        Self::new(token, field).with_execution_type(execution_type)
    }

    pub fn with_execution_type(
        mut self,
        execution_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        let matches = write_field_matches_execution_type_observed::<
            novarocks_type_contract::ValueTypeError,
        >(self.field(), &execution_type, |_| Ok(()))
        .map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        })?;
        if !matches {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "writer source facts conflict with their explicit execution projection",
            ));
        }
        validate_write_input_projection_observed(
            &execution_type,
            self.storage_type.as_ref(),
            || Ok::<(), ConnectorError>(()),
        )?;
        self.execution_type = Some(execution_type);
        Ok(self)
    }

    pub fn execution_type(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.execution_type.as_ref()
    }

    /// Add the original complete storage target to an explicitly typed input.
    /// This is a closed raw-byte projection declaration, not a cast capability.
    pub fn with_storage_type(
        mut self,
        storage_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        let input = self.execution_type.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "writer storage projection requires its complete input declaration",
            )
        })?;
        validate_write_input_projection_observed(input, Some(&storage_type), || {
            Ok::<(), ConnectorError>(())
        })?;
        self.storage_type = Some(storage_type);
        Ok(self)
    }

    pub fn storage_type(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.storage_type.as_ref()
    }

    /// Borrow the exact immutable writer-owned field and its constructor fact.
    pub fn original_field_materialization(&self) -> &novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization{
        self.field.original()
    }

    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }

    pub fn field(&self) -> &Field {
        self.field.original().field()
    }
}

/// Emit complete normalized W/T identities without formatting a shadow
/// tree or including storage lifetimes. Source Field attributes are separate.
pub fn write_input_projection_identity_observed<E>(
    input: Option<&novarocks_type_contract::FunctionValueType>,
    storage: Option<&novarocks_type_contract::FunctionValueType>,
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    write(b"novarocks-write-input-projection-v1")?;
    for value in [input, storage] {
        write(&[u8::from(value.is_some())])?;
        if let Some(value) = value {
            novarocks_type_contract::write_value_type_identity_observed(
                value.value_type(),
                &mut write,
            )?;
            novarocks_type_contract::write_value_layout_identity_observed(
                value.typed_binding().layout(),
                &mut write,
            )?;
        }
    }
    Ok(())
}

/// Validate the closed input W / storage T declaration using the neutral
/// type author. Provider Field attributes remain a separate namespace.
pub fn validate_write_input_projection_observed<E: From<ConnectorError>>(
    input: &novarocks_type_contract::FunctionValueType,
    storage: Option<&novarocks_type_contract::FunctionValueType>,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    let Some(storage) = storage else {
        return Ok(());
    };
    let valid = novarocks_type_contract::CheckedWriteInputProjection::try_new_observed(
        input.typed_binding().borrowed(),
        input.typed_binding().borrowed(),
        Some(storage.typed_binding().borrowed()),
        observe,
    )?;
    if valid.is_none() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "writer input differs from its explicit storage projection",
        )
        .into());
    }
    Ok(())
}

/// Validate the physical part of an explicitly authored writer input. The
/// provider's Field remains a source fact, and contributes no inferred logical
/// domain. Ownership and allocation admission remain with the real author.
pub fn write_field_matches_execution_type_observed<
    E: From<novarocks_type_contract::ValueTypeError>,
>(
    field: &Field,
    execution_type: &novarocks_type_contract::FunctionValueType,
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    observe(1)?;
    if field.is_nullable() != execution_type.nullable() {
        return Ok(false);
    }
    novarocks_type_contract::value_layout_matches_carrier_observed(
        execution_type.typed_binding().layout(),
        field.data_type(),
        observe,
    )
}

/// One original field loan and its exact provider token. Constructing a loan
/// neither copies the Arrow field nor validates or seals provider semantics.
#[derive(Clone, Copy, Debug)]
pub struct ConnectorWriteFieldRef<'a> {
    token: ConnectorWriteFieldToken,
    field: &'a Field,
    execution_type: Option<&'a novarocks_type_contract::FunctionValueType>,
    storage_type: Option<&'a novarocks_type_contract::FunctionValueType>,
}

impl<'a> ConnectorWriteFieldRef<'a> {
    pub const fn new(token: ConnectorWriteFieldToken, field: &'a Field) -> Self {
        Self {
            token,
            field,
            execution_type: None,
            storage_type: None,
        }
    }

    /// Borrow the original independent source Field and complete execution
    /// declaration. The recipe's existing validator checks this pair.
    pub const fn new_typed(
        token: ConnectorWriteFieldToken,
        field: &'a Field,
        execution_type: &'a novarocks_type_contract::FunctionValueType,
    ) -> Self {
        Self {
            token,
            field,
            execution_type: Some(execution_type),
            storage_type: None,
        }
    }
    /// Borrow both namespaces; the recipe validates this explicit projection.
    pub const fn new_typed_with_storage(
        token: ConnectorWriteFieldToken,
        field: &'a Field,
        execution_type: &'a novarocks_type_contract::FunctionValueType,
        storage_type: Option<&'a novarocks_type_contract::FunctionValueType>,
    ) -> Self {
        Self {
            token,
            field,
            execution_type: Some(execution_type),
            storage_type,
        }
    }
    pub const fn storage_type(&self) -> Option<&'a novarocks_type_contract::FunctionValueType> {
        self.storage_type
    }
    pub fn from_binding(binding: &'a ConnectorWriteFieldBinding) -> Self {
        Self {
            token: binding.token(),
            field: binding.field(),
            execution_type: binding.execution_type(),
            storage_type: binding.storage_type(),
        }
    }
    pub const fn execution_type(&self) -> Option<&'a novarocks_type_contract::FunctionValueType> {
        self.execution_type
    }

    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }

    pub const fn field(&self) -> &'a Field {
        self.field
    }
}

/// Provider-signed counterpart to the SQL admission input request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteInput<Fields> {
    Data {
        fields: Fields,
    },
    RowLineage {
        data_fields: Fields,
        row_identity_fields: Fields,
    },
    PositionDelete {
        identity_fields: Fields,
        partition_source_fields: Fields,
    },
    DeletionVector {
        identity_fields: Fields,
        partition_source_fields: Fields,
    },
    EqualityDelete {
        equality_fields: Fields,
    },
}

/// Owned provider input with the original concrete construction/inference API.
pub type ConnectorWriteInputShape = ConnectorWriteInput<Vec<ConnectorWriteFieldBinding>>;

impl ConnectorWriteInputShape {
    /// Exact roles, order, field tokens and complete Arrow fields, with observed
    /// nested schema/metadata work and no field copying or packed-role guess.
    /// Both shapes come from validated writer drafts, preserving the writer
    /// owner's depth/aggregate-byte domain rather than a new type-node gate.
    pub(crate) fn same_layout_observed<E>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        let same_roles = match (self, other) {
            (Self::Data { fields: left }, Self::Data { fields: right }) => {
                left.len() == right.len()
            }
            (
                Self::EqualityDelete {
                    equality_fields: left,
                },
                Self::EqualityDelete {
                    equality_fields: right,
                },
            ) => left.len() == right.len(),
            (
                Self::RowLineage {
                    data_fields: left,
                    row_identity_fields: left_identity,
                },
                Self::RowLineage {
                    data_fields: right,
                    row_identity_fields: right_identity,
                },
            ) => left.len() == right.len() && left_identity.len() == right_identity.len(),
            (
                Self::PositionDelete {
                    identity_fields: left,
                    partition_source_fields: left_partition,
                },
                Self::PositionDelete {
                    identity_fields: right,
                    partition_source_fields: right_partition,
                },
            )
            | (
                Self::DeletionVector {
                    identity_fields: left,
                    partition_source_fields: left_partition,
                },
                Self::DeletionVector {
                    identity_fields: right,
                    partition_source_fields: right_partition,
                },
            ) => left.len() == right.len() && left_partition.len() == right_partition.len(),
            _ => false,
        };
        if !same_roles {
            return Ok(false);
        }
        for (left, right) in self.fields_iter().zip(other.fields_iter()) {
            observe()?;
            if left.token() != right.token() {
                return Ok(false);
            }
            match (left.execution_type(), right.execution_type()) {
                (Some(left), Some(right))
                    if !left.exactly_equals_checked_observed(right, &mut observe)? =>
                {
                    return Ok(false);
                }
                (None, None) | (Some(_), Some(_)) => {}
                _ => return Ok(false),
            }
            match (left.storage_type(), right.storage_type()) {
                (Some(left), Some(right))
                    if !left.exactly_equals_checked_observed(right, &mut observe)? =>
                {
                    return Ok(false);
                }
                (None, None) | (Some(_), Some(_)) => {}
                _ => return Ok(false),
            }
            if !novarocks_type_contract::arrow_fields_exact_observed::<E>(
                left.field(),
                right.field(),
                &mut observe,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn field_count(&self) -> usize {
        self.source_field_count()
    }

    pub fn fields_iter(&self) -> impl Iterator<Item = &ConnectorWriteFieldBinding> {
        self.role_vectors()
            .into_iter()
            .flatten()
            .flat_map(|role| role.iter())
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.validate_with(&mut PlainCopy)
    }

    pub fn fields(&self) -> Vec<&ConnectorWriteFieldBinding> {
        self.fields_iter().collect()
    }
}

/// The borrowed form retains the same closed role vocabulary as owned input.
/// The original writer constructor validates every loan and copies each field
/// through its sole observed schema author before publishing owned output.
pub type ConnectorWriteInputRef<'a> = ConnectorWriteInput<&'a [ConnectorWriteFieldRef<'a>]>;

// Only the two original source carriers enter the common law/copy body. This
// private trait is not a provider extension point or a schema grammar.
pub(crate) trait WriteInputFields {
    fn len(&self) -> usize;
    fn field_refs(&self) -> impl Iterator<Item = ConnectorWriteFieldRef<'_>>;
    fn accumulate_source_backing<O: OwnedCopy>(
        &self,
        previous: usize,
        context: &O,
    ) -> Result<usize, O::Error>;
}

impl WriteInputFields for Vec<ConnectorWriteFieldBinding> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn field_refs(&self) -> impl Iterator<Item = ConnectorWriteFieldRef<'_>> {
        self.iter().map(ConnectorWriteFieldRef::from_binding)
    }
    fn accumulate_source_backing<O: OwnedCopy>(
        &self,
        previous: usize,
        context: &O,
    ) -> Result<usize, O::Error> {
        // Owned role Vecs retain independent allocations.
        context.add(
            previous,
            context.mul(self.capacity(), size_of::<ConnectorWriteFieldBinding>())?,
        )
    }
}

impl WriteInputFields for &[ConnectorWriteFieldRef<'_>] {
    fn len(&self) -> usize {
        <[ConnectorWriteFieldRef<'_>]>::len(self)
    }
    fn field_refs(&self) -> impl Iterator<Item = ConnectorWriteFieldRef<'_>> {
        self.iter().copied()
    }
    fn accumulate_source_backing<O: OwnedCopy>(
        &self,
        previous: usize,
        context: &O,
    ) -> Result<usize, O::Error> {
        // Borrowed roles can overlap. A necessary floor must not count the
        // same source allocation twice; the caller still invoices full backing.
        Ok(previous.max(context.mul(self.len(), size_of::<ConnectorWriteFieldRef<'_>>())?))
    }
}

impl<F> ConnectorWriteInput<F> {
    fn role_vectors(&self) -> [Option<&F>; 2] {
        match self {
            Self::Data { fields } => [Some(fields), None],
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => [Some(data_fields), Some(row_identity_fields)],
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => [Some(identity_fields), Some(partition_source_fields)],
            Self::EqualityDelete { equality_fields } => [Some(equality_fields), None],
        }
    }

    pub(crate) fn source_field_count(&self) -> usize
    where
        F: WriteInputFields,
    {
        let [first, second] = self.role_vectors();
        first
            .expect("first role exists")
            .len()
            .saturating_add(second.map_or(0, WriteInputFields::len))
    }

    pub(crate) fn field_refs(&self) -> impl Iterator<Item = ConnectorWriteFieldRef<'_>>
    where
        F: WriteInputFields,
    {
        self.role_vectors()
            .into_iter()
            .flatten()
            .flat_map(|role| role.field_refs())
    }

    /// Admit each original role's reserve and possible Vec-to-Box shrink before
    /// validation. Role Vec backings are independently owned; nested shared
    /// fields are deliberately absent from this necessary source lower floor.
    pub(crate) fn preflight_owned_roles<O: OwnedCopy>(
        &self,
        context: &mut O,
    ) -> Result<(), O::Error>
    where
        F: WriteInputFields,
    {
        let mut backing = 0;
        let mut fields = 0;
        for role in self.role_vectors().into_iter().flatten() {
            backing = role.accumulate_source_backing(backing, context)?;
            fields = context.add(fields, role.len())?;
            context.array::<ConnectorWriteFieldBinding>(role.len(), 2)?;
        }
        context.source_floor(context.add(size_of::<Self>(), backing)?)?;
        // The two passes move inline outputs and the materializing pass may
        // also move each retained binding during the original trim operation.
        context.work(context.add(
            size_of::<ConnectorWriteInputShape>(),
            context.mul(
                context.mul(fields, size_of::<ConnectorWriteFieldBinding>())?,
                3,
            )?,
        )?)
    }

    /// The constructor calls the O(1) role preflight once before validation.
    /// Counting visits the original field-copy grammar without output objects.
    pub(crate) fn owned_bounded_core<O: OwnedCopy>(
        &self,
        context: &mut O,
    ) -> Result<Option<ConnectorWriteInputShape>, O::Error>
    where
        F: WriteInputFields,
    {
        fn role<F: WriteInputFields, O: OwnedCopy>(
            fields: &F,
            context: &mut O,
        ) -> Result<Option<Vec<ConnectorWriteFieldBinding>>, O::Error> {
            let materializes = context.materializes();
            if materializes {
                context.prefunded_request(
                    std::alloc::Layout::array::<ConnectorWriteFieldBinding>(fields.len())
                        .map_err(|_| context.arithmetic())?,
                    1,
                )?;
            }
            let mut output = Vec::new();
            if materializes {
                context.flush()?;
                let reserved = output.try_reserve_exact(fields.len());
                context.reserve_exit(reserved)?;
            }
            for binding in fields.field_refs() {
                let field =
                    crate::schema::owned_field_materialization_core(binding.field(), context)?;
                if materializes {
                    let field = field.ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            "writer owned copy did not materialize a field",
                        )
                    })?;
                    let mut copied = if context.created_fields() {
                        let layout = ConnectorWriteFieldBinding::created_allocation_layout()
                            .map_err(|_| context.arithmetic())?;
                        context.created_wrapper(layout)?;
                        let storage = context.finish_field()?;
                        context.flush()?;
                        ConnectorWriteFieldBinding::from_created_materialization(
                            binding.token(),
                            field,
                            storage,
                        )
                    } else if context.source_invoice().is_some() {
                        ConnectorWriteFieldBinding::from_owned_materialization(
                            binding.token(),
                            field,
                        )
                    } else {
                        ConnectorWriteFieldBinding::new(
                            binding.token(),
                            field.into_original_field(),
                        )
                    };
                    if !context.created_fields() {
                        let _ = context.finish_field()?;
                    }
                    if let Some(value) = binding.execution_type() {
                        copied.execution_type = Some(value.clone_observed(|| context.step())?);
                    }
                    if let Some(value) = binding.storage_type() {
                        copied.storage_type = Some(value.clone_observed(|| context.step())?);
                    }
                    output.push(copied);
                } else {
                    let _ = context.finish_field()?;
                    if binding.execution_type().is_some() {
                        context.work(2)?;
                    }
                    if binding.storage_type().is_some() {
                        context.work(2)?;
                    }
                }
                context.step()?;
            }
            if !materializes {
                return Ok(None);
            }
            context.flush()?;
            // Preserve the original role-local trim; its conservative second
            // request and inline moves were admitted by preflight_owned_roles.
            if let Some(layout) = novarocks_type_contract::owned_resources::vec::box_facts(&output)
                .map_err(|_| context.arithmetic())?
                .requested_backing
            {
                context.prefunded_request(layout, 1)?;
            }
            let output = output.into_boxed_slice().into_vec();
            context.step()?;
            context.flush()?;
            Ok(Some(output))
        }
        let [first, second] = self.role_vectors();
        let first = role(
            first.expect("every write input has its first role"),
            context,
        )?;
        let second = match second {
            Some(fields) => role(fields, context)?,
            None => None,
        };
        let Some(first) = first else {
            return Ok(None);
        };
        let result = match self {
            Self::Data { .. } => ConnectorWriteInputShape::Data { fields: first },
            Self::EqualityDelete { .. } => ConnectorWriteInputShape::EqualityDelete {
                equality_fields: first,
            },
            Self::RowLineage { .. } => ConnectorWriteInputShape::RowLineage {
                data_fields: first,
                row_identity_fields: second.expect("second role was copied"),
            },
            Self::PositionDelete { .. } => ConnectorWriteInputShape::PositionDelete {
                identity_fields: first,
                partition_source_fields: second.expect("second role was copied"),
            },
            Self::DeletionVector { .. } => ConnectorWriteInputShape::DeletionVector {
                identity_fields: first,
                partition_source_fields: second.expect("second role was copied"),
            },
        };
        context.step()?;
        Ok(Some(result))
    }

    pub(crate) fn validate_with<O: OwnedCopy>(&self, context: &mut O) -> Result<(), O::Error>
    where
        F: WriteInputFields,
    {
        let [first, second] = self.role_vectors();
        let count = context.add(
            first.expect("first role exists").len(),
            second.map_or(0, WriteInputFields::len),
        )?;
        if count == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write input shape must contain at least one field",
            )
            .into());
        }
        let token_buckets = context.table::<ConnectorWriteFieldToken, ()>(count)?;
        let name_buckets = context.table::<String, ()>(count)?;
        // Both temporary sets are destroyed before schema validation, also
        // after a duplicate or interruption. These are closed Token/String
        // destructors; allocator release cost is not a cooperative CPU grant.
        context.table_cleanup::<ConnectorWriteFieldToken, ()>(count, 64)?;
        context.table_cleanup::<String, ()>(count, 128)?;
        context.flush()?;
        if let Some(layout) = hashmap::fresh_table_layout::<ConnectorWriteFieldToken, ()>(count)
            .map_err(|error| context.hash_error(error))?
            .layout
        {
            context.temporary_request(layout)?;
        }
        let mut tokens = HashSet::new();
        let reserved = tokens.try_reserve(count);
        context.reserve_exit(reserved)?;
        context.flush()?;
        if let Some(layout) = hashmap::fresh_table_layout::<String, ()>(count)
            .map_err(|error| context.hash_error(error))?
            .layout
        {
            context.temporary_request(layout)?;
        }
        let mut names = HashSet::new();
        let reserved = names.try_reserve(count);
        context.reserve_exit(reserved)?;
        let mut longest_name = 0;
        let data_width = match self {
            Self::Data { fields } => fields.len(),
            Self::RowLineage { data_fields, .. } => data_fields.len(),
            Self::EqualityDelete { .. }
            | Self::PositionDelete { .. }
            | Self::DeletionVector { .. } => 0,
        };
        for (ordinal, binding) in self.field_refs().enumerate() {
            if let Some(storage) = binding.storage_type() {
                if ordinal >= data_width {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "writer storage projection is only admitted for data fields",
                    )
                    .into());
                }
                let input = binding.execution_type().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "writer storage projection lost its complete input declaration",
                    )
                })?;
                validate_write_input_projection_observed(input, Some(storage), || context.step())?;
            }
            // Token Hash is the derived transparent [u8;32] body. Each table
            // had one fresh reserve, so its admitted bucket bound stays valid.
            if context.source_invoice().is_some() {
                let token_work =
                    hashmap::byte_array32_operations_work_upper_bound(token_buckets, 1)
                        .map_err(|error| context.hash_error(error))?;
                context.work(token_work)?;
            }
            context.flush()?;
            let unique_token = tokens.insert(binding.token());
            context.step()?;
            context.flush()?;
            // Preserve the original short circuit: a duplicate token never
            // copies the name, even before the later writer name-length law.
            if !unique_token {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write input shape contains a duplicate field token or name",
                )
                .into());
            }
            let name = binding.field().name();
            longest_name = longest_name.max(name.len());
            if context.source_invoice().is_some() {
                let name_work = hashmap::string_operations_work_upper_bound(
                    name_buckets,
                    1,
                    name.len(),
                    longest_name,
                )
                .map_err(|error| context.hash_error(error))?;
                context.work(name_work)?;
            }
            let name = context.real_string(name)?;
            context.flush()?;
            let unique_name = names.insert(name);
            context.step()?;
            context.flush()?;
            if !unique_name {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write input shape contains a duplicate field token or name",
                )
                .into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "write_input/tests.rs"]
mod tests;
