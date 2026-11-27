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

//! Provider-owned row-mutation admission and signed match contract.
//! Design: ADR-0049 (docs/adr/ADR-0049-provider-row-mutation-strategy-identity-routes-and-cohorts.md)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub use novarocks_connector_contract::{ConnectorRowMutationEffect, ConnectorWriteRouteId};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteFieldToken,
    ConnectorWriteOperationId, ConnectorWriteTargetRef, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
};

#[cfg(test)]
mod match_layout_tests;

pub const CONNECTOR_ROW_MUTATION_CONTRACT_VERSION: u32 = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorRowMutationIntent {
    Delete,
    Update,
    Merge {
        effects: Vec<ConnectorRowMutationEffect>,
    },
}

impl ConnectorRowMutationIntent {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Self::Merge { effects } = self {
            validate_effects(effects, "connector merge intent")?;
        }
        Ok(())
    }

    pub fn accepts(&self, effect: ConnectorRowMutationEffect) -> bool {
        match self {
            Self::Delete => effect == ConnectorRowMutationEffect::Delete,
            Self::Update => effect == ConnectorRowMutationEffect::Replace,
            Self::Merge { effects } => effects.contains(&effect),
        }
    }
}

enum MutationFieldValidationFailure {
    Value(novarocks_type_contract::ValueTypeError),
    Caller(ConnectorError),
}
impl From<novarocks_type_contract::ValueTypeError> for MutationFieldValidationFailure {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Value(error)
    }
}
fn mutation_field_error(error: MutationFieldValidationFailure) -> ConnectorError {
    match error {
        MutationFieldValidationFailure::Caller(error) => error,
        MutationFieldValidationFailure::Value(error) => invalid_match(error.to_string()),
    }
}

fn validate_mutation_execution_field(
    field: &Field,
    execution_type: &novarocks_type_contract::FunctionValueType,
    mut observe: impl FnMut(usize) -> Result<(), ConnectorError>,
) -> Result<(), ConnectorError> {
    let matches = novarocks_connector_contract::write_field_matches_execution_type_observed::<
        MutationFieldValidationFailure,
    >(field, execution_type, |units| {
        observe(units).map_err(MutationFieldValidationFailure::Caller)
    })
    .map_err(mutation_field_error)?;
    if !matches {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "mutation source field conflicts with its complete execution declaration",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ConnectorMutationSourceField {
    token: ConnectorWriteFieldToken,
    field: Field,
    source_ordinal: u32,
    execution_type: Option<novarocks_type_contract::FunctionValueType>,
}

impl ConnectorMutationSourceField {
    /// Preserve the original provider Field and carry its independently
    /// authored complete value binding. Source attributes decide no domain.
    pub fn try_new_typed(
        token: ConnectorWriteFieldToken,
        field: Field,
        source_ordinal: u32,
        execution_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_typed_observed(token, field, source_ordinal, execution_type, |_| Ok(()))
    }
    pub fn try_new_typed_observed(
        token: ConnectorWriteFieldToken,
        field: Field,
        source_ordinal: u32,
        execution_type: novarocks_type_contract::FunctionValueType,
        observe: impl FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        validate_mutation_execution_field(&field, &execution_type, observe)?;
        Ok(Self {
            token,
            field,
            source_ordinal,
            execution_type: Some(execution_type),
        })
    }
    pub fn execution_type(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.execution_type.as_ref()
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub const fn source_ordinal(&self) -> u32 {
        self.source_ordinal
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorMutationTargetField {
    token: ConnectorWriteFieldToken,
    field: Field,
    target_ordinal: u32,
    storage_type: Option<novarocks_type_contract::FunctionValueType>,
    execution_type: Option<novarocks_type_contract::FunctionValueType>,
    temporal_match_source: Option<[u8; 32]>,
}

impl ConnectorMutationTargetField {
    /// Preserve the original provider Field and carry its independently
    /// authored complete value binding. Source attributes decide no domain.
    pub fn try_new_typed(
        token: ConnectorWriteFieldToken,
        field: Field,
        target_ordinal: u32,
        execution_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_typed_observed(token, field, target_ordinal, execution_type, |_| Ok(()))
    }
    pub fn try_new_typed_observed(
        token: ConnectorWriteFieldToken,
        field: Field,
        target_ordinal: u32,
        execution_type: novarocks_type_contract::FunctionValueType,
        observe: impl FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        validate_mutation_execution_field(&field, &execution_type, observe)?;
        Ok(Self {
            token,
            field,
            target_ordinal,
            storage_type: None,
            execution_type: Some(execution_type),
            temporal_match_source: None,
        })
    }

    /// Seal the exact provider field that supplies the same declared scalar
    /// temporal value through its format-specific read projection.
    pub fn with_temporal_match_source(
        mut self,
        source: &Field,
        declaration: novarocks_type_contract::TemporalType,
    ) -> Result<Self, ConnectorError> {
        let temporal = self
            .execution_type()
            .and_then(|value| value.value_type().temporal());
        if self.storage_type.is_some()
            || self.temporal_match_source.is_some()
            || temporal != Some(declaration)
            || source.name() != self.field.name()
            || (source.is_nullable() && !self.field.is_nullable())
            || !matches!(
                source.data_type(),
                DataType::Time64(_) | DataType::Timestamp(..)
            )
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation temporal source differs from its signed declaration",
            ));
        }
        self.temporal_match_source = Some(temporal_match_source_digest(source)?);
        Ok(self)
    }

    pub fn with_storage_type(
        mut self,
        storage: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        if self.temporal_match_source.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation temporal source cannot carry a write storage projection",
            ));
        }
        let input = self.execution_type.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation write input lacks its complete declaration",
            )
        })?;
        novarocks_connector_contract::validate_write_input_projection_observed(
            input,
            Some(&storage),
            || Ok::<_, ConnectorError>(()),
        )?;
        self.storage_type = Some(storage);
        Ok(self)
    }
    pub fn storage_type(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.storage_type.as_ref()
    }
    pub fn execution_type(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.execution_type.as_ref()
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub const fn target_ordinal(&self) -> u32 {
        self.target_ordinal
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorMutationEffectField {
    token: ConnectorWriteFieldToken,
    field: Field,
    target_ordinal: u32,
}

impl ConnectorMutationEffectField {
    pub fn try_new(
        token: ConnectorWriteFieldToken,
        field: Field,
        target_ordinal: u32,
    ) -> Result<Self, ConnectorError> {
        if field.data_type() != &DataType::Int8 || field.is_nullable() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation effect field must be non-null Int8",
            ));
        }
        Ok(Self {
            token,
            field,
            target_ordinal,
        })
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub const fn target_ordinal(&self) -> u32 {
        self.target_ordinal
    }
}

/// A provider-signed match layout. Identity and before/after values
/// are token-bound, not inferred from column names.
#[derive(Clone, Debug)]
pub struct ConnectorMutationMatchContract {
    owner: ConnectorProviderBindingKey,
    table: ConnectorTableHandle,
    base_version: ConnectorWriteBaseVersion,
    identity_fields: Vec<ConnectorMutationSourceField>,
    before_fields: Vec<ConnectorMutationTargetField>,
    after_fields: Vec<ConnectorMutationTargetField>,
    effect_field: ConnectorMutationEffectField,
}

impl ConnectorMutationMatchContract {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        table: ConnectorTableHandle,
        base_version: ConnectorWriteBaseVersion,
        identity_fields: Vec<ConnectorMutationSourceField>,
        before_fields: Vec<ConnectorMutationTargetField>,
        after_fields: Vec<ConnectorMutationTargetField>,
        effect_field: ConnectorMutationEffectField,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_observed(
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            effect_field,
            || Ok(()),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "observe the original match field roles"
    )]
    pub fn try_new_observed(
        owner: ConnectorProviderBindingKey,
        table: ConnectorTableHandle,
        base_version: ConnectorWriteBaseVersion,
        identity_fields: Vec<ConnectorMutationSourceField>,
        before_fields: Vec<ConnectorMutationTargetField>,
        after_fields: Vec<ConnectorMutationTargetField>,
        effect_field: ConnectorMutationEffectField,
        mut check_active: impl FnMut() -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        check_active()?;
        if table.owner() != &owner.instance_id || identity_fields.is_empty() {
            return Err(invalid_match(
                "row-mutation match contract needs an exact owner and identity",
            ));
        }
        base_version.validate()?;
        let count = identity_fields
            .len()
            .checked_add(before_fields.len())
            .and_then(|count| count.checked_add(after_fields.len()))
            .and_then(|count| count.checked_add(1))
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation match layout exceeds u32 match field ordinals",
                )
            })?;
        let mut tokens = HashSet::new();
        let mut ordinal = 0;
        let mut add = |token, supplied_ordinal| {
            check_active()?;
            if !tokens.insert(token) || supplied_ordinal != ordinal {
                return Err(invalid_match(
                    "row-mutation match fields require unique tokens and dense ordinals in role order",
                ));
            }
            ordinal += 1;
            Ok(())
        };
        let mut names = HashSet::new();
        for value in &identity_fields {
            add(value.token, value.source_ordinal)?;
            if !names.insert(value.field.name()) || value.execution_type().is_none() {
                return Err(invalid_match(
                    "row-mutation identity requires unique names and complete execution declarations",
                ));
            }
        }
        for (fields, is_before) in [(&before_fields, true), (&after_fields, false)] {
            names.clear();
            for value in fields {
                add(value.token, value.target_ordinal)?;
                if is_before && value.storage_type().is_some() {
                    return Err(invalid_match(
                        "row-mutation before fields cannot carry write storage projections",
                    ));
                }
                if !names.insert(value.field.name()) || value.execution_type().is_none() {
                    return Err(invalid_match(
                        "row-mutation target role requires unique names and complete execution declarations",
                    ));
                }
            }
        }
        add(effect_field.token, effect_field.target_ordinal)?;
        debug_assert_eq!(ordinal, count);
        check_active()?;
        Ok(Self {
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            effect_field,
        })
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }
    pub fn base_version(&self) -> &ConnectorWriteBaseVersion {
        &self.base_version
    }
    pub fn identity_fields(&self) -> &[ConnectorMutationSourceField] {
        &self.identity_fields
    }
    pub fn before_fields(&self) -> &[ConnectorMutationTargetField] {
        &self.before_fields
    }
    pub fn after_fields(&self) -> &[ConnectorMutationTargetField] {
        &self.after_fields
    }
    pub fn effect_field(&self) -> &ConnectorMutationEffectField {
        &self.effect_field
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorRowMutationStrategy {
    PositionDelete,
    DeletionVector,
    MergeOnRead,
    EqualityDelete,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorMutationRouteInput {
    token: ConnectorWriteFieldToken,
    input_ordinal: u32,
}

impl ConnectorMutationRouteInput {
    pub const fn new(token: ConnectorWriteFieldToken, input_ordinal: u32) -> Self {
        Self {
            token,
            input_ordinal,
        }
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub const fn input_ordinal(&self) -> u32 {
        self.input_ordinal
    }
}

/// An ordinary Current occurrence of the physical table admitted by a row mutation.
/// The provider projects the original current schema at the signed base snapshot.
#[derive(Clone, Debug)]
pub struct ConnectorRowMutationCurrent {
    identity: super::ConnectorTableIdentity,
    original_source: super::ConnectorProcessLocalPayload,
    target_ref: ConnectorWriteTargetRef,
    snapshot_id: Option<i64>,
}

fn projected_before_storage<'a>(
    before: &ConnectorMutationTargetField,
    after: &'a ConnectorMutationTargetField,
    mut check_active: impl FnMut() -> Result<(), ConnectorError>,
) -> Result<Option<&'a novarocks_type_contract::FunctionValueType>, ConnectorError> {
    let Some(storage) = after.storage_type() else {
        return Ok(None);
    };
    let (Some(before_type), Some(after_type)) = (before.execution_type(), after.execution_type())
    else {
        return Ok(None);
    };
    if after.field().name() != before.field().name()
        || after.field().data_type() != before.field().data_type()
    {
        return Ok(None);
    }
    Ok(before_type
        .typed_binding()
        .borrowed()
        .same_physical_binding_ignoring_nullability_observed(
            after_type.typed_binding().borrowed(),
            &mut check_active,
        )?
        .then_some(storage))
}

fn invalid_match(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

/// Process-local identity of one successful preparation issuance. A clone
/// keeps the allocation alive, so simultaneous issuances cannot alias.
#[derive(Clone)]
pub struct ConnectorRowMutationPreparationIdentity(Arc<IssuedRowMutationPreparation>);

struct IssuedRowMutationPreparation;

impl std::fmt::Debug for ConnectorRowMutationPreparationIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectorRowMutationPreparationIdentity")
    }
}

impl PartialEq for ConnectorRowMutationPreparationIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ConnectorRowMutationPreparationIdentity {}

impl ConnectorRowMutationCurrent {
    pub fn identity(&self) -> &super::ConnectorTableIdentity {
        &self.identity
    }

    pub fn original_source(&self) -> &super::ConnectorProcessLocalPayload {
        &self.original_source
    }

    pub fn target_ref(&self) -> &ConnectorWriteTargetRef {
        &self.target_ref
    }

    pub const fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
}

/// Provider-signed, pure planning result. The payload is opaque to Core and
/// must be consumed with this exact operation and provider generation.
#[derive(Clone)]
pub struct ConnectorRowMutationPreparation {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorWriteOperationId,
    table: ConnectorTableHandle,
    match_source: ConnectorTableHandle,
    match_source_schema: SchemaRef,
    match_source_schema_digest: [u8; 32],
    target_ref: ConnectorWriteTargetRef,
    intent: ConnectorRowMutationIntent,
    base_version: ConnectorWriteBaseVersion,
    match_contract: ConnectorMutationMatchContract,
    strategy: ConnectorRowMutationStrategy,
    base_version_ordinal: Option<i64>,
    payload: Bytes,
    identity: ConnectorRowMutationPreparationIdentity,
}

impl ConnectorRowMutationPreparation {
    #[expect(
        clippy::too_many_arguments,
        reason = "The public constructor validates all frozen preparation facts together before sealing the provider payload"
    )]
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableHandle,
        match_source: ConnectorTableHandle,
        match_source_schema: SchemaRef,
        target_ref: ConnectorWriteTargetRef,
        intent: ConnectorRowMutationIntent,
        base_version: ConnectorWriteBaseVersion,
        match_contract: ConnectorMutationMatchContract,
        strategy: ConnectorRowMutationStrategy,
        base_version_ordinal: Option<i64>,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_observed(
            owner,
            operation_id,
            table,
            match_source,
            match_source_schema,
            target_ref,
            intent,
            base_version,
            match_contract,
            strategy,
            base_version_ordinal,
            payload,
            || Ok(()),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "observe the original preparation facts before issuing an identity"
    )]
    pub fn try_new_observed(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableHandle,
        match_source: ConnectorTableHandle,
        match_source_schema: SchemaRef,
        target_ref: ConnectorWriteTargetRef,
        intent: ConnectorRowMutationIntent,
        base_version: ConnectorWriteBaseVersion,
        match_contract: ConnectorMutationMatchContract,
        strategy: ConnectorRowMutationStrategy,
        base_version_ordinal: Option<i64>,
        payload: Bytes,
        mut check_active: impl FnMut() -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        check_active()?;
        if table.owner() != &owner.instance_id
            || match_source.owner() != &owner.instance_id
            || match_contract.owner() != &owner
            || match_contract.table() != &table
            || match_contract.base_version() != &base_version
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation preparation does not match owner and table",
            ));
        }
        intent.validate()?;
        base_version.validate()?;
        let match_source_schema_digest =
            canonical_schema_digest_observed(&match_source_schema, &mut check_active)?;
        let mut source_by_name = HashMap::new();
        for field in match_source_schema.fields() {
            check_active()?;
            if source_by_name
                .insert(field.name().as_str(), field.as_ref())
                .is_some()
            {
                return Err(invalid_match(
                    "row-mutation match source schema repeats a field name",
                ));
            }
        }
        let mut after_by_name = HashMap::new();
        for field in match_contract.after_fields() {
            check_active()?;
            after_by_name.insert(field.field().name().as_str(), field);
        }
        for (expected, storage, before, temporal_source) in match_contract
            .identity_fields()
            .iter()
            .map(|field| (field.field(), None, None, None))
            .chain(match_contract.before_fields().iter().map(|field| {
                (
                    field.field(),
                    None,
                    Some(field),
                    field.temporal_match_source,
                )
            }))
            .chain(match_contract.after_fields().iter().map(|field| {
                (
                    field.field(),
                    field.storage_type(),
                    None,
                    field.temporal_match_source,
                )
            }))
        {
            check_active()?;
            let actual = source_by_name
                .get(expected.name().as_str())
                .ok_or_else(|| {
                    invalid_match("row-mutation match source omits a signed match field")
                })?;
            let storage = if let Some(before) = before
                && actual.data_type() != expected.data_type()
                && let Some(after) = after_by_name.get(before.field().name().as_str())
            {
                projected_before_storage(before, after, &mut check_active)?
            } else {
                storage
            };
            let matches = (!actual.is_nullable() || expected.is_nullable())
                && if let Some(source_digest) = temporal_source {
                    temporal_match_source_digest_observed(actual, &mut check_active)?
                        == source_digest
                } else if let Some(storage) = storage {
                    // The match source keeps formal storage. When a before
                    // field uses the same checked byte projection as the
                    // canonical input, the after field supplies the original
                    // storage proof for both roles.
                    novarocks_type_contract::value_layout_matches_carrier_observed::<
                        MutationFieldValidationFailure,
                    >(
                        storage.typed_binding().layout(),
                        actual.data_type(),
                        |_| check_active().map_err(MutationFieldValidationFailure::Caller),
                    )
                    .map_err(mutation_field_error)?
                } else {
                    actual.data_type() == expected.data_type()
                };
            if !matches {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation match source field differs from its signed match role",
                ));
            }
        }
        validate_payload(&payload)?;
        check_active()?;
        Ok(Self {
            owner,
            operation_id,
            table,
            match_source,
            match_source_schema,
            match_source_schema_digest,
            target_ref,
            intent,
            base_version,
            match_contract,
            strategy,
            base_version_ordinal,
            payload,
            identity: ConnectorRowMutationPreparationIdentity(Arc::new(
                IssuedRowMutationPreparation,
            )),
        })
    }

    pub fn current_source(
        &self,
        table: &super::ConnectorTableMetadata,
    ) -> Result<ConnectorRowMutationCurrent, ConnectorError> {
        if table.table != self.table || table.identity.instance_id != self.owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Current row mutation read requires the signed original table",
            ));
        }
        let original_source = table.source_read_facts().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Current row mutation read requires the original table observation",
            )
        })?;
        original_source.validate_owner(&self.owner)?;
        Ok(ConnectorRowMutationCurrent {
            identity: table.identity.clone(),
            original_source: original_source.clone(),
            target_ref: self.target_ref.clone(),
            snapshot_id: self.base_version_ordinal,
        })
    }

    /// Hand the provider its original signed preparation at write-session admission.
    pub fn admission(&self) -> super::write_stack::session::ConnectorRowMutationAdmission {
        super::ConnectorProcessLocalPayload::new(
            self.owner.clone(),
            std::sync::Arc::new(self.clone()),
        )
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }
    /// Exact provider-signed source handle for the SQL match phase. This may
    /// differ from [`Self::table`] when a named ref is not the provider's
    /// current/default ref.
    pub fn match_source(&self) -> &ConnectorTableHandle {
        &self.match_source
    }
    /// Exact provider-signed Arrow schema produced when scanning [`Self::table`]
    /// for the match phase of this admitted row mutation.
    pub fn match_source_schema(&self) -> &SchemaRef {
        &self.match_source_schema
    }
    pub const fn match_source_schema_digest(&self) -> [u8; 32] {
        self.match_source_schema_digest
    }
    /// Compare independently reprojected source content with the signed schema.
    pub fn validate_match_source_schema_observed(
        expected: [u8; 32],
        current: &Schema,
        mut check_active: impl FnMut() -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        if canonical_schema_digest_observed(current, &mut check_active)? != expected {
            return Err(invalid_match(
                "row-mutation match source schema differs from its signed contents",
            ));
        }
        Ok(())
    }
    pub fn target_ref(&self) -> &ConnectorWriteTargetRef {
        &self.target_ref
    }
    pub fn intent(&self) -> &ConnectorRowMutationIntent {
        &self.intent
    }
    pub fn match_contract(&self) -> &ConnectorMutationMatchContract {
        &self.match_contract
    }
    pub fn base_version(&self) -> &ConnectorWriteBaseVersion {
        &self.base_version
    }
    pub const fn strategy(&self) -> ConnectorRowMutationStrategy {
        self.strategy
    }
    /// A provider-supplied identifier for the target ref's base state, meant to
    /// be persisted by the application's durable DML journal and shown to
    /// operators.
    ///
    /// Core stores and echoes this value; it never compares it against provider
    /// internals, orders two of them, or derives read authority from it. Read
    /// authority remains [`Self::base_version`], which stays opaque. `None`
    /// means the target ref has no base state yet.
    pub const fn base_version_ordinal(&self) -> Option<i64> {
        self.base_version_ordinal
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub fn preparation_identity(&self) -> &ConnectorRowMutationPreparationIdentity {
        &self.identity
    }
}

#[derive(Clone)]
pub struct ConnectorRowMutationPreparationRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub table: ConnectorTableHandle,
    pub target_ref: ConnectorWriteTargetRef,
    pub intent: ConnectorRowMutationIntent,
    pub context: ConnectorRequestContext,
}

impl ConnectorRowMutationPreparationRequest {
    pub fn validate(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if self.table.owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation request table is foreign to the exact lease",
            ));
        }
        self.intent.validate()
    }
}

// Boxing the prepared variant would only move the cost behind a pointer on a
// control-plane value that is constructed once per row-mutation admission, and
// it would change a frozen SPI shape that providers and Core both match on.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum ConnectorRowMutationPreparationOutcome {
    Prepared(ConnectorRowMutationPreparation),
    Denied(ConnectorError),
}

fn validate_effects(
    effects: &[ConnectorRowMutationEffect],
    subject: &str,
) -> Result<(), ConnectorError> {
    if effects.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("{subject} requires at least one effect"),
        ));
    }
    let mut seen = HashSet::new();
    if effects.iter().any(|effect| !seen.insert(*effect)) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("{subject} contains duplicate effects"),
        ));
    }
    Ok(())
}

fn validate_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation provider payload exceeds hard limit",
        ));
    }
    Ok(())
}

fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
// The encoding describes only source content. The borrowed structural walk
// first checks the shared logical depth and Arrow encoding syntax, bounding the
// recursive encoder without a second source-specific resource quota.
struct CanonicalSchemaHash<'a, F> {
    hasher: Sha256,
    check_active: &'a mut F,
}

impl<F: FnMut() -> Result<(), ConnectorError>> CanonicalSchemaHash<'_, F> {
    fn metadata(&mut self, metadata: &arrow::datatypes::Metadata) -> Result<(), ConnectorError> {
        self.hasher.update((metadata.len() as u64).to_be_bytes());
        // Arrow Metadata iterates in canonical key order.
        for (key, value) in metadata {
            (self.check_active)()?;
            digest_bytes(&mut self.hasher, key.as_bytes());
            digest_bytes(&mut self.hasher, value.as_bytes());
        }
        Ok(())
    }

    fn root_field(&mut self, field: &Field) -> Result<(), ConnectorError> {
        novarocks_type_contract::validate_arrow_layout_structure_observed(
            field.data_type(),
            |_| (self.check_active)().map_err(MutationFieldValidationFailure::Caller),
        )
        .map_err(mutation_field_error)?;
        self.field(field)
    }

    fn field(&mut self, field: &Field) -> Result<(), ConnectorError> {
        (self.check_active)()?;
        digest_bytes(&mut self.hasher, field.name().as_bytes());
        self.hasher.update([u8::from(field.is_nullable())]);
        self.data_type(field.data_type())?;
        self.metadata(field.metadata())
    }

    fn data_type(&mut self, data_type: &DataType) -> Result<(), ConnectorError> {
        (self.check_active)()?;
        match data_type {
            DataType::FixedSizeBinary(width) | DataType::FixedSizeList(_, width) if *width < 0 => {
                return Err(invalid_match(
                    "row-mutation canonical schema cannot contain a negative fixed width",
                ));
            }
            DataType::Union(fields, _) if fields.is_empty() => {
                return Err(invalid_match(
                    "row-mutation canonical schema cannot contain an empty Union",
                ));
            }
            _ => {}
        }
        let tag = match data_type {
            DataType::Null => 0,
            DataType::Boolean => 1,
            DataType::Int8 => 2,
            DataType::Int16 => 3,
            DataType::Int32 => 4,
            DataType::Int64 => 5,
            DataType::UInt8 => 6,
            DataType::UInt16 => 7,
            DataType::UInt32 => 8,
            DataType::UInt64 => 9,
            DataType::Float16 => 10,
            DataType::Float32 => 11,
            DataType::Float64 => 12,
            DataType::Timestamp(_, _) => 13,
            DataType::Date32 => 14,
            DataType::Date64 => 15,
            DataType::Time32(_) => 16,
            DataType::Time64(_) => 17,
            DataType::Duration(_) => 18,
            DataType::Interval(_) => 19,
            DataType::Binary => 20,
            DataType::FixedSizeBinary(_) => 21,
            DataType::LargeBinary => 22,
            DataType::BinaryView => 23,
            DataType::Utf8 => 24,
            DataType::LargeUtf8 => 25,
            DataType::Utf8View => 26,
            DataType::List(_) => 27,
            DataType::ListView(_) => 28,
            DataType::FixedSizeList(_, _) => 29,
            DataType::LargeList(_) => 30,
            DataType::LargeListView(_) => 31,
            DataType::Struct(_) => 32,
            DataType::Union(_, _) => 33,
            DataType::Dictionary(_, value) => {
                // The validated dictionary key is a physical representation choice.
                return self.data_type(value);
            }
            DataType::Decimal32(_, _) => 35,
            DataType::Decimal64(_, _) => 36,
            DataType::Decimal128(_, _) => 37,
            DataType::Decimal256(_, _) => 38,
            DataType::Map(_, _) => 39,
            DataType::RunEndEncoded(_, _) => 40,
        };
        self.hasher.update([tag]);
        match data_type {
            DataType::Timestamp(unit, timezone) => {
                hash_time_unit(&mut self.hasher, *unit);
                if let Some(timezone) = timezone {
                    self.hasher.update([1]);
                    digest_bytes(&mut self.hasher, timezone.as_bytes());
                } else {
                    self.hasher.update([0]);
                }
            }
            DataType::Time32(unit) | DataType::Time64(unit) | DataType::Duration(unit) => {
                hash_time_unit(&mut self.hasher, *unit);
            }
            DataType::Interval(unit) => self.hasher.update([match unit {
                IntervalUnit::YearMonth => 1,
                IntervalUnit::DayTime => 2,
                IntervalUnit::MonthDayNano => 3,
            }]),
            DataType::FixedSizeBinary(size) => self.hasher.update(size.to_be_bytes()),
            DataType::List(child)
            | DataType::ListView(child)
            | DataType::LargeList(child)
            | DataType::LargeListView(child) => self.field(child)?,
            DataType::FixedSizeList(child, size) => {
                self.hasher.update(size.to_be_bytes());
                self.field(child)?;
            }
            DataType::Struct(fields) => {
                self.hasher.update((fields.len() as u64).to_be_bytes());
                for child in fields {
                    self.field(child)?;
                }
            }
            DataType::Union(fields, mode) => {
                self.hasher.update([match mode {
                    UnionMode::Sparse => 1,
                    UnionMode::Dense => 2,
                }]);
                self.hasher.update((fields.len() as u64).to_be_bytes());
                for (type_id, child) in fields.iter() {
                    if type_id < 0 {
                        return Err(invalid_match(
                            "row-mutation canonical schema cannot contain a negative Union type id",
                        ));
                    }
                    self.hasher.update(type_id.to_be_bytes());
                    self.field(child)?;
                }
            }
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale) => {
                self.hasher.update([*precision]);
                self.hasher.update(scale.to_be_bytes());
            }
            DataType::Map(entries, sorted) => {
                self.hasher.update([u8::from(*sorted)]);
                self.field(entries)?;
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.field(run_ends)?;
                self.field(values)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
fn canonical_schema_digest(schema: &Schema) -> Result<[u8; 32], ConnectorError> {
    canonical_schema_digest_observed(schema, &mut || Ok(()))
}

fn canonical_schema_digest_observed(
    schema: &Schema,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<[u8; 32], ConnectorError> {
    check_active()?;
    let mut state = CanonicalSchemaHash {
        hasher: Sha256::new(),
        check_active,
    };
    state
        .hasher
        .update(b"novarocks.connector-row-mutation-arrow-schema.v1\0");
    state.metadata(schema.metadata())?;
    state
        .hasher
        .update((schema.fields().len() as u64).to_be_bytes());
    for field in schema.fields() {
        state.root_field(field)?;
    }
    (state.check_active)()?;
    Ok(state.hasher.finalize().into())
}

fn temporal_match_source_digest(field: &Field) -> Result<[u8; 32], ConnectorError> {
    temporal_match_source_digest_observed(field, &mut || Ok(()))
}

fn temporal_match_source_digest_observed(
    field: &Field,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<[u8; 32], ConnectorError> {
    let mut state = CanonicalSchemaHash {
        hasher: Sha256::new(),
        check_active,
    };
    state
        .hasher
        .update(b"novarocks.connector-row-mutation-temporal-source-field.v1\0");
    state.root_field(field)?;
    (state.check_active)()?;
    Ok(state.hasher.finalize().into())
}

fn hash_time_unit(hasher: &mut Sha256, unit: TimeUnit) {
    hasher.update([match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn match_source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    #[test]
    fn canonical_schema_metadata_order_is_stable() {
        let first = Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([
                    ("z".to_owned(), "2".to_owned()),
                    ("a".to_owned(), "1".to_owned()),
                ])),
            ],
            HashMap::from([
                ("right".to_owned(), "r".to_owned()),
                ("left".to_owned(), "l".to_owned()),
            ]),
        );
        let second = Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([
                    ("a".to_owned(), "1".to_owned()),
                    ("z".to_owned(), "2".to_owned()),
                ])),
            ],
            HashMap::from([
                ("left".to_owned(), "l".to_owned()),
                ("right".to_owned(), "r".to_owned()),
            ]),
        );
        assert_eq!(
            canonical_schema_digest(&first).expect("first"),
            canonical_schema_digest(&second).expect("second")
        );
        // Independently authored canonical v1 bytes, including both metadata scopes.
        assert_eq!(
            canonical_schema_digest(&first).unwrap(),
            [
                0x4d, 0xaf, 0xb1, 0xf2, 0x8c, 0x6c, 0xac, 0x24, 0xb9, 0xe8, 0xa5, 0x80, 0xda, 0x02,
                0x75, 0xf7, 0xca, 0x12, 0xa9, 0x6e, 0xd0, 0x4a, 0x24, 0x90, 0x48, 0x0c, 0xe6, 0x70,
                0x4b, 0xbc, 0x3d, 0x59
            ]
        );
    }

    /// Minimal signed preparation whose only variable is the application-facing
    /// base version ordinal.
    fn preparation_with_base_version_ordinal(
        base_version_ordinal: Option<i64>,
    ) -> ConnectorRowMutationPreparation {
        let instance_id =
            super::super::ConnectorInstanceId::parse("iceberg").expect("valid instance ID");
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: super::super::ProviderBindingEpoch::from_bytes([7u8; 16]),
        };
        let table = ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
            .expect("table");
        let base_version =
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).expect("base version");
        let identity_token = ConnectorWriteFieldToken::from_bytes([1u8; 32]);
        let effect_token = ConnectorWriteFieldToken::from_bytes([2u8; 32]);
        let match_contract = ConnectorMutationMatchContract::try_new(
            owner.clone(),
            table.clone(),
            base_version.clone(),
            vec![
                ConnectorMutationSourceField::try_new_typed(
                    identity_token,
                    Field::new("id", DataType::Int64, false),
                    0,
                    super::typed_execution_fields_tests::value(
                        novarocks_type_contract::ValueKind::Scalar(DataType::Int64),
                        DataType::Int64,
                        false,
                    ),
                )
                .expect("explicit BIGINT mutation fixture"),
            ],
            Vec::new(),
            Vec::new(),
            ConnectorMutationEffectField::try_new(
                effect_token,
                Field::new("effect", DataType::Int8, false),
                1,
            )
            .expect("effect field"),
        )
        .expect("match contract");

        ConnectorRowMutationPreparation::try_new(
            owner,
            ConnectorWriteOperationId::from_bytes([8; 16]),
            table.clone(),
            table,
            match_source_schema(),
            ConnectorWriteTargetRef::main(),
            ConnectorRowMutationIntent::Delete,
            base_version,
            match_contract,
            ConnectorRowMutationStrategy::PositionDelete,
            base_version_ordinal,
            Bytes::from_static(b"payload"),
        )
        .expect("preparation")
    }

    #[test]
    fn match_source_keeps_read_storage_for_canonical_after_input() {
        use novarocks_type_contract::FunctionValueType;
        let prepare = |source: Field, input: DataType, storage: DataType| {
            let seed = preparation_with_base_version_ordinal(None);
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                source.clone(),
            ]));
            let contract = ConnectorMutationMatchContract::try_new(
                seed.owner.clone(),
                seed.table.clone(),
                seed.base_version.clone(),
                seed.match_contract.identity_fields.clone(),
                vec![
                    ConnectorMutationTargetField::try_new_typed(
                        ConnectorWriteFieldToken::from_bytes([3; 32]),
                        source.clone(),
                        1,
                        FunctionValueType::new(source.data_type().clone(), source.is_nullable()),
                    )
                    .unwrap(),
                ],
                vec![
                    ConnectorMutationTargetField::try_new_typed(
                        ConnectorWriteFieldToken::from_bytes([4; 32]),
                        Field::new("value", input.clone(), false),
                        2,
                        FunctionValueType::new(input, false),
                    )
                    .unwrap()
                    .with_storage_type(FunctionValueType::new(storage, false))
                    .unwrap(),
                ],
                ConnectorMutationEffectField::try_new(
                    ConnectorWriteFieldToken::from_bytes([5; 32]),
                    Field::new("effect", DataType::Int8, false),
                    3,
                )
                .unwrap(),
            )
            .unwrap();
            ConnectorRowMutationPreparation::try_new(
                seed.owner,
                seed.operation_id,
                seed.table,
                seed.match_source,
                schema,
                seed.target_ref,
                ConnectorRowMutationIntent::Update,
                seed.base_version,
                contract,
                ConnectorRowMutationStrategy::MergeOnRead,
                None,
                seed.payload,
            )
        };
        let source = Field::new("value", DataType::FixedSizeBinary(4), false);
        let valid = prepare(
            source.clone(),
            DataType::Binary,
            DataType::FixedSizeBinary(4),
        )
        .unwrap();
        assert_eq!(
            valid.match_source_schema().field(1).data_type(),
            &DataType::FixedSizeBinary(4)
        );
        assert_eq!(
            valid.match_contract().after_fields()[0].field().data_type(),
            &DataType::Binary
        );
        for domain in [
            novarocks_type_contract::ValueLogicalType::Physical,
            novarocks_type_contract::ValueLogicalType::Hll,
        ] {
            let before = ConnectorMutationTargetField::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([3; 32]),
                Field::new("value", DataType::Binary, true),
                1,
                FunctionValueType::try_with_logical_type(DataType::Binary, true, domain).unwrap(),
            )
            .unwrap();
            assert_eq!(
                projected_before_storage(
                    &before,
                    &valid.match_contract().after_fields()[0],
                    || Ok(())
                )
                .unwrap()
                .is_some(),
                domain == novarocks_type_contract::ValueLogicalType::Physical,
            );
        }
        for wrong_source in [
            Field::new("value", DataType::Binary, false),
            Field::new("value", DataType::FixedSizeBinary(8), false),
            source.with_nullable(true),
        ] {
            let error = prepare(wrong_source, DataType::Binary, DataType::FixedSizeBinary(4))
                .err()
                .expect("wrong original read source must be rejected");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(
                error.message(),
                "row-mutation match source field differs from its signed match role"
            );
        }
        let nested = |carrier, nullable| {
            DataType::Struct(
                vec![Arc::new(
                    Field::new("original-child", carrier, nullable).with_metadata(HashMap::from([
                        ("PARQUET:field_id".into(), "71".into()),
                        ("source.attribute".into(), "opaque".into()),
                    ])),
                )]
                .into(),
            )
        };
        let read = nested(DataType::FixedSizeBinary(4), false);
        let input = nested(DataType::Binary, false);
        let raw = Field::new("value", read.clone(), false);
        let valid = prepare(raw.clone(), input.clone(), read.clone()).unwrap();
        assert_eq!(valid.match_source_schema().field(1), &raw);
        assert!(
            prepare(
                Field::new("value", nested(DataType::FixedSizeBinary(4), true), false),
                input,
                read
            )
            .is_err()
        );
    }

    #[test]
    fn preparation_identity_is_shared_by_clones_and_unique_per_successful_issuance() {
        let absent = preparation_with_base_version_ordinal(None);
        assert_eq!(absent.base_version_ordinal(), None);
        let present = preparation_with_base_version_ordinal(Some(41));
        let independently_issued = preparation_with_base_version_ordinal(Some(41));
        assert_eq!(present.base_version_ordinal(), Some(41));
        assert_eq!(present.operation_id(), independently_issued.operation_id());
        assert_eq!(
            present.preparation_identity(),
            present.clone().preparation_identity()
        );
        assert_ne!(
            present.preparation_identity(),
            independently_issued.preparation_identity()
        );
        assert_eq!(
            present.match_source_schema_digest(),
            independently_issued.match_source_schema_digest()
        );
    }

    #[test]
    fn canonical_schema_keeps_logical_depth_and_normalizes_dictionary_keys() {
        let depth = novarocks_type_contract::MAX_VALUE_TYPE_DEPTH;
        let schema = |ty| Schema::new(vec![Field::new("value", ty, true)]);
        let chain =
            |wrap: fn(DataType) -> DataType| (1..depth).fold(DataType::Int64, |ty, _| wrap(ty));
        let list = |ty| DataType::List(Arc::new(Field::new("item", ty, true)));
        let map = |ty| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false),
                            Field::new("value", ty, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        for wrap in [list as fn(DataType) -> DataType, map] {
            let within = chain(wrap);
            let expected = canonical_schema_digest(&schema(within.clone())).unwrap();
            for key in [DataType::Int8, DataType::Int16] {
                let encoded = DataType::Dictionary(Box::new(key), Box::new(within.clone()));
                assert_eq!(canonical_schema_digest(&schema(encoded)).unwrap(), expected);
            }
            let error = canonical_schema_digest(&schema(wrap(within))).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert!(error.message().contains(&depth.to_string()));
        }
        for invalid in [
            DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int64)),
            DataType::FixedSizeBinary(-1),
            DataType::Union(arrow::datatypes::UnionFields::empty(), UnionMode::Sparse),
        ] {
            assert_eq!(
                canonical_schema_digest(&schema(invalid))
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
        assert!(
            canonical_schema_digest(&schema(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Int64, true)),
                i32::MAX
            )))
            .is_ok()
        );
    }

    #[test]
    fn canonical_schema_uses_actual_wide_occurrences_and_source_metadata() {
        let field = Arc::new(Field::new("value", DataType::Int64, true));
        let wide = Schema::new(vec![field.clone(); 16_385]);
        let digest = canonical_schema_digest(&wide).unwrap();
        let mut fields = wide.fields().to_vec();
        fields[16_384] = Arc::new(
            field
                .as_ref()
                .clone()
                .with_metadata([("k".to_owned(), "v".to_owned())]),
        );
        assert_ne!(
            canonical_schema_digest(&Schema::new(fields)).unwrap(),
            digest
        );
    }

    #[test]
    fn source_schema_and_temporal_signatures_preserve_distinct_attributes() {
        let base = Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )
        .with_metadata([("PARQUET:field_id".to_string(), "17".to_string())]);
        let expected = temporal_match_source_digest(&base).unwrap();
        let mut changed = base
            .clone()
            .with_metadata([("PARQUET:field_id".to_string(), "18".to_string())]);
        assert_ne!(temporal_match_source_digest(&changed).unwrap(), expected);
        changed = base.clone().with_data_type(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        assert_ne!(temporal_match_source_digest(&changed).unwrap(), expected);
        assert_ne!(
            temporal_match_source_digest(&base.clone().with_nullable(false)).unwrap(),
            expected
        );
        assert_ne!(
            canonical_schema_digest(&Schema::new(vec![base])).unwrap(),
            expected
        );
    }

    #[test]
    fn merge_rejects_empty_or_duplicate_effects() {
        assert_eq!(
            ConnectorRowMutationIntent::Merge { effects: vec![] }
                .validate()
                .expect_err("empty")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorRowMutationIntent::Merge {
                effects: vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Delete
                ]
            }
            .validate()
            .expect_err("duplicate")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}

#[cfg(test)]
mod typed_execution_fields_tests {
    use super::*;
    use novarocks_type_contract::{
        FunctionValueType, TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType,
        ValueTypeRef, ValueTypeStorageLifetime,
    };
    use std::sync::Arc;
    pub(super) fn value(kind: ValueKind, carrier: DataType, nullable: bool) -> FunctionValueType {
        let storage = ValueTypeStorageLifetime::pure();
        FunctionValueType::try_from_typed_binding(
            TypedValueBinding::try_new(
                ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap(),
                ValueLayout::try_new(&carrier, storage).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn mutation_field_keeps_explicit_nested_json_and_opaque_source_attributes() {
        let item = value(
            ValueKind::Nominal(ValueLogicalType::Json),
            DataType::LargeBinary,
            false,
        );
        let carrier = DataType::List(Arc::new(Field::new(
            "item",
            item.data_type().clone(),
            item.nullable(),
        )));
        let declared = value(
            ValueKind::List(item.value_type().clone()),
            carrier.clone(),
            true,
        );
        let source = Field::new("document", carrier, true).with_metadata(HashMap::from([(
            "nr.logical.type".into(),
            "uninterpreted provider attribute".into(),
        )]));
        let token = ConnectorWriteFieldToken::from_bytes([2; 32]);
        let field =
            ConnectorMutationTargetField::try_new_typed(token, source.clone(), 7, declared.clone())
                .unwrap();
        assert_eq!(field.token(), token);
        assert_eq!(field.target_ordinal(), 7);
        assert_eq!(field.field(), &source);
        assert_eq!(field.execution_type(), Some(&declared));
        let ordinary = value(ValueKind::String, DataType::Utf8, false);
        let marker = Field::new("identity", DataType::Utf8, false)
            .with_metadata(HashMap::from([("nr.logical.type".into(), "JSON".into())]));
        let field =
            ConnectorMutationSourceField::try_new_typed(token, marker.clone(), 0, ordinary.clone())
                .unwrap();
        assert_eq!(field.field(), &marker);
        assert_eq!(
            field.execution_type(),
            Some(&ordinary),
            "source attributes author no domain"
        );
    }
    #[test]
    fn mutation_field_rejects_exact_layout_and_child_nullability_drift() {
        let token = ConnectorWriteFieldToken::from_bytes([3; 32]);
        let child = value(
            ValueKind::Nominal(ValueLogicalType::Json),
            DataType::LargeBinary,
            false,
        );
        let declared = value(
            ValueKind::List(child.value_type().clone()),
            DataType::List(Arc::new(Field::new(
                "item",
                child.data_type().clone(),
                child.nullable(),
            ))),
            true,
        );
        let widened = Field::new(
            "document",
            DataType::List(Arc::new(Field::new(
                "item",
                child.data_type().clone(),
                true,
            ))),
            true,
        );
        assert_eq!(
            ConnectorMutationTargetField::try_new_typed(token, widened, 0, declared)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let declared = value(ValueKind::Scalar(DataType::Int64), DataType::Int64, false);
        assert_eq!(
            ConnectorMutationSourceField::try_new_typed(
                token,
                Field::new("id", DataType::Int32, false),
                0,
                declared.clone()
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorMutationSourceField::try_new_typed(
                token,
                Field::new("id", DataType::Int64, true),
                0,
                declared
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
    #[test]
    fn mutation_field_first_original_control_refusal_has_no_tail() {
        let token = ConnectorWriteFieldToken::from_bytes([4; 32]);
        let declared = value(ValueKind::Scalar(DataType::Int64), DataType::Int64, false);
        let mut calls = 0;
        let failure = ConnectorMutationSourceField::try_new_typed_observed(
            token,
            Field::new("id", DataType::Int64, false),
            0,
            declared,
            |_| {
                calls += 1;
                assert_eq!(calls, 1, "callback after first refusal");
                Err(ConnectorError::new(
                    ConnectorErrorKind::Cancelled,
                    "original mutation declaration control",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(failure.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(failure.message(), "original mutation declaration control");
    }
}

#[cfg(test)]
mod typed_match_contract_tests {
    use super::typed_execution_fields_tests::value;
    use super::*;
    use novarocks_type_contract::{FunctionValueType, ValueKind, ValueLogicalType};
    use std::sync::Arc;

    fn contract(
        identity_type: FunctionValueType,
        before_type: FunctionValueType,
        after_type: FunctionValueType,
        after_attributes: HashMap<String, String>,
    ) -> ConnectorMutationMatchContract {
        let instance = super::super::ConnectorInstanceId::parse("iceberg").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance.clone(),
            incarnation: super::super::ProviderBindingEpoch::from_bytes([8; 16]),
        };
        let identity = ConnectorWriteFieldToken::from_bytes([1; 32]);
        ConnectorMutationMatchContract::try_new(
            owner,
            ConnectorTableHandle::try_new(instance, Bytes::from_static(b"table")).unwrap(),
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap(),
            vec![
                ConnectorMutationSourceField::try_new_typed(
                    identity,
                    Field::new(
                        "id",
                        identity_type.data_type().clone(),
                        identity_type.nullable(),
                    ),
                    0,
                    identity_type,
                )
                .unwrap(),
            ],
            vec![
                ConnectorMutationTargetField::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    Field::new(
                        "before",
                        before_type.data_type().clone(),
                        before_type.nullable(),
                    ),
                    1,
                    before_type,
                )
                .unwrap(),
            ],
            vec![
                ConnectorMutationTargetField::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([3; 32]),
                    Field::new(
                        "after",
                        after_type.data_type().clone(),
                        after_type.nullable(),
                    )
                    .with_metadata(after_attributes),
                    2,
                    after_type,
                )
                .unwrap(),
            ],
            ConnectorMutationEffectField::try_new(
                ConnectorWriteFieldToken::from_bytes([4; 32]),
                Field::new("effect", DataType::Int8, false),
                3,
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn text() -> FunctionValueType {
        value(ValueKind::String, DataType::Utf8, false)
    }
    fn binary() -> FunctionValueType {
        value(ValueKind::Binary, DataType::LargeBinary, false)
    }
    fn json() -> FunctionValueType {
        value(
            ValueKind::Nominal(ValueLogicalType::Json),
            DataType::LargeBinary,
            false,
        )
    }
    #[test]
    fn match_identity_covers_same_carrier_domain_in_each_declared_role() {
        let baseline = contract(binary(), binary(), binary(), HashMap::new());
        for (role, changed) in [
            contract(json(), binary(), binary(), HashMap::new()),
            contract(binary(), json(), binary(), HashMap::new()),
            contract(binary(), binary(), json(), HashMap::new()),
        ]
        .into_iter()
        .enumerate()
        {
            // Arrow names, NULL promises, attrs and carriers are identical.
            assert_eq!(
                baseline.identity_fields()[0].field(),
                changed.identity_fields()[0].field()
            );
            assert_eq!(
                baseline.before_fields()[0].field(),
                changed.before_fields()[0].field()
            );
            assert_eq!(
                baseline.after_fields()[0].field(),
                changed.after_fields()[0].field()
            );
            let declarations = [
                changed.identity_fields()[0].execution_type(),
                changed.before_fields()[0].execution_type(),
                changed.after_fields()[0].execution_type(),
            ];
            assert_eq!(
                declarations[role].unwrap().logical_type(),
                ValueLogicalType::Json
            );
            assert_eq!(
                declarations
                    .iter()
                    .filter(|value| value.unwrap().logical_type() == ValueLogicalType::Json)
                    .count(),
                1
            );
        }
    }
    #[test]
    fn match_identity_distinguishes_uuid_from_largeint_without_source_markers() {
        let uuid = value(
            ValueKind::Nominal(ValueLogicalType::Uuid),
            DataType::FixedSizeBinary(16),
            false,
        );
        let largeint = value(
            ValueKind::Nominal(ValueLogicalType::LargeInt),
            DataType::FixedSizeBinary(16),
            false,
        );
        let first = contract(text(), text(), uuid, HashMap::new());
        let second = contract(text(), text(), largeint, HashMap::new());
        assert_eq!(
            first.after_fields()[0].field(),
            second.after_fields()[0].field()
        );
        assert!(first.after_fields()[0].field().metadata().is_empty());
        assert_ne!(
            first.after_fields()[0].execution_type(),
            second.after_fields()[0].execution_type()
        );
    }
    #[test]
    fn match_identity_reads_complete_nested_binding_and_is_owner_independent() {
        let nested = |child: FunctionValueType| {
            value(
                ValueKind::List(child.value_type().clone()),
                DataType::List(Arc::new(Field::new(
                    "item",
                    child.data_type().clone(),
                    child.nullable(),
                ))),
                true,
            )
        };
        let first_type = nested(json());
        let second_type = nested(json());
        let (DataType::List(first_item), DataType::List(second_item)) =
            (first_type.data_type(), second_type.data_type())
        else {
            panic!("declared List carrier");
        };
        assert!(!Arc::ptr_eq(first_item, second_item));
        let first = contract(text(), text(), first_type, HashMap::new());
        let independently_equal = contract(text(), text(), second_type, HashMap::new());
        let binary_child = contract(text(), text(), nested(binary()), HashMap::new());
        assert_eq!(
            first.after_fields()[0].field(),
            binary_child.after_fields()[0].field()
        );
        assert_eq!(
            first.after_fields()[0].execution_type(),
            independently_equal.after_fields()[0].execution_type()
        );
        assert_ne!(
            first.after_fields()[0].execution_type(),
            binary_child.after_fields()[0].execution_type()
        );
    }
    #[test]
    fn match_source_attributes_and_schema_identity_stay_separate() {
        let first = contract(
            text(),
            text(),
            json(),
            HashMap::from([("opaque".into(), "first".into())]),
        );
        let second = contract(
            text(),
            text(),
            json(),
            HashMap::from([("opaque".into(), "second".into())]),
        );
        assert_eq!(
            first.after_fields()[0].execution_type(),
            second.after_fields()[0].execution_type()
        );
        assert_ne!(
            canonical_schema_digest(&Schema::new(vec![first.after_fields()[0].field().clone()]))
                .unwrap(),
            canonical_schema_digest(&Schema::new(vec![second.after_fields()[0].field().clone()]))
                .unwrap(),
        );
        let schema = Schema::new(vec![Field::new("after", DataType::LargeBinary, false)]);
        assert_eq!(
            canonical_schema_digest(&schema).unwrap(),
            canonical_schema_digest(&Schema::new(vec![
                first.after_fields()[0]
                    .field()
                    .clone()
                    .with_metadata(HashMap::new())
            ]))
            .unwrap()
        );
    }
    #[test]
    fn match_identity_refuses_missing_declaration_in_tests_and_production() {
        let mut typed = contract(text(), text(), json(), HashMap::new());
        typed.identity_fields[0].execution_type = None;
        let failure = ConnectorMutationMatchContract::try_new(
            typed.owner,
            typed.table,
            typed.base_version,
            typed.identity_fields,
            typed.before_fields,
            typed.after_fields,
            typed.effect_field,
        )
        .unwrap_err();
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
