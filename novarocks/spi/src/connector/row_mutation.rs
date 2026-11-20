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

#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub use novarocks_connector_contract::{ConnectorRowMutationEffect, ConnectorWriteRouteId};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteFieldToken,
    ConnectorWriteOperationId, ConnectorWriteTargetRef, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
};

mod match_bounds;
#[cfg(test)]
mod match_bounds_tests;
#[cfg(test)]
thread_local! { static MATCH_LAYOUT_ALLOCATION_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

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
    .map_err(|error| match error {
        MutationFieldValidationFailure::Caller(error) => error,
        MutationFieldValidationFailure::Value(error) => {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        }
    })?;
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
        })
    }
    pub fn with_storage_type(
        mut self,
        storage: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
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
    digest: [u8; 32],
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
        let digest = Self::validate_match_layout(
            &owner,
            &table,
            &base_version,
            &identity_fields,
            &before_fields,
            &after_fields,
            &effect_field,
        )?;
        Ok(Self {
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            effect_field,
            digest,
        })
    }

    #[allow(clippy::ptr_arg)]
    fn validate_match_layout(
        owner: &ConnectorProviderBindingKey,
        table: &ConnectorTableHandle,
        base_version: &ConnectorWriteBaseVersion,
        identity_fields: &Vec<ConnectorMutationSourceField>,
        before_fields: &Vec<ConnectorMutationTargetField>,
        after_fields: &Vec<ConnectorMutationTargetField>,
        effect_field: &ConnectorMutationEffectField,
    ) -> Result<[u8; 32], ConnectorError> {
        if table.owner() != &owner.instance_id || identity_fields.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation match contract needs an exact owner and identity",
            ));
        }
        match_bounds::preflight(identity_fields, before_fields, after_fields, effect_field)?;
        base_version.validate()?;
        if before_fields
            .iter()
            .any(|field| field.storage_type().is_some())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation before fields cannot carry write storage projections",
            ));
        }
        #[cfg(test)]
        MATCH_LAYOUT_ALLOCATION_CALLS.with(|n| n.set(n.get() + 1));
        let mut tokens = HashSet::new();
        let mut field_ordinals = HashSet::new();
        for value in identity_fields {
            if !tokens.insert(value.token) || !field_ordinals.insert(value.source_ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation identity has duplicate token or source ordinal",
                ));
            }
        }
        for value in before_fields.iter().chain(after_fields) {
            if !tokens.insert(value.token) || !field_ordinals.insert(value.target_ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation target field has duplicate token or match field ordinal",
                ));
            }
        }
        if !tokens.insert(effect_field.token) || !field_ordinals.insert(effect_field.target_ordinal)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation effect field conflicts with the match layout",
            ));
        }
        if field_ordinals.len() != tokens.len()
            || !(0..u32::try_from(field_ordinals.len()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation match layout exceeds u32 match field ordinals",
                )
            })?)
                .all(|ordinal| field_ordinals.contains(&ordinal))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation match layout match field ordinals must be dense from zero",
            ));
        }
        match_digest(
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            effect_field,
        )
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::validate_match_layout(
            &self.owner,
            &self.table,
            &self.base_version,
            &self.identity_fields,
            &self.before_fields,
            &self.after_fields,
            &self.effect_field,
        )?;
        if expected != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation match contract digest does not match contents",
            ));
        }
        Ok(())
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

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
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
    digest: [u8; 32],
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
        match_contract.validate()?;
        let match_source_schema_digest = canonical_schema_digest(match_source_schema.as_ref())?;
        for (expected, storage) in match_contract
            .identity_fields()
            .iter()
            .map(|field| (field.field(), None))
            .chain(
                match_contract
                    .before_fields()
                    .iter()
                    .map(|field| (field.field(), None)),
            )
            .chain(
                match_contract
                    .after_fields()
                    .iter()
                    .map(|field| (field.field(), field.storage_type())),
            )
        {
            let mut matching = match_source_schema
                .fields()
                .iter()
                .filter(|actual| actual.name() == expected.name());
            let actual = matching.next().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation match source omits a signed match field",
                )
            })?;
            let matches = matching.next().is_none()
                && !(actual.is_nullable() && !expected.is_nullable())
                && if let Some(storage) = storage {
                    // The match source remains ReadT; an after field carries
                    // canonical W independently. Borrow the original layout
                    // matcher so raw source attributes never become normalized
                    // execution attributes or inferred logical domains.
                    novarocks_type_contract::value_layout_matches_carrier_observed::<
                        novarocks_type_contract::ValueTypeError,
                    >(
                        storage.typed_binding().layout(),
                        actual.data_type(),
                        |_| Ok(()),
                    )
                    .map_err(|error| {
                        ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
                    })?
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
        let digest = preparation_digest(
            &owner,
            operation_id,
            &table,
            &match_source,
            match_source_schema_digest,
            &target_ref,
            &intent,
            &base_version,
            &match_contract,
            strategy,
            base_version_ordinal,
            &payload,
        );
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
            digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.owner.clone(),
            self.operation_id,
            self.table.clone(),
            self.match_source.clone(),
            self.match_source_schema.clone(),
            self.target_ref.clone(),
            self.intent.clone(),
            self.base_version.clone(),
            self.match_contract.clone(),
            self.strategy,
            self.base_version_ordinal,
            self.payload.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation preparation digest does not match contents",
            ));
        }
        Ok(())
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
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
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
fn digest_owner(hasher: &mut Sha256, owner: &ConnectorProviderBindingKey) {
    digest_bytes(hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
}
// Every argument is a distinct field of the digest this function seals. Folding
// them into a struct would add a type whose only purpose is to be destructured
// again here, and would let a caller build a partially-populated digest input.
fn match_digest(
    owner: &ConnectorProviderBindingKey,
    table: &ConnectorTableHandle,
    base: &ConnectorWriteBaseVersion,
    identity: &[ConnectorMutationSourceField],
    before: &[ConnectorMutationTargetField],
    after: &[ConnectorMutationTargetField],
    effect: &ConnectorMutationEffectField,
) -> Result<[u8; 32], ConnectorError> {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-match.v3\0");
    digest_owner(&mut hasher, owner);
    digest_bytes(&mut hasher, table.payload());
    hasher.update(base.digest());
    for field in identity {
        hasher.update(field.token.to_bytes());
        hasher.update(field.source_ordinal.to_be_bytes());
        match_bounds::digest_debug_field(&mut hasher, &field.field)?;
        digest_match_execution_type(&mut hasher, field.execution_type())?;
    }
    for field in before.iter().chain(after) {
        hasher.update(field.token.to_bytes());
        hasher.update(field.target_ordinal.to_be_bytes());
        match_bounds::digest_debug_field(&mut hasher, &field.field)?;
        digest_match_execution_type(&mut hasher, field.execution_type())?;
        novarocks_connector_contract::write_input_projection_identity_observed(
            field.execution_type(),
            field.storage_type(),
            |part| {
                hasher.update(part);
                Ok::<_, ConnectorError>(())
            },
        )?;
    }
    hasher.update(effect.token.to_bytes());
    hasher.update(effect.target_ordinal.to_be_bytes());
    match_bounds::digest_debug_field(&mut hasher, &effect.field)?;
    Ok(hasher.finalize().into())
}
fn digest_match_execution_type(
    hasher: &mut Sha256,
    execution_type: Option<&novarocks_type_contract::FunctionValueType>,
) -> Result<(), ConnectorError> {
    match execution_type {
        Some(value) => {
            hasher.update(b"complete-execution-binding\0");
            match_bounds::digest_execution_type(hasher, value)
        }
        None => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation match field lacks its complete execution declaration",
        )),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The digest covers each sealed preparation field in canonical order before the record is constructed"
)]
fn preparation_digest(
    owner: &ConnectorProviderBindingKey,
    operation: ConnectorWriteOperationId,
    table: &ConnectorTableHandle,
    match_source: &ConnectorTableHandle,
    match_source_schema_digest: [u8; 32],
    target_ref: &ConnectorWriteTargetRef,
    intent: &ConnectorRowMutationIntent,
    base: &ConnectorWriteBaseVersion,
    contract: &ConnectorMutationMatchContract,
    strategy: ConnectorRowMutationStrategy,
    base_version_ordinal: Option<i64>,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-preparation.v3\0");
    digest_owner(&mut hasher, owner);
    hasher.update(operation.to_bytes());
    digest_bytes(&mut hasher, table.payload());
    digest_bytes(&mut hasher, match_source.payload());
    hasher.update(match_source_schema_digest);
    digest_bytes(&mut hasher, target_ref.as_str().as_bytes());
    match intent {
        ConnectorRowMutationIntent::Delete => hasher.update([1]),
        ConnectorRowMutationIntent::Update => hasher.update([2]),
        ConnectorRowMutationIntent::Merge { effects } => {
            hasher.update([3]);
            for effect in effects {
                hasher.update([effect_tag(*effect)]);
            }
        }
    };
    hasher.update(base.digest());
    hasher.update(contract.digest());
    hasher.update([strategy_tag(strategy)]);
    match base_version_ordinal {
        None => hasher.update([0]),
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
    }
    digest_bytes(&mut hasher, payload);
    hasher.finalize().into()
}

// The match-source signature is still consumed by MOR preparation. Keep its
// original schema shape and retained-metadata boundaries in the hash author;
// RowConverter construction, synthetic NULL and encoded-row costs are retired.
struct CanonicalSchemaHash {
    hasher: Sha256,
    retained_bytes: usize,
    types: usize,
}

impl CanonicalSchemaHash {
    fn exhausted() -> ConnectorError {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation canonical schema exceeds its bounded source structure",
        )
    }

    fn charge(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        self.retained_bytes = self
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(Self::exhausted)?;
        if self.retained_bytes > match_bounds::MAX_CANONICAL_SCHEMA_BYTES {
            return Err(Self::exhausted());
        }
        Ok(())
    }

    fn slots<T>(&mut self, count: usize) -> Result<(), ConnectorError> {
        self.charge(
            std::mem::size_of::<T>()
                .checked_mul(count)
                .ok_or_else(Self::exhausted)?,
        )
    }

    fn metadata(
        &mut self,
        metadata: &arrow::datatypes::Metadata,
        emit: bool,
    ) -> Result<(), ConnectorError> {
        self.charge(
            novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
                metadata.len(),
            )
            .map_err(|_| Self::exhausted())?,
        )?;
        if emit {
            self.hasher.update((metadata.len() as u64).to_be_bytes());
        }
        // Arrow 60 Metadata already iterates in canonical key order.
        for (key, value) in metadata {
            self.charge(key.capacity())?;
            self.charge(value.capacity())?;
            if emit {
                digest_bytes(&mut self.hasher, key.as_bytes());
                digest_bytes(&mut self.hasher, value.as_bytes());
            }
        }
        Ok(())
    }

    fn field(&mut self, field: &Field, depth: usize, emit: bool) -> Result<(), ConnectorError> {
        self.charge(std::mem::size_of::<Field>() + 2 * std::mem::size_of::<usize>())?;
        self.charge(field.name().capacity())?;
        if emit {
            digest_bytes(&mut self.hasher, field.name().as_bytes());
            self.hasher.update([u8::from(field.is_nullable())]);
        }
        self.data_type(field.data_type(), depth, emit)?;
        self.metadata(field.metadata(), emit)
    }

    fn data_type(
        &mut self,
        data_type: &DataType,
        depth: usize,
        emit: bool,
    ) -> Result<(), ConnectorError> {
        // These are the existing source-schema limits, independent of any
        // RowConverter workspace or actual row population.
        if depth >= 64 || self.types >= 4096 {
            return Err(Self::exhausted());
        }
        self.types += 1;
        self.charge(std::mem::size_of::<DataType>())?;
        match data_type {
            DataType::FixedSizeBinary(width) | DataType::FixedSizeList(_, width) if *width < 0 => {
                return Err(Self::exhausted());
            }
            DataType::Union(fields, _) if fields.is_empty() => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
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
            DataType::Dictionary(key, value) => {
                // Key representation is not part of this logical signature,
                // but its source structure still has to satisfy the old bounds.
                self.data_type(key, depth + 1, false)?;
                return self.data_type(value, depth + 1, emit);
            }
            DataType::Decimal32(_, _) => 35,
            DataType::Decimal64(_, _) => 36,
            DataType::Decimal128(_, _) => 37,
            DataType::Decimal256(_, _) => 38,
            DataType::Map(_, _) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation canonical schema Map values are not supported",
                ));
            }
            DataType::RunEndEncoded(_, _) => 40,
        };
        if emit {
            self.hasher.update([tag]);
        }
        match data_type {
            DataType::Timestamp(unit, timezone) => {
                if emit {
                    hash_time_unit(&mut self.hasher, *unit);
                }
                match timezone {
                    Some(timezone) => {
                        self.charge(timezone.len())?;
                        self.charge(2 * std::mem::size_of::<usize>())?;
                        if emit {
                            self.hasher.update([1]);
                            digest_bytes(&mut self.hasher, timezone.as_bytes());
                        }
                    }
                    None if emit => self.hasher.update([0]),
                    None => {}
                }
            }
            DataType::Time32(unit) | DataType::Time64(unit) | DataType::Duration(unit) => {
                if emit {
                    hash_time_unit(&mut self.hasher, *unit);
                }
            }
            DataType::Interval(unit) if emit => self.hasher.update([match unit {
                IntervalUnit::YearMonth => 1,
                IntervalUnit::DayTime => 2,
                IntervalUnit::MonthDayNano => 3,
            }]),
            DataType::FixedSizeBinary(size) if emit => self.hasher.update(size.to_be_bytes()),
            DataType::List(child)
            | DataType::ListView(child)
            | DataType::LargeList(child)
            | DataType::LargeListView(child) => self.field(child, depth + 1, emit)?,
            DataType::FixedSizeList(child, size) => {
                if emit {
                    self.hasher.update(size.to_be_bytes());
                }
                self.field(child, depth + 1, emit)?;
            }
            DataType::Struct(fields) => {
                self.charge(2 * std::mem::size_of::<usize>())?;
                self.slots::<std::sync::Arc<Field>>(fields.len())?;
                if emit {
                    self.hasher.update((fields.len() as u64).to_be_bytes());
                }
                for child in fields {
                    self.field(child, depth + 1, emit)?;
                }
            }
            DataType::Union(fields, mode) => {
                self.charge(2 * std::mem::size_of::<usize>())?;
                self.slots::<(i8, std::sync::Arc<Field>)>(fields.len())?;
                if emit {
                    self.hasher.update([match mode {
                        UnionMode::Sparse => 1,
                        UnionMode::Dense => 2,
                    }]);
                }
                for (type_id, child) in fields.iter() {
                    if type_id < 0 {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "row-mutation canonical schema cannot contain a negative Union type id",
                        ));
                    }
                    if emit {
                        self.hasher.update(type_id.to_be_bytes());
                    }
                    self.field(child, depth + 1, emit)?;
                }
            }
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale)
                if emit =>
            {
                self.hasher.update([*precision]);
                self.hasher.update(scale.to_be_bytes());
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.field(run_ends, depth + 1, emit)?;
                self.field(values, depth + 1, emit)?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn canonical_schema_digest(schema: &Schema) -> Result<[u8; 32], ConnectorError> {
    if schema.fields().len() > 4096 {
        return Err(CanonicalSchemaHash::exhausted());
    }
    let mut state = CanonicalSchemaHash {
        hasher: Sha256::new(),
        retained_bytes: 0,
        types: 0,
    };
    state.charge(std::mem::size_of::<Schema>() + 4 * std::mem::size_of::<usize>())?;
    state.slots::<std::sync::Arc<Field>>(schema.fields().len())?;
    state
        .hasher
        .update(b"novarocks.connector-row-mutation-arrow-schema.v1\0");
    state.metadata(schema.metadata(), true)?;
    state
        .hasher
        .update((schema.fields().len() as u64).to_be_bytes());
    for field in schema.fields() {
        state.field(field, 0, true)?;
    }
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

const fn effect_tag(effect: ConnectorRowMutationEffect) -> u8 {
    match effect {
        ConnectorRowMutationEffect::Delete => 1,
        ConnectorRowMutationEffect::Replace => 2,
        ConnectorRowMutationEffect::Insert => 3,
    }
}
const fn strategy_tag(strategy: ConnectorRowMutationStrategy) -> u8 {
    match strategy {
        ConnectorRowMutationStrategy::PositionDelete => 1,
        ConnectorRowMutationStrategy::DeletionVector => 2,
        ConnectorRowMutationStrategy::MergeOnRead => 3,
        ConnectorRowMutationStrategy::EqualityDelete => 5,
    }
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
    fn base_version_ordinal_round_trips_and_is_digest_bound() {
        let absent = preparation_with_base_version_ordinal(None);
        assert_eq!(absent.base_version_ordinal(), None);
        absent.validate().expect("absent ordinal validates");

        let present = preparation_with_base_version_ordinal(Some(41));
        assert_eq!(present.base_version_ordinal(), Some(41));
        present.validate().expect("present ordinal validates");
        assert_ne!(
            present.digest(),
            preparation_with_base_version_ordinal(Some(42)).digest()
        );
        assert_ne!(present.digest(), absent.digest());

        let mut tampered = present;
        tampered.base_version_ordinal = Some(42);
        assert_eq!(
            tampered.validate().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn canonical_schema_keeps_its_source_shape_boundaries() {
        fn nested(depth: usize) -> DataType {
            let mut ty = DataType::Int64;
            for _ in 0..depth {
                ty = DataType::List(Arc::new(Field::new("item", ty, true)));
            }
            ty
        }
        let schema = |ty| Schema::new(vec![Field::new("value", ty, true)]);
        assert!(canonical_schema_digest(&schema(nested(63))).is_ok());
        assert_eq!(
            canonical_schema_digest(&schema(nested(64)))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        // The key does not enter the logical digest, but its physical source
        // still has to satisfy the same structure boundary before hashing.
        assert_eq!(
            canonical_schema_digest(&schema(DataType::Dictionary(
                Box::new(nested(63)),
                Box::new(DataType::Int64),
            )))
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            canonical_schema_digest(&schema(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Int64),
            )))
            .unwrap(),
            canonical_schema_digest(&schema(DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::Int64),
            )))
            .unwrap()
        );
        // A schema signature does not construct width-sized synthetic NULL
        // arrays; that RowConverter-only workspace has no remaining consumer.
        assert!(
            canonical_schema_digest(&schema(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Int64, true)),
                i32::MAX,
            )))
            .is_ok()
        );
        let field = Arc::new(Field::new("value", DataType::Int64, true));
        assert!(canonical_schema_digest(&Schema::new(vec![field.clone(); 4096])).is_ok());
        assert_eq!(
            canonical_schema_digest(&Schema::new(vec![field.clone(); 4097]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            canonical_schema_digest(&schema(DataType::Struct(vec![field; 4096].into())))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        for ty in [
            DataType::FixedSizeBinary(-1),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), -1),
        ] {
            assert_eq!(
                canonical_schema_digest(&schema(ty)).unwrap_err().kind(),
                ConnectorErrorKind::ResourceExhausted
            );
        }
        for ty in [
            DataType::Union(arrow::datatypes::UnionFields::empty(), UnionMode::Sparse),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ] {
            assert_eq!(
                canonical_schema_digest(&schema(ty)).unwrap_err().kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn canonical_schema_counts_shared_metadata_capacity_at_each_source_occurrence() {
        // A small allocation with spare capacity exercises the inherited
        // source-byte bound without materializing a 256-MiB payload.
        let mut value = String::with_capacity(4 * 1024 * 1024);
        value.push('x');
        let field = Arc::new(
            Field::new("value", DataType::Int64, true)
                .with_metadata(HashMap::from([("capacity".into(), value)])),
        );
        assert!(field.metadata()["capacity"].capacity() >= 4 * 1024 * 1024);
        assert!(canonical_schema_digest(&Schema::new(vec![field.clone()])).is_ok());
        assert_eq!(
            canonical_schema_digest(&Schema::new(vec![field; 64]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
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
            DataType::Utf8,
            false,
        );
        let carrier = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
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
            DataType::Utf8,
            false,
        );
        let declared = value(
            ValueKind::List(child.value_type().clone()),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        );
        let widened = Field::new(
            "document",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
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
mod typed_match_digest_tests {
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
    fn json() -> FunctionValueType {
        value(
            ValueKind::Nominal(ValueLogicalType::Json),
            DataType::Utf8,
            false,
        )
    }
    #[test]
    fn match_identity_covers_same_carrier_domain_in_each_declared_role() {
        let baseline = contract(text(), text(), text(), HashMap::new());
        for changed in [
            contract(json(), text(), text(), HashMap::new()),
            contract(text(), json(), text(), HashMap::new()),
            contract(text(), text(), json(), HashMap::new()),
        ] {
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
            assert_ne!(baseline.digest(), changed.digest());
            changed.validate().unwrap();
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
        assert_ne!(first.digest(), second.digest());
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
        let text_child = contract(text(), text(), nested(text()), HashMap::new());
        assert_eq!(
            first.after_fields()[0].field(),
            text_child.after_fields()[0].field()
        );
        assert_eq!(first.digest(), independently_equal.digest());
        assert_ne!(first.digest(), text_child.digest());
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
        assert_ne!(first.digest(), second.digest());
        let schema = Schema::new(vec![Field::new("after", DataType::Utf8, false)]);
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
        let failure = typed.validate().unwrap_err();
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            failure.message(),
            "row-mutation match field lacks its complete execution declaration"
        );
    }
}
