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

//! Provider-owned row-mutation admission and activation contract.
//! Design: ADR-0049 (docs/adr/ADR-0049-provider-row-mutation-strategy-identity-routes-and-cohorts.md)

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, Int8Array};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub use novarocks_connector_contract::{ConnectorRowMutationEffect, ConnectorWriteRouteId};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorPinnedFileSet, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorSealedWriteCohortSet, ConnectorTableHandle,
    ConnectorWriteBaseVersion, ConnectorWriteCohortId, ConnectorWriteFieldToken,
    ConnectorWriteInputShape, ConnectorWriteIntent, ConnectorWriteOperationId,
    ConnectorWritePreparation, ConnectorWriteTargetRef, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

mod match_bounds;
#[cfg(test)]
mod match_bounds_tests;
mod row_footprint;
mod source_owner;
pub use source_owner::{
    ConnectorRowMutationSourceArray, ConnectorRowMutationSourceBatch,
    ConnectorRowMutationSourceBuffer, ConnectorRowMutationSourceBuilder,
    ConnectorRowMutationSourceChildren, MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES,
};
#[cfg(test)]
thread_local! { static MATCH_LAYOUT_ALLOCATION_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[cfg(test)]
mod row_footprint_tests;
#[cfg(test)]
thread_local! { static ROW_CONVERSION_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
pub use row_footprint::{
    ConnectorRowConversionBatchFootprint, ConnectorRowConversionFootprint,
    MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES,
};

pub const CONNECTOR_ROW_MUTATION_CONTRACT_VERSION: u32 = 2;
pub const MAX_CONNECTOR_ROW_MUTATION_ROUTES: usize = 4096;
pub const MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES: usize = 4096;

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

#[derive(Clone, Debug)]
pub struct ConnectorMutationSourceField {
    token: ConnectorWriteFieldToken,
    field: Field,
    source_ordinal: u32,
}

impl ConnectorMutationSourceField {
    pub fn new(token: ConnectorWriteFieldToken, field: Field, source_ordinal: u32) -> Self {
        Self {
            token,
            field,
            source_ordinal,
        }
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
}

impl ConnectorMutationTargetField {
    pub fn new(token: ConnectorWriteFieldToken, field: Field, target_ordinal: u32) -> Self {
        Self {
            token,
            field,
            target_ordinal,
        }
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

/// A provider-signed match layout. Identity, before/after values and the
/// duplicate-detection tuple are token-bound, not inferred from column names.
#[derive(Clone, Debug)]
pub struct ConnectorMutationMatchContract {
    owner: ConnectorProviderBindingKey,
    table: ConnectorTableHandle,
    base_version: ConnectorWriteBaseVersion,
    identity_fields: Vec<ConnectorMutationSourceField>,
    before_fields: Vec<ConnectorMutationTargetField>,
    after_fields: Vec<ConnectorMutationTargetField>,
    uniqueness_tokens: Vec<ConnectorWriteFieldToken>,
    effect_field: ConnectorMutationEffectField,
    digest: [u8; 32],
}

/// The provider-signed semantic role of one field in a materialized mutation
/// selection. Tokens, rather than field names, identify these roles across
/// planning boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorMutationSelectionFieldRole {
    Identity,
    BeforeImage,
    AfterImage,
    Effect,
}

/// One exact tokenized field in the provider-signed selection layout.
#[derive(Clone, Copy, Debug)]
pub struct ConnectorMutationSelectionFieldRef<'a> {
    token: ConnectorWriteFieldToken,
    role: ConnectorMutationSelectionFieldRole,
    ordinal: u32,
    field: &'a Field,
}

impl<'a> ConnectorMutationSelectionFieldRef<'a> {
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }

    pub const fn role(&self) -> ConnectorMutationSelectionFieldRole {
        self.role
    }

    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub const fn field(&self) -> &'a Field {
        self.field
    }
}

impl ConnectorMutationMatchContract {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        table: ConnectorTableHandle,
        base_version: ConnectorWriteBaseVersion,
        identity_fields: Vec<ConnectorMutationSourceField>,
        before_fields: Vec<ConnectorMutationTargetField>,
        after_fields: Vec<ConnectorMutationTargetField>,
        uniqueness_tokens: Vec<ConnectorWriteFieldToken>,
        effect_field: ConnectorMutationEffectField,
    ) -> Result<Self, ConnectorError> {
        let digest = Self::validate_match_layout(
            &owner,
            &table,
            &base_version,
            &identity_fields,
            &before_fields,
            &after_fields,
            &uniqueness_tokens,
            &effect_field,
        )?;
        Ok(Self {
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            uniqueness_tokens,
            effect_field,
            digest,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::ptr_arg)]
    fn validate_match_layout(
        owner: &ConnectorProviderBindingKey,
        table: &ConnectorTableHandle,
        base_version: &ConnectorWriteBaseVersion,
        identity_fields: &Vec<ConnectorMutationSourceField>,
        before_fields: &Vec<ConnectorMutationTargetField>,
        after_fields: &Vec<ConnectorMutationTargetField>,
        uniqueness_tokens: &Vec<ConnectorWriteFieldToken>,
        effect_field: &ConnectorMutationEffectField,
    ) -> Result<[u8; 32], ConnectorError> {
        if table.owner() != &owner.instance_id
            || identity_fields.is_empty()
            || uniqueness_tokens.is_empty()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation match contract needs an exact owner, identity, and uniqueness tuple",
            ));
        }
        match_bounds::preflight(
            identity_fields,
            before_fields,
            after_fields,
            uniqueness_tokens,
            effect_field,
        )?;
        base_version.validate()?;
        #[cfg(test)]
        MATCH_LAYOUT_ALLOCATION_CALLS.with(|n| n.set(n.get() + 1));
        let mut tokens = HashSet::new();
        let mut selection_ordinals = HashSet::new();
        for value in identity_fields {
            if !tokens.insert(value.token) || !selection_ordinals.insert(value.source_ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation identity has duplicate token or source ordinal",
                ));
            }
        }
        for value in before_fields.iter().chain(after_fields) {
            if !tokens.insert(value.token) || !selection_ordinals.insert(value.target_ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation target field has duplicate token or selection ordinal",
                ));
            }
        }
        if !tokens.insert(effect_field.token)
            || !selection_ordinals.insert(effect_field.target_ordinal)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation effect field conflicts with the match layout",
            ));
        }
        if selection_ordinals.len() != tokens.len()
            || !(0..u32::try_from(selection_ordinals.len()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation match layout exceeds u32 selection ordinals",
                )
            })?)
                .all(|ordinal| selection_ordinals.contains(&ordinal))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation match layout selection ordinals must be dense from zero",
            ));
        }
        let mut unique = HashSet::new();
        if uniqueness_tokens
            .iter()
            .any(|token| !tokens.contains(token) || !unique.insert(*token))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation uniqueness tuple contains a foreign or duplicate token",
            ));
        }
        match_digest(
            owner,
            table,
            base_version,
            identity_fields,
            before_fields,
            after_fields,
            uniqueness_tokens,
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
            &self.uniqueness_tokens,
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
    pub fn uniqueness_tokens(&self) -> &[ConnectorWriteFieldToken] {
        &self.uniqueness_tokens
    }
    pub fn effect_field(&self) -> &ConnectorMutationEffectField {
        &self.effect_field
    }
    pub fn selection_field(
        &self,
        token: ConnectorWriteFieldToken,
    ) -> Option<ConnectorMutationSelectionFieldRef<'_>> {
        self.identity_fields
            .iter()
            .find(|field| field.token() == token)
            .map(|field| ConnectorMutationSelectionFieldRef {
                token,
                role: ConnectorMutationSelectionFieldRole::Identity,
                ordinal: field.source_ordinal(),
                field: field.field(),
            })
            .or_else(|| {
                self.before_fields
                    .iter()
                    .find(|field| field.token() == token)
                    .map(|field| ConnectorMutationSelectionFieldRef {
                        token,
                        role: ConnectorMutationSelectionFieldRole::BeforeImage,
                        ordinal: field.target_ordinal(),
                        field: field.field(),
                    })
            })
            .or_else(|| {
                self.after_fields
                    .iter()
                    .find(|field| field.token() == token)
                    .map(|field| ConnectorMutationSelectionFieldRef {
                        token,
                        role: ConnectorMutationSelectionFieldRole::AfterImage,
                        ordinal: field.target_ordinal(),
                        field: field.field(),
                    })
            })
            .or_else(|| {
                (self.effect_field.token() == token).then(|| ConnectorMutationSelectionFieldRef {
                    token,
                    role: ConnectorMutationSelectionFieldRole::Effect,
                    ordinal: self.effect_field.target_ordinal(),
                    field: self.effect_field.field(),
                })
            })
    }

    /// Validate the complete Arrow selection against this exact tokenized
    /// layout. Extra fields and ordinal gaps are refused because either would
    /// make a token-to-value binding ambiguous after transport.
    pub fn validate_selection(
        &self,
        selection: &ConnectorRowMutationSelection,
    ) -> Result<(), ConnectorError> {
        self.validate()?;
        selection.validate()?;
        let schema = selection.schema();
        let expected_len =
            self.identity_fields.len() + self.before_fields.len() + self.after_fields.len() + 1;
        if schema.fields().len() != expected_len {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation selection width differs from its signed match contract",
            ));
        }
        for signed in self
            .identity_fields
            .iter()
            .map(|field| (field.source_ordinal(), field.field()))
            .chain(
                self.before_fields
                    .iter()
                    .chain(&self.after_fields)
                    .map(|field| (field.target_ordinal(), field.field())),
            )
            .chain(std::iter::once((
                self.effect_field.target_ordinal(),
                self.effect_field.field(),
            )))
        {
            let actual = schema.fields().get(signed.0 as usize).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation selection omits a signed match field ordinal",
                )
            })?;
            if actual.as_ref() != signed.1 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation selection field does not match its signed type and nullability",
                ));
            }
        }
        Ok(())
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
    CopyOnWrite,
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

/// One opaque sink route. A route can accept more than one logical effect;
/// generic split sinks independently fan out one Replace to all such routes.
#[derive(Clone)]
pub struct ConnectorRowMutationRoute {
    route_id: ConnectorWriteRouteId,
    cohort_id: ConnectorWriteCohortId,
    accepted_effects: Vec<ConnectorRowMutationEffect>,
    input: ConnectorWriteInputShape,
    input_ordinals: Vec<ConnectorMutationRouteInput>,
    partition_fields: Vec<ConnectorWriteFieldToken>,
    preparation: ConnectorWritePreparation,
    digest: [u8; 32],
}

impl ConnectorRowMutationRoute {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        cohort_id: ConnectorWriteCohortId,
        accepted_effects: Vec<ConnectorRowMutationEffect>,
        input: ConnectorWriteInputShape,
        input_ordinals: Vec<ConnectorMutationRouteInput>,
        partition_fields: Vec<ConnectorWriteFieldToken>,
        preparation: ConnectorWritePreparation,
    ) -> Result<Self, ConnectorError> {
        validate_effects(&accepted_effects, "row-mutation route")?;
        input.validate()?;
        preparation.validate()?;
        let known: HashSet<_> = input
            .fields()
            .into_iter()
            .map(|field| field.token())
            .collect();
        let mut tokens = HashSet::new();
        if input_ordinals.len() != known.len()
            || input_ordinals
                .iter()
                .any(|input| !known.contains(&input.token) || !tokens.insert(input.token))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation route input bindings are incomplete, foreign, or repeat a token",
            ));
        }
        let mut partitions = HashSet::new();
        if partition_fields
            .iter()
            .any(|token| !known.contains(token) || !partitions.insert(*token))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation route partition token is foreign or duplicate",
            ));
        }
        let digest = route_digest(
            route_id,
            cohort_id,
            &accepted_effects,
            &input_ordinals,
            &partition_fields,
            &preparation,
        );
        Ok(Self {
            route_id,
            cohort_id,
            accepted_effects,
            input,
            input_ordinals,
            partition_fields,
            preparation,
            digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.route_id,
            self.cohort_id,
            self.accepted_effects.clone(),
            self.input.clone(),
            self.input_ordinals.clone(),
            self.partition_fields.clone(),
            self.preparation.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation route digest does not match contents",
            ));
        }
        Ok(())
    }
    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    pub fn accepted_effects(&self) -> &[ConnectorRowMutationEffect] {
        &self.accepted_effects
    }
    pub fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    pub fn input_ordinals(&self) -> &[ConnectorMutationRouteInput] {
        &self.input_ordinals
    }
    pub fn partition_fields(&self) -> &[ConnectorWriteFieldToken] {
        &self.partition_fields
    }
    pub fn preparation(&self) -> &ConnectorWritePreparation {
        &self.preparation
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Provider-signed, pure planning result. The payload is opaque to Core and
/// must be activated with this exact operation and retained write lease.
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
    #[allow(clippy::too_many_arguments)]
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
        validate_selection_schema_shape(match_source_schema.as_ref())?;
        for expected in match_contract
            .identity_fields()
            .iter()
            .map(ConnectorMutationSourceField::field)
            .chain(
                match_contract
                    .before_fields()
                    .iter()
                    .chain(match_contract.after_fields())
                    .map(ConnectorMutationTargetField::field),
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
            if matching.next().is_some()
                || actual.data_type() != expected.data_type()
                || (actual.is_nullable() && !expected.is_nullable())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation match source field differs from its signed match role",
                ));
            }
        }
        let match_source_schema_digest = canonical_schema_digest(match_source_schema.as_ref())?;
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

/// A bounded, non-concatenated COW match result. The caller enforces the
/// minimum of request payload and execution-memory budgets before activation.
#[derive(Clone, Debug)]
pub struct ConnectorRowMutationSelection {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    batch_row_offsets: Vec<u64>,
    row_count: u64,
    byte_count: u64,
    max_rows: u64,
    max_bytes: u64,
    digest: [u8; 32],
    // Present only when the private construction owner supplied every batch.
    source_ownership: Option<std::sync::Arc<[usize]>>,
    retention: Vec<crate::connector::ConnectorPayloadRetentionGuard>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorRowMutationSelectionOrdinal(u64);

impl ConnectorRowMutationSelectionOrdinal {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A zero-copy view located from a selection ordinal. Batch-local offsets are
/// derived from the retained batches and are deliberately not a second index.
pub struct ConnectorRowMutationSelectionView<'a> {
    ordinal: ConnectorRowMutationSelectionOrdinal,
    batch_index: usize,
    row_index: usize,
    batch: &'a RecordBatch,
}

impl<'a> ConnectorRowMutationSelectionView<'a> {
    pub const fn ordinal(&self) -> ConnectorRowMutationSelectionOrdinal {
        self.ordinal
    }
    pub const fn batch_index(&self) -> usize {
        self.batch_index
    }
    pub const fn row_index(&self) -> usize {
        self.row_index
    }
    pub const fn batch(&self) -> &'a RecordBatch {
        self.batch
    }
}

impl ConnectorRowMutationSelection {
    /// Consume receipts minted by the bounded source construction owner. No
    /// arbitrary RecordBatch can assert or acquire this source eligibility.
    pub fn try_new_owned(
        schema: SchemaRef,
        sources: Vec<ConnectorRowMutationSourceBatch>,
        max_rows: u64,
        max_bytes: u64,
    ) -> Result<Self, ConnectorError> {
        let header_peak = sources
            .capacity()
            .checked_mul(size_of::<ConnectorRowMutationSourceBatch>())
            .and_then(|bytes| {
                sources
                    .len()
                    .checked_mul(size_of::<RecordBatch>() + 3 * size_of::<usize>())?
                    .checked_add(bytes)
            });
        if sources.len() > MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES
            || header_peak.is_none_or(|bytes| bytes > 1024 * 1024)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "owned COW source containers exceed their bounded bookkeeping",
            ));
        }
        let mut source_total = ConnectorRowConversionFootprint::retained_schema_bytes(&schema)?;
        for source in &sources {
            source_total = source_total
                .checked_add(source.source_bytes())
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "owned COW source accounting overflowed",
                    )
                })?;
            if source_total as u64 > max_bytes || source_total > 64 * 1024 * 1024 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "owned COW sources exceed the retained selection profile",
                ));
            }
        }
        let mut retention = Vec::with_capacity(sources.len());
        let mut ownership = Vec::with_capacity(sources.len());
        let mut batches = Vec::with_capacity(sources.len());
        for source in sources {
            ownership.push(source.source_bytes());
            let (batch, guard) = source.into_parts();
            batches.push(batch);
            retention.extend(guard);
        }
        let mut selection = Self::try_new(schema, batches, max_rows, max_bytes)?;
        selection.source_ownership = Some(ownership.into());
        selection.retention = retention;
        Ok(selection)
    }

    /// Retain the selection's schema and index containers even when no source
    /// batch exists. Sources still require safe-factory provenance.
    pub fn try_new_owned_with_guard(
        schema: SchemaRef,
        sources: Vec<ConnectorRowMutationSourceBatch>,
        max_rows: u64,
        max_bytes: u64,
        guard: crate::connector::ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        let mut selection = Self::try_new_owned(schema, sources, max_rows, max_bytes)?;
        selection.retention.push(guard);
        Ok(selection)
    }

    /// Keep the admitted carrier holder through whole-selection clones.
    /// This does not establish owned-source eligibility for legacy batches;
    /// their raw Arrow aliases remain protected by the transitional owner.
    pub fn retain_carrier(
        mut self,
        guard: crate::connector::ConnectorPayloadRetentionGuard,
    ) -> Self {
        self.retention.push(guard);
        self
    }

    /// Only constructor-owned sources qualify for production bounded ingress.
    pub fn has_owned_sources(&self) -> bool {
        self.source_ownership.is_some()
    }

    /// Borrow the original safe-source receipt without cloning or encoding data.
    /// This is an ownership size fact, not a capacity permit. Legacy selections
    /// have no constructor receipt and cannot manufacture one here.
    pub fn owned_source_backing_upper(&self) -> Result<Option<usize>, ConnectorError> {
        let Some(ownership) = &self.source_ownership else {
            return Ok(None);
        };
        if ownership.len() != self.batches.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "COW selection source receipts differ from its batches",
            ));
        }
        let mut total = ConnectorRowConversionFootprint::retained_schema_bytes(&self.schema)?;
        for source_bytes in ownership.iter() {
            total = total.checked_add(*source_bytes).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "COW source backing receipt overflowed",
                )
            })?;
        }
        if total > 64 * 1024 * 1024 || total as u64 > self.max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "COW source backing receipt exceeds its original profile",
            ));
        }
        Ok(Some(total))
    }

    /// Private-vector blocks retained by this selection, besides the original
    /// source receipt. That receipt covers one current batch column-vector
    /// set, including a replacement after the original set exits. Schema and
    /// array backing remain shared. Every simultaneously live whole-selection
    /// clone additionally needs its prospective fresh column-vector blocks.
    pub fn retained_container_bytes(&self) -> Result<usize, ConnectorError> {
        let exhausted = || {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "COW selection container footprint overflowed",
            )
        };
        let mut bytes = size_of::<Self>();
        for (capacity, item_bytes) in [
            (self.batches.capacity(), size_of::<RecordBatch>()),
            (self.batch_row_offsets.capacity(), size_of::<u64>()),
            (
                self.retention.capacity(),
                size_of::<crate::connector::ConnectorPayloadRetentionGuard>(),
            ),
        ] {
            bytes = bytes
                .checked_add(capacity.checked_mul(item_bytes).ok_or_else(exhausted)?)
                .ok_or_else(exhausted)?;
        }
        if let Some(ownership) = &self.source_ownership {
            // Arc<[usize]> shares this allocation across every selection clone.
            // Two counters and aligned usize data are the allocation layout.
            let shared = ownership
                .len()
                .checked_add(2)
                .and_then(|slots| slots.checked_mul(size_of::<usize>()))
                .ok_or_else(exhausted)?;
            bytes = bytes.checked_add(shared).ok_or_else(exhausted)?;
        }
        Ok(bytes)
    }

    /// Fresh private blocks requested by derive(Clone); Arrow/schema/guard
    /// pointees and the original source receipt remain shared. RecordBatch's
    /// pinned Clone copies its Vec<ArrayRef>, whose entries clone only Arcs.
    pub fn cloned_container_bytes(&self) -> Result<usize, ConnectorError> {
        let exhausted = || {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "COW selection clone footprint overflowed",
            )
        };
        let mut bytes = size_of::<Self>();
        for (length, item_bytes) in [
            (self.batches.len(), size_of::<RecordBatch>()),
            (self.batch_row_offsets.len(), size_of::<u64>()),
            (
                self.retention.len(),
                size_of::<crate::connector::ConnectorPayloadRetentionGuard>(),
            ),
        ] {
            bytes = bytes
                .checked_add(length.checked_mul(item_bytes).ok_or_else(exhausted)?)
                .ok_or_else(exhausted)?;
        }
        for batch in &self.batches {
            bytes = bytes
                .checked_add(
                    batch
                        .num_columns()
                        .checked_mul(size_of::<arrow::array::ArrayRef>())
                        .ok_or_else(exhausted)?,
                )
                .ok_or_else(exhausted)?;
        }
        Ok(bytes)
    }

    pub fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        max_rows: u64,
        max_bytes: u64,
    ) -> Result<Self, ConnectorError> {
        if max_rows == 0 || max_bytes == 0 || max_rows > max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation selection has invalid row or byte budgets",
            ));
        }
        if batches.len() > MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation selection has too many Arrow batches",
            ));
        }
        check_selection_headers(batches.capacity(), batches.len())?;
        validate_selection_schema_shape(schema.as_ref())?;
        let mut rows = 0_u64;
        let mut bytes =
            ConnectorRowConversionFootprint::retained_schema_bytes(schema.as_ref())? as u64;
        if bytes > max_bytes || bytes > 64 * 1024 * 1024 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation selection schema exceeds its source byte budget",
            ));
        }
        let mut batch_row_offsets = Vec::with_capacity(batches.len() + 1);
        batch_row_offsets.push(0);
        for batch in &batches {
            let batch_bytes = ConnectorRowConversionFootprint::retained_batch_bytes(batch)? as u64;
            if batch.schema_ref() != &schema {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation selection batch differs from its explicit schema",
                ));
            }
            rows = rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation selection row accounting overflowed",
                )
            })?;
            bytes = bytes.checked_add(batch_bytes).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation selection byte accounting overflowed",
                )
            })?;
            if rows > max_rows || rows > 1_048_576 || bytes > max_bytes || bytes > 64 * 1024 * 1024
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation selection exceeds its row or byte budget",
                ));
            }
            batch_row_offsets.push(rows);
        }
        let digest = selection_digest(&schema, &batches, rows)?;
        Ok(Self {
            schema,
            batches,
            batch_row_offsets,
            row_count: rows,
            byte_count: bytes,
            max_rows,
            max_bytes,
            digest,
            source_ownership: None,
            retention: Vec::new(),
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.max_rows == 0
            || self.max_bytes == 0
            || self.max_rows > self.max_bytes
            || self.row_count > self.max_rows
            || self.byte_count > self.max_bytes
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation selection retained invalid bounds",
            ));
        }
        if let Some(ownership) = &self.source_ownership {
            let total = ownership.iter().try_fold(
                ConnectorRowConversionFootprint::retained_schema_bytes(&self.schema)?,
                |total, bytes| total.checked_add(*bytes),
            );
            if ownership.len() != self.batches.len()
                || total
                    .is_none_or(|bytes| bytes as u64 > self.max_bytes || bytes > 64 * 1024 * 1024)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "COW selection lost its exact bounded source ownership",
                ));
            }
        }
        check_selection_headers(self.batches.capacity(), self.batches.len())?;
        validate_selection_schema_shape(self.schema.as_ref())?;
        let (rows, bytes) = selection_size(self.schema.as_ref(), &self.batches)?;
        if self
            .batches
            .iter()
            .any(|batch| batch.schema_ref() != &self.schema)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation selection batch differs from its retained schema",
            ));
        }
        let mut offsets = Vec::with_capacity(self.batches.len() + 1);
        offsets.push(0);
        for batch in &self.batches {
            offsets.push(offsets.last().copied().unwrap_or(0) + batch.num_rows() as u64);
        }
        if rows != self.row_count
            || bytes != self.byte_count
            || offsets != self.batch_row_offsets
            || selection_digest(&self.schema, &self.batches, self.row_count)? != self.digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation selection digest does not match batches",
            ));
        }
        Ok(())
    }
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }
    pub const fn max_rows(&self) -> u64 {
        self.max_rows
    }
    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn locate(
        &self,
        ordinal: ConnectorRowMutationSelectionOrdinal,
    ) -> Option<ConnectorRowMutationSelectionView<'_>> {
        if ordinal.get() >= self.row_count {
            return None;
        }
        let upper = self
            .batch_row_offsets
            .partition_point(|offset| *offset <= ordinal.get());
        let batch_index = upper.checked_sub(1)?;
        Some(ConnectorRowMutationSelectionView {
            ordinal,
            batch_index,
            row_index: (ordinal.get() - self.batch_row_offsets[batch_index]) as usize,
            batch: &self.batches[batch_index],
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorRowMutationScanBinding {
    token: ConnectorWriteFieldToken,
    scan_ordinal: u32,
}

impl ConnectorRowMutationScanBinding {
    pub const fn new(token: ConnectorWriteFieldToken, scan_ordinal: u32) -> Self {
        Self {
            token,
            scan_ordinal,
        }
    }
    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }
    pub const fn scan_ordinal(&self) -> u32 {
        self.scan_ordinal
    }
}

// Boxing only the rewrite variant would expose allocation policy in this
// public transient DTO without reducing any retained facts. Recipes are
// constructed once during control-plane activation, never per data row.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum ConnectorRowMutationCohortRecipeBody {
    Rewrite {
        source: ConnectorTableHandle,
        /// Exactly the files this cohort rewrites. The cohort's commit
        /// replaces precisely these, so the read that produces its rows must
        /// be defined by the same set rather than by anything re-derived.
        pinned_source: ConnectorPinnedFileSet,
        base_version_digest: [u8; 32],
        scan_schema: SchemaRef,
        scan_schema_digest: [u8; 32],
        scan_bindings: Vec<ConnectorRowMutationScanBinding>,
        match_tokens: Vec<ConnectorWriteFieldToken>,
        written_version_token: Option<ConnectorWriteFieldToken>,
    },
    Append,
}

#[derive(Clone)]
pub struct ConnectorRowMutationCohortRecipe {
    cohort_id: ConnectorWriteCohortId,
    route_id: ConnectorWriteRouteId,
    selection_digest: [u8; 32],
    selection_ordinals: Vec<ConnectorRowMutationSelectionOrdinal>,
    body: ConnectorRowMutationCohortRecipeBody,
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorRowMutationCohortRecipe {
    pub fn try_append(
        cohort_id: ConnectorWriteCohortId,
        route_id: ConnectorWriteRouteId,
        selection: &ConnectorRowMutationSelection,
        selection_ordinals: Vec<ConnectorRowMutationSelectionOrdinal>,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            cohort_id,
            route_id,
            selection,
            selection_ordinals,
            ConnectorRowMutationCohortRecipeBody::Append,
            payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_rewrite(
        cohort_id: ConnectorWriteCohortId,
        route_id: ConnectorWriteRouteId,
        selection: &ConnectorRowMutationSelection,
        selection_ordinals: Vec<ConnectorRowMutationSelectionOrdinal>,
        source: ConnectorTableHandle,
        pinned_source: ConnectorPinnedFileSet,
        base_version_digest: [u8; 32],
        scan_schema: SchemaRef,
        mut scan_bindings: Vec<ConnectorRowMutationScanBinding>,
        match_tokens: Vec<ConnectorWriteFieldToken>,
        written_version_token: Option<ConnectorWriteFieldToken>,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        scan_bindings.sort_by_key(|binding| (binding.token(), binding.scan_ordinal()));
        let scan_schema_digest = canonical_schema_digest(scan_schema.as_ref())?;
        Self::try_new(
            cohort_id,
            route_id,
            selection,
            selection_ordinals,
            ConnectorRowMutationCohortRecipeBody::Rewrite {
                source,
                pinned_source,
                base_version_digest,
                scan_schema,
                scan_schema_digest,
                scan_bindings,
                match_tokens,
                written_version_token,
            },
            payload,
        )
    }

    fn try_new(
        cohort_id: ConnectorWriteCohortId,
        route_id: ConnectorWriteRouteId,
        selection: &ConnectorRowMutationSelection,
        selection_ordinals: Vec<ConnectorRowMutationSelectionOrdinal>,
        body: ConnectorRowMutationCohortRecipeBody,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_payload(&payload)?;
        if selection_ordinals.is_empty()
            || selection_ordinals.len() as u64 > selection.row_count()
            || selection_ordinals
                .last()
                .is_some_and(|ordinal| ordinal.get() >= selection.row_count())
            || selection_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation recipe selection ordinals must be non-empty and strictly increasing",
            ));
        }
        validate_recipe_body_shape(&body)?;
        let digest = cohort_recipe_digest(
            cohort_id,
            route_id,
            selection.digest(),
            &selection_ordinals,
            &body,
            &payload,
        );
        Ok(Self {
            cohort_id,
            route_id,
            selection_digest: selection.digest(),
            selection_ordinals,
            body,
            payload,
            digest,
        })
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }
    pub const fn selection_digest(&self) -> [u8; 32] {
        self.selection_digest
    }
    pub fn selection_ordinals(&self) -> &[ConnectorRowMutationSelectionOrdinal] {
        &self.selection_ordinals
    }
    pub fn body(&self) -> &ConnectorRowMutationCohortRecipeBody {
        &self.body
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_payload(&self.payload)?;
        validate_recipe_body_shape(&self.body)?;
        if self.selection_ordinals.is_empty()
            || self
                .selection_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || cohort_recipe_digest(
                self.cohort_id,
                self.route_id,
                self.selection_digest,
                &self.selection_ordinals,
                &self.body,
                &self.payload,
            ) != self.digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation cohort recipe digest does not match contents",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub enum ConnectorRowMutationActivationRequest {
    Direct {
        preparation: ConnectorRowMutationPreparation,
        context: ConnectorRequestContext,
    },
    CopyOnWrite {
        preparation: ConnectorRowMutationPreparation,
        selection: ConnectorRowMutationSelection,
        context: ConnectorRequestContext,
    },
}

impl ConnectorRowMutationActivationRequest {
    pub fn preparation(&self) -> &ConnectorRowMutationPreparation {
        match self {
            Self::Direct { preparation, .. } | Self::CopyOnWrite { preparation, .. } => preparation,
        }
    }
    pub fn context(&self) -> &ConnectorRequestContext {
        match self {
            Self::Direct { context, .. } | Self::CopyOnWrite { context, .. } => context,
        }
    }
    pub fn selection(&self) -> Option<&ConnectorRowMutationSelection> {
        match self {
            Self::CopyOnWrite { selection, .. } => Some(selection),
            Self::Direct { .. } => None,
        }
    }
    pub fn validate(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        let preparation = self.preparation();
        preparation.validate()?;
        if preparation.owner() != owner {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation activation has a foreign owner",
            ));
        }
        match self {
            Self::Direct { .. }
                if preparation.strategy() == ConnectorRowMutationStrategy::CopyOnWrite =>
            {
                Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write requires a bounded selection",
                ))
            }
            Self::CopyOnWrite { .. }
                if preparation.strategy() != ConnectorRowMutationStrategy::CopyOnWrite =>
            {
                Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "only copy-on-write accepts a selection",
                ))
            }
            Self::CopyOnWrite { selection, .. } => {
                selection.validate()?;
                validate_selection_against_preparation(selection, preparation)
            }
            Self::Direct { .. } => Ok(()),
        }
    }
}

/// Provider-sealed physical execution result for one row-mutation operation.
///
/// The application can route its opaque cohorts but cannot assemble an
/// unbound set of routes.  In particular, the plan always retains the exact
/// row-mutation preparation that authenticated its operation and generation.
#[derive(Clone)]
pub struct ConnectorRowMutationExecutionPlan {
    preparation: ConnectorRowMutationPreparation,
    body: ConnectorRowMutationExecutionPlanBody,
    digest: [u8; 32],
}

// The COW variant intentionally retains one bounded selection alongside its
// sealed routes. It is a process-local activation result, so an extra private
// allocation buys no useful steady-state footprint reduction.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum ConnectorRowMutationExecutionPlanBody {
    Direct {
        routes: Vec<ConnectorRowMutationRoute>,
    },
    CopyOnWrite {
        selection: ConnectorRowMutationSelection,
        routes: Vec<ConnectorRowMutationRoute>,
        sealed_cohorts: ConnectorSealedWriteCohortSet,
        cohort_recipes: Vec<ConnectorRowMutationCohortRecipe>,
        max_handle_payload_bytes: usize,
        max_total_payload_bytes: usize,
    },
}

impl ConnectorRowMutationExecutionPlan {
    pub fn try_direct(
        preparation: ConnectorRowMutationPreparation,
        routes: Vec<ConnectorRowMutationRoute>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            preparation,
            ConnectorRowMutationExecutionPlanBody::Direct { routes },
        )
    }
    pub fn try_copy_on_write(
        preparation: ConnectorRowMutationPreparation,
        selection: ConnectorRowMutationSelection,
        routes: Vec<ConnectorRowMutationRoute>,
        sealed_cohorts: ConnectorSealedWriteCohortSet,
        cohort_recipes: Vec<ConnectorRowMutationCohortRecipe>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            preparation,
            ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
                selection,
                routes,
                sealed_cohorts,
                cohort_recipes,
                max_handle_payload_bytes: context.max_handle_payload_bytes(),
                max_total_payload_bytes: context.max_total_payload_bytes(),
            },
        )
    }

    fn try_new(
        preparation: ConnectorRowMutationPreparation,
        mut body: ConnectorRowMutationExecutionPlanBody,
    ) -> Result<Self, ConnectorError> {
        preparation.validate()?;
        match &mut body {
            ConnectorRowMutationExecutionPlanBody::Direct { routes } => {
                routes.sort_by_key(ConnectorRowMutationRoute::route_id);
            }
            ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
                routes,
                cohort_recipes,
                ..
            } => {
                routes.sort_by_key(ConnectorRowMutationRoute::route_id);
                cohort_recipes.sort_by_key(|recipe| (recipe.cohort_id(), recipe.route_id()));
            }
        }
        let routes = match &body {
            ConnectorRowMutationExecutionPlanBody::Direct { routes }
            | ConnectorRowMutationExecutionPlanBody::CopyOnWrite { routes, .. } => routes,
        };
        validate_routes(routes)?;
        if routes.iter().any(|route| {
            route.preparation().owner() != preparation.owner()
                || route.preparation().table() != preparation.table()
                || route.preparation().base_version() != preparation.base_version()
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation execution route is foreign to its preparation",
            ));
        }
        if let ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
            selection,
            sealed_cohorts,
            cohort_recipes,
            max_handle_payload_bytes,
            max_total_payload_bytes,
            ..
        } = &body
        {
            if preparation.strategy() != ConnectorRowMutationStrategy::CopyOnWrite
                || cohort_recipes.is_empty()
                || sealed_cohorts.operation_id() != preparation.operation_id()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write execution plan does not match its preparation",
                ));
            }
            selection.validate()?;
            validate_selection_against_preparation(selection, &preparation)?;
            validate_copy_on_write_plan(
                &preparation,
                selection,
                routes,
                sealed_cohorts,
                cohort_recipes,
                *max_handle_payload_bytes,
                *max_total_payload_bytes,
            )?;
        } else if preparation.strategy() == ConnectorRowMutationStrategy::CopyOnWrite {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "copy-on-write preparation requires a copy-on-write execution plan",
            ));
        }
        let digest = execution_plan_digest(&preparation, &body);
        Ok(Self {
            preparation,
            body,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(self.preparation.clone(), self.body.clone())?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "row-mutation execution plan digest does not match contents",
            ));
        }
        Ok(())
    }

    /// Verifies a provider result against the exact activation request retained
    /// by the lease caller. Any mismatch is provider corruption, not a new
    /// caller error.
    pub fn validate_against_activation(
        &self,
        request: &ConnectorRowMutationActivationRequest,
        owner: &ConnectorProviderBindingKey,
    ) -> Result<(), ConnectorError> {
        let checked = (|| {
            self.validate()?;
            if self.owner() != owner
                || self.preparation.digest() != request.preparation().digest()
                || self.preparation.operation_id() != request.preparation().operation_id()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation plan does not match the exact activation preparation",
                ));
            }
            match (&self.body, request) {
                (
                    ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
                        selection,
                        max_handle_payload_bytes,
                        max_total_payload_bytes,
                        ..
                    },
                    ConnectorRowMutationActivationRequest::CopyOnWrite {
                        selection: expected,
                        context,
                        ..
                    },
                ) if selection.digest() == expected.digest()
                    && selection.row_count() == expected.row_count()
                    && selection.byte_count() == expected.byte_count()
                    && selection.max_rows() == expected.max_rows()
                    && selection.max_bytes() == expected.max_bytes()
                    && *max_handle_payload_bytes == context.max_handle_payload_bytes()
                    && *max_total_payload_bytes == context.max_total_payload_bytes() =>
                {
                    Ok(())
                }
                (
                    ConnectorRowMutationExecutionPlanBody::Direct { .. },
                    ConnectorRowMutationActivationRequest::Direct { .. },
                ) => Ok(()),
                _ => Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation plan does not retain the exact activation selection and budgets",
                )),
            }
        })();
        checked.map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("provider returned an invalid row-mutation activation: {error}"),
            )
        })
    }

    pub fn preparation(&self) -> &ConnectorRowMutationPreparation {
        &self.preparation
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        self.preparation.owner()
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.preparation.operation_id()
    }

    pub const fn source_digest(&self) -> [u8; 32] {
        self.preparation.digest()
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn routes(&self) -> &[ConnectorRowMutationRoute] {
        match &self.body {
            ConnectorRowMutationExecutionPlanBody::Direct { routes }
            | ConnectorRowMutationExecutionPlanBody::CopyOnWrite { routes, .. } => routes,
        }
    }

    /// Returns the immutable cohort set and Provider-private rewrite recipes
    /// only for a Copy-on-Write activation.  Callers may transport and seal
    /// these values, but must not decode recipe payloads.
    pub fn copy_on_write(
        &self,
    ) -> Option<(
        &ConnectorRowMutationSelection,
        &ConnectorSealedWriteCohortSet,
        &[ConnectorRowMutationCohortRecipe],
    )> {
        match &self.body {
            ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
                selection,
                sealed_cohorts,
                cohort_recipes,
                ..
            } => Some((selection, sealed_cohorts, cohort_recipes)),
            ConnectorRowMutationExecutionPlanBody::Direct { .. } => None,
        }
    }
}

// The owned outer Vec's capacity is public and separate from Arrow source
// payload bytes. Leave its old/new headers and offset index within the one
// MiB collector slice of Internal bookkeeping before constructing offsets.
fn check_selection_headers(capacity: usize, length: usize) -> Result<(), ConnectorError> {
    let bytes = capacity
        .checked_mul(2 * size_of::<RecordBatch>())
        .and_then(|v| {
            length
                .checked_add(1)?
                .checked_mul(2 * size_of::<u64>())?
                .checked_add(v)
        });
    if bytes.is_none_or(|v| v > 1024 * 1024) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation selection batch containers exceed their bounded bookkeeping",
        ));
    }
    Ok(())
}

fn validate_selection_schema_shape(schema: &Schema) -> Result<(), ConnectorError> {
    ConnectorRowConversionFootprint::for_schema(schema).map(|_| ())
}

fn selection_size(schema: &Schema, batches: &[RecordBatch]) -> Result<(u64, u64), ConnectorError> {
    batches.iter().try_fold(
        (
            0_u64,
            ConnectorRowConversionFootprint::retained_schema_bytes(schema)? as u64,
        ),
        |(rows, bytes), batch| {
            Ok((
                rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "row-mutation selection row accounting overflowed",
                    )
                })?,
                bytes
                    .checked_add(
                        ConnectorRowConversionFootprint::retained_batch_bytes(batch)? as u64,
                    )
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "row-mutation selection byte accounting overflowed",
                        )
                    })?,
            ))
        },
    )
}

fn validate_selection_against_preparation(
    selection: &ConnectorRowMutationSelection,
    preparation: &ConnectorRowMutationPreparation,
) -> Result<(), ConnectorError> {
    let contract = preparation.match_contract();
    contract.validate_selection(selection)?;
    for ordinal in 0..selection.row_count() {
        let effect = selection_effect(
            selection,
            ConnectorRowMutationSelectionOrdinal::new(ordinal),
            contract.effect_field().target_ordinal(),
        )?;
        if !preparation.intent().accepts(effect) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation selection contains an effect outside its admitted intent",
            ));
        }
    }
    Ok(())
}

pub(super) fn selection_effect(
    selection: &ConnectorRowMutationSelection,
    ordinal: ConnectorRowMutationSelectionOrdinal,
    effect_ordinal: u32,
) -> Result<ConnectorRowMutationEffect, ConnectorError> {
    let view = selection.locate(ordinal).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation recipe selection ordinal is out of bounds",
        )
    })?;
    let array = view
        .batch()
        .column(effect_ordinal as usize)
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation effect column is not Int8",
            )
        })?;
    if array.is_null(view.row_index()) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation selection contains a null effect tag",
        ));
    }
    match array.value(view.row_index()) {
        1 => Ok(ConnectorRowMutationEffect::Delete),
        2 => Ok(ConnectorRowMutationEffect::Replace),
        3 => Ok(ConnectorRowMutationEffect::Insert),
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation selection contains an invalid effect tag",
        )),
    }
}

fn validate_recipe_body_shape(
    body: &ConnectorRowMutationCohortRecipeBody,
) -> Result<(), ConnectorError> {
    let ConnectorRowMutationCohortRecipeBody::Rewrite {
        scan_schema,
        scan_schema_digest,
        scan_bindings,
        match_tokens,
        written_version_token,
        ..
    } = body
    else {
        return Ok(());
    };
    if scan_schema.fields().is_empty()
        || scan_schema.fields().len() > MAX_CONNECTOR_ROW_MUTATION_ROUTES
        || canonical_schema_digest(scan_schema.as_ref())? != *scan_schema_digest
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation rewrite recipe has an empty or corrupt scan schema",
        ));
    }
    let mut tokens = HashSet::new();
    let mut ordinals = HashSet::new();
    if scan_bindings.is_empty()
        || scan_bindings.len() > MAX_CONNECTOR_ROW_MUTATION_ROUTES
        || scan_bindings.windows(2).any(|pair| {
            (pair[0].token(), pair[0].scan_ordinal()) >= (pair[1].token(), pair[1].scan_ordinal())
        })
        || scan_bindings.iter().any(|binding| {
            binding.scan_ordinal as usize >= scan_schema.fields().len()
                || !tokens.insert(binding.token)
                || !ordinals.insert(binding.scan_ordinal)
        })
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation rewrite scan bindings are empty, foreign, or duplicate",
        ));
    }
    let mut matches = HashSet::new();
    if match_tokens.is_empty()
        || match_tokens
            .iter()
            .any(|token| !tokens.contains(token) || !matches.insert(*token))
        || written_version_token
            .is_some_and(|token| !tokens.contains(&token) || match_tokens.contains(&token))
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "row-mutation rewrite match or written-version tokens are invalid",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_copy_on_write_plan(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    routes: &[ConnectorRowMutationRoute],
    sealed: &ConnectorSealedWriteCohortSet,
    recipes: &[ConnectorRowMutationCohortRecipe],
    max_handle_payload_bytes: usize,
    max_total_payload_bytes: usize,
) -> Result<(), ConnectorError> {
    if max_handle_payload_bytes == 0
        || max_handle_payload_bytes > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        || max_total_payload_bytes < max_handle_payload_bytes
        || max_total_payload_bytes > MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES
        || recipes.len() != routes.len()
        || recipes.len() != sealed.cohorts().len()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "copy-on-write plan has invalid budgets or non-exact route/cohort/recipe cardinality",
        ));
    }
    let route_by_id: HashMap<_, _> = routes
        .iter()
        .map(|route| (route.route_id(), route))
        .collect();
    let cohort_by_id: HashMap<_, _> = sealed
        .cohorts()
        .iter()
        .map(|cohort| (cohort.cohort_id(), cohort))
        .collect();
    let mut seen_cohorts = HashSet::new();
    let row_count = usize::try_from(selection.row_count()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation selection row count does not fit validation memory",
        )
    })?;
    let bitmap_bytes = row_count.div_ceil(8);
    if bitmap_bytes > max_total_payload_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation selection coverage bitmap exceeds the request budget",
        ));
    }
    let mut covered = vec![0_u8; bitmap_bytes];
    let mut covered_count = 0_usize;
    let mut total_bytes = usize::try_from(selection.byte_count()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation selection byte count does not fit the request budget",
        )
    })?;
    total_bytes = checked_budget_add(
        total_bytes,
        selection.batch_row_offsets.len().saturating_mul(8),
    )?;
    for recipe in recipes {
        recipe.validate()?;
        let route = route_by_id.get(&recipe.route_id()).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "copy-on-write recipe references a foreign route",
            )
        })?;
        let cohort = cohort_by_id.get(&recipe.cohort_id()).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "copy-on-write recipe references a foreign cohort",
            )
        })?;
        if route.cohort_id() != recipe.cohort_id()
            || !seen_cohorts.insert(recipe.cohort_id())
            || cohort.planning_digest() != route.preparation().digest()
            || cohort.intent() != route.preparation().intent()
            || recipe.selection_digest() != selection.digest()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "copy-on-write recipe is not exactly bound to its route, cohort, and selection",
            ));
        }
        validate_recipe_against_route(recipe, route, preparation)?;
        total_bytes = checked_budget_add(total_bytes, recipe.payload().len())?;
        total_bytes = checked_budget_add(
            total_bytes,
            recipe.selection_ordinals().len().saturating_mul(8),
        )?;
        for ordinal in recipe.selection_ordinals() {
            let ordinal_index = usize::try_from(ordinal.get()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write recipe selection ordinal does not fit this process",
                )
            })?;
            if ordinal_index >= row_count
                || (covered[ordinal_index / 8] & (1 << (ordinal_index % 8))) != 0
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write recipe selection coverage is out of bounds or overlapping",
                ));
            }
            covered[ordinal_index / 8] |= 1 << (ordinal_index % 8);
            covered_count += 1;
            let effect = selection_effect(
                selection,
                *ordinal,
                preparation.match_contract().effect_field().target_ordinal(),
            )?;
            if !route.accepted_effects().contains(&effect)
                || matches!(recipe.body(), ConnectorRowMutationCohortRecipeBody::Append)
                    != (effect == ConnectorRowMutationEffect::Insert)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "copy-on-write recipe does not exactly cover its route effects",
                ));
            }
        }
        if let ConnectorRowMutationCohortRecipeBody::Rewrite {
            source,
            scan_schema,
            scan_bindings,
            match_tokens,
            ..
        } = recipe.body()
        {
            let (_, schema_bytes) = canonical_schema_facts(scan_schema.as_ref())?;
            for item_bytes in [source.payload().len(), schema_bytes] {
                if item_bytes > max_handle_payload_bytes {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "row-mutation rewrite fact exceeds the request handle budget",
                    ));
                }
                total_bytes = checked_budget_add(total_bytes, item_bytes)?;
            }
            total_bytes = checked_budget_add(total_bytes, scan_bindings.len().saturating_mul(36))?;
            total_bytes = checked_budget_add(total_bytes, match_tokens.len().saturating_mul(32))?;
        }
        if recipe.payload().len() > max_handle_payload_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation recipe payload exceeds the request handle budget",
            ));
        }
    }
    if covered_count != row_count {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "copy-on-write recipes do not exactly cover the selection",
        ));
    }
    if total_bytes > max_total_payload_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "copy-on-write plan exceeds the total request budget",
        ));
    }
    Ok(())
}

fn validate_recipe_against_route(
    recipe: &ConnectorRowMutationCohortRecipe,
    route: &ConnectorRowMutationRoute,
    preparation: &ConnectorRowMutationPreparation,
) -> Result<(), ConnectorError> {
    match recipe.body() {
        ConnectorRowMutationCohortRecipeBody::Append => {
            if route.accepted_effects() != [ConnectorRowMutationEffect::Insert]
                || route.preparation().intent() != ConnectorWriteIntent::Append
                || route.input().fields().iter().any(|binding| {
                    !preparation
                        .match_contract()
                        .after_fields()
                        .iter()
                        .any(|field| field.token() == binding.token())
                })
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "append recipe is not exactly bound to an Insert after-image route",
                ));
            }
        }
        ConnectorRowMutationCohortRecipeBody::Rewrite {
            source,
            base_version_digest,
            scan_schema,
            scan_bindings,
            match_tokens,
            written_version_token,
            ..
        } => {
            if source.owner() != &preparation.owner().instance_id
                || *base_version_digest != preparation.base_version().digest()
                || route
                    .accepted_effects()
                    .contains(&ConnectorRowMutationEffect::Insert)
                || route.preparation().intent() != ConnectorWriteIntent::RowDelta
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "rewrite recipe has a foreign owner, base, or route effect",
                ));
            }
            let route_fields: HashMap<_, _> = route
                .input()
                .fields()
                .into_iter()
                .map(|field| (field.token(), field.field()))
                .collect();
            let binding_tokens: HashSet<_> =
                scan_bindings.iter().map(|value| value.token()).collect();
            if route_fields
                .keys()
                .any(|token| !binding_tokens.contains(token))
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "rewrite recipe does not cover every route input token",
                ));
            }
            let match_fields: HashMap<_, _> = preparation
                .match_contract()
                .identity_fields()
                .iter()
                .map(|field| (field.token(), field.field()))
                .collect();
            for binding in scan_bindings {
                let expected = route_fields
                    .get(&binding.token())
                    .copied()
                    .or_else(|| match_fields.get(&binding.token()).copied())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "rewrite scan binding token is foreign to its route and match contract",
                        )
                    })?;
                let actual = scan_schema.field(binding.scan_ordinal() as usize);
                if actual.data_type() != expected.data_type()
                    || (actual.is_nullable() && !expected.is_nullable())
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "rewrite scan binding type or nullability differs from its route token",
                    ));
                }
            }
            let expected_matches = preparation.match_contract().uniqueness_tokens();
            if match_tokens.as_slice() != expected_matches {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "rewrite recipe match tokens differ from the signed uniqueness tuple",
                ));
            }
            match written_version_token {
                Some(token) => {
                    let field = route_fields.get(token).ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "rewrite written-version token is foreign to its route",
                        )
                    })?;
                    let is_row_identity = matches!(
                        route.input(),
                        ConnectorWriteInputShape::RowLineage {
                            row_identity_fields,
                            ..
                        } if row_identity_fields.iter().any(|binding| binding.token() == *token)
                    );
                    if !binding_tokens.contains(token)
                        || expected_matches.contains(token)
                        || !is_row_identity
                        || field.data_type() != &DataType::Int64
                        || field.is_nullable()
                    {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "rewrite written-version token must be signed non-null Int64",
                        ));
                    }
                }
                None => {}
            }
        }
    }
    Ok(())
}

fn checked_budget_add(total: usize, value: usize) -> Result<usize, ConnectorError> {
    total.checked_add(value).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation plan payload accounting overflowed",
        )
    })
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

fn validate_routes(routes: &[ConnectorRowMutationRoute]) -> Result<(), ConnectorError> {
    if routes.is_empty() || routes.len() > MAX_CONNECTOR_ROW_MUTATION_ROUTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation routes must be non-empty and bounded",
        ));
    }
    let mut seen = HashSet::new();
    for route in routes {
        route.validate()?;
        if !seen.insert(route.route_id()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation execution plan has duplicate route IDs",
            ));
        }
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
#[allow(clippy::too_many_arguments)]
fn match_digest(
    owner: &ConnectorProviderBindingKey,
    table: &ConnectorTableHandle,
    base: &ConnectorWriteBaseVersion,
    identity: &[ConnectorMutationSourceField],
    before: &[ConnectorMutationTargetField],
    after: &[ConnectorMutationTargetField],
    unique: &[ConnectorWriteFieldToken],
    effect: &ConnectorMutationEffectField,
) -> Result<[u8; 32], ConnectorError> {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-match.v1\0");
    digest_owner(&mut hasher, owner);
    digest_bytes(&mut hasher, table.payload());
    hasher.update(base.digest());
    for field in identity {
        hasher.update(field.token.to_bytes());
        hasher.update(field.source_ordinal.to_be_bytes());
        match_bounds::digest_debug_field(&mut hasher, &field.field)?;
    }
    for field in before.iter().chain(after) {
        hasher.update(field.token.to_bytes());
        hasher.update(field.target_ordinal.to_be_bytes());
        match_bounds::digest_debug_field(&mut hasher, &field.field)?;
    }
    for token in unique {
        hasher.update(token.to_bytes());
    }
    hasher.update(effect.token.to_bytes());
    hasher.update(effect.target_ordinal.to_be_bytes());
    match_bounds::digest_debug_field(&mut hasher, &effect.field)?;
    Ok(hasher.finalize().into())
}
fn route_digest(
    route: ConnectorWriteRouteId,
    cohort: ConnectorWriteCohortId,
    effects: &[ConnectorRowMutationEffect],
    inputs: &[ConnectorMutationRouteInput],
    partitions: &[ConnectorWriteFieldToken],
    preparation: &ConnectorWritePreparation,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-route.v1\0");
    hasher.update(route.to_bytes());
    hasher.update(cohort.to_bytes());
    for effect in effects {
        hasher.update([effect_tag(*effect)]);
    }
    for input in inputs {
        hasher.update(input.token.to_bytes());
        hasher.update(input.input_ordinal.to_be_bytes());
    }
    for token in partitions {
        hasher.update(token.to_bytes());
    }
    hasher.update(preparation.digest());
    hasher.finalize().into()
}
/// A pinned file set is part of the cohort's identity: two cohorts that differ
/// only in which files they rewrite are two different cohorts.
fn digest_pinned_file_set(hasher: &mut Sha256, pinned: &ConnectorPinnedFileSet) {
    hasher.update(b"pinned-file-set\0");
    digest_bytes(hasher, pinned.namespace().as_bytes());
    digest_bytes(hasher, pinned.table().as_bytes());
    hasher.update(pinned.version_ordinal().to_be_bytes());
    hasher.update((pinned.files().len() as u64).to_be_bytes());
    for file in pinned.files() {
        digest_bytes(hasher, file.as_bytes());
    }
}

fn cohort_recipe_digest(
    cohort: ConnectorWriteCohortId,
    route: ConnectorWriteRouteId,
    selection_digest: [u8; 32],
    selection_ordinals: &[ConnectorRowMutationSelectionOrdinal],
    body: &ConnectorRowMutationCohortRecipeBody,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-cohort-recipe.v2\0");
    hasher.update(cohort.to_bytes());
    hasher.update(route.to_bytes());
    hasher.update(selection_digest);
    hasher.update(b"selection-ordinals\0");
    hasher.update((selection_ordinals.len() as u64).to_be_bytes());
    for ordinal in selection_ordinals {
        hasher.update(ordinal.get().to_be_bytes());
    }
    match body {
        ConnectorRowMutationCohortRecipeBody::Append => hasher.update([1]),
        ConnectorRowMutationCohortRecipeBody::Rewrite {
            source,
            pinned_source,
            base_version_digest,
            scan_schema_digest,
            scan_bindings,
            match_tokens,
            written_version_token,
            ..
        } => {
            hasher.update([2]);
            digest_bytes(&mut hasher, source.owner().as_str().as_bytes());
            digest_bytes(&mut hasher, source.payload());
            digest_pinned_file_set(&mut hasher, pinned_source);
            hasher.update(base_version_digest);
            hasher.update(scan_schema_digest);
            hasher.update(b"scan-bindings\0");
            hasher.update((scan_bindings.len() as u64).to_be_bytes());
            for binding in scan_bindings {
                hasher.update(binding.token().to_bytes());
                hasher.update(binding.scan_ordinal().to_be_bytes());
            }
            hasher.update(b"match-tokens\0");
            hasher.update((match_tokens.len() as u64).to_be_bytes());
            for token in match_tokens {
                hasher.update(token.to_bytes());
            }
            match written_version_token {
                Some(token) => {
                    hasher.update([1]);
                    hasher.update(token.to_bytes());
                }
                None => hasher.update([0]),
            }
        }
    }
    digest_bytes(&mut hasher, payload);
    hasher.finalize().into()
}
fn execution_plan_digest(
    preparation: &ConnectorRowMutationPreparation,
    body: &ConnectorRowMutationExecutionPlanBody,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-execution-plan.v2\0");
    hasher.update(preparation.digest());
    match body {
        ConnectorRowMutationExecutionPlanBody::Direct { routes } => {
            hasher.update([1]);
            hasher.update(b"routes\0");
            hasher.update((routes.len() as u64).to_be_bytes());
            for route in routes {
                hasher.update(route.digest());
            }
        }
        ConnectorRowMutationExecutionPlanBody::CopyOnWrite {
            selection,
            routes,
            sealed_cohorts,
            cohort_recipes,
            max_handle_payload_bytes,
            max_total_payload_bytes,
        } => {
            hasher.update([2]);
            hasher.update(selection.digest());
            hasher.update(max_handle_payload_bytes.to_be_bytes());
            hasher.update(max_total_payload_bytes.to_be_bytes());
            hasher.update(b"routes\0");
            hasher.update((routes.len() as u64).to_be_bytes());
            for route in routes {
                hasher.update(route.digest());
            }
            hasher.update(sealed_cohorts.digest());
            hasher.update(b"recipes\0");
            hasher.update((cohort_recipes.len() as u64).to_be_bytes());
            for recipe in cohort_recipes {
                hasher.update(recipe.digest());
            }
        }
    }
    hasher.finalize().into()
}
// Same reasoning as `match_digest`: these are the sealed digest's fields, not a
// parameter list that wants grouping.
#[allow(clippy::too_many_arguments)]
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
fn selection_digest(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    rows: u64,
) -> Result<[u8; 32], ConnectorError> {
    let footprint = ConnectorRowConversionFootprint::for_schema(schema.as_ref())?;
    let (_, retained_bytes) = selection_size(schema.as_ref(), batches)?;
    let retained_bytes = usize::try_from(retained_bytes).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation selection retained byte accounting overflowed",
        )
    })?;
    if rows > 1_048_576
        || retained_bytes > 64 * 1024 * 1024
        || batches.len() > MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES
        || retained_bytes
            .checked_add(footprint.constructor_peak_bytes)
            .is_none_or(|n| n > MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "row-mutation selection exceeds its bounded conversion workspace",
        ));
    }
    // All immutable batches pass one traversal budget before Arrow's
    // constructor can allocate synthetic null arrays. No encoded Rows or
    // data copies are retained across batches.
    // A signed consumer may retain its bounded validator converter (32 MiB)
    // and all Internal bookkeeping (8 MiB) while this digest runs. Reserve
    // that coexistence before constructing a second converter.
    const OTHER_CONSUMER_BYTES: usize = 40 * 1024 * 1024;
    footprint.checked_constructor_peak_with(retained_bytes, OTHER_CONSUMER_BYTES)?;
    footprint.preflight_batches(
        batches,
        retained_bytes
            .checked_add(OTHER_CONSUMER_BYTES)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation selection bookkeeping accounting overflowed",
                )
            })?,
    )?;
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-row-mutation-selection.v2\0");
    hasher.update(rows.to_be_bytes());
    hasher.update(canonical_schema_digest(schema.as_ref())?);
    #[cfg(test)]
    ROW_CONVERSION_CALLS.with(|n| n.set(n.get() + 1));
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("row-mutation selection schema cannot be canonicalized: {error}"),
        )
    })?;
    for batch in batches {
        #[cfg(test)]
        ROW_CONVERSION_CALLS.with(|n| n.set(n.get() + 1));
        let logical_rows = converter
            .convert_columns(batch.columns())
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("row-mutation selection values cannot be canonicalized: {error}"),
                )
            })?;
        for row in logical_rows.iter() {
            digest_bytes(&mut hasher, row.as_ref());
        }
    }
    Ok(hasher.finalize().into())
}

fn canonical_schema_digest(schema: &Schema) -> Result<[u8; 32], ConnectorError> {
    canonical_schema_facts(schema).map(|(digest, _)| digest)
}

fn canonical_schema_facts(schema: &Schema) -> Result<([u8; 32], usize), ConnectorError> {
    validate_selection_schema_shape(schema)?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_usize;
    hasher.update(b"novarocks.connector-row-mutation-arrow-schema.v1\0");
    digest_metadata(&mut hasher, schema.metadata(), &mut bytes)?;
    hasher.update((schema.fields().len() as u64).to_be_bytes());
    for field in schema.fields() {
        canonical_field_hash(&mut hasher, field.as_ref(), &mut bytes)?;
    }
    Ok((hasher.finalize().into(), bytes))
}

fn canonical_field_hash(
    hasher: &mut Sha256,
    field: &Field,
    bytes: &mut usize,
) -> Result<(), ConnectorError> {
    digest_bytes(hasher, field.name().as_bytes());
    *bytes = checked_budget_add(*bytes, field.name().len())?;
    hasher.update([u8::from(field.is_nullable())]);
    canonical_data_type_hash(hasher, field.data_type(), bytes)?;
    digest_metadata(hasher, field.metadata(), bytes)?;
    Ok(())
}

fn canonical_data_type_hash(
    hasher: &mut Sha256,
    data_type: &DataType,
    bytes: &mut usize,
) -> Result<(), ConnectorError> {
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
        // Dictionary key width and assignment are physical encoding facts.
        // Hash only the logical value type so equivalent dictionaries seal
        // identically.
        DataType::Dictionary(_, value) => {
            return canonical_data_type_hash(hasher, value, bytes);
        }
        DataType::Decimal32(_, _) => 35,
        DataType::Decimal64(_, _) => 36,
        DataType::Decimal128(_, _) => 37,
        DataType::Decimal256(_, _) => 38,
        DataType::Map(_, _) => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation selection Map values are not canonically supported",
            ));
        }
        DataType::RunEndEncoded(_, _) => 40,
    };
    hasher.update([tag]);
    *bytes = checked_budget_add(*bytes, 1)?;
    match data_type {
        DataType::Timestamp(unit, timezone) => {
            hash_time_unit(hasher, *unit);
            match timezone {
                Some(timezone) => {
                    hasher.update([1]);
                    digest_bytes(hasher, timezone.as_bytes());
                    *bytes = checked_budget_add(*bytes, timezone.len())?;
                }
                None => hasher.update([0]),
            }
        }
        DataType::Time32(unit) | DataType::Time64(unit) | DataType::Duration(unit) => {
            hash_time_unit(hasher, *unit);
        }
        DataType::Interval(unit) => hasher.update([match unit {
            IntervalUnit::YearMonth => 1,
            IntervalUnit::DayTime => 2,
            IntervalUnit::MonthDayNano => 3,
        }]),
        DataType::FixedSizeBinary(size) => hasher.update(size.to_be_bytes()),
        DataType::List(child)
        | DataType::ListView(child)
        | DataType::LargeList(child)
        | DataType::LargeListView(child) => canonical_field_hash(hasher, child, bytes)?,
        DataType::FixedSizeList(child, size) => {
            hasher.update(size.to_be_bytes());
            canonical_field_hash(hasher, child, bytes)?;
        }
        DataType::Struct(fields) => {
            hasher.update((fields.len() as u64).to_be_bytes());
            for child in fields {
                canonical_field_hash(hasher, child, bytes)?;
            }
        }
        DataType::Union(fields, mode) => {
            hasher.update([match mode {
                UnionMode::Sparse => 1,
                UnionMode::Dense => 2,
            }]);
            for (type_id, child) in fields.iter() {
                hasher.update(type_id.to_be_bytes());
                canonical_field_hash(hasher, child, bytes)?;
            }
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            hasher.update([*precision]);
            hasher.update(scale.to_be_bytes());
        }
        DataType::RunEndEncoded(run_ends, values) => {
            canonical_field_hash(hasher, run_ends, bytes)?;
            canonical_field_hash(hasher, values, bytes)?;
        }
        _ => {}
    }
    Ok(())
}

fn hash_time_unit(hasher: &mut Sha256, unit: TimeUnit) {
    hasher.update([match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }]);
}

fn digest_metadata(
    hasher: &mut Sha256,
    metadata: &HashMap<String, String>,
    bytes: &mut usize,
) -> Result<(), ConnectorError> {
    let mut keys: Vec<_> = metadata.keys().collect();
    keys.sort();
    hasher.update((keys.len() as u64).to_be_bytes());
    for key in keys {
        let value = &metadata[key];
        digest_bytes(hasher, key.as_bytes());
        digest_bytes(hasher, value.as_bytes());
        *bytes = checked_budget_add(*bytes, key.len())?;
        *bytes = checked_budget_add(*bytes, value.len())?;
    }
    Ok(())
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
        ConnectorRowMutationStrategy::CopyOnWrite => 4,
        ConnectorRowMutationStrategy::EqualityDelete => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, DictionaryArray, Int16Array, Int64Array, ListArray, StringArray};
    use arrow::datatypes::{Int8Type, Int16Type, Int32Type, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            crate::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("context")
    }

    fn match_source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    #[test]
    fn selection_is_non_concat_and_bounded() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .expect("batch");
        let bytes = selection_size(schema.as_ref(), std::slice::from_ref(&batch))
            .expect("bounded source")
            .1;
        let selection = ConnectorRowMutationSelection::try_new(
            Arc::clone(&schema),
            vec![batch.clone()],
            2,
            bytes,
        )
        .expect("selection");
        assert_eq!(selection.batches().len(), 1);
        assert_eq!(selection.row_count(), 2);
        assert_eq!(
            selection
                .locate(ConnectorRowMutationSelectionOrdinal::new(1))
                .expect("located")
                .row_index(),
            1
        );
        assert_eq!(
            ConnectorRowMutationSelection::try_new(schema, vec![batch], 1, bytes)
                .expect_err("rows")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn selection_digest_binds_logical_values_not_batch_or_slice_layout() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
            Some(vec![Some(3)]),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("items", list.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(list),
            ],
        )
        .expect("batch");
        let bytes = batch.get_array_memory_size() as u64;
        let whole = ConnectorRowMutationSelection::try_new(
            Arc::clone(&schema),
            vec![batch.clone()],
            10,
            bytes * 2,
        )
        .expect("whole");
        let split = ConnectorRowMutationSelection::try_new(
            Arc::clone(&schema),
            vec![batch.slice(0, 1), batch.slice(1, 2)],
            20,
            bytes * 3,
        )
        .expect("split");
        assert_eq!(whole.digest(), split.digest());
        assert_eq!(whole.digest(), whole.clone().digest());
        assert_eq!(
            split
                .locate(ConnectorRowMutationSelectionOrdinal::new(2))
                .expect("located")
                .batch_index(),
            1
        );

        let changed = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 9, 3])),
                batch.column(1).clone(),
                batch.column(2).clone(),
            ],
        )
        .expect("changed");
        let changed = ConnectorRowMutationSelection::try_new(schema, vec![changed], 10, bytes * 2)
            .expect("changed selection");
        assert_ne!(whole.digest(), changed.digest());
    }

    #[test]
    fn selection_digest_hydrates_dictionary_values() {
        let first: DictionaryArray<Int8Type> = DictionaryArray::try_new(
            arrow::array::Int8Array::from(vec![0, 1, 0]),
            Arc::new(StringArray::from(vec!["a", "b", "unused"])),
        )
        .expect("first dictionary");
        let second: DictionaryArray<Int16Type> = DictionaryArray::try_new(
            Int16Array::from(vec![1, 0, 1]),
            Arc::new(StringArray::from(vec!["b", "a", "different-unused"])),
        )
        .expect("second dictionary");
        let first_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            first.data_type().clone(),
            false,
        )]));
        let second_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            second.data_type().clone(),
            false,
        )]));
        let first_batch =
            RecordBatch::try_new(first_schema.clone(), vec![Arc::new(first)]).expect("first batch");
        let second_batch = RecordBatch::try_new(second_schema.clone(), vec![Arc::new(second)])
            .expect("second batch");
        let first =
            ConnectorRowMutationSelection::try_new(first_schema, vec![first_batch], 3, 4096)
                .expect("first selection");
        let second =
            ConnectorRowMutationSelection::try_new(second_schema, vec![second_batch], 3, 4096)
                .expect("second selection");
        assert_eq!(first.digest(), second.digest());
    }

    #[test]
    fn empty_selection_retains_explicit_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let selection = ConnectorRowMutationSelection::try_new(schema.clone(), vec![], 1, 4096)
            .expect("empty selection");
        assert_eq!(selection.schema(), &schema);
        selection.validate().expect("valid empty selection");
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
    }

    #[test]
    fn append_recipe_enforces_exact_coverage_and_activation_selection() {
        let instance = super::super::ConnectorInstanceId::parse("iceberg").expect("instance");
        let owner = ConnectorProviderBindingKey {
            instance_id: instance.clone(),
            incarnation: super::super::ProviderBindingEpoch::from_bytes([8; 16]),
        };
        let table =
            ConnectorTableHandle::try_new(instance, Bytes::from_static(b"table")).expect("table");
        let base = ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).expect("base");
        let identity = ConnectorWriteFieldToken::from_bytes([1; 32]);
        let after = ConnectorWriteFieldToken::from_bytes([2; 32]);
        let effect = ConnectorWriteFieldToken::from_bytes([3; 32]);
        let match_contract = ConnectorMutationMatchContract::try_new(
            owner.clone(),
            table.clone(),
            base.clone(),
            vec![ConnectorMutationSourceField::new(
                identity,
                Field::new("id", DataType::Int64, true),
                0,
            )],
            vec![],
            vec![ConnectorMutationTargetField::new(
                after,
                Field::new("value", DataType::Int64, false),
                1,
            )],
            vec![identity],
            ConnectorMutationEffectField::try_new(
                effect,
                Field::new("effect", DataType::Int8, false),
                2,
            )
            .expect("effect"),
        )
        .expect("match contract");
        let operation = ConnectorWriteOperationId::new();
        let preparation = ConnectorRowMutationPreparation::try_new(
            owner.clone(),
            operation,
            table.clone(),
            table.clone(),
            match_source_schema(),
            ConnectorWriteTargetRef::main(),
            ConnectorRowMutationIntent::Merge {
                effects: vec![ConnectorRowMutationEffect::Insert],
            },
            base.clone(),
            match_contract.clone(),
            ConnectorRowMutationStrategy::CopyOnWrite,
            None,
            Bytes::new(),
        )
        .expect("preparation");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Int64, false),
            Field::new("effect", DataType::Int8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![11, 12])),
                Arc::new(arrow::array::Int8Array::from(vec![3, 3])),
            ],
        )
        .expect("batch");
        let selection =
            ConnectorRowMutationSelection::try_new(schema.clone(), vec![batch], 2, 4096)
                .expect("selection");
        let null_effect_schema = Arc::new(Schema::new(vec![Field::new(
            "effect",
            DataType::Int8,
            true,
        )]));
        let null_effect_batch = RecordBatch::try_new(
            null_effect_schema.clone(),
            vec![Arc::new(Int8Array::from(vec![None]))],
        )
        .expect("null-effect batch");
        let null_effect_selection = ConnectorRowMutationSelection::try_new(
            null_effect_schema,
            vec![null_effect_batch],
            1,
            4096,
        )
        .expect("null-effect selection");
        assert_eq!(
            selection_effect(
                &null_effect_selection,
                ConnectorRowMutationSelectionOrdinal::new(0),
                0,
            )
            .expect_err("null effect")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let input = ConnectorWriteInputShape::Data {
            fields: vec![super::super::ConnectorWriteFieldBinding::new(
                after,
                Field::new("value", DataType::Int64, false),
            )],
        };
        let writer = ConnectorWritePreparation::try_new(
            owner.clone(),
            table.clone(),
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Append,
            base.clone(),
            input.clone(),
            Bytes::new(),
        )
        .expect("writer");
        let cohort = ConnectorWriteCohortId::derive(operation, b"append", [4; 32]).expect("cohort");
        let route = ConnectorRowMutationRoute::try_new(
            ConnectorWriteRouteId::from_bytes([5; 32]),
            cohort,
            vec![ConnectorRowMutationEffect::Insert],
            input.clone(),
            vec![ConnectorMutationRouteInput::new(after, 0)],
            vec![],
            writer.clone(),
        )
        .expect("route");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation,
            vec![super::super::ConnectorWriteCohortDescriptor::new(
                cohort,
                ConnectorWriteIntent::Append,
                writer.digest(),
            )],
        )
        .expect("sealed");
        assert_eq!(
            ConnectorRowMutationCohortRecipe::try_append(
                cohort,
                route.route_id(),
                &selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(2)],
                Bytes::new(),
            )
            .err()
            .expect("out of bounds")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let partial = ConnectorRowMutationCohortRecipe::try_append(
            cohort,
            route.route_id(),
            &selection,
            vec![ConnectorRowMutationSelectionOrdinal::new(0)],
            Bytes::new(),
        )
        .expect("partial");
        assert_eq!(
            ConnectorRowMutationExecutionPlan::try_copy_on_write(
                preparation.clone(),
                selection.clone(),
                vec![route.clone()],
                sealed.clone(),
                vec![partial],
                &request_context(),
            )
            .err()
            .expect("partial coverage")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let exact = ConnectorRowMutationCohortRecipe::try_append(
            cohort,
            route.route_id(),
            &selection,
            vec![
                ConnectorRowMutationSelectionOrdinal::new(0),
                ConnectorRowMutationSelectionOrdinal::new(1),
            ],
            Bytes::new(),
        )
        .expect("exact");
        let plan = ConnectorRowMutationExecutionPlan::try_copy_on_write(
            preparation.clone(),
            selection.clone(),
            vec![route.clone()],
            sealed.clone(),
            vec![exact.clone()],
            &request_context(),
        )
        .expect("plan");
        let exact_request = ConnectorRowMutationActivationRequest::CopyOnWrite {
            preparation: preparation.clone(),
            selection: selection.clone(),
            context: request_context(),
        };
        plan.validate_against_activation(&exact_request, &owner)
            .expect("exact activation");
        let low_cohort =
            ConnectorWriteCohortId::derive(operation, b"append-low", [8; 32]).expect("low cohort");
        let low_route = ConnectorRowMutationRoute::try_new(
            ConnectorWriteRouteId::from_bytes([4; 32]),
            low_cohort,
            vec![ConnectorRowMutationEffect::Insert],
            input,
            vec![ConnectorMutationRouteInput::new(after, 0)],
            vec![],
            writer.clone(),
        )
        .expect("low route");
        let two_sealed = ConnectorSealedWriteCohortSet::try_new(
            operation,
            vec![
                super::super::ConnectorWriteCohortDescriptor::new(
                    cohort,
                    ConnectorWriteIntent::Append,
                    writer.digest(),
                ),
                super::super::ConnectorWriteCohortDescriptor::new(
                    low_cohort,
                    ConnectorWriteIntent::Append,
                    writer.digest(),
                ),
            ],
        )
        .expect("two sealed");
        let high_recipe = ConnectorRowMutationCohortRecipe::try_append(
            cohort,
            route.route_id(),
            &selection,
            vec![ConnectorRowMutationSelectionOrdinal::new(1)],
            Bytes::new(),
        )
        .expect("high recipe");
        let low_recipe = ConnectorRowMutationCohortRecipe::try_append(
            low_cohort,
            low_route.route_id(),
            &selection,
            vec![ConnectorRowMutationSelectionOrdinal::new(0)],
            Bytes::new(),
        )
        .expect("low recipe");
        let reversed = ConnectorRowMutationExecutionPlan::try_copy_on_write(
            preparation.clone(),
            selection.clone(),
            vec![route.clone(), low_route.clone()],
            two_sealed.clone(),
            vec![high_recipe.clone(), low_recipe.clone()],
            &request_context(),
        )
        .expect("reversed plan");
        let ordered = ConnectorRowMutationExecutionPlan::try_copy_on_write(
            preparation.clone(),
            selection.clone(),
            vec![low_route, route.clone()],
            two_sealed,
            vec![low_recipe, high_recipe],
            &request_context(),
        )
        .expect("ordered plan");
        assert_eq!(reversed.digest(), ordered.digest());
        assert!(reversed.routes()[0].route_id() < reversed.routes()[1].route_id());
        let (_, _, recipes) = reversed.copy_on_write().expect("COW body");
        assert!(recipes[0].cohort_id() < recipes[1].cohort_id());
        let drifted = ConnectorRowMutationActivationRequest::CopyOnWrite {
            preparation,
            selection: ConnectorRowMutationSelection::try_new(
                selection.schema().clone(),
                vec![],
                2,
                4096,
            )
            .expect("drifted selection"),
            context: request_context(),
        };
        assert_eq!(
            plan.validate_against_activation(&drifted, &owner)
                .expect_err("provider drift")
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let written = ConnectorWriteFieldToken::from_bytes([4; 32]);
        let rewrite_preparation = ConnectorRowMutationPreparation::try_new(
            owner.clone(),
            operation,
            table.clone(),
            table.clone(),
            match_source_schema(),
            ConnectorWriteTargetRef::main(),
            ConnectorRowMutationIntent::Update,
            base.clone(),
            match_contract,
            ConnectorRowMutationStrategy::CopyOnWrite,
            Some(41),
            Bytes::new(),
        )
        .expect("rewrite preparation");
        let rewrite_selection_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Int64, false),
            Field::new("effect", DataType::Int8, false),
        ]));
        let rewrite_selection_batch = RecordBatch::try_new(
            rewrite_selection_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![11])),
                Arc::new(Int8Array::from(vec![2])),
            ],
        )
        .expect("rewrite selection batch");
        let rewrite_selection = ConnectorRowMutationSelection::try_new(
            rewrite_selection_schema,
            vec![rewrite_selection_batch],
            1,
            4096,
        )
        .expect("rewrite selection");
        let rewrite_input = ConnectorWriteInputShape::RowLineage {
            data_fields: vec![super::super::ConnectorWriteFieldBinding::new(
                after,
                Field::new("value", DataType::Int64, false),
            )],
            row_identity_fields: vec![
                super::super::ConnectorWriteFieldBinding::new(
                    identity,
                    Field::new("id", DataType::Int64, true),
                ),
                super::super::ConnectorWriteFieldBinding::new(
                    written,
                    Field::new("version", DataType::Int64, false),
                ),
            ],
        };
        let rewrite_writer = ConnectorWritePreparation::try_new(
            owner.clone(),
            table.clone(),
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::RowDelta,
            base.clone(),
            rewrite_input.clone(),
            Bytes::new(),
        )
        .expect("rewrite writer");
        let rewrite_cohort =
            ConnectorWriteCohortId::derive(operation, b"rewrite", [6; 32]).expect("cohort");
        let rewrite_route = ConnectorRowMutationRoute::try_new(
            ConnectorWriteRouteId::from_bytes([7; 32]),
            rewrite_cohort,
            vec![ConnectorRowMutationEffect::Replace],
            rewrite_input,
            vec![
                ConnectorMutationRouteInput::new(after, 0),
                ConnectorMutationRouteInput::new(identity, 1),
                ConnectorMutationRouteInput::new(written, 2),
            ],
            vec![],
            rewrite_writer.clone(),
        )
        .expect("rewrite route");
        let rewrite_sealed = ConnectorSealedWriteCohortSet::try_new(
            operation,
            vec![super::super::ConnectorWriteCohortDescriptor::new(
                rewrite_cohort,
                ConnectorWriteIntent::RowDelta,
                rewrite_writer.digest(),
            )],
        )
        .expect("rewrite sealed");
        let scan_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("version", DataType::Int64, false),
        ]));
        let source = ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            Bytes::from_static(b"frozen-source"),
        )
        .expect("source");
        let pinned_source =
            ConnectorPinnedFileSet::try_new("db", "t", 11, ["s3://w/db/t/a.parquet"])
                .expect("pinned source");
        let rewrite_recipe = ConnectorRowMutationCohortRecipe::try_rewrite(
            rewrite_cohort,
            rewrite_route.route_id(),
            &rewrite_selection,
            vec![ConnectorRowMutationSelectionOrdinal::new(0)],
            source.clone(),
            pinned_source.clone(),
            base.digest(),
            scan_schema.clone(),
            vec![
                ConnectorRowMutationScanBinding::new(written, 2),
                ConnectorRowMutationScanBinding::new(after, 0),
                ConnectorRowMutationScanBinding::new(identity, 1),
            ],
            vec![identity],
            Some(written),
            Bytes::new(),
        )
        .expect("rewrite recipe");
        let ConnectorRowMutationCohortRecipeBody::Rewrite { scan_bindings, .. } =
            rewrite_recipe.body()
        else {
            panic!("rewrite body")
        };
        assert!(scan_bindings.windows(2).all(|pair| {
            (pair[0].token(), pair[0].scan_ordinal()) < (pair[1].token(), pair[1].scan_ordinal())
        }));
        let canonical_recipe = ConnectorRowMutationCohortRecipe::try_rewrite(
            rewrite_cohort,
            rewrite_route.route_id(),
            &rewrite_selection,
            vec![ConnectorRowMutationSelectionOrdinal::new(0)],
            source.clone(),
            pinned_source.clone(),
            base.digest(),
            scan_schema.clone(),
            vec![
                ConnectorRowMutationScanBinding::new(identity, 1),
                ConnectorRowMutationScanBinding::new(after, 0),
                ConnectorRowMutationScanBinding::new(written, 2),
            ],
            vec![identity],
            Some(written),
            Bytes::new(),
        )
        .expect("canonical rewrite recipe");
        assert_eq!(rewrite_recipe.digest(), canonical_recipe.digest());
        ConnectorRowMutationExecutionPlan::try_copy_on_write(
            rewrite_preparation.clone(),
            rewrite_selection.clone(),
            vec![rewrite_route.clone()],
            rewrite_sealed.clone(),
            vec![rewrite_recipe.clone()],
            &request_context(),
        )
        .expect("rewrite plan");

        for invalid in [
            ConnectorRowMutationCohortRecipe::try_rewrite(
                rewrite_cohort,
                rewrite_route.route_id(),
                &rewrite_selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(0)],
                source.clone(),
                pinned_source.clone(),
                [9; 32],
                scan_schema.clone(),
                scan_bindings.clone(),
                vec![identity],
                Some(written),
                Bytes::new(),
            )
            .expect("base-drift recipe"),
            ConnectorRowMutationCohortRecipe::try_rewrite(
                rewrite_cohort,
                rewrite_route.route_id(),
                &rewrite_selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(0)],
                ConnectorTableHandle::try_new(
                    super::super::ConnectorInstanceId::parse("foreign").expect("foreign"),
                    Bytes::new(),
                )
                .expect("foreign source"),
                pinned_source.clone(),
                base.digest(),
                scan_schema.clone(),
                scan_bindings.clone(),
                vec![identity],
                Some(written),
                Bytes::new(),
            )
            .expect("owner-drift recipe"),
            ConnectorRowMutationCohortRecipe::try_rewrite(
                rewrite_cohort,
                rewrite_route.route_id(),
                &rewrite_selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(0)],
                source.clone(),
                pinned_source.clone(),
                base.digest(),
                Arc::new(Schema::new(vec![
                    Field::new("value", DataType::Utf8, false),
                    Field::new("id", DataType::Int64, false),
                    Field::new("version", DataType::Int64, false),
                ])),
                scan_bindings.clone(),
                vec![identity],
                Some(written),
                Bytes::new(),
            )
            .expect("type-drift recipe"),
            ConnectorRowMutationCohortRecipe::try_rewrite(
                rewrite_cohort,
                rewrite_route.route_id(),
                &rewrite_selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(0)],
                source.clone(),
                pinned_source.clone(),
                base.digest(),
                Arc::new(Schema::new(vec![
                    Field::new("value", DataType::Int64, false),
                    Field::new("id", DataType::Int64, false),
                    Field::new("version", DataType::Int64, true),
                ])),
                scan_bindings.clone(),
                vec![identity],
                Some(written),
                Bytes::new(),
            )
            .expect("nullable-widening recipe"),
        ] {
            assert_eq!(
                ConnectorRowMutationExecutionPlan::try_copy_on_write(
                    rewrite_preparation.clone(),
                    rewrite_selection.clone(),
                    vec![rewrite_route.clone()],
                    rewrite_sealed.clone(),
                    vec![invalid],
                    &request_context(),
                )
                .err()
                .expect("recipe drift")
                .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
        assert_eq!(
            ConnectorRowMutationCohortRecipe::try_rewrite(
                rewrite_cohort,
                rewrite_route.route_id(),
                &rewrite_selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(0)],
                source.clone(),
                pinned_source.clone(),
                base.digest(),
                scan_schema.clone(),
                scan_bindings.clone(),
                vec![identity],
                Some(identity),
                Bytes::new(),
            )
            .err()
            .expect("written token overlaps match token")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorRowMutationCohortRecipe::try_rewrite(
                rewrite_cohort,
                rewrite_route.route_id(),
                &rewrite_selection,
                vec![ConnectorRowMutationSelectionOrdinal::new(0)],
                source,
                pinned_source,
                base.digest(),
                scan_schema,
                scan_bindings.clone(),
                vec![],
                Some(written),
                Bytes::new(),
            )
            .err()
            .expect("missing match binding")
            .kind(),
            ConnectorErrorKind::InvalidRequest
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
            vec![ConnectorMutationSourceField::new(
                identity_token,
                Field::new("id", DataType::Int64, false),
                0,
            )],
            Vec::new(),
            Vec::new(),
            vec![identity_token],
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
    fn execution_plan_canonicalizes_route_input_order() {
        let preparation = preparation_with_base_version_ordinal(None);
        let token = preparation.match_contract().identity_fields()[0].token();
        let input = ConnectorWriteInputShape::Data {
            fields: vec![super::super::ConnectorWriteFieldBinding::new(
                token,
                Field::new("id", DataType::Int64, false),
            )],
        };
        let writer = ConnectorWritePreparation::try_new(
            preparation.owner().clone(),
            preparation.table().clone(),
            preparation.target_ref().clone(),
            ConnectorWriteIntent::RowDelta,
            preparation.base_version().clone(),
            input.clone(),
            Bytes::new(),
        )
        .expect("writer");
        let make_route = |route_byte, cohort_byte| {
            let cohort = ConnectorWriteCohortId::derive(
                preparation.operation_id(),
                b"canonical-route",
                [cohort_byte; 32],
            )
            .expect("cohort");
            ConnectorRowMutationRoute::try_new(
                ConnectorWriteRouteId::from_bytes([route_byte; 32]),
                cohort,
                vec![ConnectorRowMutationEffect::Delete],
                input.clone(),
                vec![ConnectorMutationRouteInput::new(token, 0)],
                vec![],
                writer.clone(),
            )
            .expect("route")
        };
        let low = make_route(4, 4);
        let high = make_route(9, 9);
        let ascending = ConnectorRowMutationExecutionPlan::try_direct(
            preparation.clone(),
            vec![low.clone(), high.clone()],
        )
        .expect("ascending");
        let descending =
            ConnectorRowMutationExecutionPlan::try_direct(preparation, vec![high, low])
                .expect("descending");
        assert_eq!(ascending.digest(), descending.digest());
        assert_eq!(
            descending
                .routes()
                .iter()
                .map(ConnectorRowMutationRoute::route_id)
                .collect::<Vec<_>>(),
            ascending
                .routes()
                .iter()
                .map(ConnectorRowMutationRoute::route_id)
                .collect::<Vec<_>>()
        );
        assert!(descending.routes()[0].route_id() < descending.routes()[1].route_id());
    }

    #[test]
    fn route_allows_distinct_tokens_to_share_one_producer_ordinal() {
        let preparation = preparation_with_base_version_ordinal(None);
        let first = ConnectorWriteFieldToken::from_bytes([31; 32]);
        let second = ConnectorWriteFieldToken::from_bytes([32; 32]);
        let input = ConnectorWriteInputShape::Data {
            fields: vec![
                super::super::ConnectorWriteFieldBinding::new(
                    first,
                    Field::new("primary", DataType::Int64, false),
                ),
                super::super::ConnectorWriteFieldBinding::new(
                    second,
                    Field::new("alias", DataType::Int64, false),
                ),
            ],
        };
        let writer = ConnectorWritePreparation::try_new(
            preparation.owner().clone(),
            preparation.table().clone(),
            preparation.target_ref().clone(),
            ConnectorWriteIntent::RowDelta,
            preparation.base_version().clone(),
            input.clone(),
            Bytes::new(),
        )
        .expect("writer");
        let cohort = ConnectorWriteCohortId::derive(
            preparation.operation_id(),
            b"repeated-route-occurrence",
            [33; 32],
        )
        .expect("cohort");

        let route = ConnectorRowMutationRoute::try_new(
            ConnectorWriteRouteId::from_bytes([34; 32]),
            cohort,
            vec![ConnectorRowMutationEffect::Delete],
            input,
            vec![
                ConnectorMutationRouteInput::new(first, 0),
                ConnectorMutationRouteInput::new(second, 0),
            ],
            vec![second],
            writer,
        )
        .expect("distinct route tokens may share one producer occurrence");

        assert_eq!(route.input_ordinals()[0].input_ordinal(), 0);
        assert_eq!(route.input_ordinals()[1].input_ordinal(), 0);
        assert_eq!(route.partition_fields(), &[second]);
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
#[path = "row_mutation/source_receipt_tests.rs"]
mod source_receipt_tests;
