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
use crate::{
    ControlExpansions, ExprId, ExprNode, ExpressionCallClaim, PhysicalCallRequest, ValueId,
    ValueType,
};
use novarocks_type_contract::{
    ControlShape, EvaluationDemand, ExpressionControlFlow, ExpressionUseId,
    TemporalSourceDefinitions,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RootOrdinal(u32);
impl RootOrdinal {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootScope {
    Port(u32),
    AllPorts,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ExprPort {
    pub values: Box<[(ValueId, ValueType)]>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExprRoot {
    pub definition: ExprId,
    pub demand: EvaluationDemand,
    pub scope: RootScope,
}
/// Only the existing semantic depth is supplied by the enclosing owner.
/// Control definition/use/reference limits retain their shared constants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnitLimits {
    pub expression_semantic_depth: usize,
}
/// Authenticated construction material, never a retained second authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefinitionControl {
    pub shape: ControlShape,
    pub temporal_sources: Option<TemporalSourceDefinitions<ExprId>>,
}
/// Neutral immutable-construction input; not a generated DTO or SQL credential.
/// Every field and cross-reference is checked before producing BoundExprSet.
pub struct UnitParts {
    pub ports: Box<[ExprPort]>,
    pub definitions: Box<[ExprNode]>,
    pub roots: Box<[ExprRoot]>,
    pub root_uses: Box<[ExpressionUseId]>,
    pub control: ExpressionControlFlow<ExprId>,
    pub(crate) expansions: ControlExpansions,
    pub claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
    pub requests: BTreeMap<ExprId, PhysicalCallRequest>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
#[derive(Debug)]
pub struct BoundExprSet {
    ports: Box<[ExprPort]>,
    definitions: Box<[ExprNode]>,
    roots: Box<[ExprRoot]>,
    root_uses: Box<[ExpressionUseId]>,
    control: ExpressionControlFlow<ExprId>,
    expansions: ControlExpansions,
    claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
    requests: BTreeMap<ExprId, PhysicalCallRequest>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl BoundExprSet {
    pub fn ports(&self) -> &[ExprPort] {
        &self.ports
    }
    pub fn definitions(&self) -> &[ExprNode] {
        &self.definitions
    }
    pub fn definition(&self, id: ExprId) -> Option<&ExprNode> {
        self.definitions.get(id.get() as usize)
    }
    pub fn roots(&self) -> &[ExprRoot] {
        &self.roots
    }
    pub fn root(&self, ordinal: RootOrdinal) -> Option<&ExprRoot> {
        self.roots.get(ordinal.get() as usize)
    }
    pub fn root_use_ids(&self) -> &[ExpressionUseId] {
        &self.root_uses
    }
    pub fn root_use(&self, ordinal: RootOrdinal) -> Option<ExpressionUseId> {
        self.root_uses.get(ordinal.get() as usize).copied()
    }
    pub fn control_flow(&self) -> &ExpressionControlFlow<ExprId> {
        &self.control
    }
    pub fn control_expansions(&self) -> &ControlExpansions {
        &self.expansions
    }
    pub fn claims(&self) -> &BTreeMap<ExpressionUseId, ExpressionCallClaim> {
        &self.claims
    }
    pub fn requests(&self) -> &BTreeMap<ExprId, PhysicalCallRequest> {
        &self.requests
    }
}

impl BoundExprSet {
    /// Validate neutral immutable parts without inferring SQL provenance or
    /// requiring the producer's numeric use/region allocation order.
    /// The caller owns source/header admission, entry and completion.
    pub fn try_from_parts_in(
        parts: UnitParts,
        limits: UnitLimits,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitError> {
        crate::validation::validate(
            &parts.ports,
            &parts.definitions,
            &parts.roots,
            &parts.expansions,
            limits,
            work,
        )?;
        Self::finish_sealed_parts(parts, work)
    }
    pub(crate) fn finish_sealed_parts(
        parts: UnitParts,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitError> {
        crate::validation_calls::validate(&parts, work)?;
        Ok(Self::from_validated_parts(parts))
    }
    fn from_validated_parts(parts: UnitParts) -> Self {
        Self {
            ports: parts.ports,
            definitions: parts.definitions,
            roots: parts.roots,
            root_uses: parts.root_uses,
            control: parts.control,
            expansions: parts.expansions,
            claims: parts.claims,
            requests: parts.requests,
            storage: parts.storage,
        }
    }
    /// Validate existing immutable parts using the caller's original scratch
    /// scope and a separate retained scope for an escaping diagnostic. Input
    /// payloads and an already checked control flow are never reconstructed.
    pub fn try_from_parts_with_creator_in<H>(
        parts: UnitParts,
        limits: UnitLimits,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::new(temporary, retained);
        Self::try_from_parts_created_in(parts, limits, &mut creation, work)
    }
    /// The same validation core may borrow one actual caller scope for both
    /// scratch and diagnostic storage. No second scope or identity is minted;
    /// the original host retains its own conservative lifetime/refund policy.
    pub fn try_from_parts_with_single_creator_in<H>(
        parts: UnitParts,
        limits: UnitLimits,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::single(creation);
        Self::try_from_parts_created_in(parts, limits, &mut creation, work)
    }
    /// Observe actual allocation extents and the original opaque SDK bounds
    /// before the corresponding creator call. The observer and creator keep
    /// separate failure provenance, using the caller's enclosing error type.
    pub fn try_from_parts_with_creator_observed_in<H>(
        parts: UnitParts,
        limits: UnitLimits,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        observer: &mut dyn FnMut(crate::UnitConstructionRequest) -> Result<(), H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::new(temporary, retained)
            .with_observer(observer);
        Self::try_from_parts_created_in(parts, limits, &mut creation, work)
    }
    /// The numerical observer and single original creator borrow the same
    /// construction operation. Neither opens a scope or traverses a second DAG.
    pub fn try_from_parts_with_single_creator_observed_in<H>(
        parts: UnitParts,
        limits: UnitLimits,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        observer: &mut dyn FnMut(crate::UnitConstructionRequest) -> Result<(), H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let mut creation =
            crate::unit_creation::UnitValidationCreation::single(creation).with_observer(observer);
        Self::try_from_parts_created_in(parts, limits, &mut creation, work)
    }
    /// Pure receiving may observe the same numerical construction facts while
    /// supplying no creator at all. This performs no capacity admission and
    /// never fabricates a successful creation port.
    pub fn try_from_parts_observed_in<H>(
        parts: UnitParts,
        limits: UnitLimits,
        observer: &mut dyn FnMut(crate::UnitConstructionRequest) -> Result<(), H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let mut creation =
            crate::unit_creation::UnitValidationCreation::pure().with_observer(observer);
        Self::try_from_parts_created_in(parts, limits, &mut creation, work)
    }
    fn try_from_parts_created_in<H>(
        parts: UnitParts,
        limits: UnitLimits,
        creation: &mut crate::unit_creation::UnitValidationCreation<'_, H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let checked = crate::validation::validate_core(
            &parts.ports,
            &parts.definitions,
            &parts.roots,
            &parts.expansions,
            limits,
            creation,
            work,
        )
        .and_then(|()| crate::validation_calls::validate_core(&parts, creation, work));
        if let Err(error) = checked {
            return Err(creation.finish_error(error, work));
        }
        Ok(Self::from_validated_parts(parts))
    }
    pub(crate) fn finish_sealed_parts_with_creator_in<H>(
        parts: UnitParts,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::new(temporary, retained);
        if let Err(error) = crate::validation_calls::validate_core(&parts, &mut creation, work) {
            return Err(creation.finish_error(error, work));
        }
        Ok(Self::from_validated_parts(parts))
    }
}

impl UnitParts {
    pub fn new(
        ports: Box<[ExprPort]>,
        definitions: Box<[ExprNode]>,
        roots: Box<[ExprRoot]>,
        root_uses: Box<[ExpressionUseId]>,
        control: ExpressionControlFlow<ExprId>,
        claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
        requests: BTreeMap<ExprId, PhysicalCallRequest>,
    ) -> Self {
        Self {
            ports,
            definitions,
            roots,
            root_uses,
            control,
            expansions: ControlExpansions::default(),
            claims,
            requests,
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        }
    }
    pub fn with_storage_lifetime(
        mut self,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Result<Self, crate::UnitError> {
        if !self.storage.is_pure() {
            return Err(crate::UnitError::invalid(
                None,
                None,
                "unit parts already retain their construction lifetime",
            ));
        }
        self.storage = storage;
        Ok(self)
    }
    pub fn with_control_expansions(mut self, expansions: ControlExpansions) -> Self {
        self.expansions = expansions;
        self
    }
}
impl BoundExprSet {
    /// Moving parts preserves their actual construction lifetime.
    pub fn into_parts(self) -> UnitParts {
        UnitParts {
            ports: self.ports,
            definitions: self.definitions,
            roots: self.roots,
            root_uses: self.root_uses,
            control: self.control,
            expansions: self.expansions,
            claims: self.claims,
            requests: self.requests,
            storage: self.storage,
        }
    }
}

impl PartialEq for BoundExprSet {
    fn eq(&self, other: &Self) -> bool {
        self.ports == other.ports
            && self.definitions == other.definitions
            && self.roots == other.roots
            && self.root_uses == other.root_uses
            && self.control == other.control
            && self.expansions.records() == other.expansions.records()
            && self.claims == other.claims
            && self.requests == other.requests
    }
}
impl BoundExprSet {
    /// Recheck immutable structure under the enclosing deployment's limits.
    /// The same unit author owns these rules; this authenticates no installed
    /// implementation and creates no new work entry, footer or resource grant.
    pub fn validate_structure_in(
        &self,
        limits: crate::UnitLimits,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<(), crate::UnitError> {
        crate::validation::validate(
            &self.ports,
            &self.definitions,
            &self.roots,
            &self.expansions,
            limits,
            work,
        )
    }
}
// Design: ADR-0238-self-contained-expression-units-and-explicit-environments.md

impl BoundExprSet {
    /// Recheck the same immutable structure with actual caller-funded scratch.
    /// This creates no success storage, control flow, or expression payload.
    pub fn validate_structure_with_creator_in<H>(
        &self,
        limits: crate::UnitLimits,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<(), crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::new(temporary, retained);
        crate::validation::validate_core(
            &self.ports,
            &self.definitions,
            &self.roots,
            &self.expansions,
            limits,
            &mut creation,
            work,
        )
        .map_err(|error| creation.finish_error(error, work))
    }
}
