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
use crate::{ExprId, ExprNode, ExpressionCallClaim, PhysicalCallRequest, ValueId, ValueType};
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
        crate::validation::validate(&parts.ports, &parts.definitions, &parts.roots, limits, work)?;
        Self::finish_sealed_parts(parts, work)
    }
    pub(crate) fn finish_sealed_parts(
        parts: UnitParts,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::UnitError> {
        crate::validation_calls::validate(&parts, work)?;
        Ok(Self {
            ports: parts.ports,
            definitions: parts.definitions,
            roots: parts.roots,
            root_uses: parts.root_uses,
            control: parts.control,
            claims: parts.claims,
            requests: parts.requests,
            storage: parts.storage,
        })
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
            claims: self.claims,
            requests: self.requests,
            storage: self.storage,
        }
    }
}
