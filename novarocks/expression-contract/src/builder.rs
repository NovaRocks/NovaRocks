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

//! Mutable unit construction ends before any immutable unit is exposed.
use crate::{
    BoundExprSet, DefinitionControl, ExprId, ExprKind, ExprNode, ExprPort, ExprRoot,
    ExpressionCallClaim, PhysicalCallRequest, RootOrdinal, RootScope, UnitError, UnitLimits,
    UnitParts,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, EvaluationDemand, ExpressionUseId,
    ValueTypeStorageLifetime,
};
use std::collections::{BTreeMap, btree_map::Entry};

pub struct BoundExprSetBuilder {
    limits: UnitLimits,
    definitions: Vec<Option<ExprNode>>,
    roots: Vec<ExprRoot>,
    controls: BTreeMap<ExprId, DefinitionControl>,
    requests: BTreeMap<ExprId, PhysicalCallRequest>,
    storage: ValueTypeStorageLifetime,
}
impl BoundExprSetBuilder {
    pub fn new(limits: UnitLimits) -> Self {
        Self {
            limits,
            definitions: Vec::new(),
            roots: Vec::new(),
            controls: BTreeMap::new(),
            requests: BTreeMap::new(),
            storage: ValueTypeStorageLifetime::pure(),
        }
    }
    pub fn with_storage_lifetime(
        mut self,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, UnitError> {
        if !self.storage.is_pure() {
            return Err(UnitError::invalid(
                None,
                None,
                "unit builder already retains its construction lifetime",
            ));
        }
        self.storage = storage;
        Ok(self)
    }
    pub fn len(&self) -> usize {
        self.definitions.len()
    }
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }
    pub fn definition(&self, id: ExprId) -> Option<&ExprNode> {
        self.definitions
            .get(id.get() as usize)
            .and_then(Option::as_ref)
    }
    pub fn definitions(&self) -> impl Iterator<Item = &ExprNode> {
        self.definitions.iter().filter_map(Option::as_ref)
    }
    /// Reserve before a lambda body is authored. IDs remain dense; insertion
    /// order need not be topological. The caller admits these source buffers.
    pub fn reserve(&mut self) -> Result<ExprId, UnitError> {
        if self.definitions.len() == novarocks_type_contract::MAX_CONTROL_DEFINITIONS {
            return Err(novarocks_type_contract::ExpressionControlFlowError::TooManyItems.into());
        }
        let id = ExprId::new(
            u32::try_from(self.definitions.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        self.definitions
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.definitions.push(None);
        Ok(id)
    }
    pub fn add(
        &mut self,
        lambda_scope: Option<ExprId>,
        ty: crate::ValueType,
        kind: ExprKind,
    ) -> Result<ExprId, UnitError> {
        let id = self.reserve()?;
        self.insert(id, lambda_scope, ty, kind)?;
        Ok(id)
    }
    pub fn insert(
        &mut self,
        id: ExprId,
        lambda_scope: Option<ExprId>,
        ty: crate::ValueType,
        kind: ExprKind,
    ) -> Result<(), UnitError> {
        let slot = self.definitions.get_mut(id.get() as usize).ok_or_else(|| {
            UnitError::invalid(
                Some(id),
                None,
                "definition ID was not reserved by this builder",
            )
        })?;
        if slot.is_some() {
            return Err(UnitError::invalid(
                Some(id),
                None,
                "definition ID was already filled",
            ));
        }
        *slot = Some(ExprNode {
            id,
            lambda_scope,
            ty,
            kind,
        });
        Ok(())
    }
    pub fn root(
        &mut self,
        definition: ExprId,
        demand: EvaluationDemand,
        scope: RootScope,
    ) -> Result<RootOrdinal, UnitError> {
        let ordinal = RootOrdinal::new(
            u32::try_from(self.roots.len()).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        self.roots
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.roots.push(ExprRoot {
            definition,
            demand,
            scope,
        });
        Ok(ordinal)
    }
    pub fn request(
        &mut self,
        definition: ExprId,
        request: PhysicalCallRequest,
    ) -> Result<(), UnitError> {
        match self.requests.entry(definition) {
            Entry::Vacant(entry) => {
                entry.insert(request);
                Ok(())
            }
            Entry::Occupied(_) => Err(UnitError::invalid(
                Some(definition),
                None,
                "definition already has an original request",
            )),
        }
    }
    /// Installed-owner authentication remains with the producer; this method
    /// accepts only the neutral facts needed to expand actual invocations.
    pub fn definition_control(
        &mut self,
        definition: ExprId,
        control: DefinitionControl,
    ) -> Result<(), UnitError> {
        match self.controls.entry(definition) {
            Entry::Vacant(entry) => {
                entry.insert(control);
                Ok(())
            }
            Entry::Occupied(_) => Err(UnitError::invalid(
                Some(definition),
                None,
                "definition already has control construction facts",
            )),
        }
    }
    pub fn types_mut(&mut self) -> UnitTypeEditor<'_> {
        UnitTypeEditor {
            definitions: &mut self.definitions,
        }
    }
    /// Borrow the original work scope. No new entry, phase or footer is made.
    pub fn seal(
        self,
        ports: Vec<ExprPort>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<SealedUnit, UnitError> {
        let mut definitions = Vec::new();
        crate::validation::reserve(&mut definitions, self.definitions.len(), work)?;
        for (ordinal, definition) in self.definitions.into_iter().enumerate() {
            definitions.push(definition.ok_or_else(|| {
                UnitError::invalid(
                    Some(ExprId::new(ordinal as u32)),
                    None,
                    "reserved definition was not filled",
                )
            })?);
            work.step()?;
        }
        crate::validation::validate(&ports, &definitions, &self.roots, self.limits, work)?;
        let derived = crate::control::derive(&definitions, &self.roots, &self.controls, work)?;
        work.flush()?;
        let definitions = definitions.into_boxed_slice();
        let ports = ports.into_boxed_slice();
        let roots = self.roots.into_boxed_slice();
        work.flush()?;
        Ok(SealedUnit {
            ports,
            definitions,
            roots,
            root_uses: derived.root_uses,
            control: derived.flow,
            temporal_sources: derived.temporal_sources,
            requests: self.requests,
            storage: self.storage,
        })
    }
}
pub struct SealedUnit {
    ports: Box<[ExprPort]>,
    definitions: Box<[ExprNode]>,
    roots: Box<[ExprRoot]>,
    root_uses: Box<[ExpressionUseId]>,
    control: novarocks_type_contract::ExpressionControlFlow<ExprId>,
    temporal_sources:
        BTreeMap<ExpressionUseId, novarocks_type_contract::TemporalSourcePlan<ExprId>>,
    requests: BTreeMap<ExprId, PhysicalCallRequest>,
    storage: ValueTypeStorageLifetime,
}
impl SealedUnit {
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
    pub fn root_use_ids(&self) -> &[ExpressionUseId] {
        &self.root_uses
    }
    pub fn control_flow(&self) -> &novarocks_type_contract::ExpressionControlFlow<ExprId> {
        &self.control
    }
    pub fn uses(
        &self,
    ) -> &BTreeMap<ExpressionUseId, novarocks_type_contract::ExpressionInvocation<ExprId>> {
        self.control.uses()
    }
    pub fn requests(&self) -> &BTreeMap<ExprId, PhysicalCallRequest> {
        &self.requests
    }
    pub fn temporal_source(
        &self,
        use_id: ExpressionUseId,
    ) -> Option<&novarocks_type_contract::TemporalSourcePlan<ExprId>> {
        self.temporal_sources.get(&use_id)
    }
    pub fn finish(
        self,
        claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<BoundExprSet, UnitError> {
        BoundExprSet::finish_sealed_parts(
            UnitParts::new(
                self.ports,
                self.definitions,
                self.roots,
                self.root_uses,
                self.control,
                claims,
                self.requests,
            )
            .with_storage_lifetime(self.storage)?,
            work,
        )
    }
}
/// Type and scalar-signature edits are atomic before seal. No arbitrary mutable
/// definition is exposed to the enclosing plan's nullability transaction.
pub struct UnitTypeEditor<'a> {
    pub(crate) definitions: &'a mut [Option<ExprNode>],
}
impl UnitTypeEditor<'_> {
    pub fn definition(&self, id: ExprId) -> Option<&ExprNode> {
        self.definitions
            .get(id.get() as usize)
            .and_then(Option::as_ref)
    }
}
