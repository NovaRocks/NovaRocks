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
// Design: ADR-0238-self-contained-expression-units-and-explicit-environments.md
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

fn reserve_source<T, H>(
    out: &mut Vec<T>,
    mut created: Option<(
        &mut crate::unit_creation::UnitValidationCreation<'_, H>,
        &mut CompileCheckpoints<'_>,
    )>,
) -> Result<(), crate::unit_creation::UnitAllocationFailure<H>> {
    if let Some((creation, work)) = created.as_mut() {
        creation.reserve_push_retained(out, work)
    } else {
        out.try_reserve(1).map_err(|_| {
            crate::unit_creation::UnitAllocationFailure::Control(
                CompileControlError::ResourceExhausted,
            )
        })
    }
}
fn pure_source_error(
    error: crate::unit_creation::UnitAllocationFailure<std::convert::Infallible>,
) -> UnitError {
    match error {
        crate::unit_creation::UnitAllocationFailure::Control(e) => e.into(),
        crate::unit_creation::UnitAllocationFailure::Creator(never)
        | crate::unit_creation::UnitAllocationFailure::Observer(never) => match never {},
        crate::unit_creation::UnitAllocationFailure::Resource(_) => {
            CompileControlError::ResourceExhausted.into()
        }
    }
}

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
    /// Inspect root declarations while constructing the owning physical node.
    pub fn roots(&self) -> &[ExprRoot] {
        &self.roots
    }
    /// Reserve before a lambda body is authored. IDs remain dense; insertion
    /// order need not be topological. The caller admits these source buffers.
    pub fn reserve(&mut self) -> Result<ExprId, UnitError> {
        let id = self.next_definition_id()?;
        reserve_source::<_, std::convert::Infallible>(&mut self.definitions, None)
            .map_err(pure_source_error)?;
        self.definitions.push(None);
        Ok(id)
    }
    fn next_definition_id(&self) -> Result<ExprId, UnitError> {
        if self.definitions.len() == novarocks_type_contract::MAX_CONTROL_DEFINITIONS {
            return Err(novarocks_type_contract::ExpressionControlFlowError::TooManyItems.into());
        }
        Ok(ExprId::new(
            u32::try_from(self.definitions.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?,
        ))
    }
    /// Request this author's actual new definition-buffer extent before growth.
    /// The supplied scope is retained even when a later operation refuses.
    pub fn reserve_with_creator_in<H>(
        &mut self,
        scope: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ExprId, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::single(scope);
        let id = self
            .next_definition_id()
            .map_err(|error| creation.finish_error(error.into(), work))?;
        self.retain_source_storage(&mut creation, work)?;
        reserve_source(&mut self.definitions, Some((&mut creation, work)))
            .map_err(|error| creation.finish_error(error.into(), work))?;
        self.definitions.push(None);
        Ok(id)
    }
    pub fn add_with_creator_in<H>(
        &mut self,
        lambda_scope: Option<ExprId>,
        ty: crate::ValueType,
        kind: ExprKind,
        scope: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ExprId, crate::UnitConstructionFailure<H>> {
        let id = self.reserve_with_creator_in(scope, work)?;
        self.insert_source(id, lambda_scope, ty, kind)
            .map_err(|message| {
                let mut creation = crate::unit_creation::UnitValidationCreation::single(scope);
                creation.finish_error(
                    crate::unit_creation::ValidationFailure::invalid(Some(id), None, message),
                    work,
                )
            })?;
        Ok(id)
    }
    fn retain_source_storage<H>(
        &mut self,
        creation: &mut crate::unit_creation::UnitValidationCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), crate::UnitConstructionFailure<H>> {
        self.storage = creation
            .retained_storage()
            .retaining_observed(&self.storage, |layout| {
                creation.request_retained(layout, work)
            })
            .map_err(|error: crate::unit_creation::UnitAllocationFailure<H>| {
                creation.finish_error(error.into(), work)
            })?;
        Ok(())
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
        self.insert_source(id, lambda_scope, ty, kind)
            .map_err(|message| UnitError::invalid(Some(id), None, message))
    }
    fn insert_source(
        &mut self,
        id: ExprId,
        lambda_scope: Option<ExprId>,
        ty: crate::ValueType,
        kind: ExprKind,
    ) -> Result<(), &'static str> {
        let slot = self
            .definitions
            .get_mut(id.get() as usize)
            .ok_or("definition ID was not reserved by this builder")?;
        if slot.is_some() {
            return Err("definition ID was already filled");
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
        reserve_source::<_, std::convert::Infallible>(&mut self.roots, None)
            .map_err(pure_source_error)?;
        self.roots.push(ExprRoot {
            definition,
            demand,
            scope,
        });
        Ok(ordinal)
    }
    /// Source roots use the same original ordinal/order and actual buffer
    /// author as the pure entry, with the caller's real storage request port.
    pub fn root_with_creator_in<H>(
        &mut self,
        definition: ExprId,
        demand: EvaluationDemand,
        scope: RootScope,
        creator: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<RootOrdinal, crate::UnitConstructionFailure<H>> {
        let ordinal = RootOrdinal::new(
            u32::try_from(self.roots.len()).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        let mut creation = crate::unit_creation::UnitValidationCreation::single(creator);
        self.retain_source_storage(&mut creation, work)?;
        reserve_source(&mut self.roots, Some((&mut creation, work)))
            .map_err(|error| creation.finish_error(error.into(), work))?;
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
    /// Borrow the original work scope without a runtime creator. The enclosing
    /// pure producer remains responsible for its source/header allocations.
    pub fn seal(
        self,
        ports: Vec<ExprPort>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<SealedUnit, UnitError> {
        let mut creation =
            crate::unit_creation::UnitValidationCreation::<std::convert::Infallible>::pure();
        self.seal_created_in(ports, &mut creation, work)
            .map_err(crate::unit_creation::pure_error)
    }
    /// Construct through the original validation and control-expansion authors,
    /// borrowing actual scratch and retained storage. Input builder/port buffers
    /// already belong to the caller; only new construction is requested here.
    pub fn seal_with_creator_in<H>(
        self,
        ports: Vec<ExprPort>,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<SealedUnit, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::new(temporary, retained);
        self.seal_created_in(ports, &mut creation, work)
    }
    /// One existing caller scope may conservatively own both kinds of storage.
    /// Its lifetime is retained by the unit and by independently escaping Flow
    /// backing; no new scope, factory, grant or control entry is opened.
    pub fn seal_with_single_creator_in<H>(
        self,
        ports: Vec<ExprPort>,
        scope: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<SealedUnit, crate::UnitConstructionFailure<H>> {
        let mut creation = crate::unit_creation::UnitValidationCreation::single(scope);
        self.seal_created_in(ports, &mut creation, work)
    }
    fn seal_created_in<H>(
        self,
        ports: Vec<ExprPort>,
        creation: &mut crate::unit_creation::UnitValidationCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<SealedUnit, crate::UnitConstructionFailure<H>> {
        use crate::unit_creation::{UnitAllocationFailure, ValidationFailure};
        let mut definitions = Vec::new();
        creation
            .reserve_retained(&mut definitions, self.definitions.len(), work)
            .map_err(|error| creation.finish_error(error.into(), work))?;
        for (ordinal, definition) in self.definitions.into_iter().enumerate() {
            definitions.push(definition.ok_or_else(|| {
                creation.finish_error(
                    ValidationFailure::invalid(
                        Some(ExprId::new(ordinal as u32)),
                        None,
                        "reserved definition was not filled",
                    ),
                    work,
                )
            })?);
            work.step()?;
        }
        crate::validation::validate_core(
            &ports,
            &definitions,
            &self.roots,
            self.limits,
            creation,
            work,
        )
        .map_err(|error| creation.finish_error(error, work))?;
        let derived = crate::control::derive_created_in(
            &definitions,
            &self.roots,
            &self.controls,
            creation,
            work,
        )
        .map_err(|error| creation.finish_error(error, work))?;
        work.flush()?;
        let definitions = creation
            .boxed_retained(definitions, work)
            .map_err(|error| creation.finish_error(error.into(), work))?;
        let ports = creation
            .boxed_retained(ports, work)
            .map_err(|error| creation.finish_error(error.into(), work))?;
        let roots = creation
            .boxed_retained(self.roots, work)
            .map_err(|error| creation.finish_error(error.into(), work))?;
        let storage = creation
            .retained_storage()
            .retaining_observed(&self.storage, |layout| {
                creation.request_retained(layout, work)
            })
            .map_err(|error: UnitAllocationFailure<H>| creation.finish_error(error.into(), work))?;
        work.flush()?;
        Ok(SealedUnit {
            ports,
            definitions,
            roots,
            root_uses: derived.root_uses,
            control: derived.flow,
            temporal_sources: derived.temporal_sources,
            requests: self.requests,
            storage,
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
    /// Finish the original sealed control and definitions using the enclosing
    /// caller's actual validation scratch and escaping-diagnostic storage.
    pub fn finish_with_creator_in<H>(
        self,
        claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<BoundExprSet, crate::UnitConstructionFailure<H>> {
        let parts = UnitParts::new(
            self.ports,
            self.definitions,
            self.roots,
            self.root_uses,
            self.control,
            claims,
            self.requests,
        )
        .with_storage_lifetime(self.storage)
        .map_err(|error| {
            let mut creation =
                crate::unit_creation::UnitValidationCreation::new(temporary, retained);
            creation.finish_error(error.into(), work)
        })?;
        BoundExprSet::finish_sealed_parts_with_creator_in(parts, temporary, retained, work)
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
