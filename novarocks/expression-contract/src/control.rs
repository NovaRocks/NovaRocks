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

//! The sole local invocation/region expansion from authenticated definition facts.
use crate::unit_creation::{UnitValidationCreation, ValidationFailure};
use crate::{DefinitionControl, ExprId, ExprKind, ExprNode, ExprRoot};
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_type_contract::{
    CompileCheckpoints, ControlShape, EvaluationDemand, EvaluationRegionId, ExpressionControlFlow,
    ExpressionControlFlowError, ExpressionDefinitionMembership, ExpressionEffectContext,
    ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId, MAX_CONTROL_DEFINITIONS,
    MAX_CONTROL_DEPTH, MAX_CONTROL_USE_REFERENCES, RegionGuard, TemporalSourceOccurrence,
    TemporalSourcePlan, control_argument_semantics,
};
use std::alloc::Layout;
use std::collections::BTreeMap;

pub(crate) struct Membership<'a>(pub(crate) &'a [ExprNode]);
impl ExpressionDefinitionMembership<ExprId> for Membership<'_> {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.0
            .get(id.get() as usize)
            .is_some_and(|node| node.id == id)
    }
}
pub(crate) struct DerivedControl {
    pub(crate) flow: ExpressionControlFlow<ExprId>,
    pub(crate) root_uses: Box<[ExpressionUseId]>,
    pub(crate) temporal_sources: BTreeMap<ExpressionUseId, TemporalSourcePlan<ExprId>>,
}
struct Author<'s, 'w, 'c, 'a, 'scope, H> {
    definitions: &'s [ExprNode],
    controls: &'s BTreeMap<ExprId, DefinitionControl>,
    work: &'w mut CompileCheckpoints<'c>,
    creation: &'a mut UnitValidationCreation<'scope, H>,
    regions: Vec<ExpressionEvaluationRegion>,
    uses: Vec<ExpressionInvocation<ExprId>>,
    temporal_sources: BTreeMap<ExpressionUseId, TemporalSourcePlan<ExprId>>,
    references: usize,
}
impl<'s, H> Author<'s, '_, '_, '_, '_, H> {
    fn reference(&mut self) -> Result<(), ValidationFailure<'s, H>> {
        if self.references == MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionControlFlowError::TooManyItems.into());
        }
        self.references += 1;
        self.work.step()?;
        Ok(())
    }
    fn region(
        &mut self,
        parent: Option<EvaluationRegionId>,
        guard: Option<RegionGuard>,
    ) -> Result<EvaluationRegionId, ValidationFailure<'s, H>> {
        if self.regions.len() == MAX_CONTROL_DEFINITIONS {
            return Err(ExpressionControlFlowError::TooManyItems.into());
        }
        let id = EvaluationRegionId::new(self.regions.len() as u32);
        self.creation.reserve_push(&mut self.regions, self.work)?;
        self.regions
            .push(ExpressionEvaluationRegion { id, parent, guard });
        self.work.step()?;
        Ok(id)
    }
    fn invocation(
        &mut self,
        definition: ExprId,
        region: EvaluationRegionId,
        demand: EvaluationDemand,
        depth: usize,
    ) -> Result<ExpressionUseId, ValidationFailure<'s, H>> {
        if depth > MAX_CONTROL_DEPTH {
            return Err(ExpressionControlFlowError::TooDeep.into());
        }
        self.reference()?;
        let node = self
            .definitions
            .get(definition.get() as usize)
            .ok_or_else(|| {
                ValidationFailure::invalid(
                    Some(definition),
                    None,
                    "invocation definition is absent",
                )
            })?;
        self.work.step()?;
        let (shape, sources) = if let Some(shape) = node.kind.intrinsic_control_shape()? {
            (shape, None)
        } else {
            let facts = self.controls.get(&definition).ok_or_else(|| {
                ValidationFailure::invalid(
                    Some(definition),
                    None,
                    "invoked function lacks authenticated definition control",
                )
            })?;
            let ExprKind::FunctionCall { args, .. } = &node.kind else {
                return Err(ValidationFailure::invalid(
                    Some(definition),
                    None,
                    "definition control names a non-call",
                ));
            };
            match (facts.shape, &facts.temporal_sources) {
                (ControlShape::TemporalSource(shape), Some(sources)) => {
                    self.work.flush()?;
                    let actual = crate::temporal_source::temporal_source_definitions_for_unit(
                        shape.kind(),
                        self.definitions,
                        args,
                        self.creation,
                        self.work,
                    )?;
                    if sources != &actual || sources.facts.shape() != shape {
                        return Err(ValidationFailure::invalid(
                            Some(definition),
                            None,
                            "temporal construction facts differ from actual definitions",
                        ));
                    }
                    (facts.shape, Some(sources))
                }
                (ControlShape::TemporalSource(_), None) | (_, Some(_)) => {
                    return Err(ValidationFailure::invalid(
                        Some(definition),
                        None,
                        "temporal construction facts do not match control shape",
                    ));
                }
                (_, None) => (facts.shape, None),
            }
        };
        let id = ExpressionUseId::new(self.uses.len() as u32);
        self.creation.reserve_push(&mut self.uses, self.work)?;
        self.uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                region,
                demand,
            },
            definition,
            control: shape,
            arguments: Box::default(),
        });
        self.work.step()?;
        let mut children = Vec::new();
        if let Some(sources) = sources {
            self.work.flush()?;
            self.creation
                .reserve(&mut children, sources.definitions.len(), self.work)?;
            self.work.flush()?;
            for &child in &sources.definitions {
                self.reference()?;
                children.push(child);
                self.work.step()?;
            }
        } else if !matches!(shape, ControlShape::TypeOnly | ControlShape::NoArguments) {
            node.kind.invocation_references_observed(|child| {
                self.reference()?;
                self.creation.reserve_push(&mut children, self.work)?;
                children.push(child);
                Ok::<_, ValidationFailure<'s, H>>(())
            })?;
        }
        let mut arguments = Vec::new();
        self.creation
            .reserve_retained(&mut arguments, children.len(), self.work)?;
        for (ordinal, child) in children.iter().enumerate() {
            let (child_demand, guard) =
                control_argument_semantics(shape, children.len(), ordinal, demand)?;
            let child_region = if let Some(kind) = guard {
                self.region(Some(region), Some(RegionGuard { owner: id, kind }))?
            } else {
                region
            };
            let child_use = self.invocation(*child, child_region, child_demand, depth + 1)?;
            arguments.push(child_use);
            self.work.step()?;
        }
        if let Some(sources) = sources {
            let mut occurrences = Vec::new();
            self.work.flush()?;
            self.creation
                .reserve_retained(&mut occurrences, arguments.len(), self.work)?;
            self.work.flush()?;
            let roles = sources.facts.shape().roles();
            for (ordinal, (&definition, &use_id)) in
                sources.definitions.iter().zip(&arguments).enumerate()
            {
                occurrences.push(TemporalSourceOccurrence {
                    role: roles[ordinal].ok_or_else(|| {
                        ValidationFailure::invalid(
                            Some(definition),
                            None,
                            "temporal source role is absent",
                        )
                    })?,
                    definition,
                    use_id,
                });
                self.work.step()?;
            }
            self.work.flush()?;
            // This exact owned clone belongs to the retained source plan;
            // the independent authentication copy above remains scratch.
            self.creation.request_retained(
                Layout::array::<novarocks_type_contract::TemporalCastKind>(
                    sources.facts.cast_chain().len(),
                )
                .map_err(|_| LayoutResourceError::Arithmetic)?,
                self.work,
            )?;
            let plan = TemporalSourcePlan {
                facts: sources.facts.clone(),
                sources: self.creation.boxed_retained(occurrences, self.work)?,
            };
            self.work.flush()?;
            plan.validate_structure().map_err(|_| {
                ValidationFailure::invalid(
                    Some(definition),
                    None,
                    "temporal invocation grammar is invalid",
                )
            })?;
            self.work.flush()?;
            self.creation
                .map_insert_retained(&mut self.temporal_sources, id, plan, self.work)?;
            self.work.flush()?;
        }
        self.uses[id.get() as usize].arguments =
            self.creation.boxed_retained(arguments, self.work)?;
        self.work.step()?;
        Ok(id)
    }
}
/// The single expansion author accepts either no creator or the enclosing
/// caller's actual scopes. Input definitions/facts remain borrowed sources.
pub(crate) fn derive_created_in<'source, H>(
    definitions: &'source [ExprNode],
    roots: &[ExprRoot],
    controls: &'source BTreeMap<ExprId, DefinitionControl>,
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DerivedControl, ValidationFailure<'source, H>> {
    for &id in controls.keys() {
        work.step()?;
        if !definitions
            .get(id.get() as usize)
            .is_some_and(|node| matches!(node.kind, ExprKind::FunctionCall { .. }))
        {
            return Err(ValidationFailure::invalid(
                Some(id),
                None,
                "definition control is not attached to a function definition",
            ));
        }
    }
    let mut author = Author {
        definitions,
        controls,
        work,
        creation,
        regions: Vec::new(),
        uses: Vec::new(),
        temporal_sources: BTreeMap::new(),
        references: 0,
    };
    let mut root_uses = Vec::new();
    for root in roots {
        let region = author.region(None, None)?;
        let use_id = author.invocation(root.definition, region, root.demand, 1)?;
        author
            .creation
            .reserve_push_retained(&mut root_uses, author.work)?;
        root_uses.push(use_id);
        author.work.step()?;
    }
    let Author {
        regions,
        uses,
        temporal_sources,
        ..
    } = author;
    let flow = creation.flow(regions, uses, &Membership(definitions), work)?;
    work.flush()?;
    let root_uses = creation.boxed_retained(root_uses, work)?;
    work.flush()?;
    Ok(DerivedControl {
        flow,
        root_uses,
        temporal_sources,
    })
}
