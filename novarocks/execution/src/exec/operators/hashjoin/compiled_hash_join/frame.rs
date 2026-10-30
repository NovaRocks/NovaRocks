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

//! One dormant declaration, shared by the actual E/F/Q/T factories. Runtime
//! data never retains this declaration or its LocalProgram/plan owners.

use super::super::{
    build_finalize::{FinalizeOptions, JoinBuildFrame},
    build_requirements::{LookupRequirement, MatchFlagRequirement},
    build_state::{ProjectionDomains, build_contract},
    join_completion::{JoinParticipants, funded_vec},
};
use super::*;
use crate::exec::{
    hash_table::key_index::{BulkDomain, IndexPurpose, KeySpec, NullPolicy},
    operators::local_exchanger::LocalExchanger,
};
use std::sync::OnceLock;

pub(crate) struct CompiledJoinFrame {
    pub(super) program: Arc<LocalProgram>,
    pub(super) plan: Arc<CompiledHashJoinPlan>,
    pub(super) participants: JoinParticipants,
    handoff: Option<Arc<LocalExchanger>>,
    value: OnceLock<ExecutionResult<Arc<JoinBuildFrame>>>,
}
impl CompiledJoinFrame {
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        plan: Arc<CompiledHashJoinPlan>,
        participants: JoinParticipants,
        handoff: Option<Arc<LocalExchanger>>,
    ) -> Self {
        Self {
            program,
            plan,
            participants,
            handoff,
            value: OnceLock::new(),
        }
    }
    pub(super) fn prepare(&self, work: &BulkWork<'_>) -> ExecutionResult<Arc<JoinBuildFrame>> {
        self.value
            .get_or_init(|| {
                let domains = self.domains(work)?;
                JoinBuildFrame::new(
                    domains,
                    FinalizeOptions {
                        participants: self.participants,
                        handoff: self.handoff.clone(),
                        // This is the existing K sketch preference, not a row/count
                        // admission quota. Larger streams produce more partials.
                        live_slots: NonZeroUsize::MIN,
                        match_bits: self.plan.requirements.match_flags
                            == MatchFlagRequirement::Needed,
                    },
                    work,
                )
            })
            .clone()
    }
    pub(crate) fn prepared(&self) -> ExecutionResult<Arc<JoinBuildFrame>> {
        self.value
            .get()
            .cloned()
            .ok_or(ConstructionFailure::MissingOwner)?
    }
    pub(super) fn domains(&self, work: &BulkWork<'_>) -> ExecutionResult<ProjectionDomains> {
        let ProgramNodeKind::Join { condition, .. } =
            self.program.graph().nodes()[self.plan.node.index()].kind()
        else {
            return Err(build_contract(
                "compiled join declaration has no original join node",
            ));
        };
        let mode = |safe| {
            if safe {
                NullPolicy::Equal
            } else {
                NullPolicy::Unmatchable
            }
        };
        let domain = |types: &[FunctionValueType], modes: &[NullPolicy]| {
            let spec = KeySpec::new(types, modes, work.owner, &|| {
                work.control.checkpoint(0).map_err(ExecutionFailure::from)
            })?;
            BulkDomain::new(&spec, work.owner, work.control)
        };
        if !condition.is_null_aware() {
            let purpose = match self.plan.requirements.lookup {
                LookupRequirement::Membership => IndexPurpose::PresenceOnly,
                LookupRequirement::RowMatches => IndexPurpose::KeyIds,
                LookupRequirement::NotNeeded => {
                    return Err(build_contract(
                        "compiled keyed join has no lookup requirement",
                    ));
                }
            };
            let mut modes = funded_vec(work.owner, self.plan.eq_null_safe.len())?;
            for &safe in &self.plan.eq_null_safe {
                modes.push(mode(safe));
            }
            return Ok(ProjectionDomains::Ordinary {
                keys: domain(&self.plan.build_key_bindings, &modes)?,
                purpose,
            });
        }
        let scope = condition.scope_keys();
        let values = condition.keys();
        let width = scope
            .len()
            .checked_add(values.len())
            .ok_or_else(super::super::join_completion::arithmetic)?;
        let mut types = funded_vec(work.owner, width)?;
        let mut modes = funded_vec(work.owner, width)?;
        let mut columns = funded_vec(work.owner, scope.len())?;
        for (column, key) in scope.iter().enumerate() {
            work.control.checkpoint(1)?;
            let key_ordinal = u32::try_from(column).map_err(|_| {
                build_contract("join scope root ordinal exceeds its frozen u32 representation")
            })?;
            let site = ProgramExpressionRootSite::Node {
                node: self.plan.node,
                role: ProgramNodeExpressionRole::NaaScopeBuildKey { key: key_ordinal },
            };
            types.push(root_value_type(&self.program, site)?);
            modes.push(mode(key.null_safe));
            columns.push(column);
        }
        for (ty, key) in self.plan.build_key_bindings.iter().zip(values) {
            work.control.checkpoint(1)?;
            types.push(ty.clone());
            modes.push(mode(key.null_safe));
        }
        let group = if scope.is_empty() {
            None
        } else {
            Some(domain(&types[..scope.len()], &modes[..scope.len()])?)
        };
        let full = domain(&types, &modes)?;
        Ok(ProjectionDomains::Naa {
            group,
            full,
            group_full_components: columns,
            value_components: scope.len()..width,
            full_purpose: if condition.residual_predicate().is_some() {
                IndexPurpose::KeyIds
            } else {
                IndexPurpose::PresenceOnly
            },
            _storage: work.owner.storage_lifetime(),
        })
    }
}

/// Direct Int64 component data with the real compiled build declaration.
/// Plain Arrow fixture payload is outside the header construction observation.
#[cfg(test)]
pub(crate) fn frame_fixture_chunks(
    program: &LocalProgram,
    factory: &Arc<dyn InputConstructionFactory>,
) -> ExecutionResult<Vec<Chunk>> {
    let layout = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            ProgramNodeKind::Join { right_layout, .. } => Some(right_layout),
            _ => None,
        })
        .ok_or_else(|| build_contract("join fixture has no original build layout"))?;
    let schema = factory.create_compiled_chunk_schema(layout)?;
    let mut chunks = Vec::new();
    for _ in 0..2 {
        let owner = factory.begin_typed_construction(&schema)?;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(arrow::array::Int64Array::from(
                (0..300)
                    .map(|i| if i % 13 == 0 { None } else { Some(i % 17) })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::Int64Array::from(
                (0..300).map(Some).collect::<Vec<_>>(),
            )),
        ];
        chunks.push(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            schema.clone(),
            columns,
            300,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?);
    }
    Ok(chunks)
}
/// F-source fixture uses the same actual E capture/page author, without claiming
/// expression evaluation or RF lifecycle. Input operator tests exercise those.
#[cfg(test)]
pub(crate) fn frame_fixture_inputs(
    frame: &JoinBuildFrame,
    program: &LocalProgram,
    factory: &Arc<dyn InputConstructionFactory>,
    tracker: &Arc<MemTracker>,
    error: &Arc<RuntimeErrorState>,
) -> ExecutionResult<()> {
    use super::super::{
        build_capture::{EvaluatedJoinKeyPage, EvaluatedKeyProjection, InputRunWriter},
        build_pages::B3RunId,
    };
    let control = RuntimeKernelControl::new(Arc::clone(error));
    let ProjectionDomains::Ordinary { keys, .. } = &frame.domains else {
        return Err(build_contract(
            "direct Int64 fixture requires ordinary keys",
        ));
    };
    for (slot, chunk) in frame_fixture_chunks(program, factory)?
        .into_iter()
        .enumerate()
    {
        let owner = factory.begin_typed_construction(chunk.chunk_schema())?;
        let work = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(Arc::clone(tracker)),
            host: crate::runtime::tracked_state_allocator::expression_allocation_host(Arc::clone(
                tracker,
            )),
            control: &control,
        };
        let mut writer = InputRunWriter::new(
            &frame.domains,
            B3RunId::from_input_slot(slot, 2)?,
            true,
            &work,
        )?;
        writer.push_evaluated(
            &chunk,
            EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
                arrays: &chunk.columns()[..1],
                bindings: keys.spec().types(),
            }),
            &mut ValueConversionContext::default(),
            &work,
        )?;
        frame.inputs.normal_input(slot, writer.seal(&work)?)?;
    }
    Ok(())
}
