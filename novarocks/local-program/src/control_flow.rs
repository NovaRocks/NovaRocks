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

//! Dense local-program projection of the shared immutable control graph.
use crate::ProgramExprId;
use novarocks_type_contract::{
    CompilePhase, EvaluationRegionId, ExpressionUseId, PureCompileControl,
};
pub use novarocks_type_contract::{
    ControlShape, ExpressionControlFlowError as ProgramControlFlowError,
    ExpressionEvaluationRegion as ProgramEvaluationRegion, GuardKind, RegionGuard,
};
use std::collections::BTreeMap;
pub type ProgramExpressionUse = novarocks_type_contract::ExpressionInvocation<ProgramExprId>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramControlFlow(novarocks_type_contract::ExpressionControlFlow<ProgramExprId>);
struct DenseDefinitions(usize);
impl novarocks_type_contract::ExpressionDefinitionMembership<ProgramExprId> for DenseDefinitions {
    fn definition_count(&self) -> usize {
        self.0
    }
    fn contains_definition(&self, id: ProgramExprId) -> bool {
        id.index() < self.0
    }
}
impl ProgramControlFlow {
    pub fn try_new(
        regions: Vec<ProgramEvaluationRegion>,
        uses: Vec<ProgramExpressionUse>,
        definition_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramControlFlowError> {
        novarocks_type_contract::ExpressionControlFlow::try_new(
            regions,
            uses,
            &DenseDefinitions(definition_count),
            CompilePhase::LowerProgram,
            control,
        )
        .map(Self)
    }
    /// Same shared control author with independent actual validation scratch
    /// and immutable graph backing lifetimes.
    pub fn try_new_with_creator_in<H>(
        regions: Vec<ProgramEvaluationRegion>,
        uses: Vec<ProgramExpressionUse>,
        definition_count: usize,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::ProgramCreationFailure<ProgramControlFlowError, H>> {
        use crate::ProgramCreationFailure as Failure;
        use novarocks_type_contract::{
            CompileCheckpoints, ExpressionControlCreationFailure as Shared,
        };
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = novarocks_type_contract::ExpressionControlFlow::try_new_with_creator_in(
            regions,
            uses,
            &DenseDefinitions(definition_count),
            temporary,
            retained,
            &mut work,
        )
        .map(Self)
        .map_err(|e| match e {
            Shared::Control(e) => Failure::Control(e),
            Shared::Creator(e) => Failure::Creator(e),
            Shared::Resource(e) => Failure::Resource(e),
            Shared::Owner(e) => Failure::Owner(e),
        });
        if matches!(
            &result,
            Err(Failure::Control(_) | Failure::Creator(_) | Failure::Resource(_))
        ) {
            return result;
        }
        work.finish()?;
        result
    }
    /// Borrow the original checked neutral author for scoped effect composition.
    /// No second flow or evaluation-use identities are reconstructed here.
    pub const fn shared_flow(
        &self,
    ) -> &novarocks_type_contract::ExpressionControlFlow<ProgramExprId> {
        &self.0
    }
    pub fn root_use_ids(&self) -> &[ExpressionUseId] {
        self.0.root_use_ids()
    }
    pub fn regions(&self) -> &BTreeMap<EvaluationRegionId, ProgramEvaluationRegion> {
        self.0.regions()
    }
    pub fn uses(&self) -> &BTreeMap<ExpressionUseId, ProgramExpressionUse> {
        self.0.uses()
    }
    pub const fn use_reference_count(&self) -> usize {
        self.0.use_reference_count()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{
        CompileControlError, EvaluationDemand, ExpressionEffectContext, control_argument_semantics,
    };
    struct Control(bool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
            if self.0 && work > 0 {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    fn root() -> ProgramEvaluationRegion {
        ProgramEvaluationRegion {
            id: EvaluationRegionId::new(u32::MAX),
            parent: None,
            guard: None,
        }
    }
    fn value(id: u32, region: EvaluationRegionId) -> ProgramExpressionUse {
        ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(id),
                region,
                demand: EvaluationDemand::Value,
            },
            definition: ProgramExprId::new(0),
            control: ControlShape::Eager,
            arguments: Box::default(),
        }
    }
    fn branch(id: u32, kind: GuardKind) -> ProgramEvaluationRegion {
        ProgramEvaluationRegion {
            id: EvaluationRegionId::new(id),
            parent: Some(root().id),
            guard: Some(RegionGuard {
                owner: ExpressionUseId::new(0),
                kind,
            }),
        }
    }
    #[test]
    fn lambda_locals_need_values_while_the_final_body_keeps_truth_only_demand() {
        let mut owner = value(0, root().id);
        owner.control = ControlShape::LambdaBody;
        owner.context.demand = EvaluationDemand::TruthOnly;
        owner.arguments = (1..=3).map(ExpressionUseId::new).collect();
        let mut body = value(3, root().id);
        body.context.demand = EvaluationDemand::TruthOnly;
        let uses = vec![owner, value(1, root().id), value(2, root().id), body];
        ProgramControlFlow::try_new(vec![root()], uses.clone(), 1, &Control(false)).unwrap();
        for index in [1, 2, 3] {
            let mut wrong = uses.clone();
            wrong[index].context.demand = if index == 3 {
                EvaluationDemand::Value
            } else {
                EvaluationDemand::TruthOnly
            };
            assert_eq!(
                ProgramControlFlow::try_new(vec![root()], wrong, 1, &Control(false)).unwrap_err(),
                ProgramControlFlowError::InvalidDemand
            );
        }
    }

    #[test]
    fn if_retains_distinct_occurrences_and_strong_branch_regions() {
        let mut owner = value(0, root().id);
        owner.control = ControlShape::If;
        owner.arguments = vec![
            ExpressionUseId::new(1),
            ExpressionUseId::new(2),
            ExpressionUseId::new(3),
        ]
        .into_boxed_slice();
        let then = branch(1, GuardKind::IfThen);
        let otherwise = branch(2, GuardKind::IfElse);
        let mut condition = value(1, root().id);
        condition.context.demand = EvaluationDemand::TruthOnly;
        let uses = vec![owner, condition, value(2, then.id), value(3, otherwise.id)];
        let flow = ProgramControlFlow::try_new(
            vec![root(), then, otherwise],
            uses.clone(),
            1,
            &Control(false),
        )
        .unwrap();
        // All four uses share one immutable definition; no invocation is merged.
        assert_eq!(flow.uses().len(), 4);
        let mut invalid = uses;
        invalid[2].context.region = root().id;
        assert_eq!(
            ProgramControlFlow::try_new(vec![root(), then, otherwise], invalid, 1, &Control(false))
                .unwrap_err(),
            ProgramControlFlowError::InvalidGuard
        );
    }
    #[test]
    fn shared_definition_does_not_share_use_or_demand() {
        let mut truth = value(1, root().id);
        truth.context.demand = EvaluationDemand::TruthOnly;
        let value_use = value(2, root().id);
        let flow =
            ProgramControlFlow::try_new(vec![root()], vec![truth, value_use], 1, &Control(false))
                .unwrap();
        assert_ne!(
            flow.uses()[&ExpressionUseId::new(1)].context.demand,
            flow.uses()[&ExpressionUseId::new(2)].context.demand
        );
        let mut owner = value(0, root().id);
        owner.arguments = vec![ExpressionUseId::new(1), ExpressionUseId::new(1)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(
                vec![root()],
                vec![owner, value(1, root().id)],
                1,
                &Control(false)
            )
            .unwrap_err(),
            ProgramControlFlowError::SharedUse
        );
    }
    #[test]
    fn type_only_control_has_no_value_invocation_edges() {
        let mut owner = value(0, root().id);
        owner.control = ControlShape::TypeOnly;
        ProgramControlFlow::try_new(vec![root()], vec![owner.clone()], 1, &Control(false)).unwrap();
        owner.arguments = vec![ExpressionUseId::new(1)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(
                vec![root()],
                vec![owner, value(1, root().id)],
                1,
                &Control(false)
            )
            .unwrap_err(),
            ProgramControlFlowError::InvalidControlShape
        );
    }
    #[test]
    fn null_sensitive_arguments_stop_truth_only_propagation() {
        for shape in [
            ControlShape::Eager,
            ControlShape::Coalesce,
            ControlShape::Conjunction,
            ControlShape::Disjunction,
        ] {
            let mut owner = value(0, root().id);
            owner.control = shape;
            owner.context.demand = EvaluationDemand::TruthOnly;
            owner.arguments = vec![ExpressionUseId::new(1)].into_boxed_slice();
            let mut child = value(1, root().id);
            child.context.demand = control_argument_semantics(shape, 1, 0, owner.context.demand)
                .unwrap()
                .0;
            ProgramControlFlow::try_new(
                vec![root()],
                vec![owner.clone(), child.clone()],
                1,
                &Control(false),
            )
            .unwrap();
            child.context.demand = if child.context.demand == EvaluationDemand::Value {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            };
            assert_eq!(
                ProgramControlFlow::try_new(vec![root()], vec![owner, child], 1, &Control(false))
                    .unwrap_err(),
                ProgramControlFlowError::InvalidDemand
            );
        }
    }
    #[test]
    fn invocation_bound_counts_zero_argument_root_uses() {
        let cap = crate::MAX_PROGRAM_EXPANDED_OCCURRENCES;
        let uses = (0..cap as u32).map(|id| value(id, root().id)).collect();
        ProgramControlFlow::try_new(vec![root()], uses, 1, &Control(false)).unwrap();
        let uses = (0..=cap as u32).map(|id| value(id, root().id)).collect();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], uses, 1, &Control(false)).unwrap_err(),
            ProgramControlFlowError::TooManyItems
        );
    }
    #[test]
    fn cycles_invalid_sparse_ids_and_control_cancellation_fail_closed() {
        let mut cycle = value(0, root().id);
        cycle.arguments = vec![ExpressionUseId::new(0)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], vec![cycle], 1, &Control(false)).unwrap_err(),
            ProgramControlFlowError::Cycle
        );
        let mut missing = value(0, root().id);
        missing.arguments = vec![ExpressionUseId::new(u32::MAX)].into_boxed_slice();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], vec![missing], 1, &Control(false))
                .unwrap_err(),
            ProgramControlFlowError::InvalidReference
        );
        let uses = (0..1000).map(|id| value(id, root().id)).collect();
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], uses, 1, &Control(true)).unwrap_err(),
            ProgramControlFlowError::Control(CompileControlError::Cancelled)
        );
    }
    #[test]
    fn dense_definition_membership_rejects_the_exact_upper_bound() {
        let mut missing = value(0, root().id);
        missing.definition = ProgramExprId::new(1);
        assert_eq!(
            ProgramControlFlow::try_new(vec![root()], vec![missing], 1, &Control(false))
                .unwrap_err(),
            ProgramControlFlowError::InvalidReference
        );
    }
    #[test]
    fn coalesce_and_case_arguments_have_exact_guard_ordinals() {
        for (shape, guards) in [
            (
                ControlShape::Coalesce,
                vec![
                    None,
                    Some(GuardKind::CoalesceAfterNull { ordinal: 1 }),
                    Some(GuardKind::CoalesceAfterNull { ordinal: 2 }),
                ],
            ),
            (
                ControlShape::Case {
                    simple: true,
                    arms: 1,
                    has_else: true,
                },
                vec![
                    None,
                    Some(GuardKind::CaseWhen { arm: 0 }),
                    Some(GuardKind::CaseThen { arm: 0 }),
                    Some(GuardKind::CaseElse),
                ],
            ),
            (
                ControlShape::Case {
                    simple: false,
                    arms: 1,
                    has_else: false,
                },
                vec![
                    Some(GuardKind::CaseWhen { arm: 0 }),
                    Some(GuardKind::CaseThen { arm: 0 }),
                ],
            ),
        ] {
            let mut regions = vec![root()];
            let mut owner = value(0, root().id);
            owner.control = shape;
            let mut uses = Vec::new();
            let mut arguments = Vec::new();
            for (ordinal, guard) in guards.iter().enumerate() {
                let region = if let Some(guard) = guard {
                    let region = branch(ordinal as u32, *guard);
                    regions.push(region);
                    region.id
                } else {
                    root().id
                };
                arguments.push(ExpressionUseId::new(ordinal as u32 + 1));
                let mut child = value(ordinal as u32 + 1, region);
                child.context.demand =
                    control_argument_semantics(shape, guards.len(), ordinal, owner.context.demand)
                        .unwrap()
                        .0;
                uses.push(child);
            }
            owner.arguments = arguments.into_boxed_slice();
            uses.push(owner);
            ProgramControlFlow::try_new(regions, uses, 1, &Control(false)).unwrap();
        }
    }
    #[test]
    fn actual_control_flow_creator_refusal_keeps_original_cause_without_tail() {
        use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
        use std::{
            alloc::Layout,
            sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        struct FirstControl {
            refused: AtomicBool,
            tail: AtomicUsize,
        }
        impl PureCompileControl for FirstControl {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if self.refused.load(Ordering::SeqCst) {
                    self.tail.fetch_add(1, Ordering::SeqCst);
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        let control = FirstControl {
            refused: AtomicBool::new(false),
            tail: AtomicUsize::new(0),
        };
        let mut temporary_layout = |_: Layout| Ok::<(), u32>(());
        let mut temporary_opaque = |_: usize| Ok::<(), u32>(());
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut temporary_layout,
            &mut temporary_opaque,
        );
        let mut retained_layout = |_: Layout| {
            control.refused.store(true, Ordering::SeqCst);
            Err(73u32)
        };
        let mut retained_opaque = |_: usize| {
            control.refused.store(true, Ordering::SeqCst);
            Err(73u32)
        };
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_layout,
            &mut retained_opaque,
        );
        assert!(matches!(
            ProgramControlFlow::try_new_with_creator_in(
                vec![root()],
                vec![value(0, root().id)],
                1,
                &mut temporary,
                &mut retained,
                &control
            ),
            Err(crate::ProgramCreationFailure::Creator(73))
        ));
        assert_eq!(control.tail.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn control_flow_clone_retains_entity_without_validation_temporary_scope() {
        use novarocks_type_contract::{
            ValueSchemaCreation, ValueTypeStorageHold, ValueTypeStorageLifetime,
        };
        use std::{
            alloc::Layout,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
        };
        struct Hold(Arc<AtomicUsize>);
        impl ValueTypeStorageHold for Hold {}
        impl Drop for Hold {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let temporary_drops = Arc::new(AtomicUsize::new(0));
        let entity_drops = Arc::new(AtomicUsize::new(0));
        let mut t_layout = |_: Layout| Ok::<(), std::convert::Infallible>(());
        let mut t_opaque = |_: usize| Ok::<(), std::convert::Infallible>(());
        let mut r_layout = |_: Layout| Ok::<(), std::convert::Infallible>(());
        let mut r_opaque = |_: usize| Ok::<(), std::convert::Infallible>(());
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(Arc::new(Hold(temporary_drops.clone()))),
            &mut t_layout,
            &mut t_opaque,
        );
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(Arc::new(Hold(entity_drops.clone()))),
            &mut r_layout,
            &mut r_opaque,
        );
        let flow = ProgramControlFlow::try_new_with_creator_in(
            vec![root()],
            vec![value(0, root().id)],
            1,
            &mut temporary,
            &mut retained,
            &Control(false),
        )
        .unwrap();
        let last = flow.clone();
        drop(flow);
        drop(temporary);
        drop(retained);
        assert_eq!(temporary_drops.load(Ordering::SeqCst), 1);
        assert_eq!(entity_drops.load(Ordering::SeqCst), 0);
        assert_eq!(last.uses().len(), 1);
        drop(last);
        assert_eq!(entity_drops.load(Ordering::SeqCst), 1);
    }
}
