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

//! Test authors keep definitions and real node-field roots separate.
//! These helpers never infer a root, call request, control shape or effect.
use crate::{ExprId, FragmentBuilder, NodeId, ValueType};
use novarocks_expression_contract::{ExprKind, RootOrdinal, RootScope, UnitError};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) trait DefinitionFixture {
    fn define(&mut self, node: NodeId, ty: ValueType, kind: ExprKind) -> Result<ExprId, UnitError>;
    fn root_definition(
        &mut self,
        node: NodeId,
        definition: ExprId,
        demand: EvaluationDemand,
        scope: RootScope,
    ) -> RootOrdinal;
    fn finish_fixture_structure(
        self,
        root: NodeId,
        sink: crate::FragmentSink,
        dop: crate::PipelineDopDomain,
        placement: crate::StagePlacementPolicy,
    ) -> Result<crate::Fragment, crate::ValidationErrors>;
    fn finish_fixture_definition(
        self,
        root: NodeId,
        sink: crate::FragmentSink,
        dop: crate::PipelineDopDomain,
        placement: crate::StagePlacementPolicy,
    ) -> Result<crate::Fragment, crate::ValidationErrors>;
}
impl DefinitionFixture for FragmentBuilder {
    fn define(&mut self, node: NodeId, ty: ValueType, kind: ExprKind) -> Result<ExprId, UnitError> {
        self.unit(node)
            .expect("fixture draft must not already be installed")
            .add(None, ty, kind)
    }
    fn root_definition(
        &mut self,
        node: NodeId,
        definition: ExprId,
        demand: EvaluationDemand,
        scope: RootScope,
    ) -> RootOrdinal {
        self.unit(node)
            .expect("fixture draft must not already be installed")
            .root(definition, demand, scope)
            .unwrap()
    }
    fn finish_fixture_definition(
        mut self,
        root: NodeId,
        sink: crate::FragmentSink,
        dop: crate::PipelineDopDomain,
        placement: crate::StagePlacementPolicy,
    ) -> Result<crate::Fragment, crate::ValidationErrors> {
        // The fixed fixture explicitly authored every real root and its scope.
        // Finish its pure unit construction before testing fragment laws. A
        // function fixture must install actual control/requests/claims itself;
        // seal_pure_unit cannot invent any of those facts.
        let nodes = self
            .construction_nodes()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for node in nodes {
            if self.construction_unit(node).is_some() {
                seal_pure_unit(&mut self, node);
            }
        }
        self.finish_definition(root, sink, dop, placement)
    }
    fn finish_fixture_structure(
        mut self,
        root: NodeId,
        sink: crate::FragmentSink,
        dop: crate::PipelineDopDomain,
        placement: crate::StagePlacementPolicy,
    ) -> Result<crate::Fragment, crate::ValidationErrors> {
        let nodes = self
            .construction_nodes()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for node in nodes {
            if self.construction_unit(node).is_some() {
                seal_pure_unit(&mut self, node);
            }
        }
        match self.finish_structure(
            root,
            sink,
            dop,
            crate::PlanLimits::FROZEN,
            &Control,
            placement,
        ) {
            Ok(fragment) => Ok(fragment),
            Err(crate::FragmentStructureError::Structure(errors)) => Err(errors),
            Err(crate::FragmentStructureError::Control(cause)) => {
                panic!("non-refusing fixture control failed: {cause}")
            }
        }
    }
}
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
/// Use only for fixed fixtures with no function calls. The real immutable unit
/// constructor rejects a missing function control/request/claim; no effects
/// or installed capability can be synthesized by this helper.
pub(crate) fn seal_pure_unit(builder: &mut FragmentBuilder, node: NodeId) {
    let ports = builder
        .node_ports(node)
        .expect("fixture inputs must exist before sealing");
    let draft = builder
        .take_unit_builder(node)
        .expect("fixture unit draft must exist");
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let unit = draft
        .seal(ports, &mut work)
        .expect("unit fixture must be valid before exercising fragment laws")
        .finish(BTreeMap::new(), &mut work)
        .expect("pure fixture must have no function claims");
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}

/// Use only when the fixed fixture author explicitly declares every relation
/// immutable, without row errors, observable effects or environment reads.
/// This declaration is independent of the binding's legacy metadata. Tests
/// which reject missing/incorrect claims must use the real constructor directly.
pub(crate) fn with_immutable_relation_claims(fragment: crate::Fragment) -> crate::Fragment {
    use novarocks_type_contract::*;
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let mut claims = Vec::new();
    crate::visit_relational_calls_observed::<crate::FrozenCallError>(
        &fragment,
        &mut work,
        |site, binding, _| {
            let (argument_control, instance_state, own_row_error) = match binding.kind() {
                FunctionKind::Aggregate => (
                    ArgumentControl::Aggregate,
                    FunctionInstanceState::AggregateInstance,
                    FunctionIntrinsicRowError::NotRowEvaluated,
                ),
                FunctionKind::Window => (
                    ArgumentControl::Window,
                    FunctionInstanceState::WindowPartition,
                    FunctionIntrinsicRowError::NotRowEvaluated,
                ),
                FunctionKind::Table => (
                    ArgumentControl::Table,
                    FunctionInstanceState::TableInstance,
                    FunctionIntrinsicRowError::NoRowError,
                ),
                FunctionKind::Scalar => panic!("fixed relation fixture has a scalar binding"),
            };
            let id = RelationCallId::new(u32::try_from(claims.len()).unwrap());
            let context = RelationEffectContext::call(id);
            claims.push(crate::FrozenPhysicalCall {
                site,
                context,
                materialized_writer_input: matches!(
                    site,
                    crate::PhysicalCallSite::WriterPartial { .. }
                        | crate::PhysicalCallSite::WriterFinal { .. }
                )
                .then(|| RelationEffectContext::materialized_input(id)),
                effects: CallEffects {
                    value_stability: FunctionVolatility::Immutable,
                    own_row_error,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    null_behavior: FunctionNullBehavior::CalledOnNull,
                    argument_control,
                    instance_state,
                    observable_effects: ObservableEffects::NONE,
                    environment: Box::default(),
                    proof_scope: CallProofScope::Relation(context.identity),
                },
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            });
            Ok(())
        },
    )
    .unwrap();
    work.finish().unwrap();
    fragment.with_calls_observed(claims, &Control).unwrap()
}
