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

use super::{
    contract_lowering::transfer_component_request_for_test,
    expression_occurrences::{ExpressionOccurrenceError, scalar_shape},
    physical_scalar_requests::{
        author_physical_scalar_request_observed, author_scalar_result_selection_observed,
    },
    unit_source::UnitDefinitions,
};
use crate::compiler::SqlFunctionCatalog;
use novarocks_expression_contract::{
    BoundExprSet, DefinitionControl, ExprId, ExprKind, ExprNode, ExpressionCallClaim, RootOrdinal,
    RootScope, SealedUnit, UnitError, UnitLimits, UnitParts,
};
use novarocks_functions::{ConstantPolicy, EngineFunctionCatalog};
use novarocks_physical_plan::{ConstantPools, FragmentId, NodeId, PlanLimits};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, ExpressionControlFlow,
    ExpressionUseId, FunctionValueType, PureCompileControl,
};
use std::collections::BTreeMap;
struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
// One direct-component source, owned by an actual node draft. This fixture
// never mints a SQL journal receipt or claims fragment/package publication.
pub(super) struct FixtureBuilder {
    pub(super) fragment: novarocks_physical_plan::FragmentBuilder,
    pub(super) node: NodeId,
    pub(super) constants: ConstantPools,
}
impl FixtureBuilder {
    pub(super) fn new() -> Self {
        Self {
            fragment: novarocks_physical_plan::FragmentBuilder::new(FragmentId::new(71)),
            node: NodeId::new(901),
            constants: ConstantPools::empty(),
        }
    }
    pub(super) fn add(&mut self, ty: FunctionValueType, kind: ExprKind) -> ExprId {
        self.fragment
            .unit(self.node)
            .unwrap()
            .add(None, ty, kind)
            .unwrap()
    }
    pub(super) fn definitions(&self) -> UnitDefinitions<'_> {
        UnitDefinitions::from_draft(&self.fragment, self.node)
    }
    pub(super) fn definition(&self, id: ExprId) -> &ExprNode {
        self.definitions().definition(id).unwrap()
    }
    pub(super) fn root(&mut self, definition: ExprId, demand: EvaluationDemand) -> RootOrdinal {
        self.fragment
            .unit(self.node)
            .unwrap()
            .root(definition, demand, RootScope::AllPorts)
            .unwrap()
    }
    pub(super) fn seal(
        self,
        catalog: &EngineFunctionCatalog,
        constant_policy: ConstantPolicy,
    ) -> UnitFixture {
        self.seal_setup(catalog, constant_policy, true)
    }
    // The caller lends its existing observed work scope to real request,
    // installed-control and SDK seal authors. This is test-only setup, not a
    // SQL source credential or a second occurrence expansion implementation.
    pub(super) fn seal_observed(
        self,
        catalog: &dyn SqlFunctionCatalog,
        constant_policy: ConstantPolicy,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<UnitFixture, ComponentSealError> {
        self.seal_component(catalog, constant_policy, true, work)
    }
    pub(super) fn seal_without_requests(
        self,
        catalog: &EngineFunctionCatalog,
        constant_policy: ConstantPolicy,
    ) -> UnitFixture {
        self.seal_setup(catalog, constant_policy, false)
    }
    fn seal_setup(
        self,
        catalog: &dyn SqlFunctionCatalog,
        constant_policy: ConstantPolicy,
        with_requests: bool,
    ) -> UnitFixture {
        let mut work =
            CompileCheckpoints::try_new(&FixtureControl, CompilePhase::FunctionSpecialization)
                .unwrap();
        let result = self.seal_component(catalog, constant_policy, with_requests, &mut work);
        work.finish_result(result, ComponentSealError::Control)
            .unwrap()
    }
    fn seal_component(
        mut self,
        catalog: &dyn SqlFunctionCatalog,
        constant_policy: ConstantPolicy,
        with_requests: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<UnitFixture, ComponentSealError> {
        let sealed = seal_node_component(
            &mut self.fragment,
            self.node,
            &self.constants,
            catalog,
            constant_policy,
            with_requests,
            work,
        )?;
        Ok(UnitFixture {
            node: self.node,
            sealed,
            constants: self.constants,
        })
    }
}

pub(super) fn seal_node_observed(
    fragment: &mut novarocks_physical_plan::FragmentBuilder,
    node: NodeId,
    constants: &ConstantPools,
    catalog: &dyn SqlFunctionCatalog,
    constant_policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SealedUnit, ComponentSealError> {
    seal_node_component(
        fragment,
        node,
        constants,
        catalog,
        constant_policy,
        true,
        work,
    )
}
fn seal_node_component(
    fragment: &mut novarocks_physical_plan::FragmentBuilder,
    node: NodeId,
    constants: &ConstantPools,
    catalog: &dyn SqlFunctionCatalog,
    constant_policy: ConstantPolicy,
    with_requests: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SealedUnit, ComponentSealError> {
    let definitions: Vec<_> = fragment
        .construction_unit(node)
        .unwrap()
        .definitions()
        .filter_map(|source| {
            matches!(source.kind, ExprKind::FunctionCall { .. }).then_some(source.id)
        })
        .collect();
    for id in definitions {
        let (record, shape) = {
            let definitions = UnitDefinitions::from_draft(fragment, node);
            let source = definitions.definition(id).unwrap();
            let ExprKind::FunctionCall { function, args } = &source.kind else {
                unreachable!()
            };
            let request = if with_requests {
                Some(
                    author_physical_scalar_request_observed(
                        source,
                        definitions,
                        constants,
                        constant_policy,
                        work,
                    )
                    .map_err(ComponentSealError::request)?,
                )
            } else {
                None
            };
            let selected = match &request {
                Some(request) => std::sync::Arc::clone(request.selected()),
                None => author_scalar_result_selection_observed(function, None, work)?,
            };
            let logical_count = request.as_ref().map_or(args.len(), |request| {
                request.request().logical_argument_count
            });
            work.flush()?;
            let (_, control) = crate::compiler::selected_scalar_call::selected_scalar_control(
                catalog,
                &function.function_id,
                function.kind,
                &selected,
                logical_count,
                work.control(),
            )
            .map_err(|error| {
                ComponentSealError::occurrence(ExpressionOccurrenceError::function(error))
            })?;
            let shape = scalar_shape(control, args.len()).ok_or_else(|| {
                ComponentSealError::Occurrence(ExpressionOccurrenceError::InvalidFunctionControl(
                    id,
                ))
            })?;
            let record = match request {
                    Some(request) => Some(transfer_component_request_for_test(request.request(), constant_policy, work,
                        &mut |value, work| {
                            for (&pool_id, pool) in constants.entries() {
                                work.step()?;
                                if pool.backing_identity() == value.pool().backing_identity() {
                                    return Ok(novarocks_physical_plan::ConstantReference { pool: pool_id, ordinal: value.ordinal() });
                                }
                            }
                            Err(super::contract_lowering::ContractLoweringError::InvalidFunctionBinding {
                                detail: "component request has no original constant pool".into(),
                            })
                        }).map_err(ComponentSealError::transfer)?),
                    None => None,
                };
            (record, shape)
        };
        let draft = fragment.unit(node).unwrap();
        if let Some(record) = record {
            draft
                .request(id, record)
                .map_err(ComponentSealError::unit)?;
        }
        draft
            .definition_control(
                id,
                DefinitionControl {
                    shape,
                    temporal_sources: None,
                },
            )
            .map_err(ComponentSealError::unit)?;
    }
    let ports = if fragment.construction_node(node).is_some() {
        fragment.node_ports(node).unwrap()
    } else {
        vec![]
    };
    let sealed = fragment
        .take_unit_builder(node)
        .unwrap()
        .seal(ports, work)
        .map_err(ComponentSealError::unit)?;
    Ok(sealed)
}

#[derive(Debug)]
pub(super) enum ComponentSealError {
    Control(CompileControlError),
    Request(super::physical_scalar_requests::PhysicalScalarRequestError),
    Occurrence(ExpressionOccurrenceError),
    Transfer(super::contract_lowering::ContractLoweringError),
    Unit(UnitError),
}
impl From<CompileControlError> for ComponentSealError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl ComponentSealError {
    fn request(error: super::physical_scalar_requests::PhysicalScalarRequestError) -> Self {
        match error {
            super::physical_scalar_requests::PhysicalScalarRequestError::Control(cause) => {
                Self::Control(cause)
            }
            other => Self::Request(other),
        }
    }
    fn occurrence(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Occurrence(other),
        }
    }
    fn transfer(error: super::contract_lowering::ContractLoweringError) -> Self {
        match error {
            super::contract_lowering::ContractLoweringError::Control(cause) => Self::Control(cause),
            other => Self::Transfer(other),
        }
    }
    fn unit(error: UnitError) -> Self {
        match error {
            UnitError::Control(cause) => Self::Control(cause),
            other => Self::Unit(other),
        }
    }
}

pub(super) struct UnitFixture {
    pub(super) node: NodeId,
    pub(super) sealed: SealedUnit,
    pub(super) constants: ConstantPools,
}
impl UnitFixture {
    pub(super) fn definitions(&self) -> UnitDefinitions<'_> {
        UnitDefinitions::from_sealed(
            crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(self.node),
            &self.sealed,
        )
    }
    pub(super) fn definition(&self, id: ExprId) -> &ExprNode {
        self.sealed.definition(id).unwrap()
    }
    pub(super) fn flow(&self) -> &ExpressionControlFlow<ExprId> {
        self.sealed.control_flow()
    }
    pub(super) fn root_use(&self, root: RootOrdinal) -> ExpressionUseId {
        self.sealed.root_use_ids()[root.get() as usize]
    }
    pub(super) fn validate_claims(
        &self,
        claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
    ) -> BoundExprSet {
        let parts = UnitParts::new(
            self.sealed.ports().to_vec().into_boxed_slice(),
            self.sealed.definitions().to_vec().into_boxed_slice(),
            self.sealed.roots().to_vec().into_boxed_slice(),
            self.sealed.root_use_ids().to_vec().into_boxed_slice(),
            self.sealed.control_flow().clone(),
            claims,
            self.sealed.requests().clone(),
        );
        let control = FixtureControl;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let unit = BoundExprSet::try_from_parts_in(
            parts,
            UnitLimits {
                expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
            },
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        unit
    }
}
