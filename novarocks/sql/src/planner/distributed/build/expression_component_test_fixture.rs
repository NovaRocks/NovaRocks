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

//! Direct relation fixtures use the production expression author.
use std::collections::BTreeMap;

use novarocks_expression::{
    CallAuthorError, CallAuthorInputs, EvaluationEnvironment, UnitCallAuthorship,
    author_call_controls, author_unit_calls,
};
use novarocks_expression_contract::{
    ExprId, ExprKind, ExprNode, RootOrdinal, RootScope, SealedUnit, UnitError,
};
use novarocks_functions::ConstantValidation;
use novarocks_physical_plan::{ConstantPools, FragmentBuilder, FragmentId, NodeId};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    ExpressionControlFlow, ExpressionUseId, FunctionValueType, SemanticParameters,
};

use super::{
    contract_lowering::transfer_component_request_for_test,
    physical_scalar_requests::author_physical_scalar_request_observed,
    unit_source::UnitDefinitions,
};

pub(super) fn author_component_calls(
    unit: &SealedUnit,
    constants: &ConstantPools,
    parameters: &SemanticParameters,
    overflow: DecimalOverflowPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnitCallAuthorship, CallAuthorError> {
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let decimal_overflow = unit
        .uses()
        .values()
        .filter_map(|usage| {
            let definition = unit.definition(usage.definition)?;
            matches!(definition.kind, ExprKind::FunctionCall { .. })
                .then_some((definition.id, overflow))
        })
        .collect();
    author_unit_calls(
        unit,
        &CallAuthorInputs {
            environment: EvaluationEnvironment {
                constants,
                parameters,
            },
            functions: &functions,
            decimal_overflow: &decimal_overflow,
        },
        work,
    )
}

pub(super) fn seal_node_observed(
    fragment: &mut FragmentBuilder,
    node: NodeId,
    constants: &ConstantPools,
    functions: &novarocks_functions::PureEngineFunctionCatalog,
    policy: ConstantValidation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SealedUnit, ComponentSealError> {
    let definitions = UnitDefinitions::from_draft(fragment, node);
    let mut requests = Vec::new();
    for source in fragment.construction_unit(node).unwrap().definitions() {
        if !matches!(source.kind, ExprKind::FunctionCall { .. }) {
            continue;
        }
        let input = author_physical_scalar_request_observed(
            source,
            definitions.clone(),
            constants,
            policy,
            work,
        )
        .map_err(ComponentSealError::request)?;
        let request =
            transfer_component_request_for_test(input.request(), work, &mut |value, work| {
                for (&pool_id, pool) in constants.entries() {
                    work.step()?;
                    if pool.backing_identity() == value.pool().backing_identity() {
                        return Ok(novarocks_physical_plan::ConstantReference {
                            pool: pool_id,
                            ordinal: value.ordinal(),
                        });
                    }
                }
                Err(
                    super::contract_lowering::ContractLoweringError::InvalidFunctionBinding {
                        detail: "component request has no original constant pool".into(),
                    },
                )
            })
            .map_err(ComponentSealError::transfer)?;
        requests.push((source.id, request));
    }
    let ports = if fragment.construction_node(node).is_some() {
        fragment.node_ports(node).unwrap()
    } else {
        vec![]
    };
    let mut draft = fragment.take_unit_builder(node).unwrap();
    for (id, request) in requests {
        draft
            .request(id, request)
            .map_err(ComponentSealError::unit)?;
    }
    let parameters = SemanticParameters::try_new([]).unwrap();
    let decimal_overflow = BTreeMap::new();
    let inputs = CallAuthorInputs {
        environment: EvaluationEnvironment {
            constants,
            parameters: &parameters,
        },
        functions,
        decimal_overflow: &decimal_overflow,
    };
    for (definition, control) in
        author_call_controls(&draft, &inputs, work).map_err(ComponentSealError::author)?
    {
        draft
            .definition_control(definition, control)
            .map_err(ComponentSealError::unit)?;
    }
    draft.seal(ports, work).map_err(ComponentSealError::unit)
}

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
    pub(super) fn seal_observed(
        mut self,
        functions: &novarocks_functions::PureEngineFunctionCatalog,
        policy: ConstantValidation,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<UnitFixture, ComponentSealError> {
        Ok(UnitFixture {
            sealed: seal_node_observed(
                &mut self.fragment,
                self.node,
                &self.constants,
                functions,
                policy,
                work,
            )?,
        })
    }
}
#[derive(Debug)]
pub(super) enum ComponentSealError {
    Control(CompileControlError),
    Request(super::physical_scalar_requests::PhysicalScalarRequestError),
    Author(CallAuthorError),
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
    fn author(error: CallAuthorError) -> Self {
        match error {
            CallAuthorError::Control(cause) => Self::Control(cause),
            other => Self::Author(other),
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
    pub(super) sealed: SealedUnit,
}
impl UnitFixture {
    pub(super) fn definition(&self, id: ExprId) -> &ExprNode {
        self.sealed.definition(id).unwrap()
    }
    pub(super) fn flow(&self) -> &ExpressionControlFlow<ExprId> {
        self.sealed.control_flow()
    }
    pub(super) fn root_use(&self, root: RootOrdinal) -> ExpressionUseId {
        self.sealed.root_use_ids()[root.get() as usize]
    }
}
