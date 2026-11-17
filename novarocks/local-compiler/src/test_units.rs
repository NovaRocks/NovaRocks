// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Test authors for actual private pure units and their published references.
use novarocks_expression::{CompiledExprSet, CompiledRoot, StaticExprKind};
use novarocks_expression_contract::{ExprId, ExprKind, RootOrdinal, RootScope, UnitError};
use novarocks_local_program::{LocalProgram, ProgramRootRef};
use novarocks_physical_plan::{FragmentBuilder, NodeId};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionArgumentType, FunctionValueType,
    PureCompileControl,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) trait PureUnitFixture {
    fn fixture_definition(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> Result<ExprId, UnitError>;
    fn fixture_occurrence(
        &mut self,
        node: NodeId,
        definition: ExprId,
        demand: EvaluationDemand,
        scope: RootScope,
    ) -> Result<RootOrdinal, UnitError>;
    fn fixture_value_root(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> Result<RootOrdinal, UnitError> {
        let definition = self.fixture_definition(node, ty, kind)?;
        self.fixture_occurrence(
            node,
            definition,
            EvaluationDemand::Value,
            RootScope::Port(0),
        )
    }
    fn fixture_repeated_root(
        &mut self,
        node: NodeId,
        root: RootOrdinal,
    ) -> Result<RootOrdinal, UnitError>;
    fn finish_fixture_unit(
        &mut self,
        node: NodeId,
        control: &dyn PureCompileControl,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
impl PureUnitFixture for FragmentBuilder {
    fn fixture_definition(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> Result<ExprId, UnitError> {
        self.unit(node)
            .expect("test owns this node's actual draft")
            .add(None, ty, kind)
    }
    fn fixture_occurrence(
        &mut self,
        node: NodeId,
        definition: ExprId,
        demand: EvaluationDemand,
        scope: RootScope,
    ) -> Result<RootOrdinal, UnitError> {
        self.unit(node)
            .expect("test owns this node's actual draft")
            .root(definition, demand, scope)
    }
    fn fixture_repeated_root(
        &mut self,
        node: NodeId,
        root: RootOrdinal,
    ) -> Result<RootOrdinal, UnitError> {
        let unit = self.unit(node).expect("test owns the original draft");
        let source = &unit.roots()[root.get() as usize];
        let (definition, demand, scope) = (source.definition, source.demand, source.scope);
        unit.root(definition, demand, scope)
    }
    fn finish_fixture_unit(
        &mut self,
        node: NodeId,
        control: &dyn PureCompileControl,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ports = self.node_ports(node)?;
        let draft = self.take_unit_builder(node)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let unit = draft
            .seal(ports, &mut work)?
            .finish(BTreeMap::new(), &mut work)?;
        self.install_unit(node, Arc::new(unit))?;
        Ok(())
    }
}
pub(crate) fn compiled_root(
    program: &LocalProgram,
    reference: ProgramRootRef,
) -> (&CompiledExprSet, CompiledRoot) {
    let unit = program.unit(reference.unit).expect("actual program unit");
    (
        unit,
        *unit.root(reference.root).expect("actual root ordinal"),
    )
}
pub(crate) fn kind(program: &LocalProgram, reference: ProgramRootRef) -> &StaticExprKind {
    let (unit, root) = compiled_root(program, reference);
    unit.definitions().node(root.definition).unwrap().kind()
}
pub(crate) fn root_type(
    program: &LocalProgram,
    reference: ProgramRootRef,
) -> &FunctionArgumentType {
    let (unit, root) = compiled_root(program, reference);
    &unit.types()[root.definition.index()]
}
pub(crate) fn input_ordinal(program: &LocalProgram, reference: ProgramRootRef) -> u32 {
    let (unit, root) = compiled_root(program, reference);
    assert!(matches!(kind(program, reference), StaticExprKind::Input));
    unit.input(root.use_id)
        .expect("actual per-use input binding")
        .ordinal
}
