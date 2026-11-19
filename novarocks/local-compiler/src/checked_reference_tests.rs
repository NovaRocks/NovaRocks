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

use super::*;
use arrow_array::{Array, ArrayRef, Int64Array, StructArray};
use arrow_schema::{DataType, Field};
use novarocks_expression::{EvaluationEnvironment, ScopeLayouts, StaticExprKind};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, RootOrdinal, RootScope, UnitLimits,
};
use novarocks_functions::{ConstantPolicy, ConstantValue, FunctionKind, PureEngineFunctionCatalog};
use novarocks_functions::{ConstantPool, EngineFunctionCatalogBuilder, InstalledPureKernel};
use novarocks_local_program::ProgramRootRef;
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, Fragment, FragmentBuilder, FragmentCuts,
    FragmentId, FragmentPackage, FragmentPackageInput, FragmentSink, FrozenFragmentPruning, NodeId,
    PipelineDopDomain, PlanLimits, PlanVersionId, RequiredContracts, ResultField, ResultPort,
    StagePlacementPolicy, ValidationErrors, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::HashMap, sync::Mutex};

struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(vec![]),
            refusal: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            refusal: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() < at, "no callback after the primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if trace.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), false);
    let field = Arc::new(
        Field::new("authored-source", array.data_type().clone(), false).with_metadata(
            HashMap::from([("provider.unknown".into(), "keep-root-facts".into())]),
        ),
    );
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
fn integer_pool() -> ConstantPool {
    pool(Arc::new(Int64Array::from(vec![9, 42, -7])))
}
fn functions() -> PureEngineFunctionCatalog {
    use novarocks_functions::{
        FunctionId, FunctionOverloadId, PureImplementationDeclaration, PureImplementationId,
        PureKernelAbi,
    };
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    // A real RAND-only registry satisfies the catalogue's nonempty contract;
    // these roots do not manufacture or prepare a RAND invocation.
    builder
        .seal_pure(
            ["()->f64;strict;legacy", "(i64)->f64;strict;legacy"]
                .into_iter()
                .map(|profile| InstalledPureKernel {
                    function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                    kind: FunctionKind::Scalar,
                    implementation: PureImplementationDeclaration {
                        overload: FunctionOverloadId::try_new(format!(
                            "builtin.scalar/rand/{profile}"
                        ))
                        .unwrap(),
                        implementation: PureImplementationId::try_new(
                            "builtin.scalar/rand/selected-v1",
                        )
                        .unwrap(),
                        abi: PureKernelAbi::ScalarV1,
                    },
                    aggregate_state_format: None,
                }),
        )
        .unwrap()
}

trait UnitFixture {
    fn define(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> Result<ExprId, novarocks_expression_contract::UnitError>;
    fn add_root(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
        scope: RootScope,
        demand: EvaluationDemand,
    ) -> Result<RootOrdinal, novarocks_expression_contract::UnitError>;
    fn root_definition(
        &mut self,
        node: NodeId,
        definition: ExprId,
        scope: RootScope,
        demand: EvaluationDemand,
    ) -> RootOrdinal;
    fn repeat_root(&mut self, node: NodeId, root: RootOrdinal) -> RootOrdinal;
    fn finish_fixture(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop: PipelineDopDomain,
        placement: StagePlacementPolicy,
    ) -> Result<Fragment, ValidationErrors>;
}
impl UnitFixture for FragmentBuilder {
    fn define(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> Result<ExprId, novarocks_expression_contract::UnitError> {
        self.unit(node)
            .expect("fixture original draft")
            .add(None, ty, kind)
    }
    fn add_root(
        &mut self,
        node: NodeId,
        ty: FunctionValueType,
        kind: ExprKind,
        scope: RootScope,
        demand: EvaluationDemand,
    ) -> Result<RootOrdinal, novarocks_expression_contract::UnitError> {
        let d = self.define(node, ty, kind)?;
        self.unit(node).unwrap().root(d, demand, scope)
    }
    fn root_definition(
        &mut self,
        node: NodeId,
        definition: ExprId,
        scope: RootScope,
        demand: EvaluationDemand,
    ) -> RootOrdinal {
        self.unit(node)
            .unwrap()
            .root(definition, demand, scope)
            .unwrap()
    }
    fn repeat_root(&mut self, node: NodeId, root: RootOrdinal) -> RootOrdinal {
        let r = self.unit(node).unwrap().roots()[root.get() as usize];
        self.unit(node)
            .unwrap()
            .root(r.definition, r.demand, r.scope)
            .unwrap()
    }
    fn finish_fixture(
        mut self,
        root: NodeId,
        sink: FragmentSink,
        dop: PipelineDopDomain,
        placement: StagePlacementPolicy,
    ) -> Result<Fragment, ValidationErrors> {
        let nodes = self
            .construction_nodes()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for node in nodes {
            if self.construction_unit(node).is_some() {
                let ports = self.node_ports(node).unwrap();
                let d = self.take_unit_builder(node).unwrap();
                let control = Control::good();
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
                let unit = d
                    .seal(ports, &mut work)
                    .unwrap()
                    .finish(BTreeMap::new(), &mut work)
                    .unwrap();
                work.finish().unwrap();
                self.install_unit(node, Arc::new(unit)).unwrap();
            }
        }
        self.finish_definition(root, sink, dop, placement)
    }
}
fn lower(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, crate::FragmentCreationError<std::convert::Infallible>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let mut lowered =
        LoweredExpressions::new::<std::convert::Infallible>(package, None, &mut work)?;
    for node in package.fragment().nodes().values() {
        if let Some(unit) = &node.expressions {
            assert!(
                unit.ports().is_empty(),
                "the direct Values component fixtures have no runtime input"
            );
            let compiled = novarocks_expression::compile_expr_set_pure(
                unit,
                EvaluationEnvironment {
                    constants: package.constants(),
                    parameters: package.parameters(),
                },
                ScopeLayouts {
                    ports: &[],
                    all_ports: None,
                },
                policy(),
                &functions(),
                &mut work,
            )
            .map_err(ExpressionLoweringError::from)?;
            lowered.units[lowered.node_units[&node.id].index()] = Some(compiled);
        }
    }
    work.finish()?;
    Ok(lowered)
}
/// The negative Values component receives another genuine immutable unit.
/// It has a different actual typed CV, rather than mutating the published unit.
fn replace_constant(
    lowered: &mut LoweredExpressions,
    reference: ProgramRootRef,
    value: ConstantValue,
) {
    let original = lowered.units[reference.unit.index()].as_ref().unwrap();
    let mut pools = ConstantPools::empty();
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
    });
    for (ordinal, root) in original.roots().iter().enumerate() {
        let actual = if ordinal == reference.root.get() as usize {
            value.clone()
        } else {
            let StaticExprKind::Constant(value) =
                original.definitions().node(root.definition).unwrap().kind()
            else {
                panic!("constant Values fixture")
            };
            value.clone()
        };
        let pool = ConstantPoolId::new(ordinal as u32);
        pools.insert(pool, actual.pool().clone()).unwrap();
        let definition = builder
            .add(
                None,
                actual.value_type().clone(),
                ExprKind::Constant(ConstantReference {
                    pool,
                    ordinal: actual.ordinal(),
                }),
            )
            .unwrap();
        builder
            .root(definition, root.demand, RootScope::AllPorts)
            .unwrap();
    }
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let unit = builder
        .seal(vec![], &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let compiled = novarocks_expression::compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        policy(),
        &functions(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    lowered.units[reference.unit.index()] = Some(compiled);
}
fn root_definition(
    program: &novarocks_local_program::LocalProgram,
    reference: ProgramRootRef,
) -> novarocks_expression::CompiledExprId {
    program
        .unit(reference.unit)
        .unwrap()
        .root(reference.root)
        .unwrap()
        .definition
}
fn root_use(
    program: &novarocks_local_program::LocalProgram,
    reference: ProgramRootRef,
) -> novarocks_type_contract::ExpressionUseId {
    program
        .unit(reference.unit)
        .unwrap()
        .root(reference.root)
        .unwrap()
        .use_id
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

#[path = "values_lowering_tests.rs"]
pub(crate) mod values_lowering_tests;

#[path = "sort_lowering_tests.rs"]
mod sort_lowering_tests;

#[path = "topn_lowering_tests.rs"]
mod topn_lowering_tests;
