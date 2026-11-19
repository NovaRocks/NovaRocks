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

//! Public fragment compilation owns node-to-unit and real runtime-column mapping.
//! These fixed inputs use no relation or scalar call; actual constants remain
//! source-owned pools. No SQL, provider runtime or execution dependency is needed.
use arrow_schema::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_constant_contract::{ConstantPoolId, ConstantPools, ConstantReference};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::{ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, ProgramNodeKind};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}

#[test]
fn node_units_keep_the_actual_column_after_a_repeated_publication() {
    let c = Control;
    let ty = FunctionValueType::new(DataType::Int64, false);
    let mut pools = ConstantPools::empty();
    let mut builder = FragmentBuilder::new(FragmentId::new(37));
    let source = NodeId::new(900);
    let duplicate = NodeId::new(7);
    let final_node = NodeId::new(0);
    let mut source_roots = Vec::new();
    let mut source_values = Vec::new();
    for (ordinal, number) in [5, 7].into_iter().enumerate() {
        let value = ConstantValue::from_i64(
            Arc::new(ty.try_to_field("source").unwrap()),
            ty.clone(),
            number,
            policy(),
            CompilePhase::Validate,
            &c,
        )
        .unwrap();
        let pool = ConstantPoolId::new(ordinal as u32);
        pools.insert(pool, value.pool().clone()).unwrap();
        let def = builder
            .unit(source)
            .unwrap()
            .add(
                None,
                ty.clone(),
                ExprKind::Constant(ConstantReference {
                    pool,
                    ordinal: value.ordinal(),
                }),
            )
            .unwrap();
        let root = builder
            .unit(source)
            .unwrap()
            .root(def, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        source_roots.push(root);
        source_values.push(
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    builder
        .add_values(
            source,
            Box::from([source_roots.into_boxed_slice()]),
            source_values.clone().into_boxed_slice(),
        )
        .unwrap();
    let a = builder
        .unit(duplicate)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Value(source_values[0]))
        .unwrap();
    let b = builder
        .unit(duplicate)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Value(source_values[1]))
        .unwrap();
    let mut roots = Vec::new();
    for def in [a, a, b] {
        roots.push(
            builder
                .unit(duplicate)
                .unwrap()
                .root(def, EvaluationDemand::Value, RootScope::Port(0))
                .unwrap(),
        );
    }
    let projected = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: duplicate,
                root: roots[0],
            },
        )
        .unwrap();
    let other = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: duplicate,
                root: roots[2],
            },
        )
        .unwrap();
    builder
        .add_project(
            duplicate,
            source,
            Box::from([
                (roots[0], projected),
                (roots[1], projected),
                (roots[2], other),
            ]),
            Box::from([projected, projected, other]),
        )
        .unwrap();
    let def = builder
        .unit(final_node)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Value(other))
        .unwrap();
    let root = builder
        .unit(final_node)
        .unwrap()
        .root(def, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: final_node,
                root,
            },
        )
        .unwrap();
    builder
        .add_project(
            final_node,
            duplicate,
            Box::from([(root, output)]),
            Box::from([output]),
        )
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    for node in [source, duplicate, final_node] {
        let ports = builder.node_ports(node).unwrap();
        let unit = builder
            .take_unit_builder(node)
            .unwrap()
            .seal(ports, &mut work)
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    let fragment = builder
        .finish_definition(
            final_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&final_node].output.clone(),
        fields: Box::from([ResultField {
            domain: ResultValueDomain::Plain,
            name: "actual_other".into(),
            alias: None,
            value: output,
            ty,
        }]),
    };
    let admission = FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    };
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([37; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: pools,
                fragment,
                statement_constant_policy: policy(),
                pruning: FrozenFragmentPruning::try_new(FragmentId::new(37), vec![], &c).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: BTreeMap::new(),
            },
            admission,
            &c,
        )
        .unwrap(),
    );
    let providers = PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &c).unwrap();
    let functions = catalogue();
    let program = compile_fragment(
        validate_fragment_providers(package, &providers, &c).unwrap(),
        &functions,
        LocalCompileOptions {
            pipeline_dop: NonZeroUsize::new(1).unwrap(),
            root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
            kernel_abi: KernelAbiVersion::CURRENT,
            constants: policy(),
        },
        &c,
    )
    .unwrap();
    let graph = program.graph();
    let ProgramNodeKind::Project { exprs, .. } = graph.nodes()[2].kind() else {
        panic!("final Project")
    };
    let reference = exprs[0];
    let unit = &program.units()[reference.unit.index()];
    let root = unit.root(reference.root).unwrap();
    let binding = unit.input(root.use_id).unwrap();
    assert_eq!(binding.scope, RootScope::Port(0));
    assert_eq!(binding.ordinal, 2);
    let scope = unit.scope(binding.scope).unwrap();
    assert_eq!(scope.schema().fields().len(), 3);
    assert_eq!(scope.columns().len(), 2);
    assert!(Arc::ptr_eq(
        scope.schema(),
        graph.nodes()[1].output_layout().schema()
    ));
    assert_eq!(scope.columns()[1].ordinal, 2);
    assert!(program.resolved_calls().calls().is_empty());
    assert_eq!(program.units().len(), 3);
}

fn catalogue() -> novarocks_functions::PureEngineFunctionCatalog {
    use novarocks_functions::{
        FunctionId, FunctionKind, FunctionOverloadId, InstalledPureKernel,
        PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
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
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
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
