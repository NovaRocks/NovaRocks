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

//! One real builtin owner through the final mandatory checked chain. This is
//! a subset algorithm test, not Server catalogue or native execution closure.

use crate::*;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder,
    EvaluatedArgument, FunctionArgument, FunctionArgumentType, FunctionBindingRequest, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionResultType, FunctionValueType, InstalledPureKernel,
    KernelEvaluationControl, KernelFailure, PreparedPureKernel, PureCallPreparation,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    PureCompileControl, SemanticParameters,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct OriginalCompileControl(Mutex<Vec<(CompilePhase, u32)>>);
impl PureCompileControl for OriginalCompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        self.0.lock().unwrap().push((phase, units));
        Ok(())
    }
}
struct Evaluation;
impl KernelEvaluationControl for Evaluation {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("RNG must not wait");
    }
}

fn actual_rng_catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let definition = actual
        .definition("rand", FunctionKind::Scalar)
        .unwrap()
        .clone();
    // Retain the actual owner/resolver from production registration. These two
    // independent installed records describe the real RNG CPU implementation,
    // not declaration iteration masquerading as a Server installation seal.
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    let function = FunctionId::try_new("builtin.scalar/rand/v1").unwrap();
    let implementation = PureImplementationId::try_new("builtin.scalar/rand/selected-v1").unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: function.clone(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: implementation.clone(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

#[test]
fn final_program_borrows_actual_rng_kernel_and_isolates_fresh_instance_state() {
    let control = OriginalCompileControl::default();
    let catalogue = actual_rng_catalogue();
    let seed_type = FunctionValueType::new(DataType::Int64, false);
    let arguments = [FunctionArgument::Value {
        value_type: seed_type.clone(),
        constant: Some(
            ConstantValue::from_i64(
                Arc::new(seed_type.try_to_field("fixture").unwrap()),
                seed_type.clone(),
                42,
                ConstantPolicy {
                    max_rows: 1,
                    max_array_nodes: 1,
                    max_logical_elements: 1,
                    max_retained_buffer_bytes: 4096,
                    max_type_depth: 1,
                    max_type_nodes: 1,
                    max_dictionary_depth: 0,
                    max_metadata_bytes: 4096,
                    max_library_validation_work: 65536,
                    max_library_validation_bytes: 65536,
                },
                CompilePhase::FunctionSpecialization,
                &control,
            )
            .unwrap(),
        ),
    }];
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: 1,
    };
    let bound = catalogue
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &control)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("RNG is scalar");
    };
    let result_type = result_type.clone();
    assert_eq!(result_type.data_type(), &DataType::Float64);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        region: EvaluationRegionId::new(u32::MAX),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(u32::MAX))];
    let input = CallEffectInput {
        context,
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&uses),
        function_id: &bound.function_id,
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Region(context.region),
    };
    let options = || PureCallPreparation::Scalar {
        arguments: ScopedExpressionEffects::pure_value(context),
    };
    let authored = catalogue
        .prepare_fresh(input, selected.clone(), options(), &control)
        .unwrap();
    let frozen_facts = authored.call_contract().effects().clone();
    let frozen = catalogue
        .prepare_frozen(input, selected.clone(), &frozen_facts, options(), &control)
        .unwrap();
    let PreparedPureKernel::Scalar(expected_kernel) = frozen.prepared() else {
        panic!("RNG scalar lifecycle");
    };
    let expected_kernel = expected_kernel.clone();
    assert_eq!(
        expected_kernel.contract().effects().instance_state,
        FunctionInstanceState::ScalarInstance
    );

    let arena = Arc::new(
        ImmutableExpressions::try_new_for_compile(
            vec![
                StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Int64(42)),
                    DataType::Int64,
                    None,
                ),
                StaticExprNode::new(
                    StaticExprKind::BoundCall {
                        args: vec![ProgramExprId::new(0)],
                    },
                    DataType::Float64,
                    None,
                ),
            ],
            false,
            HashMap::new(),
            None,
            &control,
        )
        .unwrap(),
    );
    let source_layout = StaticLayout::try_new_for_compile(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            DataType::Int64,
            false,
        )])),
        Arc::from([SlotId::new(77)]),
        &control,
    )
    .unwrap();
    let values = StaticValues::try_new_for_compile(
        RecordBatch::try_new(
            source_layout.schema().clone(),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .unwrap(),
        source_layout.clone(),
        &control,
    )
    .unwrap();
    let output_layout = StaticLayout::try_new_for_compile(
        Arc::new(Schema::new(vec![
            result_type.try_to_field("sample").unwrap(),
        ])),
        Arc::from([SlotId::new(u32::MAX)]),
        &control,
    )
    .unwrap();
    let requirements = BindingRequirements::try_new_for_compile(
        vec![BindingRequirement::ResultSink {
            layout: output_layout.clone(),
        }],
        &control,
    )
    .unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(2).unwrap(),
        None,
        output_layout.identity_for_compile(&control).unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let graph = LocalProgramGraph::try_new_with_sink_for_compile(
        vec![
            ProgramNode::new_local(
                ProgramNodeId::new(0),
                vec![DiagnosticSourceNodeId::new(0)],
                ProgramNodeKind::Values { values },
                source_layout,
            ),
            ProgramNode::new_local(
                ProgramNodeId::new(1),
                vec![DiagnosticSourceNodeId::new(u32::MAX)],
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![ProgramExprId::new(1)],
                    expr_slot_ids: output_layout.slots().to_vec(),
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                output_layout,
            ),
        ],
        ProgramNodeId::new(1),
        arena.clone(),
        profile,
        requirements,
        Some(StaticSinkProgram::Result),
        &control,
    )
    .unwrap();
    let root_site = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    };
    let flow = ProgramControlFlow::try_new(
        vec![ProgramEvaluationRegion {
            id: context.region,
            parent: None,
            guard: None,
        }],
        vec![
            ProgramExpressionUse {
                context,
                definition: ProgramExprId::new(1),
                control: ControlShape::Eager,
                arguments: Box::from([ExpressionUseId::new(u32::MAX)]),
            },
            ProgramExpressionUse {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(u32::MAX),
                    ..context
                },
                definition: ProgramExprId::new(0),
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        2,
        &control,
    )
    .unwrap();
    let snapshot = ProgramRootControlBindings::try_new(
        graph,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        vec![ProgramRootUseBinding {
            site: root_site,
            use_id: context.use_id,
        }],
        &control,
    )
    .unwrap();
    let call_site = ProgramCallSite::Expression(ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: context.use_id,
    });
    let calls =
        ProgramResolvedCalls::try_new(snapshot, vec![(call_site, frozen)], &control).unwrap();
    let expressions = ProgramTypedExpressions::try_new(
        calls,
        BTreeMap::from([(
            ProgramExpressionArena::Main,
            vec![
                FunctionArgumentType::Value(seed_type.clone()),
                FunctionArgumentType::Value(result_type.clone()),
            ],
        )]),
        &control,
    )
    .unwrap();
    let channels = ProgramTypedChannels::try_new(
        expressions,
        vec![
            (
                ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(0),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: 0,
                },
                seed_type,
            ),
            (
                ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(1),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: 0,
                },
                result_type,
            ),
        ],
        &control,
    )
    .unwrap();
    let lexical = ProgramLexicalBindings::try_new(channels, vec![], vec![], &control).unwrap();
    let metrics = OperatorMetricAggregation {
        cpu_time: MetricAggregation::Sum,
        wall_time: MetricAggregation::Maximum,
        peak_retained_bytes: MetricAggregation::Maximum,
    };
    let operators = [(37, 0, 0), (u32::MAX, 1, u32::MAX)]
        .into_iter()
        .map(|(operator, node, source)| {
            let id = LocalOperatorId::new(operator);
            LocalOperatorProvenance {
                id,
                lowered_nodes: Box::from([ProgramNodeId::new(node)]),
                sources: Box::from([DiagnosticSourceNodeId::new(source)]),
                origin: LocalOperatorOrigin::Direct,
                cost_owner: id,
                metrics,
            }
        })
        .collect();
    let allowed = BTreeSet::from([
        DiagnosticSourceNodeId::new(0),
        DiagnosticSourceNodeId::new(u32::MAX),
    ]);
    let program = LocalProgram::try_new(
        lexical,
        operators,
        &allowed,
        CompiledProgramFacts {
            writes: BTreeMap::new(),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &control,
    )
    .unwrap();
    assert!(Arc::ptr_eq(program.graph().expressions(), &arena));
    assert_eq!(program.graph().profile().pipeline_dop().get(), 2);
    assert_eq!(
        program
            .provenance()
            .operators()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![LocalOperatorId::new(37), LocalOperatorId::new(u32::MAX)]
    );
    let ProgramStateTemplate::Scalar { scope, kernel } = program.state_template(call_site).unwrap()
    else {
        panic!("actual scalar state template");
    };
    assert_eq!(scope.root, root_site);
    assert_eq!(scope.occurrence.use_id, context.use_id);
    assert!(Arc::ptr_eq(kernel, &expected_kernel));
    assert!(Arc::ptr_eq(
        kernel.contract().call().selected_owner(),
        &selected
    ));

    // Independent seed-42 bits captured from official rand 0.8.5 StdRng.
    // Advancing A does not consume B; both borrow the same immutable recipe.
    let mut a = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let mut b = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let seed: ArrayRef = Arc::new(Int64Array::from(vec![42]));
    let evaluated = [EvaluatedArgument::Scalar(&seed)];
    let bits = |instance: &mut ScalarEvaluationInstance, rows| {
        let output = instance
            .evaluate(Selection::all(rows), &evaluated, &Evaluation)
            .unwrap();
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        bits(&mut a, 2),
        vec![0x3fe0d98eec6444e4, 0x3fe15e014267f5aa]
    );
    assert_eq!(bits(&mut b, 1), vec![0x3fe0d98eec6444e4]);
    assert_eq!(bits(&mut a, 1), vec![0x3fe45dec0e3bca26]);
    assert_eq!(bits(&mut b, 1), vec![0x3fe15e014267f5aa]);
    assert_eq!(a.retained_upper_bound(), b.retained_upper_bound());
    assert!(!control.0.lock().unwrap().is_empty());
}
