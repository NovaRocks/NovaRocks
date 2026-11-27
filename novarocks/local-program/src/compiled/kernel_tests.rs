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
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EvaluatedArgument, FunctionArgument,
    FunctionBindingRequest, FunctionKind, FunctionResultType, FunctionValueType,
    KernelEvaluationControl, KernelFailure, PreparedPureKernel, PureCallPreparation,
    ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    FunctionInstanceState, PureCompileControl, SemanticParameters,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
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

#[test]
fn final_program_borrows_actual_rng_kernel_and_isolates_fresh_instance_state() {
    let control = OriginalCompileControl::default();
    let catalogue = crate::test_support::actual_rng_catalogue();
    let bound_type = FunctionValueType::new(DataType::Int64, false);
    let policy = ConstantPolicy {
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
    };
    let arguments = [FunctionArgument::Value {
        value_type: bound_type.clone(),
        constant: Some(
            ConstantValue::from_i64(
                Arc::new(bound_type.try_to_field("fixture").unwrap()),
                bound_type.clone(),
                1,
                policy,
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
    assert_eq!(result_type.data_type(), &DataType::Int64);
    use novarocks_constant_contract::{ConstantPoolId, ConstantPools, ConstantReference};
    use novarocks_expression::{EvaluationEnvironment, ScopeLayouts, compile_expr_set_pure};
    use novarocks_expression_contract::{
        BoundExprSetBuilder, BoundFunction, DefinitionControl, ExprKind, ExpressionCallClaim,
        PhysicalCallRequest, RootOrdinal, RootScope, UnitLimits,
    };
    use novarocks_type_contract::{CompileCheckpoints, ControlShape, MAX_CONTROL_DEPTH};
    let parameters = SemanticParameters::try_new([]).unwrap();
    let source_layout = StaticLayout::try_new_typed_for_compile(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            DataType::Int64,
            false,
        )])),
        Arc::from([SlotId::new(77)]),
        vec![(bound_type.typed_binding().clone(), None)],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &control,
    )
    .unwrap();
    let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = &arguments[0]
    else {
        panic!("actual admitted bound constant")
    };
    let value = value.clone();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(17),
        ordinal: value.ordinal(),
    };
    let mut pools = ConstantPools::empty();
    pools.insert(reference.pool, value.pool().clone()).unwrap();
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let bound_expression = builder
        .add(None, bound_type.clone(), ExprKind::Constant(reference))
        .unwrap();
    let call = builder
        .add(
            None,
            result_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction::from_exact_signature(
                    bound.function_id.clone(),
                    selected.overload.clone(),
                    FunctionKind::Scalar,
                    selected.argument_types.clone(),
                    result_type.clone(),
                ),
                args: Box::from([bound_expression]),
            },
        )
        .unwrap();
    builder
        .request(
            call,
            PhysicalCallRequest {
                arguments: Box::from([novarocks_function_contract::FunctionArgument::Value {
                    value_type: bound_type.clone(),
                    constant: Some(reference),
                }]),
                logical_argument_count: 1,
                expected_result_type: None,
            },
        )
        .unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::OrderedNull,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let sealed = builder.seal(vec![], &mut work).unwrap();
    let (use_id, invocation) = sealed
        .uses()
        .iter()
        .find(|(_, u)| u.definition == call)
        .unwrap();
    let uses = invocation
        .arguments
        .iter()
        .copied()
        .map(Some)
        .collect::<Vec<_>>();
    let child = &sealed.control_flow().uses()[&invocation.arguments[0]];
    let argument_effects = ScopedExpressionEffects::pure_value(invocation.context)
        .join_control_argument(
            ScopedExpressionEffects::pure_value(child.context),
            sealed.control_flow(),
            0,
        )
        .unwrap();
    let authored = catalogue
        .prepare_fresh(
            CallEffectInput {
                context: invocation.context.into(),
                argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&uses),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Region(invocation.context.region),
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: argument_effects,
            },
            &control,
        )
        .unwrap();
    let call_use = *use_id;
    let claims = BTreeMap::from([(
        *use_id,
        ExpressionCallClaim {
            context: invocation.context,
            effects: authored.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            temporal_source: None,
        },
    )]);
    let bound_unit = sealed.finish(claims, &mut work).unwrap();
    let unit = compile_expr_set_pure(
        &bound_unit,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        novarocks_functions::ConstantValidation::bounded(policy),
        &catalogue,
        &mut work,
    )
    .unwrap();
    let PreparedPureKernel::Scalar(expected_kernel) = unit.calls()[&call_use].prepared() else {
        panic!("RNG scalar lifecycle")
    };
    let expected_kernel = expected_kernel.clone();
    let selected = expected_kernel.contract().call().selected_owner().clone();
    assert_eq!(
        expected_kernel.contract().effects().instance_state,
        FunctionInstanceState::ScalarInstance
    );
    let values = StaticValues::try_new_for_compile(
        RecordBatch::try_new(
            source_layout.schema().clone(),
            vec![Arc::new(Int64Array::from(vec![1]))],
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
    let units = vec![unit.clone()];
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
                    exprs: vec![crate::test_support::root(0, 0)],
                    expr_slot_ids: output_layout.slots().to_vec(),
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                output_layout,
            ),
        ],
        ProgramNodeId::new(1),
        &units,
        profile,
        requirements,
        Some(StaticSinkProgram::Result),
        &control,
    )
    .unwrap();
    let channels = vec![
        (
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(0),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 0,
            },
            bound_type,
        ),
        (
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(1),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 0,
            },
            result_type,
        ),
    ];
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
        graph,
        units,
        vec![],
        channels,
        operators,
        &allowed,
        CompiledProgramFacts::default(),
        &control,
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        program.unit(ProgramUnitId::new(0)).unwrap(),
        &unit
    ));
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
    let novarocks_expression::evaluator_view::UnitStateTemplate::Scalar { scope, kernel } =
        novarocks_expression::evaluator_view::state_template(
            program.unit(ProgramUnitId::new(0)).unwrap(),
            call_use,
        )
        .unwrap()
    else {
        panic!("actual scalar state template")
    };
    assert_eq!(scope.root, RootOrdinal::new(0));
    assert_eq!(scope.occurrence, call_use);
    assert!(Arc::ptr_eq(kernel, &expected_kernel));
    assert!(Arc::ptr_eq(
        kernel.contract().call().selected_owner(),
        &selected
    ));
    // Both instances borrow the same immutable recipe and evaluate their own batches.
    let mut a = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let mut b = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let bound: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let evaluated = [EvaluatedArgument::Scalar(&bound)];
    let evaluate = |instance: &mut ScalarEvaluationInstance, rows| {
        let output = instance
            .evaluate(Selection::all(rows), &evaluated, &Evaluation)
            .unwrap();
        assert!(output.errors().is_empty());
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(0); rows]);
    };
    evaluate(&mut a, 2);
    evaluate(&mut b, 1);
    evaluate(&mut a, 1);
    evaluate(&mut b, 2);
    assert_eq!(a.retained_upper_bound(), b.retained_upper_bound());
    assert!(!control.0.lock().unwrap().is_empty());
}
