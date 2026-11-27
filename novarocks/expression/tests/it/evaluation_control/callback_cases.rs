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
use arrow::{
    array::{ArrayRef, BinaryArray, Int32Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPools, ConstantValidation, ConstantValue};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, CompiledFilterConjunctionInstance,
    EvaluationEnvironment, EvaluationInput, ExprEvaluationFailure, ScopeLayouts, StaticExprKind,
    testkit::{
        CallbackControl, ClaimAuthor, CountingAllocator, HarnessControl, PassThroughCallHost,
        add_call, callback_causes, compile_fixture_unit, declare_call_controls, finish_unit,
        fixture_scope, register_call_requests, register_constant,
    },
};
use novarocks_expression_contract::{
    BinaryOperator, BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope,
    UnitLimits, ValueId,
};
use novarocks_functions::{
    AggregateStateAllocator, FunctionArgument, KernelEvaluationControl, KernelFailure,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
    testkit::installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, FunctionVolatility, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

const POLICY: ConstantValidation = novarocks_constant_contract::ConstantValidation::bounded(
    novarocks_constant_contract::ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    },
);

struct Callback(CallbackControl);
impl Callback {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self(CallbackControl::new(cause, stop))
    }

    fn trace(&self) -> Vec<u32> {
        self.0.trace()
    }
}
impl KernelEvaluationControl for Callback {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        self.0.checkpoint(units)
    }

    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.0.wait(duration)
    }
}

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("scalar and predicate callbacks never wait")
    }
}

fn compile_unit<const N: usize>(
    name: &str,
    ty: FunctionValueType,
    demand: EvaluationDemand,
    build: impl FnOnce(
        &novarocks_functions::PureEngineFunctionCatalog,
        &mut BoundExprSetBuilder,
        &mut ConstantPools,
        &mut BTreeMap<ExprId, ClaimAuthor>,
        ValueId,
    ) -> [ExprId; N],
) -> (Arc<CompiledExprSet>, [RootOrdinal; N], SchemaRef) {
    let functions = installed_builtin_owner_catalogue();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let mut constants = ConstantPools::empty();
    let mut authors = BTreeMap::new();
    let column = ValueId::new(0);
    let roots = build(&functions, &mut draft, &mut constants, &mut authors, column)
        .map(|definition| draft.root(definition, demand, RootScope::Port(0)).unwrap());
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        ty.data_type().clone(),
        ty.nullable(),
    )]));
    let port = ExprPort {
        values: Box::from([(column, ty)]),
    };
    let scope = fixture_scope(&port, schema.clone());
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    declare_call_controls(&mut draft, &functions, &constants, &parameters, &mut work);
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        &functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        &mut work,
    );
    work.finish().unwrap();
    let compiled = compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        POLICY,
        &functions,
    );
    (compiled, roots, schema)
}

fn allocation_host() -> (Arc<CountingAllocator>, Arc<dyn AggregateStateAllocator>) {
    let allocator = Arc::new(CountingAllocator::new(8 * 1024 * 1024));
    (allocator.clone(), allocator)
}

#[test]
fn md5_actual_frame_seven_causes_exact_prefix_and_failed_group_latch() {
    let ty = FunctionValueType::new(DataType::Binary, true);
    let (unit, [root], schema) = compile_unit(
        "v",
        ty.clone(),
        EvaluationDemand::Value,
        |functions, draft, _, authors, column| {
            let input = draft
                .add(None, ty.clone(), ExprKind::input(column))
                .unwrap();
            let author = ClaimAuthor::with_exact_ordinary_binding(
                functions,
                "md5",
                vec![FunctionArgument::Value {
                    value_type: ty,
                    constant: None,
                }],
                POLICY,
                &HarnessControl,
            )
            .with_decimal_policy(DecimalOverflowPolicy::OutputNull);
            assert_eq!(
                author.result(),
                FunctionValueType::new(DataType::Binary, true)
            );
            [add_call(draft, authors, author, vec![input])]
        },
    );
    let compiled_root = unit.root(root).unwrap();
    let invocation = &unit.control_flow().uses()[&compiled_root.use_id];
    assert_eq!(invocation.control, ControlShape::OrderedNull);
    let StaticExprKind::BoundCall { args, .. } = unit
        .definitions()
        .node(compiled_root.definition)
        .unwrap()
        .kind()
    else {
        panic!("actual MD5 root");
    };
    assert_eq!(args.len(), 1);
    assert_eq!(invocation.arguments.len(), 1);
    assert_eq!(
        unit.control_flow().uses()[&invocation.arguments[0]].definition,
        args[0],
    );
    let contract = unit.calls()[&compiled_root.use_id].call_contract();
    assert_eq!(contract.function_id().as_str(), "builtin.scalar/md5/v1");
    assert_eq!(contract.selected().argument_types.len(), 1);
    assert_eq!(contract.logical_argument_count(), 1);
    assert_eq!(
        contract.effects().value_stability,
        FunctionVolatility::Immutable
    );
    let bytes = (0..321)
        .map(|i| match i {
            0 => Some(vec![0xff, 0]),
            _ if i % 3 == 1 => None,
            _ => Some((i + 17).to_string().into_bytes()),
        })
        .collect::<Vec<_>>();
    let input = BinaryArray::from_iter(bytes.iter().map(|value| value.as_deref()));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap();
    let control = Callback::new(usize::MAX, KernelFailure::Cancelled);
    let (tracker, host) = allocation_host();
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        unit.clone(),
        root,
        &Control,
        Some(host),
    )
    .unwrap();
    let out = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
            ScalarInvocationActivation::Activated,
            &control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let digests = out.values().as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(
        digests.value(0),
        &[
            0xe0, 0xe8, 0xbf, 0xaf, 0xbb, 0x06, 0x89, 0x56, 0x3b, 0x2f, 0xba, 0x78, 0x9c, 0x97,
            0xb3, 0xcc
        ],
    );
    assert_eq!(digests.iter().nth(1), Some(None));
    assert!(out.errors().is_empty());
    drop(out);
    drop(frame);
    assert_eq!(tracker.outstanding(), 0);
    let trace = control.trace();
    assert!(trace.contains(&256));
    for cause in callback_causes(["original MD5 source"; 3]) {
        for stop in [0, trace.len() / 2, trace.len() - 1] {
            let control = Callback::new(stop + 1, cause.clone());
            let (tracker, host) = allocation_host();
            let mut frame = CompiledExpressionInstance::try_new_with_allocator(
                unit.clone(),
                root,
                &Control,
                Some(host),
            )
            .unwrap();
            assert_eq!(
                frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
                        ScalarInvocationActivation::Activated,
                        &control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err(),
                ExprEvaluationFailure::Kernel(cause.clone())
            );
            assert_eq!(control.trace(), trace[..=stop]);
            let count = control.trace().len();
            assert_eq!(
                frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
                        ScalarInvocationActivation::Activated,
                        &control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err(),
                ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
            );
            assert_eq!(control.trace().len(), count);
            drop(frame);
            assert_eq!(tracker.outstanding(), 0);
        }
    }
}

#[test]
fn scan_ordered_required_all_seven_actual_callback_causes_stop_and_latch() {
    let column_type = FunctionValueType::new(DataType::Int32, true);
    let (unit, roots, schema) = compile_unit(
        "k",
        column_type.clone(),
        EvaluationDemand::TruthOnly,
        |_, draft, constants, _, column| {
            let nonnull = FunctionValueType::new(DataType::Boolean, false);
            let boolean = FunctionValueType::new(DataType::Boolean, true);
            let literal_type = FunctionValueType::new(DataType::Int32, false);
            let input = draft
                .add(None, column_type.clone(), ExprKind::input(column))
                .unwrap();
            let not_null = draft
                .add(
                    None,
                    nonnull,
                    ExprKind::IsNull {
                        expr: input,
                        negated: true,
                    },
                )
                .unwrap();
            let mut comparison = |op, value| {
                let left = draft
                    .add(None, column_type.clone(), ExprKind::input(column))
                    .unwrap();
                let literal = ConstantValue::from_i32(
                    Arc::new(literal_type.try_to_field("constant").unwrap()),
                    literal_type.clone(),
                    value,
                    POLICY,
                    CompilePhase::Validate,
                    &HarnessControl,
                )
                .unwrap();
                let reference = register_constant(constants, &literal);
                let right = draft
                    .add(None, literal_type.clone(), ExprKind::Constant(reference))
                    .unwrap();
                draft
                    .add(
                        None,
                        boolean.clone(),
                        ExprKind::Binary {
                            left,
                            op,
                            right,
                            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                            allow_throw_exception: None,
                        },
                    )
                    .unwrap()
            };
            // The original scan emits three roots in this order.
            [
                not_null,
                comparison(BinaryOperator::GtEq, 10),
                comparison(BinaryOperator::Lt, 20),
            ]
        },
    );
    let predicates = roots.map(|root| (unit.clone(), root));
    let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
    CompiledFilterConjunctionInstance::try_new_with_allocator(&predicates, &record, None).unwrap();
    let trace = record.trace();
    for stop in 1..=trace.len() {
        for cause in callback_causes([
            "scan-original-invalid",
            "scan-original-internal",
            "scan-original-operational",
        ]) {
            let c = Callback::new(stop, cause.clone());
            assert!(
                matches!(CompiledFilterConjunctionInstance::try_new_with_allocator(&predicates, &c, None,),Err(actual) if actual==cause)
            );
            assert_eq!(c.trace(), trace[..stop]);
        }
    }
    let values = Int32Array::from(
        (0..513)
            .map(|row| if row % 7 == 0 { None } else { Some(row % 25) })
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values) as ArrayRef]).unwrap();
    for rows in [
        (0..513).collect::<Vec<_>>(),
        (0..513).step_by(3).collect(),
        Vec::new(),
    ] {
        let selection = Selection::try_sparse(513, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        let mut instance =
            CompiledFilterConjunctionInstance::try_new_with_allocator(&predicates, &Control, None)
                .unwrap();
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                selection,
                &record,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = record.trace();
        if rows.len() == 513 {
            assert!(
                trace.contains(&256),
                "actual dense path reaches its work quantum"
            );
        }
        for stop in 1..=trace.len() {
            for cause in callback_causes([
                "scan-original-invalid",
                "scan-original-internal",
                "scan-original-operational",
            ]) {
                let c = Callback::new(stop, cause.clone());
                let mut instance = CompiledFilterConjunctionInstance::try_new_with_allocator(
                    &predicates,
                    &Control,
                    None,
                )
                .unwrap();
                let failure = instance
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        selection,
                        &c,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err();
                assert_eq!(failure, ExprEvaluationFailure::Kernel(cause));
                assert_eq!(c.trace(), trace[..stop]);
                let again = instance
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        selection,
                        &c,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err();
                assert_eq!(
                    again,
                    ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
                );
                assert_eq!(c.trace(), trace[..stop]);
            }
        }
    }
}
