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
use novarocks_functions::{ConstantPool, EngineFunctionCatalogBuilder, InstalledPureKernel};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning, NodeId,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueOrigin,
};
use novarocks_type_contract::{
    EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, FunctionValueType, SemanticParameters,
};
use std::sync::Mutex;

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
        match self.refusal {
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
        ty.try_to_field("authored-source")
            .unwrap()
            .with_metadata(HashMap::from([(
                "provider.unknown".into(),
                "keep-root-facts".into(),
            )])),
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

fn checked_package(pool: &ConstantPool, ordinals: &[u32]) -> FragmentPackage {
    let mut constants = ConstantPools::empty();
    constants
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(91));
    builder
        .add_values(NodeId::new(99), Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut assignments = Vec::new();
    let mut output = Vec::new();
    let mut fields = Vec::new();
    for &ordinal in ordinals {
        let ty = pool.value_type().clone();
        let expr = builder
            .add_expression(
                NodeId::new(8),
                ty.clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(u32::MAX),
                    ordinal,
                }),
            )
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node: NodeId::new(8),
                    expr,
                },
            )
            .unwrap();
        assignments.push((expr, value));
        output.push(value);
        fields.push(ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: format!("result-{ordinal}").into(),
            alias: None,
            value,
            ty,
        });
    }
    builder
        .add_project(
            NodeId::new(8),
            NodeId::new(99),
            assignments.into_boxed_slice(),
            output.into_boxed_slice(),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            NodeId::new(8),
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control::good()).unwrap();
    let domain = EvaluationDomainId::new(17);
    let mut invocations = vec![];
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (&site, root))| {
            let use_id = ExpressionUseId::new(ordinal as u32 + 42);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, bindings, &Control::good()).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &Control::good())
        .unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control::good()).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: fields.into_boxed_slice(),
    };
    FragmentPackage::try_new(
        FragmentPackageInput {
            version: PlanVersionId::try_new([91; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            expression_uses,
            calls,
            constants,
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
            pruning,
        },
        package_admission(),
        &Control::good(),
    )
    .unwrap()
}
fn lower(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, ExpressionLoweringError> {
    lower_expressions(package, policy(), &BTreeMap::new(), control)
}
fn replace_constant(lowered: &mut LoweredExpressions, id: ProgramExprId, value: ConstantValue) {
    let mut nodes = lowered.arena.nodes().to_vec();
    nodes[id.index()] = StaticExprNode::new(
        StaticExprKind::Constant(value.clone()),
        value.value_type().data_type.clone(),
        None,
    );
    lowered.arena = Arc::new(
        ImmutableExpressions::try_new_for_compile(
            nodes,
            false,
            HashMap::new(),
            None,
            &Control::good(),
        )
        .unwrap(),
    );
}

#[test]
fn checked_references_keep_multirow_backing_nonzero_ordinals_and_constant_arguments() {
    let original = integer_pool();
    let package = checked_package(&original, &[1, 2, 1]);
    let lowered = lower(&package, &Control::good()).unwrap();
    for (&id, source) in package.fragment().expressions().iter() {
        let ExprKind::Constant(reference) = source.kind else {
            panic!("reference source")
        };
        let local = lowered.arena.node(lowered.ids[&id]).unwrap();
        let StaticExprKind::Constant(value) = local.kind() else {
            panic!("checked constant")
        };
        assert_eq!(value.ordinal(), reference.ordinal);
        assert_eq!(value.pool().backing_identity(), original.backing_identity());
        assert!(Arc::ptr_eq(value.pool().field_ref(), original.field_ref()));
        assert_eq!(value.value_type(), &source.ty);
        let argument = literal_argument(source, local, &Control::good())
            .unwrap()
            .unwrap();
        assert_eq!(
            argument.pool().backing_identity(),
            original.backing_identity()
        );
        assert_eq!(argument.ordinal(), reference.ordinal);
        assert_eq!(
            argument.try_i64().unwrap(),
            Some(if reference.ordinal == 1 { 42 } else { -7 })
        );
    }
    assert!(
        prepare_calls(&package, &lowered, &functions(), &Control::good())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn checked_reference_preparation_rejects_swapped_equal_backing_or_ordinal() {
    let original = integer_pool();
    let package = checked_package(&original, &[1]);
    let functions = functions();
    for replacement in [integer_pool().value(1).unwrap(), original.value(0).unwrap()] {
        let mut lowered = lower(&package, &Control::good()).unwrap();
        let id = *lowered.ids.values().next().unwrap();
        replace_constant(&mut lowered, id, replacement);
        assert!(matches!(
            prepare_calls(&package, &lowered, &functions, &Control::good()),
            Err(ExpressionLoweringError::Reference(
                ConstantReferenceError::InvalidConsumer(_)
            ))
        ));
    }
}

#[test]
fn checked_reference_preparation_rejects_payload_or_projection_full_type_mismatch() {
    let original = integer_pool();
    let package = checked_package(&original, &[1]);
    let functions = functions();
    let mut lowered = lower(&package, &Control::good()).unwrap();
    let id = *lowered.ids.values().next().unwrap();
    let ty = FunctionValueType::new(DataType::Int64, true);
    let wrong = ConstantValue::from_i64(
        Arc::new(ty.try_to_field("wrong").unwrap()),
        ty.clone(),
        42,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    replace_constant(&mut lowered, id, wrong);
    assert!(matches!(
        prepare_calls(&package, &lowered, &functions, &Control::good()),
        Err(ExpressionLoweringError::Reference(
            ConstantReferenceError::InvalidConsumer(_)
        ))
    ));
    lowered.types[id.index()] = FunctionArgumentType::Value(ty);
    assert!(matches!(
        prepare_calls(&package, &lowered, &functions, &Control::good()),
        Err(ExpressionLoweringError::Invalid(
            "lowered full type differs from source"
        ))
    ));
}

#[test]
fn checked_reference_loops_keep_each_original_control_prefix_and_ordinary_tail() {
    // Physical fields have a 256-entry metadata cap. Wide actual fields,
    // each with one metadata association, exercise a legal long traversal.
    let array = StructArray::from(
        (0..320)
            .map(|i| {
                let child = Arc::new(
                    Field::new(format!("child-{i:04}"), DataType::Int64, false).with_metadata(
                        HashMap::from([(format!("provider.key.{i:04}"), format!("value-{i:04}"))]),
                    ),
                );
                (
                    child,
                    Arc::new(Int64Array::from(vec![9, 42, -7])) as ArrayRef,
                )
            })
            .collect::<Vec<_>>(),
    );
    let original = pool(Arc::new(array));
    let package = checked_package(&original, &[1, 2]);
    let baseline = Control::good();
    let lowered = lower(&package, &baseline).unwrap();
    let lowering_trace = baseline.trace();
    assert!(lowering_trace.iter().any(|(_, units)| *units == 256));
    let baseline = Control::good();
    let functions = functions();
    assert!(
        prepare_calls(&package, &lowered, &functions, &baseline)
            .unwrap()
            .is_empty()
    );
    let preparing_trace = baseline.trace();
    assert!(preparing_trace.iter().any(|(_, units)| *units == 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=lowering_trace.len() {
            let control = Control::refusing(at, cause);
            assert!(
                matches!(lower(&package, &control), Err(ExpressionLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), lowering_trace[..at]);
        }
        for at in 1..=preparing_trace.len() {
            let control = Control::refusing(at, cause);
            assert!(
                matches!(prepare_calls(&package, &lowered, &functions, &control), Err(ExpressionLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), preparing_trace[..at]);
        }
    }
    let small = integer_pool();
    let small_package = checked_package(&small, &[1]);
    let mut wrong = lower(&small_package, &Control::good()).unwrap();
    let id = *wrong.ids.values().next().unwrap();
    replace_constant(&mut wrong, id, small.value(0).unwrap());
    let baseline = Control::good();
    assert!(matches!(
        prepare_calls(&small_package, &wrong, &functions, &baseline),
        Err(ExpressionLoweringError::Reference(_))
    ));
    let ordinary_trace = baseline.trace();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=ordinary_trace.len() {
            let control = Control::refusing(at, cause);
            assert!(
                matches!(prepare_calls(&small_package, &wrong, &functions, &control), Err(ExpressionLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), ordinary_trace[..at]);
        }
    }
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
