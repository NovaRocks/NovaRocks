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

//! Exact private source/lowered correspondence at its single expression compiler owner.
use super::*;
use arrow::array::{Array, ArrayRef, Int64Array, StructArray};
use arrow::datatypes::{DataType, Field};
use novarocks_constant_contract::{ConstantPool, ConstantPoolId, ConstantPools, ConstantReference};
use novarocks_expression_contract::{BoundExprSetBuilder, RootScope, UnitLimits};
use novarocks_functions::{EngineFunctionCatalogBuilder, InstalledPureKernel};
use novarocks_type_contract::{EvaluationDemand, FunctionValueType, SemanticParameters};
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
fn policy() -> ConstantValidation {
    novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
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
        },
    )
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
            [
                "()->f64;strict;legacy",
                "(i8)->i8;strict;coerce",
                "(i16)->i16;strict;coerce",
                "(i32)->i32;strict;coerce",
                "(i64)->i64;strict;coerce",
            ]
            .into_iter()
            .map(|profile| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!("builtin.scalar/rand/{profile}"))
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

fn checked_package(pool: &ConstantPool, ordinals: &[u32]) -> (BoundExprSet, ConstantPools) {
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    for &ordinal in ordinals {
        let definition = builder
            .add(
                None,
                pool.value_type().clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(u32::MAX),
                    ordinal,
                }),
            )
            .unwrap();
        builder
            .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = builder
        .seal(vec![], &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    (unit, pools)
}
fn lower(
    package: &(BoundExprSet, ConstantPools),
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, ExprCompileError> {
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core::<std::convert::Infallible, crate::creation::NoFactory>(
        &package.0,
        crate::EvaluationEnvironment {
            constants: &package.1,
            parameters: &parameters,
        },
        control,
        &mut work,
        None,
        None,
    )
    .map_err(ExprCompileFailure::into_pure);
    finish(result, &mut work)
}
fn prepare_calls(
    package: &(BoundExprSet, ConstantPools),
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<Vec<(ExpressionUseId, PureCallSpecialization)>, ExprCompileError> {
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = prepare_core::<std::convert::Infallible, crate::creation::NoFactory>(
        &package.0,
        crate::EvaluationEnvironment {
            constants: &package.1,
            parameters: &parameters,
        },
        lowered,
        functions,
        control,
        &mut work,
        None,
        None,
        None,
    )
    .map(|(calls, _, _)| calls)
    .map_err(ExprCompileFailure::into_pure);
    finish(result, &mut work)
}
fn replace_constant(lowered: &mut LoweredExpressions, id: CompiledExprId, value: ConstantValue) {
    let mut nodes = lowered.arena.nodes().to_vec();
    nodes[id.index()] = StaticExprNode::new_value_typed(
        StaticExprKind::Constant(value.clone()),
        value.value_type().clone(),
    );
    lowered.arena =
        Arc::new(ImmutableExpressions::try_new_for_compile(nodes, &Control::good()).unwrap());
}

#[test]
fn checked_references_keep_multirow_backing_nonzero_ordinals_and_constant_arguments() {
    let original = integer_pool();
    let package = checked_package(&original, &[1, 2, 1]);
    let lowered = lower(&package, &Control::good()).unwrap();
    for source in package.0.definitions() {
        let id = source.id;
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
            Err(ExprCompileError::Reference(
                ConstantReferenceError::Constant(ConstantError::Invalid(
                    "lowered constant reference differs from its checked source"
                ))
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
        Err(ExprCompileError::Reference(
            ConstantReferenceError::Constant(ConstantError::Invalid(
                "lowered constant reference differs from its checked source"
            ))
        ))
    ));
    // This pure negative fixture authors another immutable collection. The
    // published checked slice remains read-only for every real consumer.
    let mut declarations = lowered.types.to_vec();
    declarations[id.index()] = FunctionArgumentType::Value(ty);
    lowered.types = novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
        declarations.into_boxed_slice(),
    );
    assert!(matches!(
        prepare_calls(&package, &lowered, &functions, &Control::good()),
        Err(ExprCompileError::Invalid(
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
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(lower(&package, &control), Err(ExprCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), lowering_trace[..at]);
        }
        for at in 1..=preparing_trace.len() {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(prepare_calls(&package, &lowered, &functions, &control), Err(ExprCompileError::Control(actual)) if actual == cause)
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
        Err(ExprCompileError::Reference(_))
    ));
    let ordinary_trace = baseline.trace();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=ordinary_trace.len() {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(prepare_calls(&small_package, &wrong, &functions, &control), Err(ExprCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), ordinary_trace[..at]);
        }
    }
}
