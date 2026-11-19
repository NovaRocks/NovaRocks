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

use super::fragment_fixture::{compile, numeric_catalogue, options, package};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::testkit::{HarnessControl, checked_constants};
use novarocks_expression_contract::{BinaryOperator, ExprKind, RootScope};
use novarocks_functions::PureEngineFunctionCatalog;
use novarocks_local_compiler::FragmentCompileError;
use novarocks_physical_plan::{
    FragmentBuilder, FragmentId, FragmentPackage, NodeId, ResultField, ResultValueDomain, ValueDef,
    ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters,
};
use std::sync::{Arc, Mutex};

fn allow_ref() -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}

fn cast_fixture(
    source_type: FunctionValueType,
    result_type: FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (PureEngineFunctionCatalog, Arc<FragmentPackage>) {
    let mut source_pools = ConstantPools::empty();
    let constants = options().constants;

    let fid = FragmentId::new(213);
    let start = NodeId::new(u32::MAX);
    let input_node = NodeId::new(41);
    let output_node = NodeId::new(0);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let seed_type = FunctionValueType::new(DataType::Int64, true);
    let columns = [
        (ValueId::new(91), source_type.clone()),
        (ValueId::new(7), seed_type.clone()),
        (ValueId::new(333), boolean.clone()),
    ];
    let mut builder = FragmentBuilder::new(fid);
    builder
        .add_values(start, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = Vec::new();
    for (id, ty) in &columns {
        let definition = if !ty.nullable() && *ty.data_type() == DataType::Float32 {
            let wide_type = FunctionValueType::new(DataType::Float64, false);
            let exact = builder
                .unit(input_node)
                .unwrap()
                .add(
                    None,
                    wide_type.clone(),
                    checked_constants::kind(
                        &mut source_pools,
                        checked_constants::f64_bits(&wide_type, 0.0_f64.to_bits(), constants),
                    ),
                )
                .unwrap();
            builder
                .unit(input_node)
                .unwrap()
                .add(
                    None,
                    ty.clone(),
                    ExprKind::Cast {
                        expr: exact,
                        target: DataType::Float32,
                        decimal_overflow_policy: policy,
                        allow_throw_exception: allow_ref(),
                    },
                )
                .unwrap()
        } else {
            let value = if ty.nullable() {
                checked_constants::null(ty, constants)
            } else {
                match ty.data_type() {
                    DataType::Int64 => checked_constants::i64(ty, 0, constants),
                    DataType::Float64 => {
                        checked_constants::f64_bits(ty, 0.0_f64.to_bits(), constants)
                    }
                    _ => panic!("nonnullable fixture source requires an exact literal"),
                }
            };
            builder
                .unit(input_node)
                .unwrap()
                .add(
                    None,
                    ty.clone(),
                    checked_constants::kind(&mut source_pools, value),
                )
                .unwrap()
        };
        let definition = builder
            .unit(input_node)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input_node,
                    root: definition,
                },
            })
            .unwrap();
        projections.push((definition, *id));
    }
    builder
        .add_project(
            input_node,
            start,
            projections.into_boxed_slice(),
            Box::from([columns[0].0, columns[1].0, columns[2].0]),
        )
        .unwrap();
    let child = builder
        .unit(output_node)
        .unwrap()
        .add(None, source_type, ExprKind::Value(columns[0].0))
        .unwrap();
    let cast = builder
        .unit(output_node)
        .unwrap()
        .add(
            None,
            result_type.clone(),
            ExprKind::Cast {
                expr: child,
                target: result_type.data_type().clone(),
                decimal_overflow_policy: policy,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap();
    let root = builder
        .unit(output_node)
        .unwrap()
        .root(cast, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output_node,
                root,
            },
        )
        .unwrap();
    builder
        .add_project(
            output_node,
            input_node,
            Box::from([(root, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fields = Box::from([ResultField {
        domain: ResultValueDomain::Plain,
        name: "cast_result".into(),
        alias: None,
        value,
        ty: result_type,
    }]);
    let parameters = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(allow),
    )])
    .unwrap();
    (
        numeric_catalogue(),
        package(
            213,
            builder,
            output_node,
            fields,
            parameters,
            source_pools,
            constants,
        ),
    )
}

fn nullsafe_fixture(ty: FunctionValueType) -> (PureEngineFunctionCatalog, Arc<FragmentPackage>) {
    let mut source_pools = ConstantPools::empty();
    let constants = options().constants;
    let mut builder = FragmentBuilder::new(FragmentId::new(307));
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(43);
    let output = NodeId::new(0);
    let ids = [ValueId::new(903), ValueId::new(71)];
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for id in ids {
        let expr = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(&mut source_pools, checked_constants::null(&ty, constants)),
            )
            .unwrap();
        let input_root = builder
            .unit(input)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: input_root,
                },
            })
            .unwrap();
        items.push((input_root, id));
    }
    builder
        .add_project(input, source, items.into_boxed_slice(), Box::from(ids))
        .unwrap();
    let left = builder
        .unit(output)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Value(ids[0]))
        .unwrap();
    let right = builder
        .unit(output)
        .unwrap()
        .add(None, ty, ExprKind::Value(ids[1]))
        .unwrap();
    let result_type = FunctionValueType::new(DataType::Boolean, false);
    let expr = builder
        .unit(output)
        .unwrap()
        .add(
            None,
            result_type.clone(),
            ExprKind::Binary {
                op: BinaryOperator::EqForNull,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let output_root = builder
        .unit(output)
        .unwrap()
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let result = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                root: output_root,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(output_root, result)]),
            Box::from([result]),
        )
        .unwrap();
    let fields = Box::from([ResultField {
        domain: ResultValueDomain::Plain,
        name: "null_safe_result".into(),
        alias: None,
        value: result,
        ty: result_type,
    }]);
    (
        numeric_catalogue(),
        package(
            193,
            builder,
            output,
            fields,
            SemanticParameters::try_new([]).unwrap(),
            source_pools,
            constants,
        ),
    )
}

struct CompileCallbacks {
    cause: CompileControlError,
    stop_at: usize,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl CompileCallbacks {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }

    fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(Vec::new()),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for CompileCallbacks {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no compile callback after refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn date_float_actual_compiler_every_compile_control_refusal_keeps_typed_cause() {
    for target in [DataType::Float32, DataType::Float64] {
        let (functions, package) = cast_fixture(
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(target, true),
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        compile(&functions, package.clone(), &recorder).unwrap();
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileCallbacks::new(cause.clone(), at);
                assert!(
                    matches!(compile(&functions,package.clone(),&control),Err(FragmentCompileError::Control(error)) if error==cause)
                );
                assert_eq!(control.trace(), trace[..at]);
            }
        }
    }
}

#[test]
fn float_date_actual_compiler_all_three_compile_causes_preserve_real_callback_prefix() {
    for source in [DataType::Float32, DataType::Float64] {
        let (functions, package) = cast_fixture(
            FunctionValueType::new(source, false),
            FunctionValueType::new(DataType::Date32, false),
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        compile(&functions, package.clone(), &recorder).unwrap();
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileCallbacks::new(cause.clone(), at);
                assert!(
                    matches!(compile(&functions,package.clone(),&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..at]);
            }
        }
    }
}

#[test]
fn float_cast_compile_three_primary_causes_preserve_success_and_ordinary_false_allow_tail() {
    for valid in [true, false] {
        let (functions, package) = cast_fixture(
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::Int8, valid),
            DecimalOverflowPolicy::ReportError,
            false,
        );
        assert!(matches!(
            package.parameters().require(allow_ref()).unwrap(),
            SemanticParameterValue::AllowThrowException(false)
        ));
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        let result = compile(&functions, package.clone(), &recorder);
        assert_eq!(result.is_ok(), valid);
        if let Err(error) = result {
            assert!(!matches!(error, FragmentCompileError::Control(_)));
            assert!(error.to_string().contains("successful-NULL"));
        }
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusal = CompileCallbacks::new(cause.clone(), stop_at);
                assert!(
                    matches!(compile(&functions, package.clone(), &refusal), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(refusal.trace(), trace[..stop_at]);
            }
        }
    }
}

#[test]
fn observed_list_cast_actual_compiler_nonnullable_child_is_explicit_unsupported_shape_even_empty() {
    let source = DataType::List(Arc::new(Field::new("item", DataType::Null, true)));
    let target = DataType::List(Arc::new(Field::new("element", DataType::Int32, false)));
    let (functions, package) = cast_fixture(
        FunctionValueType::new(source, true),
        FunctionValueType::new(target, true),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let error = compile(&functions, package, &HarnessControl).unwrap_err();
    assert!(
        error.to_string().contains("implemented exact Physical"),
        "{error}"
    );
    assert!(!matches!(error, FragmentCompileError::Control(_)));
}

#[test]
fn cast_compile_preserves_all_original_refusals_and_observes_ordinary_narrowing_rejection_tail() {
    for valid in [true, false] {
        let (functions, package) = cast_fixture(
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int8, valid),
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert!(matches!(
            package.parameters().require(allow_ref()).unwrap(),
            SemanticParameterValue::AllowThrowException(true)
        ));
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        let result = compile(&functions, package.clone(), &recorder);
        assert_eq!(result.is_ok(), valid);
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileCallbacks::new(cause.clone(), stop_at);
                assert!(
                    matches!(compile(&functions, package.clone(), &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..stop_at]);
            }
        }
    }
}

#[test]
fn nullsafe_compilation_keeps_all_original_refusals_including_unsupported_tail() {
    // UInt32 is a valid physical snapshot domain, but not a legacy null-safe leaf.
    for supported in [true, false] {
        let ty = if supported {
            DataType::Int64
        } else {
            DataType::UInt32
        };
        let (functions, package) = nullsafe_fixture(FunctionValueType::new(ty, true));
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        let result = compile(&functions, package.clone(), &recorder);
        assert_eq!(result.is_ok(), supported);
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusal = CompileCallbacks::new(cause.clone(), stop_at);
                let failure = compile(&functions, package.clone(), &refusal).unwrap_err();
                assert!(
                    matches!(failure, novarocks_local_compiler::FragmentCompileError::Control(actual) if actual == cause)
                );
                assert_eq!(refusal.trace(), trace[..stop_at]);
            }
        }
    }
}
