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

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionArgumentType,
    FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionDefinition, FunctionFailureBehavior,
    FunctionId, FunctionOverloadDeclaration, FunctionOverloadId, FunctionResolutionError,
    FunctionResultType, FunctionVisibility, FunctionVolatility, ResolvedAggregateSignature,
    ResolvedFunctionBinding, ResolvedFunctionSignature,
};
use novarocks_parser::Span;
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionIntrinsicRowError, FunctionKind,
    FunctionNullBehavior, FunctionValueType, ObservableEffects, PureCompileControl,
    ValueLogicalType,
};

use crate::analysis::{ExprKind, LambdaParam, TypedExpr};
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::compiler::SqlFunctionCatalog;

const SPAN: Span = Span::new(11, 47);
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Trace {
    fn recorded(&self) -> Vec<(CompilePhase, u32)> {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push((phase, units));
        if let Some((index, cause)) = self.stop.clone()
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn source(ty: FunctionValueType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: crate::column_id::ColumnId::new_for_test(73),
            qualifier: Some("original".into()),
            column: "value".into(),
        },
        value_type: ty,
    }
}
fn lambda() -> TypedExpr {
    let ty = FunctionValueType::new(DataType::Int64, false);
    TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![LambdaParam {
                name: "x".into(),
                slot_id: 19,
                value_type: ty.clone(),
            }],
            body: Box::new(TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: "x".into(),
                    slot_id: 19,
                },
                value_type: ty,
            }),
        },
        // This is a callable AST, not an invented ordinary value argument.
        value_type: FunctionValueType::new(DataType::Null, true),
    }
}

#[derive(Clone, Copy)]
enum Behavior {
    Echo,
    Widen,
    Drift,
}
struct Resolver(Behavior);
impl Resolver {
    fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            if request.arguments.len() != 1 || request.logical_argument_count != 1 {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let argument = match &request.arguments[0] {
                FunctionArgument::Value { value_type, .. } => {
                    let mut target = value_type.clone();
                    if !matches!(self.0, Behavior::Echo) {
                        target = novarocks_type_contract::FunctionValueType::try_with_logical_type(
                            match value_type.data_type() {
                                DataType::Int32 => DataType::Int64,
                                DataType::Int64 if matches!(self.0, Behavior::Drift) => {
                                    DataType::Int16
                                }
                                DataType::Int64 => DataType::Int64,
                                _ => return Err(FunctionBindingError::NoMatchingOverload),
                            },
                            target.nullable(),
                            target.logical_type(),
                        )
                        .unwrap();
                    }
                    FunctionArgumentType::Value(target)
                }
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => FunctionArgumentType::Lambda {
                    parameter_types: parameter_types.clone(),
                    result_type: result_type.clone(),
                },
            };
            work.step()?;
            // The result really belongs to this resolver. Coercion may change
            // it without changing the stable identity or overload.
            let result = match &request.arguments[0] {
                FunctionArgument::Value { value_type, .. }
                    if *value_type.data_type() == DataType::Int32 =>
                {
                    FunctionValueType::new(DataType::Int32, true)
                }
                _ => FunctionValueType::new(DataType::Int64, false),
            };
            Ok(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("test.scalar/rebind/0/v1").unwrap(),
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![argument].into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(result),
                aggregate: None,
            })
        })();
        if matches!(result, Err(FunctionBindingError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}
impl FunctionBindingResolver for Resolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.selection(request, control)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if selected == &self.selection(request, control)? {
            Ok(())
        } else {
            Err(FunctionBindingError::NoMatchingOverload)
        }
    }
}
fn catalog(behavior: Behavior) -> Arc<dyn SqlFunctionCatalog> {
    let declaration = FunctionBindingDeclaration::try_new_complete(
        FunctionId::try_new("test.scalar/rebind/v1").unwrap(),
        FunctionKind::Scalar,
        [FunctionOverloadDeclaration::from_effects(
            FunctionOverloadId::try_new("test.scalar/rebind/0/v1").unwrap(),
            "one selected value or callable",
            "resolver-owned scalar result",
            None,
            FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::Strict,
                argument_control: ArgumentControl::Eager,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
        )],
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_bound(
                "rebind_probe",
                FunctionVisibility::Public,
                declaration,
                Arc::new(Resolver(behavior)),
            )
            .unwrap(),
        )
        .unwrap();
    Arc::new(builder.seal_bound().unwrap())
}

#[derive(Clone, Copy, Debug)]
enum Path {
    Ordinary,
    Resolved,
}
fn bind(
    path: Path,
    catalog: &dyn SqlFunctionCatalog,
    args: Vec<TypedExpr>,
    policy: DecimalOverflowPolicy,
    control: &dyn PureCompileControl,
) -> Result<
    (
        Vec<TypedExpr>,
        crate::binding::SqlFunctionBinding,
        FunctionValueType,
    ),
    AnalyzeError,
> {
    match path {
        Path::Ordinary => {
            let bound = super::resolve_expr::bind_scalar_function_call_with_catalog(
                catalog,
                "rebind_probe",
                args,
                policy,
                crate::constant::test_constant_policy(),
                control,
            )?;
            let result = bound.value_type().clone();
            Ok((bound.args, bound.binding, result))
        }
        Path::Resolved => {
            let expr = super::resolve_expr::resolved_scalar_call_at(
                catalog,
                "rebind_probe",
                args,
                SPAN,
                policy,
                crate::constant::test_constant_policy(),
                control,
            )?;
            let ExprKind::FunctionCall { args, binding, .. } = expr.kind else {
                panic!("bound call");
            };
            Ok((args, binding, expr.value_type))
        }
    }
}
fn assert_ordinary(path: Path, error: &AnalyzeError) {
    assert_eq!(error.control_error(), None);
    match path {
        Path::Ordinary => {
            assert_eq!(error.kind(), AnalyzeErrorKind::Internal);
            assert_eq!(error.span(), None);
        }
        Path::Resolved => {
            assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
            assert_eq!(error.span(), Some(SPAN));
        }
    }
}

#[test]
fn scalar_rebind_immutable_engine_resolver_cannot_publish_non_fixed_point_arguments() {
    let catalog = catalog(Behavior::Drift);
    let ty = FunctionValueType::new(DataType::Int32, false);
    // Both selections pass the actual declaring engine's resolution checks.
    let first = catalog
        .resolve_scalar_binding(
            "rebind_probe",
            &[FunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            }],
            &Trace::default(),
        )
        .unwrap();
    let second = catalog
        .resolve_scalar_binding(
            "rebind_probe",
            &[FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int64, false),
                constant: None,
            }],
            &Trace::default(),
        )
        .unwrap();
    assert_eq!(first.function_id, second.function_id);
    assert_eq!(first.selected.overload, second.selected.overload);
    assert_eq!(
        first.selected.argument_types[0],
        FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, false))
    );
    assert_eq!(
        second.selected.argument_types[0],
        FunctionArgumentType::Value(FunctionValueType::new(DataType::Int16, false))
    );
    for path in [Path::Ordinary, Path::Resolved] {
        let error = bind(
            path,
            catalog.as_ref(),
            vec![source(ty.clone())],
            DecimalOverflowPolicy::ReportError,
            &Trace::default(),
        )
        .unwrap_err();
        assert_ordinary(path, &error);
    }
}

#[test]
fn scalar_rebind_lawful_canonical_source_keeps_policy_and_catalog_owned_changed_result() {
    let catalog = catalog(Behavior::Widen);
    let original = source(FunctionValueType::new(DataType::Int32, false));
    for path in [Path::Ordinary, Path::Resolved] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let (args, binding, result) = bind(
                path,
                catalog.as_ref(),
                vec![original.clone()],
                policy,
                &Trace::default(),
            )
            .unwrap();
            assert_eq!(args.len(), 1);
            assert_eq!(
                args[0].value_type,
                FunctionValueType::new(DataType::Int64, false)
            );
            let ExprKind::Cast {
                expr,
                target,
                decimal_overflow_policy,
            } = &args[0].kind
            else {
                panic!("actual widening cast");
            };
            assert_eq!(*target, DataType::Int64);
            assert_eq!(*decimal_overflow_policy, policy);
            assert_eq!(expr.value_type, original.value_type);
            assert!(
                matches!(expr.kind, ExprKind::ColumnRef { column_id, .. } if column_id == crate::column_id::ColumnId::new_for_test(73))
            );
            assert_eq!(result, FunctionValueType::new(DataType::Int64, false));
            assert_eq!(
                binding.selected.result_type,
                FunctionResultType::Scalar(result)
            );
            assert_eq!(binding.decimal_overflow_policy(), policy);
        }
    }
}

#[derive(Clone, Debug)]
enum Fault {
    Short,
    Long,
    Kind,
    Relation,
    Shape,
    Type,
    Nullable,
    Logical,
    Zone,
    NestedName,
    LambdaParameter,
    LambdaResult,
    Control(CompileControlError),
}
#[derive(Clone, Debug)]
struct FaultCatalog {
    base: Arc<dyn SqlFunctionCatalog>,
    at: usize,
    fault: Fault,
    calls: Arc<AtomicUsize>,
}
impl FaultCatalog {
    fn new(base: Arc<dyn SqlFunctionCatalog>, at: usize, fault: Fault) -> Self {
        Self {
            base,
            at,
            fault,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl SqlFunctionCatalog for FaultCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        self.base.resolve_scalar_signature(name, args, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let at = self.calls.fetch_add(1, Ordering::SeqCst);
        if at == self.at
            && let Fault::Control(cause) = self.fault.clone()
        {
            return Err(cause.into());
        }
        let mut bound = self.base.resolve_scalar_binding(name, args, control)?;
        if at != self.at {
            return Ok(bound);
        }
        let mut arguments = bound.selected.argument_types.to_vec();
        match self.fault {
            Fault::Short => arguments.clear(),
            Fault::Long => arguments = vec![arguments[0].clone(); 2],
            Fault::Kind => bound.kind = FunctionKind::Window,
            Fault::Relation => {
                bound.selected.result_type = FunctionResultType::Relation(
                    novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        vec![FunctionValueType::new(DataType::Int64, false)].into_boxed_slice(),
                    ),
                )
            }
            Fault::Shape => {
                arguments[0] = FunctionArgumentType::Lambda {
                    parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::new([]),
                    ),
                    result_type: FunctionValueType::new(DataType::Int64, false),
                }
            }
            Fault::Type => {
                arguments[0] =
                    FunctionArgumentType::Value(FunctionValueType::new(DataType::Int16, false))
            }
            Fault::Nullable => {
                let FunctionArgumentType::Value(ty) = &mut arguments[0] else {
                    panic!("value");
                };
                *ty = ty.with_nullable(!ty.nullable()).unwrap();
            }
            Fault::Logical => {
                arguments[0] = FunctionArgumentType::Value(
                    FunctionValueType::try_with_logical_type(
                        DataType::Utf8,
                        true,
                        ValueLogicalType::Json,
                    )
                    .unwrap(),
                )
            }
            Fault::Zone => {
                arguments[0] = FunctionArgumentType::Value(FunctionValueType::new(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ))
            }
            Fault::NestedName => {
                let FunctionArgumentType::Value(ty) = &mut arguments[0] else {
                    panic!("value");
                };
                let DataType::Struct(fields) = &ty.data_type() else {
                    panic!("struct");
                };
                let mut field = fields[0].as_ref().clone();
                field = Field::new(
                    "different_actual",
                    field.data_type().clone(),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone());
                *ty = novarocks_type_contract::FunctionValueType::try_with_logical_type(
                    DataType::Struct(vec![Arc::new(field)].into()),
                    ty.nullable(),
                    ty.logical_type(),
                )
                .unwrap();
            }
            Fault::LambdaParameter | Fault::LambdaResult => {
                let FunctionArgumentType::Lambda {
                    parameter_types,
                    result_type,
                } = &mut arguments[0]
                else {
                    panic!("lambda");
                };
                if matches!(self.fault, Fault::LambdaParameter) {
                    let mut parameters = parameter_types.to_vec();
                    parameters[0] = FunctionValueType::new(DataType::Int32, false);
                    *parameter_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        parameters.into_boxed_slice(),
                    );
                } else {
                    *result_type = FunctionValueType::new(DataType::Int64, true);
                }
            }
            Fault::Control(_) => unreachable!(),
        }
        bound.selected.argument_types =
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(arguments.into_boxed_slice());
        Ok(bound)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.base.contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.base.resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.base.resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.base.volatility(name)
    }
}

#[test]
fn scalar_rebind_catalog_shapes_never_truncate_arguments_or_publish_non_scalar_results() {
    for path in [Path::Ordinary, Path::Resolved] {
        for at in [0, 1] {
            for fault in [
                Fault::Short,
                Fault::Long,
                Fault::Kind,
                Fault::Relation,
                Fault::Shape,
            ] {
                let catalog = FaultCatalog::new(catalog(Behavior::Echo), at, fault.clone());
                let error = bind(
                    path,
                    &catalog,
                    vec![source(FunctionValueType::new(DataType::Int64, false))],
                    DecimalOverflowPolicy::OutputNull,
                    &Trace::default(),
                )
                .unwrap_err();
                assert_ordinary(path, &error);
                assert_eq!(
                    catalog.calls.load(Ordering::SeqCst),
                    at + 1,
                    "{path:?} {fault:?}"
                );
            }
        }
    }
}

#[test]
fn scalar_rebind_full_value_and_callable_targets_match_actual_coerced_ast() {
    let decorated = DataType::Struct(
        vec![Arc::new(
            Field::new("actual", DataType::Utf8, false).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                String::from("provider"),
                String::from("original"),
            )])),
        )]
        .into(),
    );
    let cases = [
        (Fault::Type, FunctionValueType::new(DataType::Int64, false)),
        (
            Fault::Nullable,
            FunctionValueType::new(DataType::Int64, false),
        ),
        (Fault::Logical, FunctionValueType::new(DataType::Utf8, true)),
        (
            Fault::Zone,
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ),
        (Fault::NestedName, FunctionValueType::new(decorated, true)),
    ];
    for path in [Path::Ordinary, Path::Resolved] {
        for (fault, ty) in &cases {
            let catalog = FaultCatalog::new(catalog(Behavior::Echo), 1, fault.clone());
            let error = bind(
                path,
                &catalog,
                vec![source(ty.clone())],
                DecimalOverflowPolicy::ReportError,
                &Trace::default(),
            )
            .unwrap_err();
            assert_ordinary(path, &error);
            assert_eq!(catalog.calls.load(Ordering::SeqCst), 2);
        }
        let (args, binding, result) = bind(
            path,
            catalog(Behavior::Echo).as_ref(),
            vec![lambda()],
            DecimalOverflowPolicy::ReportError,
            &Trace::default(),
        )
        .unwrap();
        assert!(matches!(args[0].kind, ExprKind::LambdaFunction { .. }));
        assert_eq!(
            binding.selected.argument_types[0],
            FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![FunctionValueType::new(DataType::Int64, false)].into_boxed_slice()
                ),
                result_type: FunctionValueType::new(DataType::Int64, false),
            }
        );
        assert_eq!(result, FunctionValueType::new(DataType::Int64, false));
        for fault in [Fault::LambdaParameter, Fault::LambdaResult] {
            let catalog = FaultCatalog::new(catalog(Behavior::Echo), 1, fault);
            let error = bind(
                path,
                &catalog,
                vec![lambda()],
                DecimalOverflowPolicy::ReportError,
                &Trace::default(),
            )
            .unwrap_err();
            assert_ordinary(path, &error);
        }
    }
}

#[test]
fn scalar_rebind_catalog_first_and_second_control_causes_never_retry() {
    for path in [Path::Ordinary, Path::Resolved] {
        for at in [0, 1] {
            for cause in CAUSES {
                let catalog =
                    FaultCatalog::new(catalog(Behavior::Echo), at, Fault::Control(cause.clone()));
                let error = bind(
                    path,
                    &catalog,
                    vec![source(FunctionValueType::new(DataType::Int64, false))],
                    DecimalOverflowPolicy::OutputNull,
                    &Trace::default(),
                )
                .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(catalog.calls.load(Ordering::SeqCst), at + 1);
            }
        }
    }
}

#[test]
fn scalar_rebind_actual_success_and_ordinary_tail_keep_every_control_prefix() {
    for path in [Path::Ordinary, Path::Resolved] {
        for behavior in [Behavior::Widen, Behavior::Drift] {
            let catalog = catalog(behavior);
            let args = vec![source(FunctionValueType::new(DataType::Int32, false))];
            let trace = Trace::default();
            let result = bind(
                path,
                catalog.as_ref(),
                args.clone(),
                DecimalOverflowPolicy::ReportError,
                &trace,
            );
            assert_eq!(result.is_ok(), matches!(behavior, Behavior::Widen));
            if let Err(error) = &result {
                assert_ordinary(path, error);
            }
            let recorded = trace.recorded();
            assert!(!recorded.is_empty());
            assert_eq!(
                recorded.last().unwrap().0,
                CompilePhase::FunctionSpecialization
            );
            for at in 0..recorded.len() {
                for cause in CAUSES {
                    let control = Trace {
                        stop: Some((at, cause.clone())),
                        ..Trace::default()
                    };
                    let error = bind(
                        path,
                        catalog.as_ref(),
                        args.clone(),
                        DecimalOverflowPolicy::ReportError,
                        &control,
                    )
                    .unwrap_err();
                    assert_eq!(error.control_error(), Some(cause), "callback {at}");
                    assert_eq!(control.recorded(), recorded[..=at], "callback {at}");
                }
            }
        }
    }
}

#[test]
fn scalar_rebind_wide_actual_nested_fvt_gate_observes_quantum_and_original_control() {
    let fields = (0..320)
        .map(|i| {
            Arc::new(
                Field::new(format!("source_{i}"), DataType::Int64, i % 2 == 0).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        String::from("provider"),
                        format!("original_{i}"),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    let args = vec![source(FunctionValueType::new(
        DataType::Struct(fields.into()),
        true,
    ))];
    let catalog = catalog(Behavior::Echo);
    for path in [Path::Ordinary, Path::Resolved] {
        let trace = Trace::default();
        let (actual, _, _) = bind(
            path,
            catalog.as_ref(),
            args.clone(),
            DecimalOverflowPolicy::ReportError,
            &trace,
        )
        .unwrap();
        assert_eq!(actual[0].value_type, args[0].value_type);
        let recorded = trace.recorded();
        let quantum = recorded
            .iter()
            .enumerate()
            .filter_map(|(i, (_, n))| (*n == 256).then_some(i))
            .collect::<Vec<_>>();
        assert!(!quantum.is_empty());
        // Sample first/last real quantum plus entry/tail, not thousands of
        // replays of the same wide source.
        let mut positions = vec![0, quantum[0], *quantum.last().unwrap(), recorded.len() - 1];
        positions.sort_unstable();
        positions.dedup();
        for at in positions {
            for cause in CAUSES {
                let control = Trace {
                    stop: Some((at, cause.clone())),
                    ..Trace::default()
                };
                let error = bind(
                    path,
                    catalog.as_ref(),
                    args.clone(),
                    DecimalOverflowPolicy::ReportError,
                    &control,
                )
                .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(control.recorded(), recorded[..=at]);
            }
        }
    }
}
