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

use super::*;
use crate::compiler::SqlFunctionCatalog;
use novarocks_functions::{FunctionSpecializationFailure, PureKernelAbi};
use novarocks_type_contract::{
    ArgumentControl, CompileControlError, CompilePhase, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, PureCompileControl,
};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(
                at <= stop,
                "callback after the original declaration refusal"
            );
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn prefixes(call: impl Fn(&Control) -> Result<(), FunctionSpecializationFailure>, success: bool) {
    let baseline = Control::default();
    assert_eq!(call(&baseline).is_ok(), success);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(
        trace.first(),
        Some(&(CompilePhase::FunctionSpecialization, 0))
    );
    if success {
        assert!(trace.iter().any(|(_, units)| *units > 0));
    }
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FunctionSpecializationFailure::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn sql_overload_declaration_borrows_same_installed_records_with_exact_lifecycle_hand_facts() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let snapshot = catalog.snapshot();
    // Independent literal ABI/effect expectations, not selected preparation output.
    let cases = [
        (
            "abs",
            FunctionKind::Scalar,
            PureKernelAbi::ScalarV1,
            ArgumentControl::OrderedNull,
            FunctionInstanceState::None,
            FunctionNullBehavior::Strict,
            FunctionIntrinsicRowError::NoRowError,
        ),
        (
            "if",
            FunctionKind::Scalar,
            PureKernelAbi::ControlIntrinsicV1,
            ArgumentControl::If,
            FunctionInstanceState::None,
            FunctionNullBehavior::ControlDefined,
            FunctionIntrinsicRowError::NoRowError,
        ),
        (
            "coalesce",
            FunctionKind::Scalar,
            PureKernelAbi::ControlIntrinsicV1,
            ArgumentControl::Coalesce,
            FunctionInstanceState::None,
            FunctionNullBehavior::ControlDefined,
            FunctionIntrinsicRowError::NoRowError,
        ),
        (
            "count",
            FunctionKind::Aggregate,
            PureKernelAbi::AggregateWindowV1,
            ArgumentControl::Aggregate,
            FunctionInstanceState::AggregateInstance,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        (
            "unnest",
            FunctionKind::Table,
            PureKernelAbi::TableV1,
            ArgumentControl::Table,
            FunctionInstanceState::TableInstance,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NoRowError,
        ),
        (
            "row_number",
            FunctionKind::Window,
            PureKernelAbi::WindowV1,
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        (
            "rank",
            FunctionKind::Window,
            PureKernelAbi::WindowV1,
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        (
            "dense_rank",
            FunctionKind::Window,
            PureKernelAbi::WindowV1,
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        (
            "cume_dist",
            FunctionKind::Window,
            PureKernelAbi::WindowV1,
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        (
            "percent_rank",
            FunctionKind::Window,
            PureKernelAbi::WindowV1,
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionNullBehavior::CalledOnNull,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
    ];
    for (name, kind, abi, arguments, state, nulls, row_error) in cases {
        let definition = catalog.definition(name, kind).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        assert_eq!(declaration.kind(), kind);
        assert!(!declaration.overloads().is_empty());
        for overload in declaration.overloads() {
            let direct_control = Control::default();
            let direct = EngineFunctionCatalog::pure_overload_declaration_observed(
                &catalog,
                declaration.function_id(),
                kind,
                &overload.identity,
                &direct_control,
            )
            .unwrap();
            let adapter_control = Control::default();
            let adapter = SqlFunctionCatalog::pure_overload_declaration_observed(
                &catalog,
                declaration.function_id(),
                kind,
                &overload.identity,
                &adapter_control,
            )
            .unwrap();
            let snapshot_control = Control::default();
            let projected = snapshot
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    kind,
                    &overload.identity,
                    &snapshot_control,
                )
                .unwrap();
            assert_eq!(
                *adapter_control.trace.lock().unwrap(),
                *direct_control.trace.lock().unwrap()
            );
            assert_eq!(
                *snapshot_control.trace.lock().unwrap(),
                *direct_control.trace.lock().unwrap()
            );
            assert!(std::ptr::eq(
                adapter.implementation(),
                direct.implementation()
            ));
            assert!(std::ptr::eq(
                projected.implementation(),
                direct.implementation()
            ));
            assert!(std::ptr::eq(adapter.effects(), direct.effects()));
            assert!(std::ptr::eq(projected.effects(), direct.effects()));
            assert!(std::ptr::eq(
                direct.effects(),
                overload.effects.as_ref().unwrap()
            ));
            assert!(direct_control.trace.lock().unwrap().last().unwrap().1 > 0);
            assert_eq!(direct.implementation().overload, overload.identity);
            assert_eq!(direct.implementation().abi, abi);
            assert_eq!(direct.effects().argument_control, arguments);
            assert_eq!(direct.effects().instance_state, state);
            assert_eq!(direct.effects().null_behavior, nulls);
            let expected_row_error = if name == "abs"
                && ["(i8)", "(i16)", "(i32)", "(i64)"]
                    .iter()
                    .any(|input| overload.argument_pattern.starts_with(input))
            {
                FunctionIntrinsicRowError::MayRaise
            } else {
                row_error
            };
            assert_eq!(direct.effects().own_row_error, expected_row_error);
            assert_eq!(
                direct.effects().value_stability,
                FunctionVolatility::Immutable
            );
            assert_eq!(
                direct.effects().failure_behavior,
                FunctionFailureBehavior::Propagate
            );
            assert!(direct.effects().observable_effects.is_empty());
            assert!(direct.effects().environment_dependencies.is_empty());
        }
    }
}

#[test]
fn sql_overload_declaration_exact_ids_and_kind_never_reresolve_a_name_or_overload() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let snapshot = catalog.snapshot();
    let declaration = catalog
        .definition("abs", FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap();
    let missing_function = FunctionId::try_new("missing.function/v1").unwrap();
    let missing_overload = FunctionOverloadId::try_new("missing.overload/v1").unwrap();
    for (case, (function, kind, overload)) in [
        (
            &missing_function,
            FunctionKind::Scalar,
            &declaration.overloads()[0].identity,
        ),
        (
            declaration.function_id(),
            FunctionKind::Window,
            &declaration.overloads()[0].identity,
        ),
        (
            declaration.function_id(),
            FunctionKind::Scalar,
            &missing_overload,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let direct_control = Control::default();
        let direct = EngineFunctionCatalog::pure_overload_declaration_observed(
            &catalog,
            function,
            kind,
            overload,
            &direct_control,
        )
        .unwrap_err();
        let assert_refusal = |error: &FunctionSpecializationFailure| match case {
            0 => assert!(matches!(
                error,
                FunctionSpecializationFailure::Binding(FunctionBindingError::UnknownFunction)
            )),
            1 => assert!(matches!(
                error,
                FunctionSpecializationFailure::InvalidInput(
                    "pure preparation has a different exact kind or selected owner"
                )
            )),
            2 => assert!(matches!(
                error,
                FunctionSpecializationFailure::Binding(FunctionBindingError::UnknownOverload(actual))
                    if actual == &missing_overload
            )),
            _ => unreachable!(),
        };
        assert_refusal(&direct);
        // The source refusal is already authored; no optional footer may
        // observe a later control cause and replace it.
        let trace = direct_control.trace.lock().unwrap().clone();
        assert_eq!(
            trace.first(),
            Some(&(CompilePhase::FunctionSpecialization, 0))
        );
        let adapter_control = Control::default();
        let adapter = snapshot
            .pure_overload_declaration_observed(function, kind, overload, &adapter_control)
            .unwrap_err();
        assert_eq!(
            std::mem::discriminant(&adapter),
            std::mem::discriminant(&direct)
        );
        assert_eq!(adapter.to_string(), direct.to_string());
        assert_eq!(
            *adapter_control.trace.lock().unwrap(),
            *direct_control.trace.lock().unwrap()
        );
        assert_refusal(&adapter);
        for cause in CAUSES {
            let late = Control {
                trace: Mutex::default(),
                refusal: Some((trace.len(), cause.clone())),
            };
            let original = EngineFunctionCatalog::pure_overload_declaration_observed(
                &catalog, function, kind, overload, &late,
            )
            .unwrap_err();
            assert_refusal(&original);
            assert_eq!(*late.trace.lock().unwrap(), trace);
            let late = Control {
                trace: Mutex::default(),
                refusal: Some((trace.len(), cause)),
            };
            let original = snapshot
                .pure_overload_declaration_observed(function, kind, overload, &late)
                .unwrap_err();
            assert_refusal(&original);
            assert_eq!(*late.trace.lock().unwrap(), trace);
        }
        prefixes(
            |control| {
                snapshot
                    .pure_overload_declaration_observed(function, kind, overload, control)
                    .map(|_| ())
            },
            false,
        );
    }
}

#[test]
fn sql_overload_declaration_every_actual_success_callback_preserves_three_original_causes() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let snapshot = catalog.snapshot();
    for (name, kind) in [
        ("abs", FunctionKind::Scalar),
        ("if", FunctionKind::Scalar),
        ("count", FunctionKind::Aggregate),
        ("unnest", FunctionKind::Table),
        ("rank", FunctionKind::Window),
    ] {
        let declaration = catalog
            .definition(name, kind)
            .unwrap()
            .binding_declaration()
            .unwrap();
        let overload = &declaration.overloads()[0].identity;
        prefixes(
            |control| {
                snapshot
                    .pure_overload_declaration_observed(
                        declaration.function_id(),
                        kind,
                        overload,
                        control,
                    )
                    .map(|_| ())
            },
            true,
        );
        prefixes(
            |control| {
                SqlFunctionCatalog::pure_overload_declaration_observed(
                    &catalog,
                    declaration.function_id(),
                    kind,
                    overload,
                    control,
                )
                .map(|_| ())
            },
            true,
        );
    }
}

#[derive(Clone, Debug)]
struct MetadataOnly(EngineFunctionCatalog);
impl SqlFunctionCatalog for MetadataOnly {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        panic!("declaration lookup must not resolve a SQL name")
    }
    fn resolve_aggregate_signature(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("declaration lookup must not resolve an aggregate name")
    }
    fn resolve_aggregate_trusted(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("declaration lookup must not use a trusted legacy resolver")
    }
    fn contains_aggregate(&self, _: &str) -> bool {
        panic!("declaration lookup must not infer a name")
    }
    fn volatility(&self, _: &str) -> FunctionVolatility {
        panic!("declaration lookup must not infer legacy effects")
    }
}

#[test]
fn sql_metadata_only_catalog_refuses_even_with_real_function_declarations_and_original_control() {
    let catalog = MetadataOnly(build_builtin_engine_function_catalog().unwrap());
    let declaration = catalog
        .0
        .definition("abs", FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap();
    assert!(declaration.overloads()[0].effects.is_some());
    let snapshot = catalog.snapshot();
    for projected in [&catalog as &dyn SqlFunctionCatalog, snapshot.as_ref()] {
        let control = Control::default();
        let error = projected
            .pure_overload_declaration_observed(
                declaration.function_id(),
                FunctionKind::Scalar,
                &declaration.overloads()[0].identity,
                &control,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            FunctionSpecializationFailure::InvalidInput(
                "SQL function snapshot has no installed pure overload declaration owner"
            )
        ));
        assert_eq!(
            *control.trace.lock().unwrap(),
            [(CompilePhase::FunctionSpecialization, 0)]
        );
        prefixes(
            |control| {
                projected
                    .pure_overload_declaration_observed(
                        declaration.function_id(),
                        FunctionKind::Scalar,
                        &declaration.overloads()[0].identity,
                        control,
                    )
                    .map(|_| ())
            },
            false,
        );
    }
}
