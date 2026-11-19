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
//! Actual owner, normal-channel source proof and original regex behavior.
use super::*;
use crate::{
    EvaluatedArgument, FunctionArgument, FunctionSpecializationFailure, FunctionValueType,
    ScalarEvaluationInstance, ScopedExpressionEffects, Selection, specialize_frozen_scalar,
    specialize_scalar,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, RegexpCountPatternSource as Source,
    SemanticParameters,
};
use std::{sync::Mutex, time::Duration};
fn owner() -> RegexpCountOwner {
    let (_, signatures) = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(name, _)| name == "regexp_count")
        .unwrap();
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        "regexp_count",
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    RegexpCountOwner::new("regexp_count", declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    }
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        expected_result_type: None,
        logical_argument_count: arguments.len(),
    }
}
fn input<'a>(
    owner: &'a RegexpCountOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
    source: Source,
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: (context()).into(),
        argument_uses: crate::CallArgumentUses::RegexpCountPattern {
            source,
            channels: uses,
        },
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    }
}
fn prepared(
    source: Source,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    let owner = owner();
    let arguments = [false, true].map(|nullable| FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Utf8, nullable),
        constant: None,
    });
    let selected = Arc::new(owner.resolve(request(&arguments), control)?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
    specialize_scalar(
        &owner,
        input(&owner, &selected, &arguments, &parameters, &uses, source),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )
    .map(|s| s.into_prepared())
}
#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Trace {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push(units);
        if let Some((fail, cause)) = &self.refusal
            && at == *fail
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("REGEXP_COUNT never waits")
    }
}
#[test]
fn regexp_count_actual_owner_all_nullable_profiles_and_source_effects() {
    let owner = owner();
    assert_eq!(owner.implementation_declarations().len(), 1);
    assert_eq!(
        owner.implementation_declarations()[0].abi,
        PureKernelAbi::ScalarV1
    );
    for left in [false, true] {
        for right in [false, true] {
            for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
                let arguments = [left, right].map(|n| FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Utf8, n),
                    constant: None,
                });
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                let parameters = SemanticParameters::try_new([]).unwrap();
                let uses = [
                    Some(ExpressionUseId::new(42)),
                    Some(ExpressionUseId::new(43)),
                ];
                let exact = input(&owner, &selected, &arguments, &parameters, &uses, source);
                let fresh = specialize_scalar(
                    &owner,
                    exact,
                    selected.clone(),
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                let facts = fresh.prepared().contract().effects();
                assert_eq!(
                    facts.own_row_error,
                    if source.invalid_pattern_is_error() {
                        FunctionIntrinsicRowError::MayRaise
                    } else {
                        FunctionIntrinsicRowError::NoRowError
                    }
                );
                assert_eq!(facts.argument_control, ArgumentControl::Eager);
                assert_eq!(
                    fresh.prepared().contract().regexp_count_pattern_source(),
                    Some(source)
                );
                let frozen = specialize_frozen_scalar(
                    &owner,
                    exact,
                    selected.clone(),
                    facts,
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                assert_eq!(fresh.prepared().contract(), frozen.prepared().contract());
                let mut absent = exact;
                absent.argument_uses = crate::CallArgumentUses::SelectedChannels(&uses);
                assert!(
                    specialize_scalar(
                        &owner,
                        absent,
                        selected.clone(),
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control()
                    )
                    .is_err()
                );
                let mut opposite = exact;
                opposite.argument_uses = crate::CallArgumentUses::RegexpCountPattern {
                    source: if source == Source::Dynamic {
                        Source::NativeV1Utf8LiteralWhenPresent
                    } else {
                        Source::Dynamic
                    },
                    channels: &uses,
                };
                assert!(
                    owner
                        .prepare_scalar(
                            opposite,
                            fresh.prepared().contract().clone(),
                            crate::binding_test_control()
                        )
                        .is_err()
                );
            }
        }
    }
}
#[test]
fn regexp_count_same_invalid_bytes_follow_real_source_fact_and_selected_null_mask() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("guard"),
        None,
        Some("abc"),
        Some("éé"),
        Some("a{,}"),
        Some("guard"),
    ]));
    let patterns: ArrayRef = Arc::new(StringArray::from(vec![
        Some("guard"),
        Some("["),
        Some("["),
        Some("."),
        Some("a{,}"),
        Some("guard"),
    ]));
    let strings = strings.slice(1, 4);
    let patterns = patterns.slice(1, 4);
    let args = [
        EvaluatedArgument::Column(&strings),
        EvaluatedArgument::Column(&patterns),
    ];
    let rows = [0, 1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
        // Nullable source fixture is constructed by the actual owner profile.
        let owner = owner();
        let arguments = [0, 1].map(|_| FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: None,
        });
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [
            Some(ExpressionUseId::new(42)),
            Some(ExpressionUseId::new(43)),
        ];
        let p = specialize_scalar(
            &owner,
            input(&owner, &selected, &arguments, &parameters, &uses, source),
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .unwrap()
        .into_prepared();
        let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
        let out = k.evaluate(selection, &args, &Trace::default()).unwrap();
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, None, Some(2), Some(0)]
        );
        if source == Source::Dynamic {
            assert!(out.errors().is_empty())
        } else {
            assert_eq!(out.errors().len(), 1);
            assert_eq!(out.errors()[0].selected_ordinal(), 1);
            assert!(
                out.errors()[0]
                    .message()
                    .starts_with("Invalid regex expression: [. Detail message:")
            );
        }
        assert!(
            k.evaluate(
                Selection::try_sparse(4, &[]).unwrap(),
                &args,
                &Trace::default()
            )
            .unwrap()
            .errors()
            .is_empty()
        );
    }
}
#[test]
fn regexp_count_every_runtime_checkpoint_preserves_seven_causes_and_failed_latch() {
    for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
        for pattern in [".", "[", "a{,}", ""] {
            let text: ArrayRef = Arc::new(StringArray::from(vec!["é".repeat(300)]));
            let pattern: ArrayRef = Arc::new(StringArray::from(vec![pattern]));
            let args = [
                EvaluatedArgument::Scalar(&text),
                EvaluatedArgument::Scalar(&pattern),
            ];
            let p = prepared(source, crate::binding_test_control()).unwrap();
            let trace = Trace::default();
            ScalarEvaluationInstance::instantiate(p.clone())
                .unwrap()
                .evaluate(Selection::all(3), &args, &trace)
                .unwrap();
            let trace = trace.calls.lock().unwrap().clone();
            for at in 0..trace.len() {
                for cause in [
                    KernelFailure::Cancelled,
                    KernelFailure::DeadlineExceeded,
                    KernelFailure::ResourceExhausted,
                    invalid("original host cause"),
                    crate::kernel_control::internal("original host cause"),
                    KernelFailure::Operational(crate::KernelDiagnostic::new("original host cause")),
                    KernelFailure::InstanceFailed,
                ] {
                    let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                    let failure = Trace {
                        calls: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        k.evaluate(Selection::all(3), &args, &failure).unwrap_err(),
                        cause
                    );
                    assert_eq!(*failure.calls.lock().unwrap(), trace[..=at]);
                    let after = Trace::default();
                    assert_eq!(
                        k.evaluate(Selection::all(3), &args, &after).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.calls.lock().unwrap().is_empty());
                }
            }
        }
    }
}
#[derive(Default)]
struct CompileTrace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileTrace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push((phase, units));
        if let Some((fail, cause)) = &self.refusal
            && *fail == at
        {
            return Err(cause.clone());
        }
        Ok(())
    }
}
#[test]
fn regexp_count_every_actual_compile_checkpoint_preserves_three_causes() {
    for source in [Source::Dynamic, Source::NativeV1Utf8LiteralWhenPresent] {
        let ctl = CompileTrace::default();
        prepared(source, &ctl).unwrap();
        let count = ctl.calls.lock().unwrap().len();
        for at in 0..count {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let ctl = CompileTrace {
                    calls: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                let error = prepared(source, &ctl).unwrap_err();
                // Preparation retains the established KernelFailure boundary;
                // binding and effect callbacks retain CompileControlError.
                let actual = match &error {
                    FunctionSpecializationFailure::Control(actual) => Some(actual.clone()),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                };
                assert_eq!(actual, Some(cause), "{at}: {error:?}");
                assert_eq!(ctl.calls.lock().unwrap().len(), at + 1);
            }
        }
    }
}
