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

use super::super::string_parse_url_owner::{
    operation, owner_for_test, prepared_for_test_with_control, prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionSpecializationFailure,
    FunctionValueType, PureFunctionMetadataOwner, ScalarEvaluationInstance, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionNullBehavior,
    PureCompileControl,
};
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("parse_url never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn instance(arity: usize) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "parse_url",
            &sources(arity),
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<String>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(
            ty.try_to_field("original")
                .unwrap()
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    "source-note".into(),
                    "kept selected backing".into(),
                )])),
        ),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 1,
            max_logical_elements: 8,
            max_retained_buffer_bytes: 2 * 1024 * 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 4 * 1024 * 1024,
            max_library_validation_bytes: 4 * 1024 * 1024,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}

fn sources(arity: usize) -> Vec<FunctionValueType> {
    vec![source(true); arity]
}
#[test]
fn string_parse_url_exact_profiles_metadata_and_null_key_rule() {
    for arity in [2, 3] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let owner = owner_for_test("parse_url");
            assert_eq!(owner.implementation_declarations().len(), 2);
            let prepared =
                prepared_for_test_with_policy("parse_url", &sources(arity), policy).unwrap();
            let contract = prepared.contract();
            assert_eq!(contract.result_type(), &source(true));
            assert_eq!(
                contract.effects().null_behavior,
                FunctionNullBehavior::CalledOnNull
            );
            assert_eq!(
                contract.effects().argument_control,
                novarocks_type_contract::ArgumentControl::Eager
            );
            assert_eq!(
                contract.effects().own_row_error,
                crate::FunctionIntrinsicRowError::NoRowError
            );
            assert!(contract.effects().environment.is_empty());
            assert_eq!(contract.decimal_overflow_policy(), policy);
            let urls = strings(vec![
                Some("https://EXAMPLE.com/a?q=a+b&q=second#f"),
                Some("https://x/a"),
                Some("https://x/a"),
                Some("bad"),
                None,
            ]);
            let parts = strings(vec![
                Some("query"),
                Some("HOST"),
                Some("QUERY"),
                Some("HOST"),
                Some("PATH"),
            ]);
            let keys = strings(vec![Some("q"), None, None, None, Some("x")]);
            let all = [
                EvaluatedArgument::Column(&urls),
                EvaluatedArgument::Column(&parts),
                EvaluatedArgument::Column(&keys),
            ];
            let result = ScalarEvaluationInstance::instantiate(prepared)
                .unwrap()
                .evaluate(Selection::all(5), &all[..arity], &Control::default())
                .unwrap();
            let first = if arity == 2 { "q=a+b&q=second" } else { "a b" };
            assert_eq!(
                output(&result),
                vec![Some(first.into()), Some("x".into()), None, None, None]
            );
        }
    }
    for arity in [0, 1, 4] {
        assert!(
            prepared_for_test_with_policy(
                "parse_url",
                &sources(arity),
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
    assert_eq!(operation("parse_url"), Some(()));
    assert_eq!(operation("PARSE_URL"), None);
}
#[test]
fn string_parse_url_selected_slices_compact_scalar_and_nonzero_constant_pool_are_exact() {
    for arity in [2, 3] {
        let backing = strings(vec![
            Some("unused"),
            Some("https://x/a?q=a+b"),
            None,
            Some("https://x/b?q=c"),
            Some("unused"),
        ]);
        let text = backing.slice(1, 3);
        let rows = [0, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let compact = SelectedValues::try_new(
            selection,
            &DataType::Utf8,
            strings(vec![Some("https://x/a?q=a+b"), Some("https://x/b?q=c")]),
            Box::default(),
        )
        .unwrap();
        let parts = strings(vec![Some("QUERY")]);
        let original_pool = pool(strings(vec![None, Some("unused"), Some("q")]));
        let value = original_pool.value(2).unwrap();
        for url in [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            let all = [
                url,
                EvaluatedArgument::Scalar(&parts),
                EvaluatedArgument::Constant(&value),
            ];
            let result = instance(arity)
                .evaluate(selection, &all[..arity], &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(
                output(&result),
                if arity == 2 {
                    vec![Some("q=a+b".into()), Some("q=c".into())]
                } else {
                    vec![Some("a b".into()), Some("c".into())]
                }
            );
        }
        assert!(Arc::ptr_eq(value.pool().array(), original_pool.array()));
        let empty = Selection::try_sparse(3, &[]).unwrap();
        let all = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Scalar(&parts),
            EvaluatedArgument::Constant(&value),
        ];
        assert!(
            instance(arity)
                .evaluate(empty, &all[..arity], &Control::default())
                .unwrap()
                .values()
                .is_empty()
        );
    }
}
#[test]
fn string_parse_url_selected_hidden_null_inactive_long_and_nonnull_profiles() {
    let padded = strings(vec![
        Some("https://guard/"),
        Some("https://x/a"),
        Some("https://hidden/secret"),
        Some("invalid"),
        Some("guard"),
    ]);
    let text = Arc::new(StringArray::new(
        padded
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .offsets()
            .clone(),
        padded
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .values()
            .clone(),
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    )) as ArrayRef;
    let text = text.slice(1, 3);
    let part = strings(vec![Some("HOST")]);
    let key = strings(vec![None]);
    for arity in [2, 3] {
        let all = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Scalar(&part),
            EvaluatedArgument::Scalar(&key),
        ];
        let rows = [0, 1];
        assert_eq!(
            output(
                &instance(arity)
                    .evaluate(
                        Selection::try_sparse(3, &rows).unwrap(),
                        &all[..arity],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some("x".into()), None]
        );
        for mask in 0..(1 << arity) {
            let ty: Vec<_> = (0..arity).map(|i| source(mask & (1 << i) != 0)).collect();
            let url = strings(vec![Some("invalid")]);
            let part = strings(vec![Some("HOST")]);
            let key = strings(vec![Some("x")]);
            let args = [
                EvaluatedArgument::Column(&url),
                EvaluatedArgument::Column(&part),
                EvaluatedArgument::Column(&key),
            ];
            assert_eq!(
                output(
                    &ScalarEvaluationInstance::instantiate(
                        prepared_for_test_with_policy(
                            "parse_url",
                            &ty,
                            DecimalOverflowPolicy::OutputNull
                        )
                        .unwrap()
                    )
                    .unwrap()
                    .evaluate(Selection::all(1), &args[..arity], &Control::default())
                    .unwrap()
                ),
                vec![None]
            );
        }
    }
}
#[test]
fn string_parse_url_actual_runtime_callbacks_preserve_all_seven_causes_and_failed_instance() {
    let long = format!("https://x/{}", "a".repeat(700));
    for arity in [2, 3] {
        for urls in [
            strings(vec![Some(&long)]),
            strings(vec![None; 321]),
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
        ] {
            let parts = strings(vec![Some("PATH")]);
            let keys = strings(vec![None]);
            let args = [
                EvaluatedArgument::Column(&urls),
                EvaluatedArgument::Scalar(&parts),
                EvaluatedArgument::Scalar(&keys),
            ];
            let good = Control::default();
            let ok = instance(arity)
                .evaluate(Selection::all(urls.len()), &args[..arity], &good)
                .is_ok();
            assert_eq!(ok, urls.data_type() == &DataType::Utf8);
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            if ok {
                assert!(trace.contains(&256));
            }
            for at in 0..trace.len() {
                for cause in [
                    KernelFailure::Cancelled,
                    KernelFailure::DeadlineExceeded,
                    KernelFailure::ResourceExhausted,
                    invalid("original refusal"),
                    internal("original refusal"),
                    KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
                    KernelFailure::InstanceFailed,
                ] {
                    let refusal = Control {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let mut kernel = instance(arity);
                    assert_eq!(
                        kernel
                            .evaluate(Selection::all(urls.len()), &args[..arity], &refusal)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*refusal.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        kernel
                            .evaluate(Selection::all(urls.len()), &args[..arity], &after)
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.trace.lock().unwrap().is_empty());
                }
            }
        }
    }
}
#[test]
fn string_parse_url_prepare_actual_callback_prefixes_preserve_three_compile_causes() {
    for arity in [2, 3] {
        for bad in [false, true] {
            let mut ty = sources(arity);
            if bad {
                ty[0] = FunctionValueType::new(DataType::Binary, true);
            }
            let good = CompileControl::default();
            assert_eq!(
                prepared_for_test_with_control(
                    "parse_url",
                    &ty,
                    DecimalOverflowPolicy::OutputNull,
                    &good
                )
                .is_ok(),
                !bad
            );
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for at in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let refusal = CompileControl {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let actual = prepared_for_test_with_control(
                        "parse_url",
                        &ty,
                        DecimalOverflowPolicy::OutputNull,
                        &refusal,
                    )
                    .err()
                    .and_then(|error| match error {
                        FunctionSpecializationFailure::Control(c) => Some(c),
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
                    });
                    assert_eq!(actual, Some(cause));
                    assert_eq!(*refusal.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
#[test]
fn string_parse_url_output_representation_gates_do_not_claim_funding() {
    assert!(output_capacity(usize::MAX, 0).is_err());
    assert!(output_capacity(0, i32::MAX as usize + 1).is_err());
    assert!(output_capacity(0, 0).is_ok());
}
