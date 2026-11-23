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
    EvaluatedArgument, FunctionSpecializationFailure, FunctionValueType, PureFunctionMetadataOwner,
    ScalarEvaluationInstance, Selection,
};
use arrow_schema::DataType;
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
        panic!("URL functions never wait")
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
fn instance(name: &str, arity: usize) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(name, &sources(arity), DecimalOverflowPolicy::OutputNull)
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
fn sources(arity: usize) -> Vec<FunctionValueType> {
    vec![source(true); arity]
}
#[test]
fn url_parameter_selects_raw_keys_and_propagates_null() {
    let owner = owner_for_test("url_extract_parameter");
    assert_eq!(owner.implementation_declarations().len(), 1);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let prepared =
            prepared_for_test_with_policy("url_extract_parameter", &sources(2), policy).unwrap();
        let contract = prepared.contract();
        assert_eq!(contract.result_type(), &source(true));
        assert_eq!(
            contract.effects().null_behavior,
            FunctionNullBehavior::Strict
        );
        assert_eq!(
            contract.effects().own_row_error,
            crate::FunctionIntrinsicRowError::NoRowError
        );
        assert!(contract.effects().environment.is_empty());
        assert_eq!(contract.decimal_overflow_policy(), policy);
        let urls = strings(vec![
            Some("HTTP://ExAmPle.test:80/a/../b?%71=first+name&q=second%26name&q=third"),
            Some("HTTP://ExAmPle.test:80/a/../b?%71=first+name&q=second%26name&q=third"),
            Some("/x?q="),
            Some("/x?q=%"),
            Some("/x?q=inactive"),
            None,
        ]);
        let keys = strings(vec![
            Some("q"),
            Some("%71"),
            Some("q"),
            Some("q"),
            None,
            Some("q"),
        ]);
        let arguments = [
            EvaluatedArgument::Column(&urls),
            EvaluatedArgument::Column(&keys),
        ];
        let result = ScalarEvaluationInstance::instantiate(prepared)
            .unwrap()
            .evaluate(Selection::all(6), &arguments, &Control::default())
            .unwrap();
        assert_eq!(
            output(&result),
            [
                Some("second&name".into()),
                Some("first name".into()),
                Some("".into()),
                None,
                None,
                None
            ]
        );
    }
    assert!(operation("parse_url").is_none());
}

#[test]
fn url_decode_keeps_percent_escape_errors_row_local() {
    let input = strings(vec![Some("a+b"), Some("%GG"), None, Some("%41")]);
    let arguments = [EvaluatedArgument::Column(&input)];
    let result = instance("url_decode", 1)
        .evaluate(Selection::all(4), &arguments, &Control::default())
        .unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some("a b"), None, None, Some("A")]
    );
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
    assert_eq!(
        result.errors()[0].reason(),
        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
    );
}

#[test]
fn string_parse_url_actual_runtime_callbacks_preserve_all_seven_causes_and_failed_instance() {
    let long = format!("https://x/{}?q=value", "a".repeat(700));
    for (name, arity) in [("url_extract_path", 1), ("url_extract_parameter", 2)] {
        for urls in [
            strings(vec![Some(&long)]),
            strings(vec![None; 321]),
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
        ] {
            let keys = strings(vec![Some("q")]);
            let args = [
                EvaluatedArgument::Column(&urls),
                EvaluatedArgument::Scalar(&keys),
            ];
            let good = Control::default();
            let ok = instance(name, arity)
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
                    let mut kernel = instance(name, arity);
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
    for (name, arity) in [("url_extract_path", 1), ("url_extract_parameter", 2)] {
        for bad in [false, true] {
            let mut ty = sources(arity);
            if bad {
                ty[0] = FunctionValueType::new(DataType::Binary, true);
            }
            let good = CompileControl::default();
            assert_eq!(
                prepared_for_test_with_control(name, &ty, DecimalOverflowPolicy::OutputNull, &good)
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
                        name,
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

#[test]
fn parse_url_trino_preserves_raw_components_and_parameter_order() {
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let url = "HTTP://ExAmPle.test:80/a/../b?%71=first+name&q=second%26name&q=third#c%2B+d";
    let result = |part, key, work: &mut EvaluationCheckpoints<'_>| {
        url_text_part(part, url, key, work)
            .unwrap()
            .unwrap()
            .unwrap()
    };
    assert_eq!(result(TrinoUrlPart::Protocol, None, &mut work), "HTTP");
    assert_eq!(result(TrinoUrlPart::Host, None, &mut work), "ExAmPle.test");
    assert_eq!(result(TrinoUrlPart::Path, None, &mut work), "/a/../b");
    assert_eq!(result(TrinoUrlPart::Fragment, None, &mut work), "c++d");
    assert_eq!(
        result(TrinoUrlPart::Parameter, Some("%71"), &mut work),
        "first name"
    );
    assert_eq!(
        result(TrinoUrlPart::Parameter, Some("q"), &mut work),
        "second&name"
    );
    assert_eq!(
        TrinoUri::parse(url, &mut work).unwrap().unwrap().port,
        Some(80)
    );
    assert_eq!(
        TrinoUri::parse("a?q=1", &mut work).unwrap().unwrap().port,
        None
    );
}

#[test]
fn parse_url_trino_distinguishes_empty_absent_and_invalid_components() {
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    for (source, host, port, path) in [
        ("///", "", None, "/"),
        ("mailto:me@example.com?subject=Hi", "", None, ""),
        ("http://a_b:7/", "", None, "/"),
        ("http://a:2147483648/", "", None, "/"),
        ("http://a:65536/", "a", Some(65536), "/"),
        ("http://[fe80::1%eth0]/", "[fe80::1%eth0]", None, "/"),
        (
            "http://[::ffff:192.001.001.1]/",
            "[::ffff:192.001.001.1]",
            None,
            "/",
        ),
    ] {
        let parsed = TrinoUri::parse(source, &mut work).unwrap().unwrap();
        assert_eq!(parsed.port, port, "{source}");
        assert_eq!(parsed.text(TrinoUrlPart::Host), host, "{source}");
        assert_eq!(parsed.text(TrinoUrlPart::Path), path, "{source}");
    }
    for source in ["//", "http:", "http://[v1.z]/", "/a[b]", "/a?q=%"] {
        assert!(
            TrinoUri::parse(source, &mut work).unwrap().is_none(),
            "{source}"
        );
    }
    for (key, value) in [("a", Some("")), ("q", Some("")), ("missing", None)] {
        assert_eq!(
            TrinoUri::parse("/a?a&q=&q=after", &mut work)
                .unwrap()
                .unwrap()
                .raw_parameter(key, &mut work)
                .unwrap(),
            value
        );
    }
    assert_eq!(
        url_text_part(TrinoUrlPart::Query, "/a?q=[a%20b]+", None, &mut work)
            .unwrap()
            .unwrap(),
        Some("q=[a b]+".into())
    );
}

#[test]
fn url_decode_trino_form_keeps_java_replacement_and_escape_rules() {
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    for (source, value) in [
        ("%4a+%e4%b8%ad", "J 中"),
        ("%ED%A0%80", "\u{fffd}"),
        ("%ED%A0%41", "\u{fffd}A"),
        ("%E0%80%80", "\u{fffd}\u{fffd}\u{fffd}"),
        ("%F0%9F%98%80", "😀"),
    ] {
        assert_eq!(
            TrinoUrlDecode::Form
                .decode(source, &mut work)
                .unwrap()
                .unwrap(),
            value
        );
    }
    for source in ["%0", "%+1", "%ＡＦ", "%GG"] {
        assert!(
            TrinoUrlDecode::Form
                .decode(source, &mut work)
                .unwrap()
                .is_err()
        );
    }
}
