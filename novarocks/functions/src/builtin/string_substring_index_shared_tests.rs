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

use super::super::string_substring_index_owner::prepared_for_test_with_policy;
use super::*;
use crate::{EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance, Selection};
use novarocks_type_contract::DecimalOverflowPolicy;
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
        if let Some((stop, _)) = self.refusal.as_ref() {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("substring_index never waits")
    }
}

#[test]
fn string_substring_index_shared_complete_success_traces_and_bounded_seven_causes() {
    let wide = "x".repeat(1025);
    let repeated = "x".repeat(320);
    let large = "x".repeat(1_048_577);
    let fixtures = [
        (Some("aaaaa"), Some("aa"), Some(1), 0),
        (Some("aaaaa"), Some("aa"), Some(-1), 0),
        (Some("aaaaa"), Some("aa"), Some(-3), 0),
        (Some("no match"), Some("/"), Some(i32::MIN), 0),
        (Some("é::你好::\0"), Some("::"), Some(-2), 0),
        (Some("hidden text"), Some(""), Some(-1), 0),
        (Some("hidden text"), Some("/"), Some(0), 0),
        (None, Some("/"), Some(1), 0),
        (Some("hidden text"), None, Some(1), 0),
        (Some("hidden text"), Some("/"), None, 0),
        (Some(wide.as_str()), Some("y"), Some(-1), 1),
        (Some(repeated.as_str()), Some("x"), Some(1), 1),
        (Some(repeated.as_str()), Some("x"), Some(-1), 1),
        (Some(large.as_str()), Some("y"), Some(1), 2),
    ];
    let mut observations = vec![];
    let mut checks = 0usize;
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let types = [
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Int32, true),
        ];
        let prepared = prepared_for_test_with_policy("substring_index", &types, policy).unwrap();
        let make = || ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        for (case, &(text, delimiter, count, sampling)) in fixtures.iter().enumerate() {
            let arrays = [
                Arc::new(StringArray::from(vec![text])) as ArrayRef,
                Arc::new(StringArray::from(vec![delimiter])) as ArrayRef,
                Arc::new(Int32Array::from(vec![count])) as ArrayRef,
            ];
            let arguments = arrays
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            for (mask, selection) in [
                ("all", Selection::all(1)),
                ("sparse", Selection::try_sparse(1, &[0]).unwrap()),
                ("empty", Selection::try_sparse(1, &[]).unwrap()),
            ] {
                let good = Control::default();
                let result = make().evaluate(selection, &arguments, &good).unwrap();
                assert!(result.errors().is_empty());
                assert_eq!(result.selection(), selection);
                let output = result
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(str::to_owned))
                    .collect::<Vec<_>>();
                let trace = good.trace.lock().unwrap().clone();
                observations.push(serde_json::json!({
                    "policy": format!("{policy:?}"), "case": case, "selection": mask,
                    "output": output, "units": trace,
                }));
                // Preserve all success traces, but fault only one full-row run.
                // Wide scans sample entry/tail/256; the >1 MiB value samples
                // entry/tail only, avoiding quadratic failure injection.
                if mask != "all" {
                    continue;
                }
                assert!(!trace.is_empty());
                // Each match flushes, so repeated-match scans alone need not
                // emit a 256-unit callback. Wide output copying does.
                if matches!(case, 10 | 13) {
                    assert!(trace.contains(&256));
                }
                for (at, units) in trace.iter().enumerate() {
                    if sampling > 0
                        && at != 0
                        && at + 1 != trace.len()
                        && (sampling == 2 || *units != 256)
                    {
                        continue;
                    }
                    for cause in [
                        KernelFailure::Cancelled,
                        KernelFailure::DeadlineExceeded,
                        KernelFailure::ResourceExhausted,
                        invalid("original refusal"),
                        internal("original refusal"),
                        KernelFailure::Operational(crate::KernelDiagnostic::new(
                            "original refusal",
                        )),
                        KernelFailure::InstanceFailed,
                    ] {
                        let control = Control {
                            trace: Mutex::new(vec![]),
                            refusal: Some((at, cause.clone())),
                        };
                        let mut kernel = make();
                        assert_eq!(
                            kernel
                                .evaluate(selection, &arguments, &control)
                                .unwrap_err(),
                            cause
                        );
                        assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                        let after = Control::default();
                        assert_eq!(
                            kernel.evaluate(selection, &arguments, &after).unwrap_err(),
                            KernelFailure::InstanceFailed
                        );
                        assert!(after.trace.lock().unwrap().is_empty());
                        checks += 1;
                    }
                }
            }
        }
    }
    assert_eq!(observations.len(), 84);
    if let Some(directory) = std::env::var_os("NOVA_SUBSTRING_INDEX_TRACE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("success-traces.json"),
            serde_json::to_vec(&observations).unwrap(),
        )
        .unwrap();
    }
    println!(
        "C12-P3 complete success observations={} seven-cause checks={checks}; bounded wide sampling",
        observations.len()
    );
}
