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
mod call_definitions {
    use arrow::datatypes::DataType;
    use novarocks_sql::compiler::{SqlCompileError, check_pure_call_definitions_observed};
    use novarocks_sql_testkit::sql_source;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Refuse {
        cause: CompileControlError,
        calls: AtomicUsize,
    }
    impl PureCompileControl for Refuse {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            assert_eq!(
                self.calls.fetch_add(1, Ordering::Relaxed),
                0,
                "no tail callback"
            );
            Err(self.cause.clone())
        }
    }
    #[test]
    fn e08s1_late_actual_source_three_compile_causes_stop_before_first_definition() {
        let source = sql_source(
            "SELECT reverse(CAST(k AS VARCHAR)) FROM fixture",
            DataType::Int32,
        );
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Refuse {
                cause: cause.clone(),
                calls: AtomicUsize::new(0),
            };
            let expected: SqlCompileError = cause.into();
            assert_eq!(
                check_pure_call_definitions_observed(&source, &control).unwrap_err(),
                expected
            );
            assert_eq!(control.calls.load(Ordering::Relaxed), 1);
        }
    }
}
mod result_declaration {
    use arrow::datatypes::DataType;
    use novarocks_sql::compiler::ResultDeclarationError;
    use novarocks_sql_testkit::sql_source as nonnull_source;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;
    struct Trace {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        fail: Option<usize>,
        cause: CompileControlError,
    }
    impl PureCompileControl for Trace {
        fn checkpoint(&self, p: CompilePhase, n: u32) -> Result<(), CompileControlError> {
            assert!(n <= 256);
            let mut calls = self.calls.lock().unwrap();
            calls.push((p, n));
            if self.fail == Some(calls.len() - 1) {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn numeric_unary_public_transport_observed_receipt_preserves_first_control_cause() {
        let label = "x".repeat(700);
        let source = nonnull_source(
            &format!("SELECT -k AS `{label}` FROM fixture"),
            DataType::Int8,
        );
        let ok = Trace {
            calls: Mutex::new(Vec::new()),
            fail: None,
            cause: CompileControlError::Cancelled,
        };
        source
            .checked_original_result_declaration_observed(&ok)
            .unwrap();
        let prefix = ok.calls.lock().unwrap().clone();
        assert!(prefix.iter().any(|(_, n)| *n == 256));
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..prefix.len() {
                let fail = Trace {
                    calls: Mutex::new(Vec::new()),
                    fail: Some(at),
                    cause: cause.clone(),
                };
                match source.checked_original_result_declaration_observed(&fail) {
                    Err(ResultDeclarationError::Control(got)) => assert_eq!(got, cause),
                    _ => panic!("receipt must retain the actual compile control failure"),
                };
                assert_eq!(
                    &*fail.calls.lock().unwrap(),
                    &prefix[..=at],
                    "no secondary footer/checkpoint after refusal"
                );
            }
        }
    }
}
