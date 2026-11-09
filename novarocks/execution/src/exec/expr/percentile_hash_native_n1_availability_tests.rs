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

//! N1 owner-derived admission before real folding; Original binding remains full N.
use super::sql_fold_observation_test_source::{EVALUATOR, SERIAL, fold_count, reset, session};
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionValueType,
};
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SqlCompileControl, SqlCompileError, SqlCompileIntent,
    SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlStatementInput,
    builtin_sql_function_catalog,
};
#[test]
fn hash_native_n1_exact_admission_preserves_original_variadic_binding_and_refuses_before_fold() {
    let _serial = SERIAL.lock().unwrap();
    let original = builtin_sql_function_catalog().snapshot();
    let exact = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    for n in [1, 2, 5] {
        let args = (0..n)
            .map(|_| FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int32, true),
                constant: None,
            })
            .collect::<Vec<_>>();
        let bound = original
            .resolve_scalar_binding("percentile_hash", &args, &control)
            .unwrap();
        assert_eq!(bound.admitted_execution_abi, None);
        assert_eq!(bound.logical_argument_count, n);
        assert_eq!(bound.selected.argument_types.len(), n);
        let request = FunctionBindingRequest {
            arguments: &args,
            logical_argument_count: n,
            expected_result_type: None,
        };
        original
            .select_exact_overload_observed(
                &bound.function_id,
                bound.kind,
                &bound.selected.overload,
                request,
                &control,
            )
            .unwrap();
        if n == 1 {
            assert_eq!(bound.admitted_execution_abi, None);
            let actual_abi = original
                .pure_overload_declaration_observed(
                    &bound.function_id,
                    bound.kind,
                    &bound.selected.overload,
                    &control,
                )
                .unwrap()
                .implementation()
                .abi;
            let mut admitted = exact
                .resolve_scalar_binding("percentile_hash", &args, &control)
                .unwrap();
            assert_eq!(admitted.admitted_execution_abi, Some(actual_abi));
            admitted.admitted_execution_abi = None;
            assert_eq!(
                admitted, bound,
                "complete original selected source is unchanged"
            );
            exact
                .select_exact_overload_observed(
                    &bound.function_id,
                    bound.kind,
                    &bound.selected.overload,
                    request,
                    &control,
                )
                .unwrap();
        } else {
            assert!(
                matches!(exact.resolve_scalar_binding("percentile_hash", &args, &control),
                Err(FunctionBindingError::UnavailableImplementation(ref o)) if o == &bound.selected.overload)
            );
            assert!(
                matches!(exact.select_exact_overload_observed(&bound.function_id, bound.kind,
                &bound.selected.overload, request, &control),
                Err(FunctionBindingError::UnavailableImplementation(ref o)) if o == &bound.selected.overload)
            );
        }
    }
    for sql in [
        "SELECT percentile_hash(CAST(17 AS INT),CAST('bad' AS INT)) AS encoded",
        "SELECT percentile_hash(CAST(17 AS INT),CAST(1 AS INT),CAST(2 AS INT),CAST(3 AS INT),CAST(4 AS INT))",
    ] {
        reset();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([88; 16]).unwrap(),
            SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(sql)),
            SqlCompileIntent::Query,
            session(),
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            &EVALUATOR,
            super::test_support::constant_policy(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        );
        let SqlCompileError::Analyze(error) = request
            .try_into_completion()
            .err()
            .expect("deferred production arity must fail before completing the source")
        else {
            panic!("expected original typed analysis category");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert!(error.span().is_some());
        assert!(error.message().contains("builtin.scalar/percentile_hash/"));
        assert_eq!(fold_count(), 0);
    }
}
