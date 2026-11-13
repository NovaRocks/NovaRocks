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
mod scalar {
    use arrow::{
        array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
        datatypes::DataType,
    };
    use novarocks_execution::exec::expr::compiled_program::CompiledExpressionInstance;
    use novarocks_functions::*;
    use novarocks_sql_testkit::sql_source;
    use novarocks_test_compile_to_execute::programs::{batch, producer_root, programs};
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
            assert!(n <= 256);
            Ok(())
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
            assert!(n <= 256);
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("numeric transaction never waits")
        }
    }
    #[test]
    fn numeric_unary_owned_transaction_actual_sql_three_widths_frame_and_exchange_keep_public_receipt()
     {
        for input in [
            Arc::new(Int8Array::from(vec![i8::MIN, 1])) as ArrayRef,
            Arc::new(Int16Array::from(vec![i16::MIN, 1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![i32::MIN, 1])) as ArrayRef,
        ] {
            let source = sql_source(
                "SELECT -k AS original_negated FROM fixture",
                input.data_type().clone(),
            );
            let original = source
                .checked_original_result_declaration_observed(&Control)
                .unwrap()
                .unwrap();
            let computed = source.plan().result_port().unwrap();
            assert!(!original.fields()[0].ty.nullable());
            assert!(!computed.fields[0].ty.nullable());
            assert_eq!(original.fields()[0].ty, computed.fields[0].ty);
            assert_eq!(computed.output, original.original_port().output);
            assert_eq!(computed.fields[0].name, original.fields()[0].name);
            assert_eq!(computed.fields[0].alias, original.fields()[0].alias);
            assert_eq!(computed.fields[0].value, original.fields()[0].value);
            for edge in source.plan().edges().values() {
                for &value in edge.source.projection.iter() {
                    assert!(
                        !source.plan().fragments()[&edge.source.fragment].values()[&value]
                            .ty
                            .nullable()
                    );
                }
            }
            let programs = programs(&source);
            let (program, root) = producer_root(&programs);
            let data = batch(&program, root, input.clone());
            let mut frame =
                CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
            let selected = frame.evaluate(&data, Selection::all(2), &Control).unwrap();
            assert_eq!(selected.errors().len(), 1);
            assert_eq!(selected.errors()[0].selected_ordinal(), 0);
            assert_eq!(
                selected.errors()[0].reason(),
                DataFailureReason::NumericOutOfRange
            );
            assert!(selected.values().is_null(0));
            assert_eq!(selected.values().null_count(), 1);
            let rows = [1];
            let sparse = frame
                .evaluate(&data, Selection::try_sparse(2, &rows).unwrap(), &Control)
                .unwrap();
            assert_eq!(sparse.values().null_count(), 0);
            assert_eq!(
                sparse.values().to_data(),
                selected.values().slice(1, 1).to_data()
            );
            assert_eq!(
                frame
                    .evaluate(&data, Selection::try_sparse(2, &[]).unwrap(), &Control)
                    .unwrap()
                    .values()
                    .len(),
                0
            );
        }
    }
    #[test]
    fn numeric_unary_owned_transaction_actual_coalesce_isnull_and_if_are_consumers_of_computed_domain()
     {
        for sql in [
            "SELECT COALESCE(-k,CAST(0 AS TINYINT)) AS observed FROM fixture",
            "SELECT -k IS NULL AS observed FROM fixture",
            "SELECT IF(k=CAST(-128 AS TINYINT),CAST(0 AS TINYINT),-k) AS observed FROM fixture",
        ] {
            let source = sql_source(sql, DataType::Int8);
            let programs = programs(&source);
            let (program, root) = producer_root(&programs);
            let data = batch(&program, root, Arc::new(Int8Array::from(vec![i8::MIN, 1])));
            let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
            let out = frame.evaluate(&data, Selection::all(2), &Control).unwrap();
            if sql.contains("IF(") {
                assert!(out.errors().is_empty(), "inactive MIN is not evaluated");
                assert_eq!(out.values().null_count(), 0);
                let values = out.values().as_any().downcast_ref::<Int8Array>().unwrap();
                assert_eq!(values.values().as_ref(), &[0, -1]);
            } else {
                assert_eq!(out.errors().len(), 1);
                assert_eq!(out.errors()[0].selected_ordinal(), 0);
                assert_eq!(
                    out.errors()[0].reason(),
                    DataFailureReason::NumericOutOfRange
                );
                assert!(
                    out.values().is_null(0),
                    "required error has only a placeholder"
                );
                assert!(!out.values().is_null(1));
                if sql.contains("IS NULL") {
                    let values = out
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap();
                    assert!(!values.value(1));
                } else {
                    let values = out.values().as_any().downcast_ref::<Int8Array>().unwrap();
                    assert_eq!(values.value(1), -1);
                }
            }
        }
    }
    #[test]
    fn numeric_unary_owned_transaction_actual_int64_inactive_overflow_and_failed_frame_preserve_exact_control()
     {
        let source = sql_source("SELECT -k AS observed FROM fixture", DataType::Int64);
        let programs = programs(&source);
        let (program, root) = producer_root(&programs);
        let data = batch(
            &program,
            root,
            Arc::new(Int64Array::from(vec![i64::MIN, 1])),
        );
        let rows = [1];
        let selection = Selection::try_sparse(2, &rows).unwrap();
        let mut successful =
            CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
        assert!(
            successful
                .evaluate(&data, selection, &Control)
                .unwrap()
                .errors()
                .is_empty()
        );
        #[derive(Default)]
        struct Trace {
            events: Mutex<Vec<u32>>,
            refusal: Option<(usize, KernelFailure)>,
        }
        impl KernelEvaluationControl for Trace {
            fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
                assert!(n <= 256);
                let mut events = self.events.lock().unwrap();
                let at = events.len();
                if let Some((limit, _)) = &self.refusal {
                    assert!(at <= *limit);
                }
                events.push(n);
                match &self.refusal {
                    Some((limit, cause)) if at == *limit => Err(cause.clone()),
                    _ => Ok(()),
                }
            }
            fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
                panic!("never waits")
            }
        }
        let success = Trace::default();
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
        frame.evaluate(&data, selection, &success).unwrap();
        let trace = success.events.into_inner().unwrap();
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("original internal")),
                KernelFailure::Operational(KernelDiagnostic::new("original operational")),
                KernelFailure::InstanceFailed,
            ] {
                let control = Trace {
                    events: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                };
                let mut frame =
                    CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
                assert_eq!(
                    frame.evaluate(&data, selection, &control).unwrap_err(),
                    cause
                );
                assert_eq!(*control.events.lock().unwrap(), trace[..=at]);
                assert_eq!(
                    frame.evaluate(&data, selection, &control).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert_eq!(*control.events.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
mod ordered {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        FunctionArgumentType, NodeKind, PhysicalCallDefinition, PhysicalCallSite,
        StaticFunctionArgument,
    };
    use novarocks_sql_testkit::installed_builtin_owner_catalogue;
    use novarocks_sql_testkit::sql_source_with_semantics;
    use novarocks_test_compile_to_execute::programs::programs_with_catalogue;

    fn check(sql: &str, group_concat: bool) {
        let semantics = novarocks_sql::sql_mode::SqlSemanticSettings::default();
        let semantics = if group_concat {
            semantics.with_group_concat_max_len(4096)
        } else {
            semantics
        };
        let source = sql_source_with_semantics(sql, DataType::Int8, semantics);
        let mut count = 0usize;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    continue;
                };
                for (ordinal, call) in calls.iter().enumerate() {
                    let site = PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let request = fragment
                        .call_requests()
                        .get(PhysicalCallDefinition::Relational(site))
                        .unwrap();
                    let logical = request.logical_argument_count;
                    assert_eq!(
                        request.arguments.len() - logical,
                        1,
                        "actual ordered call source {sql}"
                    );
                    let StaticFunctionArgument::Value {
                        value_type,
                        constant: None,
                    } = &request.arguments[logical]
                    else {
                        panic!("the real -k ORDER channel has no constant backing")
                    };
                    assert_eq!(value_type.data_type().clone(), DataType::Int8);
                    assert!(
                        !value_type.nullable(),
                        "both modes retain required signed -k in their own ORDER request {sql} site {site:?}"
                    );
                    let FunctionArgumentType::Value(bound) =
                        &call.binding.function.argument_types[logical]
                    else {
                        panic!("actual ORDER selected Value")
                    };
                    assert_eq!(
                        bound, value_type,
                        "selected ORDER uses its actual request {site:?}"
                    );
                    if call.binding.phase.consumes_logical_arguments() {
                        assert_eq!(call.order_by.len(), 1);
                        assert_eq!(
                            fragment
                                .expressions()
                                .get(call.order_by[0].expr)
                                .unwrap()
                                .ty,
                            value_type.clone()
                        );
                    }
                    count += 1;
                }
            }
        }
        assert!(
            count >= 1,
            "actual SQL must retain an actual ordered aggregate call"
        );
        {
            let actual = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
            assert_eq!(actual.len(), source.plan().fragments().len());
        }
    }

    #[test]
    fn numeric_unary_exact_ordered_sql_array_agg() {
        check(
            "SELECT ARRAY_AGG(k ORDER BY -k) AS observed FROM fixture",
            false,
        );
    }

    #[test]
    fn numeric_unary_exact_ordered_sql_group_concat() {
        check(
            "SELECT GROUP_CONCAT(CAST(k AS VARCHAR) ORDER BY -k) AS observed FROM fixture",
            true,
        );
    }
}
