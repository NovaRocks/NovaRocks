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
//! Actual installed lifecycle/static-profile admission. This is not the full E08s1 seal.
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionValueType, PureCallLifecycle as Lifecycle,
    PureKernelAbi,
};
use novarocks_sql::compiler::{SqlCompileControl, builtin_sql_function_catalog};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn arg(ty: DataType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(ty, true),
        constant: None,
    }
}
#[test]
fn e08s1_selected_loan_checks_actual_five_lifecycles_without_preparing_a_value() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    let scalar = original
        .resolve_scalar_binding("reverse", &[arg(DataType::Utf8)], &c)
        .unwrap();
    let aggregate = original
        .resolve_aggregate_binding("count", 1, &[arg(DataType::Int32)], &c)
        .unwrap();
    let window = original
        .resolve_window_binding("row_number", &[], &c)
        .unwrap();
    let table = original
        .resolve_table_binding(
            "unnest",
            &[arg(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int64,
                true,
            ))))],
            &c,
        )
        .unwrap();
    for (binding, lifecycle, abi) in [
        (&scalar, Lifecycle::Scalar, PureKernelAbi::ScalarV1),
        (
            &aggregate,
            Lifecycle::Aggregate,
            PureKernelAbi::AggregateWindowV1,
        ),
        (
            &aggregate,
            Lifecycle::AggregateWindow {
                distinct: false,
                ordered: false,
            },
            PureKernelAbi::AggregateWindowV1,
        ),
        (&window, Lifecycle::Window, PureKernelAbi::WindowV1),
        (&table, Lifecycle::Table, PureKernelAbi::TableV1),
    ] {
        let loan = original
            .pure_overload_declaration_observed(
                &binding.function_id,
                binding.kind,
                &binding.selected.overload,
                &c,
            )
            .unwrap();
        assert_eq!(loan.implementation().abi, abi);
        scoped
            .admit_bound_lifecycle_observed(binding, lifecycle, &c)
            .unwrap();
        original
            .admit_bound_lifecycle_observed(binding, lifecycle, &c)
            .unwrap();
    }
    assert!(
        matches!(scoped.admit_bound_lifecycle_observed(&scalar, Lifecycle::Table, &c),
        Err(FunctionBindingError::UnavailableImplementation(id)) if id == scalar.selected.overload)
    );
}
struct Refuse {
    cause: CompileControlError,
    calls: AtomicUsize,
}
impl PureCompileControl for Refuse {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::Relaxed),
            0,
            "no checkpoint after first refusal"
        );
        Err(self.cause.clone())
    }
}
#[test]
fn e08s1_actual_selected_lifecycle_retains_three_compile_causes_and_original_has_no_lookup() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    let binding = original
        .resolve_aggregate_binding("count", 1, &[arg(DataType::Int32)], &c)
        .unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Refuse {
            cause: cause.clone(),
            calls: AtomicUsize::new(0),
        };
        original
            .admit_bound_lifecycle_observed(
                &binding,
                Lifecycle::AggregateWindow {
                    distinct: false,
                    ordered: false,
                },
                &control,
            )
            .unwrap();
        assert_eq!(control.calls.load(Ordering::Relaxed), 0);
        assert!(
            matches!(scoped.admit_bound_lifecycle_observed(&binding, Lifecycle::AggregateWindow { distinct: false, ordered: false }, &control),
            Err(FunctionBindingError::Control(actual)) if actual == cause)
        );
        assert_eq!(control.calls.load(Ordering::Relaxed), 1);
    }
}
