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
//! Actual analysis/request-snapshot admission probes; not a complete support seal.
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionKind, FunctionSpecializationFailure,
    FunctionValueType, ResolvedFunctionBinding,
};
use novarocks_sql::compiler::{
    SqlCompileControl, SqlFunctionCatalog, builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn argument(dtype: DataType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(dtype, true),
        constant: None,
    }
}
#[test]
fn sql_presence_candidate_snapshot_keeps_exact_bindings_and_table_owner_separate() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let again = scoped.snapshot().snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    let args = [argument(DataType::Utf8)];
    let original_binding = original
        .resolve_scalar_binding("reverse", &args, &control)
        .unwrap();
    assert_eq!(
        original_binding.admitted_execution_abi, None,
        "original declaration is not admission"
    );
    let actual_abi = original
        .pure_overload_declaration_observed(
            &original_binding.function_id,
            original_binding.kind,
            &original_binding.selected.overload,
            &control,
        )
        .unwrap()
        .implementation()
        .abi;
    // Scoped admission adds the real installed ABI without replacing source facts.
    let mut expected = original_binding.clone();
    expected.admitted_execution_abi = Some(actual_abi);
    for scope in [scoped.as_ref(), again.as_ref()] {
        let mut admitted = scope
            .resolve_scalar_binding("reverse", &args, &control)
            .unwrap();
        assert_eq!(admitted.admitted_execution_abi, Some(actual_abi));
        assert_eq!(admitted, expected);
        // Only the independently checked admission refinement differs. Every
        // original function, selected signature, semantics and source fact stays.
        admitted.admitted_execution_abi = None;
        assert_eq!(admitted, original_binding);
    }
    let repeat = [argument(DataType::Int64), argument(DataType::Int64)];
    let bound = original
        .resolve_scalar_binding("array_repeat", &repeat, &control)
        .unwrap();
    for scope in [scoped.as_ref(), again.as_ref()] {
        assert!(
            matches!(scope.resolve_scalar_binding("array_repeat", &repeat, &control), Err(FunctionBindingError::UnavailableImplementation(ref id)) if id == &bound.selected.overload)
        );
    }
    // This uses the actual registered table owner, separately from scalar admission.
    let bounds = [argument(DataType::List(Arc::new(
        arrow::datatypes::Field::new("item", DataType::Int64, true),
    )))];
    let table = original
        .resolve_table_binding("unnest", &bounds, &control)
        .unwrap();
    assert_eq!(table.kind, FunctionKind::Table);
    assert_eq!(table.admitted_execution_abi, None);
    let actual_table_abi = original
        .pure_overload_declaration_observed(
            &table.function_id,
            table.kind,
            &table.selected.overload,
            &control,
        )
        .unwrap()
        .implementation()
        .abi;
    for scope in [scoped.as_ref(), again.as_ref()] {
        let mut admitted = scope
            .resolve_table_binding("unnest", &bounds, &control)
            .unwrap();
        assert_eq!(admitted.admitted_execution_abi, Some(actual_table_abi));
        // The actual table implementation author establishes the admission
        // refinement independently; all original complete binding facts agree.
        admitted.admitted_execution_abi = None;
        assert_eq!(admitted, table);
    }
}

#[test]
fn sql_presence_candidate_exact_selected_loan_cannot_bypass_missing_owner() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    let args = [argument(DataType::Int64), argument(DataType::Int64)];
    let bound = original
        .resolve_scalar_binding("array_repeat", &args, &control)
        .unwrap();
    let request = novarocks_functions::FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: args.len(),
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
    assert!(matches!(scoped.select_exact_overload_observed(
        &bound.function_id, bound.kind, &bound.selected.overload, request, &control,
    ), Err(FunctionBindingError::UnavailableImplementation(ref id)) if id == &bound.selected.overload));
}

#[derive(Clone, Debug)]
struct RefusePresence {
    original: Arc<dyn SqlFunctionCatalog>,
    cause: CompileControlError,
    calls: Arc<AtomicUsize>,
}
impl SqlFunctionCatalog for RefusePresence {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        _: &novarocks_functions::FunctionId,
        _: FunctionKind,
        _: &novarocks_functions::FunctionOverloadId,
        _: &dyn PureCompileControl,
    ) -> Result<novarocks_functions::PureOverloadDeclaration<'a>, FunctionSpecializationFailure>
    {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(FunctionSpecializationFailure::Control(self.cause.clone()))
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        types: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original.resolve_scalar_signature(name, types, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.original.resolve_scalar_binding(name, args, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.original.contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        types: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_aggregate_signature(name, types, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        types: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_aggregate_trusted(name, types, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        self.original.volatility(name)
    }
}
#[test]
fn sql_presence_candidate_declared_owner_control_is_first_cause_without_later_admission() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let original = RefusePresence {
            original: builtin_sql_function_catalog().snapshot(),
            cause: cause.clone(),
            calls: Arc::clone(&calls),
        };
        let args = [argument(DataType::Utf8)];
        let scoped = original.snapshot_for_scalar_presence();
        assert!(
            matches!(scoped.resolve_scalar_binding("reverse", &args, &SqlCompileControl::unbounded()), Err(FunctionBindingError::Control(actual)) if actual == cause)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        // Original mode continues to use the actual unprojected resolver, not the failing loan.
        original
            .resolve_scalar_binding("reverse", &args, &SqlCompileControl::unbounded())
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
