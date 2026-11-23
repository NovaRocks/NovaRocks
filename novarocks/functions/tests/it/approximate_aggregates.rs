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

use arrow_schema::DataType;
use novarocks_functions::builtin::catalogue::builtin_engine_function_catalog;
use novarocks_functions::{
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionValueType, PureCallLifecycle,
    ResolvedFunctionBinding,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn select(name: &str, types: &[DataType]) -> ResolvedFunctionBinding {
    let arguments = types
        .iter()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type.clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    builtin_engine_function_catalog()
        .resolve_bound_user(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap()
}

#[test]
fn approximate_aggregates_distinct_installed_and_declared_only_contracts() {
    let catalog = builtin_engine_function_catalog();
    for (input, state, default, explicit) in [
        (
            DataType::Null,
            "unknown-v1",
            "unknown-v1",
            "unknown-error-v1",
        ),
        (
            DataType::Boolean,
            "boolean-set-v1",
            "boolean-v1",
            "boolean-error-v1",
        ),
        (
            DataType::Int64,
            "hll-xxhash64-p11-v1",
            "default-hll-v1",
            "explicit-error-hll-v1",
        ),
    ] {
        let bound = select("approx_distinct", &[input.clone()]);
        assert_eq!(
            bound.selected.overload.as_str(),
            format!("builtin.aggregate/approx_distinct/{default}")
        );
        let installed = catalog
            .pure_overload_declaration_observed(
                &bound.function_id,
                bound.kind,
                &bound.selected.overload,
                &Control,
            )
            .unwrap();
        installed
            .admit_selected_lifecycle_observed(
                &bound.selected,
                1,
                PureCallLifecycle::Aggregate,
                &Control,
            )
            .unwrap();
        let state = format!("novarocks/approx_distinct/{state}");
        assert_eq!(
            bound.selected.aggregate.unwrap().state_format.as_str(),
            state
        );
        let epsilon = select("approx_distinct", &[input, DataType::Float64]);
        assert_eq!(
            epsilon.selected.overload.as_str(),
            format!("builtin.aggregate/approx_distinct/{explicit}")
        );
        assert!(
            catalog
                .definition_by_id(&epsilon.function_id)
                .unwrap()
                .is_declared_only_pure_overload(&epsilon.selected.overload)
        );
        assert!(
            catalog
                .pure_overload_declaration_observed(
                    &epsilon.function_id,
                    epsilon.kind,
                    &epsilon.selected.overload,
                    &Control
                )
                .is_err()
        );
    }
}
