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

use arrow_schema::{DataType, Field};
use novarocks_functions::builtin::catalogue::{
    array_projection_diagnostic_private_test_catalog, build_builtin_engine_function_catalog,
};
use novarocks_functions::{
    ConstantPolicy, ConstantValue, FunctionArgument, FunctionBindingRequest, FunctionKind,
    FunctionResultType, FunctionSpecializationFailure,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[test]
fn scalar_invocation_three_compile_causes_remain_typed_at_actual_declaration_loan() {
    struct Stop(CompileControlError);
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }

    let policy = ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    };
    let source = FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("ID", DataType::Int32, true))].into()),
            true,
        ))),
        true,
    );
    let name_type = FunctionValueType::new(DataType::Utf8, false);
    let name = ConstantValue::from_utf8(
        Arc::new(name_type.try_to_field("actual field name").unwrap()),
        name_type.clone(),
        "id",
        policy,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let builtin = build_builtin_engine_function_catalog().unwrap();
    let random = builtin
        .resolve_bound_user(
            "rand",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &[],
                logical_argument_count: 0,
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap();
    let FunctionResultType::Scalar(random_type) = random.selected.result_type else {
        panic!("RAND tail has its real scalar result type");
    };
    // The original Direct profile passed the source and folded field name
    // followed by its actual RAND tail, even on the no-argument failure path.
    let arguments = [
        FunctionArgument::Value {
            value_type: source,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: name_type,
            constant: Some(name),
        },
        FunctionArgument::Value {
            value_type: random_type,
            constant: None,
        },
    ];
    let functions = array_projection_diagnostic_private_test_catalog();
    let bound = functions
        .resolve_bound_user(
            "__array_struct_subfield",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap();
    assert_eq!(
        bound.function_id.as_str(),
        "builtin.scalar/__array_struct_subfield/v1"
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let result = functions.pure_overload_declaration_observed(
            &bound.function_id,
            FunctionKind::Scalar,
            &bound.selected.overload,
            &Stop(cause.clone()),
        );
        assert!(
            matches!(result, Err(FunctionSpecializationFailure::Control(actual)) if actual == cause)
        );
    }
}
