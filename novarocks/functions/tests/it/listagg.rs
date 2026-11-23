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
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionValueType,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[test]
fn listagg_binds_five_logical_values_and_preserves_order_channels() {
    let catalog = builtin_engine_function_catalog();
    let arguments = [
        DataType::Utf8,
        DataType::Utf8,
        DataType::Boolean,
        DataType::Utf8,
        DataType::Boolean,
        DataType::Int64,
    ]
    .map(|data_type| FunctionArgument::Value {
        value_type: FunctionValueType::new(data_type, true),
        constant: None,
    });
    let declared = catalog
        .resolve_bound_user(
            "listagg",
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 5,
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap();
    assert_eq!(declared.logical_argument_count, 5);
    assert_eq!(declared.selected.argument_types.len(), 6);
    assert_eq!(
        declared.selected.overload.as_str(),
        "builtin.aggregate/listagg/derived-v1"
    );
}
