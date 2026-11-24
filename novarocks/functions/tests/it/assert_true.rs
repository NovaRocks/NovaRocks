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

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, StringArray};
use arrow_schema::DataType;
use novarocks_functions::{
    EvaluatedArgument, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionKind, Selection, builtin::catalogue::builtin_engine_function_catalog,
};
use novarocks_type_contract::FunctionValueType;

use super::scalar_rows::{Control, prepare_bound};

#[test]
fn failed_assertion_uses_default_or_first_non_null_message_and_tracks_the_failed_row() {
    let condition: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    let message: ArrayRef = Arc::new(StringArray::from(vec![None, Some("custom failure"), None]));
    let arguments = [DataType::Boolean, DataType::Utf8].map(|data_type| FunctionArgument::Value {
        value_type: FunctionValueType::new(data_type, true),
        constant: None,
    });
    let catalog = builtin_engine_function_catalog();
    for argc in 1..=2 {
        let request = FunctionBindingRequest {
            arguments: &arguments[..argc],
            logical_argument_count: argc,
            expected_result_type: None,
        };
        assert!(matches!(
            catalog.resolve_bound_user("assert_true", FunctionKind::Scalar, request, &Control),
            Err(FunctionBindingError::HiddenFunction)
        ));
        let bound = catalog
            .resolve_bound_trusted("assert_true", FunctionKind::Scalar, request, &Control)
            .unwrap();
        let mut instance = prepare_bound(catalog, bound, request);
        let values = [
            EvaluatedArgument::Column(&condition),
            EvaluatedArgument::Column(&message),
        ];
        let output = instance
            .evaluate(Selection::all(3), &values[..argc], &Control)
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(true), None, None],
        );
        let failures = output.errors();
        assert_eq!(failures[0].selected_ordinal(), 1);
        assert_eq!(
            failures[0].message(),
            if argc == 1 {
                "assert_true failed due to false value"
            } else {
                "custom failure"
            }
        );
        assert_eq!(failures[1].selected_ordinal(), 2);
        assert_eq!(
            failures[1].message(),
            "assert_true failed due to null value"
        );
    }
}
