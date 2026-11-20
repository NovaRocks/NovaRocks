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

use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_functions::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionKind, Selection, builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::{CompilePhase, FunctionValueType};

use super::scalar_rows::{Control, prepare};

#[test]
fn bar_and_bucket_use_frozen_constants_and_preserve_original_bounds() {
    let ty = FunctionValueType::new(DataType::Int64, true);
    let mut arguments = vec![FunctionArgument::Value {
        value_type: ty.clone(),
        constant: None,
    }];
    for value in [0, 10, 6] {
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("bound").unwrap()),
            ty.clone(),
            Int64Array::from(vec![Some(value)]).to_data(),
            ConstantPolicy {
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
            },
            CompilePhase::Validate,
            &Control,
        )
        .unwrap();
        arguments.push(FunctionArgument::Value {
            value_type: ty.clone(),
            constant: Some(pool.value(0).unwrap()),
        });
    }
    let data: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(0),
        Some(5),
        Some(10),
        Some(11),
        None,
    ]));
    let mut values = vec![EvaluatedArgument::Column(&data)];
    for argument in &arguments[1..] {
        let FunctionArgument::Value {
            constant: Some(value),
            ..
        } = argument
        else {
            unreachable!()
        };
        values.push(EvaluatedArgument::Constant(value));
    }
    let selection = Selection::all(data.len());
    for name in ["bar", "equiwidth_bucket"] {
        assert_eq!(
            prepare(name, &arguments).contract().effects().own_row_error,
            novarocks_functions::FunctionIntrinsicRowError::MayRaise
        );
    }
    let bar = prepare("bar", &arguments)
        .evaluate(selection, &values, &Control)
        .unwrap();
    let bars = bar.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        bars.iter().collect::<Vec<_>>(),
        [Some(""), Some("▓▓▓"), Some("▓▓▓▓▓▓"), None, None]
    );
    assert!(bar.errors()[0].message().contains("size <= max"));
    let bucket = prepare("equiwidth_bucket", &arguments)
        .evaluate(selection, &values, &Control)
        .unwrap();
    let buckets = bucket
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        buckets.iter().collect::<Vec<_>>(),
        [Some(0), Some(5), Some(10), None, None]
    );
    assert_eq!(
        bucket.errors()[0].message(),
        "equiwidth_bucket requirement: size <= max"
    );

    let catalog = build_builtin_engine_function_catalog().unwrap();
    arguments[1] = FunctionArgument::Value {
        value_type: ty,
        constant: None,
    };
    for name in ["bar", "equiwidth_bucket"] {
        let error = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 4,
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("argument[min] must be constant"),
            "{error}"
        );
    }
}
