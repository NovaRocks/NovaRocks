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

use super::*;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;

fn admitted(default: &FunctionValueType, result: &FunctionValueType) -> bool {
    supports_common_offset_type(default, result, || Ok::<(), ValueTypeError>(())).unwrap()
}

#[test]
fn common_type_preserves_nominal_identity_and_nested_field_facts() {
    let physical = FunctionValueType::new(DataType::LargeBinary, false);
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    assert!(admitted(&json.with_nullable(false).unwrap(), &json));
    assert!(!admitted(&physical, &json));

    let row = |name, nullable| {
        FunctionValueType::new(
            DataType::Struct(vec![Arc::new(Field::new(name, DataType::Int64, nullable))].into()),
            true,
        )
    };
    let result = row("amount", true);
    assert!(admitted(&result.with_nullable(false).unwrap(), &result));
    assert!(!admitted(&row("amount", false), &result));
    assert!(!admitted(&row("other", true), &result));
}
