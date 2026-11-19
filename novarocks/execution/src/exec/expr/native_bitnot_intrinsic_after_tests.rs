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
//! Exact physical NULL is refused at BitwiseNot recipe preparation.

use arrow::datatypes::DataType;
use novarocks_functions::{ArithmeticPrepareError, PreparedNativeBitNotRecipe};
use novarocks_type_contract::FunctionValueType;

#[test]
fn native_bitnot_intrinsic_exact_physical_null_is_refused_before_folding() {
    let source = FunctionValueType::new(DataType::Null, true);
    let target = FunctionValueType::new(DataType::Int8, true);
    let refused = PreparedNativeBitNotRecipe::try_new(
        &source,
        &target,
        &novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap_err();
    assert!(matches!(refused, ArithmeticPrepareError::Kernel(_)));
    assert!(
        refused
            .to_string()
            .contains("native BitwiseNot unsupported source shape"),
        "typed recipe refusal: {refused}"
    );
}
