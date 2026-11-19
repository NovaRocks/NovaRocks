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
//! SQL admission rejects exact NULL before BitwiseNot folding.

use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;

#[test]
fn native_bitnot_intrinsic_exact_physical_null_is_refused_before_folding() {
    let refused =
        std::panic::catch_unwind(|| sql_source("SELECT ~NULL", DataType::Int64)).unwrap_err();
    let message = refused
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| refused.downcast_ref::<&str>().copied())
        .unwrap();
    assert!(
        message.contains("native BitwiseNot unsupported source shape"),
        "actual pre-fold source refusal: {message}"
    );
}
