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

use arrow::datatypes::{DataType, Field};
use novarocks_sql::{
    analyze_error::AnalyzeErrorKind,
    compiler::{SqlCompileError, SqlCompileProgressError},
};
use novarocks_sql_testkit::try_sql_source_with_declared_fields;

#[test]
fn numeric_not_rejects_dynamic_values_and_scalar_constants_during_typing() {
    for (name, carrier, literals) in [
        ("TINYINT", DataType::Int8, ["0", "1", "0-1", "0"]),
        ("SMALLINT", DataType::Int16, ["0", "1", "0-1", "0"]),
        ("INT", DataType::Int32, ["0", "1", "0-1", "0"]),
        ("BIGINT", DataType::Int64, ["0", "1", "0-1", "0"]),
        ("REAL", DataType::Float32, ["0e0", "1e0", "'NaN'", "'-0'"]),
        ("DOUBLE", DataType::Float64, ["0e0", "1e0", "'NaN'", "'-0'"]),
        (
            "DECIMAL(18,4)",
            DataType::Decimal128(18, 4),
            ["'0'", "'0.0001'", "'-0.0001'", "'0'"],
        ),
    ] {
        let fields = [Field::new("k", carrier, false)];
        let constants =
            literals.map(|value| format!("SELECT NOT CAST({value} AS {name}) AS inverted"));
        for sql in std::iter::once("SELECT NOT k AS inverted FROM fixture.source")
            .chain(constants.iter().map(String::as_str))
        {
            let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) =
                try_sql_source_with_declared_fields(sql, &fields)
            else {
                panic!("numeric NOT must fail during SQL typing: {sql}");
            };
            assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch, "{sql}");
            assert!(
                error
                    .message()
                    .starts_with("NOT requires a boolean operand"),
                "{sql}: {error:?}"
            );
        }
    }
}
