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
#[test]
fn actual_map_key_keeps_original_exact_binding_and_candidate_named_refusal() {
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileProgressError},
    };
    use novarocks_sql_testkit::try_sql_source_with_declared_fields;
    {
        let result = try_sql_source_with_declared_fields(
            "SELECT map{CAST(1 AS TINYINT):10,CAST(2 AS TINYINT):20}[k] AS picked FROM t",
            &[Field::new("k", DataType::Int64, false)],
        );

        let failure = result.unwrap_err();
        let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
            panic!("actual FE named missing implementation refusal: {failure:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert_eq!(
            error.message(),
            "selected overload `builtin.scalar/map/dynamic-v1` has no installed implementation for the selected carrier"
        );
    }
}
