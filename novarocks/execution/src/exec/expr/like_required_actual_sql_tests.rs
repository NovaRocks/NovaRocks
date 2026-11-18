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

//! Actual original LIKE source FVT and permanent Frame success oracles.
//! Before preparation these are RED at UnsupportedExpression(Like).
use arrow::datatypes::{DataType, Field};
use novarocks_sql_testkit::{inspect_like_source, sql_source_with_single_field};

#[test]
fn like_sql_source_original_required_name_pattern_full_fvt() {
    for sql in [
        "SELECT name LIKE 'a%' FROM t_filter_in_between_like",
        "SELECT name NOT LIKE 'a%' FROM t_filter_in_between_like",
        "SELECT name LIKE name FROM t_filter_in_between_like",
        "SELECT name LIKE 'a!%' ESCAPE '!' FROM t_filter_in_between_like",
        "SELECT name FROM t_filter_in_between_like WHERE name LIKE 'a%'",
    ] {
        // Exact original required column declaration; isolated LIKE subtree.
        // This does not claim the multi-column native plan was reproduced.
        let source = sql_source_with_single_field(sql, Field::new("name", DataType::Utf8, true));
        assert!(inspect_like_source(&source) > 0);
    }
}
