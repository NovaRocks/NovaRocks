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

//! Actual original SQL source receipts and permanent IN/NOT IN Frame witnesses.
//! Expected truth values are fixed independently of any value evaluator.
use arrow::datatypes::{DataType, Field};
use novarocks_sql_testkit::{inspect_inlist_source, sql_source_with_single_field};

#[test]
fn inlist_sql_source_original_required_numeric_and_nullable_region_facts() {
    for (sql, field) in [
        (
            "SELECT ship_code IN (1,2) FROM t0",
            Field::new("ship_code", DataType::Int32, true),
        ),
        (
            "SELECT c_region IN ('ASIA','AMERICA') FROM t_filter_customer_metrics",
            Field::new("c_region", DataType::Utf8, true),
        ),
        (
            "SELECT c_region NOT IN ('ASIA','AMERICA',NULL) FROM t_filter_customer_metrics",
            Field::new("c_region", DataType::Utf8, true),
        ),
        (
            "SELECT (CASE WHEN ship_code >= 90 THEN 'A' WHEN ship_code >= 80 THEN 'B' WHEN ship_code >= 70 THEN 'C' WHEN ship_code >= 60 THEN 'D' ELSE 'E' END) IN ('A','B') FROM t0",
            Field::new("ship_code", DataType::Int32, true),
        ),
    ] {
        let source = sql_source_with_single_field(sql, field);
        assert!(
            inspect_inlist_source(&source) > 0,
            "original source must retain real IN definition"
        );
    }
}
