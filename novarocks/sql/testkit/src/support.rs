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
use novarocks_functions::ConstantPolicy;

/// Parse one owned query before handing test syntax to the compiler.
pub fn parse_query(sql: &str) -> Box<novarocks_parser::ast::Query> {
    let [novarocks_parser::ast::Statement::Query(query)]: [novarocks_parser::ast::Statement; 1] =
        novarocks_parser::parse(sql)
            .expect("parse test query")
            .try_into()
            .expect("exactly one test statement")
    else {
        panic!("expected a test query");
    };
    Box::new(query)
}

/// Constant admission shared by the SQL source fixtures.
pub fn sql_fixture_constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
