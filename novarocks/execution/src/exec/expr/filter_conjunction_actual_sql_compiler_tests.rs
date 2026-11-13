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
// Transitional re-export: chain inputs live in the compile-to-execute lib.
// The NDV test remains here until its separate source fixture migrates.
use super::ndv_filter_actual_sql_source_tests::ndv_sql_source;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use novarocks_local_program::ProgramNodeKind;
use novarocks_physical_plan::NodeKind;
pub(super) use novarocks_test_compile_to_execute::compiler_results::{
    compile_options, compiler_results, compiler_results_with_declared_read_fields, source_packages,
    validate_package,
};
const HAVING: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING ndv(v) = 0 AND approx_count_distinct(v) = 0 ORDER BY k;";
fn permanent() {
    let source = ndv_sql_source(HAVING);
    let mut filter_fragment = None;
    for (&id, fragment) in source.plan().fragments() {
        for node in fragment.nodes().values() {
            if let NodeKind::Filter { predicates } = &node.kind
                && predicates.len() > 1
            {
                assert_eq!(node.inputs.len(), 1);
                assert_eq!(predicates.len(), 2);
                assert!(filter_fragment.replace((id, node.id)).is_none());
            }
        }
    }
    let (id, original_node) = filter_fragment.unwrap();
    let mut actual = compiler_results(&source, &installed_builtin_owner_catalogue());
    let program = actual
        .remove(&id)
        .unwrap()
        .expect("the full original SQL Filter must lower successfully");
    assert_eq!(
        program
            .graph()
            .nodes()
            .iter()
            .filter(|node| matches!(node.kind(), ProgramNodeKind::Filter { .. }))
            .count(),
        1
    );
    eprintln!("actual full Filter lowered from original node {original_node:?} in mode compiled");
}

#[test]
fn filter_conjunction_candidate_sql_compiler_complete() {
    permanent();
}
