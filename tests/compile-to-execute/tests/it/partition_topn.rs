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

//! Real SQL -> frozen package -> LocalCompiler partition ranking contract.
use arrow::datatypes::DataType;
use novarocks_local_program::{
    LocalSortMode, PartitionRank, ProgramChannelLayoutRole, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeKind, ProgramRootInput, StaticExprKind,
    root_input_layout,
};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue;

#[test]
fn partition_topn_preserves_actual_sql_rank_limit_and_key_roots() {
    for (function, keep, expected_rank) in [
        ("rank", 2, PartitionRank::Rank),
        ("dense_rank", 2, PartitionRank::DenseRank),
        ("row_number", 1, PartitionRank::RowNumber),
    ] {
        // One source field satisfies the existing frozen-read fixture. Explicit
        // order flags differ from the partition prefix and must survive lowering.
        let sql = format!(
            "SELECT k, r FROM (SELECT k, {function}() OVER \
             (PARTITION BY k ORDER BY k DESC NULLS LAST) AS r \
             FROM fixture.source) AS ranked WHERE r <= {keep}"
        );
        let source = sql_source(&sql, DataType::Int64);
        let catalogue = installed_builtin_owner_catalogue();
        let programs = programs_with_catalogue(&source, &catalogue);
        let mut found = 0;
        for program in programs.values() {
            for node in program.graph().nodes() {
                let ProgramNodeKind::Sort {
                    input,
                    order_by,
                    mode:
                        LocalSortMode::PartitionTopN {
                            partition_by,
                            limit,
                            rank,
                        },
                } = node.kind()
                else {
                    continue;
                };
                found += 1;
                assert_eq!(limit.get(), keep);
                assert_eq!(*rank, expected_rank);
                assert_eq!(partition_by.len(), 1);
                assert_eq!(order_by.len(), 1);
                assert!(partition_by[0].asc);
                assert!(partition_by[0].nulls_first);
                assert!(!order_by[0].asc);
                assert!(!order_by[0].nulls_first);

                let node_id = node.local_id().expect("compiled Sort has a local identity");
                let snapshot = program
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                for (role, expression) in [
                    (
                        ProgramNodeExpressionRole::SortPartition { key: 0 },
                        partition_by[0].expr,
                    ),
                    (
                        ProgramNodeExpressionRole::SortOrder { key: 0 },
                        order_by[0].expr,
                    ),
                ] {
                    let site = ProgramExpressionRootSite::Node {
                        node: node_id,
                        role,
                    };
                    let root = snapshot
                        .roots()
                        .sites()
                        .get(&site)
                        .expect("the actual Sort field has its declared root role");
                    assert_eq!(root.definition, expression);
                    assert!(snapshot.bindings().contains_key(&site));
                    let ProgramRootInput::Layout {
                        node: input_node,
                        role: input_role,
                    } = root_input_layout(program.graph(), site).unwrap()
                    else {
                        panic!("a source key reads the actual Sort input");
                    };
                    assert_eq!(input_node, *input);
                    assert_eq!(input_role, ProgramChannelLayoutRole::NodeOutput);
                    let layout = program
                        .checked()
                        .channels()
                        .channel_layout(input_node, input_role)
                        .unwrap();
                    assert_eq!(layout.schema().fields().len(), 1);
                    assert_eq!(layout.schema().field(0).data_type(), &DataType::Int64);
                    let definition = program.graph().expressions().node(expression).unwrap();
                    let StaticExprKind::SlotId(slot) = definition.kind() else {
                        panic!("SQL k remains the declared source slot");
                    };
                    assert_eq!(*slot, layout.slots()[0]);
                    assert_eq!(
                        definition.value_type().unwrap().data_type(),
                        &DataType::Int64
                    );
                    assert!(!definition.value_type().unwrap().nullable());
                }
            }
        }
        assert_eq!(
            found, 1,
            "{function} must lower the original partition limit"
        );
    }
}
