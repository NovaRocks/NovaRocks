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
use novarocks_parser::Span;
use novarocks_physical_plan::{
    Distribution, NodeKind, RowMultiplicity, SetOperationKind, StagePlacementPolicy, ValueOrigin,
};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};
use novarocks_sql_testkit::{sql_source_with_declared_fields, try_sql_source_with_declared_fields};

#[test]
fn all_is_refused_only_for_comparison_set_operations() {
    for operator in ["INTERSECT", "EXCEPT"] {
        let sql = format!("SELECT 1 {operator} ALL SELECT 2");
        let failure = try_sql_source_with_declared_fields(&sql, &[]).unwrap_err();
        let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
            panic!("expected analysis refusal: {failure:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedQueryShape);
        assert_eq!(error.code().as_str(), "sql.analyze.unsupported_query_shape");
        assert_eq!(error.span(), Some(Span::new(0, sql.len())));
        assert_eq!(error.message(), format!("{operator} ALL is not supported"));
    }

    let compiled = sql_source_with_declared_fields("SELECT 1 UNION ALL SELECT 1", &[]);
    let plan = compiled.plan();
    assert!(plan.edges().values().all(|edge| {
        edge.partitioning.source == Distribution::Singleton
            && edge.partitioning.destination == Distribution::Singleton
    }));
    assert!(plan.fragments().values().any(|fragment| {
        fragment.stage_placement() == &StagePlacementPolicy::Singleton
            && fragment.nodes().values().any(|node| {
                matches!(
                    &node.kind,
                    NodeKind::SetOp {
                        kind: SetOperationKind::UnionAll,
                        ..
                    }
                ) && node.output_properties.distribution == Distribution::Singleton
            })
    }));
}

#[test]
fn distributed_except_hashes_the_widened_comparison_value_at_the_sender() {
    let compiled = sql_source_with_declared_fields(
        "SELECT narrow_key FROM t EXCEPT SELECT wide_key FROM t",
        &[
            Field::new("narrow_key", DataType::Int32, false),
            Field::new("wide_key", DataType::Int64, false),
        ],
    );
    let plan = compiled.plan();
    let (stage, set_op) = plan
        .fragments()
        .iter()
        .find_map(|(fragment_id, fragment)| {
            fragment.nodes().values().find_map(|node| {
                matches!(
                    &node.kind,
                    NodeKind::SetOp {
                        kind: SetOperationKind::Except,
                        ..
                    }
                )
                .then_some((fragment_id, node))
            })
        })
        .expect("EXCEPT comparison stage");
    let mut scheme = None;
    for (ordinal, input) in set_op.inputs.iter().enumerate() {
        let edge = plan
            .edges()
            .values()
            .find(|edge| edge.destination.fragment == *stage && edge.destination.node == *input)
            .expect("each comparison input arrives through its hash exchange");
        let Distribution::Hash {
            keys,
            scheme: current,
        } = &edge.partitioning.source
        else {
            panic!("comparison input is not hash partitioned");
        };
        assert_eq!(keys.as_ref(), edge.source.projection.as_ref());
        assert!(scheme.is_none_or(|previous| previous == current));
        scheme = Some(current);

        let sender = &plan.fragments()[&edge.source.fragment];
        let key = &sender.values()[&keys[0]];
        assert_eq!(key.ty.data_type(), &DataType::Int64);
        let receiving_key = edge.destination.receive_mapping[0].1;
        assert_eq!(
            plan.fragments()[stage].values()[&receiving_key]
                .ty
                .data_type(),
            &DataType::Int64
        );
        if ordinal == 0 {
            let ValueOrigin::Expr { node, .. } = key.origin else {
                panic!("the integer input must be converted at the sender");
            };
            assert_eq!(node, sender.root());
            let projection = &sender.nodes()[&node];
            assert!(matches!(&projection.kind, NodeKind::Project { .. }));
            let before_cast = &sender.nodes()[&projection.inputs[0]];
            assert_eq!(
                sender.values()[&before_cast.output.columns[0]]
                    .ty
                    .data_type(),
                &DataType::Int32
            );
        }
    }
}

#[test]
fn values_enters_the_scan_union_through_a_single_copy_round_robin_edge() {
    let compiled = sql_source_with_declared_fields(
        "SELECT k FROM t UNION ALL VALUES ROW(7)",
        &[Field::new("k", DataType::Int32, false)],
    );
    let plan = compiled.plan();
    let round_robin: Vec<_> = plan
        .edges()
        .values()
        .filter(|edge| {
            edge.partitioning.source == Distribution::RoundRobin
                && edge.partitioning.destination == Distribution::RoundRobin
        })
        .collect();
    let [edge] = round_robin.as_slice() else {
        panic!("one singleton VALUES producer must use a RoundRobin edge");
    };
    assert_eq!(
        edge.partitioning.source_multiplicity,
        RowMultiplicity::SingleCopy
    );
    assert_eq!(
        edge.partitioning.destination_multiplicity,
        RowMultiplicity::SingleCopy
    );
    let source = &plan.fragments()[&edge.source.fragment];
    assert_eq!(source.stage_placement(), &StagePlacementPolicy::Singleton);
    assert!(
        source
            .nodes()
            .values()
            .any(|node| { matches!(&node.kind, NodeKind::Values { .. }) })
    );

    let destination = &plan.fragments()[&edge.destination.fragment];
    assert_eq!(
        destination.stage_placement(),
        &StagePlacementPolicy::ScanWork
    );
    assert!(destination.nodes().values().any(|node| {
        matches!(
            &node.kind,
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                ..
            }
        ) && node.inputs.contains(&edge.destination.node)
    }));
}
