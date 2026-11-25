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
use novarocks_physical_plan::{Distribution, Edge, EdgeKind, NodeKind, StagePlacementPolicy};
use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError, StructuralWaitCycle};
use novarocks_sql_testkit::{sql_source_with_declared_fields, try_sql_source_with_declared_fields};

fn fixture_fields() -> [Field; 1] {
    [Field::new("k", DataType::Int64, false)]
}

fn cte_edges(plan: &novarocks_physical_plan::PhysicalPlan) -> impl Iterator<Item = &Edge> {
    plan.edges()
        .values()
        .filter(|edge| edge.kind == EdgeKind::CteMulticast)
}

#[test]
fn a_retained_projection_keeps_the_limit_gather_after_the_cte_receiver() {
    let compiled = sql_source_with_declared_fields(
        "WITH c AS (SELECT k FROM t)
         SELECT k FROM (SELECT k FROM c LIMIT 1) AS limited UNION ALL SELECT k FROM c",
        &fixture_fields(),
    );
    let plan = compiled.plan();
    let edges = cte_edges(plan).collect::<Vec<_>>();
    let [limited, direct] = edges.as_slice() else {
        panic!("expected a CTE edge for each UNION ALL arm");
    };
    assert_eq!(limited.source.fragment, direct.source.fragment);
    assert_ne!(limited.destination.fragment, direct.destination.fragment);
    for edge in [limited, direct] {
        assert_eq!(edge.partitioning.source, Distribution::RoundRobin);
        assert_eq!(edge.partitioning.destination, Distribution::RoundRobin);
    }

    let producer = limited.source.fragment;
    let consumer = &plan.fragments()[&limited.destination.fragment];
    assert!(matches!(
        consumer.stage_placement(),
        StagePlacementPolicy::FollowSources { sources } if sources.as_ref() == [producer]
    ));
    let projection = &consumer.nodes()[&consumer.root()];
    assert!(matches!(&projection.kind, NodeKind::Project { .. }));
    assert_eq!(projection.inputs.as_ref(), &[limited.destination.node]);
    let gather = plan
        .edges()
        .values()
        .find(|edge| edge.kind == EdgeKind::Stream && edge.source.fragment == consumer.id())
        .expect("the retained projection sends an ordinary exchange to the limit");
    assert_eq!(gather.partitioning.source, Distribution::Singleton);
    assert_eq!(gather.partitioning.destination, Distribution::Singleton);
    assert_eq!(gather.destination.fragment, direct.destination.fragment);
    assert_ne!(gather.destination.node, direct.destination.node);
    let limit_stage = &plan.fragments()[&gather.destination.fragment];
    assert_eq!(
        limit_stage.stage_placement(),
        &StagePlacementPolicy::Singleton
    );
    assert!(limit_stage.nodes().values().any(|node| {
        matches!(&node.kind, NodeKind::Limit { .. })
            && node.inputs.as_ref() == [gather.destination.node]
    }));
}

#[test]
fn a_filter_stays_before_the_gather_and_follows_the_cte_width() {
    let compiled = sql_source_with_declared_fields(
        "WITH c AS (SELECT k FROM t)
         SELECT k FROM (SELECT k FROM c WHERE k > 7 LIMIT 1) AS limited UNION ALL SELECT k FROM c",
        &fixture_fields(),
    );
    let plan = compiled.plan();
    let (edge, consumer) = cte_edges(plan)
        .find_map(|edge| {
            let consumer = &plan.fragments()[&edge.destination.fragment];
            consumer
                .nodes()
                .values()
                .any(|node| {
                    matches!(&node.kind, NodeKind::Filter { .. })
                        && node.inputs.contains(&edge.destination.node)
                })
                .then_some((edge, consumer))
        })
        .expect("one CTE receiver feeds the retained filter");
    assert_eq!(edge.partitioning.source, Distribution::RoundRobin);
    assert_eq!(edge.partitioning.destination, Distribution::RoundRobin);
    assert!(matches!(
        consumer.stage_placement(),
        StagePlacementPolicy::FollowSources { sources } if sources.as_ref() == [edge.source.fragment]
    ));
    assert!(plan.edges().values().any(|outbound| {
        outbound.kind == EdgeKind::Stream
            && outbound.source.fragment == consumer.id()
            && outbound.partitioning.destination == Distribution::Singleton
    }));
}

#[test]
fn a_partitioned_semi_join_hashes_directly_from_the_cte_producer() {
    let compiled = sql_source_with_declared_fields(
        "WITH c AS (SELECT k FROM t)
         SELECT k FROM c WHERE k IN (SELECT k FROM t) UNION ALL SELECT k FROM c",
        &fixture_fields(),
    );
    let plan = compiled.plan();
    let edges = cte_edges(plan).collect::<Vec<_>>();
    let [hashed, direct] = edges.as_slice() else {
        panic!("expected a CTE edge for each UNION ALL arm");
    };
    assert_eq!(hashed.source.fragment, direct.source.fragment);
    assert_eq!(hashed.destination.fragment, direct.destination.fragment);
    assert_ne!(hashed.destination.node, direct.destination.node);
    assert_eq!(direct.partitioning.source, Distribution::RoundRobin);
    assert_eq!(direct.partitioning.destination, Distribution::RoundRobin);

    let (
        Distribution::Hash {
            keys: sent_keys,
            scheme: sent_scheme,
        },
        Distribution::Hash {
            keys: received_keys,
            scheme: received_scheme,
        },
    ) = (
        &hashed.partitioning.source,
        &hashed.partitioning.destination,
    )
    else {
        panic!("the probe hashes directly from the CTE producer");
    };
    assert_eq!(sent_scheme, received_scheme);
    let [(sent, received)] = hashed.destination.receive_mapping.as_ref() else {
        panic!("expected one imported join key");
    };
    assert_eq!(sent_keys.as_ref(), &[*sent]);
    assert_eq!(received_keys.as_ref(), &[*received]);

    let consumer = &plan.fragments()[&hashed.destination.fragment];
    assert!(consumer.nodes().values().any(|node| {
        matches!(&node.kind, NodeKind::HashJoin { .. })
            && node.inputs.first() == Some(&hashed.destination.node)
    }));
}

#[test]
fn an_independent_join_can_broadcast_a_reused_small_cte_directly() {
    let compiled = sql_source_with_declared_fields(
        "WITH c AS (SELECT k FROM t LIMIT 1)
         SELECT c.k FROM t AS probe LEFT JOIN c ON probe.k IS NOT DISTINCT FROM c.k
         UNION ALL SELECT k FROM c",
        &fixture_fields(),
    );
    let plan = compiled.plan();
    let edge = cte_edges(plan)
        .find(|edge| edge.partitioning.destination == Distribution::Broadcast)
        .expect("the adjacent broadcast belongs to the small CTE's branch");
    let consumer = &plan.fragments()[&edge.destination.fragment];
    assert!(consumer.nodes().values().any(|node| {
        matches!(&node.kind, NodeKind::HashJoin { .. })
            && node.inputs.get(1) == Some(&edge.destination.node)
    }));
}

#[test]
fn a_shared_cte_self_join_has_a_structural_wait_cycle() {
    let error = try_sql_source_with_declared_fields(
        "WITH c AS (SELECT k FROM t) SELECT l.k FROM c AS l JOIN c AS r ON l.k = r.k",
        &fixture_fields(),
    )
    .unwrap_err();
    let SqlCompileProgressError::Compile(SqlCompileError::CompilationSource(cause)) = error else {
        panic!("expected a typed structural wait cycle: {error:?}");
    };
    assert!(cause.downcast_ref::<StructuralWaitCycle>().is_some());
}

#[test]
fn a_single_reference_is_inlined() {
    let compiled = sql_source_with_declared_fields(
        "WITH c AS (SELECT k FROM t) SELECT k FROM c",
        &fixture_fields(),
    );
    assert!(cte_edges(compiled.plan()).next().is_none());
}
