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

use std::num::{NonZeroU32, NonZeroUsize};

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution_contract::task_execution::descriptor::{
    DataStreamPartitionType, ExchangeDestination, ExchangeEdge, ExchangeInbound,
    ExchangeSource, ExchangeTopology, RuntimeEndpoint, TaskDescriptor,
};
use novarocks_execution_contract::task_execution::domain::ExchangeEdgeId;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_execution_contract::{FragmentNodeId, FragmentSinkKind};
use novarocks_proto_codec::ProtocolErrorKind;
use novarocks_proto_models::novarocks;
use novarocks_types::UniqueId;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};

use super::project_task_instance;

fn task(stage: u32, task: u32) -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(7, 8), AttemptId::new(1).expect("attempt"))
            .expect("execution"),
        StageId::new(stage).expect("stage"),
        TaskId::new(task).expect("task"),
        BackendProcessId::new_v7(),
    )
}

/// A consumer of two inbound nodes and the producer of one outbound edge.
fn descriptor() -> TaskDescriptor {
    let inbound = |node: i32, sources: u32| {
        ExchangeInbound::try_new(
            FragmentNodeId::new(node),
            (0..sources)
                .map(|ordinal| {
                    ExchangeSource::new(
                        task(1, ordinal + 1),
                        UniqueId::new(i64::from(node), i64::from(ordinal)),
                        ordinal,
                    )
                })
                .collect(),
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
        )
        .expect("legal inbound")
    };
    let edge = ExchangeEdge::try_new(
        ExchangeEdgeId::new(5).expect("edge"),
        FragmentNodeId::new(30),
        DataStreamPartitionType::Unpartitioned,
        vec![ExchangeDestination::new(
            task(3, 1),
            UniqueId::new(30, 1),
            RuntimeEndpoint::new("be.local", 8060).expect("endpoint"),
            FragmentNodeId::new(30),
        )],
        1,
        NonZeroU32::new(4).expect("senders"),
    )
    .expect("legal edge");
    TaskDescriptor::try_new(
        task(2, 1),
        UniqueId::new(9, 10),
        NonZeroUsize::new(3).expect("dop"),
        Vec::new(),
        ExchangeTopology::try_new(vec![edge], vec![inbound(20, 2), inbound(21, 1)])
            .expect("legal topology"),
    )
    .expect("legal descriptor")
}

fn file_range() -> novarocks::ScanRangeParams {
    novarocks::ScanRangeParams {
        range: Some(novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::File(
                novarocks::FileScanRange {
                    file_format: "PARQUET".into(),
                    full_path: Some("s3://bucket/data.parquet".into()),
                    length: 16,
                    file_length: 128,
                    ..Default::default()
                },
            )),
        }),
        ..Default::default()
    }
}

fn assignment() -> novarocks::TaskAssignment {
    novarocks::TaskAssignment {
        instance_ordinal: 4,
        initial_scan_ranges: vec![
            novarocks::TaskScanRanges {
                plan_node_id: 1,
                ranges: vec![file_range()],
            },
            novarocks::TaskScanRanges {
                plan_node_id: 2,
                ranges: Vec::new(),
            },
        ],
        sink_edge_ids: vec![5],
    }
}

#[test]
fn a_task_instance_is_projected_from_each_fact_owner() {
    let descriptor = descriptor();
    let participation_options = QueryOptions {
        pipeline_dop: Some(8),
        batch_size: Some(1024),
        ..QueryOptions::default()
    };
    let instance = project_task_instance(
        &descriptor,
        assignment(),
        participation_options,
        FragmentSinkKind::DataStream,
    )
    .expect("every owner supplied its fact");

    assert_eq!(
        instance.query_id,
        QueryId::new(7, 8),
        "the identity's query"
    );
    assert_eq!(
        instance.backend_process_id,
        descriptor.identity().backend_process_id(),
        "the admitted task's exact backend process"
    );
    assert_eq!(
        instance.task_key.get(),
        UniqueId::new(9, 10),
        "the descriptor's kernel key"
    );
    assert_eq!(
        instance.instance_ordinal.get(),
        4,
        "the assignment's ordinal"
    );
    assert_eq!(
        instance.pipeline_dop.get(),
        3,
        "the descriptor's parallelism, not the participation's"
    );
    assert_eq!(
        instance.query_options.pipeline_dop(),
        Some(8),
        "the participation's options are taken as installed"
    );
    assert_eq!(instance.query_options.batch_size(), Some(1024));
    assert_eq!(
        instance
            .raw_scan_ranges
            .iter()
            .map(|(node, ranges)| (node.get(), ranges.len()))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 0)],
        "an initial scan node with no ranges stays an explicit empty assignment"
    );
    assert_eq!(
        instance
            .exchange_inputs
            .iter()
            .map(|(node, input)| (node.get(), input.sender_count().get()))
            .collect::<Vec<_>>(),
        vec![(20, 2), (21, 1)],
        "each inbound node's sender count is its complete frozen source set"
    );
    assert!(!instance.typed_result_sink);
    assert_eq!(instance.sink_edge_ids, vec![5]);

    let root = project_task_instance(
        &descriptor,
        assignment(),
        QueryOptions::default(),
        FragmentSinkKind::Result,
    )
    .expect("every owner supplied its fact");
    assert!(
        root.typed_result_sink,
        "only a validated result sink is typed"
    );
}

#[test]
fn an_initial_scan_range_refusal_names_its_assignment_path() {
    let mut malformed = assignment();
    malformed.initial_scan_ranges[1]
        .ranges
        .push(novarocks::ScanRangeParams::default());
    let error = project_task_instance(
        &descriptor(),
        malformed,
        QueryOptions::default(),
        FragmentSinkKind::Noop,
    )
    .expect_err("a scan range without a range is refused");
    assert_eq!(
        error.to_string(),
        "native protocol error at creation_metadata.assignment.initial_scan_ranges[1].ranges[0].range (missing field): native ScanRangeParams requires range"
    );
}

#[test]
fn a_repeated_initial_scan_node_is_refused_rather_than_overwritten() {
    let mut repeated = assignment();
    repeated.initial_scan_ranges[1].plan_node_id = 1;
    let error = project_task_instance(
        &descriptor(),
        repeated,
        QueryOptions::default(),
        FragmentSinkKind::Noop,
    )
    .expect_err("one node cannot own two initial assignments");
    assert_eq!(error.kind(), &ProtocolErrorKind::DuplicateField);
}
