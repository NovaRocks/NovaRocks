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

//! Native kernel-instance projection for fragment submission.
//!
//! The execution kernel prepares one fragment instance from a small set of
//! facts: its query, its kernel key, its instance ordinal, its initial scan
//! ranges, its inbound sender counts, its parallelism, its query options, and
//! the outbound edge serving each static sink branch. A task creation never
//! carries those as one per-instance copy. The backend that wins a task's
//! creation projects each of them from the single owner of that fact, in
//! [`project_task_instance`], so no two copies exist that could disagree and
//! none has to be checked against another.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution::runtime::task::{
    ExchangeInputAssignment, ExchangeInputAssignments, InstanceOrdinal, TaskKey,
};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::{FragmentNodeId, FragmentSinkKind};
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks as proto;
use novarocks_types::{BackendProcessId, QueryId};

/// The immutable Execution facts of one fragment instance.
#[derive(Debug)]
pub struct NativeTaskInstanceInput {
    pub query_id: QueryId,
    pub backend_process_id: BackendProcessId,
    pub task_key: TaskKey,
    pub instance_ordinal: InstanceOrdinal,
    pub query_options: QueryOptions,
    pub pipeline_dop: NonZeroUsize,
    pub raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    pub exchange_inputs: ExchangeInputAssignments,
    pub typed_result_sink: bool,
    /// In static sink/route order, the outbound topology edge whose
    /// destination set serves that sink or route.
    pub sink_edge_ids: Vec<u32>,
}

/// Where a task's assignment lives on the wire, so a refusal names the field
/// the frontend actually sent.
fn assignment_path() -> FieldPath {
    FieldPath::root("creation_metadata").field("assignment")
}

/// The wire path of a task's sink edge bindings, for refusals raised while the
/// kernel binds them to the static sink.
pub(crate) fn task_sink_edge_ids_path() -> FieldPath {
    assignment_path().field("sink_edge_ids")
}

/// Projects one task's kernel instance from the single owner of each fact.
///
/// | Kernel fact | Owner |
/// |---|---|
/// | query id | the exact query execution the task identity names |
/// | backend process id | the exact process the admitted task identity names |
/// | fragment instance id | the descriptor's kernel key |
/// | backend num | the assignment's instance ordinal |
/// | per-node scan ranges | the assignment's initial scan ranges |
/// | per-exchange sender counts | the descriptor's inbound topology |
/// | pipeline parallelism | the descriptor |
/// | query options | the established participation |
/// | typed result sink | the validated static sink |
/// | sink edge ids | the assignment, in static sink order |
///
/// Nothing here defaults a fact that its owner did not supply. The instance
/// ordinal must be representable by the kernel, every initial scan node must
/// appear once with exactly the ranges it was assigned -- an empty list stays
/// an empty list -- and every scan range must be one the kernel can read.
/// Whether the static plan has the scan nodes and sink branches the
/// assignment names is proved later, by the plan decoder, against this value.
pub fn project_task_instance(
    descriptor: &TaskDescriptor,
    assignment: proto::TaskAssignment,
    query_options: QueryOptions,
    sink_kind: FragmentSinkKind,
) -> Result<NativeTaskInstanceInput, ProtocolError> {
    let path = assignment_path();
    let ordinal = i32::try_from(assignment.instance_ordinal).map_err(|_| {
        error(
            path.clone().field("instance_ordinal"),
            ProtocolErrorKind::OutOfRange,
            format!(
                "instance ordinal {} exceeds the kernel's instance width",
                assignment.instance_ordinal
            ),
        )
    })?;
    let instance_ordinal = InstanceOrdinal::try_new(ordinal).map_err(|detail| {
        error(
            path.clone().field("instance_ordinal"),
            ProtocolErrorKind::InvalidValue,
            detail.to_string(),
        )
    })?;

    let mut raw_scan_ranges = BTreeMap::new();
    for (index, node) in assignment.initial_scan_ranges.into_iter().enumerate() {
        let node_path = path.clone().field("initial_scan_ranges").index(index);
        let mut ranges = Vec::with_capacity(node.ranges.len());
        for (range_index, range) in node.ranges.iter().enumerate() {
            ranges.push(decode_scan_range_params_at(
                range,
                node_path.clone().field("ranges").index(range_index),
            )?);
        }
        if raw_scan_ranges
            .insert(FragmentNodeId::new(node.plan_node_id), ranges)
            .is_some()
        {
            return Err(error(
                node_path.field("plan_node_id"),
                ProtocolErrorKind::DuplicateField,
                format!(
                    "initial scan node {} is assigned more than once",
                    node.plan_node_id
                ),
            ));
        }
    }

    // The complete inbound source set is the sender count; the topology froze
    // it once, so there is no second count to reconcile it with.
    let exchange_inputs = descriptor
        .topology()
        .inbound()
        .iter()
        .map(|inbound| {
            let senders = NonZeroUsize::new(inbound.sources().len())
                .expect("a validated inbound exchange node has sources");
            (inbound.node_id(), ExchangeInputAssignment::new(senders))
        })
        .collect::<BTreeMap<_, _>>();

    let identity = descriptor.identity();
    Ok(NativeTaskInstanceInput {
        query_id: identity.query_execution_id().query_id(),
        backend_process_id: identity.backend_process_id(),
        task_key: TaskKey::new(descriptor.task_key()),
        instance_ordinal,
        query_options,
        pipeline_dop: descriptor.pipeline_dop(),
        raw_scan_ranges,
        exchange_inputs: ExchangeInputAssignments::new(exchange_inputs),
        typed_result_sink: sink_kind == FragmentSinkKind::Result,
        sink_edge_ids: assignment.sink_edge_ids,
    })
}

/// Decodes one native scan-range payload outside a complete task assignment.
///
/// This is used by role-local fixtures which construct otherwise validated
/// fragment inputs. A task's own initial scan ranges are decoded by
/// [`project_task_instance`], so the scan node and range index remain in the
/// protocol error path.
pub fn decode_scan_range_params(
    src: &proto::ScanRangeParams,
) -> Result<ScanRangeParams, ProtocolError> {
    decode_scan_range_params_at(src, FieldPath::root("scan_ranges"))
}

fn decode_scan_range_params_at(
    src: &proto::ScanRangeParams,
    path: FieldPath,
) -> Result<ScanRangeParams, ProtocolError> {
    let range = src.range.as_ref().ok_or_else(|| {
        error(
            path.clone().field("range"),
            ProtocolErrorKind::MissingField,
            "native ScanRangeParams requires range",
        )
    })?;
    range.kind.as_ref().ok_or_else(|| {
        error(
            path.clone().field("range").field("kind"),
            ProtocolErrorKind::MissingField,
            "native ScanRange requires kind",
        )
    })?;
    ScanRangeParams::parse(src.clone())
        .map_err(|parse_error| error(path, ProtocolErrorKind::InvalidValue, parse_error.detail()))
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

#[cfg(test)]
mod tests {
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
}
