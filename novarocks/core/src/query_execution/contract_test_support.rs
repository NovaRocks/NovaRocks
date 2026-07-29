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

//! Feature-gated cross-crate contract fixtures.
//!
//! This module is absent from default and compat production dependency graphs.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatchOptions;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::query_execution::backend::{BackendTopologySnapshot, LiveBackendTarget};
use crate::query_execution::cancellation::{QueryCancellationSource, QueryCancellationView};
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
    DistributedQueryOutcome, DistributedQueryRequest,
    build_distributed_query_request_with_execution,
};
use crate::query_execution::fragment_transport::FetchedQueryBatch;
use crate::query_execution::preparation::{
    PreparedFragmentRole, prepared_fragment_set_for_test,
    prepared_fragment_set_with_runtime_filter_for_test,
};
use crate::query_execution::request_context::QueryExecutionContext;
use crate::query_execution::write::NativeExecutionReport;
use crate::runtime::query_options::QueryOptions;
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};

fn fixture_execution(
    backends: &[(usize, SocketAddr)],
    cancellation: QueryCancellationView,
) -> QueryExecutionContext {
    fixture_execution_with_deadline(backends, None, cancellation)
}

fn fixture_execution_with_deadline(
    backends: &[(usize, SocketAddr)],
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
) -> QueryExecutionContext {
    QueryExecutionContext::new(
        crate::common::app_config::ClusterRole::AllInOne,
        BackendTopologySnapshot::try_new(
            0,
            backends
                .iter()
                .map(|(backend_idx, endpoint)| LiveBackendTarget::new(*backend_idx, *endpoint, 0))
                .collect(),
        )
        .expect("contract fixture topology"),
        deadline,
        cancellation,
        crate::sql::optimizer::options::SessionOptimizerSettings::default(),
    )
}

pub struct ResultContractFixture {
    request: DistributedQueryRequest,
    backends: Vec<(usize, SocketAddr)>,
    result_chunk: Chunk,
    cancellation: QueryCancellationSource,
}

impl ResultContractFixture {
    pub fn backends(&self) -> &[(usize, SocketAddr)] {
        &self.backends
    }

    pub fn result_batch(&self) -> FetchedQueryBatch {
        FetchedQueryBatch::new(self.result_chunk.clone())
    }

    pub fn cancellation_source(&self) -> QueryCancellationSource {
        self.cancellation.clone()
    }

    pub fn failed_fragment_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 41, lo: 73 },
            crate::common::types::UniqueId {
                hi: 41,
                lo: i64::from(11_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 1,
                message: "contract native failure".to_string(),
            },
            None,
        )
    }

    pub fn successful_fragment_report_proto(&self) -> crate::proto::novarocks::ExecStatusReport {
        crate::proto::novarocks::ExecStatusReport {
            query_id: Some(crate::proto::common::UniqueId { hi: 41, lo: 73 }),
            fragment_instance_id: Some(crate::proto::common::UniqueId {
                hi: 41,
                lo: i64::from(11_u32) << 16,
            }),
            backend_num: 0,
            done: true,
            status: Some(crate::proto::common::Status {
                code: 0,
                message: String::new(),
            }),
            ..Default::default()
        }
    }

    pub fn into_request(self) -> DistributedQueryRequest {
        self.request
    }
}

pub fn non_empty_result_contract_fixture() -> ResultContractFixture {
    let backends = vec![
        (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031)),
        (8, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19032)),
    ];
    let topology = BackendTopologySnapshot::try_new(
        0,
        backends
            .iter()
            .map(|(backend_idx, endpoint)| LiveBackendTarget::new(*backend_idx, *endpoint, 0))
            .collect(),
    )
    .expect("contract fixture topology");
    non_empty_result_contract_fixture_with_topology(topology)
}

/// Builds the result fixture with the exact topology captured by the caller.
/// Frontend contract tests use this to prove that submission consumes the
/// same snapshot observed at statement admission.
pub fn non_empty_result_contract_fixture_with_topology(
    topology: BackendTopologySnapshot,
) -> ResultContractFixture {
    let edge = FragmentEdge {
        source_fragment_id: 11,
        target_fragment_id: 19,
        target_exchange_node_id: 190,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    };
    let prepared = prepared_fragment_set_for_test(
        vec![
            (11, PreparedFragmentRole::NonTerminal, Vec::new()),
            (19, PreparedFragmentRole::Result, Vec::new()),
        ],
        vec![11, 19],
        19,
        vec![edge],
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 11,
                ..Default::default()
            },
            crate::proto::plan::PlanFragment {
                fragment_id: 19,
                ..Default::default()
            },
        ])
        .expect("contract fixture native bundle");
    let backends = topology
        .targets()
        .iter()
        .map(|target| (target.backend_idx(), target.endpoint()))
        .collect::<Vec<_>>();
    let cancellation = QueryCancellationSource::new();
    let execution = QueryExecutionContext::new(
        crate::common::app_config::ClusterRole::AllInOne,
        topology,
        None,
        cancellation.view(),
        crate::sql::optimizer::options::SessionOptimizerSettings::default(),
    );
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(2),
            query_timeout: Some(5),
            ..Default::default()
        }),
        DistributedQueryIntent::Result,
        &execution,
    )
    .expect("contract fixture request");
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("one-row zero-column contract batch");
    let result_chunk = Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
        .expect("contract result chunk");
    ResultContractFixture {
        request,
        backends,
        result_chunk,
        cancellation,
    }
}

/// A two-fragment contract fixture with a sealed producer/consumer membership
/// channel. It exercises schedule-bound compilation without exposing the
/// runtime-filter graph or compiler DTO to the frontend crate.
pub fn non_empty_runtime_filter_contract_fixture() -> ResultContractFixture {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity,
        NullSemantics, PlanFragmentId, PlanNodeId, ReductionRequirement, RuntimeFilterLifecycle,
        RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirementData, PlanLocation,
        ProducerBindingTarget, ProducerRequirement, RuntimeFilterBindingRoleData,
        RuntimeFilterBindingSpecData, RuntimeFilterChannelSpec, RuntimeFilterGraph,
    };
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};

    let edge = FragmentEdge {
        source_fragment_id: 11,
        target_fragment_id: 19,
        target_exchange_node_id: 190,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    };
    let channel_id = ChannelId::new(1);
    let witness = CoverageWitnessId::new(1);
    let expression = || TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int64,
        nullable: false,
    };
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: Coverage::AllOf(vec![Coverage::Leaf(witness)]),
            terminal_coverage: Coverage::AllOf(vec![Coverage::Leaf(witness)]),
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 2_000,
                max_retries: 2,
            },
        })
        .expect("unique contract runtime-filter channel");
    graph
        .insert_binding(RuntimeFilterBindingSpecData {
            binding_id: BindingId::new(1),
            channel_id,
            coverage_witness_id: Some(witness),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(11),
                node_id: PlanNodeId::new(110),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                contribution_kinds: BTreeSet::from([
                    ContributionKind::FinalDomainShard,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                ),
                target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        })
        .expect("unique contract producer binding");
    graph
        .insert_binding(RuntimeFilterBindingSpecData {
            binding_id: BindingId::new(2),
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(19),
                node_id: PlanNodeId::new(190),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation: ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                target: ConsumerBindingTarget::DirectInput { input_ordinal: 0 },
            }),
        })
        .expect("unique contract consumer binding");
    let prepared = prepared_fragment_set_with_runtime_filter_for_test(
        vec![
            (
                11,
                PreparedFragmentRole::NonTerminal,
                vec![(
                    1_100,
                    vec![
                        crate::runtime::scan_range::ScanRangeParams::starrocks_tablet(1, 1, 1)
                            .expect("first contract scan range"),
                        crate::runtime::scan_range::ScanRangeParams::starrocks_tablet(2, 1, 1)
                            .expect("second contract scan range"),
                    ],
                )],
            ),
            (19, PreparedFragmentRole::Result, Vec::new()),
        ],
        vec![11, 19],
        19,
        vec![edge],
        graph,
        Default::default(),
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 11,
                ..Default::default()
            },
            crate::proto::plan::PlanFragment {
                fragment_id: 19,
                ..Default::default()
            },
        ])
        .expect("contract fixture native bundle");
    let backends = vec![
        (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031)),
        (8, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19032)),
    ];
    let cancellation = QueryCancellationSource::new();
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(2),
            query_timeout: Some(5),
            ..Default::default()
        }),
        DistributedQueryIntent::Result,
        &fixture_execution(&backends, cancellation.view()),
    )
    .expect("runtime-filter contract fixture request");
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("one-row zero-column contract batch");
    let result_chunk = Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
        .expect("contract result chunk");
    ResultContractFixture {
        request,
        backends,
        result_chunk,
        cancellation,
    }
}

pub fn assert_result_outcome_preserved(
    outcome: DistributedQueryOutcome,
    expected_rows: usize,
) -> Result<(), DistributedQueryError> {
    let result = outcome.into_result()?.into_query_result();
    if result.row_count() != expected_rows {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "engine consumed {} rows from Result outcome, expected {expected_rows}",
                result.row_count()
            ),
        ));
    }
    Ok(())
}

pub struct WriteContractFixture {
    request: DistributedQueryRequest,
    backends: Vec<(usize, SocketAddr)>,
    cancellation: QueryCancellationSource,
}

impl WriteContractFixture {
    pub fn backends(&self) -> &[(usize, SocketAddr)] {
        &self.backends
    }

    pub fn cancellation_source(&self) -> QueryCancellationSource {
        self.cancellation.clone()
    }

    pub fn successful_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(23_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 0,
                message: String::new(),
            },
            None,
        )
    }

    pub fn failed_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(23_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 1,
                message: "contract writer failure".to_string(),
            },
            None,
        )
    }

    pub fn wrong_backend_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test_with_write_metadata(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(23_u32) << 16,
            },
            99,
            true,
        )
    }

    pub fn conflicting_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test_with_write_metadata(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(23_u32) << 16,
            },
            0,
            true,
        )
    }

    pub fn successful_non_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(22_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 0,
                message: String::new(),
            },
            None,
        )
    }

    pub fn failed_non_writer_report(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(22_u32) << 16,
            },
            0,
            crate::proto::common::Status {
                code: 1,
                message: "contract producer failure".to_string(),
            },
            None,
        )
    }

    pub fn non_writer_report_with_write_metadata(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test_with_write_metadata(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(22_u32) << 16,
            },
            0,
            true,
        )
    }

    pub fn nonfinal_non_writer_report_with_write_metadata(&self) -> NativeExecutionReport {
        NativeExecutionReport::for_contract_test_with_write_metadata(
            crate::common::types::UniqueId { hi: 51, lo: 91 },
            crate::common::types::UniqueId {
                hi: 51,
                lo: i64::from(22_u32) << 16,
            },
            0,
            false,
        )
    }

    pub fn into_request(self) -> DistributedQueryRequest {
        self.request
    }
}

pub fn non_empty_write_contract_fixture() -> WriteContractFixture {
    non_empty_write_contract_fixture_with_query_timeout_seconds(5)
}

pub fn non_empty_write_contract_fixture_with_query_timeout_seconds(
    query_timeout_seconds: i32,
) -> WriteContractFixture {
    let edge = FragmentEdge {
        source_fragment_id: 22,
        target_fragment_id: 23,
        target_exchange_node_id: 230,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    };
    let prepared = prepared_fragment_set_for_test(
        vec![
            (22, PreparedFragmentRole::NonTerminal, Vec::new()),
            (23, PreparedFragmentRole::TerminalWrite, Vec::new()),
        ],
        vec![22, 23],
        23,
        vec![edge],
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 22,
                ..Default::default()
            },
            crate::proto::plan::PlanFragment {
                fragment_id: 23,
                ..Default::default()
            },
        ])
        .expect("write contract native bundle");
    let backends = vec![(3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031))];
    let cancellation = QueryCancellationSource::new();
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(1),
            query_timeout: Some(query_timeout_seconds),
            ..Default::default()
        }),
        DistributedQueryIntent::Write,
        &fixture_execution_with_deadline(
            &backends,
            (query_timeout_seconds > 0)
                .then(|| Instant::now() + Duration::from_secs(query_timeout_seconds as u64)),
            cancellation.view(),
        ),
    )
    .expect("write contract request");
    WriteContractFixture {
        request,
        backends,
        cancellation,
    }
}

pub fn assert_write_outcome_preserved(
    outcome: DistributedQueryOutcome,
) -> Result<(), DistributedQueryError> {
    let (_, commit, abort) = outcome.into_write()?.into_parts();
    match (commit, abort) {
        (Some(commit), None) if commit.writers.len() == 1 => Ok(()),
        (None, Some(abort)) if !abort.reason.is_empty() => Ok(()),
        _ => Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "engine did not receive a non-empty commit or abort payload",
        )),
    }
}

pub fn assert_write_abort_reason(
    outcome: DistributedQueryOutcome,
    expected: &str,
) -> Result<(), DistributedQueryError> {
    let (_, commit, abort) = outcome.into_write()?.into_parts();
    if commit.is_some() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "write failure unexpectedly produced a commit payload",
        ));
    }
    let abort = abort.ok_or_else(|| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "write failure did not produce an abort payload",
        )
    })?;
    if !abort.reason.contains(expected) {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "write abort reason '{}' does not contain '{expected}'",
                abort.reason
            ),
        ));
    }
    Ok(())
}

pub struct ProfileContractFixture {
    request: DistributedQueryRequest,
    backends: Vec<(usize, SocketAddr)>,
    result_chunk: Chunk,
}

impl ProfileContractFixture {
    pub fn backends(&self) -> &[(usize, SocketAddr)] {
        &self.backends
    }

    pub fn result_batch(&self) -> FetchedQueryBatch {
        FetchedQueryBatch::new(self.result_chunk.clone())
    }

    pub fn fragment_profile_reports(&self) -> Vec<NativeExecutionReport> {
        vec![
            NativeExecutionReport::for_contract_test(
                crate::common::types::UniqueId { hi: 61, lo: 101 },
                crate::common::types::UniqueId {
                    hi: 61,
                    lo: i64::from(11_u32) << 16,
                },
                0,
                crate::proto::common::Status {
                    code: 0,
                    message: String::new(),
                },
                Some(
                    crate::runtime::profile::Profiler::new("contract-fragment-profile")
                        .to_native_tree(),
                ),
            ),
            NativeExecutionReport::for_contract_test(
                crate::common::types::UniqueId { hi: 61, lo: 101 },
                crate::common::types::UniqueId {
                    hi: 61,
                    lo: i64::from(19_u32) << 16,
                },
                1,
                crate::proto::common::Status {
                    code: 0,
                    message: String::new(),
                },
                Some(
                    crate::runtime::profile::Profiler::new("contract-root-profile")
                        .to_native_tree(),
                ),
            ),
        ]
    }

    pub fn fragment_final_reports_without_profiles(&self) -> Vec<NativeExecutionReport> {
        [11_u32, 19_u32]
            .into_iter()
            .enumerate()
            .map(|(backend_num, fragment_id)| {
                NativeExecutionReport::for_contract_test(
                    crate::common::types::UniqueId { hi: 61, lo: 101 },
                    crate::common::types::UniqueId {
                        hi: 61,
                        lo: i64::from(fragment_id) << 16,
                    },
                    backend_num as i32,
                    crate::proto::common::Status {
                        code: 0,
                        message: String::new(),
                    },
                    None,
                )
            })
            .collect()
    }

    pub fn into_request(self) -> DistributedQueryRequest {
        self.request
    }
}

pub fn non_empty_profile_contract_fixture() -> ProfileContractFixture {
    non_empty_profile_contract_fixture_with_query_timeout_seconds(5)
}

pub fn non_empty_profile_contract_fixture_with_query_timeout_seconds(
    query_timeout_seconds: i32,
) -> ProfileContractFixture {
    let edge = FragmentEdge {
        source_fragment_id: 11,
        target_fragment_id: 19,
        target_exchange_node_id: 190,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    };
    let prepared = prepared_fragment_set_for_test(
        vec![
            (11, PreparedFragmentRole::NonTerminal, Vec::new()),
            (19, PreparedFragmentRole::Result, Vec::new()),
        ],
        vec![11, 19],
        19,
        vec![edge],
    );
    let native_bundle =
        crate::protocol::native::encode::native_fragment_bundle_for_contract_test(vec![
            crate::proto::plan::PlanFragment {
                fragment_id: 11,
                ..Default::default()
            },
            crate::proto::plan::PlanFragment {
                fragment_id: 19,
                ..Default::default()
            },
        ])
        .expect("profile contract native bundle");
    let backends = vec![
        (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19031)),
        (8, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19032)),
    ];
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(2),
            query_timeout: Some(query_timeout_seconds),
            enable_profile: true,
            ..Default::default()
        }),
        DistributedQueryIntent::Profile,
        &fixture_execution_with_deadline(
            &backends,
            (query_timeout_seconds > 0)
                .then(|| Instant::now() + Duration::from_secs(query_timeout_seconds as u64)),
            QueryCancellationSource::new().view(),
        ),
    )
    .expect("profile contract request");
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("one-row zero-column profile contract batch");
    let result_chunk = Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
        .expect("profile contract result chunk");
    ProfileContractFixture {
        request,
        backends,
        result_chunk,
    }
}

pub fn assert_profile_outcome_preserved(
    outcome: DistributedQueryOutcome,
    expected_rows: usize,
) -> Result<(), DistributedQueryError> {
    let (result, profiles) = outcome.into_profile()?.into_parts();
    let profiles = profiles.into_profiles();
    let names = profiles
        .iter()
        .map(|profile| profile.root.name.as_str())
        .collect::<Vec<_>>();
    if result.row_count() != expected_rows
        || profiles.len() != 2
        || !names.contains(&"contract-fragment-profile")
        || !names.contains(&"contract-root-profile")
    {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "engine did not receive the expected non-empty Profile payload",
        ));
    }
    Ok(())
}
