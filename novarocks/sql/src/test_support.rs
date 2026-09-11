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

//! Feature-gated fixtures for consumers that need a complete, sealed SQL plan.
//!
//! This module is intentionally absent from the default public surface.  Its
//! fixtures are sealed through the same planner validation path as production
//! plans; it exposes neither a draft graph nor a mutation or sealing entrypoint.

use arrow::datatypes::DataType;
use std::num::NonZeroU64;

use crate::analysis::cte::CteId;
use crate::analysis::{ExprKind, OutputColumn, SubqueryKind, TypedExpr};
use crate::column_id::ColumnId;
use crate::common::{
    BinOp, JoinKind, LambdaParam, LiteralValue, ScanVariantColumn, UnOp, WindowBound, WindowFrame,
    WindowFrameType,
};
use crate::functions::FunctionVolatility;
use crate::plan_read::{DistributedPlan, SortItem};
use crate::planner::distributed::write::change_stream::{
    ChangeStreamRoute, ChangeStreamRouterSink, ChangeStreamWriteDagSpec, ChangeStreamWriteRouteSpec,
};
use crate::planner::distributed::write::contract::{
    ConnectorWriteInputBinding, test_support::simple_sql_write_plan_input,
};
use crate::planner::distributed::write::plan::{
    finalize_sql_change_stream_table_writer_finish_test_plan,
    finalize_sql_table_writer_finish_test_plan,
};
use crate::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, PartitionKind,
    PlanFragment,
};
use crate::planner::payload::PlanScanNode;
use crate::planner::payload::{
    AggregateCall, PlanAssertOneRowNode, PlanGenerateSeriesNode, PlanProjectNode, PlanSortNode,
    PlanValuesNode,
};
use crate::planner::physical::{
    AggMode, AggregateOutputLayout, JoinDistribution, PhysicalHashAggregateNode,
    PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanKind, PhysicalPlanStats,
    PhysicalTopNNode, PlannerConfidence, TopNPhase,
};
use crate::planner::table::{
    SqlMvTargetLocatorScan, SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter,
    SqlMvTargetStateScan, SqlScanKind, SqlTableVersionSelector, TableDef, test_sql_scan_source,
};
use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
    ConnectorWriteRouteId,
};

/// Closed fixture catalog for native encoder consumers.
///
/// New cases belong here only when an encoder assertion needs a distinct sealed
/// SQL shape. There is deliberately no caller-supplied draft or mutation hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeEncoderPlanFixture {
    Minimal,
    HashExchange,
    HashAggregate,
    ReconciledHashJoin,
    ChangeStreamRouter,
    DuplicateProject,
    TopNDuplicateProject,
    NestLoopJoin,
    AssertOneRow,
    Sort,
    PrunedConnectorScanStreamEdge,
    AggregateLayoutStreamEdge,
    LocalAverageStreamEdge,
    ZeroColumnStreamEdge,
}

/// Closed sealed scan shapes used by native encoder binding tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeScanFixture {
    ConnectorRead,
    PinnedFileSet,
    DeltaWithStaleUnprojectedColumn,
    DeltaForPreparedBinding,
    DeltaWithInvalidProjection,
    OrdinaryIcebergWithUnprojectedPayload,
    OrdinaryIcebergIdProjection,
    OrdinaryIcebergUnrestricted,
    OrdinaryIcebergAllColumns,
    OrdinaryIcebergWithRequiredPayload,
    OrdinaryIcebergWithIdEqualityPredicate,
    UnsupportedPredicate,
    RefreshSnapshot,
    FrozenSnapshotEleven,
    FrozenSnapshotTwelve,
    FrozenTimestamp,
    VersionSnapshotWithStaleOutput,
    RefreshMvTargetLocator,
    RefreshMvTargetState,
    MvTargetLocator,
    MvTargetState,
    VariantProjection,
    TargetLocatorProjection,
    TargetStateProjection,
    EqualityKeyHidden,
    EqualityKeyProjected,
    ProjectionMissingColumn,
    ProjectionTypeMismatch,
    ProjectionNullabilityMismatch,
    JoinRefreshCoalesce,
}

/// Confirm the closed MV scan fixture carries a tokenized `Data(Current)`
/// source. The source itself remains SQL-private; Core tests receive only this
/// immutable invariant.
pub fn native_mv_data_current_scan_is_tokenized() -> bool {
    matches!(
        test_sql_scan_source(SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        }),
        crate::planner::table::ScanSource::Sql(_)
    )
}

/// Copied scan-admission facts for Core fixture binding. This is intentionally
/// a snapshot, not a re-export of the tokenized SQL scan carrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeScanFixtureBinding {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub is_delta: bool,
}

/// Closed build-plan shapes for Core's native-fragment projection tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBuildFixture {
    BroadcastStream,
    RandomOtherStream,
    HashPartitionedStream,
    LimitOffsetStream,
    TopNSplitStream,
    CteMulticastStream,
    CteMulticastOrdering,
    RouterStream,
}

/// Closed stream shapes used by native plan-wire topology tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativePlanEncodingFixture {
    ReorderedSlots,
    LoweredSlots,
    ZeroColumns,
    GenerateSeries,
}

/// Closed sealed plans for Core execution-preparation tests. The malformed
/// result case is produced wholly inside SQL so Core never receives a draft or
/// mutation hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativePreparationFixture {
    ResultOutput,
    MissingResultOutput,
}

/// Closed sealed NCP-6 dataflow write shapes: `TableWriter` fragments gathering
/// into one Root `TableFinish` fragment.
///
/// These are the only fixtures downstream crates need for the dataflow write
/// plan; draft construction and the writer/finish builders stay SQL-private.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeWriteDataflowFixture {
    /// One writer fragment gathering into the Root finish fragment.
    SingleWriter,
    /// A change-stream router driving two route writer fragments, both
    /// gathering into the same Root finish fragment.
    ChangeStreamTwoWriters,
}

/// Build one sealed NCP-6 dataflow write plan through the production builder
/// and the production seal path.
/// The dense, query-local write target ordinal a fixture branch feeds.
fn write_target_ordinal_for_test(
    value: u32,
) -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
    novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(value)
        .expect("bounded write target ordinal")
}

pub fn native_write_dataflow_plan(
    fixture: NativeWriteDataflowFixture,
) -> Result<DistributedPlan, String> {
    match fixture {
        NativeWriteDataflowFixture::SingleWriter => native_single_table_writer_plan(),
        NativeWriteDataflowFixture::ChangeStreamTwoWriters => {
            native_change_stream_table_writer_plan()
        }
    }
}

fn native_single_table_writer_plan() -> Result<DistributedPlan, String> {
    let output_columns = vec![output_column(1, "order_id", DataType::Int64)];
    let draft = crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![PlanFragment {
            fragment_id: 0,
            root: values_node(0, 10, output_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        Some(0),
        Vec::new(),
        Default::default(),
    );
    finalize_sql_table_writer_finish_test_plan(
        draft,
        simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
    )
    .map_err(|error| format!("native table writer fixture must seal: {error}"))
}

fn native_change_stream_table_writer_plan() -> Result<DistributedPlan, String> {
    let output_columns = vec![
        OutputColumn {
            column_id: ColumnId(1),
            name: "__row_mutation_effect".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        },
        output_column(3, "delete_id", DataType::Int64),
        output_column(4, "replacement", DataType::Int64),
    ];
    let draft = crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![PlanFragment {
            fragment_id: 0,
            root: values_node(0, 10, output_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        Some(0),
        Vec::new(),
        Default::default(),
    );
    let route = |route_byte: u8,
                 write_target_ordinal: u32,
                 effects: Vec<ConnectorRowMutationEffect>,
                 input_ordinal: u32,
                 output_partition_ordinals: Vec<usize>| {
        ChangeStreamWriteRouteSpec {
            route_id: ConnectorWriteRouteId::from_bytes([route_byte; 32]),
            write_target_ordinal: write_target_ordinal_for_test(write_target_ordinal),
            accepted_effects: effects,
            input_ordinals: vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([route_byte.wrapping_add(2); 32]),
                input_ordinal,
            )],
            output_partition_ordinals,
            sink: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
        }
    };
    let dag = ChangeStreamWriteDagSpec {
        effect_output_ordinal: 0,
        routes: vec![
            route(7, 0, vec![ConnectorRowMutationEffect::Delete], 1, vec![1]),
            route(
                8,
                1,
                vec![ConnectorRowMutationEffect::Replace],
                2,
                Vec::new(),
            ),
        ],
    };
    finalize_sql_change_stream_table_writer_finish_test_plan(draft, dag)
        .map_err(|error| format!("native change-stream table writer fixture must seal: {error}"))
}

/// Build one complete preparation fixture. Each normal case is sealed through
/// the production validation path; the malformed case is a closed SQL-owned
/// negative fixture for Core's fail-closed output-contract assertion.
pub fn native_preparation_plan(
    fixture: NativePreparationFixture,
) -> Result<DistributedPlan, String> {
    match fixture {
        NativePreparationFixture::ResultOutput => native_preparation_result_plan(),
        NativePreparationFixture::MissingResultOutput => {
            let mut plan = native_preparation_result_plan()?;
            plan.remove_fragment_output_for_test(7);
            Ok(plan)
        }
    }
}

/// Build a sealed scan fixture without exporting a mutable planner draft.
pub fn native_scan_plan(fixture: NativeScanFixture) -> Result<DistributedPlan, String> {
    match fixture {
        NativeScanFixture::ConnectorRead => native_scan_fixture_plan(
            SqlScanKind::ConnectorRead,
            ordinary_iceberg_columns(),
            vec![output_column(1, "id", DataType::Int32)],
            Some(vec!["id".to_string()]),
            Vec::new(),
        ),
        NativeScanFixture::PinnedFileSet => native_scan_fixture_plan(
            SqlScanKind::PinnedFileSet,
            ordinary_iceberg_columns(),
            vec![output_column(1, "id", DataType::Int32)],
            Some(vec!["id".to_string()]),
            Vec::new(),
        ),
        NativeScanFixture::DeltaWithStaleUnprojectedColumn => native_delta_scan_plan(),
        NativeScanFixture::DeltaForPreparedBinding => native_prepared_delta_scan_plan(),
        NativeScanFixture::DeltaWithInvalidProjection => native_scan_fixture_plan(
            SqlScanKind::Delta {
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
            ordinary_iceberg_columns(),
            vec![output_column(1, "missing", DataType::Int32)],
            Some(vec!["missing".to_string()]),
            Vec::new(),
        ),
        NativeScanFixture::OrdinaryIcebergWithUnprojectedPayload => {
            native_ordinary_iceberg_scan_plan()
        }
        NativeScanFixture::OrdinaryIcebergIdProjection => {
            native_ordinary_iceberg_id_projection_scan_plan()
        }
        NativeScanFixture::OrdinaryIcebergUnrestricted => {
            native_ordinary_iceberg_unrestricted_scan_plan()
        }
        NativeScanFixture::OrdinaryIcebergAllColumns => {
            native_ordinary_iceberg_all_columns_scan_plan()
        }
        NativeScanFixture::OrdinaryIcebergWithRequiredPayload => {
            native_ordinary_iceberg_required_payload_scan_plan()
        }
        NativeScanFixture::OrdinaryIcebergWithIdEqualityPredicate => {
            native_ordinary_iceberg_id_equality_predicate_scan_plan()
        }
        NativeScanFixture::UnsupportedPredicate => native_unsupported_predicate_scan_plan(),
        NativeScanFixture::RefreshSnapshot => native_refresh_scan_plan(refresh_snapshot_source()),
        NativeScanFixture::FrozenSnapshotEleven => native_scan_fixture_plan(
            SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::Snapshot(11),
            },
            ordinary_iceberg_columns(),
            vec![output_column(1, "id", DataType::Int32)],
            Some(vec!["id".to_string()]),
            Vec::new(),
        ),
        NativeScanFixture::FrozenSnapshotTwelve => native_scan_fixture_plan(
            SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::Snapshot(12),
            },
            ordinary_iceberg_columns(),
            vec![output_column(1, "id", DataType::Int32)],
            Some(vec!["id".to_string()]),
            Vec::new(),
        ),
        NativeScanFixture::FrozenTimestamp => native_scan_fixture_plan(
            SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::TimestampMillis(1_704_067_200_000),
            },
            ordinary_iceberg_columns(),
            vec![output_column(1, "id", DataType::Int32)],
            Some(vec!["id".to_string()]),
            Vec::new(),
        ),
        NativeScanFixture::VersionSnapshotWithStaleOutput => {
            native_version_snapshot_with_stale_output_scan_plan()
        }
        NativeScanFixture::RefreshMvTargetLocator => {
            native_refresh_scan_plan(mv_target_locator_source("bound_order_id"))
        }
        NativeScanFixture::RefreshMvTargetState => {
            native_refresh_scan_plan(mv_target_state_source("bound_order_id"))
        }
        NativeScanFixture::MvTargetLocator => {
            native_basic_scan_plan(mv_target_locator_source("order_id"))
        }
        NativeScanFixture::MvTargetState => {
            native_basic_scan_plan(mv_target_state_source("order_id"))
        }
        NativeScanFixture::VariantProjection => native_variant_projection_scan_plan(),
        NativeScanFixture::TargetLocatorProjection => native_target_locator_projection_scan_plan(),
        NativeScanFixture::TargetStateProjection => native_target_state_projection_scan_plan(),
        NativeScanFixture::EqualityKeyHidden => native_equality_key_hidden_scan_plan(),
        NativeScanFixture::EqualityKeyProjected => native_equality_key_projected_scan_plan(),
        NativeScanFixture::ProjectionMissingColumn => {
            native_projection_mismatch_scan_plan("missing", DataType::Int32, false)
        }
        NativeScanFixture::ProjectionTypeMismatch => {
            native_projection_mismatch_scan_plan("id", DataType::Int64, false)
        }
        NativeScanFixture::ProjectionNullabilityMismatch => {
            native_projection_mismatch_scan_plan("id", DataType::Int32, true)
        }
        NativeScanFixture::JoinRefreshCoalesce => native_join_refresh_coalesce_plan(),
    }
}

/// Build two scan occurrences of the same fully-qualified source and the same
/// SQL binding token. The plan node identities are the only correct way to
/// distinguish the self-join occurrences.
pub fn native_self_join_scan_plan() -> Result<DistributedPlan, String> {
    let source = test_sql_scan_source(SqlScanKind::ConnectorRead);
    let make_scan = |node_id: i32, column_id: u32, alias: &str| DistributedNode {
        node_id,
        fragment_id: 0,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "orders".to_string(),
                columns: vec![column_def("id", DataType::Int64, false)],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: source.clone(),
            },
            alias: Some(alias.to_string()),
            columns: vec![output_column(column_id, "id", DataType::Int64)],
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
    };
    let output_columns = vec![
        output_column(1, "left_id", DataType::Int64),
        output_column(2, "right_id", DataType::Int64),
    ];
    let root = DistributedNode {
        node_id: 30,
        fragment_id: 0,
        tuple_ids: vec![30],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            make_scan(10, 1, "left_orders"),
            make_scan(20, 2, "right_orders"),
        ],
        stats: physical_stats(),
        payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: Vec::new(),
            other_condition: None,
            distribution: JoinDistribution::Unknown,
            execution_mode: None,
            build_runtime_filters: Vec::new(),
            output_columns: output_columns.clone(),
        })),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

/// Build a final scan plan carrying the marker emitted by the optimizer's MV
/// rewrite path. Downstream tests can obtain an opaque rewrite action only
/// through the sealed scan contract.
pub fn native_mv_rewritten_scan_plan() -> Result<DistributedPlan, String> {
    let plan = native_scan_fixture_plan(
        SqlScanKind::ConnectorRead,
        vec![column_def("id", DataType::Int64, false)],
        vec![output_column(1, "id", DataType::Int64)],
        None,
        Vec::new(),
    )?;
    let fragment = plan.fragments()[0].clone();
    let mut root = fragment.root;
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        unreachable!("MV rewrite fixture is a scan")
    };
    let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
    let occurrence =
        crate::planner::payload::SqlScanOccurrence::from_scan(source.binding, &scan.columns)
            .expect("MV rewrite fixture scan has an occurrence anchor");
    let publication = crate::compiler::SqlMvRewriteSelectionFacts::try_new_for_target(
        [7; 16],
        [9; 32],
        vec!["ice.db.orders".to_string()],
        "test_catalog.test_db.test_table".to_string(),
    )?;
    scan.mv_rewritten_from = Some(crate::planner::payload::MvRewriteSelection::selected(
        "mv_orders".to_string(),
        [7; 16],
        [9; 32],
        vec![(occurrence, 0)],
        publication.publication_inputs().to_vec(),
        publication.publication_target().clone(),
    ));
    seal_fixture_plan(vec![PlanFragment { root, ..fragment }])
}

/// Build a malformed final MV scan whose annotation lacks publication proof.
/// It exists only to prove that SQL sealing rejects the pre-UEA unverified
/// marker rather than exposing it as an executable rewrite action.
pub fn native_unverified_mv_rewritten_scan_plan() -> Result<DistributedPlan, String> {
    let plan = native_scan_fixture_plan(
        SqlScanKind::ConnectorRead,
        vec![column_def("id", DataType::Int64, false)],
        vec![output_column(1, "id", DataType::Int64)],
        None,
        Vec::new(),
    )?;
    let fragment = plan.fragments()[0].clone();
    let mut root = fragment.root;
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        unreachable!("MV rewrite fixture is a scan")
    };
    scan.mv_rewritten_from = Some(crate::planner::payload::MvRewriteSelection::unverified(
        "mv_orders".to_string(),
    ));
    seal_fixture_plan(vec![PlanFragment { root, ..fragment }])
}

/// Build the same final MV scan with an explicit optimizer-owned mapping from
/// pre-rewrite scan occurrences. This exists only for cross-crate contract
/// tests of self-join and mapping transplantation.
pub fn native_mv_rewritten_scan_plan_with_inputs(
    input_mapping: Vec<(crate::planning::query_execution::SqlScanOccurrence, usize)>,
) -> Result<DistributedPlan, String> {
    let plan = native_scan_fixture_plan(
        SqlScanKind::ConnectorRead,
        vec![column_def("id", DataType::Int64, false)],
        vec![output_column(1, "id", DataType::Int64)],
        None,
        Vec::new(),
    )?;
    let fragment = plan.fragments()[0].clone();
    let mut root = fragment.root;
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        unreachable!("MV rewrite fixture is a scan")
    };
    let publication = crate::compiler::SqlMvRewriteSelectionFacts::try_new_for_target(
        [7; 16],
        [9; 32],
        vec!["ice.db.orders".to_string()],
        "test_catalog.test_db.test_table".to_string(),
    )?;
    scan.mv_rewritten_from = Some(crate::planner::payload::MvRewriteSelection::selected(
        "mv_orders".to_string(),
        [7; 16],
        [9; 32],
        input_mapping,
        publication.publication_inputs().to_vec(),
        publication.publication_target().clone(),
    ));
    seal_fixture_plan(vec![PlanFragment { root, ..fragment }])
}

fn native_join_refresh_coalesce_plan() -> Result<DistributedPlan, String> {
    let allocator = crate::binding::SqlTableBindingAllocator::try_new_for_test(
        NonZeroU64::new(1).expect("fixture scope"),
    )?;
    let (optimized, _) = crate::planner::imv_rewrite::entrypoint::tests::tests_support::
        build_tokenized_join_refresh_coalesce_plan_for_lowering(allocator.scope());
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized)
        .map_err(|error| format!("join-refresh fixture physical plan: {error}"))?;
    crate::planner::pipeline::build_distributed_plan(physical)
        .map_err(|error| format!("join-refresh fixture distributed plan: {error}"))
}

/// Return the copied admission identity needed by Core's prepared-binding
/// fixture, without exposing a scan source, binding token, or planner graph.
pub fn native_scan_fixture_binding(plan: &DistributedPlan) -> Option<NativeScanFixtureBinding> {
    let scan = plan
        .fragments()
        .iter()
        .find_map(|fragment| match &fragment.root.payload {
            DistributedNodeKind::Scan(scan) => Some(scan),
            _ => None,
        })?;
    let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
    Some(NativeScanFixtureBinding {
        catalog: source.table.catalog.clone(),
        namespace: source.table.namespace.clone(),
        table: source.table.table.clone(),
        is_delta: matches!(source.kind, SqlScanKind::Delta { .. }),
    })
}

/// Return the analyzer table carrier from a sealed scan fixture. The fixture
/// keeps the planner table payload internal while Core receives the same opaque
/// resolved-table value it uses in ordinary prepared bindings.
pub fn native_scan_fixture_resolved_table(
    plan: &DistributedPlan,
    catalog: Option<&str>,
    database: &str,
) -> Option<crate::catalog::ResolvedAnalyzerTable> {
    plan.fragments()
        .iter()
        .find_map(|fragment| match &fragment.root.payload {
            DistributedNodeKind::Scan(scan) => {
                Some(crate::catalog::ResolvedAnalyzerTable::from_planner(
                    catalog,
                    database,
                    scan.table.clone(),
                ))
            }
            _ => None,
        })
}

/// Build a sealed topology fixture without exposing physical-plan or draft APIs.
pub fn native_build_plan(fixture: NativeBuildFixture) -> Result<DistributedPlan, String> {
    match fixture {
        NativeBuildFixture::BroadcastStream => native_broadcast_stream_plan(),
        NativeBuildFixture::RandomOtherStream => native_random_other_stream_plan(),
        NativeBuildFixture::HashPartitionedStream => native_hash_partitioned_stream_plan(),
        NativeBuildFixture::LimitOffsetStream => {
            native_stream_exchange_plan(ExchangeFlavor::LimitOffset {
                limit: Some(1),
                offset: Some(0),
            })
        }
        NativeBuildFixture::TopNSplitStream => {
            native_stream_exchange_plan(ExchangeFlavor::TopNSplit {
                items: Vec::new(),
                limit: Some(1),
                offset: Some(0),
            })
        }
        NativeBuildFixture::CteMulticastStream => native_cte_multicast_stream_plan(),
        NativeBuildFixture::CteMulticastOrdering => native_cte_multicast_ordering_plan(),
        NativeBuildFixture::RouterStream => native_router_stream_plan(),
    }
}

/// Build a sealed topology shape for native plan encoding without exposing a
/// distributed draft or physical node constructors to Core tests.
pub fn native_plan_encoding_plan(
    fixture: NativePlanEncodingFixture,
) -> Result<DistributedPlan, String> {
    match fixture {
        NativePlanEncodingFixture::ReorderedSlots => native_reordered_slots_stream_plan(),
        NativePlanEncodingFixture::LoweredSlots => native_lowered_slots_stream_plan(),
        NativePlanEncodingFixture::ZeroColumns => native_zero_columns_stream_plan(),
        NativePlanEncodingFixture::GenerateSeries => native_generate_series_stream_plan(),
    }
}

/// Build one complete plan used by native-encoder integration fixtures.
///
/// The returned plan has passed the production distributed-plan seal. Consumers
/// may inspect it only through [`crate::plan_read`].
pub fn native_encoder_plan(fixture: NativeEncoderPlanFixture) -> Result<DistributedPlan, String> {
    match fixture {
        NativeEncoderPlanFixture::Minimal => {
            crate::planner::distributed::native_encoder_test_fixture_plan()
        }
        NativeEncoderPlanFixture::HashExchange => native_hash_exchange_plan(),
        NativeEncoderPlanFixture::HashAggregate => native_hash_aggregate_plan(),
        NativeEncoderPlanFixture::ReconciledHashJoin => native_reconciled_hash_join_plan(),
        NativeEncoderPlanFixture::ChangeStreamRouter => native_change_stream_router_plan(),
        NativeEncoderPlanFixture::DuplicateProject => native_duplicate_project_plan(false),
        NativeEncoderPlanFixture::TopNDuplicateProject => native_duplicate_project_plan(true),
        NativeEncoderPlanFixture::NestLoopJoin => native_nest_loop_join_plan(),
        NativeEncoderPlanFixture::AssertOneRow => native_assert_one_row_plan(),
        NativeEncoderPlanFixture::Sort => native_sort_plan(),
        NativeEncoderPlanFixture::PrunedConnectorScanStreamEdge => {
            native_pruned_connector_scan_stream_edge_plan()
        }
        NativeEncoderPlanFixture::AggregateLayoutStreamEdge => {
            native_aggregate_layout_stream_edge_plan()
        }
        NativeEncoderPlanFixture::LocalAverageStreamEdge => native_local_average_stream_edge_plan(),
        NativeEncoderPlanFixture::ZeroColumnStreamEdge => native_zero_column_stream_edge_plan(),
    }
}

fn native_preparation_result_plan() -> Result<DistributedPlan, String> {
    let columns = vec![
        output_column(1, "a", DataType::Int64),
        OutputColumn {
            column_id: ColumnId(2),
            name: "b".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        },
    ];
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 7,
        root: values_node(7, 70, columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

/// Rust physical-plan variants expected by the native encoder. This avoids
/// exposing the internal physical-plan enum merely for a test-only guard.
pub fn native_physical_plan_variant_names() -> &'static [&'static str] {
    PhysicalPlanKind::variant_names_for_test()
}

/// A sealed two-fragment hash exchange used by native wire round-trip tests.
fn native_hash_exchange_plan() -> Result<DistributedPlan, String> {
    let output = vec![output_column(10, "v", DataType::Int64)];
    let source = PlanFragment {
        fragment_id: 0,
        root: values_node(0, 11, output.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition {
            kind: PartitionKind::Hash,
            exprs: vec![column_expr(10, "v", DataType::Int64)],
        },
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: output.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let receiver = DistributedNode {
        node_id: 42,
        fragment_id: 1,
        tuple_ids: vec![42],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
            partition: DataPartition::hash(vec![column_expr(10, "v", DataType::Int64)]),
            source_fragment_id: 0,
            output_columns: output.clone(),
            output_qualifier: Some("recv".to_string()),
            flavor: ExchangeFlavor::Distribution,
        }),
    };
    let target = PlanFragment {
        fragment_id: 1,
        root: receiver,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: output,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let builder = crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(1),
        vec![FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: DataPartition {
                kind: PartitionKind::Hash,
                exprs: vec![column_expr(10, "v", DataType::Int64)],
            },
            stream_kind: FragmentStreamKind::Partitioned,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![10],
        }],
        Default::default(),
    );
    builder
        .seal()
        .map_err(|error| format!("native hash exchange fixture must seal: {error}"))
}

fn native_hash_aggregate_plan() -> Result<DistributedPlan, String> {
    let group = output_column(1, "group_key", DataType::Int64);
    let aggregate = DistributedNode {
        node_id: 7,
        fragment_id: 0,
        tuple_ids: vec![7],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 8, vec![group.clone()])],
        stats: physical_stats(),
        payload: DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Local,
            group_by: vec![TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: None,
                    column: "group_key".to_string(),
                },
                data_type: DataType::Int64,
                nullable: false,
            }],
            aggregates: Vec::new(),
            is_merge: Vec::new(),
            output_layout: AggregateOutputLayout::new(vec![group.clone()], Vec::new()),
            output_columns: vec![group.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root: aggregate,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![group],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_reconciled_hash_join_plan() -> Result<DistributedPlan, String> {
    let left = output_column(1, "l_k", DataType::Int64);
    let right = output_column(2, "r_k", DataType::Int64);
    let root = DistributedNode {
        node_id: 1,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            values_node(0, 2, vec![left.clone()]),
            values_node(0, 3, vec![right.clone()]),
        ],
        stats: physical_stats(),
        payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: Vec::new(),
            other_condition: None,
            distribution: JoinDistribution::Unknown,
            execution_mode: None,
            build_runtime_filters: Vec::new(),
            output_columns: vec![
                left.clone(),
                right.clone(),
                output_column(999, "stale", DataType::Int64),
            ],
        })),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![left, right],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_change_stream_router_plan() -> Result<DistributedPlan, String> {
    let output_columns = vec![
        output_column(1, "__row_mutation_effect", DataType::Int8),
        output_column(3, "bucket", DataType::Int32),
    ];
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root: values_node(0, 10, output_columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
            group_id: 0,
            effect_output_ordinal: 0,
            routes: vec![ChangeStreamRoute {
                route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
                write_target_ordinal: write_target_ordinal_for_test(0),
                accepted_effects: vec![ConnectorRowMutationEffect::Delete],
                input_ordinals: vec![ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([9; 32]),
                    1,
                )],
                target_fragment_id: 1,
                target_exchange_node_id: 20,
                output_partition_ordinals: vec![1],
            }],
        }),
        output_exprs: None,
        output_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_duplicate_project_plan(topn: bool) -> Result<DistributedPlan, String> {
    let child_columns = vec![
        output_column(1, "c1", DataType::Int64),
        output_column(2, "c2", DataType::Int64),
    ];
    let duplicate_output = vec![
        output_column(1, "c1", DataType::Int64),
        output_column(1, "c1", DataType::Int64),
    ];
    let project = DistributedNode {
        node_id: 30,
        fragment_id: 0,
        tuple_ids: vec![30],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 29, child_columns)],
        stats: physical_stats(),
        payload: DistributedNodeKind::Project(PlanProjectNode {
            items: duplicate_output
                .iter()
                .map(|column| crate::analysis::ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: column.column_id,
                            qualifier: None,
                            column: column.name.clone(),
                        },
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                    },
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                })
                .collect(),
            output_qualifier: None,
        }),
    };
    let root = if topn {
        DistributedNode {
            node_id: 32,
            fragment_id: 0,
            tuple_ids: vec![32],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![project],
            stats: physical_stats(),
            payload: DistributedNodeKind::TopN(PhysicalTopNNode {
                items: Vec::new(),
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
        }
    } else {
        project
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: duplicate_output,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_nest_loop_join_plan() -> Result<DistributedPlan, String> {
    let left = output_column(1, "l_k", DataType::Int64);
    let right = output_column(2, "r_k", DataType::Int64);
    let root = DistributedNode {
        node_id: 41,
        fragment_id: 0,
        tuple_ids: vec![1, 2],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![
            values_node(0, 42, vec![left.clone()]),
            values_node(0, 43, vec![right.clone()]),
        ],
        stats: physical_stats(),
        payload: DistributedNodeKind::NestLoopJoin(PhysicalNestLoopJoinNode {
            join_type: JoinKind::Inner,
            condition: None,
            output_columns: vec![left.clone(), right.clone()],
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![left, right],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_assert_one_row_plan() -> Result<DistributedPlan, String> {
    let column = output_column(1, "only_row", DataType::Int64);
    let root = DistributedNode {
        node_id: 42,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 43, vec![column.clone()])],
        stats: physical_stats(),
        payload: DistributedNodeKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
            "select 1",
        )),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![column],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_sort_plan() -> Result<DistributedPlan, String> {
    let columns = vec![
        output_column(4, "l_shipdate", DataType::Date32),
        output_column(1, "l_orderkey", DataType::Int64),
    ];
    let root = DistributedNode {
        node_id: 42,
        fragment_id: 0,
        tuple_ids: vec![1],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![values_node(0, 41, columns.clone())],
        stats: physical_stats(),
        payload: DistributedNodeKind::Sort(PlanSortNode {
            items: Vec::new(),
            analytic_partition_by: Vec::new(),
            output_columns: columns.clone(),
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_pruned_connector_scan_stream_edge_plan() -> Result<DistributedPlan, String> {
    let all_columns = vec![
        output_column(1, "v1", DataType::Int64),
        output_column(2, "s2", DataType::Utf8),
        output_column(3, "array1", DataType::Int64),
    ];
    let stream_columns = vec![all_columns[1].clone(), all_columns[2].clone()];
    let source = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: vec![11],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: connector_read_table(&[
                    ("v1", DataType::Int64),
                    ("s2", DataType::Utf8),
                    ("array1", DataType::Int64),
                ]),
                alias: None,
                columns: all_columns,
                predicates: Vec::new(),
                required_columns: Some(vec!["s2".to_string(), "array1".to_string()]),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: stream_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    seal_stream_edge_fixture(source, vec![2, 3])
}

fn native_aggregate_layout_stream_edge_plan() -> Result<DistributedPlan, String> {
    let group_column = output_column(2, "c1", DataType::Utf8);
    let source = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: vec![11],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![values_node(0, 10, vec![group_column.clone()])],
            stats: physical_stats(),
            payload: DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Local,
                group_by: vec![column_expr(2, "c1", DataType::Utf8)],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: AggregateOutputLayout::new(vec![group_column], Vec::new()),
                output_columns: Vec::new(),
                topn_runtime_filter_builds: Vec::new(),
            })),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    seal_stream_edge_fixture(source, vec![2])
}

fn native_local_average_stream_edge_plan() -> Result<DistributedPlan, String> {
    let group_column = output_column(2, "c0", DataType::Int64);
    let value_column = output_column(3, "c1", DataType::Int64);
    let average_column = output_column(15, "avg(c1)", DataType::Float64);
    let source = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: vec![11],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![values_node(0, 10, vec![group_column.clone(), value_column])],
            stats: physical_stats(),
            payload: DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Local,
                group_by: vec![column_expr(2, "c0", DataType::Int64)],
                aggregates: vec![AggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_expr(3, "c1", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId(15),
                    resolved: crate::functions::test_resolved_aggregate(
                        "avg",
                        &[DataType::Int64],
                        false,
                    ),
                }],
                is_merge: vec![false],
                output_layout: AggregateOutputLayout::new(
                    vec![group_column.clone()],
                    vec![average_column.clone()],
                ),
                output_columns: Vec::new(),
                topn_runtime_filter_builds: Vec::new(),
            })),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: vec![group_column, average_column],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    seal_stream_edge_fixture(source, vec![2, 15])
}

fn native_zero_column_stream_edge_plan() -> Result<DistributedPlan, String> {
    let source = PlanFragment {
        fragment_id: 0,
        root: values_node(0, 10, Vec::new()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    seal_stream_edge_fixture(source, Vec::new())
}

fn native_reordered_slots_stream_plan() -> Result<DistributedPlan, String> {
    let source_columns = vec![
        output_column(1, "old", DataType::Int64),
        output_column(2, "delta", DataType::Int64),
    ];
    native_plan_encoding_stream_plan(
        values_node(1, 10, source_columns.clone()),
        source_columns,
        vec![
            output_column(2, "delta", DataType::Int64),
            output_column(1, "old", DataType::Int64),
        ],
        vec![2, 1],
    )
}

fn native_lowered_slots_stream_plan() -> Result<DistributedPlan, String> {
    let source_columns = vec![
        output_column(10, "employee_id", DataType::Int64),
        output_column(20, "name", DataType::Utf8),
        output_column(30, "title", DataType::Utf8),
    ];
    native_plan_encoding_stream_plan(
        values_node(1, 10, source_columns.clone()),
        source_columns,
        vec![
            output_column(10, "employee_id", DataType::Int64),
            output_column(20, "name", DataType::Utf8),
        ],
        vec![43, 44],
    )
}

fn native_zero_columns_stream_plan() -> Result<DistributedPlan, String> {
    native_plan_encoding_stream_plan(
        values_node(1, 10, Vec::new()),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

fn native_generate_series_stream_plan() -> Result<DistributedPlan, String> {
    let source = DistributedNode {
        node_id: 10,
        fragment_id: 1,
        tuple_ids: vec![10],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::GenerateSeries(PlanGenerateSeriesNode {
            start: 1,
            end: 3,
            step: 1,
            column_name: "generate_series".to_string(),
            alias: None,
            output_column_id: ColumnId(7),
        }),
    };
    native_plan_encoding_stream_plan(
        source,
        Vec::new(),
        vec![output_column(7, "generate_series", DataType::Int64)],
        vec![7],
    )
}

fn native_plan_encoding_stream_plan(
    source_root: DistributedNode,
    source_columns: Vec<OutputColumn>,
    receiver_columns: Vec<OutputColumn>,
    output_slot_ids: Vec<i32>,
) -> Result<DistributedPlan, String> {
    let source = PlanFragment {
        fragment_id: 1,
        root: source_root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: source_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let target = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 20,
            fragment_id: 0,
            tuple_ids: vec![20],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: 1,
                output_columns: receiver_columns,
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(0),
        vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids,
        }],
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native plan topology fixture must seal: {error}"))
}

fn connector_read_table(columns: &[(&str, DataType)]) -> TableDef {
    TableDef {
        name: "sc2".to_string(),
        columns: columns
            .iter()
            .map(|(name, data_type)| column_def(name, data_type.clone(), true))
            .collect(),
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: test_sql_scan_source(SqlScanKind::ConnectorRead),
    }
}

fn seal_stream_edge_fixture(
    source: PlanFragment,
    output_slot_ids: Vec<i32>,
) -> Result<DistributedPlan, String> {
    let target = PlanFragment {
        fragment_id: 1,
        root: DistributedNode {
            node_id: 42,
            fragment_id: 1,
            tuple_ids: vec![42],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(1),
        vec![FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 42,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids,
        }],
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native stream-edge fixture must seal: {error}"))
}

fn native_broadcast_stream_plan() -> Result<DistributedPlan, String> {
    native_stream_exchange_with_topology(
        ExchangeFlavor::Distribution,
        DataPartition::unpartitioned(),
        FragmentStreamKind::Broadcast,
    )
}

fn native_random_other_stream_plan() -> Result<DistributedPlan, String> {
    native_stream_exchange_with_topology(
        ExchangeFlavor::Distribution,
        DataPartition {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        },
        FragmentStreamKind::Other,
    )
}

fn native_hash_partitioned_stream_plan() -> Result<DistributedPlan, String> {
    native_stream_exchange_with_topology(
        ExchangeFlavor::Distribution,
        DataPartition::hash(vec![column_expr(1, "k", DataType::Int64)]),
        FragmentStreamKind::Partitioned,
    )
}

fn native_stream_exchange_plan(flavor: ExchangeFlavor) -> Result<DistributedPlan, String> {
    native_stream_exchange_with_topology(
        flavor,
        DataPartition::unpartitioned(),
        FragmentStreamKind::Gather,
    )
}

fn native_stream_exchange_with_topology(
    flavor: ExchangeFlavor,
    partition: DataPartition,
    stream_kind: FragmentStreamKind,
) -> Result<DistributedPlan, String> {
    let columns = vec![output_column(1, "k", DataType::Int64)];
    let source = PlanFragment {
        fragment_id: 1,
        root: values_node(1, 10, columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: partition.clone(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: columns.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let target = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 20,
            fragment_id: 0,
            tuple_ids: vec![20],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: partition.clone(),
                source_fragment_id: 1,
                output_columns: columns.clone(),
                output_qualifier: None,
                flavor,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(0),
        vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: partition,
            stream_kind,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![1],
        }],
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native topology fixture must seal: {error}"))
}

fn native_cte_multicast_stream_plan() -> Result<DistributedPlan, String> {
    let cte_id: CteId = 7;
    let producer_columns = vec![
        output_column(1, "k", DataType::Int64),
        output_column(2, "v", DataType::Int64),
        output_column(3, "payload", DataType::Int64),
    ];
    let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
    let receive_producer_column_ids = vec![ColumnId(1), ColumnId(3)];
    let source = PlanFragment {
        fragment_id: 1,
        root: values_node(1, 10, producer_columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: producer_columns,
        cte_id: Some(cte_id),
        cte_exchange_nodes: Vec::new(),
    };
    let target = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 20,
            fragment_id: 0,
            tuple_ids: vec![20],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: 1,
                output_columns: receive_columns.clone(),
                output_qualifier: Some("c".to_string()),
                flavor: ExchangeFlavor::CteMulticast {
                    cte_id,
                    receive_producer_column_ids: receive_producer_column_ids.clone(),
                },
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: receive_columns,
        cte_id: None,
        cte_exchange_nodes: vec![(cte_id, 20, receive_producer_column_ids.clone())],
    };
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(0),
        vec![FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            },
            output_slot_ids: vec![1, 3],
        }],
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native CTE topology fixture must seal: {error}"))
}

/// A closed CTE multicast plan whose two target receivers are deliberately
/// declared and edged in descending exchange-node order. Core consumes only
/// the sealed read model to verify its deterministic projection ordering.
fn native_cte_multicast_ordering_plan() -> Result<DistributedPlan, String> {
    let cte_id: CteId = 42;
    let producer_columns = vec![
        output_column(1, "first", DataType::Int64),
        output_column(2, "second", DataType::Int64),
        output_column(3, "third", DataType::Int64),
        output_column(4, "fourth", DataType::Int64),
    ];
    let first_receive_columns = vec![producer_columns[3].clone(), producer_columns[1].clone()];
    let first_receive_ids = vec![ColumnId(4), ColumnId(2)];
    let second_receive_columns = vec![producer_columns[2].clone(), producer_columns[0].clone()];
    let second_receive_ids = vec![ColumnId(3), ColumnId(1)];
    let source = PlanFragment {
        fragment_id: 1,
        root: values_node(1, 10, producer_columns.clone()),
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: producer_columns,
        cte_id: Some(cte_id),
        cte_exchange_nodes: Vec::new(),
    };
    let first_receiver = DistributedNode {
        node_id: 11,
        fragment_id: 0,
        tuple_ids: vec![11],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
            partition: DataPartition::unpartitioned(),
            source_fragment_id: 1,
            output_columns: first_receive_columns.clone(),
            output_qualifier: Some("cte_first".to_string()),
            flavor: ExchangeFlavor::CteMulticast {
                cte_id,
                receive_producer_column_ids: first_receive_ids.clone(),
            },
        }),
    };
    let second_receiver = DistributedNode {
        node_id: 3,
        fragment_id: 0,
        tuple_ids: vec![3],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
            partition: DataPartition::unpartitioned(),
            source_fragment_id: 1,
            output_columns: second_receive_columns.clone(),
            output_qualifier: Some("cte_second".to_string()),
            flavor: ExchangeFlavor::CteMulticast {
                cte_id,
                receive_producer_column_ids: second_receive_ids.clone(),
            },
        }),
    };
    let target_output_columns = vec![
        first_receive_columns[0].clone(),
        first_receive_columns[1].clone(),
        second_receive_columns[0].clone(),
        second_receive_columns[1].clone(),
    ];
    let target = PlanFragment {
        fragment_id: 0,
        root: DistributedNode {
            node_id: 30,
            fragment_id: 0,
            tuple_ids: vec![30],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![first_receiver, second_receiver],
            stats: physical_stats(),
            payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: Vec::new(),
                other_condition: None,
                distribution: JoinDistribution::Unknown,
                execution_mode: None,
                build_runtime_filters: Vec::new(),
                output_columns: target_output_columns.clone(),
            })),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: target_output_columns,
        cte_id: None,
        cte_exchange_nodes: vec![
            (cte_id, 11, first_receive_ids.clone()),
            (cte_id, 3, second_receive_ids.clone()),
        ],
    };
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![source, target],
        Some(0),
        vec![
            FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 11,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids: first_receive_ids,
                },
                output_slot_ids: vec![4, 2],
            },
            FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 3,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids: second_receive_ids,
                },
                output_slot_ids: vec![3, 1],
            },
        ],
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native CTE ordering fixture must seal: {error}"))
}

fn native_router_stream_plan() -> Result<DistributedPlan, String> {
    let output_columns = vec![
        OutputColumn {
            column_id: ColumnId(1),
            name: "__row_mutation_effect".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        },
        output_column(3, "delete_id", DataType::Int64),
    ];
    let draft = crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        vec![PlanFragment {
            fragment_id: 0,
            root: values_node(0, 10, output_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        Some(0),
        Vec::new(),
        Default::default(),
    );
    let dag = ChangeStreamWriteDagSpec {
        effect_output_ordinal: 0,
        routes: vec![ChangeStreamWriteRouteSpec {
            route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
            write_target_ordinal: write_target_ordinal_for_test(0),
            accepted_effects: vec![ConnectorRowMutationEffect::Delete],
            input_ordinals: vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([9; 32]),
                1,
            )],
            output_partition_ordinals: vec![1],
            sink: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
        }],
    };
    finalize_sql_change_stream_table_writer_finish_test_plan(draft, dag)
        .map_err(|error| format!("native router fixture must seal: {error}"))
}

fn native_delta_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Delta {
            from_snapshot_id: 1,
            to_snapshot_id: 2,
        },
        vec![
            column_def("order_id", DataType::Int64, false),
            column_def("stale_unprojected", DataType::Utf8, true),
        ],
        vec![output_column(1, "order_id", DataType::Int64)],
        None,
        Vec::new(),
    )
}

fn native_prepared_delta_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Delta {
            from_snapshot_id: 6,
            to_snapshot_id: 7,
        },
        vec![column_def("id", DataType::Int32, false)],
        vec![output_column(1, "id", DataType::Int32)],
        Some(vec!["id".to_string()]),
        Vec::new(),
    )
}

fn native_ordinary_iceberg_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![
            column_def("order_id", DataType::Int64, false),
            column_def("unprojected_payload", DataType::Utf8, true),
        ],
        vec![output_column(1, "order_id", DataType::Int64)],
        Some(vec!["order_id".to_string()]),
        Vec::new(),
    )
}

fn native_ordinary_iceberg_id_equality_predicate_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan_with_predicates(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        ordinary_iceberg_columns(),
        vec![output_column(1, "id", DataType::Int32)],
        Some(vec!["id".to_string()]),
        Vec::new(),
        vec![id_equality_predicate(12)],
    )
}

fn ordinary_iceberg_columns() -> Vec<novarocks_types::schema::ColumnDef> {
    vec![
        column_def("id", DataType::Int32, false),
        column_def("category", DataType::Utf8, true),
    ]
}

fn native_ordinary_iceberg_unrestricted_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        ordinary_iceberg_columns(),
        vec![output_column(1, "id", DataType::Int32)],
        None,
        Vec::new(),
    )
}

fn native_ordinary_iceberg_id_projection_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        ordinary_iceberg_columns(),
        vec![output_column(1, "id", DataType::Int32)],
        Some(vec!["id".to_string()]),
        Vec::new(),
    )
}

fn native_ordinary_iceberg_all_columns_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        ordinary_iceberg_columns(),
        vec![
            output_column(1, "id", DataType::Int32),
            output_column_with_nullability(3, "category", DataType::Utf8, true),
        ],
        None,
        Vec::new(),
    )
}

fn native_ordinary_iceberg_required_payload_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        ordinary_iceberg_columns(),
        vec![output_column(1, "id", DataType::Int32)],
        Some(vec!["id".to_string(), "category".to_string()]),
        Vec::new(),
    )
}

fn native_unsupported_predicate_scan_plan() -> Result<DistributedPlan, String> {
    let root = DistributedNode {
        node_id: 10,
        fragment_id: 0,
        tuple_ids: vec![10],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "orders".to_string(),
                columns: ordinary_iceberg_columns(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: test_sql_scan_source(SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                }),
            },
            alias: None,
            columns: vec![output_column(1, "id", DataType::Int32)],
            predicates: vec![TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: "abs".to_string(),
                    args: vec![TypedExpr {
                        kind: ExprKind::BinaryOp {
                            left: Box::new(column_expr(1, "id", DataType::Int32)),
                            op: BinOp::Eq,
                            right: Box::new(TypedExpr {
                                kind: ExprKind::Literal(LiteralValue::Int(12)),
                                data_type: DataType::Int32,
                                nullable: false,
                            }),
                        },
                        data_type: DataType::Boolean,
                        nullable: false,
                    }],
                    distinct: false,
                    volatility: FunctionVolatility::Immutable,
                },
                data_type: DataType::Boolean,
                nullable: false,
            }],
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![output_column(1, "id", DataType::Int32)],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn native_refresh_scan_plan(source: SqlScanKind) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        source,
        vec![
            column_def("stale", DataType::Utf8, true),
            column_def("stale_unprojected", DataType::Utf8, true),
        ],
        vec![
            output_column(1, "stale", DataType::Utf8),
            output_column(2, "stale_meta", DataType::Int64),
        ],
        None,
        Vec::new(),
    )
}

fn native_basic_scan_plan(source: SqlScanKind) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        source,
        vec![column_def("order_id", DataType::Int64, false)],
        vec![output_column(1, "order_id", DataType::Int64)],
        None,
        Vec::new(),
    )
}

fn native_variant_projection_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![column_def("v", DataType::LargeBinary, false)],
        vec![
            output_column(1, "v", DataType::LargeBinary),
            OutputColumn {
                column_id: ColumnId(2),
                name: "__nr_var_v_0".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: true,
            },
        ],
        Some(vec!["__nr_var_v_0".to_string()]),
        vec![ScanVariantColumn {
            source_column_id: ColumnId(1),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId(2),
            synthetic_column: "__nr_var_v_0".to_string(),
            canonical_path: "$.a.b".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        }],
    )
}

fn native_version_snapshot_with_stale_output_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Snapshot(6),
        },
        vec![column_def("id", DataType::Int32, false)],
        vec![
            output_column(1, "id", DataType::Int32),
            output_column(99, "stale_planner_only", DataType::Utf8),
        ],
        None,
        Vec::new(),
    )
}

fn native_target_locator_projection_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan_with_lineage(
        SqlScanKind::MvTargetLocator {
            facts: SqlMvTargetLocatorScan {
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                apply_key_column: "id".to_string(),
                branch_id_column: None,
            },
        },
        vec![
            column_def("id", DataType::Int32, false),
            column_def("extra", DataType::Utf8, true),
        ],
        vec![
            output_column(1, "id", DataType::Int32),
            OutputColumn {
                column_id: ColumnId(2),
                name: "extra".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                is_internal: false,
            },
            output_column(11, "_file", DataType::Utf8),
            output_column(12, "_pos", DataType::Int64),
            output_column(13, "_row_id", DataType::Int64),
            OutputColumn {
                column_id: ColumnId(14),
                name: "_last_updated_sequence_number".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            },
        ],
        None,
        Vec::new(),
        Vec::new(),
        vec![
            column_def("_file", DataType::Utf8, false),
            column_def("_pos", DataType::Int64, false),
            column_def("_row_id", DataType::Int64, false),
            column_def("_last_updated_sequence_number", DataType::Int64, true),
        ],
    )
}

fn native_target_state_projection_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan_with_lineage(
        SqlScanKind::MvTargetState {
            facts: SqlMvTargetStateScan {
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                aggregate_state_layout_version: 1,
                columns: vec![
                    column_def("id", DataType::Int32, false),
                    column_def("agg", DataType::Binary, true),
                    column_def("extra", DataType::Utf8, true),
                ],
                group_key_names: vec!["id".to_string()],
                aggregate_state_names: vec!["agg".to_string()],
                physical_column_names: vec!["id".to_string(), "agg".to_string()],
                row_id_column_name: "_row_id".to_string(),
                row_filter: SqlMvTargetStateRowFilter::DeltaInputRowIds {
                    row_id_column_name: "_row_id".to_string(),
                    branch_scope: None,
                },
                partition_constraint: SqlMvTargetStatePartitionConstraint::Unpartitioned,
            },
        },
        vec![
            column_def("id", DataType::Int32, false),
            column_def("agg", DataType::Binary, true),
            column_def("extra", DataType::Utf8, true),
        ],
        vec![
            output_column(1, "id", DataType::Int32),
            OutputColumn {
                column_id: ColumnId(3),
                name: "agg".to_string(),
                data_type: DataType::Binary,
                nullable: true,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(4),
                name: "extra".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                is_internal: false,
            },
            output_column(11, "_file", DataType::Utf8),
            output_column(12, "_pos", DataType::Int64),
            output_column(13, "_row_id", DataType::Int64),
            OutputColumn {
                column_id: ColumnId(14),
                name: "_last_updated_sequence_number".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            },
        ],
        None,
        Vec::new(),
        Vec::new(),
        vec![
            column_def("_file", DataType::Utf8, false),
            column_def("_pos", DataType::Int64, false),
            column_def("_row_id", DataType::Int64, false),
            column_def("_last_updated_sequence_number", DataType::Int64, true),
        ],
    )
}

fn native_equality_key_hidden_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![
            column_def("id", DataType::Int32, false),
            column_def("category", DataType::Utf8, true),
        ],
        vec![output_column(1, "id", DataType::Int32)],
        Some(vec!["id".to_string()]),
        Vec::new(),
    )
}

fn native_equality_key_projected_scan_plan() -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![
            column_def("id", DataType::Int32, false),
            column_def("category", DataType::Utf8, true),
        ],
        vec![
            output_column(1, "id", DataType::Int32),
            OutputColumn {
                column_id: ColumnId(3),
                name: "category".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                is_internal: false,
            },
        ],
        Some(vec!["id".to_string(), "category".to_string()]),
        Vec::new(),
    )
}

fn native_projection_mismatch_scan_plan(
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan(
        SqlScanKind::Data {
            version: SqlTableVersionSelector::Current,
        },
        vec![column_def("id", DataType::Int32, false)],
        vec![OutputColumn {
            column_id: ColumnId(1),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }],
        Some(vec![name.to_string()]),
        Vec::new(),
    )
}

fn refresh_snapshot_source() -> SqlScanKind {
    SqlScanKind::Data {
        version: SqlTableVersionSelector::Snapshot(1),
    }
}

fn mv_target_locator_source(apply_key_column: &str) -> SqlScanKind {
    SqlScanKind::MvTargetLocator {
        facts: SqlMvTargetLocatorScan {
            target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
            target_snapshot_id: Some(1),
            apply_key_column: apply_key_column.to_string(),
            branch_id_column: None,
        },
    }
}

fn mv_target_state_source(row_id_column_name: &str) -> SqlScanKind {
    SqlScanKind::MvTargetState {
        facts: SqlMvTargetStateScan {
            target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
            target_snapshot_id: Some(1),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: vec![row_id_column_name.to_string()],
            aggregate_state_names: Vec::new(),
            physical_column_names: vec![row_id_column_name.to_string()],
            row_id_column_name: row_id_column_name.to_string(),
            row_filter: SqlMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: row_id_column_name.to_string(),
                branch_scope: None,
            },
            partition_constraint: SqlMvTargetStatePartitionConstraint::Unpartitioned,
        },
    }
}

fn native_scan_fixture_plan(
    source: SqlScanKind,
    table_columns: Vec<novarocks_types::schema::ColumnDef>,
    output_columns: Vec<OutputColumn>,
    required_columns: Option<Vec<String>>,
    variant_columns: Vec<ScanVariantColumn>,
) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan_with_predicates(
        source,
        table_columns,
        output_columns,
        required_columns,
        variant_columns,
        Vec::new(),
    )
}

fn native_scan_fixture_plan_with_predicates(
    source: SqlScanKind,
    table_columns: Vec<novarocks_types::schema::ColumnDef>,
    output_columns: Vec<OutputColumn>,
    required_columns: Option<Vec<String>>,
    variant_columns: Vec<ScanVariantColumn>,
    predicates: Vec<TypedExpr>,
) -> Result<DistributedPlan, String> {
    native_scan_fixture_plan_with_lineage(
        source,
        table_columns,
        output_columns,
        required_columns,
        variant_columns,
        predicates,
        Vec::new(),
    )
}

fn native_scan_fixture_plan_with_lineage(
    source: SqlScanKind,
    table_columns: Vec<novarocks_types::schema::ColumnDef>,
    output_columns: Vec<OutputColumn>,
    required_columns: Option<Vec<String>>,
    variant_columns: Vec<ScanVariantColumn>,
    predicates: Vec<TypedExpr>,
    iceberg_row_lineage_metadata_columns: Vec<novarocks_types::schema::ColumnDef>,
) -> Result<DistributedPlan, String> {
    let table = TableDef {
        name: "orders".to_string(),
        columns: table_columns,
        iceberg_row_lineage_metadata_columns,
        source: test_sql_scan_source(source),
    };
    let root = DistributedNode {
        node_id: 10,
        fragment_id: 0,
        tuple_ids: vec![10],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table,
            alias: None,
            columns: output_columns.clone(),
            predicates,
            required_columns,
            variant_columns,
            mv_rewritten_from: None,
        }),
    };
    seal_fixture_plan(vec![PlanFragment {
        fragment_id: 0,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns,
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    }])
}

fn id_equality_predicate(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: Some("ice_t".to_string()),
                    column: "id".to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(value)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

/// A column reference suitable for native expression-encoding assertions.
pub fn column_expr(id: u32, name: &str, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId(id),
            qualifier: Some("t".to_string()),
            column: name.to_string(),
        },
        data_type,
        nullable: true,
    }
}

/// An integer literal suitable for native expression-encoding assertions.
pub fn int_expr(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

/// The expression variants covered by the native protobuf encoder contract.
pub fn native_expression_variants() -> Vec<TypedExpr> {
    let column = column_expr(1, "c1", DataType::Int64);
    let literal = int_expr(2);
    let lambda_body = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: "x".to_string(),
                    slot_id: 3,
                },
                data_type: DataType::Int64,
                nullable: true,
            }),
            op: BinOp::Add,
            right: Box::new(int_expr(1)),
        },
        data_type: DataType::Int64,
        nullable: true,
    };
    let sort_item = SortItem {
        expr: column.clone(),
        asc: false,
        nulls_first: true,
    };

    vec![
        column_expr(1, "c1", DataType::Int64),
        TypedExpr {
            kind: ExprKind::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 3,
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        literal.clone(),
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column.clone()),
                op: BinOp::Gt,
                right: Box::new(literal.clone()),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::UnaryOp {
                op: UnOp::Not,
                expr: Box::new(bool_expr(true)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![column.clone()],
                distinct: false,
                volatility: FunctionVolatility::Immutable,
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        lambda_expr(lambda_body.clone()),
        TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "sum".to_string(),
                args: vec![column.clone()],
                distinct: true,
                order_by: vec![sort_item.clone()],
                resolved: crate::functions::test_resolved_aggregate(
                    "sum",
                    &[DataType::Int64],
                    true,
                ),
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(column.clone()),
                target: DataType::Float64,
            },
            data_type: DataType::Float64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(column.clone()),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(string_expr("x")),
                list: vec![string_expr("a"), string_expr("b")],
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(column.clone()),
                low: Box::new(int_expr(1)),
                high: Box::new(int_expr(9)),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Like {
                expr: Box::new(string_expr("abc")),
                pattern: Box::new(string_expr("a%")),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(bool_expr(true), int_expr(1))],
                else_expr: Some(Box::new(int_expr(0))),
            },
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::IsTruthValue {
                expr: Box::new(bool_expr(false)),
                value: false,
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
        TypedExpr {
            kind: ExprKind::Nested(Box::new(column.clone())),
            data_type: DataType::Int64,
            nullable: true,
        },
        TypedExpr {
            kind: ExprKind::WindowCall {
                name: "rank".to_string(),
                args: vec![],
                distinct: false,
                function_order_by: vec![],
                aggregate_binding: None,
                partition_by: vec![column],
                order_by: vec![sort_item],
                window_frame: Some(WindowFrame {
                    frame_type: WindowFrameType::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                }),
                ignore_nulls: false,
            },
            data_type: DataType::Int64,
            nullable: false,
        },
    ]
}

/// A lambda expression with its private parameter vocabulary already sealed in
/// the returned public expression carrier.
pub fn native_lambda_expression() -> TypedExpr {
    lambda_expr(int_expr(1))
}

/// An immutable scalar call representative of the function-call wire arm.
pub fn native_immutable_function_expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![column_expr(1, "c1", DataType::Int64)],
            distinct: false,
            volatility: FunctionVolatility::Immutable,
        },
        data_type: DataType::Int64,
        nullable: true,
    }
}

/// A sealed cast expression for native protocol encoder tests.
pub fn native_cast_expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(column_expr(7, "amount", DataType::Int64)),
            target: DataType::Float64,
        },
        data_type: DataType::Float64,
        nullable: true,
    }
}

/// A sealed lambda parameter reference for native protocol encoder tests.
pub fn native_lambda_parameter_expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::LambdaParamRef {
            name: "x".to_string(),
            slot_id: 3,
        },
        data_type: DataType::Int64,
        nullable: true,
    }
}

/// A sealed window expression for native protocol encoder tests.
pub fn native_window_expression() -> TypedExpr {
    let column = column_expr(7, "amount", DataType::Int64);
    TypedExpr {
        kind: ExprKind::WindowCall {
            name: "rank".to_string(),
            args: vec![],
            distinct: false,
            function_order_by: vec![],
            aggregate_binding: None,
            partition_by: vec![column.clone()],
            order_by: vec![SortItem {
                expr: column,
                asc: false,
                nulls_first: true,
            }],
            window_frame: Some(WindowFrame {
                frame_type: WindowFrameType::Rows,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            }),
            ignore_nulls: false,
        },
        data_type: DataType::Int64,
        nullable: false,
    }
}

/// A deliberately unsupported placeholder used to assert the encoder's
/// fail-fast branch without exposing the private subquery-kind enum.
pub fn subquery_placeholder_expr(id: u32, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::SubqueryPlaceholder {
            id: id as usize,
            kind: SubqueryKind::Scalar,
            data_type: data_type.clone(),
        },
        data_type,
        nullable: true,
    }
}

/// A sealed literal expression for native protocol encoder tests.
pub fn native_literal_expression(value: LiteralValue, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(value),
        data_type,
        nullable: false,
    }
}

fn literal_expr(value: LiteralValue, data_type: DataType) -> TypedExpr {
    native_literal_expression(value, data_type)
}

fn bool_expr(value: bool) -> TypedExpr {
    literal_expr(LiteralValue::Bool(value), DataType::Boolean)
}

fn string_expr(value: &str) -> TypedExpr {
    literal_expr(LiteralValue::String(value.to_string()), DataType::Utf8)
}

fn lambda_expr(body: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![LambdaParam {
                name: "x".to_string(),
                slot_id: 3,
                data_type: DataType::Int64,
                nullable: true,
            }],
            body: Box::new(body),
        },
        data_type: DataType::Int64,
        nullable: true,
    }
}

fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
    output_column_with_nullability(id, name, data_type, false)
}

fn output_column_with_nullability(
    id: u32,
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        data_type,
        nullable,
        is_internal: false,
    }
}

fn column_def(
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> novarocks_types::schema::ColumnDef {
    novarocks_types::schema::ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

fn physical_stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: PlannerConfidence::Fallback,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn values_node(fragment_id: u32, node_id: i32, columns: Vec<OutputColumn>) -> DistributedNode {
    DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: physical_stats(),
        payload: DistributedNodeKind::Values(PlanValuesNode {
            rows: Vec::new(),
            columns,
        }),
    }
}

fn seal_fixture_plan(fragments: Vec<PlanFragment>) -> Result<DistributedPlan, String> {
    let root_fragment_id = fragments
        .last()
        .map(|fragment| fragment.fragment_id)
        .ok_or_else(|| "native encoder fixture requires one fragment".to_string())?;
    crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
        fragments,
        Some(root_fragment_id),
        Vec::new(),
        Default::default(),
    )
    .seal()
    .map_err(|error| format!("native encoder fixture must seal: {error}"))
}

#[cfg(test)]
mod tests {
    use super::native_mv_data_current_scan_is_tokenized;
    use super::{
        NativeBuildFixture, NativeEncoderPlanFixture, NativePreparationFixture, native_build_plan,
        native_encoder_plan, native_preparation_plan,
    };
    use super::{NativePlanEncodingFixture, native_plan_encoding_plan};
    use super::{NativeScanFixture, native_scan_plan};
    use super::{NativeWriteDataflowFixture, native_write_dataflow_plan};

    #[test]
    fn native_mv_data_current_scan_fixture_remains_tokenized() {
        assert!(native_mv_data_current_scan_is_tokenized());
    }

    #[test]
    fn native_encoder_fixtures_return_complete_sealed_plans() {
        for fixture in [
            NativeEncoderPlanFixture::Minimal,
            NativeEncoderPlanFixture::HashExchange,
            NativeEncoderPlanFixture::HashAggregate,
            NativeEncoderPlanFixture::ReconciledHashJoin,
            NativeEncoderPlanFixture::ChangeStreamRouter,
            NativeEncoderPlanFixture::DuplicateProject,
            NativeEncoderPlanFixture::TopNDuplicateProject,
            NativeEncoderPlanFixture::NestLoopJoin,
            NativeEncoderPlanFixture::AssertOneRow,
            NativeEncoderPlanFixture::Sort,
            NativeEncoderPlanFixture::PrunedConnectorScanStreamEdge,
            NativeEncoderPlanFixture::AggregateLayoutStreamEdge,
            NativeEncoderPlanFixture::LocalAverageStreamEdge,
            NativeEncoderPlanFixture::ZeroColumnStreamEdge,
        ] {
            let plan = native_encoder_plan(fixture).expect("fixture must seal");
            assert!(
                plan.fragments()
                    .iter()
                    .any(|fragment| fragment.fragment_id == plan.root_fragment_id())
            );
        }
        for fixture in [
            NativeScanFixture::ConnectorRead,
            NativeScanFixture::PinnedFileSet,
            NativeScanFixture::DeltaWithStaleUnprojectedColumn,
            NativeScanFixture::DeltaForPreparedBinding,
            NativeScanFixture::DeltaWithInvalidProjection,
            NativeScanFixture::OrdinaryIcebergWithUnprojectedPayload,
            NativeScanFixture::OrdinaryIcebergIdProjection,
            NativeScanFixture::OrdinaryIcebergUnrestricted,
            NativeScanFixture::OrdinaryIcebergAllColumns,
            NativeScanFixture::OrdinaryIcebergWithRequiredPayload,
            NativeScanFixture::UnsupportedPredicate,
            NativeScanFixture::RefreshSnapshot,
            NativeScanFixture::FrozenSnapshotEleven,
            NativeScanFixture::FrozenSnapshotTwelve,
            NativeScanFixture::FrozenTimestamp,
            NativeScanFixture::VersionSnapshotWithStaleOutput,
            NativeScanFixture::RefreshMvTargetLocator,
            NativeScanFixture::RefreshMvTargetState,
            NativeScanFixture::MvTargetLocator,
            NativeScanFixture::MvTargetState,
            NativeScanFixture::VariantProjection,
            NativeScanFixture::TargetLocatorProjection,
            NativeScanFixture::TargetStateProjection,
            NativeScanFixture::EqualityKeyHidden,
            NativeScanFixture::EqualityKeyProjected,
            NativeScanFixture::ProjectionMissingColumn,
            NativeScanFixture::ProjectionTypeMismatch,
            NativeScanFixture::ProjectionNullabilityMismatch,
            NativeScanFixture::JoinRefreshCoalesce,
        ] {
            let scan = native_scan_plan(fixture).expect("scan fixture must seal");
            let expected_fragments = if fixture == NativeScanFixture::JoinRefreshCoalesce {
                15
            } else {
                1
            };
            assert_eq!(scan.fragments().len(), expected_fragments);
        }
        for fixture in [
            NativePlanEncodingFixture::ReorderedSlots,
            NativePlanEncodingFixture::LoweredSlots,
            NativePlanEncodingFixture::ZeroColumns,
            NativePlanEncodingFixture::GenerateSeries,
        ] {
            let plan = native_plan_encoding_plan(fixture).expect("plan topology fixture must seal");
            assert_eq!(plan.fragments().len(), 2);
        }
        for fixture in [
            NativeBuildFixture::BroadcastStream,
            NativeBuildFixture::RandomOtherStream,
            NativeBuildFixture::HashPartitionedStream,
            NativeBuildFixture::LimitOffsetStream,
            NativeBuildFixture::TopNSplitStream,
            NativeBuildFixture::CteMulticastStream,
            NativeBuildFixture::CteMulticastOrdering,
        ] {
            let plan = native_build_plan(fixture).expect("build fixture must seal");
            assert_eq!(plan.fragments().len(), 2);
        }
        // The router fixture is the dataflow write shape: producer, one route
        // writer, and the Root finish fragment they both stream into.
        let router = native_build_plan(NativeBuildFixture::RouterStream)
            .expect("router build fixture must seal");
        assert_eq!(router.fragments().len(), 3);
    }

    #[test]
    fn preparation_fixtures_keep_construction_and_negative_mutation_inside_sql() {
        for fixture in [
            NativePreparationFixture::ResultOutput,
            NativePreparationFixture::MissingResultOutput,
        ] {
            native_preparation_plan(fixture).expect("closed preparation fixture");
        }
    }

    #[test]
    fn write_dataflow_fixtures_seal_into_one_table_finish_root() {
        for (fixture, expected_writers) in [
            (NativeWriteDataflowFixture::SingleWriter, 1),
            (NativeWriteDataflowFixture::ChangeStreamTwoWriters, 2),
        ] {
            let plan = native_write_dataflow_plan(fixture).expect("closed write dataflow fixture");
            let finish_fragments = plan
                .fragments()
                .iter()
                .filter(|fragment| {
                    matches!(
                        fragment.root.payload,
                        crate::planner::distributed::DistributedNodeKind::TableFinish(_)
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(finish_fragments.len(), 1);
            assert_eq!(finish_fragments[0].fragment_id, plan.root_fragment_id());
            let writers = plan
                .fragments()
                .iter()
                .filter(|fragment| {
                    matches!(
                        fragment.root.payload,
                        crate::planner::distributed::DistributedNodeKind::TableWriter(_)
                    )
                })
                .count();
            assert_eq!(writers, expected_writers);
        }
    }
}
