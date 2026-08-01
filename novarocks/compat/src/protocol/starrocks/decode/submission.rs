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

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use crate::thrift::{descriptors, internal_service, planner, types};
use novarocks::connector::starrocks::lake_meta::{LakeMetaStorageFacts, LakeMetaStorageRequest};
use novarocks::exec::expr::ExprArena;
use novarocks::exec::fragment::program::{
    ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgramBuilder,
    FragmentProgramOptions, RuntimeFilterContract,
};
use novarocks::exec::node::scan::BoundScanRanges;
use novarocks::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use novarocks::exec::row_position::RowPositionDescriptor;
use novarocks::protocol::FieldPath;
use novarocks::runtime::descriptor_snapshot::DescriptorSnapshot;
use novarocks::runtime::fragment::FragmentSubmission;
use novarocks::runtime::fragment::{
    ExchangeFrameTransmitter, ExchangeInputAssignment, ExchangeInputAssignments, FragmentEventSink,
    FragmentInstanceSpec, FragmentResultWriter, FragmentRuntimeOptions, ResultPresentation,
    ResultProjection, ResultWriteSpec, ScanAssignments,
};
use novarocks::runtime::query_options::query_expire_durations;
use novarocks::runtime::starrocks_fragment_query::{
    LookupFetcherLifecycle, StarRocksFragmentQueryRuntime,
};
use novarocks_spi::connector::{
    ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use super::dependency::{
    FragmentExprArenaOwner, QueryProfilePatch, StarRocksExternalDependency,
    StarRocksExternalDependencyDraft, StarRocksResolvedDependencies,
    StarRocksResolvedDependencyValue,
};
use super::error::{
    StarRocksDependencyContractError, StarRocksDependencyContractErrorKind,
    StarRocksFragmentDecodeError,
};
use super::instance::{
    DecodedStarRocksInstanceParts, StarRocksDecodeFacts, decode_instance_parts,
    decode_scan_contracts_and_raw_ranges,
};
use super::layout::{build_tuple_slot_order, infer_tuple_slot_order, reorder_tuple_slots};
use super::node::{
    ScanRangeCarrier, StarRocksPlanDecodeContext, decode_broker_file_program_facts, lower_plan,
};
use super::sink::fragment::{DecodedStarRocksFragmentSink, decode_fragment_sink};

pub(crate) struct StarRocksDecodeInput<'a> {
    pub(crate) fragment: &'a planner::TPlanFragment,
    pub(crate) descriptors: Option<&'a descriptors::TDescriptorTable>,
    pub(crate) params: &'a internal_service::TPlanFragmentExecParams,
    pub(crate) query_options: Option<&'a internal_service::TQueryOptions>,
    pub(crate) query_globals: Option<&'a internal_service::TQueryGlobals>,
    pub(crate) db_name: Option<&'a str>,
    pub(crate) coord: Option<&'a types::TNetworkAddress>,
    pub(crate) backend_num: Option<i32>,
    pub(crate) pipeline_dop: i32,
    pub(crate) group_execution_scan_dop: Option<i32>,
    pub(crate) batch_exchange_sender_counts: &'a HashMap<i32, usize>,
    pub(crate) typed_result_sink: bool,
    pub(crate) facts: &'a StarRocksDecodeFacts,
    pub(crate) table_schema_provider:
        Option<Arc<dyn novarocks::connector::starrocks::ports::TableSchemaProvider>>,
    pub(crate) schema_load_provider:
        Option<Arc<dyn novarocks::connector::schema::SchemaLoadProvider>>,
    pub(crate) sink_frontend_provider:
        Option<Arc<dyn novarocks::connector::starrocks::ports::SinkFrontendProvider>>,
    pub(crate) starlet_metadata_provider:
        Option<Arc<dyn novarocks::connector::starrocks::ports::StarletMetadataProvider>>,
    pub(crate) storage_metadata_provider:
        Option<Arc<dyn novarocks::connector::starrocks::ports::StorageMetadataProvider>>,
    pub compat_iceberg_execution:
        Option<&'a std::sync::Arc<novarocks_spi::connector::ConnectorExecutionBinding>>,
    pub(crate) compat_connector_writer: Option<novarocks_spi::connector::ConnectorWriterIdentity>,
}

#[derive(Debug)]
pub(crate) struct StarRocksFragmentDraft {
    parts: DecodedDraftParts,
    external_dependencies: Vec<StarRocksExternalDependency>,
    query_profile_patches: Vec<QueryProfilePatch>,
    lake_meta_values_patches: Vec<super::node::LakeMetaValuesPatch>,
}

impl StarRocksFragmentDraft {
    pub(crate) fn external_dependencies(&self) -> &[StarRocksExternalDependency] {
        &self.external_dependencies
    }
}

#[derive(Debug)]
pub(crate) struct DecodedStarRocksFragment {
    submission: FragmentSubmission,
    metadata: StarRocksSubmissionMetadata,
}

impl DecodedStarRocksFragment {
    pub(crate) fn into_parts(self) -> (FragmentSubmission, StarRocksSubmissionMetadata) {
        (self.submission, self.metadata)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksSubmissionMetadata {
    descriptor_snapshot: Option<DescriptorSnapshot>,
    row_position_descriptors: HashMap<i32, RowPositionDescriptor>,
    result_override: Option<(ResultPresentation, Option<Vec<ResultProjection>>)>,
    typed_result_sink: bool,
    root_sink_dop: Option<i32>,
    group_execution_scan_dop: Option<i32>,
    report_destination: Option<StarRocksReportDestination>,
    lookup_fetcher_lifecycles: HashMap<i32, LookupFetcherLifecycle>,
    lookup_close_targets: Vec<StarRocksLookupCloseTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksReportDestination {
    Coordinator(novarocks::runtime::endpoint::RuntimeEndpoint),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StarRocksLookupCloseTarget {
    lookup_node_id: i32,
    host: String,
    port: u16,
}

impl StarRocksLookupCloseTarget {
    pub(crate) const fn lookup_node_id(&self) -> i32 {
        self.lookup_node_id
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) const fn port(&self) -> u16 {
        self.port
    }
}

impl StarRocksSubmissionMetadata {
    pub(crate) fn descriptor_snapshot(&self) -> Option<&DescriptorSnapshot> {
        self.descriptor_snapshot.as_ref()
    }

    pub(crate) fn row_position_descriptors(&self) -> &HashMap<i32, RowPositionDescriptor> {
        &self.row_position_descriptors
    }

    pub(crate) fn result_override(
        &self,
    ) -> Option<&(ResultPresentation, Option<Vec<ResultProjection>>)> {
        self.result_override.as_ref()
    }

    fn result_write_spec(
        &self,
        fragment_instance_id: novarocks_types::UniqueId,
    ) -> Option<ResultWriteSpec> {
        self.result_override
            .as_ref()
            .map(|(presentation, projections)| {
                ResultWriteSpec::new(
                    fragment_instance_id,
                    *presentation,
                    projections.clone(),
                    self.typed_result_sink,
                )
            })
    }

    pub(crate) const fn root_sink_dop(&self) -> Option<i32> {
        self.root_sink_dop
    }

    pub(crate) const fn group_execution_scan_dop(&self) -> Option<i32> {
        self.group_execution_scan_dop
    }

    pub(crate) fn report_destination(&self) -> Option<&StarRocksReportDestination> {
        self.report_destination.as_ref()
    }

    pub(crate) fn lookup_fetcher_lifecycles(&self) -> &HashMap<i32, LookupFetcherLifecycle> {
        &self.lookup_fetcher_lifecycles
    }

    pub(crate) fn lookup_close_targets(&self) -> &[StarRocksLookupCloseTarget] {
        &self.lookup_close_targets
    }

    pub(crate) fn into_prepare_context(
        &self,
        profiler: Option<novarocks::runtime::profile::Profiler>,
        mem_tracker: Option<std::sync::Arc<novarocks::runtime::mem_tracker::MemTracker>>,
        exchange_transmitter: std::sync::Arc<
            dyn novarocks::runtime::fragment::ExchangeFrameTransmitter,
        >,
        lookup_client: std::sync::Arc<dyn novarocks::runtime::fragment::FragmentLookupClient>,
        result_writer: std::sync::Arc<dyn FragmentResultWriter>,
        event_sink: std::sync::Arc<dyn FragmentEventSink>,
        fragment_instance_id: novarocks_types::UniqueId,
    ) -> novarocks::runtime::fragment::FragmentPrepareContext {
        novarocks::runtime::fragment::FragmentPrepareContext::new_with_execution_overrides(
            profiler,
            mem_tracker,
            self.result_write_spec(fragment_instance_id),
            self.root_sink_dop,
            self.group_execution_scan_dop,
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
        )
    }
}

#[derive(Debug)]
struct DecodedDraftParts {
    program: FragmentProgramBuilder,
    instance: FragmentInstanceSpec,
    metadata: StarRocksSubmissionMetadata,
}

pub(crate) fn prepare_fragment_submission(
    input: StarRocksDecodeInput<'_>,
) -> Result<StarRocksFragmentDraft, StarRocksFragmentDecodeError> {
    let instance = decode_input_instance(&input)?;
    let dependencies = StarRocksExternalDependencyDraft::new_with_table_schema_provider(
        instance.report_endpoint.clone(),
        BTreeMap::new(),
        input.table_schema_provider.clone(),
        input.schema_load_provider.clone(),
        input.sink_frontend_provider.clone(),
    )
    .with_starlet_metadata_provider(input.starlet_metadata_provider.clone())
    .with_storage_metadata_provider(input.storage_metadata_provider.clone());
    let parts = decode_draft_parts(&input, instance, &dependencies)?;
    let external_dependencies = dependencies.external_dependencies();
    let query_profile_patches = dependencies.query_profile_patches();
    let lake_meta_values_patches = dependencies.lake_meta_values_patches();
    Ok(StarRocksFragmentDraft {
        parts,
        external_dependencies,
        query_profile_patches,
        lake_meta_values_patches,
    })
}

pub(crate) fn finish_fragment_submission(
    mut draft: StarRocksFragmentDraft,
    resolved: StarRocksResolvedDependencies,
) -> Result<DecodedStarRocksFragment, StarRocksFragmentDecodeError> {
    validate_resolved_dependencies(&draft.external_dependencies, &resolved)?;
    for patch in &draft.query_profile_patches {
        let Some(StarRocksResolvedDependencyValue::QueryProfile(profile)) =
            resolved.get(patch.dependency_id())
        else {
            unreachable!("exact dependency contract was validated");
        };
        query_profile_arena_mut(&mut draft.parts.program, patch.target().owner())?
            .replace_literal(
                patch.target().expr_id(),
                novarocks::exec::expr::LiteralValue::Utf8(profile.clone()),
            )
            .map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_value(
                    FieldPath::root("exec_plan_fragment").field("resolved_dependencies"),
                    detail,
                )
            })?;
    }
    for patch in &draft.lake_meta_values_patches {
        let Some(StarRocksResolvedDependencyValue::LakeMetaStorage(facts)) =
            resolved.get(patch.dependency_id())
        else {
            unreachable!("exact dependency contract was validated");
        };
        let chunk = patch.materialize(facts).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                FieldPath::root("exec_plan_fragment").field("resolved_dependencies"),
                detail,
            )
        })?;
        if !replace_values_chunk(
            draft.parts.program.plan_mut().root_mut(),
            patch.node_id(),
            chunk,
        ) {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                FieldPath::root("exec_plan_fragment").field("resolved_dependencies"),
                format!(
                    "LAKE_META_SCAN_NODE patch target {} is missing",
                    patch.node_id()
                ),
            ));
        }
    }
    let program = draft
        .parts
        .program
        .finish()
        .map_err(StarRocksFragmentDecodeError::from)?;
    let submission = FragmentSubmission::try_new(Arc::new(program), draft.parts.instance)
        .map_err(StarRocksFragmentDecodeError::Binding)?;
    Ok(DecodedStarRocksFragment {
        submission,
        metadata: draft.parts.metadata,
    })
}

fn query_profile_arena_mut(
    program: &mut FragmentProgramBuilder,
    owner: FragmentExprArenaOwner,
) -> Result<&mut ExprArena, StarRocksFragmentDecodeError> {
    if owner == FragmentExprArenaOwner::Plan {
        return Ok(program.plan_mut().arena_mut());
    }
    let sink = program.sink_program_mut();
    let arena = match (owner, sink) {
        (
            FragmentExprArenaOwner::DataStream,
            novarocks::exec::fragment::sink::FragmentSinkProgram::DataStream(program),
        ) => program.partition_arena_mut(),
        (
            FragmentExprArenaOwner::MultiCastDataStream,
            novarocks::exec::fragment::sink::FragmentSinkProgram::MultiCastDataStream(program),
        ) => program.partition_arena_mut(),
        (
            FragmentExprArenaOwner::SplitDataStream,
            novarocks::exec::fragment::sink::FragmentSinkProgram::SplitDataStream(program),
        ) => program.arena_mut(),
        (
            FragmentExprArenaOwner::IcebergTable,
            novarocks::exec::fragment::sink::FragmentSinkProgram::ConnectorWrite(program),
        ) => program.expression_projection_arena_mut().ok_or_else(|| {
            query_profile_patch_target_error(owner, "connector write projection is missing")
        })?,
        (
            FragmentExprArenaOwner::ChangeStreamRouter,
            novarocks::exec::fragment::sink::FragmentSinkProgram::SplitDataStream(program),
        ) => program.arena_mut(),
        _ => {
            return Err(query_profile_patch_target_error(
                owner,
                "owner does not match the retained fragment sink",
            ));
        }
    };
    Ok(arena)
}

fn query_profile_patch_target_error(
    owner: FragmentExprArenaOwner,
    detail: impl std::fmt::Display,
) -> StarRocksFragmentDecodeError {
    StarRocksFragmentDecodeError::invalid_value(
        FieldPath::root("exec_plan_fragment").field("resolved_dependencies"),
        format!("query-profile patch target {owner:?} is invalid: {detail}"),
    )
}

fn replace_values_chunk(
    node: &mut ExecNode,
    target_node_id: i32,
    chunk: novarocks::exec::chunk::Chunk,
) -> bool {
    match &mut node.kind {
        ExecNodeKind::Values(values) if values.node_id == target_node_id => {
            values.chunk = chunk;
            true
        }
        ExecNodeKind::AssertNumRows(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::Project(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::Filter(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::Repeat(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::ChangeEventExpand(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::Limit(value) => replace_values_chunk(&mut value.input, target_node_id, chunk),
        ExecNodeKind::Fetch(value) => replace_values_chunk(&mut value.input, target_node_id, chunk),
        ExecNodeKind::Aggregate(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::Sort(value) => replace_values_chunk(&mut value.input, target_node_id, chunk),
        ExecNodeKind::TableFunction(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::Analytic(value) => {
            replace_values_chunk(&mut value.input, target_node_id, chunk)
        }
        ExecNodeKind::RuntimeFilterConsumer(value) => {
            replace_values_chunk(value.input_mut(), target_node_id, chunk)
        }
        ExecNodeKind::Join(value) => {
            replace_values_chunk(&mut value.left, target_node_id, chunk.clone())
                || replace_values_chunk(&mut value.right, target_node_id, chunk)
        }
        ExecNodeKind::NestedLoopJoin(value) => {
            replace_values_chunk(&mut value.left, target_node_id, chunk.clone())
                || replace_values_chunk(&mut value.right, target_node_id, chunk)
        }
        ExecNodeKind::UnionAll(value) => value
            .inputs
            .iter_mut()
            .any(|input| replace_values_chunk(input, target_node_id, chunk.clone())),
        ExecNodeKind::SetOp(value) => value
            .inputs
            .iter_mut()
            .any(|input| replace_values_chunk(input, target_node_id, chunk.clone())),
        ExecNodeKind::Values(_)
        | ExecNodeKind::ExchangeSource(_)
        | ExecNodeKind::Scan(_)
        | ExecNodeKind::LookUp(_) => false,
    }
}

fn decode_input_instance(
    input: &StarRocksDecodeInput<'_>,
) -> Result<DecodedStarRocksInstanceParts, StarRocksFragmentDecodeError> {
    decode_instance_parts(
        input.params,
        input.query_options,
        input.coord,
        input.backend_num,
        input.pipeline_dop,
        input.batch_exchange_sender_counts,
        input.typed_result_sink,
        input.facts,
        FieldPath::root("exec_plan_fragment"),
    )
}

fn decode_draft_parts(
    input: &StarRocksDecodeInput<'_>,
    instance: DecodedStarRocksInstanceParts,
    dependencies: &StarRocksExternalDependencyDraft,
) -> Result<DecodedDraftParts, StarRocksFragmentDecodeError> {
    let plan_path = FieldPath::root("exec_plan_fragment")
        .field("fragment")
        .field("plan");
    let plan = input.fragment.plan.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(plan_path.clone(), "PlanFragment requires plan")
    })?;
    let sink_path = FieldPath::root("exec_plan_fragment")
        .field("fragment")
        .field("output_sink");
    let sink = input.fragment.output_sink.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            sink_path.clone(),
            "PlanFragment requires output_sink",
        )
    })?;
    let mut tuple_slots = build_tuple_slot_order(input.descriptors);
    let inferred = infer_tuple_slot_order(input.fragment);
    if tuple_slots.is_empty() {
        tuple_slots = inferred;
    } else {
        for (tuple_id, slots) in inferred {
            tuple_slots.entry(tuple_id).or_insert(slots);
        }
    }
    reorder_tuple_slots(&mut tuple_slots, input.descriptors);
    let layout_hints = tuple_slots.clone();
    let mut arena = ExprArena::default();
    let allow_throw_exception = instance.query_options.allow_throw_exception()
        || input.query_options.is_some_and(|options| {
            matches!(
                options.overflow_mode,
                Some(mode) if mode == internal_service::TOverflowMode::REPORT_ERROR
            )
        });
    arena.set_allow_throw_exception(allow_throw_exception);
    arena.set_session_time_zone(
        input
            .query_globals
            .and_then(|globals| globals.time_zone.clone()),
    );
    let (scan_contracts, raw_scan_ranges) = decode_scan_contracts_and_raw_ranges(
        &plan.nodes,
        &instance.scan_ranges,
        input.descriptors,
        input.facts,
        FieldPath::root("exec_plan_fragment")
            .field("params")
            .field("per_node_scan_ranges"),
    )?;
    // Capture slot for the enriched `BoundScanRanges` the scan decoders produce;
    // drained into the instance's scan assignments after `lower_plan`.
    let captured_scan_ranges: RefCell<BTreeMap<FragmentNodeId, BoundScanRanges>> =
        RefCell::new(BTreeMap::new());
    let broker_file_program_facts = decode_broker_file_program_facts(
        &plan.nodes,
        &instance.scan_ranges,
        &mut arena,
        plan_path.clone().field("nodes"),
        FieldPath::root("exec_plan_fragment")
            .field("params")
            .field("per_node_scan_ranges"),
    )?;
    let lake_scan_program_facts = super::decode_lake_scan_program_facts(
        &plan.nodes,
        &instance.scan_ranges,
        FieldPath::root("exec_plan_fragment")
            .field("params")
            .field("per_node_scan_ranges"),
    )?;
    let lake_meta_scan_range_facts = super::decode_lake_meta_scan_range_facts(
        &plan.nodes,
        &instance.scan_ranges,
        FieldPath::root("exec_plan_fragment")
            .field("params")
            .field("per_node_scan_ranges"),
    )?;
    let plan_context = StarRocksPlanDecodeContext::new(
        Some(instance.query_id),
        Some(instance.fragment_instance_id.get()),
        Some(ScanRangeCarrier::new(
            &raw_scan_ranges,
            &captured_scan_ranges,
        )),
        Some(&broker_file_program_facts),
        Some(&lake_scan_program_facts),
        Some(&lake_meta_scan_range_facts),
        Some(&instance.per_exchange_sender_counts),
        &instance.batch_exchange_sender_counts,
        instance.query_options.clone(),
        input.facts,
        input.compat_iceberg_execution,
    );
    let last_query_id = input
        .query_globals
        .and_then(|globals| globals.last_query_id.as_deref());
    let lowered = lower_plan(
        plan,
        &mut arena,
        &tuple_slots,
        input.descriptors,
        input.fragment.query_global_dicts.as_deref(),
        input.fragment.query_global_dict_exprs.as_ref(),
        &plan_context,
        input.db_name,
        &layout_hints,
        last_query_id,
        Some(dependencies),
        FieldPath::root("exec_plan_fragment")
            .field("fragment")
            .field("query_global_dicts"),
        FieldPath::root("exec_plan_fragment")
            .field("fragment")
            .field("query_global_dict_exprs"),
        plan_path,
    )?;
    let compat_connector_context = input
        .compat_connector_writer
        .as_ref()
        .map(|_| {
            let (_, query_expire) = query_expire_durations(Some(&instance.query_options));
            ConnectorRequestContext::try_new(
                Instant::now() + query_expire,
                StarRocksFragmentQueryRuntime::new().connector_cancellation(instance.query_id),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(
                    sink_path.clone(),
                    format!("build compat connector writer context: {error}"),
                )
            })
        })
        .transpose()?;
    let DecodedStarRocksFragmentSink {
        spec,
        assignment,
        result_override,
        root_sink_dop,
    } = decode_fragment_sink(
        sink,
        &input.fragment,
        &instance.destinations,
        instance.sender_id,
        input.descriptors,
        &mut arena,
        &lowered,
        last_query_id,
        input
            .query_globals
            .and_then(|globals| globals.time_zone.as_deref()),
        dependencies,
        input.compat_iceberg_execution,
        input.compat_connector_writer.as_ref(),
        compat_connector_context.as_ref(),
        sink_path,
        FieldPath::root("exec_plan_fragment").field("fragment"),
    )?;
    let runtime_filters = RuntimeFilterContract::default();
    let plan = novarocks::exec::node::ExecPlanBuilder::new(arena, lowered.node)
        .finish()
        .map_err(StarRocksFragmentDecodeError::from)?;
    let exchange_contracts = collect_exchange_contracts(plan.root())?;
    let exchange_assignments = decode_exchange_assignments(
        &exchange_contracts,
        &instance.per_exchange_sender_counts,
        &instance.batch_exchange_sender_counts,
    )?;
    let mut program = novarocks::exec::fragment::program::FragmentProgramBuilder::new(
        plan,
        spec,
        FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
    )
    .scan_sources(scan_contracts)
    .exchange_inputs(exchange_contracts)
    .runtime_filters(runtime_filters);
    let mut row_position_descriptors = HashMap::new();
    collect_row_position_descriptors(program.plan_mut().root(), &mut row_position_descriptors)
        .map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                FieldPath::root("exec_plan_fragment").field("fragment"),
                detail,
            )
        })?;
    let descriptor_snapshot = input
        .descriptors
        .map(super::descriptor::descriptor_snapshot_from_thrift)
        .transpose()
        .map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                FieldPath::root("exec_plan_fragment").field("desc_tbl"),
                detail,
            )
        })?;
    let lookup_fetcher_lifecycles = decode_lookup_fetcher_lifecycles(input)?;
    let lookup_close_targets = decode_lookup_close_targets(input)?;
    let report_destination = instance
        .report_endpoint
        .clone()
        .map(StarRocksReportDestination::Coordinator);
    // Drain the enriched per-node `BoundScanRanges` captured during `lower_plan`
    // into the instance's scan assignments (`materialize_scan_bindings` binds
    // these). Uses `borrow_mut` (not `into_inner`) so `plan_context`'s borrow of
    // `captured_scan_ranges` can coexist.
    let scan_assignments =
        ScanAssignments::try_new(std::mem::take(&mut captured_scan_ranges.borrow_mut()))
            .map_err(StarRocksFragmentDecodeError::Binding)?;
    let instance_spec = FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        instance.query_id,
        instance.fragment_instance_id,
        scan_assignments,
        exchange_assignments,
        assignment,
        FragmentRuntimeOptions::new(instance.query_options, instance.typed_result_sink),
        instance.pipeline_dop,
        instance.backend_num,
    );
    Ok(DecodedDraftParts {
        program,
        instance: instance_spec,
        metadata: StarRocksSubmissionMetadata {
            descriptor_snapshot,
            row_position_descriptors,
            result_override,
            typed_result_sink: instance.typed_result_sink,
            root_sink_dop,
            group_execution_scan_dop: input.group_execution_scan_dop,
            report_destination,
            lookup_fetcher_lifecycles,
            lookup_close_targets,
        },
    })
}

fn decode_lookup_fetcher_lifecycles(
    input: &StarRocksDecodeInput<'_>,
) -> Result<HashMap<i32, LookupFetcherLifecycle>, StarRocksFragmentDecodeError> {
    let Some(plan) = input.fragment.plan.as_ref() else {
        return Ok(HashMap::new());
    };
    let mut lifecycles = HashMap::new();
    for node in plan
        .nodes
        .iter()
        .filter(|node| node.node_type == crate::thrift::plan_nodes::TPlanNodeType::LOOKUP_NODE)
    {
        let lifecycle = match input
            .params
            .per_look_up_num_fetchers
            .as_ref()
            .and_then(|counts| counts.get(&node.node_id))
        {
            Some(count) => {
                LookupFetcherLifecycle::Exact(usize::try_from(*count).map_err(|_| {
                    StarRocksFragmentDecodeError::out_of_range(
                        FieldPath::root("exec_plan_fragment")
                            .field("params")
                            .field("per_look_up_num_fetchers")
                            .map_key(node.node_id.to_string()),
                        format!(
                            "lookup node {} has negative fetcher count {count}",
                            node.node_id
                        ),
                    )
                })?)
            }
            None => LookupFetcherLifecycle::Unknown,
        };
        lifecycles.insert(node.node_id, lifecycle);
    }
    Ok(lifecycles)
}

fn decode_lookup_close_targets(
    input: &StarRocksDecodeInput<'_>,
) -> Result<Vec<StarRocksLookupCloseTarget>, StarRocksFragmentDecodeError> {
    let Some(plan) = input.fragment.plan.as_ref() else {
        return Ok(Vec::new());
    };
    let mut targets = std::collections::HashSet::new();
    for (node_index, node) in plan.nodes.iter().enumerate() {
        if node.node_type != crate::thrift::plan_nodes::TPlanNodeType::FETCH_NODE {
            continue;
        }
        let Some(fetch) = node.fetch_node.as_ref() else {
            continue;
        };
        let (Some(lookup_node_id), Some(nodes_info)) =
            (fetch.target_node_id, fetch.nodes_info.as_ref())
        else {
            continue;
        };
        for (target_index, target) in nodes_info.nodes.iter().enumerate() {
            if target.host.trim().is_empty() {
                return Err(StarRocksFragmentDecodeError::invalid_value(
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("plan")
                        .field("nodes")
                        .index(node_index)
                        .field("fetch_node")
                        .field("nodes_info")
                        .field("nodes")
                        .index(target_index)
                        .field("host"),
                    "lookup close target host is empty",
                ));
            }
            let port = u16::try_from(target.async_internal_port).map_err(|_| {
                StarRocksFragmentDecodeError::out_of_range(
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("plan")
                        .field("nodes")
                        .index(node_index)
                        .field("fetch_node")
                        .field("nodes_info")
                        .field("nodes")
                        .index(target_index)
                        .field("async_internal_port"),
                    format!(
                        "lookup async_internal_port {} is out of u16 range",
                        target.async_internal_port
                    ),
                )
            })?;
            targets.insert(StarRocksLookupCloseTarget {
                lookup_node_id,
                host: target.host.clone(),
                port,
            });
        }
    }
    Ok(targets.into_iter().collect())
}

fn collect_exchange_contracts(
    root: &ExecNode,
) -> Result<BTreeMap<FragmentNodeId, ExchangeInputContract>, StarRocksFragmentDecodeError> {
    fn visit(
        node: &ExecNode,
        contracts: &mut BTreeMap<FragmentNodeId, ExchangeInputContract>,
    ) -> Result<(), StarRocksFragmentDecodeError> {
        match &node.kind {
            ExecNodeKind::ExchangeSource(exchange) => {
                let id = FragmentNodeId::new(exchange.node_id);
                if contracts
                    .insert(
                        id,
                        ExchangeInputContract::new(Arc::clone(&exchange.expected_chunk_schema)),
                    )
                    .is_some()
                {
                    return Err(StarRocksFragmentDecodeError::invalid_value(
                        FieldPath::root("exec_plan_fragment")
                            .field("fragment")
                            .field("plan"),
                        format!("duplicate exchange node id {}", id.get()),
                    ));
                }
            }
            ExecNodeKind::AssertNumRows(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Project(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Filter(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Repeat(value) => visit(&value.input, contracts)?,
            ExecNodeKind::ChangeEventExpand(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Limit(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Fetch(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Aggregate(value) => visit(&value.input, contracts)?,
            ExecNodeKind::NestedLoopJoin(value) => {
                visit(&value.left, contracts)?;
                visit(&value.right, contracts)?;
            }
            ExecNodeKind::Join(value) => {
                visit(&value.left, contracts)?;
                visit(&value.right, contracts)?;
            }
            ExecNodeKind::Sort(value) => visit(&value.input, contracts)?,
            ExecNodeKind::TableFunction(value) => visit(&value.input, contracts)?,
            ExecNodeKind::Analytic(value) => visit(&value.input, contracts)?,
            ExecNodeKind::UnionAll(value) => {
                for input in &value.inputs {
                    visit(input, contracts)?;
                }
            }
            ExecNodeKind::SetOp(value) => {
                for input in &value.inputs {
                    visit(input, contracts)?;
                }
            }
            ExecNodeKind::RuntimeFilterConsumer(value) => visit(value.input(), contracts)?,
            ExecNodeKind::Values(_) | ExecNodeKind::Scan(_) | ExecNodeKind::LookUp(_) => {}
        }
        Ok(())
    }
    let mut contracts = BTreeMap::new();
    visit(root, &mut contracts)?;
    Ok(contracts)
}

fn decode_exchange_assignments(
    contracts: &BTreeMap<FragmentNodeId, ExchangeInputContract>,
    per_exchange: &BTreeMap<i32, i32>,
    batch: &HashMap<i32, usize>,
) -> Result<ExchangeInputAssignments, StarRocksFragmentDecodeError> {
    let mut assignments = BTreeMap::new();
    for id in contracts.keys() {
        let raw = per_exchange
            .get(&id.get())
            .and_then(|value| usize::try_from(*value).ok())
            .or_else(|| batch.get(&id.get()).copied())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    FieldPath::root("exec_plan_fragment")
                        .field("params")
                        .field("per_exch_num_senders")
                        .map_key(id.get().to_string()),
                    "exchange sender count must be positive",
                )
            })?;
        assignments.insert(*id, ExchangeInputAssignment::new(raw));
    }
    Ok(ExchangeInputAssignments::new(assignments))
}

fn collect_row_position_descriptors(
    node: &ExecNode,
    output: &mut HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    fn merge(
        output: &mut HashMap<i32, RowPositionDescriptor>,
        incoming: &HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        for (tuple_id, descriptor) in incoming {
            if let Some(existing) = output.get(tuple_id) {
                if existing.row_position_type != descriptor.row_position_type
                    || existing.row_source_slot != descriptor.row_source_slot
                    || existing.fetch_ref_slots != descriptor.fetch_ref_slots
                    || existing.lookup_ref_slots != descriptor.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={tuple_id}"
                    ));
                }
            } else {
                output.insert(*tuple_id, descriptor.clone());
            }
        }
        Ok(())
    }
    match &node.kind {
        ExecNodeKind::LookUp(value) => merge(output, &value.row_pos_descs)?,
        ExecNodeKind::Fetch(value) => {
            merge(output, &value.row_pos_descs)?;
            collect_row_position_descriptors(&value.input, output)?;
        }
        ExecNodeKind::AssertNumRows(value) => {
            collect_row_position_descriptors(&value.input, output)?
        }
        ExecNodeKind::Project(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::Filter(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::Repeat(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::ChangeEventExpand(value) => {
            collect_row_position_descriptors(&value.input, output)?
        }
        ExecNodeKind::Limit(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::Aggregate(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::Join(value) => {
            collect_row_position_descriptors(&value.left, output)?;
            collect_row_position_descriptors(&value.right, output)?;
        }
        ExecNodeKind::NestedLoopJoin(value) => {
            collect_row_position_descriptors(&value.left, output)?;
            collect_row_position_descriptors(&value.right, output)?;
        }
        ExecNodeKind::Sort(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::TableFunction(value) => {
            collect_row_position_descriptors(&value.input, output)?
        }
        ExecNodeKind::Analytic(value) => collect_row_position_descriptors(&value.input, output)?,
        ExecNodeKind::UnionAll(value) => {
            for input in &value.inputs {
                collect_row_position_descriptors(input, output)?;
            }
        }
        ExecNodeKind::SetOp(value) => {
            for input in &value.inputs {
                collect_row_position_descriptors(input, output)?;
            }
        }
        ExecNodeKind::RuntimeFilterConsumer(value) => {
            collect_row_position_descriptors(value.input(), output)?
        }
        ExecNodeKind::Values(_) | ExecNodeKind::ExchangeSource(_) | ExecNodeKind::Scan(_) => {}
    }
    Ok(())
}

fn validate_resolved_dependencies(
    requirements: &[StarRocksExternalDependency],
    resolved: &StarRocksResolvedDependencies,
) -> Result<(), StarRocksDependencyContractError> {
    for requirement in requirements {
        let id = requirement.id();
        let Some(value) = resolved.get(id) else {
            return Err(StarRocksDependencyContractError::new(
                StarRocksDependencyContractErrorKind::Missing,
                id,
                "declared dependency was not resolved",
            ));
        };
        let matches = matches!(
            (requirement, value),
            (
                StarRocksExternalDependency::QueryProfile { .. },
                StarRocksResolvedDependencyValue::QueryProfile(_)
            ) | (
                StarRocksExternalDependency::LakeMetaStorage { .. },
                StarRocksResolvedDependencyValue::LakeMetaStorage(_)
            )
        );
        if !matches {
            return Err(StarRocksDependencyContractError::new(
                StarRocksDependencyContractErrorKind::WrongKind,
                id,
                format!("resolved dependency has wrong kind {}", value.kind_name()),
            ));
        }
    }
    let required = requirements
        .iter()
        .map(StarRocksExternalDependency::id)
        .collect::<BTreeSet<_>>();
    if let Some((&id, _)) = resolved.iter().find(|(id, _)| !required.contains(id)) {
        return Err(StarRocksDependencyContractError::new(
            StarRocksDependencyContractErrorKind::Extra,
            id,
            "resolution contains an undeclared dependency",
        ));
    }
    Ok(())
}

fn split_resolved_dependencies(
    requirements: &[StarRocksExternalDependency],
    resolved: StarRocksResolvedDependencies,
) -> (
    BTreeMap<String, String>,
    BTreeMap<u64, LakeMetaStorageFacts>,
) {
    let mut profiles = BTreeMap::new();
    let mut lake_meta = BTreeMap::new();
    for requirement in requirements {
        match (requirement, resolved.get(requirement.id())) {
            (
                StarRocksExternalDependency::QueryProfile { query_id, .. },
                Some(StarRocksResolvedDependencyValue::QueryProfile(value)),
            ) => {
                profiles.insert(query_id.clone(), value.clone());
            }
            (
                StarRocksExternalDependency::LakeMetaStorage { id, .. },
                Some(StarRocksResolvedDependencyValue::LakeMetaStorage(value)),
            ) => {
                lake_meta.insert(*id, value.clone());
            }
            _ => unreachable!("resolution contract was validated"),
        }
    }
    (profiles, lake_meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TStringLiteral};
    use crate::thrift::{data_sinks, partitions, plan_nodes};
    use novarocks::exec::expr::{ExprNode, LiteralValue};
    use novarocks::exec::fragment::program::FragmentSinkKind;
    use novarocks::exec::fragment::sink::FragmentSinkProgram;
    use novarocks::exec::node::ExecNodeKind;
    use novarocks::runtime::endpoint::FragmentDestination;
    use novarocks::runtime::exchange::{ExchangeKey, snapshot_receiver_state};
    use novarocks::runtime::result_buffer::{self, TryFetchResult};
    use novarocks::runtime::runtime_filter_observability::{
        QueryKey, RuntimeFilterLifecycleRegistry,
    };
    use novarocks_types::QueryId;
    use novarocks_types::UniqueId;
    use std::sync::LazyLock;

    static EMPTY_BATCH_SENDERS: LazyLock<HashMap<i32, usize>> = LazyLock::new(HashMap::new);
    static EMPTY_DECODE_FACTS: LazyLock<StarRocksDecodeFacts> =
        LazyLock::new(StarRocksDecodeFacts::default);

    fn empty_set_node() -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode::new(
            11,
            plan_nodes::TPlanNodeType::EMPTY_SET_NODE,
            0,
            -1,
            vec![],
            vec![],
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn noop_sink() -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::NOOP_SINK,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn values_noop_fragment() -> planner::TPlanFragment {
        planner::TPlanFragment::new(
            plan_nodes::TPlan::new(vec![empty_set_node()]),
            None,
            noop_sink(),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None,
                None,
                None,
            ),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn test_expr_node(
        node_type: TExprNodeType,
        type_: types::TTypeDesc,
        num_children: i32,
    ) -> TExprNode {
        TExprNode {
            node_type,
            type_,
            opcode: None,
            num_children,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: -1,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn get_query_profile_expr(query_id: &str) -> TExpr {
        let string_type = crate::protocol::starrocks::type_mapping::thrift_type_desc_from_primitive(
            types::TPrimitiveType::VARCHAR,
        );
        let mut call = test_expr_node(TExprNodeType::FUNCTION_CALL, string_type.clone(), 1);
        call.fn_ = Some(types::TFunction::new(
            types::TFunctionName::new(None, "get_query_profile".to_string()),
            types::TFunctionBinaryType::BUILTIN,
            vec![string_type.clone()],
            string_type.clone(),
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let mut literal = test_expr_node(TExprNodeType::STRING_LITERAL, string_type, 0);
        literal.string_literal = Some(TStringLiteral::new(query_id.to_string()));
        TExpr::new(vec![call, literal])
    }

    fn profile_partitioned_stream_sink(query_id: &str) -> data_sinks::TDataStreamSink {
        data_sinks::TDataStreamSink::new(
            7,
            partitions::TDataPartition::new(
                partitions::TPartitionType::HASH_PARTITIONED,
                vec![get_query_profile_expr(query_id)],
                None,
                None,
            ),
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn fragment_with_sink(sink: data_sinks::TDataSink) -> planner::TPlanFragment {
        let mut fragment = values_noop_fragment();
        fragment.output_sink = Some(sink);
        fragment
    }

    fn resolved_profile_for(draft: &StarRocksFragmentDraft) -> StarRocksResolvedDependencies {
        let [StarRocksExternalDependency::QueryProfile { id, .. }] = draft.external_dependencies()
        else {
            panic!("fixture must declare exactly one query-profile dependency");
        };
        let mut resolved = StarRocksResolvedDependencies::default();
        resolved.insert(
            *id,
            StarRocksResolvedDependencyValue::QueryProfile("resolved-profile".to_string()),
        );
        resolved
    }

    fn assert_resolved_profile(arena: &ExprArena, expr_id: novarocks::exec::expr::ExprId) {
        assert!(matches!(
            arena.node(expr_id),
            Some(ExprNode::Literal(LiteralValue::Utf8(value))) if value == "resolved-profile"
        ));
    }

    fn params(query: UniqueId, finst: UniqueId) -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId {
                hi: query.high(),
                lo: query.low(),
            },
            types::TUniqueId {
                hi: finst.high(),
                lo: finst.low(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn decode_input<'a>(
        fragment: &'a planner::TPlanFragment,
        params: &'a internal_service::TPlanFragmentExecParams,
    ) -> StarRocksDecodeInput<'a> {
        StarRocksDecodeInput {
            fragment,
            descriptors: None,
            params,
            query_options: None,
            query_globals: None,
            db_name: None,
            coord: None,
            backend_num: Some(3),
            pipeline_dop: 1,
            group_execution_scan_dop: None,
            batch_exchange_sender_counts: &EMPTY_BATCH_SENDERS,
            typed_result_sink: false,
            facts: &EMPTY_DECODE_FACTS,
            table_schema_provider: None,
            schema_load_provider: None,
            sink_frontend_provider: None,
            starlet_metadata_provider: None,
            storage_metadata_provider: None,
            compat_iceberg_execution: None,
            compat_connector_writer: None,
        }
    }

    #[test]
    fn values_noop_decodes_to_validated_starrocks_submission() {
        let query = UniqueId::new(11, 12);
        let finst = UniqueId::new(21, 22);
        let fragment = values_noop_fragment();
        let params = params(query, finst);
        let draft = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect("prepare values/noop fragment");
        assert!(draft.external_dependencies().is_empty());
        let decoded = finish_fragment_submission(draft, StarRocksResolvedDependencies::default())
            .expect("finish values/noop fragment");
        let (submission, metadata) = decoded.into_parts();
        assert_eq!(submission.instance().query_id(), QueryId::new(11, 12));
        assert_eq!(submission.instance().fragment_instance_id().get(), finst);
        assert_eq!(submission.program().sink().kind(), FragmentSinkKind::Noop);
        assert!(matches!(
            submission.program().plan().root().kind,
            ExecNodeKind::Values(_)
        ));
        assert!(metadata.descriptor_snapshot().is_none());
    }

    #[test]
    fn lookup_lifecycle_and_close_targets_are_preserved() {
        let mut lookup = empty_set_node();
        lookup.node_id = 41;
        lookup.node_type = plan_nodes::TPlanNodeType::LOOKUP_NODE;

        let mut fetch = empty_set_node();
        fetch.node_id = 42;
        fetch.node_type = plan_nodes::TPlanNodeType::FETCH_NODE;
        fetch.fetch_node = Some(plan_nodes::TFetchNode {
            target_node_id: Some(41),
            row_pos_descs: None,
            nodes_info: Some(descriptors::TNodesInfo::new(
                1,
                vec![descriptors::TNodeInfo::new(
                    7,
                    0,
                    "be-lookup".to_string(),
                    8060,
                )],
            )),
        });

        let mut fragment = values_noop_fragment();
        fragment.plan.as_mut().expect("plan").nodes = vec![lookup, fetch];
        let mut params = params(UniqueId::new(43, 44), UniqueId::new(45, 46));
        params.per_look_up_num_fetchers = Some(BTreeMap::from([(41, 3)]));
        let input = decode_input(&fragment, &params);

        let lifecycles = decode_lookup_fetcher_lifecycles(&input).expect("lookup lifecycles");
        let targets = decode_lookup_close_targets(&input).expect("lookup close targets");

        assert_eq!(lifecycles.get(&41), Some(&LookupFetcherLifecycle::Exact(3)));
        assert_eq!(
            targets,
            vec![StarRocksLookupCloseTarget {
                lookup_node_id: 41,
                host: "be-lookup".to_string(),
                port: 8060,
            }]
        );
    }

    #[test]
    fn query_profile_resolution_patches_data_stream_sink_owned_arena() {
        let mut sink = noop_sink();
        sink.type_ = data_sinks::TDataSinkType::DATA_STREAM_SINK;
        sink.stream_sink = Some(profile_partitioned_stream_sink("query-7"));
        let fragment = fragment_with_sink(sink);
        let params = params(UniqueId::new(81, 82), UniqueId::new(83, 84));

        let draft = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect("prepare profile-partitioned data stream");
        let resolved = resolved_profile_for(&draft);
        let decoded = finish_fragment_submission(draft, resolved)
            .expect("finish profile-partitioned data stream");
        let (submission, _) = decoded.into_parts();
        let FragmentSinkProgram::DataStream(program) = submission.program().sink().program() else {
            panic!("fixture must decode to DATA_STREAM_SINK");
        };
        let expr_id = program.output_partition_exprs()[0];
        assert_resolved_profile(program.partition_arena(), expr_id);
    }

    #[test]
    fn query_profile_resolution_patches_multicast_sink_owned_arena() {
        let multi_cast = data_sinks::TMultiCastDataStreamSink::new(
            vec![profile_partitioned_stream_sink("query-7")],
            vec![Vec::new()],
        );
        let mut sink = noop_sink();
        sink.type_ = data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK;
        sink.multi_cast_stream_sink = Some(multi_cast);
        let fragment = fragment_with_sink(sink);
        let params = params(UniqueId::new(85, 86), UniqueId::new(87, 88));

        let draft = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect("prepare profile-partitioned multicast");
        let resolved = resolved_profile_for(&draft);
        let decoded = finish_fragment_submission(draft, resolved)
            .expect("finish profile-partitioned multicast");
        let (submission, _) = decoded.into_parts();
        let FragmentSinkProgram::MultiCastDataStream(program) =
            submission.program().sink().program()
        else {
            panic!("fixture must decode to MULTI_CAST_DATA_STREAM_SINK");
        };
        let expr_id = program.sinks()[0].output_partition_exprs()[0];
        assert_resolved_profile(program.partition_arena(), expr_id);
    }

    #[test]
    fn query_profile_resolution_preserves_plan_owned_arena_patch() {
        let mut fragment = values_noop_fragment();
        let root = &mut fragment.plan.as_mut().expect("plan").nodes[0];
        root.conjuncts = Some(vec![get_query_profile_expr("query-7")]);
        let params = params(UniqueId::new(89, 90), UniqueId::new(91, 92));

        let draft = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect("prepare plan-owned profile expression");
        let resolved = resolved_profile_for(&draft);
        let decoded = finish_fragment_submission(draft, resolved)
            .expect("finish plan-owned profile expression");
        let (submission, _) = decoded.into_parts();
        let ExecNodeKind::Filter(filter) = &submission.program().plan().root().kind else {
            panic!("root conjunct must lower to a filter");
        };
        assert_resolved_profile(submission.program().plan().arena(), filter.predicate);
    }

    #[test]
    fn instance_first_decode_owns_domain_destinations() {
        let query = UniqueId::new(13, 14);
        let finst = UniqueId::new(23, 24);
        let mut params = params(query, finst);
        params.destinations = Some(vec![data_sinks::TPlanFragmentDestination::new(
            types::TUniqueId { hi: 33, lo: 34 },
            types::TNetworkAddress::new("127.0.0.1".to_string(), 8060),
            None,
            None,
        )]);
        let facts = super::super::instance::StarRocksDecodeFacts::default();
        let instance = decode_instance_parts(
            &params,
            None,
            None,
            Some(3),
            1,
            &EMPTY_BATCH_SENDERS,
            false,
            &facts,
            FieldPath::root("exec_plan_fragment"),
        )
        .expect("decode instance facts");

        fn assert_domain_destinations(_: &[FragmentDestination]) {}
        assert_domain_destinations(&instance.destinations);
    }

    #[test]
    fn broker_file_scan_is_rejected_as_unsupported() {
        let mut scan_node = empty_set_node();
        scan_node.node_id = 17;
        scan_node.node_type = plan_nodes::TPlanNodeType::FILE_SCAN_NODE;
        let mut fragment = values_noop_fragment();
        fragment.plan.as_mut().expect("plan").nodes = vec![scan_node];
        let params = params(UniqueId::new(71, 72), UniqueId::new(81, 82));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("broker FILE_SCAN_NODE must be retired");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.kind(),
            novarocks::protocol::ProtocolErrorKind::Unsupported
        );
        assert_eq!(
            protocol.path().to_string(),
            "exec_plan_fragment.fragment.plan.nodes[0].node_type"
        );
    }

    #[test]
    fn olap_table_sink_is_rejected_as_unsupported() {
        let mut fragment = values_noop_fragment();
        fragment.output_sink.as_mut().expect("output sink").type_ =
            data_sinks::TDataSinkType::OLAP_TABLE_SINK;
        let params = params(UniqueId::new(73, 74), UniqueId::new(83, 84));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("OLAP_TABLE_SINK must be retired");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.kind(),
            novarocks::protocol::ProtocolErrorKind::Unsupported
        );
        assert_eq!(
            protocol.path().to_string(),
            "exec_plan_fragment.fragment.output_sink.type"
        );
    }

    #[test]
    fn lake_late_materialization_is_rejected_as_unsupported() {
        let mut scan_node = empty_set_node();
        scan_node.node_type = plan_nodes::TPlanNodeType::LAKE_SCAN_NODE;
        let lake = plan_nodes::TLakeScanNode::new(
            0,
            Vec::new(),
            Vec::new(),
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(true),
        );
        scan_node.lake_scan_node = Some(lake);
        let mut fragment = values_noop_fragment();
        fragment.plan.as_mut().expect("plan").nodes = vec![scan_node];
        let params = params(UniqueId::new(75, 76), UniqueId::new(85, 86));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("lake late materialization must be retired");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.kind(),
            novarocks::protocol::ProtocolErrorKind::Unsupported
        );
        assert_eq!(
            protocol.path().to_string(),
            "exec_plan_fragment.fragment.plan.nodes[0].lake_scan_node.enable_global_late_materialization"
        );
    }

    #[test]
    fn instance_first_decode_normalizes_hdfs_lake_and_schema_assignments() {
        let mut hdfs_node = empty_set_node();
        hdfs_node.node_id = 18;
        hdfs_node.node_type = plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
        let mut lake_node = empty_set_node();
        lake_node.node_id = 19;
        lake_node.node_type = plan_nodes::TPlanNodeType::LAKE_SCAN_NODE;
        let mut schema_node = empty_set_node();
        schema_node.node_id = 20;
        schema_node.node_type = plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE;
        schema_node.schema_scan_node = Some(plan_nodes::TSchemaScanNode::new(
            1,
            "be_tablets".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let hdfs = plan_nodes::THdfsScanRange {
            relative_path: None,
            offset: Some(0),
            length: Some(64),
            partition_id: None,
            file_length: Some(64),
            file_format: Some(crate::thrift::descriptors::THdfsFileFormat::PARQUET),
            text_file_desc: None,
            full_path: Some("s3://bucket/data.parquet".to_string()),
            hudi_logs: None,
            use_hudi_jni_reader: None,
            delete_files: None,
            skip_header: None,
            use_paimon_jni_reader: None,
            paimon_split_info: None,
            paimon_predicate_info: None,
            modification_time: None,
            datacache_options: None,
            identity_partition_slot_ids: None,
            use_odps_jni_reader: None,
            odps_split_infos: None,
            delete_column_slot_ids: None,
            use_iceberg_jni_metadata_reader: None,
            serialized_split: None,
            use_kudu_jni_reader: None,
            kudu_master: None,
            kudu_scan_token: None,
            paimon_deletion_file: None,
            extended_columns: None,
            partition_value: None,
            table_id: None,
            deletion_vector_descriptor: None,
            candidate_node: None,
            record_count: None,
            is_first_split: None,
            min_max_values: None,
            bucket_id: None,
            first_row_id: None,
            data_sequence_number: None,
            included_positions: None,
        };
        let internal = plan_nodes::TInternalScanRange::new(
            Vec::new(),
            "1".to_string(),
            "7".to_string(),
            "0".to_string(),
            300,
            "db".to_string(),
            None,
            None,
            Some("tbl".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let raw_ranges = BTreeMap::from([
            (
                18,
                vec![internal_service::TScanRangeParams::new(
                    plan_nodes::TScanRange::new(None, None, None, None, Some(hdfs), None, None),
                    None,
                    Some(false),
                    Some(false),
                )],
            ),
            (
                19,
                vec![internal_service::TScanRangeParams::new(
                    plan_nodes::TScanRange::new(Some(internal), None, None, None, None, None, None),
                    None,
                    Some(false),
                    Some(false),
                )],
            ),
            (
                20,
                vec![internal_service::TScanRangeParams::new(
                    plan_nodes::TScanRange::default(),
                    None,
                    Some(false),
                    Some(false),
                )],
            ),
        ]);
        let facts = super::super::instance::StarRocksDecodeFacts::default();
        let (contracts, assignments) =
            super::super::instance::decode_scan_contracts_and_raw_ranges(
                &[hdfs_node, lake_node, schema_node],
                &raw_ranges,
                None,
                &facts,
                FieldPath::root("exec_plan_fragment")
                    .field("params")
                    .field("per_node_scan_ranges"),
            )
            .expect("decode typed scan assignments");
        for (node_id, kind) in [
            (
                18,
                novarocks::exec::fragment::program::ScanAssignmentKind::File,
            ),
            (
                19,
                novarocks::exec::fragment::program::ScanAssignmentKind::StarRocksTablet,
            ),
            (
                20,
                novarocks::exec::fragment::program::ScanAssignmentKind::SchemaSelection,
            ),
        ] {
            let id = FragmentNodeId::new(node_id);
            assert_eq!(contracts[&id].assignment_kind(), kind);
            assert_eq!(assignments.get(&id).expect("assignment").1.len(), 1);
        }
    }

    #[test]
    fn hdfs_candidate_node_survives_incremental_range_decode() {
        let hdfs_range = plan_nodes::THdfsScanRange {
            full_path: Some("/tmp/pbf3-candidate.parquet".to_string()),
            offset: Some(0),
            length: Some(64),
            file_length: Some(64),
            file_format: Some(descriptors::THdfsFileFormat::PARQUET),
            candidate_node: Some("  backend-7  ".to_string()),
            ..Default::default()
        };
        let ranges = vec![internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(None, None, None, None, Some(hdfs_range), None, None),
            None,
            Some(false),
            Some(false),
        )];
        let decoded = super::super::instance::decode_incremental_scan_ranges(18, &ranges, None)
            .expect("decode incremental HDFS candidate node");
        let [novarocks::exec::node::scan::IncrementalScanRange::Hdfs { range, .. }] =
            decoded.as_slice()
        else {
            panic!("expected one incremental HDFS range");
        };
        assert_eq!(
            range
                .external_datacache
                .as_ref()
                .and_then(|options| options.candidate_node.as_deref()),
            Some("backend-7")
        );
    }

    #[test]
    fn scan_range_error_preserves_map_key_and_raw_range_index() {
        let mut hdfs_node = empty_set_node();
        hdfs_node.node_id = 17;
        hdfs_node.node_type = plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
        let raw_ranges = BTreeMap::from([(
            17,
            vec![
                internal_service::TScanRangeParams::new(
                    plan_nodes::TScanRange::default(),
                    None,
                    Some(true),
                    Some(false),
                ),
                internal_service::TScanRangeParams::new(
                    plan_nodes::TScanRange::default(),
                    None,
                    Some(false),
                    Some(false),
                ),
            ],
        )]);
        let error = super::super::instance::decode_scan_contracts_and_raw_ranges(
            &[hdfs_node],
            &raw_ranges,
            None,
            &super::super::instance::StarRocksDecodeFacts::default(),
            FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("per_node_scan_ranges"),
        )
        .expect_err("second raw range is missing hdfs payload");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "exec_plan_fragment.params.per_node_scan_ranges[\"17\"][1].scan_range.hdfs_scan_range"
        );
    }

    #[test]
    fn unknown_schema_values_does_not_declare_scan_contract() {
        let mut node = empty_set_node();
        node.node_id = 21;
        node.node_type = plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE;
        node.schema_scan_node = Some(plan_nodes::TSchemaScanNode::new(
            1,
            "future_unknown_table".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let facts = super::super::instance::StarRocksDecodeFacts::default();
        let (contracts, assignments) =
            super::super::instance::decode_scan_contracts_and_raw_ranges(
                &[node],
                &BTreeMap::new(),
                None,
                &facts,
                FieldPath::root("exec_plan_fragment")
                    .field("params")
                    .field("per_node_scan_ranges"),
            )
            .expect("unknown schema values has no scan inventory");
        assert!(contracts.is_empty());
        assert!(assignments.is_empty());
    }

    #[test]
    fn negative_child_count_reports_exact_node_field_path() {
        let mut fragment = values_noop_fragment();
        fragment.plan.as_mut().expect("plan").nodes[0].num_children = -1;
        let params = params(UniqueId::new(35, 36), UniqueId::new(45, 46));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("negative child count must fail");
        assert_eq!(
            error
                .protocol()
                .expect("typed protocol error")
                .path()
                .to_string(),
            "exec_plan_fragment.fragment.plan.nodes[0].num_children"
        );
    }

    #[test]
    fn negative_flat_child_count_reports_exact_node_field_path() {
        let mut fragment = values_noop_fragment();
        let plan = fragment.plan.as_mut().expect("plan");
        let mut child = plan.nodes[0].clone();
        plan.nodes[0].num_children = 1;
        child.node_id += 1;
        child.num_children = -1;
        plan.nodes.push(child);
        let params = params(UniqueId::new(37, 38), UniqueId::new(47, 48));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("negative child count must fail");
        assert_eq!(
            error
                .protocol()
                .expect("typed protocol error")
                .path()
                .to_string(),
            "exec_plan_fragment.fragment.plan.nodes[1].num_children"
        );
    }

    #[test]
    fn trailing_flat_node_reports_exact_node_path() {
        let mut fragment = values_noop_fragment();
        let plan = fragment.plan.as_mut().expect("plan");
        let mut trailing = plan.nodes[0].clone();
        trailing.node_id += 1;
        plan.nodes.push(trailing);
        let params = params(UniqueId::new(39, 40), UniqueId::new(49, 50));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("trailing flat node must fail");
        assert_eq!(
            error
                .protocol()
                .expect("typed protocol error")
                .path()
                .to_string(),
            "exec_plan_fragment.fragment.plan.nodes[1]"
        );
    }

    #[test]
    fn missing_plan_fails_before_missing_sink_with_exact_path() {
        let mut fragment = values_noop_fragment();
        fragment.plan = None;
        fragment.output_sink = None;
        let params = params(UniqueId::new(31, 32), UniqueId::new(41, 42));
        let error = prepare_fragment_submission(decode_input(&fragment, &params))
            .expect_err("missing plan must fail");
        let protocol = error.protocol().expect("missing plan is a protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "exec_plan_fragment.fragment.plan"
        );
        assert_eq!(
            protocol.family(),
            novarocks::protocol::ProtocolFamily::StarRocks
        );
    }

    #[test]
    fn malformed_starrocks_fragment_has_zero_runtime_side_effects() {
        let query = QueryId::new(51, 52);
        let finst = UniqueId::new(61, 62);
        let mut fragment = values_noop_fragment();
        fragment.plan = None;
        let params = params(UniqueId::new(query.high(), query.low()), finst);
        let exchange_key = ExchangeKey {
            finst_id_hi: finst.high(),
            finst_id_lo: finst.low(),
            node_id: 11,
        };
        let rf_key = QueryKey::from_hi_lo(query.high(), query.low());

        assert!(prepare_fragment_submission(decode_input(&fragment, &params)).is_err());
        assert!(matches!(
            result_buffer::try_fetch(finst),
            TryFetchResult::Error(_)
        ));
        assert!(snapshot_receiver_state(exchange_key).is_none());
        assert!(
            RuntimeFilterLifecycleRegistry::global()
                .snapshot(rf_key)
                .is_none()
        );
    }

    #[test]
    fn dependency_contract_error_is_not_reported_as_wire_or_binding() {
        let requirements = vec![StarRocksExternalDependency::QueryProfile {
            id: 7,
            query_id: "query-7".to_string(),
        }];
        let error = validate_resolved_dependencies(
            &requirements,
            &StarRocksResolvedDependencies::default(),
        )
        .expect_err("missing dependency must fail");
        assert_eq!(error.kind(), StarRocksDependencyContractErrorKind::Missing);
        assert_eq!(error.dependency_id(), 7);

        let mut wrong = StarRocksResolvedDependencies::default();
        wrong.insert(
            7,
            StarRocksResolvedDependencyValue::LakeMetaStorage(LakeMetaStorageFacts {
                total_rows: 0,
                column_arrays: BTreeMap::new(),
            }),
        );
        assert_eq!(
            validate_resolved_dependencies(&requirements, &wrong)
                .expect_err("wrong kind must fail")
                .kind(),
            StarRocksDependencyContractErrorKind::WrongKind,
        );

        let mut extra = StarRocksResolvedDependencies::default();
        extra.insert(
            7,
            StarRocksResolvedDependencyValue::QueryProfile("profile".to_string()),
        );
        extra.insert(
            8,
            StarRocksResolvedDependencyValue::QueryProfile("extra".to_string()),
        );
        assert_eq!(
            validate_resolved_dependencies(&requirements, &extra)
                .expect_err("extra dependency must fail")
                .kind(),
            StarRocksDependencyContractErrorKind::Extra,
        );
    }

    #[test]
    fn external_dependencies_require_exact_resolution_before_submission() {
        let lake_request = LakeMetaStorageRequest::new(
            QueryId::new(71, 72),
            "catalog".to_string(),
            "db".to_string(),
            "table".to_string(),
            1,
            2,
            3,
            Vec::new(),
            Vec::new(),
        );
        let requirements = vec![
            StarRocksExternalDependency::QueryProfile {
                id: 7,
                query_id: "query-7".to_string(),
            },
            StarRocksExternalDependency::LakeMetaStorage {
                id: lake_request.id(),
                request: lake_request.clone(),
            },
        ];
        let mut resolved = StarRocksResolvedDependencies::default();
        resolved.insert(
            7,
            StarRocksResolvedDependencyValue::QueryProfile("profile".to_string()),
        );
        assert_eq!(
            validate_resolved_dependencies(&requirements, &resolved)
                .expect_err("lake facts are still missing")
                .kind(),
            StarRocksDependencyContractErrorKind::Missing,
        );
        resolved.insert(
            lake_request.id(),
            StarRocksResolvedDependencyValue::LakeMetaStorage(LakeMetaStorageFacts {
                total_rows: 0,
                column_arrays: BTreeMap::new(),
            }),
        );
        validate_resolved_dependencies(&requirements, &resolved)
            .expect("every declared dependency resolves exactly once with the right kind");
    }
}
