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

//! The `TableFinish` operator: the single-owner write completion data plane.
//!
//! One driver on one Root BE consumes every writer row that arrives through the
//! gather Exchange. It validates each row's shape against the frozen relation,
//! checks the target ordinal against the sealed set, hands the canonical bytes
//! to the validator port for a structural check, and charges both the frozen
//! per-fragment and prepared-write-set budgets. Exceeding a budget is a typed
//! rejection, never a truncation.
//!
//! Nothing is emitted before every sender reaches EOS: a prepared write set is
//! complete or it does not exist. On abort the buffered fragments are released
//! immediately rather than held until the driver is dropped.
//!
//! Design: ADR-0135 (docs/adr/ADR-0135-ordinary-aggregate-statistics-dataflow.md)

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, Int8Array, Int32Array, Int64Array, new_null_array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES;
use novarocks_spi::connector::write_stack::{
    PreparedWriteSetLedger, RootRowKind, RootWriteResultRowShape, WriteRowCountAccumulator,
    WriteTargetOrdinal, WriterRowKind, row_count_from_wire, row_count_to_wire,
    target_ordinal_from_wire, target_ordinal_to_wire, validate_artifact_draft_nested_values,
    validate_root_write_result_row, validate_writer_multiplex_row,
};

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
use crate::exec::node::table_finish::TableFinishNode;
use crate::exec::node::table_write_aggregate::{
    WriterFinalAggregatePlan, WriterGroupedUnpivotMapping, WriterGroupedUnpivotPlan,
};
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentCarrierValidator, RootWriteResultRelationSchema,
    WriterMultiplexRelationSchema,
};
#[cfg(debug_assertions)]
use crate::exec::node::table_write_relation::{
    TableWriteAggregateBoundary, TableWriteAggregateGuard,
};
use crate::exec::node::unpivot::{UnpivotPassthroughColumn, UnpivotValueMapping};
use crate::exec::operators::aggregate::AggregateProcessorFactory;
use crate::exec::operators::blocked_duration::BlockedDuration;
use crate::exec::operators::table_writer::TableWriteRelationColumns;
use crate::exec::operators::unpivot_processor::UnpivotProcessorFactory;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::profile::{OperatorProfiles, ProfileUnit};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Factory for the single-driver table finish operator.
pub struct TableFinishOperatorFactory {
    name: String,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    writer_schema: WriterMultiplexRelationSchema,
    root_schema: RootWriteResultRelationSchema,
    final_plan: WriterFinalAggregatePlan,
    #[cfg(debug_assertions)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
    arena: Arc<ExprArena>,
}

impl TableFinishOperatorFactory {
    /// Construct the NCP-8 composite with the same immutable expression arena
    /// that decoded the generic Unpivot constants.
    pub fn new_with_arena(node: &TableFinishNode, arena: Arc<ExprArena>) -> Self {
        let name = if node.node_id >= 0 {
            format!("TABLE_FINISH (id={})", node.node_id)
        } else {
            "TABLE_FINISH".to_string()
        };
        Self {
            name,
            expected_targets: Arc::clone(node.expected_targets()),
            fragment_validator: Arc::clone(node.fragment_validator()),
            writer_schema: node.writer_multiplex_schema().clone(),
            root_schema: node.root_result_schema().clone(),
            final_plan: node.final_aggregate_plan().clone(),
            #[cfg(debug_assertions)]
            aggregate_guard: Arc::clone(node.aggregate_guard()),
            arena,
        }
    }

    fn create_operator(&self, dop: i32, driver_id: i32) -> TableFinishOperator {
        // The builder creates this factory's pipeline at DOP 1, converging every
        // writer input into it first, so any other degree of parallelism means
        // the plan and the pipeline disagree about who owns the complete
        // prepared write set. A second driver would each see a partial set and
        // each believe it was complete, so fail closed at `prepare` rather than
        // silently aggregating a partial set per driver.
        let parallelism_error = (dop.max(1) != 1 || driver_id != 0).then(|| {
            format!(
                "table finish must run at DOP 1 on a single driver, but was created with dop={dop} driver_id={driver_id}"
            )
        });
        TableFinishOperator {
            name: self.name.clone(),
            expected_targets: Arc::clone(&self.expected_targets),
            fragment_validator: Arc::clone(&self.fragment_validator),
            writer_schema: self.writer_schema.clone(),
            root_schema: self.root_schema.clone(),
            final_plan: self.final_plan.clone(),
            #[cfg(debug_assertions)]
            aggregate_guard: Arc::clone(&self.aggregate_guard),
            channel_to_call: self
                .writer_schema
                .contract()
                .auxiliary_channels()
                .iter()
                .map(|channel| {
                    self.final_plan
                        .calls
                        .iter()
                        .position(|call| call.intermediate_input_slot_id.0 == channel.slot_id())
                })
                .collect(),
            arena: Arc::clone(&self.arena),
            parallelism_error,
            rows: WriteRowCountAccumulator::new(),
            ledger: PreparedWriteSetLedger::new(),
            fragments: Vec::new(),
            aggregate: None,
            aggregate_coverage: HashMap::new(),
            final_groups_seen: HashSet::new(),
            grouped_unpivot: None,
            prefix_output: None,
            phase: FinishPhase::Consuming,
            mem_tracker: None,
            fragment_tracker: None,
            output_tracker: None,
            profiles: None,
            runtime_error: None,
            final_aggregate_blocked_time: BlockedDuration::default(),
        }
    }
}

impl OperatorFactory for TableFinishOperatorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.create_operator(dop, driver_id))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FinishPhase {
    Consuming,
    Finalizing,
    Producing,
    Failed,
    Finished,
}

struct BufferedFragment {
    target: WriteTargetOrdinal,
    bytes: Vec<u8>,
    _accounting: Option<TrackedBytes>,
}

/// Streams the non-statistics Root prefix without materializing a second copy
/// of the complete prepared write set. The summary is its own first batch;
/// prepared fragments then move through bounded, byte-aware batches.
struct PrefixOutputDriver {
    summary_rows: Option<i64>,
    fragments: VecDeque<BufferedFragment>,
    root_schema: crate::exec::chunk::ChunkSchemaRef,
    tracker: Option<Arc<MemTracker>>,
}

impl PrefixOutputDriver {
    fn try_new(
        rows: u64,
        fragments: Vec<BufferedFragment>,
        root_schema: crate::exec::chunk::ChunkSchemaRef,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<Self, String> {
        let summary_rows = row_count_to_wire(rows)
            .map_err(|error| format!("table finish summary row count: {error}"))?;
        validate_root_write_result_row(
            RootRowKind::Summary,
            RootWriteResultRowShape {
                row_count: Some(summary_rows),
                ..RootWriteResultRowShape::default()
            },
        )
        .map_err(|error| format!("table finish summary row shape: {error}"))?;
        Ok(Self {
            summary_rows: Some(summary_rows),
            fragments: fragments.into(),
            root_schema,
            tracker,
        })
    }

    fn has_output(&self) -> bool {
        self.summary_rows.is_some() || !self.fragments.is_empty()
    }

    fn is_finished(&self) -> bool {
        !self.has_output()
    }

    fn pull(&mut self) -> Result<Option<Chunk>, String> {
        if let Some(summary_rows) = self.summary_rows.take() {
            let root_arrow = self.root_schema.arrow_schema_ref();
            let batch = RecordBatch::try_new(
                root_arrow,
                vec![
                    Arc::new(Int8Array::from(vec![RootRowKind::Summary.to_wire()])) as ArrayRef,
                    new_null_array(self.root_schema.arrow_schema_ref().field(1).data_type(), 1),
                    Arc::new(Int64Array::from(vec![Some(summary_rows)])),
                    new_null_array(self.root_schema.arrow_schema_ref().field(3).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(4).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(5).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(6).data_type(), 1),
                    new_null_array(self.root_schema.arrow_schema_ref().field(7).data_type(), 1),
                ],
            )
            .map_err(|error| format!("build table finish summary batch: {error}"))?;
            return self.track_batch(batch).map(Some);
        }
        if self.fragments.is_empty() {
            return Ok(None);
        }

        // Select a bounded candidate before copying any Binary values. The
        // conservative fixed allowance covers validity/offset buffers and IPC
        // framing; an exact retained/logical/IPC check below is authoritative.
        const FIXED_BATCH_ALLOWANCE: usize = 16 * 1024;
        const PER_ROW_ALLOWANCE: usize = 256;
        let mut selected = 0usize;
        let mut selected_payload = 0usize;
        for fragment in &self.fragments {
            let next_payload = selected_payload
                .checked_add(fragment.bytes.len())
                .ok_or_else(|| {
                    "ResourceExhausted: table finish fragment batch size overflow".to_string()
                })?;
            let next_rows = selected + 1;
            let estimate = FIXED_BATCH_ALLOWANCE
                .saturating_add(next_payload)
                .saturating_add(next_rows.saturating_mul(PER_ROW_ALLOWANCE));
            if estimate > MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES {
                break;
            }
            selected = next_rows;
            selected_payload = next_payload;
        }
        if selected == 0 {
            return Err(format!(
                "ResourceExhausted: one prepared fragment cannot fit the {} byte Root output batch limit",
                MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
            ));
        }

        // The conservative estimate should normally fit on the first attempt.
        // Retrying with a smaller prefix keeps even unusual Arrow/IPC overhead
        // bounded to one candidate batch instead of copying the complete ledger.
        loop {
            let batch = self.build_fragment_batch(selected)?;
            let size = root_output_size(&batch, self.tracker.as_ref())?;
            if size <= MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES {
                let output = self.track_batch(batch)?;
                for _ in 0..selected {
                    let _released = self
                        .fragments
                        .pop_front()
                        .expect("selected fragments remain buffered until output admission");
                }
                return Ok(Some(output));
            }
            if selected == 1 {
                return Err(format!(
                    "ResourceExhausted: one prepared fragment produces a {size} byte Root output batch, exceeding the {} byte limit",
                    MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                ));
            }
            selected = selected.div_ceil(2);
        }
    }

    fn build_fragment_batch(&self, rows: usize) -> Result<RecordBatch, String> {
        let mut kinds = Vec::with_capacity(rows);
        let mut ordinals = Vec::with_capacity(rows);
        let mut payloads = BinaryBuilder::new();
        for fragment in self.fragments.iter().take(rows) {
            let target = target_ordinal_to_wire(fragment.target)
                .map_err(|error| format!("table finish target ordinal: {error}"))?;
            validate_root_write_result_row(
                RootRowKind::PreparedFragment,
                RootWriteResultRowShape {
                    target: Some(target),
                    fragment_len: Some(fragment.bytes.len()),
                    ..RootWriteResultRowShape::default()
                },
            )
            .map_err(|error| format!("table finish prepared fragment row shape: {error}"))?;
            kinds.push(RootRowKind::PreparedFragment.to_wire());
            ordinals.push(Some(target));
            payloads.append_value(&fragment.bytes);
        }
        let root_arrow = self.root_schema.arrow_schema_ref();
        RecordBatch::try_new(
            root_arrow,
            vec![
                Arc::new(Int8Array::from(kinds)) as ArrayRef,
                Arc::new(Int32Array::from(ordinals)),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(2).data_type(),
                    rows,
                ),
                Arc::new(payloads.finish()),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(4).data_type(),
                    rows,
                ),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(5).data_type(),
                    rows,
                ),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(6).data_type(),
                    rows,
                ),
                new_null_array(
                    self.root_schema.arrow_schema_ref().field(7).data_type(),
                    rows,
                ),
            ],
        )
        .map_err(|error| format!("build table finish fragment batch: {error}"))
    }

    fn track_batch(&self, batch: RecordBatch) -> Result<Chunk, String> {
        let mut output = Chunk::try_new_with_chunk_schema(batch, Arc::clone(&self.root_schema))?;
        if let Some(tracker) = self.tracker.as_ref() {
            output.try_transfer_to(tracker).map_err(|error| {
                format!("ResourceExhausted: table finish output memory admission failed: {error}")
            })?;
        }
        Ok(output)
    }
}

fn root_output_size(
    batch: &RecordBatch,
    tracker: Option<&Arc<MemTracker>>,
) -> Result<usize, String> {
    let retained = batch.get_array_memory_size();
    let logical = crate::exec::chunk::record_batch_bytes(batch);
    let mut encoded = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut encoded, &batch.schema())
            .map_err(|error| format!("table finish output size encoding failed: {error}"))?;
        writer
            .write(batch)
            .map_err(|error| format!("table finish output size encoding failed: {error}"))?;
        writer
            .finish()
            .map_err(|error| format!("table finish output size encoding failed: {error}"))?;
    }
    let _encoded_accounting = tracker
        .map(|tracker| {
            TrackedBytes::try_new(encoded.capacity(), Arc::clone(tracker)).map_err(|error| {
                format!(
                    "ResourceExhausted: table finish size probe memory admission failed: {error}"
                )
            })
        })
        .transpose()?;
    Ok(retained.max(logical).max(encoded.len()))
}

struct TableFinishOperator {
    name: String,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    writer_schema: WriterMultiplexRelationSchema,
    root_schema: RootWriteResultRelationSchema,
    final_plan: WriterFinalAggregatePlan,
    #[cfg(debug_assertions)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
    channel_to_call: Vec<Option<usize>>,
    arena: Arc<ExprArena>,
    parallelism_error: Option<String>,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<BufferedFragment>,
    aggregate: Option<Box<dyn Operator>>,
    aggregate_coverage: HashMap<u32, Vec<bool>>,
    final_groups_seen: HashSet<u32>,
    grouped_unpivot: Option<GroupedUnpivotDriver>,
    prefix_output: Option<PrefixOutputDriver>,
    phase: FinishPhase,
    mem_tracker: Option<Arc<MemTracker>>,
    fragment_tracker: Option<Arc<MemTracker>>,
    output_tracker: Option<Arc<MemTracker>>,
    profiles: Option<OperatorProfiles>,
    runtime_error: Option<Arc<RuntimeErrorState>>,
    final_aggregate_blocked_time: BlockedDuration,
}

impl TableFinishOperator {
    fn release_buffer(&mut self) {
        self.fragments.clear();
        self.prefix_output = None;
        self.grouped_unpivot = None;
        if let Some(aggregate) = self.aggregate.as_mut() {
            aggregate.cancel();
        }
        self.aggregate = None;
        self.aggregate_coverage.clear();
        self.final_groups_seen.clear();
    }

    fn fail<T>(&mut self, error: String) -> Result<T, String> {
        self.release_buffer();
        self.phase = FinishPhase::Failed;
        self.finish_final_aggregate_blocked_interval();
        self.sync_metrics();
        Err(error)
    }

    /// Read one target ordinal off the signed carrier. A negative value is
    /// corrupt data, never a very large unsigned ordinal.
    fn target_in_sealed_set(&self, raw: i32) -> Result<WriteTargetOrdinal, String> {
        let target = target_ordinal_from_wire(raw)
            .map_err(|error| format!("table finish write target ordinal: {error}"))?;
        // An exact set test, not a bound: the query's expected set need not be
        // dense from zero, so "at or below the highest ordinal" would admit a
        // target this query compiled no writer for.
        if !self.expected_targets.contains(&target) {
            return Err(format!(
                "table finish received a write target ordinal {raw} outside the sealed set of {} targets",
                self.expected_targets.len()
            ));
        }
        Ok(target)
    }

    fn accept_row(
        &mut self,
        chunk: &Chunk,
        columns: &TableWriteRelationColumns<'_>,
        row: usize,
    ) -> Result<(), String> {
        if columns.kinds.is_null(row) {
            return Err("table finish received a writer row with a null kind".to_string());
        }
        let kind = WriterRowKind::from_wire(columns.kinds.value(row)).map_err(|error| {
            format!(
                "table finish writer row kind {}: {error}",
                columns.kinds.value(row)
            )
        })?;
        let row_count = (!columns.row_counts.is_null(row)).then(|| columns.row_counts.value(row));
        let fragment_len =
            (!columns.fragments.is_null(row)).then(|| columns.fragments.value(row).len());
        let auxiliary_non_null = self
            .writer_schema
            .contract()
            .auxiliary_channels()
            .iter()
            .map(|channel| {
                let slot = novarocks_types::SlotId::new(channel.slot_id());
                let index = chunk
                    .chunk_schema()
                    .index_of(slot)
                    .expect("exact writer schema contains every auxiliary slot");
                !chunk.columns()[index].is_null(row)
            })
            .collect::<Vec<_>>();
        validate_writer_multiplex_row(
            self.writer_schema.contract(),
            kind,
            row_count,
            fragment_len,
            &auxiliary_non_null,
        )
        .map_err(|error| format!("table finish writer row shape: {error}"))?;

        if columns.ordinals.is_null(row) {
            return Err(
                "table finish received a writer row with a null write target ordinal".to_string(),
            );
        }
        let target = self.target_in_sealed_set(columns.ordinals.value(row))?;

        match kind {
            WriterRowKind::RowCount => {
                let rows = row_count.ok_or_else(|| {
                    "table finish ROW_COUNT row lost its validated row count".to_string()
                })?;
                // A negative row count is corrupt data, never a huge unsigned
                // one: it would become the statement's affected row count.
                let rows = row_count_from_wire(rows)
                    .map_err(|error| format!("table finish row count: {error}"))?;
                self.rows
                    .add(rows)
                    .map_err(|error| format!("table finish row count: {error}"))
            }
            WriterRowKind::CommitFragment => {
                let encoded = columns.fragments.value(row);
                self.fragment_validator
                    .validate(target, encoded)
                    .map_err(|error| format!("table finish commit fragment carrier: {error}"))?;
                self.ledger
                    .reserve_fragment(encoded.len())
                    .map_err(|error| format!("table finish prepared write set: {error}"))?;
                let bytes = encoded.to_vec();
                let accounting = self
                    .fragment_tracker
                    .as_ref()
                    .map(|tracker| {
                        TrackedBytes::try_new(bytes.capacity(), Arc::clone(tracker)).map_err(
                            |error| {
                                format!(
                                    "ResourceExhausted: table finish fragment memory admission failed: {error}"
                                )
                            },
                        )
                    })
                    .transpose()?;
                self.fragments.push(BufferedFragment {
                    target,
                    bytes,
                    _accounting: accounting,
                });
                Ok(())
            }
            WriterRowKind::AggregatePartial => {
                if self.final_plan.calls.is_empty() {
                    return Err(
                        "table finish received aggregate partials without a final aggregate plan"
                            .to_string(),
                    );
                }
                let coverage = self
                    .aggregate_coverage
                    .entry(target.get())
                    .or_insert_with(|| vec![false; self.final_plan.calls.len()]);
                for (channel, non_null) in auxiliary_non_null.into_iter().enumerate() {
                    if non_null {
                        let index = self.channel_to_call[channel].ok_or_else(|| {
                            "table finish writer auxiliary channel has no final aggregate"
                                .to_string()
                        })?;
                        coverage[index] = true;
                    }
                }
                Ok(())
            }
        }
    }

    fn validate_coverage(&self) -> Result<(), String> {
        let Some(unpivot) = self.final_plan.unpivot.as_ref() else {
            return if self.final_plan.calls.is_empty() {
                Ok(())
            } else {
                Err("table finish final aggregates are missing grouped Unpivot".to_string())
            };
        };
        let call_by_output = self
            .final_plan
            .calls
            .iter()
            .enumerate()
            .map(|(index, call)| (call.final_output_slot_id, index))
            .collect::<HashMap<_, _>>();
        for mapping in &unpivot.mappings {
            let call = call_by_output
                .get(&mapping.input_value_slot_id)
                .copied()
                .ok_or_else(|| {
                    "table finish grouped Unpivot references an unknown final output".to_string()
                })?;
            if !self
                .aggregate_coverage
                .get(&mapping.grouping_key)
                .and_then(|coverage| coverage.get(call))
                .copied()
                .unwrap_or(false)
            {
                return Err(format!(
                    "table finish target {} has no non-null aggregate partial for channel {}",
                    mapping.grouping_key, call
                ));
            }
        }
        Ok(())
    }

    fn filter_aggregate_rows(&self, chunk: &Chunk, partial_rows: &[bool]) -> Result<Chunk, String> {
        let mask = BooleanArray::from(partial_rows.to_vec());
        let batch = filter_record_batch(&chunk.batch, &mask)
            .map_err(|error| format!("table finish filters aggregate partial rows: {error}"))?;
        Chunk::try_new_with_chunk_schema(batch, Arc::clone(self.writer_schema.chunk_schema()))
    }

    fn begin_finalize(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.validate_coverage()?;
        self.prefix_output = Some(PrefixOutputDriver::try_new(
            self.rows.get(),
            std::mem::take(&mut self.fragments),
            Arc::clone(self.root_schema.chunk_schema()),
            self.output_tracker.as_ref().map(Arc::clone),
        )?);
        let Some(aggregate) = self.aggregate.as_mut() else {
            self.phase = FinishPhase::Producing;
            return Ok(());
        };
        let processor = aggregate
            .as_processor_mut()
            .ok_or_else(|| "table finish final aggregate is not a processor".to_string())?;
        processor.set_finishing(state)?;
        // The child may now be asynchronously finalizing while already
        // producing zero or more ordinary aggregate batches. TableFinish
        // streams those batches instead of imposing a scalar/single-batch ABI.
        self.phase = FinishPhase::Producing;
        Ok(())
    }

    fn start_grouped_unpivot(&mut self, final_chunk: Chunk) -> Result<(), String> {
        let arena = Arc::clone(&self.arena);
        let plan =
            self.final_plan.unpivot.as_ref().cloned().ok_or_else(|| {
                "table finish final aggregate is missing grouped Unpivot".to_string()
            })?;
        let driver = GroupedUnpivotDriver::try_new(
            arena,
            plan,
            Arc::clone(self.root_schema.chunk_schema()),
            final_chunk,
            self.output_tracker.as_ref().map(Arc::clone),
        )?;
        for target in driver.targets() {
            if !self.final_groups_seen.insert(target) {
                return Err(format!(
                    "table finish final aggregate produced duplicate target {target} across output batches"
                ));
            }
        }
        self.grouped_unpivot = Some(driver);
        Ok(())
    }

    fn validate_final_groups_complete(&self) -> Result<(), String> {
        let expected = self
            .final_plan
            .unpivot
            .as_ref()
            .into_iter()
            .flat_map(|plan| plan.mappings.iter().map(|mapping| mapping.grouping_key))
            .collect::<HashSet<_>>();
        if expected == self.final_groups_seen {
            return Ok(());
        }
        let missing = expected
            .difference(&self.final_groups_seen)
            .copied()
            .collect::<Vec<_>>();
        let unexpected = self
            .final_groups_seen
            .difference(&expected)
            .copied()
            .collect::<Vec<_>>();
        Err(format!(
            "table finish final aggregate target coverage mismatch: missing={missing:?}, unexpected={unexpected:?}"
        ))
    }

    fn runtime_error(&self) -> Option<String> {
        self.runtime_error.as_ref().and_then(|state| state.error())
    }

    fn record_output(&self, output: &Chunk) {
        if let Some(profiles) = self.profiles.as_ref() {
            profiles
                .common
                .counter_add_unit("RootOutputRows", output.len() as i64);
            profiles
                .common
                .counter_add_bytes("RootOutputBytes", output.logical_bytes() as i64);
        }
    }

    fn finish_final_aggregate_blocked_interval(&self) {
        self.final_aggregate_blocked_time.observe(false);
    }

    fn passive_ready_work_available(&self) -> bool {
        let aggregate_ready = self.aggregate.as_ref().is_some_and(|aggregate| {
            aggregate.is_finished()
                || aggregate
                    .as_processor_ref()
                    .is_some_and(ProcessorOperator::has_passive_ready_work)
        });
        self.runtime_error().is_some()
            || (self.phase == FinishPhase::Producing
                && (self.prefix_output.is_some()
                    || self.grouped_unpivot.is_some()
                    || aggregate_ready))
    }

    fn sync_metrics(&self) {
        let Some(profiles) = self.profiles.as_ref() else {
            return;
        };
        profiles.common.counter_set_unit(
            "FinalAggregateBlockedCount",
            i64::try_from(self.final_aggregate_blocked_time.intervals()).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set(
            "FinalAggregateBlockedTime",
            ProfileUnit::TimeNs,
            crate::runtime::profile::clamp_u128_to_i64(
                self.final_aggregate_blocked_time.elapsed_ns(),
            ),
        );
    }
}

fn build_final_aggregate(
    plan: &WriterFinalAggregatePlan,
    arena: &Arc<ExprArena>,
    function_set: Arc<crate::exec::expr::agg::SealedExecutionFunctionSet>,
    node_id: i32,
) -> Result<Box<dyn Operator>, String> {
    let unpivot = plan
        .unpivot
        .as_ref()
        .ok_or_else(|| "table finish final aggregates require grouped Unpivot".to_string())?;
    let mut aggregate_arena = arena.as_ref().clone();
    let grouping = aggregate_arena.push_typed(
        ExprNode::SlotId(unpivot.grouping_input_slot_id),
        DataType::Int32,
    );
    let mut functions = Vec::with_capacity(plan.calls.len());
    let mut resolved = Vec::with_capacity(plan.calls.len());
    let mut fields = vec![Field::new("write_target_ordinal", DataType::Int32, false)];
    let mut slot_ids = vec![unpivot.grouping_output_slot_id];
    for (index, call) in plan.calls.iter().enumerate() {
        let input = aggregate_arena.push_typed(
            ExprNode::SlotId(call.intermediate_input_slot_id),
            call.resolved.intermediate_type.clone(),
        );
        functions.push(AggFunction {
            name: call.function_name.to_string(),
            inputs: vec![input],
            input_is_intermediate: true,
            types: Some(AggTypeSignature {
                intermediate_type: Some(call.resolved.intermediate_type.clone()),
                output_type: Some(call.resolved.output_type.clone()),
                input_arg_type: call.resolved.argument_types.first().cloned(),
            }),
            order: Default::default(),
        });
        fields.push(Field::new(
            format!("final_aggregate_{index}"),
            call.resolved.output_type.clone(),
            true,
        ));
        slot_ids.push(call.final_output_slot_id);
        resolved.push(call.resolved.clone());
    }
    let output_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(&Schema::new(fields), &slot_ids)?;
    let factory = AggregateProcessorFactory::new_native(
        node_id,
        Arc::new(aggregate_arena),
        vec![grouping],
        functions,
        function_set,
        resolved,
        false,
        false,
        output_schema,
        Vec::new(),
        None,
        1,
        None,
    )?;
    Ok(factory.create(1, 0))
}

struct GroupedUnpivotDriver {
    arena: Arc<ExprArena>,
    plan: WriterGroupedUnpivotPlan,
    root_schema: crate::exec::chunk::ChunkSchemaRef,
    final_chunk: Chunk,
    rows: Vec<(u32, usize)>,
    next_row: usize,
    mappings: BTreeMap<u32, Vec<WriterGroupedUnpivotMapping>>,
    active: Option<Box<dyn Operator>>,
    tracker: Option<Arc<MemTracker>>,
}

impl GroupedUnpivotDriver {
    fn try_new(
        arena: Arc<ExprArena>,
        plan: WriterGroupedUnpivotPlan,
        root_schema: crate::exec::chunk::ChunkSchemaRef,
        mut final_chunk: Chunk,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<Self, String> {
        if let Some(tracker) = tracker.as_ref() {
            final_chunk.try_transfer_to(tracker).map_err(|error| {
                format!(
                    "ResourceExhausted: table finish final aggregate output memory admission failed: {error}"
                )
            })?;
        }
        let groups = final_chunk
            .column_by_slot_id(plan.grouping_output_slot_id)?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "table finish final aggregate grouping output is not Int32".to_string())?
            .clone();
        let mut rows = Vec::with_capacity(groups.len());
        let mut seen = std::collections::HashSet::with_capacity(groups.len());
        for row in 0..groups.len() {
            if groups.is_null(row) {
                return Err("table finish final aggregate produced a null grouping key".to_string());
            }
            let raw = groups.value(row);
            let target = target_ordinal_from_wire(raw)
                .map_err(|error| format!("table finish final aggregate grouping key: {error}"))?;
            if !seen.insert(target.get()) {
                return Err(format!(
                    "table finish final aggregate produced duplicate target {}",
                    target.get()
                ));
            }
            rows.push((target.get(), row));
        }
        let mut mappings = BTreeMap::<u32, Vec<_>>::new();
        for mapping in &plan.mappings {
            mappings
                .entry(mapping.grouping_key)
                .or_default()
                .push(mapping.clone());
        }
        for (target, _) in &rows {
            if !mappings.contains_key(target) {
                return Err(format!(
                    "table finish final aggregate produced target {target} with no grouped Unpivot mapping"
                ));
            }
        }
        Ok(Self {
            arena,
            plan,
            root_schema,
            final_chunk,
            rows,
            next_row: 0,
            mappings,
            active: None,
            tracker,
        })
    }

    fn targets(&self) -> impl Iterator<Item = u32> + '_ {
        self.rows.iter().map(|(target, _)| *target)
    }

    fn output_schema(&self, target: u32) -> Result<crate::exec::chunk::ChunkSchemaRef, String> {
        let slots = [
            self.plan.passthrough_output_slot_id,
            self.plan.literal_output_slot_ids[0],
            self.plan.literal_output_slot_ids[1],
            self.plan.value_output_slot_id,
            self.plan.literal_output_slot_ids[2],
        ];
        let mut fields = slots
            .iter()
            .map(|slot| {
                self.root_schema
                    .slot(*slot)
                    .cloned()
                    .ok_or_else(|| format!("table finish Root schema is missing slot {slot}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        fields[0] = fields[0].with_nullable(false);
        let mappings = self
            .mappings
            .get(&target)
            .ok_or_else(|| format!("table finish target {target} has no grouped mappings"))?;
        let literal_positions = [1usize, 2, 4];
        for (literal_index, position) in literal_positions
            .iter()
            .copied()
            .enumerate()
            .take(self.plan.literal_output_slot_ids.len())
        {
            let nullable = mappings.iter().any(|mapping| {
                matches!(
                    mapping.constants[literal_index],
                    crate::exec::node::unpivot::UnpivotConstant::Scalar { nullable: true, .. }
                )
            });
            fields[position] = fields[position].with_nullable(nullable);
        }
        let value_nullable = mappings.iter().try_fold(false, |nullable, mapping| {
            self.final_chunk
                .chunk_schema()
                .slot(mapping.input_value_slot_id)
                .map(|slot| nullable || slot.nullable())
                .ok_or_else(|| {
                    format!(
                        "table finish final aggregate is missing value slot {}",
                        mapping.input_value_slot_id
                    )
                })
        })?;
        fields[3] = fields[3].with_nullable(value_nullable);
        ChunkSchema::try_new(fields).map(Arc::new)
    }

    fn start_next(&mut self, state: &RuntimeState) -> Result<bool, String> {
        let Some((target, row)) = self.rows.get(self.next_row).copied() else {
            return Ok(false);
        };
        let mappings = self
            .mappings
            .get(&target)
            .ok_or_else(|| format!("table finish target {target} lost its grouped mappings"))?
            .iter()
            .map(|mapping| UnpivotValueMapping {
                input_value_slot_id: mapping.input_value_slot_id,
                constants: mapping.constants.clone(),
            })
            .collect();
        let factory = UnpivotProcessorFactory::new(
            -1,
            Arc::clone(&self.arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: self.plan.grouping_output_slot_id,
                output_slot_id: self.plan.passthrough_output_slot_id,
            }],
            self.plan.value_output_slot_id,
            self.plan.literal_output_slot_ids.clone(),
            mappings,
            self.output_schema(target)?,
            self.plan.max_output_rows,
            self.plan.max_output_bytes,
        )?;
        let mut active = factory.create(1, 0);
        if let Some(tracker) = self.tracker.as_ref() {
            active.set_mem_tracker(Arc::clone(tracker));
        }
        active.prepare()?;
        active.bind_runtime_state(state)?;
        let processor = active
            .as_processor_mut()
            .ok_or_else(|| "table finish grouped Unpivot is not a processor".to_string())?;
        processor.push_chunk(state, self.final_chunk.slice(row, 1))?;
        processor.set_finishing(state)?;
        self.active = Some(active);
        Ok(true)
    }

    fn pull(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        loop {
            if self.active.is_none() && !self.start_next(state)? {
                return Ok(None);
            }
            let active = self.active.as_mut().expect("started grouped Unpivot");
            if let Some(chunk) = active
                .as_processor_mut()
                .expect("grouped Unpivot processor")
                .pull_chunk(state)?
            {
                return root_artifact_chunk(chunk, &self.root_schema, self.tracker.as_ref());
            }
            if !active.is_finished() {
                return Ok(None);
            }
            self.active = None;
            self.next_row += 1;
        }
    }

    fn is_finished(&self) -> bool {
        self.next_row == self.rows.len() && self.active.is_none()
    }

    fn source_observable(
        &self,
    ) -> Option<Arc<crate::exec::pipeline::schedule::observer::Observable>> {
        self.active
            .as_ref()
            .and_then(|active| active.as_processor_ref())
            .and_then(ProcessorOperator::source_observable)
    }
}

fn root_artifact_chunk(
    artifact: Chunk,
    root_schema: &crate::exec::chunk::ChunkSchemaRef,
    tracker: Option<&Arc<MemTracker>>,
) -> Result<Option<Chunk>, String> {
    let rows = artifact.len();
    if rows == 0 {
        return Ok(None);
    }
    let slot = |index: usize| root_schema.slot_ids()[index];
    let target = artifact.column_by_slot_id(slot(1))?;
    let input_fields = artifact.column_by_slot_id(slot(4))?;
    let blob_type = artifact.column_by_slot_id(slot(5))?;
    let body = artifact.column_by_slot_id(slot(6))?;
    let properties = artifact.column_by_slot_id(slot(7))?;
    let input_lists = input_fields
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| "table finish artifact input_fields is not List<Int32>".to_string())?;
    let blob_types = blob_type
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| "table finish artifact blob_type is not Utf8".to_string())?;
    let bodies = body
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .ok_or_else(|| "table finish artifact body is not Binary".to_string())?;
    let maps = properties
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| "table finish artifact properties is not Map<Utf8,Utf8>".to_string())?;
    let targets = target
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| "table finish artifact target is not Int32".to_string())?;
    for row in 0..rows {
        let list = input_lists.value(row);
        let ids = list
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "table finish artifact input_fields values are not Int32".to_string())?;
        let ids = (0..ids.len())
            .map(|index| (!ids.is_null(index)).then(|| ids.value(index)))
            .collect::<Vec<_>>();
        let map = maps.value(row);
        let entries = map
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .ok_or_else(|| "table finish artifact properties entries are not Struct".to_string())?;
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| "table finish artifact property keys are not Utf8".to_string())?;
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| "table finish artifact property values are not Utf8".to_string())?;
        let pairs = (0..entries.len())
            .map(|index| {
                (
                    (!keys.is_null(index)).then(|| keys.value(index)),
                    (!values.is_null(index)).then(|| values.value(index)),
                )
            })
            .collect::<Vec<_>>();
        validate_artifact_draft_nested_values(&ids, &pairs)
            .map_err(|error| format!("table finish artifact nested values: {error}"))?;
        validate_root_write_result_row(
            RootRowKind::ArtifactDraft,
            RootWriteResultRowShape {
                target: (!targets.is_null(row)).then(|| targets.value(row)),
                input_fields_len: (!input_lists.is_null(row)).then_some(ids.len()),
                blob_type_len: (!blob_types.is_null(row)).then(|| blob_types.value(row).len()),
                body_len: (!bodies.is_null(row)).then(|| bodies.value(row).len()),
                properties_len: (!maps.is_null(row)).then_some(pairs.len()),
                ..RootWriteResultRowShape::default()
            },
        )
        .map_err(|error| format!("table finish artifact row shape: {error}"))?;
    }
    let root_arrow = root_schema.arrow_schema_ref();
    let columns = vec![
        Arc::new(Int8Array::from(vec![
            RootRowKind::ArtifactDraft.to_wire();
            rows
        ])) as ArrayRef,
        target,
        new_null_array(root_arrow.field(2).data_type(), rows),
        new_null_array(root_arrow.field(3).data_type(), rows),
        input_fields,
        blob_type,
        body,
        properties,
    ];
    let batch = RecordBatch::try_new(root_arrow, columns)
        .map_err(|error| format!("build table finish artifact Root batch: {error}"))?;
    let mut output = Chunk::try_new_with_chunk_schema(batch, Arc::clone(root_schema))?;
    if let Some(tracker) = tracker {
        output.try_transfer_to(tracker).map_err(|error| {
            format!("ResourceExhausted: table finish artifact output admission failed: {error}")
        })?;
    }
    Ok(Some(output))
}

impl Operator for TableFinishOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn prepare(&mut self) -> Result<(), String> {
        if let Some(error) = self.parallelism_error.take() {
            self.phase = FinishPhase::Failed;
            return Err(error);
        }
        if self.final_plan.calls.is_empty() != self.final_plan.unpivot.is_none() {
            self.phase = FinishPhase::Failed;
            return Err(
                "table finish final aggregate and grouped Unpivot must be both empty or both present"
                    .to_string(),
            );
        }
        if self.channel_to_call.iter().any(Option::is_none)
            || self.channel_to_call.len() != self.final_plan.calls.len()
        {
            self.phase = FinishPhase::Failed;
            return Err(
                "table finish final aggregates do not exactly cover the frozen writer tail"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.mem_tracker = Some(Arc::clone(&tracker));
        self.fragment_tracker = Some(MemTracker::new_child("TableFinishFragments", &tracker));
        self.output_tracker = Some(MemTracker::new_child("TableFinishOutput", &tracker));
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.profiles = Some(profiles);
        self.sync_metrics();
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.runtime_error = Some(state.error_state());
        if self.final_plan.calls.is_empty() {
            return Ok(());
        }
        let arena = Arc::clone(&self.arena);
        let function_set = state
            .execution_runtime()
            .map(|runtime| Arc::clone(runtime.function_set()))
            .ok_or_else(|| {
                "table finish aggregate plan requires the execution function set".to_string()
            })?;
        let mut aggregate = build_final_aggregate(&self.final_plan, &arena, function_set, -1)?;
        if let Some(tracker) = self.mem_tracker.as_ref() {
            aggregate.set_mem_tracker(MemTracker::new_child("TableFinishFinalAggregate", tracker));
        }
        if let Some(profiles) = self.profiles.as_ref() {
            aggregate.set_profiles(OperatorProfiles::new(
                profiles.operator.child("TableFinishFinalAggregate"),
            ));
        }
        aggregate.prepare()?;
        aggregate.bind_runtime_state(state)?;
        self.aggregate = Some(aggregate);
        Ok(())
    }

    fn cancel(&mut self) {
        self.release_buffer();
        self.phase = FinishPhase::Failed;
        self.finish_final_aggregate_blocked_interval();
        self.sync_metrics();
    }

    fn close(&mut self) -> Result<(), String> {
        self.finish_final_aggregate_blocked_interval();
        self.sync_metrics();
        Ok(())
    }

    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        let finished = matches!(self.phase, FinishPhase::Failed | FinishPhase::Finished);
        if finished {
            self.finish_final_aggregate_blocked_interval();
            self.sync_metrics();
        }
        finished
    }

    fn pending_finish(&self) -> bool {
        let pending = matches!(self.phase, FinishPhase::Finalizing | FinishPhase::Producing)
            && self
                .aggregate
                .as_ref()
                .is_some_and(|aggregate| aggregate.pending_finish());
        // PipelineDriver parks an EOS'd pipeline solely through this callback;
        // it does not call `has_output` while an asynchronous final aggregate
        // still reports pending finish. Observe that real scheduler boundary
        // here, keeping repeated polls inside one continuous interval. Do not
        // disturb a pre-EOS input-backpressure interval while still Consuming.
        if self.phase != FinishPhase::Consuming {
            self.final_aggregate_blocked_time
                .observe(pending && self.runtime_error().is_none());
        }
        pending
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for TableFinishOperator {
    fn need_input(&self) -> bool {
        let aggregate_ready = self
            .aggregate
            .as_ref()
            .and_then(|aggregate| aggregate.as_processor_ref())
            .is_none_or(ProcessorOperator::need_input);
        let consuming = self.phase == FinishPhase::Consuming && self.runtime_error().is_none();
        if self.phase == FinishPhase::Consuming {
            self.final_aggregate_blocked_time
                .observe(consuming && !aggregate_ready);
        } else {
            self.finish_final_aggregate_blocked_interval();
        }
        consuming && aggregate_ready
    }

    /// Nothing is available before every sender reached EOS.
    fn has_output(&self) -> bool {
        let producing = self.phase == FinishPhase::Producing;
        let other_output = self.prefix_output.is_some() || self.grouped_unpivot.is_some();
        if producing {
            self.final_aggregate_blocked_time.observe(
                !other_output
                    && self.aggregate.is_some()
                    && !self.passive_ready_work_available()
                    && self.runtime_error().is_none(),
            );
        } else if self.phase != FinishPhase::Consuming {
            self.finish_final_aggregate_blocked_interval();
        }
        self.passive_ready_work_available()
    }

    fn has_passive_ready_work(&self) -> bool {
        self.passive_ready_work_available()
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.phase != FinishPhase::Consuming {
            return Err("table finish received a writer row after EOS".to_string());
        }
        if let Some(error) = self.runtime_error() {
            return self.fail(format!("table finish runtime failed: {error}"));
        }
        if self.aggregate.as_ref().is_some_and(|aggregate| {
            !aggregate
                .as_processor_ref()
                .expect("final aggregate processor")
                .need_input()
        }) {
            return self.fail(
                "table finish received input while its final aggregate was not ready".to_string(),
            );
        }
        if chunk.chunk_schema() != self.writer_schema.chunk_schema().as_ref() {
            return self.fail(
                "table finish writer relation schema drifted from the frozen plan".to_string(),
            );
        }
        let columns = match TableWriteRelationColumns::try_from_chunk(&chunk) {
            Ok(columns) => columns,
            Err(error) => return self.fail(error),
        };
        let mut partial_rows = vec![false; chunk.len()];
        for (row, is_partial) in partial_rows.iter_mut().enumerate() {
            let raw_kind = if columns.kinds.is_null(row) {
                None
            } else {
                Some(columns.kinds.value(row))
            };
            if let Err(error) = self.accept_row(&chunk, &columns, row) {
                return self.fail(error);
            }
            *is_partial = raw_kind == Some(WriterRowKind::AGGREGATE_PARTIAL);
        }
        if partial_rows.iter().any(|value| *value) {
            let filtered = self.filter_aggregate_rows(&chunk, &partial_rows)?;
            #[cfg(debug_assertions)]
            if let Err(error) = self
                .aggregate_guard
                .check(TableWriteAggregateBoundary::FinalMerge)
            {
                return self.fail(format!("table finish final aggregate merge: {error}"));
            }
            let Some(aggregate) = self.aggregate.as_mut() else {
                return self.fail(
                    "table finish aggregate partials arrived before aggregate binding".to_string(),
                );
            };
            if !aggregate
                .as_processor_ref()
                .expect("final aggregate processor")
                .need_input()
            {
                return self.fail(
                    "table finish received input while its final aggregate was not ready"
                        .to_string(),
                );
            }
            if let Err(error) = aggregate
                .as_processor_mut()
                .expect("final aggregate processor")
                .push_chunk(state, filtered)
            {
                return self.fail(format!("table finish final aggregate merge: {error}"));
            }
        }
        if let Some(profiles) = self.profiles.as_ref() {
            profiles
                .common
                .counter_add_unit("WriterMultiplexRows", chunk.len() as i64);
            profiles
                .common
                .counter_add_bytes("WriterMultiplexBytes", chunk.logical_bytes() as i64);
        }
        Ok(())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if let Some(error) = self.runtime_error() {
            return self.fail(format!("table finish runtime failed: {error}"));
        }
        if self.phase != FinishPhase::Producing {
            return Ok(None);
        }

        loop {
            if let Some(prefix) = self.prefix_output.as_mut() {
                match prefix.pull() {
                    Ok(Some(output)) => {
                        self.finish_final_aggregate_blocked_interval();
                        self.record_output(&output);
                        return Ok(Some(output));
                    }
                    Ok(None) if prefix.is_finished() => {
                        self.prefix_output = None;
                    }
                    Ok(None) => return Ok(None),
                    Err(error) => return self.fail(error),
                }
            }

            if let Some(unpivot) = self.grouped_unpivot.as_mut() {
                match unpivot.pull(state) {
                    Ok(Some(output)) => {
                        self.finish_final_aggregate_blocked_interval();
                        self.record_output(&output);
                        return Ok(Some(output));
                    }
                    Ok(None) if unpivot.is_finished() => {
                        self.grouped_unpivot = None;
                    }
                    Ok(None) => return Ok(None),
                    Err(error) => return self.fail(error),
                }
            }

            let Some(aggregate) = self.aggregate.as_ref() else {
                self.aggregate_coverage.clear();
                self.final_groups_seen.clear();
                self.phase = FinishPhase::Finished;
                return Ok(None);
            };
            let has_output = aggregate
                .as_processor_ref()
                .expect("final aggregate processor")
                .has_output();
            if has_output {
                self.finish_final_aggregate_blocked_interval();
                let started = Instant::now();
                let aggregate = self.aggregate.as_mut().expect("final aggregate");
                let result = aggregate
                    .as_processor_mut()
                    .expect("final aggregate processor")
                    .pull_chunk(state);
                if let Some(profiles) = self.profiles.as_ref() {
                    profiles.common.counter_add(
                        "FinalAggregateCpuTime",
                        ProfileUnit::TimeNs,
                        i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX),
                    );
                }
                match result {
                    Ok(Some(final_chunk)) => {
                        if let Err(error) = self.start_grouped_unpivot(final_chunk) {
                            return self.fail(error);
                        }
                        continue;
                    }
                    Ok(None) if aggregate.is_finished() => {}
                    Ok(None) => {
                        return self.fail(
                            "table finish final aggregate advertised output but produced none"
                                .to_string(),
                        );
                    }
                    Err(error) => {
                        return self.fail(format!(
                            "table finish final aggregate output failed: {error}"
                        ));
                    }
                }
            }

            if self
                .aggregate
                .as_ref()
                .is_some_and(|aggregate| aggregate.is_finished())
            {
                if let Err(error) = self.validate_final_groups_complete() {
                    return self.fail(error);
                }
                self.aggregate = None;
                self.finish_final_aggregate_blocked_interval();
                continue;
            }
            self.final_aggregate_blocked_time.observe(true);
            return Ok(None);
        }
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if matches!(
            self.phase,
            FinishPhase::Finalizing | FinishPhase::Producing | FinishPhase::Finished
        ) {
            return Ok(());
        }
        if self.phase == FinishPhase::Failed {
            return Err("table finish cannot finalize a failed write stream".to_string());
        }
        self.phase = FinishPhase::Finalizing;
        self.finish_final_aggregate_blocked_interval();
        #[cfg(debug_assertions)]
        if self.aggregate.is_some()
            && let Err(error) = self
                .aggregate_guard
                .check(TableWriteAggregateBoundary::FinalFinalize)
        {
            return self.fail(format!("table finish final aggregate finalize: {error}"));
        }
        let started = Instant::now();
        let result = self.begin_finalize(state);
        if let Some(profiles) = self.profiles.as_ref() {
            profiles.common.counter_add(
                "FinalAggregateCpuTime",
                ProfileUnit::TimeNs,
                i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX),
            );
        }
        if let Err(error) = result {
            return self.fail(error);
        }
        Ok(())
    }

    fn sink_observable(
        &self,
    ) -> Option<Arc<crate::exec::pipeline::schedule::observer::Observable>> {
        if self.phase != FinishPhase::Consuming {
            return None;
        }
        self.aggregate
            .as_ref()
            .and_then(|aggregate| aggregate.as_processor_ref())
            .and_then(ProcessorOperator::sink_observable)
    }

    fn source_observable(
        &self,
    ) -> Option<Arc<crate::exec::pipeline::schedule::observer::Observable>> {
        match self.phase {
            FinishPhase::Finalizing | FinishPhase::Producing => self
                .grouped_unpivot
                .as_ref()
                .and_then(GroupedUnpivotDriver::source_observable)
                .or_else(|| {
                    self.aggregate
                        .as_ref()
                        .and_then(|aggregate| aggregate.as_processor_ref())
                        .and_then(ProcessorOperator::source_observable)
                }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::{BinaryArray, ListArray, StringArray};
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::write_stack::{
        MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
        MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES, ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
        ROOT_WRITE_RESULT_BODY_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
        ROOT_WRITE_RESULT_PROPERTIES_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootWriteResultSchema,
        WriterAuxiliaryChannel, WriterMultiplexSchema, root_write_result_column_id,
    };
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    use super::*;
    use crate::exec::node::ExecNode;
    use crate::exec::node::table_write_aggregate::{
        WriterFinalAggregateCall, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
        WriterGroupedUnpivotPlan,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::operators::table_writer::tests::target;
    use crate::runtime::ExecutionRuntime;
    use crate::runtime::execution_runtime::{ExecutionRuntimeConfig, ExecutionSpillStorageConfig};

    /// A row shape the tests can express, including shapes a correct
    /// `TableWriter` would never produce.
    type RawRow = (i8, Option<i32>, Option<i64>, Option<Vec<u8>>);

    #[derive(Default)]
    struct AcceptEveryCarrier {
        calls: AtomicUsize,
    }

    impl ConnectorCommitFragmentCarrierValidator for AcceptEveryCarrier {
        fn validate(
            &self,
            _target: WriteTargetOrdinal,
            _encoded: &[u8],
        ) -> Result<(), ConnectorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct RejectEveryCarrier;

    impl ConnectorCommitFragmentCarrierValidator for RejectEveryCarrier {
        fn validate(
            &self,
            _target: WriteTargetOrdinal,
            _encoded: &[u8],
        ) -> Result<(), ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "not a canonical carrier of the expected provider",
            ))
        }
    }

    #[cfg(debug_assertions)]
    struct RejectAggregateBoundary(TableWriteAggregateBoundary);

    #[cfg(debug_assertions)]
    impl TableWriteAggregateGuard for RejectAggregateBoundary {
        fn check(&self, boundary: TableWriteAggregateBoundary) -> Result<(), ConnectorError> {
            if boundary == self.0 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("rejected {boundary:?}"),
                ));
            }
            Ok(())
        }
    }

    struct ScriptedFinalAggregate {
        outputs: VecDeque<Chunk>,
        accepting: Arc<AtomicBool>,
        ready: Arc<AtomicBool>,
        observable: Arc<crate::exec::pipeline::schedule::observer::Observable>,
        finishing: bool,
        pushed_rows: usize,
    }

    impl ScriptedFinalAggregate {
        fn new(outputs: Vec<Chunk>, accepting: bool, ready: bool) -> Self {
            Self {
                outputs: outputs.into(),
                accepting: Arc::new(AtomicBool::new(accepting)),
                ready: Arc::new(AtomicBool::new(ready)),
                observable: Arc::new(crate::exec::pipeline::schedule::observer::Observable::new()),
                finishing: false,
                pushed_rows: 0,
            }
        }
    }

    impl Operator for ScriptedFinalAggregate {
        fn name(&self) -> &str {
            "SCRIPTED_FINAL_AGGREGATE"
        }

        fn is_finished(&self) -> bool {
            self.finishing && self.ready.load(Ordering::Acquire) && self.outputs.is_empty()
        }

        fn pending_finish(&self) -> bool {
            self.finishing && !self.ready.load(Ordering::Acquire)
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ScriptedFinalAggregate {
        fn need_input(&self) -> bool {
            !self.finishing && self.accepting.load(Ordering::Acquire)
        }

        fn has_output(&self) -> bool {
            self.finishing && self.ready.load(Ordering::Acquire) && !self.outputs.is_empty()
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            if !self.need_input() {
                return Err("scripted final aggregate is blocked".to_string());
            }
            self.pushed_rows += chunk.len();
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            if !self.has_output() {
                return Ok(None);
            }
            Ok(self.outputs.pop_front())
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            Ok(())
        }

        fn source_observable(
            &self,
        ) -> Option<Arc<crate::exec::pipeline::schedule::observer::Observable>> {
            Some(Arc::clone(&self.observable))
        }

        fn sink_observable(
            &self,
        ) -> Option<Arc<crate::exec::pipeline::schedule::observer::Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    struct ErrorOnlyFinalAggregate;

    impl Operator for ErrorOnlyFinalAggregate {
        fn name(&self) -> &str {
            "ERROR_ONLY_FINAL_AGGREGATE"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ErrorOnlyFinalAggregate {
        fn need_input(&self) -> bool {
            true
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
            state
                .error_state()
                .set_error("scripted final aggregate failed asynchronously".to_string());
            Ok(())
        }
    }

    struct FinalizeErrorAggregate;

    impl Operator for FinalizeErrorAggregate {
        fn name(&self) -> &str {
            "FINALIZE_ERROR_AGGREGATE"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for FinalizeErrorAggregate {
        fn need_input(&self) -> bool {
            true
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Err("scripted synchronous finalize failure".to_string())
        }
    }

    /// One writer input. The finish node is n-ary, so tests build a `Vec`; the
    /// operator's behaviour does not depend on how many inputs converge, only
    /// on the rows that arrive.
    fn values_input() -> Vec<ExecNode> {
        vec![ExecNode {
            kind: crate::exec::node::ExecNodeKind::Values(ValuesNode {
                chunk: Chunk::default(),
                node_id: 1,
            }),
        }]
    }

    fn finish_node(
        targets: usize,
        validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    ) -> TableFinishNode {
        let expected = (0..targets)
            .map(|value| target(u32::try_from(value).expect("bounded")))
            .collect();
        TableFinishNode::try_new(values_input(), 3, expected, validator).expect("table finish node")
    }

    fn factory(targets: usize) -> TableFinishOperatorFactory {
        TableFinishOperatorFactory::new_with_arena(
            &finish_node(targets, Arc::new(AcceptEveryCarrier::default())),
            Arc::new(ExprArena::default()),
        )
    }

    fn writer_rows(rows: Vec<RawRow>) -> Chunk {
        let mut kinds = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        let mut row_counts = Vec::with_capacity(rows.len());
        let mut payloads = BinaryBuilder::new();
        for (kind, ordinal, row_count, payload) in rows {
            kinds.push(kind);
            ordinals.push(ordinal);
            row_counts.push(row_count);
            match payload {
                Some(bytes) => payloads.append_value(&bytes),
                None => payloads.append_null(),
            }
        }
        // Arrow permits constructing a null bitmap against a non-null field;
        // the operator must still validate the row rather than trusting schema
        // metadata as proof about an untrusted Exchange batch.
        let relation = WriterMultiplexRelationSchema::empty();
        let schema = Arc::clone(relation.contract().arrow_schema());
        let chunk_schema = Arc::clone(relation.chunk_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(payloads.finish()) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("writer relation batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("writer relation chunk")
    }

    type CompositeRow = (i8, i32, Option<i64>, Option<Vec<u8>>, Vec<Option<Vec<u8>>>);

    fn composite_rows(schema: &WriterMultiplexRelationSchema, rows: Vec<CompositeRow>) -> Chunk {
        let mut kinds = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        let mut row_counts = Vec::with_capacity(rows.len());
        let mut fragments = BinaryBuilder::new();
        let channels = schema.contract().auxiliary_channels().len();
        let mut auxiliary = (0..channels)
            .map(|_| BinaryBuilder::new())
            .collect::<Vec<_>>();
        for (kind, target, row_count, fragment, values) in rows {
            assert_eq!(values.len(), channels);
            kinds.push(kind);
            ordinals.push(target);
            row_counts.push(row_count);
            match fragment {
                Some(bytes) => fragments.append_value(bytes),
                None => fragments.append_null(),
            }
            for (builder, value) in auxiliary.iter_mut().zip(values) {
                match value {
                    Some(bytes) => builder.append_value(bytes),
                    None => builder.append_null(),
                }
            }
        }
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(fragments.finish()),
        ];
        columns.extend(
            auxiliary
                .into_iter()
                .map(|mut builder| Arc::new(builder.finish()) as ArrayRef),
        );
        let batch = RecordBatch::try_new(schema.contract().arrow_schema().clone(), columns)
            .expect("composite writer batch");
        Chunk::try_new_with_chunk_schema(batch, Arc::clone(schema.chunk_schema()))
            .expect("composite writer chunk")
    }

    fn composite_runtime_state(
        function_set: Arc<crate::exec::expr::agg::SealedExecutionFunctionSet>,
        tracker: Option<Arc<MemTracker>>,
    ) -> RuntimeState {
        let runtime = Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    scan_threads: 1,
                    scan_queue_capacity: 8,
                    spill_io_threads: 1,
                    spill_io_queue_capacity: 8,
                    spill_storage: ExecutionSpillStorageConfig::default(),
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    connector_io_tasks_per_scan_operator: 1,
                    scan_submit_fail_max: 1,
                    scan_submit_fail_timeout_ms: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                function_set,
            )
            .expect("composite test runtime"),
        );
        RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            tracker,
            None,
            None,
            Some(runtime),
            None,
        )
    }

    fn composite_fixture(
        target_count: usize,
        channel_count: usize,
        max_output_rows: usize,
    ) -> (
        TableFinishOperatorFactory,
        RuntimeState,
        WriterMultiplexRelationSchema,
    ) {
        let function_set = crate::exec::expr::agg::test_builtin_execution_function_set();
        let resolved = function_set
            .catalog()
            .resolve_aggregate_trusted("any_value", &[DataType::Binary])
            .expect("any_value(Binary)");
        let writer_contract = WriterMultiplexSchema::try_new(
            (0..channel_count)
                .map(|index| {
                    WriterAuxiliaryChannel::try_new(
                        100 + u32::try_from(index).expect("channel slot"),
                        format!("partial_{index}"),
                        DataType::Binary,
                    )
                    .expect("auxiliary channel")
                })
                .collect(),
        )
        .expect("writer schema");
        let writer_schema = WriterMultiplexRelationSchema::try_new(writer_contract)
            .expect("execution writer schema");
        let root_schema = RootWriteResultRelationSchema::try_new(RootWriteResultSchema::new())
            .expect("root schema");
        let grouping_output_slot_id = novarocks_types::SlotId::new(10_000);
        let calls = (0..channel_count)
            .map(|index| WriterFinalAggregateCall {
                function_name: Arc::from("any_value"),
                resolved: resolved.clone(),
                intermediate_input_slot_id: novarocks_types::SlotId::new(
                    100 + u32::try_from(index).expect("channel slot"),
                ),
                final_output_slot_id: novarocks_types::SlotId::new(
                    10_001 + u32::try_from(index).expect("final slot"),
                ),
            })
            .collect::<Vec<_>>();
        let mut arena = ExprArena::default();
        let blob_type = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Utf8(
                "test/blob-v1".to_string(),
            )),
            DataType::Utf8,
        );
        let mappings = (0..target_count)
            .flat_map(|target_index| {
                (0..channel_count).map(move |channel_index| (target_index, channel_index))
            })
            .map(
                |(target_index, channel_index)| WriterGroupedUnpivotMapping {
                    grouping_key: u32::try_from(target_index).expect("target"),
                    input_value_slot_id: calls[channel_index].final_output_slot_id,
                    constants: vec![
                        crate::exec::node::unpivot::UnpivotConstant::Int32List(vec![
                            i32::try_from(target_index * channel_count + channel_index + 1)
                                .expect("field id"),
                        ]),
                        crate::exec::node::unpivot::UnpivotConstant::Scalar {
                            expr_id: blob_type,
                            nullable: false,
                        },
                        crate::exec::node::unpivot::UnpivotConstant::Utf8Map(Vec::new()),
                    ],
                },
            )
            .collect();
        let final_plan = WriterFinalAggregatePlan {
            calls,
            unpivot: Some(WriterGroupedUnpivotPlan {
                grouping_input_slot_id:
                    crate::exec::node::table_write_relation::WRITE_RELATION_TARGET_SLOT,
                grouping_output_slot_id,
                passthrough_output_slot_id: novarocks_types::SlotId::new(
                    root_write_result_column_id(ROOT_WRITE_RESULT_TARGET_INDEX),
                ),
                value_output_slot_id: novarocks_types::SlotId::new(root_write_result_column_id(
                    ROOT_WRITE_RESULT_BODY_INDEX,
                )),
                literal_output_slot_ids: vec![
                    novarocks_types::SlotId::new(root_write_result_column_id(
                        ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
                    )),
                    novarocks_types::SlotId::new(root_write_result_column_id(
                        ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
                    )),
                    novarocks_types::SlotId::new(root_write_result_column_id(
                        ROOT_WRITE_RESULT_PROPERTIES_INDEX,
                    )),
                ],
                mappings,
                max_output_rows,
                max_output_bytes: 32 * 1024 * 1024,
            }),
        };
        let expected = (0..target_count)
            .map(|index| target(u32::try_from(index).expect("target")))
            .collect();
        let node = TableFinishNode::try_new_with_relations(
            values_input(),
            3,
            expected,
            Arc::new(AcceptEveryCarrier::default()),
            writer_schema.clone(),
            root_schema,
            final_plan,
        )
        .expect("composite finish node");
        let state = composite_runtime_state(Arc::clone(&function_set), None);
        (
            TableFinishOperatorFactory::new_with_arena(&node, Arc::new(arena)),
            state,
            writer_schema,
        )
    }

    fn scripted_final_chunk(
        factory: &TableFinishOperatorFactory,
        rows: Vec<(i32, Vec<Option<Vec<u8>>>)>,
    ) -> Chunk {
        let unpivot = factory.final_plan.unpivot.as_ref().expect("unpivot plan");
        let mut groups = Vec::with_capacity(rows.len());
        let mut values = (0..factory.final_plan.calls.len())
            .map(|_| BinaryBuilder::new())
            .collect::<Vec<_>>();
        for (group, row_values) in rows {
            assert_eq!(row_values.len(), values.len());
            groups.push(group);
            for (builder, value) in values.iter_mut().zip(row_values) {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
        }
        let mut fields = vec![Field::new("write_target_ordinal", DataType::Int32, false)];
        let mut slots = vec![unpivot.grouping_output_slot_id];
        let mut columns = vec![Arc::new(Int32Array::from(groups)) as ArrayRef];
        for (index, (call, mut builder)) in factory.final_plan.calls.iter().zip(values).enumerate()
        {
            fields.push(Field::new(
                format!("final_aggregate_{index}"),
                call.resolved.output_type.clone(),
                true,
            ));
            slots.push(call.final_output_slot_id);
            columns.push(Arc::new(builder.finish()));
        }
        let schema = Schema::new(fields);
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &slots)
            .expect("scripted final schema");
        let batch = RecordBatch::try_new(Arc::new(schema), columns).expect("scripted final batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("scripted final chunk")
    }

    fn row_count_row(ordinal: i32, rows: i64) -> RawRow {
        (
            WriterRowKind::RowCount.to_wire(),
            Some(ordinal),
            Some(rows),
            None,
        )
    }

    fn fragment_row(ordinal: i32, bytes: Vec<u8>) -> RawRow {
        (
            WriterRowKind::CommitFragment.to_wire(),
            Some(ordinal),
            None,
            Some(bytes),
        )
    }

    fn finish_columns(chunk: &Chunk) -> (Int8Array, Int32Array, Int64Array, BinaryArray) {
        let kinds = chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("kind")
            .clone();
        let ordinals = chunk.columns()[1]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ordinal")
            .clone();
        let row_counts = chunk.columns()[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row count")
            .clone();
        let fragments = chunk.columns()[3]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("fragment")
            .clone();
        (kinds, ordinals, row_counts, fragments)
    }

    fn run(operator: &mut Box<dyn Operator>, chunks: Vec<Chunk>) -> Result<Vec<Chunk>, String> {
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        for chunk in chunks {
            processor.push_chunk(&state, chunk)?;
        }
        assert!(
            !processor.has_output(),
            "table finish must emit nothing before EOS"
        );
        processor.set_finishing(&state)?;
        let mut outputs = Vec::new();
        while !operator.is_finished() {
            if let Some(output) = operator
                .as_processor_mut()
                .expect("processor")
                .pull_chunk(&state)?
            {
                outputs.push(output);
            }
        }
        Ok(outputs)
    }

    #[test]
    fn table_finish_runs_at_dop_one_and_rejects_any_other_parallelism() {
        let factory = factory(1);
        let mut single = factory.create(1, 0);
        assert!(single.prepare().is_ok());

        let mut wide = factory.create(4, 0);
        let error = wide.prepare().expect_err("DOP > 1 must fail closed");
        assert!(error.contains("must run at DOP 1"));

        let mut second_driver = factory.create(1, 1);
        let error = second_driver
            .prepare()
            .expect_err("a second driver must fail closed");
        assert!(error.contains("must run at DOP 1"));
    }

    #[test]
    fn table_finish_checked_sums_row_counts_from_many_senders_in_any_order() {
        let factory = factory(2);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let outputs = run(
            &mut operator,
            vec![
                writer_rows(vec![
                    fragment_row(1, b"second-target".to_vec()),
                    row_count_row(1, 7),
                ]),
                writer_rows(vec![row_count_row(0, 5)]),
                writer_rows(vec![
                    row_count_row(0, 0),
                    fragment_row(0, b"first-target".to_vec()),
                ]),
            ],
        )
        .expect("finish");
        let mut prepared = Vec::new();
        let mut summaries = 0;
        for output in &outputs {
            let (kinds, ordinals, row_counts, fragments) = finish_columns(output);
            for row in 0..output.len() {
                match RootRowKind::from_wire(kinds.value(row)).expect("root row kind") {
                    RootRowKind::Summary => {
                        summaries += 1;
                        assert!(ordinals.is_null(row));
                        assert_eq!(row_counts.value(row), 12);
                        assert!(fragments.is_null(row));
                    }
                    RootRowKind::PreparedFragment => {
                        assert!(row_counts.is_null(row));
                        prepared.push((ordinals.value(row), fragments.value(row).to_vec()));
                    }
                    RootRowKind::ArtifactDraft => panic!("fragment-only finish emitted artifact"),
                }
            }
        }
        assert_eq!(summaries, 1);
        prepared.sort_unstable();
        assert_eq!(
            prepared,
            vec![
                (0, b"first-target".to_vec()),
                (1, b"second-target".to_vec())
            ]
        );
    }

    #[test]
    fn table_finish_emits_only_a_summary_when_no_writer_staged_anything() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let outputs =
            run(&mut operator, vec![writer_rows(vec![row_count_row(0, 0)])]).expect("finish");
        assert_eq!(outputs.len(), 1);
        let output = &outputs[0];
        assert_eq!(output.len(), 1);
        let (kinds, _, row_counts, _) = finish_columns(output);
        assert_eq!(kinds.value(0), RootRowKind::Summary.to_wire());
        assert_eq!(row_counts.value(0), 0);
    }

    #[test]
    fn table_finish_rejects_inconsistent_null_and_kind_combinations() {
        let payload_mismatch = "payload does not match its row kind";
        let cases: Vec<(Vec<RawRow>, &str)> = vec![
            // ROW_COUNT with no row count
            (
                vec![(WriterRowKind::RowCount.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            // ROW_COUNT carrying a commit fragment
            (
                vec![(
                    WriterRowKind::RowCount.to_wire(),
                    Some(0),
                    Some(1),
                    Some(b"x".to_vec()),
                )],
                payload_mismatch,
            ),
            // COMMIT_FRAGMENT carrying a row count
            (
                vec![(
                    WriterRowKind::CommitFragment.to_wire(),
                    Some(0),
                    Some(1),
                    Some(b"x".to_vec()),
                )],
                payload_mismatch,
            ),
            // COMMIT_FRAGMENT with no fragment
            (
                vec![(WriterRowKind::CommitFragment.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            // both payloads null
            (
                vec![(WriterRowKind::CommitFragment.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            (
                vec![(0, Some(0), Some(1), None)],
                "unknown connector writer row kind",
            ),
            (vec![(3, Some(0), Some(1), None)], payload_mismatch),
        ];

        for (rows, expected) in cases {
            let factory = factory(1);
            let mut operator = factory.create(1, 0);
            operator.prepare().expect("prepare");
            let state = RuntimeState::default();
            let error = operator
                .as_processor_mut()
                .expect("processor")
                .push_chunk(&state, writer_rows(rows))
                .expect_err("invalid row shape");
            assert!(
                error.contains(expected),
                "expected {expected:?} in error {error:?}"
            );
        }
    }

    #[test]
    fn table_finish_rejects_a_target_ordinal_outside_the_sealed_set() {
        let factory = factory(2);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(2, 1)]))
            .expect_err("ordinal outside the sealed set");
        assert!(error.contains("outside the sealed set of 2 targets"));
    }

    #[test]
    fn table_finish_rejects_a_non_canonical_carrier_before_it_enters_the_set() {
        let node = finish_node(1, Arc::new(RejectEveryCarrier));
        let factory =
            TableFinishOperatorFactory::new_with_arena(&node, Arc::new(ExprArena::default()));
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![fragment_row(0, b"junk".to_vec())]))
            .expect_err("foreign carrier");
        assert!(error.contains("not a canonical carrier of the expected provider"));
    }

    #[test]
    fn table_finish_row_count_overflow_is_an_error_not_a_saturating_counter() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        // The accumulator stays unsigned and checked, so two maximal signed
        // row counts fit and the third must fail rather than wrap.
        for _ in 0..2 {
            processor
                .push_chunk(&state, writer_rows(vec![row_count_row(0, i64::MAX)]))
                .expect("within the unsigned accumulator");
        }
        let error = processor
            .push_chunk(&state, writer_rows(vec![row_count_row(0, i64::MAX)]))
            .expect_err("row count overflow");
        assert!(error.contains("row count overflowed"));
    }

    #[test]
    fn table_finish_rejects_a_negative_row_count_as_corrupt_data() {
        // The relation's carrier is signed, so a corrupt negative value is
        // newly expressible. It must be rejected, never reinterpreted as a
        // huge unsigned row count the client would see as truth.
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(0, -1)]))
            .expect_err("a negative row count is corrupt data");
        assert!(
            error.contains("payload does not match"),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn table_finish_rejects_a_negative_target_ordinal_as_corrupt_data() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(-1, 1)]))
            .expect_err("a negative target ordinal is corrupt data");
        assert!(
            error.contains("target ordinal is negative"),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn table_finish_accepts_the_exact_single_fragment_limit_and_rejects_one_more() {
        let state = RuntimeState::default();
        let at_limit = factory(1);
        let mut operator = at_limit.create(1, 0);
        operator.prepare().expect("prepare");
        operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                writer_rows(vec![fragment_row(
                    0,
                    vec![7; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                )]),
            )
            .expect("the exact single-fragment budget is legal");

        let over_limit = factory(1);
        let mut operator = over_limit.create(1, 0);
        operator.prepare().expect("prepare");
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                writer_rows(vec![fragment_row(
                    0,
                    vec![7; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1],
                )]),
            )
            .expect_err("over the single-fragment budget");
        assert!(error.contains("exceeds the frozen single-fragment budget"));
    }

    #[test]
    fn table_finish_rejects_the_prepared_write_set_byte_budget() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        let full = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;
        for _ in 0..full {
            processor
                .push_chunk(
                    &state,
                    writer_rows(vec![fragment_row(
                        0,
                        vec![1; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                    )]),
                )
                .expect("within the set byte budget");
        }
        let error = processor
            .push_chunk(&state, writer_rows(vec![fragment_row(0, vec![1])]))
            .expect_err("over the set byte budget");
        assert!(error.contains("exceeds the frozen byte budget"));
    }

    #[test]
    fn table_finish_rejects_the_prepared_write_set_entry_budget() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        let rows = (0..MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES)
            .map(|_| fragment_row(0, vec![1]))
            .collect::<Vec<_>>();
        processor
            .push_chunk(&state, writer_rows(rows))
            .expect("the exact entry budget is legal");
        let error = processor
            .push_chunk(&state, writer_rows(vec![fragment_row(0, vec![1])]))
            .expect_err("over the entry budget");
        assert!(error.contains("exceeds the frozen entry budget"));
    }

    #[test]
    fn table_finish_streams_a_large_prepared_set_in_byte_bounded_batches() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        let tracker = MemTracker::new_root("table-finish-large-prefix");
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        for index in 0..40u8 {
            operator
                .as_processor_mut()
                .expect("processor")
                .push_chunk(
                    &state,
                    writer_rows(vec![fragment_row(
                        0,
                        vec![index; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                    )]),
                )
                .expect("bounded prepared fragment");
        }
        operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("gather EOS");
        let mut outputs = Vec::new();
        while !operator.is_finished() {
            if let Some(output) = operator
                .as_processor_mut()
                .expect("processor")
                .pull_chunk(&state)
                .expect("stream prefix")
            {
                assert!(
                    root_output_size(&output.batch, None).expect("output size")
                        <= MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                );
                outputs.push(output);
            }
        }
        let fragment_batches = outputs
            .iter()
            .filter(|output| {
                output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("kind")
                    .value(0)
                    == RootRowKind::PreparedFragment.to_wire()
            })
            .count();
        assert!(fragment_batches >= 2, "large prefix must be multi-batch");
        let fragment_rows = outputs
            .iter()
            .flat_map(|output| {
                let kinds = output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("kind");
                (0..output.len()).map(move |row| kinds.value(row))
            })
            .filter(|kind| *kind == RootRowKind::PreparedFragment.to_wire())
            .count();
        assert_eq!(fragment_rows, 40);
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("kind"))
                .flat_map(|kinds| (0..kinds.len()).map(|row| kinds.value(row)))
                .filter(|kind| *kind == RootRowKind::Summary.to_wire())
                .count(),
            1
        );
        drop(outputs);
        assert_eq!(tracker.current(), 0, "all prefix/output leases released");
        assert!(tracker.peak() > 40 * 1024 * 1024);
    }

    #[test]
    fn table_finish_emits_nothing_before_eos_and_releases_its_buffer_on_abort() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        {
            let processor = operator.as_processor_mut().expect("processor");
            processor
                .push_chunk(
                    &state,
                    writer_rows(vec![
                        row_count_row(0, 11),
                        fragment_row(0, vec![3; 4096]),
                        fragment_row(0, vec![4; 4096]),
                    ]),
                )
                .expect("accumulate");
            assert!(!processor.has_output());
            assert!(
                processor
                    .pull_chunk(&state)
                    .expect("pull before EOS")
                    .is_none()
            );
        }

        operator.cancel();
        let processor = operator.as_processor_mut().expect("processor");
        assert!(!processor.has_output());
        assert!(
            processor
                .pull_chunk(&state)
                .expect("pull after abort")
                .is_none()
        );
        assert!(operator.is_finished());
    }

    #[test]
    fn table_finish_drops_its_buffer_when_a_row_is_rejected() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(&state, writer_rows(vec![fragment_row(0, vec![1; 1024])]))
            .expect("first fragment");
        let error = processor
            .push_chunk(
                &state,
                writer_rows(vec![(
                    WriterRowKind::CommitFragment.to_wire(),
                    Some(0),
                    Some(1),
                    Some(vec![2; 1024]),
                )]),
            )
            .expect_err("invalid row");
        assert!(error.contains("payload does not match its row kind"));
        let finish_error = processor
            .set_finishing(&state)
            .expect_err("a failed stream cannot be repaired into a summary");
        assert!(finish_error.contains("cannot finalize a failed write stream"));
        assert!(!processor.has_output());
    }

    #[test]
    fn table_finish_rejects_a_writer_row_that_arrives_after_eos() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        processor.set_finishing(&state).expect("finish");
        let error = processor
            .push_chunk(&state, writer_rows(vec![row_count_row(0, 1)]))
            .expect_err("a row after EOS is a contract violation");
        assert!(error.contains("after EOS"));
    }

    #[test]
    fn table_finish_node_requires_a_non_empty_duplicate_free_target_set() {
        assert!(
            TableFinishNode::try_new(
                values_input(),
                3,
                Vec::new(),
                Arc::new(AcceptEveryCarrier::default()),
            )
            .is_err()
        );
        assert!(
            TableFinishNode::try_new(
                values_input(),
                3,
                vec![target(1), target(1)],
                Arc::new(AcceptEveryCarrier::default()),
            )
            .is_err()
        );
    }

    /// A copy-on-write statement drives one query per rewritten file against a
    /// single write session, and each of those queries compiles exactly one
    /// writer -- the one at that group's own ordinal. Query `k` therefore
    /// expects `[k]`, which is correctly not dense from zero. Denseness belongs
    /// to the session's sealed target set, not to any one query.
    #[test]
    fn table_finish_accepts_a_single_writer_query_at_a_non_zero_ordinal() {
        let node = TableFinishNode::try_new(
            values_input(),
            3,
            vec![target(2)],
            Arc::new(AcceptEveryCarrier::default()),
        )
        .expect("a single-writer query at ordinal 2");
        assert!(node.accepts_target(target(2)));
        // Membership stays exact: a bound check would have admitted every
        // ordinal below the one this query actually feeds.
        assert!(!node.accepts_target(target(0)));
        assert!(!node.accepts_target(target(1)));

        let factory =
            TableFinishOperatorFactory::new_with_arena(&node, Arc::new(ExprArena::default()));
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(&state, writer_rows(vec![row_count_row(2, 5)]))
            .expect("its own writer's rows are accepted");
        let error = processor
            .push_chunk(&state, writer_rows(vec![row_count_row(0, 1)]))
            .expect_err("a target this query never compiled a writer for");
        assert!(error.contains("outside the sealed set"), "{error}");
    }

    #[test]
    fn composite_finish_demultiplexes_interleaved_rows_merges_sparse_partials_and_streams_unpivot()
    {
        let (factory, state, writer_schema) = composite_fixture(2, 2, 1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind aggregate");
        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![
                        (
                            WriterRowKind::CommitFragment.to_wire(),
                            1,
                            None,
                            Some(b"fragment-one".to_vec()),
                            vec![None, None],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            0,
                            None,
                            None,
                            vec![Some(b"zero-a".to_vec()), None],
                        ),
                        (
                            WriterRowKind::RowCount.to_wire(),
                            1,
                            Some(7),
                            None,
                            vec![None, None],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            1,
                            None,
                            None,
                            vec![Some(b"one-a".to_vec()), None],
                        ),
                    ],
                ),
            )
            .expect("first interleaved batch");
        processor
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![
                        (
                            WriterRowKind::RowCount.to_wire(),
                            0,
                            Some(5),
                            None,
                            vec![None, None],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            0,
                            None,
                            None,
                            vec![None, Some(b"zero-b".to_vec())],
                        ),
                        (
                            WriterRowKind::CommitFragment.to_wire(),
                            0,
                            None,
                            Some(b"fragment-zero".to_vec()),
                            vec![None, None],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            1,
                            None,
                            None,
                            vec![None, Some(b"one-b".to_vec())],
                        ),
                    ],
                ),
            )
            .expect("second interleaved batch");
        assert!(!processor.has_output(), "Gather has not reached EOS");
        assert!(processor.pull_chunk(&state).expect("early pull").is_none());
        processor
            .set_finishing(&state)
            .expect("all gather inputs reached EOS");

        let mut outputs = Vec::new();
        while !operator.is_finished() {
            if let Some(chunk) = operator
                .as_processor_mut()
                .expect("processor")
                .pull_chunk(&state)
                .expect("root output")
            {
                outputs.push(chunk);
            }
        }
        assert!(
            outputs.len() >= 6,
            "summary and fragments stream separately before four bounded artifacts"
        );
        let mut summaries = 0;
        let mut prepared = Vec::new();
        let mut artifacts = Vec::new();
        for output in &outputs {
            let kinds = output.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("kind");
            for row in 0..output.len() {
                match RootRowKind::from_wire(kinds.value(row)).expect("root kind") {
                    RootRowKind::Summary => {
                        summaries += 1;
                        let counts = output.columns()[2]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("row counts");
                        assert_eq!(counts.value(row), 12);
                    }
                    RootRowKind::PreparedFragment => {
                        let targets = output.columns()[1]
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("targets");
                        let fragments = output.columns()[3]
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .expect("fragments");
                        prepared.push((targets.value(row), fragments.value(row).to_vec()));
                    }
                    RootRowKind::ArtifactDraft => {
                        let targets = output.columns()[1]
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("targets");
                        let bodies = output.columns()[6]
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .expect("bodies");
                        let types = output.columns()[5]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("blob types");
                        let fields = output.columns()[4]
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .expect("input fields");
                        assert_eq!(types.value(row), "test/blob-v1");
                        let ids = fields.value(row);
                        let ids = ids.as_any().downcast_ref::<Int32Array>().expect("ids");
                        artifacts.push((
                            targets.value(row),
                            ids.value(0),
                            bodies.value(row).to_vec(),
                        ));
                    }
                }
            }
        }
        prepared.sort();
        artifacts.sort();
        assert_eq!(summaries, 1);
        assert_eq!(
            prepared,
            vec![
                (0, b"fragment-zero".to_vec()),
                (1, b"fragment-one".to_vec())
            ]
        );
        assert_eq!(
            artifacts,
            vec![
                (0, 1, b"zero-a".to_vec()),
                (0, 2, b"zero-b".to_vec()),
                (1, 3, b"one-a".to_vec()),
                (1, 4, b"one-b".to_vec()),
            ]
        );
    }

    #[test]
    fn composite_finish_streams_multiple_final_aggregate_batches_exactly_once() {
        let (factory, state, writer_schema) = composite_fixture(2, 1, 8);
        let final_batches = vec![
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"zero".to_vec())])]),
            scripted_final_chunk(&factory, vec![(1, vec![Some(b"one".to_vec())])]),
        ];
        let mut operator = factory.create_operator(1, 0);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.aggregate = Some(Box::new(ScriptedFinalAggregate::new(
            final_batches,
            true,
            true,
        )));
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            0,
                            None,
                            None,
                            vec![Some(b"partial-zero".to_vec())],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            1,
                            None,
                            None,
                            vec![Some(b"partial-one".to_vec())],
                        ),
                    ],
                ),
            )
            .expect("partial input");
        operator.set_finishing(&state).expect("gather EOS");

        let mut artifacts = BTreeMap::new();
        let mut summaries = 0;
        while !operator.is_finished() {
            let Some(output) = operator.pull_chunk(&state).expect("root output") else {
                continue;
            };
            let kinds = output.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("kind");
            let targets = output.columns()[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("target");
            let bodies = output.columns()[ROOT_WRITE_RESULT_BODY_INDEX]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("body");
            for row in 0..output.len() {
                match RootRowKind::from_wire(kinds.value(row)).expect("kind") {
                    RootRowKind::Summary => summaries += 1,
                    RootRowKind::PreparedFragment => panic!("unexpected fragment"),
                    RootRowKind::ArtifactDraft => {
                        assert!(
                            artifacts
                                .insert(targets.value(row), bodies.value(row).to_vec())
                                .is_none(),
                            "each target must be emitted exactly once"
                        );
                    }
                }
            }
        }
        assert_eq!(summaries, 1);
        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[&0], b"zero");
        assert_eq!(artifacts[&1], b"one");
    }

    #[test]
    fn composite_finish_forwards_child_readiness_before_and_after_gather_eos() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let final_batch =
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"complete".to_vec())])]);
        let scripted = ScriptedFinalAggregate::new(vec![final_batch], false, false);
        let accepting = Arc::clone(&scripted.accepting);
        let ready = Arc::clone(&scripted.ready);
        let observable = Arc::clone(&scripted.observable);
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "final-aggregate-blocked-metrics",
        ));
        operator.set_profiles(profiles.clone());
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.aggregate = Some(Box::new(scripted));

        assert!(
            !operator.need_input(),
            "blocked child must stop Gather input"
        );
        assert!(Arc::ptr_eq(
            &operator.sink_observable().expect("sink observable"),
            &observable
        ));
        std::thread::sleep(Duration::from_millis(1));
        accepting.store(true, Ordering::Release);
        observable.notify_observers();
        assert!(operator.need_input());
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    )],
                ),
            )
            .expect("partial input");
        operator.set_finishing(&state).expect("gather EOS");
        assert!(operator.pending_finish());
        assert!(Arc::ptr_eq(
            &operator.source_observable().expect("source observable"),
            &observable
        ));
        ready.store(true, Ordering::Release);
        observable.notify_observers();
        assert!(!operator.pending_finish());
        while !operator.is_finished() {
            let _ = operator.pull_chunk(&state).expect("root output");
        }
        operator.close().expect("close");
        assert!(
            profiles
                .common
                .counter_value("FinalAggregateBlockedCount")
                .is_some_and(|intervals| intervals >= 1)
        );
        assert!(
            profiles
                .common
                .counter_value("FinalAggregateBlockedTime")
                .is_some_and(|elapsed| elapsed > 0)
        );
    }

    #[test]
    fn composite_finish_counts_driver_pending_finalize_as_one_wait_interval() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let final_batch =
            scripted_final_chunk(&factory, vec![(0, vec![Some(b"complete".to_vec())])]);
        let scripted = ScriptedFinalAggregate::new(vec![final_batch], true, false);
        let ready = Arc::clone(&scripted.ready);
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "driver-pending-finalize-metrics",
        ));
        operator.set_profiles(profiles.clone());
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.aggregate = Some(Box::new(scripted));
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    )],
                ),
            )
            .expect("partial input");

        // This is PipelineDriver's EOS sequence: set finishing once, then
        // poll only pending_finish while the asynchronous owner is parked.
        operator.set_finishing(&state).expect("gather EOS");
        assert!(operator.pending_finish());
        assert!(operator.pending_finish());
        std::thread::sleep(Duration::from_millis(1));
        assert!(operator.pending_finish());
        ready.store(true, Ordering::Release);
        assert!(!operator.pending_finish());

        while !operator.is_finished() {
            let _ = operator.pull_chunk(&state).expect("root output");
        }
        operator.close().expect("close");
        assert_eq!(
            profiles.common.counter_value("FinalAggregateBlockedCount"),
            Some(1),
            "repeated pending_finish polls belong to one continuous wait"
        );
        assert!(
            profiles
                .common
                .counter_value("FinalAggregateBlockedTime")
                .is_some_and(|elapsed| elapsed > 0),
            "the EOS pending-finish wait must contribute wall time"
        );
    }

    #[test]
    fn composite_finish_closes_driver_pending_finalize_on_failure() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let scripted = ScriptedFinalAggregate::new(Vec::new(), true, false);
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "failed-driver-pending-finalize-metrics",
        ));
        operator.set_profiles(profiles.clone());
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.aggregate = Some(Box::new(scripted));
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    )],
                ),
            )
            .expect("partial input");
        operator.set_finishing(&state).expect("gather EOS");
        assert!(operator.pending_finish());
        assert!(operator.pending_finish());
        std::thread::sleep(Duration::from_millis(1));

        operator.on_driver_failure();
        assert!(operator.is_finished());
        assert!(!operator.pending_finish());
        let elapsed_after_failure = profiles
            .common
            .counter_value("FinalAggregateBlockedTime")
            .expect("failure must publish the closed interval");
        std::thread::sleep(Duration::from_millis(1));
        operator.close().expect("close after failure");
        assert_eq!(
            profiles.common.counter_value("FinalAggregateBlockedCount"),
            Some(1)
        );
        assert_eq!(
            profiles.common.counter_value("FinalAggregateBlockedTime"),
            Some(elapsed_after_failure),
            "failure must stop the timer rather than leave a live interval"
        );
    }

    #[test]
    fn composite_finish_surfaces_error_only_finalization_without_waiting_for_output() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let mut operator = factory.create_operator(1, 0);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.aggregate = Some(Box::new(ErrorOnlyFinalAggregate));
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    )],
                ),
            )
            .expect("partial input");
        operator.set_finishing(&state).expect("async finish starts");
        assert!(
            operator.has_output(),
            "runtime error must make the wrapper runnable"
        );
        let error = operator
            .pull_chunk(&state)
            .expect_err("runtime finalization error");
        assert!(error.contains("failed asynchronously"), "{error}");
        assert!(operator.is_finished());
    }

    #[test]
    fn composite_finish_fails_closed_when_final_aggregate_rejects_finalize() {
        let (factory, state, writer_schema) = composite_fixture(1, 1, 8);
        let tracker = MemTracker::new_root("table-finish-finalize-error");
        let mut operator = factory.create_operator(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.aggregate = Some(Box::new(FinalizeErrorAggregate));
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![
                        (
                            WriterRowKind::CommitFragment.to_wire(),
                            0,
                            None,
                            Some(vec![7; 4096]),
                            vec![None],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            0,
                            None,
                            None,
                            vec![Some(b"partial".to_vec())],
                        ),
                    ],
                ),
            )
            .expect("writer input");
        let error = operator
            .set_finishing(&state)
            .expect_err("final aggregate rejects finalize");
        assert!(error.contains("synchronous finalize failure"), "{error}");
        assert!(operator.is_finished());
        assert_eq!(tracker.current(), 0, "failed finalize releases all buffers");
    }

    #[cfg(debug_assertions)]
    #[test]
    fn final_aggregate_guard_rejects_merge_without_root_output() {
        let (mut factory, state, writer_schema) = composite_fixture(1, 1, 8);
        factory.aggregate_guard = Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::FinalMerge,
        ));
        let mut operator = factory.create_operator(1, 0);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");

        let error = operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    )],
                ),
            )
            .expect_err("final merge fault");

        assert!(error.contains("FinalMerge"), "{error}");
        assert!(operator.is_finished(), "the Root operator must fail closed");
        assert!(
            !operator.has_output(),
            "a failed Root cannot expose EOF data"
        );
        assert!(
            operator
                .pull_chunk(&state)
                .expect("failed Root pull")
                .is_none(),
            "a failed Root cannot emit a successful result row"
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn final_aggregate_guard_rejects_finalize_without_root_output() {
        let (mut factory, state, writer_schema) = composite_fixture(1, 1, 8);
        factory.aggregate_guard = Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::FinalFinalize,
        ));
        let mut operator = factory.create_operator(1, 0);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"partial".to_vec())],
                    )],
                ),
            )
            .expect("partial input before finalize fault");

        let error = operator
            .set_finishing(&state)
            .expect_err("final finalize fault");

        assert!(error.contains("FinalFinalize"), "{error}");
        assert!(operator.is_finished(), "the Root operator must fail closed");
        assert!(
            !operator.has_output(),
            "a failed Root cannot expose EOF data"
        );
        assert!(
            operator
                .pull_chunk(&state)
                .expect("failed Root pull")
                .is_none(),
            "a failed Root cannot emit a successful result row"
        );
    }

    #[test]
    fn composite_finish_streams_more_than_one_thousand_channels_and_releases_memory() {
        let channels = 1_001usize;
        let (factory, state, writer_schema) = composite_fixture(1, channels, 64);
        let tracker = MemTracker::new_root("table-finish-wide-unpivot");
        let mut operator = factory.create_operator(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        (0..channels)
                            .map(|index| Some(vec![u8::try_from(index % 251).expect("byte")]))
                            .collect(),
                    )],
                ),
            )
            .expect("wide partial input");
        operator.set_finishing(&state).expect("gather EOS");

        let mut outputs = Vec::new();
        while !operator.is_finished() {
            if let Some(output) = operator.pull_chunk(&state).expect("root output") {
                outputs.push(output);
            }
        }
        let mut artifact_rows = 0usize;
        let mut artifact_batches = 0usize;
        for output in &outputs {
            let kinds = output.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("kind");
            let in_batch = (0..output.len())
                .filter(|row| kinds.value(*row) == RootRowKind::ArtifactDraft.to_wire())
                .count();
            artifact_rows += in_batch;
            artifact_batches += usize::from(in_batch > 0);
        }
        assert_eq!(artifact_rows, channels);
        assert!(
            artifact_batches > 1,
            "wide Unpivot must emit multiple batches"
        );
        drop(outputs);
        assert_eq!(
            tracker.current(),
            0,
            "all final/Unpivot/output memory released"
        );
        assert!(tracker.peak() > 0);
    }

    #[test]
    fn composite_finish_fails_closed_on_missing_channel_coverage_at_eos() {
        let (factory, state, writer_schema) = composite_fixture(1, 2, 16);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind aggregate");
        operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![Some(b"present".to_vec()), None],
                    )],
                ),
            )
            .expect("one sparse partial");
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect_err("the second channel never arrived");
        assert!(error.contains("no non-null aggregate partial for channel 1"));
        assert!(operator.is_finished());
    }

    #[test]
    fn composite_finish_rejects_all_null_partial_and_schema_drift_at_nearest_ingress() {
        let (factory, state, writer_schema) = composite_fixture(1, 2, 16);
        let mut all_null = factory.create(1, 0);
        all_null.prepare().expect("prepare");
        all_null.bind_runtime_state(&state).expect("bind aggregate");
        let error = all_null
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![(
                        WriterRowKind::AggregatePartial.to_wire(),
                        0,
                        None,
                        None,
                        vec![None, None],
                    )],
                ),
            )
            .expect_err("an all-null partial carries no contribution");
        assert!(error.contains("payload does not match its row kind"));

        let mut drifted = factory.create(1, 0);
        drifted.prepare().expect("prepare");
        drifted.bind_runtime_state(&state).expect("bind aggregate");
        let error = drifted
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(0, 1)]))
            .expect_err("the four-column legacy schema is not the frozen multiplex schema");
        assert!(error.contains("schema drifted from the frozen plan"));
    }

    #[test]
    fn composite_finish_cancel_releases_fragment_final_and_output_memory() {
        let (factory, base_state, writer_schema) = composite_fixture(1, 1, 1);
        let tracker = MemTracker::new_root("table-finish-test");
        let state = composite_runtime_state(
            Arc::clone(
                base_state
                    .execution_runtime()
                    .expect("runtime")
                    .function_set(),
            ),
            Some(Arc::clone(&tracker)),
        );
        let mut operator = factory.create(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind aggregate");
        operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                composite_rows(
                    &writer_schema,
                    vec![
                        (
                            WriterRowKind::CommitFragment.to_wire(),
                            0,
                            None,
                            Some(vec![7; 4096]),
                            vec![None],
                        ),
                        (
                            WriterRowKind::AggregatePartial.to_wire(),
                            0,
                            None,
                            None,
                            vec![Some(vec![8; 4096])],
                        ),
                    ],
                ),
            )
            .expect("buffer input");
        assert!(tracker.current() > 0);
        operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("finalize");
        operator.cancel();
        drop(operator);
        assert_eq!(tracker.current(), 0, "cancel releases every retained owner");
    }
}
